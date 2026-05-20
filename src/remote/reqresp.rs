//! Remote request/response over iroh.
//!
//! Each call opens a fresh bi-directional QUIC stream. The wire
//! protocol is described in [`super::handshake`]:
//!
//! 1. Client writes `[u32 REQRESP_MAGIC][version][req_type_hash]
//!    [resp_type_hash][u32 req_size][u32 resp_size][u16 topic_len]
//!    [topic][u32 length][request bytes]` and `finish()`es its
//!    send half.
//! 2. Server reads the handshake, looks up its registered handler
//!    for the topic, calls it, then writes
//!    `[u32 length][response bytes]` on its send half and `finish()`.
//! 3. Client reads the response frame on its recv half.
//!
//! Payloads use `bytemuck::Pod` for serialization (same as the
//! pub/sub path — Phase 4 doesn't bring serde in yet).

use std::any::type_name;
use std::marker::PhantomData;
use std::sync::Arc;

use bytemuck::Pod;
use iroh::endpoint::{RecvStream, SendStream};

use crate::error::{Error, Result};
use crate::local::layout::fnv1a64;
use crate::remote::handshake::{HANDSHAKE_VERSION, MAX_TOPIC_LEN, REQRESP_MAGIC};
use crate::remote::transport::{
    ensure_peer_connection, read_frame, write_frame, ErasedReqHandler, InnerShared,
    RequestServerEntry, RemoteTransport,
};

impl RemoteTransport {
    /// Register a request server for this transport's topic. The
    /// handler runs on the transport's internal tokio runtime; it
    /// must be `Send + Sync + 'static` and may not block.
    pub fn serve_requests<Req, Resp, F>(&self, handler: F) -> Result<()>
    where
        Req: Pod + Send + 'static,
        Resp: Pod + Send + 'static,
        F: Fn(Req) -> Resp + Send + Sync + 'static,
    {
        let req_size = std::mem::size_of::<Req>() as u32;
        let resp_size = std::mem::size_of::<Resp>() as u32;
        let req_hash = fnv1a64(type_name::<Req>());
        let resp_hash = fnv1a64(type_name::<Resp>());

        let handler = Arc::new(handler);
        let erased: ErasedReqHandler = Arc::new(move |bytes: &[u8]| {
            if bytes.len() != req_size as usize {
                return Err(Error::Remote(format!(
                    "request size mismatch: got {} bytes, expected {}",
                    bytes.len(),
                    req_size
                )));
            }
            // SAFETY: `Req: Pod` guarantees any byte pattern of the
            // correct size is a valid value.
            let req: Req = *bytemuck::from_bytes(bytes);
            let resp = (handler)(req);
            Ok(bytemuck::bytes_of(&resp).to_vec())
        });

        let topic = self.name().to_string();
        let shared = self.shared();
        let mut servers = shared.request_servers.lock().unwrap_or_else(|p| p.into_inner());
        if servers.contains_key(&topic) {
            return Err(Error::invalid_argument(
                "a request server is already registered for this topic",
            ));
        }
        servers.insert(
            topic,
            RequestServerEntry {
                handler: erased,
                req_type_hash: req_hash,
                resp_type_hash: resp_hash,
                req_size,
                resp_size,
            },
        );
        Ok(())
    }

    /// Build a client handle for the remote request server on this
    /// topic. Requires `peer(...)` to have been set on the builder.
    pub fn client<Req: Pod + Send + 'static, Resp: Pod + Send + 'static>(
        &self,
    ) -> Result<RemoteClient<Req, Resp>> {
        if self.shared().peer.is_none() {
            return Err(Error::invalid_argument(
                "RemoteTransport::client requires a peer; set one with .peer(addr) on the builder",
            ));
        }
        Ok(RemoteClient {
            shared: self.shared().clone(),
            runtime: self.runtime().clone(),
            topic: self.name().to_string(),
            _phantom: PhantomData,
        })
    }
}

/// Client handle for remote req/resp.
pub struct RemoteClient<Req: Pod, Resp: Pod> {
    shared: Arc<InnerShared>,
    runtime: Arc<tokio::runtime::Runtime>,
    topic: String,
    _phantom: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> RemoteClient<Req, Resp>
where
    Req: Pod + Send + 'static,
    Resp: Pod + Send + 'static,
{
    /// Send a request and wait for the response. Blocks the calling
    /// thread on the transport's internal runtime.
    pub fn call(&mut self, req: Req) -> Result<Resp> {
        let topic = self.topic.clone();
        let shared = self.shared.clone();
        let req_bytes = bytemuck::bytes_of(&req).to_vec();
        let req_hash = fnv1a64(type_name::<Req>());
        let resp_hash = fnv1a64(type_name::<Resp>());
        let req_size = std::mem::size_of::<Req>() as u32;
        let resp_size = std::mem::size_of::<Resp>() as u32;

        self.runtime.block_on(async move {
            let conn = ensure_peer_connection(&shared).await?;
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;

            write_request_handshake(
                &mut send,
                &topic,
                req_hash,
                resp_hash,
                req_size,
                resp_size,
            )
            .await?;
            write_frame(&mut send, &req_bytes).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;

            let resp_bytes = read_frame(&mut recv).await?.ok_or_else(|| {
                Error::Remote("server closed without writing a response".to_string())
            })?;
            if resp_bytes.len() != resp_size as usize {
                return Err(Error::Remote(format!(
                    "response size mismatch: got {} bytes, expected {}",
                    resp_bytes.len(),
                    resp_size
                )));
            }
            // SAFETY: `Resp: Pod` + length check guarantees a valid
            // value of `Resp` for any byte pattern of the right size.
            let resp: Resp = *bytemuck::from_bytes(&resp_bytes);
            Ok(resp)
        })
    }
}

/// Handle one incoming req/resp bi stream on the server side.
///
/// Called from [`super::transport::serve_bi`] after the dispatch
/// magic has been consumed.
pub(crate) async fn serve_request_bi(
    inner: Arc<InnerShared>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (topic, req_hash, resp_hash, req_size, resp_size) =
        read_request_handshake_tail(&mut recv).await?;

    let entry = {
        let map = inner.request_servers.lock().unwrap_or_else(|p| p.into_inner());
        match map.get(&topic) {
            Some(e) => RequestServerEntry {
                handler: e.handler.clone(),
                req_type_hash: e.req_type_hash,
                resp_type_hash: e.resp_type_hash,
                req_size: e.req_size,
                resp_size: e.resp_size,
            },
            None => return Ok(()), // no server registered → drop quietly
        }
    };

    if entry.req_type_hash != req_hash
        || entry.resp_type_hash != resp_hash
        || entry.req_size != req_size
        || entry.resp_size != resp_size
    {
        return Err(Error::TypeMismatch {
            expected: "<request server>",
            got: format!(
                "req_hash=0x{:x} resp_hash=0x{:x} req_size={} resp_size={}",
                req_hash, resp_hash, req_size, resp_size
            ),
        });
    }

    let req_bytes = read_frame(&mut recv)
        .await?
        .ok_or_else(|| Error::Remote("client closed without sending a request".to_string()))?;
    let resp_bytes = (entry.handler)(&req_bytes)?;
    write_frame(&mut send, &resp_bytes).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
    Ok(())
}

async fn write_request_handshake(
    send: &mut SendStream,
    topic: &str,
    req_hash: u64,
    resp_hash: u64,
    req_size: u32,
    resp_size: u32,
) -> Result<()> {
    if topic.len() > MAX_TOPIC_LEN as usize {
        return Err(Error::invalid_argument("topic name too long"));
    }
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 8 + 4 + 4 + 2 + topic.len());
    buf.extend_from_slice(&REQRESP_MAGIC.to_le_bytes());
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&req_hash.to_le_bytes());
    buf.extend_from_slice(&resp_hash.to_le_bytes());
    buf.extend_from_slice(&req_size.to_le_bytes());
    buf.extend_from_slice(&resp_size.to_le_bytes());
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("write request handshake: {e}")))
}

/// Read the request handshake tail. Magic was consumed by the
/// dispatcher in [`super::transport::serve_bi`].
async fn read_request_handshake_tail(
    recv: &mut RecvStream,
) -> Result<(String, u64, u64, u32, u32)> {
    let mut header = [0u8; 4 + 8 + 8 + 4 + 4 + 2];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| Error::Remote(format!("request handshake: {e}")))?;
    let version = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if version != HANDSHAKE_VERSION {
        return Err(Error::Remote(format!(
            "handshake version mismatch: peer={version} local={HANDSHAKE_VERSION}"
        )));
    }
    let req_hash = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let resp_hash = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let req_size = u32::from_le_bytes(header[20..24].try_into().unwrap());
    let resp_size = u32::from_le_bytes(header[24..28].try_into().unwrap());
    let topic_len = u16::from_le_bytes(header[28..30].try_into().unwrap());
    if topic_len > MAX_TOPIC_LEN {
        return Err(Error::Remote(format!("topic too long: {topic_len}")));
    }
    let mut topic_buf = vec![0u8; topic_len as usize];
    recv.read_exact(&mut topic_buf)
        .await
        .map_err(|e| Error::Remote(format!("topic name: {e}")))?;
    let topic = String::from_utf8(topic_buf)
        .map_err(|_| Error::Remote("topic name is not UTF-8".to_string()))?;
    Ok((topic, req_hash, resp_hash, req_size, resp_size))
}
