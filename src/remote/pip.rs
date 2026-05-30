//! Standalone pip over iroh (`RemoteTransport`).
//!
//! pip is the bidirectional session primitive. This standalone
//! (escape-hatch) form is **collect-then-respond**: the client sends
//! `0..N` messages and a done marker, the server handler receives them
//! all and returns `0..N` replies. Wire-compatible with the `Node`
//! pip path (magic `QBI1`); for a truly interactive session use the
//! `Node` API. Payloads are `bytemuck::Pod`.

use std::marker::PhantomData;
use std::sync::Arc;

use bytemuck::Pod;
use iroh::endpoint::{RecvStream, SendStream};

use crate::error::{Error, Result};
use crate::pip::{PIP_KIND_DONE, PIP_KIND_ITEM};
use crate::remote::handshake::PIP_MAGIC;
use crate::remote::items::{
    read_kind_frame, write_item_handshake, write_kind_done, write_kind_item,
};
use crate::remote::reqresp::read_request_handshake_tail;
use crate::remote::transport::{InnerShared, RemoteTransport, ensure_peer_connection};
use crate::transport::wire_type_hash;

/// Type-erased pip handler: collected client byte frames → server byte
/// frames.
pub(crate) type ErasedPipHandler =
    Arc<dyn Fn(Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>> + Send + Sync + 'static>;

pub(crate) struct PipServerEntry {
    pub handler: ErasedPipHandler,
    pub client_type_hash: u64,
    pub server_type_hash: u64,
    pub client_size: u32,
    pub server_size: u32,
}

impl RemoteTransport {
    /// Register a pip server for this transport's topic
    /// (collect-then-respond; see the module docs).
    pub fn serve_sessions<ClientMsg, ServerMsg, F>(&self, handler: F) -> Result<()>
    where
        ClientMsg: Pod + Send + 'static,
        ServerMsg: Pod + Send + 'static,
        F: Fn(Vec<ClientMsg>) -> Vec<ServerMsg> + Send + Sync + 'static,
    {
        let client_size = std::mem::size_of::<ClientMsg>() as u32;
        let server_size = std::mem::size_of::<ServerMsg>() as u32;
        let client_hash = wire_type_hash::<ClientMsg>();
        let server_hash = wire_type_hash::<ServerMsg>();

        let handler = Arc::new(handler);
        let erased: ErasedPipHandler = Arc::new(move |frames: Vec<Vec<u8>>| {
            let mut msgs = Vec::with_capacity(frames.len());
            for f in &frames {
                if f.len() != client_size as usize {
                    return Err(Error::Remote(format!(
                        "pip client-message size mismatch: got {} bytes, expected {}",
                        f.len(),
                        client_size
                    )));
                }
                // SAFETY: `ClientMsg: Pod` + length check.
                msgs.push(*bytemuck::from_bytes::<ClientMsg>(f));
            }
            let replies = (handler)(msgs);
            Ok(replies
                .iter()
                .map(|m| bytemuck::bytes_of(m).to_vec())
                .collect())
        });

        let topic = self.name().to_string();
        let mut servers =
            crate::trace::recover_poison(self.shared().pip_servers.lock(), "pip_servers");
        if servers.contains_key(&topic) {
            return Err(Error::invalid_argument(
                "a pip server is already registered for this topic",
            ));
        }
        servers.insert(
            topic,
            PipServerEntry {
                handler: erased,
                client_type_hash: client_hash,
                server_type_hash: server_hash,
                client_size,
                server_size,
            },
        );
        Ok(())
    }

    /// Build a pip client. Requires `peer(...)` on the builder.
    pub fn pip_client<ClientMsg: Pod + Send + 'static, ServerMsg: Pod + Send + 'static>(
        &self,
    ) -> Result<RemotePipClient<ClientMsg, ServerMsg>> {
        if self.shared().peer.is_none() {
            return Err(Error::invalid_argument(
                "RemoteTransport::pip_client requires a peer; set one with .peer(addr)",
            ));
        }
        Ok(RemotePipClient {
            shared: self.shared().clone(),
            runtime: self.runtime().clone(),
            topic: self.name().to_string(),
            _phantom: PhantomData,
        })
    }
}

/// Client handle for standalone pip (collect-then-respond).
pub struct RemotePipClient<ClientMsg: Pod, ServerMsg: Pod> {
    shared: Arc<InnerShared>,
    runtime: Arc<tokio::runtime::Runtime>,
    topic: String,
    _phantom: PhantomData<fn(ClientMsg) -> ServerMsg>,
}

impl<ClientMsg, ServerMsg> RemotePipClient<ClientMsg, ServerMsg>
where
    ClientMsg: Pod + Send + 'static,
    ServerMsg: Pod + Send + 'static,
{
    /// Send all `msgs`, then collect the server's replies. Blocks on
    /// the transport's internal runtime.
    pub fn exchange(&mut self, msgs: &[ClientMsg]) -> Result<Vec<ServerMsg>> {
        let topic = self.topic.clone();
        let shared = self.shared.clone();
        let frames: Vec<Vec<u8>> = msgs
            .iter()
            .map(|m| bytemuck::bytes_of(m).to_vec())
            .collect();
        let client_hash = wire_type_hash::<ClientMsg>();
        let server_hash = wire_type_hash::<ServerMsg>();
        let client_size = std::mem::size_of::<ClientMsg>() as u32;
        let server_size = std::mem::size_of::<ServerMsg>() as u32;

        self.runtime.block_on(async move {
            let conn = ensure_peer_connection(&shared).await?;
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
            write_item_handshake(
                &mut send,
                PIP_MAGIC,
                &topic,
                client_hash,
                server_hash,
                client_size,
                server_size,
            )
            .await?;
            for frame in &frames {
                write_kind_item(&mut send, PIP_KIND_ITEM, frame).await?;
            }
            write_kind_done(&mut send, PIP_KIND_DONE).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;

            let mut replies = Vec::new();
            while let Some(frame) = read_kind_frame(&mut recv, PIP_KIND_ITEM, PIP_KIND_DONE).await?
            {
                if frame.len() != server_size as usize {
                    return Err(Error::Remote(format!(
                        "pip server-message size mismatch: got {} bytes, expected {}",
                        frame.len(),
                        server_size
                    )));
                }
                replies.push(*bytemuck::from_bytes(&frame));
            }
            Ok(replies)
        })
    }
}

/// Handle one incoming pip bi stream (magic already consumed).
pub(crate) async fn serve_pip_bi(
    inner: Arc<InnerShared>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (topic, client_hash, server_hash, client_size, server_size) =
        read_request_handshake_tail(&mut recv).await?;

    let entry = {
        let map = crate::trace::recover_poison(inner.pip_servers.lock(), "pip_servers");
        match map.get(&topic) {
            Some(e) => PipServerEntry {
                handler: e.handler.clone(),
                client_type_hash: e.client_type_hash,
                server_type_hash: e.server_type_hash,
                client_size: e.client_size,
                server_size: e.server_size,
            },
            None => return Ok(()),
        }
    };
    if entry.client_type_hash != client_hash
        || entry.server_type_hash != server_hash
        || entry.client_size != client_size
        || entry.server_size != server_size
    {
        return Err(Error::TypeMismatch {
            expected: "<pip server>",
            got: format!("client_hash=0x{client_hash:x} server_hash=0x{server_hash:x}"),
        });
    }

    let mut msgs = Vec::new();
    while let Some(frame) = read_kind_frame(&mut recv, PIP_KIND_ITEM, PIP_KIND_DONE).await? {
        msgs.push(frame);
    }
    let replies = (entry.handler)(msgs)?;
    for reply in replies {
        write_kind_item(&mut send, PIP_KIND_ITEM, &reply).await?;
    }
    write_kind_done(&mut send, PIP_KIND_DONE).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
    Ok(())
}
