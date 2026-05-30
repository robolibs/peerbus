//! Standalone put/ack over iroh (`RemoteTransport`).
//!
//! The client uploads `0..N` put items and a done marker; the server
//! replies with one ack. Wire-compatible with the `Node` put/ack path
//! (magic `QBP1`). Payloads are `bytemuck::Pod`.

use std::marker::PhantomData;
use std::sync::Arc;

use bytemuck::Pod;
use iroh::endpoint::{RecvStream, SendStream};

use crate::error::{Error, Result};
use crate::putack::{PUT_KIND_DONE, PUT_KIND_ITEM};
use crate::remote::handshake::PUTACK_MAGIC;
use crate::remote::items::{
    STANDALONE_MAX_INFLIGHT, read_kind_frame, write_item_handshake, write_kind_done,
    write_kind_item,
};
use crate::remote::reqresp::read_request_handshake_tail;
use crate::remote::transport::{
    InnerShared, RemoteTransport, ensure_peer_connection, read_item_reassembled, write_frame,
};
use crate::transport::wire_type_hash;

/// Type-erased upload handler: collected put byte frames → ack bytes.
pub(crate) type ErasedPutHandler =
    Arc<dyn Fn(Vec<Vec<u8>>) -> Result<Vec<u8>> + Send + Sync + 'static>;

pub(crate) struct PutServerEntry {
    pub handler: ErasedPutHandler,
    pub put_type_hash: u64,
    pub ack_type_hash: u64,
    pub put_size: u32,
    pub ack_size: u32,
}

impl RemoteTransport {
    /// Register a put/ack server for this transport's topic. The
    /// handler receives all uploaded items and returns one ack.
    pub fn serve_uploads<Put, Ack, F>(&self, handler: F) -> Result<()>
    where
        Put: Pod + Send + 'static,
        Ack: Pod + Send + 'static,
        F: Fn(Vec<Put>) -> Ack + Send + Sync + 'static,
    {
        let put_size = std::mem::size_of::<Put>() as u32;
        let ack_size = std::mem::size_of::<Ack>() as u32;
        let put_hash = wire_type_hash::<Put>();
        let ack_hash = wire_type_hash::<Ack>();

        let handler = Arc::new(handler);
        let erased: ErasedPutHandler = Arc::new(move |frames: Vec<Vec<u8>>| {
            let mut puts = Vec::with_capacity(frames.len());
            for f in &frames {
                if f.len() != put_size as usize {
                    return Err(Error::Remote(format!(
                        "put size mismatch: got {} bytes, expected {}",
                        f.len(),
                        put_size
                    )));
                }
                // SAFETY: `Put: Pod` + length check.
                puts.push(*bytemuck::from_bytes::<Put>(f));
            }
            let ack = (handler)(puts);
            Ok(bytemuck::bytes_of(&ack).to_vec())
        });

        let topic = self.name().to_string();
        let mut servers =
            crate::trace::recover_poison(self.shared().putack_servers.lock(), "putack_servers");
        if servers.contains_key(&topic) {
            return Err(Error::invalid_argument(
                "a put/ack server is already registered for this topic",
            ));
        }
        servers.insert(
            topic,
            PutServerEntry {
                handler: erased,
                put_type_hash: put_hash,
                ack_type_hash: ack_hash,
                put_size,
                ack_size,
            },
        );
        Ok(())
    }

    /// Build a put/ack client. Requires `peer(...)` on the builder.
    pub fn put_client<Put: Pod + Send + 'static, Ack: Pod + Send + 'static>(
        &self,
    ) -> Result<RemotePutClient<Put, Ack>> {
        if self.shared().peer.is_none() {
            return Err(Error::invalid_argument(
                "RemoteTransport::put_client requires a peer; set one with .peer(addr)",
            ));
        }
        Ok(RemotePutClient {
            shared: self.shared().clone(),
            runtime: self.runtime().clone(),
            topic: self.name().to_string(),
            _phantom: PhantomData,
        })
    }
}

/// Client handle for standalone put/ack.
pub struct RemotePutClient<Put: Pod, Ack: Pod> {
    shared: Arc<InnerShared>,
    runtime: Arc<tokio::runtime::Runtime>,
    topic: String,
    _phantom: PhantomData<fn(Put) -> Ack>,
}

impl<Put, Ack> RemotePutClient<Put, Ack>
where
    Put: Pod + Send + 'static,
    Ack: Pod + Send + 'static,
{
    /// Upload all `puts` and wait for the ack. Blocks on the
    /// transport's internal runtime.
    pub fn upload(&mut self, puts: &[Put]) -> Result<Ack> {
        let topic = self.topic.clone();
        let shared = self.shared.clone();
        let put_frames: Vec<Vec<u8>> = puts
            .iter()
            .map(|p| bytemuck::bytes_of(p).to_vec())
            .collect();
        let put_hash = wire_type_hash::<Put>();
        let ack_hash = wire_type_hash::<Ack>();
        let put_size = std::mem::size_of::<Put>() as u32;
        let ack_size = std::mem::size_of::<Ack>() as u32;

        self.runtime.block_on(async move {
            let conn = ensure_peer_connection(&shared).await?;
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
            write_item_handshake(
                &mut send,
                PUTACK_MAGIC,
                &topic,
                put_hash,
                ack_hash,
                put_size,
                ack_size,
            )
            .await?;
            for frame in &put_frames {
                write_kind_item(&mut send, PUT_KIND_ITEM, frame).await?;
            }
            write_kind_done(&mut send, PUT_KIND_DONE).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;

            let ack_bytes = read_item_reassembled(&mut recv, STANDALONE_MAX_INFLIGHT)
                .await?
                .ok_or_else(|| Error::Remote("server closed without writing an ack".to_string()))?;
            if ack_bytes.len() != ack_size as usize {
                return Err(Error::Remote(format!(
                    "ack size mismatch: got {} bytes, expected {}",
                    ack_bytes.len(),
                    ack_size
                )));
            }
            Ok(*bytemuck::from_bytes(&ack_bytes))
        })
    }
}

/// Handle one incoming put/ack bi stream (magic already consumed).
pub(crate) async fn serve_putack_bi(
    inner: Arc<InnerShared>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (topic, put_hash, ack_hash, put_size, ack_size) =
        read_request_handshake_tail(&mut recv).await?;

    let entry = {
        let map = crate::trace::recover_poison(inner.putack_servers.lock(), "putack_servers");
        match map.get(&topic) {
            Some(e) => PutServerEntry {
                handler: e.handler.clone(),
                put_type_hash: e.put_type_hash,
                ack_type_hash: e.ack_type_hash,
                put_size: e.put_size,
                ack_size: e.ack_size,
            },
            None => return Ok(()),
        }
    };
    if entry.put_type_hash != put_hash
        || entry.ack_type_hash != ack_hash
        || entry.put_size != put_size
        || entry.ack_size != ack_size
    {
        return Err(Error::TypeMismatch {
            expected: "<put/ack server>",
            got: format!("put_hash=0x{put_hash:x} ack_hash=0x{ack_hash:x}"),
        });
    }

    let mut puts = Vec::new();
    while let Some(frame) = read_kind_frame(&mut recv, PUT_KIND_ITEM, PUT_KIND_DONE).await? {
        puts.push(frame);
    }
    let ack = (entry.handler)(puts)?;
    write_frame(&mut send, &ack).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
    Ok(())
}
