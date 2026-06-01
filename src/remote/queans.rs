//! Standalone que/ans over iroh (`RemoteTransport`).
//!
//! Mirrors [`super::reqresp`] but for the finite-answer-stream shape:
//! the client sends one que; the server replies with `0..N` ans items
//! and a done marker. Wire-compatible with the `Node` que/ans path
//! (magic `QBA1`), so a standalone server interoperates with a `Node`
//! client and vice versa. Payloads are `bytemuck::Pod`.

use std::marker::PhantomData;
use std::sync::Arc;

use bytemuck::Pod;
use iroh::endpoint::{RecvStream, SendStream};

use crate::error::{Error, Result};
use crate::queans::{ANS_KIND_DONE, ANS_KIND_ITEM};
use crate::remote::handshake::QUEANS_MAGIC;
use crate::remote::items::{
    STANDALONE_MAX_INFLIGHT, read_kind_frame, write_item_handshake, write_kind_done,
    write_kind_item,
};
use crate::remote::reqresp::read_request_handshake_tail;
use crate::remote::transport::{
    InnerShared, RemoteTransport, ensure_peer_connection, read_item_reassembled, write_frame,
};
use crate::transport::wire_type_hash;

/// Type-erased que handler: que bytes → a list of answer byte frames.
pub(crate) type ErasedQueHandler =
    Arc<dyn Fn(&[u8]) -> Result<Vec<Vec<u8>>> + Send + Sync + 'static>;

pub(crate) struct QueServerEntry {
    pub handler: ErasedQueHandler,
    pub que_type_hash: u64,
    pub ans_type_hash: u64,
    pub que_size: u32,
    pub ans_size: u32,
}

impl RemoteTransport {
    /// Register a que/ans server for this transport's topic. The
    /// handler receives one que and returns all ans items at once.
    pub fn serve_ques<Que, Ans, F>(&self, handler: F) -> Result<()>
    where
        Que: Pod + Send + 'static,
        Ans: Pod + Send + 'static,
        F: Fn(Que) -> Vec<Ans> + Send + Sync + 'static,
    {
        let que_size = std::mem::size_of::<Que>() as u32;
        let ans_size = std::mem::size_of::<Ans>() as u32;
        let que_hash = wire_type_hash::<Que>();
        let ans_hash = wire_type_hash::<Ans>();

        let handler = Arc::new(handler);
        let erased: ErasedQueHandler = Arc::new(move |bytes: &[u8]| {
            if bytes.len() != que_size as usize {
                return Err(Error::Remote(format!(
                    "que size mismatch: got {} bytes, expected {}",
                    bytes.len(),
                    que_size
                )));
            }
            // SAFETY: `Que: Pod` + length check.
            let que: Que = *bytemuck::from_bytes(bytes);
            let answers = (handler)(que);
            Ok(answers
                .iter()
                .map(|a| bytemuck::bytes_of(a).to_vec())
                .collect())
        });

        let topic = self.name().to_string();
        let mut servers =
            crate::trace::recover_poison(self.shared().queans_servers.lock(), "queans_servers");
        if servers.contains_key(&topic) {
            return Err(Error::invalid_argument(
                "a que/ans server is already registered for this topic",
            ));
        }
        servers.insert(
            topic,
            QueServerEntry {
                handler: erased,
                que_type_hash: que_hash,
                ans_type_hash: ans_hash,
                que_size,
                ans_size,
            },
        );
        Ok(())
    }

    /// Build a que/ans client. Requires `peer(...)` on the builder.
    pub fn que_client<Que: Pod + Send + 'static, Ans: Pod + Send + 'static>(
        &self,
    ) -> Result<RemoteQueClient<Que, Ans>> {
        if self.shared().peer.is_none() {
            return Err(Error::invalid_argument(
                "RemoteTransport::que_client requires a peer; set one with .peer(addr)",
            ));
        }
        Ok(RemoteQueClient {
            shared: self.shared().clone(),
            runtime: self.runtime().clone(),
            topic: self.name().to_string(),
            _phantom: PhantomData,
        })
    }
}

/// Client handle for standalone que/ans.
pub struct RemoteQueClient<Que: Pod, Ans: Pod> {
    shared: Arc<InnerShared>,
    runtime: Arc<tokio::runtime::Runtime>,
    topic: String,
    _phantom: PhantomData<fn(Que) -> Ans>,
}

impl<Que, Ans> RemoteQueClient<Que, Ans>
where
    Que: Pod + Send + 'static,
    Ans: Pod + Send + 'static,
{
    /// Send a query and collect all answers. Blocks on the transport's
    /// internal runtime.
    pub fn send(&mut self, que: Que) -> Result<Vec<Ans>> {
        let topic = self.topic.clone();
        let shared = self.shared.clone();
        let que_bytes = bytemuck::bytes_of(&que).to_vec();
        let que_hash = wire_type_hash::<Que>();
        let ans_hash = wire_type_hash::<Ans>();
        let que_size = std::mem::size_of::<Que>() as u32;
        let ans_size = std::mem::size_of::<Ans>() as u32;

        self.runtime.block_on(async move {
            let conn = ensure_peer_connection(&shared).await?;
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
            write_item_handshake(
                &mut send,
                QUEANS_MAGIC,
                &topic,
                que_hash,
                ans_hash,
                que_size,
                ans_size,
            )
            .await?;
            write_frame(&mut send, &que_bytes).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;

            let mut answers = Vec::new();
            while let Some(frame) = read_kind_frame(&mut recv, ANS_KIND_ITEM, ANS_KIND_DONE).await?
            {
                if frame.len() != ans_size as usize {
                    return Err(Error::Remote(format!(
                        "ans size mismatch: got {} bytes, expected {}",
                        frame.len(),
                        ans_size
                    )));
                }
                answers.push(*bytemuck::from_bytes(&frame));
            }
            Ok(answers)
        })
    }
}

/// Handle one incoming que/ans bi stream (magic already consumed).
pub(crate) async fn serve_queans_bi(
    inner: Arc<InnerShared>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (topic, que_hash, ans_hash, que_size, ans_size) =
        read_request_handshake_tail(&mut recv).await?;

    let entry = {
        let map = crate::trace::recover_poison(inner.queans_servers.lock(), "queans_servers");
        match map.get(&topic) {
            Some(e) => QueServerEntry {
                handler: e.handler.clone(),
                que_type_hash: e.que_type_hash,
                ans_type_hash: e.ans_type_hash,
                que_size: e.que_size,
                ans_size: e.ans_size,
            },
            None => return Ok(()),
        }
    };
    if entry.que_type_hash != que_hash
        || entry.ans_type_hash != ans_hash
        || entry.que_size != que_size
        || entry.ans_size != ans_size
    {
        return Err(Error::TypeMismatch {
            expected: "<que/ans server>",
            got: format!("que_hash=0x{que_hash:x} ans_hash=0x{ans_hash:x}"),
        });
    }

    let que_bytes = read_item_reassembled(&mut recv, STANDALONE_MAX_INFLIGHT)
        .await?
        .ok_or_else(|| Error::Remote("client closed without sending a que".to_string()))?;
    let answers = (entry.handler)(&que_bytes)?;
    for ans in answers {
        write_kind_item(&mut send, ANS_KIND_ITEM, &ans).await?;
    }
    write_kind_done(&mut send, ANS_KIND_DONE).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
    Ok(())
}
