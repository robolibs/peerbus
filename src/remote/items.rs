//! Shared wire helpers for the standalone (`RemoteTransport`) item
//! modes: que/ans, put/ack, pip.
//!
//! These produce/consume the exact same bytes as the `Node` item
//! codecs, so a standalone server interoperates with a `Node` client
//! and vice versa. The handshake is the shared typed-topic tail
//! (`super::reqresp::read_request_handshake_tail`, which accepts both
//! v2 and v3); the standalone side emits v2 (single-frame items) and
//! reassembles chunk frames on read, matching the standalone req/res
//! path. Streaming directions are framed with a one-byte kind marker
//! (`*_KIND_ITEM` / `*_KIND_DONE`) per item, same as `Node`.

use iroh::endpoint::{RecvStream, SendStream};

use crate::error::{Error, Result};
use crate::remote::handshake::{HANDSHAKE_VERSION, MAX_TOPIC_LEN};
use crate::remote::transport::{read_item_reassembled, write_frame};

/// Generous reassembly bound for the standalone (escape-hatch) path —
/// keeps it from rejecting large chunked messages from a `Node` peer.
pub(crate) const STANDALONE_MAX_INFLIGHT: usize = 1 << 30; // 1 GiB

/// Write a typed-topic item handshake (`[magic][v2][hash_a][hash_b]
/// [size_a][size_b][topic_len][topic]`). Emits v2; a `Node` peer's
/// parser accepts it (no chunking advertised this direction).
pub(crate) async fn write_item_handshake(
    send: &mut SendStream,
    magic: u32,
    topic: &str,
    hash_a: u64,
    hash_b: u64,
    size_a: u32,
    size_b: u32,
) -> Result<()> {
    if topic.len() > MAX_TOPIC_LEN as usize {
        return Err(Error::TopicNameTooLong {
            len: topic.len(),
            limit: MAX_TOPIC_LEN as usize,
        });
    }
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 8 + 4 + 4 + 2 + topic.len());
    buf.extend_from_slice(&magic.to_le_bytes());
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&hash_a.to_le_bytes());
    buf.extend_from_slice(&hash_b.to_le_bytes());
    buf.extend_from_slice(&size_a.to_le_bytes());
    buf.extend_from_slice(&size_b.to_le_bytes());
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("item handshake write: {e}")))
}

/// Write one kind-marked streaming item: `[kind][u32 len][bytes]`.
pub(crate) async fn write_kind_item(send: &mut SendStream, kind: u8, payload: &[u8]) -> Result<()> {
    send.write_all(&[kind])
        .await
        .map_err(|e| Error::Remote(format!("item kind write: {e}")))?;
    write_frame(send, payload).await
}

/// Write the done marker for a streaming direction.
pub(crate) async fn write_kind_done(send: &mut SendStream, done_kind: u8) -> Result<()> {
    send.write_all(&[done_kind])
        .await
        .map_err(|e| Error::Remote(format!("item done write: {e}")))
}

/// Read one kind-marked item. Returns `Ok(None)` on the done marker or
/// a clean stream end. Reassembles chunk frames (a `Node` peer may
/// chunk a large item).
pub(crate) async fn read_kind_frame(
    recv: &mut RecvStream,
    item_kind: u8,
    done_kind: u8,
) -> Result<Option<Vec<u8>>> {
    let mut kind = [0u8; 1];
    if recv.read_exact(&mut kind).await.is_err() {
        return Ok(None);
    }
    if kind[0] == done_kind {
        return Ok(None);
    }
    if kind[0] != item_kind {
        return Err(Error::HandshakeMalformed(format!(
            "unexpected item kind {} (want {item_kind} or {done_kind})",
            kind[0]
        )));
    }
    read_item_reassembled(recv, STANDALONE_MAX_INFLIGHT)
        .await?
        .ok_or_else(|| Error::Remote("item kind marker without a frame".to_string()))
        .map(Some)
}
