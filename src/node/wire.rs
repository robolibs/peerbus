use super::*;

pub(crate) async fn pump_recv_stream(
    mut recv: iroh::endpoint::RecvStream,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    qos: TopicQos,
    incomplete_dropped: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
) -> Result<()> {
    let mut reassembler = Reassembler::new(qos.delivery, qos.max_inflight_bytes);
    let mut observed_incomplete_dropped = 0;
    loop {
        let mut len_buf = [0u8; 4];
        if recv.read_exact(&mut len_buf).await.is_err() {
            return Ok(());
        }
        let raw_len = u32::from_le_bytes(len_buf);
        let chunked = (raw_len & CHUNK_FRAME_FLAG) != 0;
        let len = (raw_len & CHUNK_FRAME_LEN_MASK) as usize;
        let limit = if chunked {
            CHUNK_FRAME_LEN_MASK as usize
        } else {
            qos.max_message_bytes.min(MAX_PAYLOAD_LEN as usize)
        };
        if len > limit {
            return Err(Error::FrameTooLarge {
                actual: len as u64,
                limit: limit as u64,
            });
        }
        let mut buf = vec![0u8; len];
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| Error::Remote(format!("frame body: {e}")))?;
        if chunked {
            let frame = parse_chunk_payload(&buf)?;
            let message = reassembler.push(frame)?;
            let dropped = reassembler.incomplete_dropped();
            if dropped > observed_incomplete_dropped {
                incomplete_dropped
                    .fetch_add(dropped - observed_incomplete_dropped, Ordering::Relaxed);
                observed_incomplete_dropped = dropped;
            }
            if let Some(message) = message {
                bytes_received.fetch_add(message.len() as u64, Ordering::Relaxed);
                if tx.send(message).await.is_err() {
                    return Ok(());
                }
            }
        } else {
            bytes_received.fetch_add(buf.len() as u64, Ordering::Relaxed);
            if tx.send(buf).await.is_err() {
                return Ok(());
            }
        }
    }
}

// ---- chunk-aware item codec (req/res, que/ans, put/ack, pip) ----
//
// The item modes carry one logical message as either a single legacy
// frame (`[u32 len][bytes]`, capped at `qos.max_message_bytes`) or, when
// the payload exceeds `chunk_bytes` and the peer negotiated chunking
// (handshake v3), a contiguous run of chunk frames using the same
// `CHUNK_FRAME_FLAG`/`make_chunk_payload` scheme as pub/sub. This lifts
// the 64 MiB single-frame cap; total size is bounded by
// `qos.max_inflight_bytes`. Reliable/ordered only — these modes do not
// drop stale items, so `DeliveryPolicy::Latest`/`BestEffort` are N/A.

/// Write one logical item. Chunks when the peer negotiated chunking and
/// the payload exceeds `chunk_bytes`; otherwise a single legacy frame.
pub(crate) async fn write_item_chunked(
    send: &mut iroh::endpoint::SendStream,
    payload: &[u8],
    chunk_bytes: usize,
    peer_chunks: bool,
) -> Result<()> {
    if !peer_chunks || payload.len() <= chunk_bytes {
        return write_frame(send, payload).await;
    }
    let chunk_bytes = chunk_bytes.max(1);
    let chunk_count = payload.len().div_ceil(chunk_bytes);
    if chunk_count > u32::MAX as usize {
        return Err(Error::FrameTooLarge {
            actual: payload.len() as u64,
            limit: (chunk_bytes as u64) * (u32::MAX as u64),
        });
    }
    let message_id = NEXT_REMOTE_REQ_ID.fetch_add(1, Ordering::AcqRel) + 1;
    for (idx, chunk) in payload.chunks(chunk_bytes).enumerate() {
        let frame = make_chunk_payload(
            message_id,
            idx as u32,
            chunk_count as u32,
            payload.len(),
            chunk,
        )?;
        if frame.len() > CHUNK_FRAME_LEN_MASK as usize {
            return Err(Error::FrameTooLarge {
                actual: frame.len() as u64,
                limit: CHUNK_FRAME_LEN_MASK as u64,
            });
        }
        let len = CHUNK_FRAME_FLAG | (frame.len() as u32);
        send.write_all(&len.to_le_bytes())
            .await
            .map_err(|e| Error::Remote(format!("write chunk len: {e}")))?;
        send.write_all(&frame)
            .await
            .map_err(|e| Error::Remote(format!("write chunk body: {e}")))?;
    }
    Ok(())
}

/// Read one logical item, reassembling chunk frames if present. Returns
/// `Ok(None)` on a clean stream end before any frame. `max_message_bytes`
/// caps a single legacy frame; `max_inflight_bytes` bounds reassembly.
pub(crate) async fn read_item_chunked(
    recv: &mut iroh::endpoint::RecvStream,
    max_message_bytes: usize,
    max_inflight_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let mut reassembler: Option<Reassembler> = None;
    loop {
        let mut len_buf = [0u8; 4];
        if reassembler.is_none() {
            // First frame of the item: a clean EOF here means "no item".
            if recv.read_exact(&mut len_buf).await.is_err() {
                return Ok(None);
            }
        } else {
            recv.read_exact(&mut len_buf)
                .await
                .map_err(|e| Error::Remote(format!("chunk frame header: {e}")))?;
        }
        let raw_len = u32::from_le_bytes(len_buf);
        let chunked = (raw_len & CHUNK_FRAME_FLAG) != 0;
        let len = (raw_len & CHUNK_FRAME_LEN_MASK) as usize;
        let limit = if chunked {
            CHUNK_FRAME_LEN_MASK as usize
        } else {
            max_message_bytes.min(MAX_PAYLOAD_LEN as usize)
        };
        if len > limit {
            return Err(Error::FrameTooLarge {
                actual: len as u64,
                limit: limit as u64,
            });
        }
        let mut buf = vec![0u8; len];
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| Error::Remote(format!("frame body: {e}")))?;
        if !chunked {
            return Ok(Some(buf));
        }
        let frame = parse_chunk_payload(&buf)?;
        let r = reassembler
            .get_or_insert_with(|| Reassembler::new(DeliveryPolicy::Reliable, max_inflight_bytes));
        if let Some(message) = r.push(frame)? {
            return Ok(Some(message));
        }
    }
}

// ---- iroh wire helpers ----

pub(crate) async fn write_topic_handshake(
    send: &mut iroh::endpoint::SendStream,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
    qos: TopicQos,
) -> Result<()> {
    const NODE_TOPIC_LEN_LIMIT: usize = 1024;
    let bytes = topic.as_bytes();
    if bytes.len() > NODE_TOPIC_LEN_LIMIT {
        return Err(Error::TopicNameTooLong {
            len: bytes.len(),
            limit: NODE_TOPIC_LEN_LIMIT,
        });
    }
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 4 + 8 + 8 + 4 + 1 + 1 + 2 + bytes.len());
    buf.extend_from_slice(&HANDSHAKE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&PUBSUB_HANDSHAKE_VERSION_QOS.to_le_bytes());
    buf.extend_from_slice(&type_hash.to_le_bytes());
    buf.extend_from_slice(&payload_size.to_le_bytes());
    buf.extend_from_slice(&(qos.max_message_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&(qos.max_inflight_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&(qos.chunk_bytes as u32).to_le_bytes());
    buf.push(qos.delivery.to_wire());
    buf.push(qos.priority);
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("topic handshake write: {e}")))?;
    Ok(())
}

pub(crate) async fn read_topic_handshake_tail(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<crate::remote::PubSubHandshake> {
    const NODE_TOPIC_LEN_LIMIT: usize = 1024;
    let mut fixed_tail = [0u8; 4 + 8 + 4 + 2];
    recv.read_exact(&mut fixed_tail)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("handshake tail: {e}")))?;
    let version = u32::from_le_bytes(fixed_tail[0..4].try_into().unwrap());
    if version == PUBSUB_HANDSHAKE_VERSION_QOS {
        const V2_FIXED_LEN: usize = 4 + 8 + 4 + 2;
        const TOPIC_LEN_OFFSET: usize = 4 + 8 + 4 + 8 + 8 + 4 + 1 + 1;
        const V3_FIXED_LEN: usize = TOPIC_LEN_OFFSET + 2;
        let mut qos_tail = [0u8; V3_FIXED_LEN - V2_FIXED_LEN];
        recv.read_exact(&mut qos_tail)
            .await
            .map_err(|e| Error::HandshakeMalformed(format!("qos handshake tail: {e}")))?;
        let mut extended = [0u8; V3_FIXED_LEN];
        extended[..V2_FIXED_LEN].copy_from_slice(&fixed_tail);
        extended[V2_FIXED_LEN..].copy_from_slice(&qos_tail);
        let n = u16::from_le_bytes(
            extended[TOPIC_LEN_OFFSET..TOPIC_LEN_OFFSET + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        if n > NODE_TOPIC_LEN_LIMIT {
            return Err(Error::TopicNameTooLong {
                len: n,
                limit: NODE_TOPIC_LEN_LIMIT,
            });
        }
        let mut topic = vec![0u8; n];
        recv.read_exact(&mut topic)
            .await
            .map_err(|e| Error::HandshakeMalformed(format!("handshake topic: {e}")))?;

        let mut tail = Vec::with_capacity(extended.len() + topic.len());
        tail.extend_from_slice(&extended);
        tail.extend_from_slice(&topic);
        return parse_pubsub_handshake_tail_qos(&tail);
    }

    let n = u16::from_le_bytes(fixed_tail[16..18].try_into().unwrap()) as usize;
    if n > NODE_TOPIC_LEN_LIMIT {
        return Err(Error::TopicNameTooLong {
            len: n,
            limit: NODE_TOPIC_LEN_LIMIT,
        });
    }
    let mut topic = vec![0u8; n];
    recv.read_exact(&mut topic)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("handshake topic: {e}")))?;

    let mut tail = Vec::with_capacity(fixed_tail.len() + topic.len());
    tail.extend_from_slice(&fixed_tail);
    tail.extend_from_slice(&topic);
    parse_pubsub_handshake_tail_qos(&tail)
}

// ---- shared item-stream handshake (req/res, que/ans, put/ack, pip) ----
//
// All four item modes share an identical post-magic tail. Version 3
// appends data-agnostic byte limits before the topic name; version 2
// (legacy) is still parsed. The four magics keep the streams
// distinguishable on the accept side.

pub(crate) const ITEM_HS_V2_FIXED: usize = 4 + 8 + 8 + 4 + 4 + 2; // 30; topic_len at [28..30]
pub(crate) const ITEM_HS_QOS_EXTRA: usize = 8 + 8 + 4; // 20 (max_message, max_inflight, chunk_bytes)
pub(crate) const ITEM_HS_V3_FIXED: usize = ITEM_HS_V2_FIXED - 2 + ITEM_HS_QOS_EXTRA + 2; // 50; topic_len at [48..50]
pub(crate) const ITEM_TOPIC_LEN_LIMIT: usize = 1024;

/// Byte limits negotiated (or defaulted) for an item stream, plus
/// whether the peer advertised chunk support (handshake v3).
///
/// Several fields are only read inside `tracing` diagnostics, so they
/// read as dead code when the `tracing` feature is disabled.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(feature = "tracing"), allow(dead_code))]
pub(crate) struct ItemHsQos {
    pub(crate) peer_chunks: bool,
    pub(crate) max_message_bytes: usize,
    pub(crate) max_inflight_bytes: usize,
    pub(crate) chunk_bytes: usize,
}

impl ItemHsQos {
    pub(crate) fn legacy() -> Self {
        let q = TopicQos::default();
        Self {
            peer_chunks: false,
            max_message_bytes: q.max_message_bytes,
            max_inflight_bytes: q.max_inflight_bytes,
            chunk_bytes: q.chunk_bytes,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_item_handshake(
    send: &mut iroh::endpoint::SendStream,
    magic: u32,
    topic: &str,
    hash_a: u64,
    hash_b: u64,
    size_a: u32,
    size_b: u32,
    qos: TopicQos,
) -> Result<()> {
    let bytes = topic.as_bytes();
    if bytes.len() > ITEM_TOPIC_LEN_LIMIT {
        return Err(Error::TopicNameTooLong {
            len: bytes.len(),
            limit: ITEM_TOPIC_LEN_LIMIT,
        });
    }
    let mut buf = Vec::with_capacity(ITEM_HS_V3_FIXED + bytes.len());
    buf.extend_from_slice(&magic.to_le_bytes());
    buf.extend_from_slice(&ITEM_HANDSHAKE_VERSION_CHUNKED.to_le_bytes());
    buf.extend_from_slice(&hash_a.to_le_bytes());
    buf.extend_from_slice(&hash_b.to_le_bytes());
    buf.extend_from_slice(&size_a.to_le_bytes());
    buf.extend_from_slice(&size_b.to_le_bytes());
    buf.extend_from_slice(&(qos.max_message_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&(qos.max_inflight_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&(qos.chunk_bytes as u32).to_le_bytes());
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("item handshake write: {e}")))?;
    Ok(())
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn read_item_handshake_tail(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(String, u64, u64, u32, u32, ItemHsQos)> {
    let mut v2_fixed = [0u8; ITEM_HS_V2_FIXED];
    recv.read_exact(&mut v2_fixed)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("item handshake tail: {e}")))?;
    let version = u32::from_le_bytes(v2_fixed[0..4].try_into().unwrap());
    let (mut fixed, topic_len) = if version == ITEM_HANDSHAKE_VERSION_CHUNKED {
        let mut extra = [0u8; ITEM_HS_V3_FIXED - ITEM_HS_V2_FIXED];
        recv.read_exact(&mut extra)
            .await
            .map_err(|e| Error::HandshakeMalformed(format!("item qos tail: {e}")))?;
        let mut fixed = Vec::with_capacity(ITEM_HS_V3_FIXED);
        fixed.extend_from_slice(&v2_fixed);
        fixed.extend_from_slice(&extra);
        let n = u16::from_le_bytes(fixed[48..50].try_into().unwrap()) as usize;
        (fixed, n)
    } else {
        let n = u16::from_le_bytes(v2_fixed[28..30].try_into().unwrap()) as usize;
        (v2_fixed.to_vec(), n)
    };
    if topic_len > ITEM_TOPIC_LEN_LIMIT {
        return Err(Error::TopicNameTooLong {
            len: topic_len,
            limit: ITEM_TOPIC_LEN_LIMIT,
        });
    }
    let mut topic = vec![0u8; topic_len];
    recv.read_exact(&mut topic)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("item handshake topic: {e}")))?;
    fixed.extend_from_slice(&topic);
    let parsed = parse_item_handshake_tail(&fixed)?;
    let hs = parsed.5;
    qb_debug!(
        target: "peerbus::node",
        peer_chunks = hs.peer_chunks,
        max_message_bytes = hs.max_message_bytes,
        max_inflight_bytes = hs.max_inflight_bytes,
        chunk_bytes = hs.chunk_bytes,
        "item handshake negotiated"
    );
    Ok(parsed)
}

/// Parse the post-magic item handshake tail (req/res, que/ans, put/ack,
/// pip). Accepts version 2 (legacy, no chunking) and version 3 (byte
/// limits + chunking).
pub(crate) fn parse_item_handshake_tail(
    bytes: &[u8],
) -> Result<(String, u64, u64, u32, u32, ItemHsQos)> {
    if bytes.len() < ITEM_HS_V2_FIXED {
        return Err(Error::HandshakeMalformed(format!(
            "item handshake tail truncated: {} bytes",
            bytes.len()
        )));
    }
    let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let hash_a = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let hash_b = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let size_a = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let size_b = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
    let (qos, topic_len_off) = if version == HANDSHAKE_VERSION {
        (ItemHsQos::legacy(), 28usize)
    } else if version == ITEM_HANDSHAKE_VERSION_CHUNKED {
        if bytes.len() < ITEM_HS_V3_FIXED {
            return Err(Error::HandshakeMalformed(format!(
                "item handshake v3 tail truncated: {} bytes",
                bytes.len()
            )));
        }
        let max_message_bytes = u64::from_le_bytes(bytes[28..36].try_into().unwrap()) as usize;
        let max_inflight_bytes = u64::from_le_bytes(bytes[36..44].try_into().unwrap()) as usize;
        let chunk_bytes = u32::from_le_bytes(bytes[44..48].try_into().unwrap()) as usize;
        (
            ItemHsQos {
                peer_chunks: true,
                max_message_bytes,
                max_inflight_bytes,
                chunk_bytes,
            },
            48usize,
        )
    } else {
        return Err(Error::HandshakeVersionMismatch {
            local: ITEM_HANDSHAKE_VERSION_CHUNKED,
            peer: version,
        });
    };
    let topic_len =
        u16::from_le_bytes(bytes[topic_len_off..topic_len_off + 2].try_into().unwrap()) as usize;
    if topic_len > ITEM_TOPIC_LEN_LIMIT {
        return Err(Error::TopicNameTooLong {
            len: topic_len,
            limit: ITEM_TOPIC_LEN_LIMIT,
        });
    }
    let topic_start = topic_len_off + 2;
    let topic_end = topic_start.saturating_add(topic_len);
    if bytes.len() < topic_end {
        return Err(Error::HandshakeMalformed(format!(
            "item topic name truncated: declared {} bytes, have {}",
            topic_len,
            bytes.len().saturating_sub(topic_start)
        )));
    }
    let topic = std::str::from_utf8(&bytes[topic_start..topic_end])
        .map_err(|_| Error::HandshakeMalformed("item topic name is not UTF-8".to_string()))?
        .to_string();
    Ok((topic, hash_a, hash_b, size_a, size_b, qos))
}

pub(crate) async fn write_answer_item(
    send: &mut iroh::endpoint::SendStream,
    frame: &[u8],
    chunk_bytes: usize,
    peer_chunks: bool,
) -> Result<()> {
    send.write_all(&[ANS_KIND_ITEM])
        .await
        .map_err(|e| Error::Remote(format!("answer kind write: {e}")))?;
    write_item_chunked(send, frame, chunk_bytes, peer_chunks).await
}

pub(crate) async fn write_answer_done(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[ANS_KIND_DONE])
        .await
        .map_err(|e| Error::Remote(format!("answer done write: {e}")))?;
    Ok(())
}

pub(crate) async fn read_answer_frame(
    recv: &mut iroh::endpoint::RecvStream,
    max_message_bytes: usize,
    max_inflight_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let mut kind = [0u8; 1];
    if recv.read_exact(&mut kind).await.is_err() {
        return Ok(None);
    }
    match kind[0] {
        ANS_KIND_ITEM => read_item_chunked(recv, max_message_bytes, max_inflight_bytes)
            .await?
            .ok_or_else(|| Error::Remote("answer item missing frame".to_string()))
            .map(Some),
        ANS_KIND_DONE => Ok(None),
        kind => Err(Error::HandshakeMalformed(format!(
            "unknown que/ans answer kind {kind}"
        ))),
    }
}

pub(crate) async fn write_put_item(
    send: &mut iroh::endpoint::SendStream,
    frame: &[u8],
    chunk_bytes: usize,
    peer_chunks: bool,
) -> Result<()> {
    send.write_all(&[PUT_KIND_ITEM])
        .await
        .map_err(|e| Error::Remote(format!("put kind write: {e}")))?;
    write_item_chunked(send, frame, chunk_bytes, peer_chunks).await
}

pub(crate) async fn write_put_done(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[PUT_KIND_DONE])
        .await
        .map_err(|e| Error::Remote(format!("put done write: {e}")))?;
    Ok(())
}

pub(crate) async fn read_put_frame(
    recv: &mut iroh::endpoint::RecvStream,
    max_message_bytes: usize,
    max_inflight_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let mut kind = [0u8; 1];
    if recv.read_exact(&mut kind).await.is_err() {
        return Ok(None);
    }
    match kind[0] {
        PUT_KIND_ITEM => read_item_chunked(recv, max_message_bytes, max_inflight_bytes)
            .await?
            .ok_or_else(|| Error::Remote("put item missing frame".to_string()))
            .map(Some),
        PUT_KIND_DONE => Ok(None),
        kind => Err(Error::HandshakeMalformed(format!(
            "unknown put/ack item kind {kind}"
        ))),
    }
}

pub(crate) async fn write_pip_item(
    send: &mut iroh::endpoint::SendStream,
    frame: &[u8],
    chunk_bytes: usize,
    peer_chunks: bool,
) -> Result<()> {
    send.write_all(&[PIP_KIND_ITEM])
        .await
        .map_err(|e| Error::Remote(format!("pip kind write: {e}")))?;
    write_item_chunked(send, frame, chunk_bytes, peer_chunks).await
}

pub(crate) async fn write_pip_done(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[PIP_KIND_DONE])
        .await
        .map_err(|e| Error::Remote(format!("pip done write: {e}")))?;
    Ok(())
}

pub(crate) async fn read_pip_frame(
    recv: &mut iroh::endpoint::RecvStream,
    max_message_bytes: usize,
    max_inflight_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let mut kind = [0u8; 1];
    if recv.read_exact(&mut kind).await.is_err() {
        return Ok(None);
    }
    match kind[0] {
        PIP_KIND_ITEM => read_item_chunked(recv, max_message_bytes, max_inflight_bytes)
            .await?
            .ok_or_else(|| Error::Remote("pip item missing frame".to_string()))
            .map(Some),
        PIP_KIND_DONE => Ok(None),
        kind => Err(Error::HandshakeMalformed(format!(
            "unknown pip item kind {kind}"
        ))),
    }
}

pub(crate) async fn write_pubsub_message(
    send: &mut iroh::endpoint::SendStream,
    payload: &[u8],
    qos: TopicQos,
    subscriber_version: u32,
) -> Result<()> {
    write_item_chunked(
        send,
        payload,
        qos.chunk_bytes,
        subscriber_version == PUBSUB_HANDSHAKE_VERSION_QOS,
    )
    .await
}

pub(crate) async fn write_frame(send: &mut iroh::endpoint::SendStream, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_PAYLOAD_LEN as usize {
        return Err(Error::PayloadTooLarge {
            actual: payload.len(),
            capacity: MAX_PAYLOAD_LEN as usize,
        });
    }
    let len = payload.len() as u32;
    send.write_all(&len.to_le_bytes())
        .await
        .map_err(|e| Error::Remote(format!("write_frame len: {e}")))?;
    send.write_all(payload)
        .await
        .map_err(|e| Error::Remote(format!("write_frame body: {e}")))?;
    Ok(())
}

// ---- connection cache ----

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn ensure_peer_connection(
    inner: &Arc<NodeInner>,
    peer: EndpointId,
    addr_hint: Option<EndpointAddr>,
) -> Result<Connection> {
    let slot = {
        let mut map =
            crate::trace::recover_poison(inner.peer_connections.lock(), "Node::peer_connections");
        map.entry(*peer.as_bytes())
            .or_insert_with(|| Arc::new(AsyncMutex::new(PeerSlot::default())))
            .clone()
    };

    let mut guard = slot.lock().await;

    // Refresh the address hint if the caller supplied one. A later
    // subscribe with a richer address (e.g. direct IPs) overrides
    // an earlier id-only hint.
    if let Some(a) = addr_hint {
        guard.addr_hint = Some(a);
    }

    if let Some(conn) = guard.conn.as_ref() {
        // `close_reason()` is `None` for a live connection. Any
        // `Some(_)` value means iroh has observed an end-of-life
        // event (peer closed, idle timeout, transport error) — the
        // cached handle is unusable and must be re-dialed.
        if conn.close_reason().is_none() {
            return Ok(conn.clone());
        }
        qb_warn!(
            target: "peerbus::node",
            peer = %peer,
            reason = ?conn.close_reason(),
            "cached connection is dead, re-dialing"
        );
        guard.conn = None;
    }

    let addr = guard
        .addr_hint
        .clone()
        .unwrap_or_else(|| EndpointAddr::new(peer));
    qb_info!(
        target: "peerbus::node",
        peer = %peer,
        "dialing peer"
    );
    let conn = inner
        .endpoint
        .connect(addr, &inner.alpn)
        .await
        .map_err(|e| {
            qb_warn!(
                target: "peerbus::node",
                peer = %peer,
                error = %e,
                "dial failed"
            );
            Error::ConnectFailed(format!("{e}"))
        })?;
    qb_info!(
        target: "peerbus::node",
        peer = %peer,
        "connected to peer"
    );
    guard.conn = Some(conn.clone());
    Ok(conn)
}

// ---- identity resolution ----
