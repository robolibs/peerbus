use super::*;

// --- wire helpers ---

pub(crate) async fn write_handshake(
    send: &mut SendStream,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
) -> Result<()> {
    if topic.len() > MAX_TOPIC_LEN as usize {
        return Err(Error::TopicNameTooLong {
            len: topic.len(),
            limit: MAX_TOPIC_LEN as usize,
        });
    }
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 4 + 2 + topic.len());
    buf.extend_from_slice(&HANDSHAKE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&type_hash.to_le_bytes());
    buf.extend_from_slice(&payload_size.to_le_bytes());
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("write handshake: {e}")))
}

/// Read the pub/sub handshake tail (everything after the magic).
/// The magic itself has already been consumed by [`serve_bi`].
pub(crate) async fn read_pubsub_handshake_tail(recv: &mut RecvStream) -> Result<(String, u64, u32)> {
    let mut header = [0u8; 4 + 8 + 4 + 2];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("handshake tail: {e}")))?;
    let version = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let topic_len_offset = match version {
        HANDSHAKE_VERSION => 16,
        PUBSUB_HANDSHAKE_VERSION_QOS => {
            let mut qos_tail = [0u8; PUBSUB_V3_FIXED_LEN - PUBSUB_V2_FIXED_LEN];
            recv.read_exact(&mut qos_tail)
                .await
                .map_err(|e| Error::HandshakeMalformed(format!("qos handshake tail: {e}")))?;
            let mut extended = [0u8; PUBSUB_V3_FIXED_LEN];
            extended[..PUBSUB_V2_FIXED_LEN].copy_from_slice(&header);
            extended[PUBSUB_V2_FIXED_LEN..].copy_from_slice(&qos_tail);
            let topic_len = u16::from_le_bytes(
                extended[PUBSUB_V3_TOPIC_LEN_OFFSET..PUBSUB_V3_TOPIC_LEN_OFFSET + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let mut topic_buf = vec![0u8; topic_len];
            recv.read_exact(&mut topic_buf)
                .await
                .map_err(|e| Error::Remote(format!("topic name: {e}")))?;
            let mut buf = Vec::with_capacity(extended.len() + topic_buf.len());
            buf.extend_from_slice(&extended);
            buf.extend_from_slice(&topic_buf);
            let h = parse_pubsub_handshake_tail_qos(&buf)?;
            return Ok((h.topic, h.type_hash, h.payload_size));
        }
        _ => 16,
    };
    let topic_len = u16::from_le_bytes(
        header[topic_len_offset..topic_len_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let mut topic_buf = vec![0u8; topic_len];
    recv.read_exact(&mut topic_buf)
        .await
        .map_err(|e| Error::Remote(format!("topic name: {e}")))?;

    // Concatenate header + topic and feed to the pure parser so
    // wire / fuzz tests exercise the exact same logic.
    let mut buf = Vec::with_capacity(header.len() + topic_buf.len());
    buf.extend_from_slice(&header);
    buf.extend_from_slice(&topic_buf);
    let h = parse_pubsub_handshake_tail_qos(&buf)?;
    Ok((h.topic, h.type_hash, h.payload_size))
}

const PUBSUB_V2_FIXED_LEN: usize = 4 + 8 + 4 + 2;
const PUBSUB_V3_TOPIC_LEN_OFFSET: usize = 4 + 8 + 4 + 8 + 8 + 4 + 1 + 1;
const PUBSUB_V3_FIXED_LEN: usize = PUBSUB_V3_TOPIC_LEN_OFFSET + 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubHandshake {
    pub topic: String,
    pub type_hash: u64,
    pub payload_size: u32,
    pub qos: TopicQos,
    pub version: u32,
}

/// Pure-byte parser for the pub/sub handshake tail (the part after
/// the 4-byte `HANDSHAKE_MAGIC`). Public so fuzz targets and tests
/// can hammer it without driving an actual `RecvStream`.
pub fn parse_pubsub_handshake_tail(bytes: &[u8]) -> Result<(String, u64, u32)> {
    let h = parse_pubsub_handshake_tail_qos(bytes)?;
    Ok((h.topic, h.type_hash, h.payload_size))
}

/// Parse a pub/sub handshake and preserve v3 QoS fields.
///
/// v2 layout:
/// `[u32 version][u64 type_hash][u32 payload_size][u16 topic_len][topic]`.
///
/// v3 layout:
/// `[u32 version][u64 type_hash][u32 payload_size][u64 max_message_bytes]
/// [u64 max_inflight_bytes][u32 chunk_bytes][u8 delivery_policy][u8 priority]
/// [u16 topic_len][topic]`.
pub fn parse_pubsub_handshake_tail_qos(bytes: &[u8]) -> Result<PubSubHandshake> {
    if bytes.len() < 4 + 8 + 4 + 2 {
        return Err(Error::HandshakeMalformed(format!(
            "handshake tail truncated: {} bytes",
            bytes.len()
        )));
    }
    let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if version != HANDSHAKE_VERSION && version != PUBSUB_HANDSHAKE_VERSION_QOS {
        return Err(Error::HandshakeVersionMismatch {
            local: PUBSUB_HANDSHAKE_VERSION_QOS,
            peer: version,
        });
    }
    let type_hash = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let payload_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());

    let (qos, topic_len_offset, fixed_len) = if version == PUBSUB_HANDSHAKE_VERSION_QOS {
        if bytes.len() < PUBSUB_V3_FIXED_LEN {
            return Err(Error::HandshakeMalformed(format!(
                "v3 handshake tail truncated: {} bytes",
                bytes.len()
            )));
        }
        let max_message_bytes = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let max_inflight_bytes = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;
        let chunk_bytes = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
        let delivery = DeliveryPolicy::from_wire(bytes[36]).ok_or_else(|| {
            Error::HandshakeMalformed(format!("invalid delivery policy {}", bytes[36]))
        })?;
        let priority = bytes[37];
        (
            TopicQos {
                delivery,
                max_message_bytes,
                max_inflight_bytes,
                chunk_bytes,
                subscriber_queue: TopicQos::default().subscriber_queue,
                priority,
            },
            PUBSUB_V3_TOPIC_LEN_OFFSET,
            PUBSUB_V3_FIXED_LEN,
        )
    } else {
        (TopicQos::default(), 16, PUBSUB_V2_FIXED_LEN)
    };

    let topic_len = u16::from_le_bytes(
        bytes[topic_len_offset..topic_len_offset + 2]
            .try_into()
            .unwrap(),
    );
    if topic_len > MAX_TOPIC_LEN {
        return Err(Error::TopicNameTooLong {
            len: topic_len as usize,
            limit: MAX_TOPIC_LEN as usize,
        });
    }
    let topic_end = fixed_len.saturating_add(topic_len as usize);
    if bytes.len() < topic_end {
        return Err(Error::HandshakeMalformed(format!(
            "topic name truncated: declared {} bytes, have {}",
            topic_len,
            bytes.len().saturating_sub(fixed_len)
        )));
    }
    let topic = std::str::from_utf8(&bytes[fixed_len..topic_end])
        .map_err(|_| Error::HandshakeMalformed("topic name is not UTF-8".to_string()))?
        .to_string();
    Ok(PubSubHandshake {
        topic,
        type_hash,
        payload_size,
        qos,
        version,
    })
}

/// Pure-byte parser for the framed payload header (`[u32 length]`)
/// plus body. Returns the body slice. Exposed for fuzz tests.
pub fn parse_frame(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        return Err(Error::HandshakeMalformed(format!(
            "frame header truncated: {} bytes",
            bytes.len()
        )));
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if len > MAX_PAYLOAD_LEN {
        return Err(Error::FrameTooLarge {
            actual: len as u64,
            limit: MAX_PAYLOAD_LEN as u64,
        });
    }
    let end = 4usize.saturating_add(len as usize);
    if bytes.len() < end {
        return Err(Error::HandshakeMalformed(format!(
            "frame body truncated: declared {} bytes, have {}",
            len,
            bytes.len().saturating_sub(4)
        )));
    }
    Ok(&bytes[4..end])
}

pub(crate) async fn write_frame(send: &mut SendStream, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 > MAX_PAYLOAD_LEN as u64 {
        return Err(Error::PayloadTooLarge {
            actual: bytes.len(),
            capacity: MAX_PAYLOAD_LEN as usize,
        });
    }
    send.write_all(&(bytes.len() as u32).to_le_bytes())
        .await
        .map_err(|e| Error::Remote(format!("frame header: {e}")))?;
    send.write_all(bytes)
        .await
        .map_err(|e| Error::Remote(format!("frame body: {e}")))
}

/// Read one logical item, reassembling chunk frames if the peer chunked
/// it (matches the Node item codec). Accepts a legacy single frame too.
/// `max_inflight_bytes` bounds reassembly. Returns `Ok(None)` on a clean
/// stream end before any frame.
pub(crate) async fn read_item_reassembled(
    recv: &mut RecvStream,
    max_inflight_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let mut reassembler: Option<Reassembler> = None;
    loop {
        let mut len_buf = [0u8; 4];
        if reassembler.is_none() {
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
        // A chunked frame is `[chunk header][chunk body]`; the body is at
        // most one message's worth of bytes, so the whole frame can't
        // exceed the inflight budget plus the fixed header. Clamp before
        // the `vec![0u8; len]` below so the ~2 GiB `CHUNK_FRAME_LEN_MASK`
        // ceiling can't be used to force a huge allocation ahead of any
        // reassembler-level inflight check.
        let limit = if chunked {
            (CHUNK_FRAME_LEN_MASK as usize).min(max_inflight_bytes.saturating_add(CHUNK_HEADER_LEN))
        } else {
            MAX_PAYLOAD_LEN as usize
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

pub(crate) async fn read_frame(recv: &mut RecvStream) -> Result<Option<Arc<[u8]>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        // Quinn-style: a clean FIN on read_exact for 0 bytes is the
        // graceful end-of-stream signal.
        Err(e) => {
            // Treat any read error after the publisher closes as
            // end-of-stream; surfacing it as `Ok(None)` lets
            // subscribers complete cleanly.
            let _ = e;
            return Ok(None);
        }
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_PAYLOAD_LEN {
        return Err(Error::FrameTooLarge {
            actual: len as u64,
            limit: MAX_PAYLOAD_LEN as u64,
        });
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| Error::Remote(format!("frame body: {e}")))?;
    Ok(Some(Arc::from(buf.into_boxed_slice())))
}
