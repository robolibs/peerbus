use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) fn maybe_register_pubsub_datagram_route(
    inner: &Arc<NodeInner>,
    conn: Connection,
    peer_id: EndpointId,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
    qos: TopicQos,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    stale_dropped: Arc<AtomicU64>,
    incomplete_dropped: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
) {
    if qos.delivery != DeliveryPolicy::BestEffort || conn.max_datagram_size().is_none() {
        return;
    }

    let session_id =
        pubsub_datagram_session_id(peer_id, inner.endpoint_id, topic, type_hash, payload_size);
    let peer = *peer_id.as_bytes();
    let key = DatagramRouteKey { peer, session_id };
    {
        let mut routes = crate::trace::recover_poison(
            inner.pubsub_datagram_routes.lock(),
            "Node::pubsub_datagram_routes",
        );
        routes.insert(
            key,
            Arc::new(PubsubDatagramSink {
                tx,
                reassembler: Mutex::new(Reassembler::new(
                    DeliveryPolicy::BestEffort,
                    qos.max_inflight_bytes,
                )),
                stale_dropped,
                incomplete_dropped,
                bytes_received,
            }),
        );
    }

    let stable_id = conn.stable_id();
    let should_spawn = {
        let mut readers = crate::trace::recover_poison(
            inner.pubsub_datagram_readers.lock(),
            "Node::pubsub_datagram_readers",
        );
        match readers.get(&peer).copied() {
            Some(existing) if existing == stable_id => false,
            _ => {
                readers.insert(peer, stable_id);
                true
            }
        }
    };
    if should_spawn {
        let inner_loop = inner.clone();
        inner.rt.spawn(async move {
            run_pubsub_datagram_reader(inner_loop, peer_id, conn).await;
        });
    }
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn run_pubsub_datagram_reader(inner: Arc<NodeInner>, peer_id: EndpointId, conn: Connection) {
    let peer = *peer_id.as_bytes();
    let stable_id = conn.stable_id();
    loop {
        let datagram = match conn.read_datagram().await {
            Ok(datagram) => datagram,
            Err(e) => {
                qb_debug!(
                    target: "peerbus::node",
                    peer = %peer_id,
                    error = %e,
                    "pub/sub datagram reader stopped"
                );
                break;
            }
        };

        let parsed = parse_pubsub_datagram(&datagram);
        let (session_id, frame) = match parsed {
            Ok(parsed) => parsed,
            Err(e) => {
                qb_warn!(
                    target: "peerbus::node",
                    peer = %peer_id,
                    error = %e,
                    "dropping malformed pub/sub datagram"
                );
                continue;
            }
        };
        let key = DatagramRouteKey { peer, session_id };
        let sink = {
            crate::trace::recover_poison(
                inner.pubsub_datagram_routes.lock(),
                "Node::pubsub_datagram_routes",
            )
            .get(&key)
            .cloned()
        };
        let Some(sink) = sink else {
            continue;
        };

        let before = {
            let reassembler =
                crate::trace::recover_poison(sink.reassembler.lock(), "PubsubDatagramSink");
            reassembler.incomplete_dropped()
        };
        let message = {
            let mut reassembler =
                crate::trace::recover_poison(sink.reassembler.lock(), "PubsubDatagramSink");
            match reassembler.push(frame) {
                Ok(message) => {
                    let after = reassembler.incomplete_dropped();
                    if after > before {
                        sink.incomplete_dropped
                            .fetch_add(after - before, Ordering::Relaxed);
                    }
                    message
                }
                Err(e) => {
                    qb_warn!(
                        target: "peerbus::node",
                        peer = %peer_id,
                        error = %e,
                        "dropping malformed pub/sub datagram chunk"
                    );
                    continue;
                }
            }
        };

        if let Some(message) = message {
            let len = message.len();
            match sink.tx.try_send(message) {
                Ok(()) => {
                    sink.bytes_received.fetch_add(len as u64, Ordering::Relaxed);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    sink.stale_dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    crate::trace::recover_poison(
                        inner.pubsub_datagram_routes.lock(),
                        "Node::pubsub_datagram_routes",
                    )
                    .remove(&key);
                }
            }
        }
    }

    let mut readers = crate::trace::recover_poison(
        inner.pubsub_datagram_readers.lock(),
        "Node::pubsub_datagram_readers",
    );
    if readers.get(&peer).copied() == Some(stable_id) {
        readers.remove(&peer);
    }
}

pub(crate) fn send_pubsub_datagrams(
    conn: &Connection,
    session_id: u64,
    payload: &[u8],
    qos: TopicQos,
) -> Result<()> {
    let max_datagram_size = conn
        .max_datagram_size()
        .ok_or_else(|| Error::Remote("QUIC datagrams are unavailable on this path".to_string()))?;
    let overhead = PUBSUB_DATAGRAM_HEADER_LEN + CHUNK_HEADER_LEN;
    if max_datagram_size <= overhead {
        return Err(Error::FrameTooLarge {
            actual: overhead as u64,
            limit: max_datagram_size as u64,
        });
    }
    let chunk_bytes = qos
        .chunk_bytes
        .max(1)
        .min(max_datagram_size.saturating_sub(overhead));
    let chunk_count = payload.len().div_ceil(chunk_bytes).max(1);
    if chunk_count > u32::MAX as usize {
        return Err(Error::FrameTooLarge {
            actual: payload.len() as u64,
            limit: (chunk_bytes as u64) * (u32::MAX as u64),
        });
    }
    let message_id = NEXT_REMOTE_REQ_ID.fetch_add(1, Ordering::AcqRel) + 1;
    for (idx, chunk) in payload.chunks(chunk_bytes).enumerate() {
        let chunk_payload = make_chunk_payload(
            message_id,
            idx as u32,
            chunk_count as u32,
            payload.len(),
            chunk,
        )?;
        let datagram = make_pubsub_datagram(session_id, &chunk_payload);
        if datagram.len() > max_datagram_size {
            return Err(Error::FrameTooLarge {
                actual: datagram.len() as u64,
                limit: max_datagram_size as u64,
            });
        }
        conn.send_datagram(Bytes::from(datagram))
            .map_err(|e| Error::Remote(format!("send datagram: {e}")))?;
    }
    if payload.is_empty() {
        let chunk_payload = make_chunk_payload(message_id, 0, 1, 0, &[])?;
        let datagram = make_pubsub_datagram(session_id, &chunk_payload);
        conn.send_datagram(Bytes::from(datagram))
            .map_err(|e| Error::Remote(format!("send empty datagram: {e}")))?;
    }
    Ok(())
}

pub(crate) fn make_pubsub_datagram(session_id: u64, chunk_payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PUBSUB_DATAGRAM_HEADER_LEN + chunk_payload.len());
    out.extend_from_slice(PUBSUB_DATAGRAM_MAGIC);
    out.extend_from_slice(&session_id.to_le_bytes());
    out.extend_from_slice(chunk_payload);
    out
}

pub(crate) fn parse_pubsub_datagram(bytes: &[u8]) -> Result<(u64, crate::chunk::ChunkFrame<'_>)> {
    if bytes.len() < PUBSUB_DATAGRAM_HEADER_LEN {
        return Err(Error::HandshakeMalformed(format!(
            "pub/sub datagram truncated: {} bytes",
            bytes.len()
        )));
    }
    if &bytes[..4] != PUBSUB_DATAGRAM_MAGIC {
        return Err(Error::HandshakeMalformed(
            "unknown pub/sub datagram magic".to_string(),
        ));
    }
    let session_id = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let frame = parse_chunk_payload(&bytes[PUBSUB_DATAGRAM_HEADER_LEN..])?;
    Ok((session_id, frame))
}

pub(crate) fn pubsub_datagram_session_id(
    publisher_id: EndpointId,
    subscriber_id: EndpointId,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
) -> u64 {
    fnv1a64(&format!(
        "pubsub-dgram:{publisher_id}:{subscriber_id}:{topic}:{type_hash:x}:{payload_size}"
    ))
}
