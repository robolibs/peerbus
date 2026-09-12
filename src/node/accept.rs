use super::*;

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn run_accept_loop(inner: Arc<NodeInner>) -> Result<()> {
    qb_debug!(target: "peerbus::node", "accept loop started");
    while let Some(incoming) = inner.endpoint.accept().await {
        let inner = inner.clone();
        tokio::spawn(async move {
            let mut accepting = match incoming.accept() {
                Ok(a) => a,
                Err(e) => {
                    qb_warn!(target: "peerbus::node", error = %e, "incoming.accept failed");
                    return;
                }
            };
            let _alpn = match accepting.alpn().await {
                Ok(a) => a,
                Err(e) => {
                    qb_warn!(target: "peerbus::node", error = %e, "alpn negotiation failed");
                    return;
                }
            };
            let conn = match accepting.await {
                Ok(c) => c,
                Err(e) => {
                    qb_warn!(target: "peerbus::node", error = %e, "connection handshake failed");
                    return;
                }
            };
            // Peer ACL. Enforced here — once, on the connection, right
            // after the QUIC handshake and before any bi stream is
            // accepted — so pub/sub and all five request modes
            // (req/res, que/ans, put/ack, pip) plus best-effort
            // datagrams are covered by this single gate. Rejection
            // closes the connection with a distinct application code;
            // the dialing side sees a clean ConnectFailed rather than a
            // hang.
            let remote = conn.remote_id();
            let rejection = {
                let policy = crate::trace::recover_poison(
                    inner.inbound_policy.read(),
                    "Node::inbound_policy",
                );
                (!policy.allows(&remote)).then(|| policy.reject_reason())
            };
            if let Some(reason) = rejection {
                qb_warn!(
                    target: "peerbus::node",
                    remote = %remote,
                    reason,
                    "rejecting inbound connection from peer"
                );
                conn.close(ACL_REJECT_CODE.into(), ACL_REJECT_REASON);
                return;
            }
            qb_debug!(
                target: "peerbus::node",
                remote = %remote,
                "accepted connection"
            );
            let _ = serve_incoming_connection(inner, conn).await;
        });
    }
    qb_debug!(target: "peerbus::node", "accept loop exiting");
    Ok(())
}

// `e` below is consumed only by `qb_warn!`, which compiles to nothing
// without the `tracing` feature, so the binding reads as unused there.
#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn serve_incoming_connection(
    inner: Arc<NodeInner>,
    conn: Connection,
) -> Result<()> {
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let inner = inner.clone();
                let conn = conn.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_bi(inner, conn, send, recv).await {
                        qb_warn!(target: "peerbus::node", error = %e, "subscriber stream failed");
                    }
                });
            }
            Err(_) => return Ok(()),
        }
    }
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn serve_bi(
    inner: Arc<NodeInner>,
    conn: Connection,
    send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let mut magic = [0u8; 4];
    recv.read_exact(&mut magic)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("stream magic: {e}")))?;
    match u32::from_le_bytes(magic) {
        HANDSHAKE_MAGIC => serve_pubsub_bi(inner, conn, send, recv).await,
        REQRESP_MAGIC => serve_reqres_bi(inner, send, recv).await,
        QUEANS_MAGIC => serve_queans_bi(inner, send, recv).await,
        PUTACK_MAGIC => serve_putack_bi(inner, send, recv).await,
        PIP_MAGIC => serve_pip_bi(inner, send, recv).await,
        magic => Err(Error::HandshakeMalformed(format!(
            "unknown stream magic 0x{magic:x}"
        ))),
    }
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn serve_pubsub_bi(
    inner: Arc<NodeInner>,
    conn: Connection,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let handshake = read_topic_handshake_tail(&mut recv).await?;
    let topic = handshake.topic;
    let type_hash = handshake.type_hash;
    let payload_size = handshake.payload_size;
    let subscriber_version = handshake.version;
    qb_debug!(
        target: "peerbus::node",
        topic = %topic,
        type_hash = format_args!("0x{type_hash:x}"),
        payload_size,
        "subscriber handshake received"
    );
    let remote_id = conn.remote_id();
    let session_id = pubsub_datagram_session_id(
        inner.endpoint_id,
        remote_id,
        &topic,
        type_hash,
        payload_size,
    );

    let (mut rx, qos, stale_dropped, bytes_sent, send_errors) = {
        let map =
            crate::trace::recover_poison(inner.publisher_topics.lock(), "Node::publisher_topics");
        match map.get(&topic) {
            Some(state) => {
                if state.type_hash != type_hash || state.payload_size != payload_size {
                    qb_warn!(
                        target: "peerbus::node",
                        topic = %topic,
                        expected_hash = format_args!("0x{:x}", state.type_hash),
                        peer_hash = format_args!("0x{type_hash:x}"),
                        expected_size = state.payload_size,
                        peer_size = payload_size,
                        "rejecting subscriber: type mismatch"
                    );
                    return Err(Error::TypeMismatch {
                        expected: "<publisher type>",
                        got: format!("hash=0x{type_hash:x} size={payload_size}"),
                    });
                }
                (
                    state.iroh_tx.subscribe(),
                    state.qos,
                    state.stale_dropped.clone(),
                    state.bytes_sent.clone(),
                    state.send_errors.clone(),
                )
            }
            None => {
                qb_debug!(
                    target: "peerbus::node",
                    topic = %topic,
                    "no local publisher for requested topic"
                );
                return Ok(());
            }
        }
    };

    loop {
        match rx.recv().await {
            Ok(mut bytes) => {
                let mut skipped = 0u64;
                if matches!(
                    qos.delivery,
                    DeliveryPolicy::Latest | DeliveryPolicy::BestEffort
                ) {
                    loop {
                        match rx.try_recv() {
                            Ok(newer) => {
                                bytes = newer;
                                skipped += 1;
                            }
                            Err(broadcast::error::TryRecvError::Empty) => break,
                            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                                skipped += n;
                                continue;
                            }
                            Err(broadcast::error::TryRecvError::Closed) => {
                                let _ = send.finish();
                                return Ok(());
                            }
                        }
                    }
                }
                if skipped > 0 {
                    stale_dropped.fetch_add(skipped, Ordering::Relaxed);
                    qb_warn!(
                        target: "peerbus::node",
                        topic = %topic,
                        dropped = skipped,
                        "remote subscriber fell behind; sending newest sample"
                    );
                }
                if qos.delivery == DeliveryPolicy::BestEffort
                    && subscriber_version == PUBSUB_HANDSHAKE_VERSION_QOS
                    && conn.max_datagram_size().is_some()
                {
                    if let Err(e) = send_pubsub_datagrams(&conn, session_id, &bytes, qos) {
                        send_errors.fetch_add(1, Ordering::Relaxed);
                        qb_warn!(
                            target: "peerbus::node",
                            topic = %topic,
                            error = %e,
                            "best-effort datagram send failed; dropping sample"
                        );
                        continue;
                    }
                } else {
                    if write_pubsub_message(&mut send, &bytes, qos, subscriber_version)
                        .await
                        .is_err()
                    {
                        send_errors.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                }
                bytes_sent.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            }
            Err(broadcast::error::RecvError::Closed) => {
                let _ = send.finish();
                return Ok(());
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                if qos.delivery == DeliveryPolicy::Reliable {
                    send_errors.fetch_add(1, Ordering::Relaxed);
                    qb_warn!(
                        target: "peerbus::node",
                        topic = %topic,
                        dropped = n,
                        "reliable subscriber exceeded publisher queue capacity"
                    );
                    return Err(Error::Lagged { dropped: n });
                }
                qb_warn!(
                    target: "peerbus::node",
                    topic = %topic,
                    dropped = n,
                    "broadcast lagged on serve path"
                );
                continue;
            }
        }
    }
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn serve_reqres_bi(
    inner: Arc<NodeInner>,
    send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let (topic, req_hash, res_hash, req_header_size, res_header_size, hs_qos) =
        read_item_handshake_tail(&mut recv).await?;
    let (tx, qos, stats) = {
        let map = crate::trace::recover_poison(inner.request_topics.lock(), "Node::request_topics");
        let Some(state) = map.get(&topic) else {
            qb_debug!(
                target: "peerbus::node",
                topic = %topic,
                "no local req/res server for requested topic"
            );
            return Ok(());
        };
        if state.req_type_hash != req_hash
            || state.res_type_hash != res_hash
            || state.req_header_size != req_header_size
            || state.res_header_size != res_header_size
        {
            qb_warn!(
                target: "peerbus::node",
                topic = %topic,
                expected_req_hash = format_args!("0x{:x}", state.req_type_hash),
                peer_req_hash = format_args!("0x{req_hash:x}"),
                expected_res_hash = format_args!("0x{:x}", state.res_type_hash),
                peer_res_hash = format_args!("0x{res_hash:x}"),
                "rejecting req/res client: type mismatch"
            );
            return Err(Error::TypeMismatch {
                expected: "<req/res server>",
                got: format!(
                    "req_hash=0x{req_hash:x} res_hash=0x{res_hash:x} req_size={req_header_size} res_size={res_header_size}"
                ),
            });
        }
        (state.tx.clone(), state.qos, state.stats.clone())
    };

    let frame = read_item_chunked(&mut recv, qos.max_message_bytes, qos.max_inflight_bytes)
        .await?
        .ok_or_else(|| Error::Remote("client closed without sending a request".to_string()))?;
    stats.record_in(frame.len());
    let req_id = NEXT_REMOTE_REQ_ID.fetch_add(1, Ordering::AcqRel) + 1;
    tx.send(RemotePendingReq {
        req_id,
        frame,
        send,
        qos,
        peer_chunks: hs_qos.peer_chunks,
        stats,
    })
    .await
    .map_err(|_| Error::Remote("req/res server was dropped".to_string()))?;
    Ok(())
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn serve_queans_bi(
    inner: Arc<NodeInner>,
    send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let (topic, que_hash, ans_hash, que_header_size, ans_header_size, hs_qos) =
        read_item_handshake_tail(&mut recv).await?;
    let (tx, qos, stats) = {
        let map = crate::trace::recover_poison(inner.que_topics.lock(), "Node::que_topics");
        let Some(state) = map.get(&topic) else {
            qb_debug!(
                target: "peerbus::node",
                topic = %topic,
                "no local que/ans server for requested topic"
            );
            return Ok(());
        };
        if state.que_type_hash != que_hash
            || state.ans_type_hash != ans_hash
            || state.que_header_size != que_header_size
            || state.ans_header_size != ans_header_size
        {
            qb_warn!(
                target: "peerbus::node",
                topic = %topic,
                expected_que_hash = format_args!("0x{:x}", state.que_type_hash),
                peer_que_hash = format_args!("0x{que_hash:x}"),
                expected_ans_hash = format_args!("0x{:x}", state.ans_type_hash),
                peer_ans_hash = format_args!("0x{ans_hash:x}"),
                "rejecting que/ans client: type mismatch"
            );
            return Err(Error::TypeMismatch {
                expected: "<que/ans server>",
                got: format!(
                    "que_hash=0x{que_hash:x} ans_hash=0x{ans_hash:x} que_size={que_header_size} ans_size={ans_header_size}"
                ),
            });
        }
        (state.tx.clone(), state.qos, state.stats.clone())
    };

    let frame = read_item_chunked(&mut recv, qos.max_message_bytes, qos.max_inflight_bytes)
        .await?
        .ok_or_else(|| Error::Remote("client closed without sending a que".to_string()))?;
    stats.record_in(frame.len());
    let req_id = NEXT_REMOTE_REQ_ID.fetch_add(1, Ordering::AcqRel) + 1;
    tx.send(RemotePendingQue {
        req_id,
        frame,
        send,
        qos,
        peer_chunks: hs_qos.peer_chunks,
        stats,
    })
    .await
    .map_err(|_| Error::Remote("que/ans server was dropped".to_string()))?;
    Ok(())
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn serve_putack_bi(
    inner: Arc<NodeInner>,
    send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let (topic, put_hash, ack_hash, put_header_size, ack_header_size, hs_qos) =
        read_item_handshake_tail(&mut recv).await?;
    let (tx, qos, stats) = {
        let map = crate::trace::recover_poison(inner.put_topics.lock(), "Node::put_topics");
        let Some(state) = map.get(&topic) else {
            qb_debug!(
                target: "peerbus::node",
                topic = %topic,
                "no local put/ack server for requested topic"
            );
            return Ok(());
        };
        if state.put_type_hash != put_hash
            || state.ack_type_hash != ack_hash
            || state.put_header_size != put_header_size
            || state.ack_header_size != ack_header_size
        {
            qb_warn!(
                target: "peerbus::node",
                topic = %topic,
                expected_put_hash = format_args!("0x{:x}", state.put_type_hash),
                peer_put_hash = format_args!("0x{put_hash:x}"),
                expected_ack_hash = format_args!("0x{:x}", state.ack_type_hash),
                peer_ack_hash = format_args!("0x{ack_hash:x}"),
                "rejecting put/ack client: type mismatch"
            );
            return Err(Error::TypeMismatch {
                expected: "<put/ack server>",
                got: format!(
                    "put_hash=0x{put_hash:x} ack_hash=0x{ack_hash:x} put_size={put_header_size} ack_size={ack_header_size}"
                ),
            });
        }
        (state.tx.clone(), state.qos, state.stats.clone())
    };

    let req_id = NEXT_REMOTE_REQ_ID.fetch_add(1, Ordering::AcqRel) + 1;
    tx.send(RemotePendingPuts {
        req_id,
        recv,
        send,
        qos,
        peer_chunks: hs_qos.peer_chunks,
        stats,
    })
    .await
    .map_err(|_| Error::Remote("put/ack server was dropped".to_string()))?;
    Ok(())
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn serve_pip_bi(
    inner: Arc<NodeInner>,
    send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let (topic, client_hash, server_hash, client_header_size, server_header_size, hs_qos) =
        read_item_handshake_tail(&mut recv).await?;
    let (tx, qos, stats) = {
        let map = crate::trace::recover_poison(inner.pip_topics.lock(), "Node::pip_topics");
        let Some(state) = map.get(&topic) else {
            qb_debug!(
                target: "peerbus::node",
                topic = %topic,
                "no local pip server for requested topic"
            );
            return Ok(());
        };
        if state.client_type_hash != client_hash
            || state.server_type_hash != server_hash
            || state.client_header_size != client_header_size
            || state.server_header_size != server_header_size
        {
            qb_warn!(
                target: "peerbus::node",
                topic = %topic,
                expected_client_hash = format_args!("0x{:x}", state.client_type_hash),
                peer_client_hash = format_args!("0x{client_hash:x}"),
                expected_server_hash = format_args!("0x{:x}", state.server_type_hash),
                peer_server_hash = format_args!("0x{server_hash:x}"),
                "rejecting pip client: type mismatch"
            );
            return Err(Error::TypeMismatch {
                expected: "<pip server>",
                got: format!(
                    "client_hash=0x{client_hash:x} server_hash=0x{server_hash:x} client_size={client_header_size} server_size={server_header_size}"
                ),
            });
        }
        (state.tx.clone(), state.qos, state.stats.clone())
    };

    let session_id = NEXT_REMOTE_REQ_ID.fetch_add(1, Ordering::AcqRel) + 1;
    tx.send(RemotePendingPip {
        session_id,
        recv,
        send,
        qos,
        peer_chunks: hs_qos.peer_chunks,
        stats,
    })
    .await
    .map_err(|_| Error::Remote("pip server was dropped".to_string()))?;
    Ok(())
}
