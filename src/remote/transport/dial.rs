use super::*;

// --- subscriber side ---

/// Initial backoff between reconnect attempts on the subscriber
/// dispatcher loop. Doubles up to [`RECONNECT_BACKOFF_MAX`].
const RECONNECT_BACKOFF_MIN: std::time::Duration = std::time::Duration::from_millis(100);
const RECONNECT_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(10);

/// Dispatcher loop for a topic. Repeatedly dials the peer,
/// re-issues the handshake, and pumps frames into the topic's
/// broadcast channel. Exits when the broadcast sender is dropped
/// (i.e. the transport itself is gone).
#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn run_subscriber(inner: Arc<InnerShared>, topic: String, type_hash: u64, payload_size: u32) {
    let mut backoff = RECONNECT_BACKOFF_MIN;
    loop {
        let sender = {
            let map = crate::trace::recover_poison(
                inner.subscriber_topics.lock(),
                "RemoteTransport::subscriber_topics",
            );
            match map.get(&topic) {
                Some(t) => t.tx.clone(),
                // Topic state is gone — the transport is being
                // torn down. Exit cleanly.
                None => {
                    qb_debug!(
                        target: "peerbus::remote",
                        topic = %topic,
                        "dispatcher exiting: topic state gone"
                    );
                    return;
                }
            }
        };
        // Bail out if every subscriber has dropped — no point
        // re-establishing the wire just to feed nobody. A new
        // `subscriber()` call will spawn a fresh dispatcher, but only
        // if `dispatcher_started` has been reset; do that under the same
        // lock that guards the flag. Re-check `receiver_count()` while
        // holding the lock so we can't race with a `subscriber()` that
        // added a receiver (and saw `dispatcher_started == true`, so did
        // not spawn) between the unlocked check and the reset.
        if sender.receiver_count() == 0 {
            let mut map = crate::trace::recover_poison(
                inner.subscriber_topics.lock(),
                "RemoteTransport::subscriber_topics",
            );
            match map.get_mut(&topic) {
                Some(entry) => {
                    if entry.tx.receiver_count() == 0 {
                        entry.dispatcher_started = false;
                        drop(map);
                        qb_debug!(
                            target: "peerbus::remote",
                            topic = %topic,
                            "dispatcher exiting: no remaining receivers"
                        );
                        return;
                    }
                    // A new receiver appeared under the lock; keep serving.
                }
                // Topic state is gone — the transport is being torn down.
                None => return,
            }
        }

        match subscribe_pump_once(&inner, &topic, type_hash, payload_size, &sender).await {
            Ok(()) => {
                qb_warn!(
                    target: "peerbus::remote",
                    topic = %topic,
                    "subscriber stream ended, will reconnect"
                );
                // A subscriber started before its publisher exists sees a
                // clean EOF immediately; without a floor delay this loop
                // spins at 100% CPU. Sleep the minimum backoff before
                // retrying — still prompt, but bounded.
                tokio::time::sleep(RECONNECT_BACKOFF_MIN).await;
                backoff = RECONNECT_BACKOFF_MIN;
            }
            Err(e) => {
                qb_debug!(
                    target: "peerbus::remote",
                    topic = %topic,
                    error = %e,
                    backoff_ms = backoff.as_millis() as u64,
                    "subscriber reconnect attempt failed"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }
}

async fn subscribe_pump_once(
    inner: &Arc<InnerShared>,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
    sender: &broadcast::Sender<Arc<[u8]>>,
) -> Result<()> {
    let conn = ensure_peer_connection(inner).await?;
    let (mut send, mut recv) = tokio::time::timeout(HANDSHAKE_TIMEOUT, conn.open_bi())
        .await
        .map_err(|_| Error::Timeout(HANDSHAKE_TIMEOUT))?
        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;

    write_handshake(&mut send, topic, type_hash, payload_size).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;

    loop {
        match read_frame(&mut recv).await {
            Ok(Some(bytes)) => {
                // `broadcast::send` errors only when there are zero
                // receivers — fine; the dispatcher can drop the frame.
                let _ = sender.send(bytes);
            }
            Ok(None) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn ensure_peer_connection(inner: &Arc<InnerShared>) -> Result<Connection> {
    let mut guard = inner.peer_conn.lock().await;
    if let Some(conn) = guard.as_ref() {
        if conn.close_reason().is_none() {
            return Ok(conn.clone());
        }
        qb_warn!(
            target: "peerbus::remote",
            reason = ?conn.close_reason(),
            "cached connection is dead, re-dialing"
        );
        *guard = None;
    }
    let peer = inner
        .peer
        .clone()
        .ok_or_else(|| Error::invalid_argument("no peer configured"))?;
    qb_info!(
        target: "peerbus::remote",
        peer = %peer.id,
        "dialing peer"
    );
    let conn = tokio::time::timeout(
        DIAL_TIMEOUT,
        inner.endpoint.connect(peer.clone(), &inner.alpn),
    )
    .await
    .map_err(|_| {
        qb_warn!(
            target: "peerbus::remote",
            peer = %peer.id,
            timeout_s = DIAL_TIMEOUT.as_secs(),
            "dial timed out"
        );
        Error::Timeout(DIAL_TIMEOUT)
    })?
    .map_err(|e| {
        qb_warn!(
            target: "peerbus::remote",
            peer = %peer.id,
            error = %e,
            "dial failed"
        );
        Error::ConnectFailed(format!("{e}"))
    })?;
    qb_info!(
        target: "peerbus::remote",
        peer = %peer.id,
        "connected to peer"
    );
    *guard = Some(conn.clone());
    Ok(conn)
}

