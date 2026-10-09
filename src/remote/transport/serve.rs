use super::*;

// --- accept side (publisher endpoint) ---

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn run_accept_loop(inner: Arc<InnerShared>) -> Result<()> {
    qb_debug!(target: "peerbus::remote", name = %inner.name, "accept loop started");
    while let Some(accept) = inner.endpoint.accept().await {
        let inner = inner.clone();
        tokio::spawn(async move {
            let mut iconn = match accept.accept() {
                Ok(c) => c,
                Err(e) => {
                    qb_warn!(target: "peerbus::remote", error = %e, "incoming.accept failed");
                    return;
                }
            };
            let _alpn = match iconn.alpn().await {
                Ok(a) => a,
                Err(e) => {
                    qb_warn!(target: "peerbus::remote", error = %e, "alpn negotiation failed");
                    return;
                }
            };
            let conn = match iconn.await {
                Ok(c) => c,
                Err(e) => {
                    qb_warn!(target: "peerbus::remote", error = %e, "connection handshake failed");
                    return;
                }
            };
            qb_debug!(
                target: "peerbus::remote",
                remote = %conn.remote_id(),
                "accepted connection"
            );
            if let Err(e) = serve_incoming_connection(inner, conn).await {
                qb_warn!(
                    target: "peerbus::remote",
                    error = %e,
                    "serve_incoming_connection ended with error"
                );
            }
        });
    }
    qb_debug!(target: "peerbus::remote", "accept loop exiting");
    Ok(())
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn serve_incoming_connection(inner: Arc<InnerShared>, conn: Connection) -> Result<()> {
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let inner = inner.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_bi(inner, send, recv).await {
                        qb_warn!(
                            target: "peerbus::remote",
                            error = %e,
                            "serve_bi ended with error"
                        );
                    }
                });
            }
            Err(_) => return Ok(()),
        }
    }
}

async fn serve_bi(inner: Arc<InnerShared>, send: SendStream, mut recv: RecvStream) -> Result<()> {
    // Peek the magic so we can dispatch pub/sub vs req/res. Bounded so a
    // peer that opens a stream but never writes can't park this task.
    let mut magic_buf = [0u8; 4];
    tokio::time::timeout(HANDSHAKE_TIMEOUT, recv.read_exact(&mut magic_buf))
        .await
        .map_err(|_| Error::Timeout(HANDSHAKE_TIMEOUT))?
        .map_err(|e| Error::Remote(format!("magic: {e}")))?;
    let magic = u32::from_le_bytes(magic_buf);
    match magic {
        HANDSHAKE_MAGIC => serve_pubsub_bi(inner, send, recv).await,
        REQRESP_MAGIC => crate::remote::reqresp::serve_request_bi(inner, send, recv).await,
        QUEANS_MAGIC => crate::remote::queans::serve_queans_bi(inner, send, recv).await,
        PUTACK_MAGIC => crate::remote::putack::serve_putack_bi(inner, send, recv).await,
        PIP_MAGIC => crate::remote::pip::serve_pip_bi(inner, send, recv).await,
        _ => Err(Error::Remote(format!("unknown stream magic 0x{magic:x}"))),
    }
}

async fn serve_pubsub_bi(
    inner: Arc<InnerShared>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (topic, type_hash, payload_size) = read_pubsub_handshake_tail(&mut recv).await?;

    let (broadcast_rx, expected_type_hash, expected_size) = {
        let map = crate::trace::recover_poison(
            inner.publisher_topics.lock(),
            "RemoteTransport::publisher_topics",
        );
        let entry = match map.get(&topic) {
            Some(e) => e,
            None => return Ok(()), // No publisher; drop quietly.
        };
        (entry.tx.subscribe(), entry.type_hash, entry.payload_size)
    };

    if expected_type_hash != type_hash || expected_size != payload_size {
        return Err(Error::TypeMismatch {
            expected: "<remote publisher>",
            got: format!("hash=0x{:x} size={}", type_hash, payload_size),
        });
    }

    pump_broadcast(broadcast_rx, &mut send).await
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn pump_broadcast(
    mut rx: broadcast::Receiver<Arc<[u8]>>,
    send: &mut SendStream,
) -> Result<()> {
    loop {
        match rx.recv().await {
            Ok(bytes) => {
                if write_frame(send, &bytes).await.is_err() {
                    return Ok(());
                }
            }
            Err(broadcast::error::RecvError::Closed) => {
                let _ = send.finish();
                return Ok(());
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                qb_warn!(
                    target: "peerbus::remote",
                    dropped = n,
                    "broadcast lagged on publisher serve path"
                );
                continue;
            }
        }
    }
}

