use super::*;

impl Node {
    /// Build a publisher for `topic`. Writes go to both:
    /// * local SHM service named `<identity>__<topic>` (same-host
    ///   subscribers attach to this and read zero-copy);
    /// * a broadcast queue feeding the iroh accept loop, which
    ///   serves attached remote subscribers.
    pub fn publisher<T>(&self, topic: &str) -> Result<Publisher<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.publisher_with_qos(topic, TopicQos::default())
    }

    pub fn publisher_with_qos<T>(&self, topic: &str, qos: TopicQos) -> Result<Publisher<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let type_name = type_name::<T>();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;

        let (iroh_tx, published, remote_dropped, stale_dropped, bytes_sent, send_errors) = {
            let mut map = crate::trace::recover_poison(
                self.inner.publisher_topics.lock(),
                "Node::publisher_topics",
            );
            let entry = map.entry(route_topic.clone()).or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(qos.subscriber_queue.max(1));
                PublisherTopicState {
                    iroh_tx: tx,
                    type_hash,
                    payload_size,
                    qos,
                    published: Arc::new(AtomicU64::new(0)),
                    remote_dropped: Arc::new(AtomicU64::new(0)),
                    stale_dropped: Arc::new(AtomicU64::new(0)),
                    bytes_sent: Arc::new(AtomicU64::new(0)),
                    send_errors: Arc::new(AtomicU64::new(0)),
                }
            });
            if entry.type_hash != type_hash || entry.payload_size != payload_size {
                return Err(Error::TypeMismatch {
                    expected: "<existing publisher type>",
                    got: type_name.to_string(),
                });
            }
            if entry.qos != qos {
                return Err(Error::invalid_argument(format!(
                    "publisher QoS mismatch for topic '{route_topic}'"
                )));
            }
            (
                entry.iroh_tx.clone(),
                entry.published.clone(),
                entry.remote_dropped.clone(),
                entry.stale_dropped.clone(),
                entry.bytes_sent.clone(),
                entry.send_errors.clone(),
            )
        };

        let svc_name = self.primary_service_name(topic)?;
        let service = LocalService::<T>::open_or_create(&svc_name, self.inner.local_cfg.clone())?;
        let local_publisher = service.publisher()?;

        // Auto-alias: when the primary service name uses an
        // identity_name (e.g. `.identity("rover-a")`), also open an
        // alias service under the hex-EndpointId composition so
        // subscribers that only know the public key (`did:key:…`
        // strings, bare `EndpointId`) can also route locally instead
        // of falling back to iroh loopback. No-op when the publisher
        // already composes by hex (`identity_file` / ephemeral).
        let alias = if self.inner.system_did.is_none() && self.inner.identity_name.is_some() {
            let hex_name = service_name(None, self.inner.endpoint_id.as_bytes(), topic);
            let alias_service =
                LocalService::<T>::open_or_create(&hex_name, self.inner.local_cfg.clone())?;
            let alias_publisher = alias_service.publisher()?;
            Some(PublisherAlias {
                publisher: alias_publisher,
                _service: alias_service,
            })
        } else {
            None
        };

        Ok(Publisher {
            local_publisher,
            _local_service: service,
            alias,
            iroh_tx,
            qos,
            published,
            remote_dropped,
            stale_dropped,
            bytes_sent,
            send_errors,
        })
    }

    /// Subscribe to a topic in this node's configured system DID.
    ///
    /// Resolution order:
    ///
    /// 1. Try local SHM for `(system_did, topic)`.
    /// 2. If absent, use an explicit route added via
    ///    [`add_topic_route`](Self::add_topic_route) and subscribe over iroh.
    pub fn subscribe<T>(&self, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.subscribe_with_qos(topic, TopicQos::default())
    }

    pub fn subscribe_with_qos<T>(&self, topic: &str, _qos: TopicQos) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.system_route_topic(topic)?;
        let svc_name = system_service_name(
            self.inner
                .system_did
                .as_deref()
                .expect("system_route_topic validates presence"),
            topic,
        );

        if let Ok(svc) = LocalService::<T>::open_existing(&svc_name) {
            let local_sub = svc.subscriber()?;
            return Ok(Subscriber {
                source: SubscriberSource::Local {
                    sub: local_sub,
                    qos: _qos,
                    _svc: svc,
                },
                received: Arc::new(AtomicU64::new(0)),
                disconnects: Arc::new(AtomicU64::new(0)),
                stale_dropped: Arc::new(AtomicU64::new(0)),
                incomplete_dropped: Arc::new(AtomicU64::new(0)),
                bytes_received: Arc::new(AtomicU64::new(0)),
            });
        }

        let endpoint =
            crate::trace::recover_poison(self.inner.system_routes.lock(), "Node::system_routes")
                .get(&route_topic)
                .cloned()
                .or_else(|| {
                    crate::trace::recover_poison(
                        self.inner.system_peers.lock(),
                        "Node::system_peers",
                    )
                    .first()
                    .cloned()
                })
                .ok_or_else(|| Error::ServiceNotFound(route_topic.clone()))?;

        self.remote_subscriber::<T>(endpoint.id, Some(endpoint), route_topic, _qos)
    }

    /// Subscribe to `peer`'s publication of `topic`.
    ///
    /// Routing:
    /// * If we can open an existing local SHM service for the
    ///   peer + topic on this host → attach locally (zero copy).
    /// * Otherwise → dial `peer` over iroh, open a bi stream,
    ///   write the topic handshake, pump received frames to an
    ///   internal queue.
    pub fn subscriber<T>(&self, peer: impl IntoPeer, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.subscriber_with_qos(peer, topic, TopicQos::default())
    }

    pub fn subscriber_with_qos<T>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        _qos: TopicQos,
    ) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        // Try local first. The open-only call returns Err if
        // the service hasn't been created anywhere on the host. We
        // try the named composition first (if any) then fall through
        // to the hex-EndpointId composition — that second probe is
        // what makes `did:key:` and bare-EndpointId peers route to a
        // local publisher whose identity is also un-named (i.e. an
        // `identity_file` or ephemeral key).
        let mut candidates: Vec<String> = Vec::with_capacity(2);
        if peer.name.is_some() {
            candidates.push(service_name(peer.name.as_deref(), &peer_bytes, topic));
        }
        candidates.push(service_name(None, &peer_bytes, topic));
        for svc_name in candidates {
            if let Ok(svc) = LocalService::<T>::open_existing(&svc_name) {
                let local_sub = svc.subscriber()?;
                return Ok(Subscriber {
                    source: SubscriberSource::Local {
                        sub: local_sub,
                        qos: _qos,
                        _svc: svc,
                    },
                    received: Arc::new(AtomicU64::new(0)),
                    disconnects: Arc::new(AtomicU64::new(0)),
                    stale_dropped: Arc::new(AtomicU64::new(0)),
                    incomplete_dropped: Arc::new(AtomicU64::new(0)),
                    bytes_received: Arc::new(AtomicU64::new(0)),
                });
            }
        }

        self.remote_subscriber::<T>(peer.endpoint_id, peer.addr.clone(), topic.to_string(), _qos)
    }

}

pub(crate) struct RemotePubsubOpen {
    pub(crate) conn: Connection,
    pub(crate) recv: iroh::endpoint::RecvStream,
}

/// Single dial + handshake attempt. Returns the publisher connection and
/// `RecvStream` ready for [`pump_recv_stream`].
pub(crate) async fn subscribe_once(
    inner: &Arc<NodeInner>,
    peer_id: EndpointId,
    addr_hint: Option<EndpointAddr>,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
    qos: TopicQos,
) -> Result<RemotePubsubOpen> {
    let conn = ensure_peer_connection(inner, peer_id, addr_hint).await?;
    let (mut send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
    write_topic_handshake(&mut send, topic, type_hash, payload_size, qos).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
    Ok(RemotePubsubOpen { conn, recv })
}

/// Reconnect loop. Repeatedly redials and re-issues the handshake
/// when the wire side disconnects. Exits cleanly when the
/// subscriber drops its `mpsc::Receiver` (detected via
/// `tx.is_closed()`).
#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_subscriber_loop(
    inner: Arc<NodeInner>,
    peer_id: EndpointId,
    topic: String,
    type_hash: u64,
    payload_size: u32,
    qos: TopicQos,
    stale_dropped: Arc<AtomicU64>,
    incomplete_dropped: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
) {
    let mut backoff = RECONNECT_BACKOFF_MIN;
    loop {
        if tx.is_closed() {
            qb_debug!(
                target: "peerbus::node",
                topic = %topic,
                "subscriber receiver dropped, exiting reconnect loop"
            );
            return;
        }
        // Reconnect uses the addr_hint cached in `peer_connections`
        // by the initial dial — pass `None` so we don't override
        // it.
        match subscribe_once(&inner, peer_id, None, &topic, type_hash, payload_size, qos).await {
            Ok(open) => {
                qb_info!(
                    target: "peerbus::node",
                    topic = %topic,
                    peer = %peer_id,
                    "subscriber stream re-established"
                );
                maybe_register_pubsub_datagram_route(
                    &inner,
                    open.conn.clone(),
                    peer_id,
                    &topic,
                    type_hash,
                    payload_size,
                    qos,
                    tx.clone(),
                    stale_dropped.clone(),
                    incomplete_dropped.clone(),
                    bytes_received.clone(),
                );
                backoff = RECONNECT_BACKOFF_MIN;
                let _ = pump_recv_stream(
                    open.recv,
                    tx.clone(),
                    qos,
                    incomplete_dropped.clone(),
                    bytes_received.clone(),
                )
                .await;
                qb_warn!(
                    target: "peerbus::node",
                    topic = %topic,
                    peer = %peer_id,
                    "subscriber stream ended, will reconnect"
                );
            }
            Err(e) => {
                qb_debug!(
                    target: "peerbus::node",
                    topic = %topic,
                    peer = %peer_id,
                    error = %e,
                    backoff_ms = backoff.as_millis() as u64,
                    "reconnect attempt failed"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }
}

// ---- publisher ----

/// Snapshot of a [`Publisher`]'s lifetime counters.
///
/// All fields are monotonically increasing since the publisher was
/// created. Reading a snapshot is wait-free; callers can poll as
/// often as they want without disturbing the publish path.
#[derive(Debug, Default, Clone, Copy)]
pub struct PublisherStats {
    /// Successful `publish()` calls (local SHM accepted the sample).
    pub published: u64,
    /// Outbound remote frames the iroh broadcast queue refused —
    /// today, this means no remote subscriber was attached at the
    /// moment of publish.
    pub remote_dropped: u64,
    /// Stale samples skipped for latest/best-effort remote subscribers.
    pub stale_dropped: u64,
    /// Bytes successfully written to remote subscriber streams.
    pub bytes_sent: u64,
    /// Remote stream write failures observed while serving subscribers.
    pub send_errors: u64,
}

/// Transport counters for an item-stream mode (req/res, que/ans,
/// put/ack, pip), exposed via `.stats()` on each server and client.
///
/// Counts the **remote (iroh)** path: locally-routed (SHM) endpoints
/// report zeros, mirroring how [`PublisherStats::bytes_sent`] is
/// remote-only. "Messages" are logical items — a chunked message counts
/// once, after reassembly. `bytes_*` count reassembled payload bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ItemStats {
    /// Logical items this endpoint sent over the remote stream
    /// (requests/queries/puts/answers/acks/responses/pip messages,
    /// per role).
    pub messages_out: u64,
    /// Logical items this endpoint received over the remote stream.
    pub messages_in: u64,
    /// Reassembled payload bytes sent.
    pub bytes_out: u64,
    /// Reassembled payload bytes received.
    pub bytes_in: u64,
    /// Remote-path errors observed by this endpoint.
    pub errors: u64,
}

/// Per-mode stats aliases. All share [`ItemStats`]; the field meanings
/// specialize by role (see [`ItemStats`]).
pub type ReqStats = ItemStats;
pub type QueStats = ItemStats;
pub type PutStats = ItemStats;
pub type PipStats = ItemStats;

// Internal atomic counters behind the per-mode `.stats()` snapshots.
// `pub` + `#[doc(hidden)]` only because it appears as a field of the
// public `ReqReply`/`AnsReply` reply enums' `Remote` variants; it is
// not part of the documented API.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct ItemStatsInner {
    pub(crate) messages_out: AtomicU64,
    pub(crate) messages_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
    pub(crate) bytes_in: AtomicU64,
    pub(crate) errors: AtomicU64,
}

impl ItemStatsInner {
    pub(crate) fn snapshot(&self) -> ItemStats {
        ItemStats {
            messages_out: self.messages_out.load(Ordering::Relaxed),
            messages_in: self.messages_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_out(&self, bytes: usize) {
        self.messages_out.fetch_add(1, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_in(&self, bytes: usize) {
        self.messages_in.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Mirror publisher kept alive under the hex-EndpointId name when
/// the primary publisher used an identity-name. See `Publisher`.
pub(crate) struct PublisherAlias<T: datapod::DataPod + 'static> {
    pub(crate) publisher: LocalPublisher<T>,
    pub(crate) _service: LocalService<T>,
}

pub struct Publisher<T: datapod::DataPod + 'static> {
    pub(crate) local_publisher: LocalPublisher<T>,
    pub(crate) _local_service: LocalService<T>,
    pub(crate) alias: Option<PublisherAlias<T>>,
    pub(crate) iroh_tx: broadcast::Sender<Vec<u8>>,
    pub(crate) qos: TopicQos,
    pub(crate) published: Arc<AtomicU64>,
    pub(crate) remote_dropped: Arc<AtomicU64>,
    pub(crate) stale_dropped: Arc<AtomicU64>,
    pub(crate) bytes_sent: Arc<AtomicU64>,
    pub(crate) send_errors: Arc<AtomicU64>,
}

impl<T: datapod::DataPod + 'static> Publisher<T> {
    /// Loan a slot with `byte_count` payload bytes. The returned
    /// [`Loan`] exposes a `T::Header` plus a writable `[u8]` slice.
    /// For fixed-Pod `T` pass `0`; for heap-bearing `T` pass the
    /// expected byte length of the cast payload.
    pub fn loan(&mut self, byte_count: usize) -> Result<Loan<T>> {
        self.local_publisher.loan(byte_count)
    }

    pub fn publish(&mut self, loan: Loan<T>) -> Result<u64> {
        // Snapshot header + payload bytes before local publish consumes
        // the loan; needed for the iroh broadcast and (when present)
        // the hex-aliased publisher's mirror loan.
        let header_bytes = bytemuck::bytes_of(loan.header()).to_vec();
        let payload_bytes = loan.payload().to_vec();
        let mut frame = Vec::with_capacity(header_bytes.len() + payload_bytes.len());
        frame.extend_from_slice(&header_bytes);
        frame.extend_from_slice(&payload_bytes);
        if frame.len() > self.qos.max_message_bytes {
            return Err(Error::PayloadTooLarge {
                actual: frame.len(),
                capacity: self.qos.max_message_bytes,
            });
        }
        let seq = self.local_publisher.publish(loan)?;
        if let Some(alias) = self.alias.as_mut() {
            // Best-effort mirror. A failed alias publish should not
            // break the primary path; downgrade to a warning so the
            // operator notices if the hex service falls behind.
            if let Err(e) = mirror_publish::<T>(alias, &header_bytes, &payload_bytes) {
                let _ = &e;
                qb_warn!(
                    target: "peerbus::node",
                    error = %e,
                    "publisher hex-alias mirror failed; DID:KEY subscribers may fall back to iroh"
                );
            }
        }
        if self.iroh_tx.send(frame).is_err() {
            self.remote_dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.published.fetch_add(1, Ordering::Relaxed);
        Ok(seq)
    }

    /// Convenience: build header + bytes from `value` and publish.
    pub fn send(&mut self, value: &T) -> Result<u64> {
        let bytes = value.payload_bytes();
        let mut loan = self.loan(bytes.len())?;
        *loan.header_mut() = value.header();
        loan.payload_mut().copy_from_slice(bytes);
        self.publish(loan)
    }

    /// Snapshot the publisher's lifetime counters.
    pub fn stats(&self) -> PublisherStats {
        PublisherStats {
            published: self.published.load(Ordering::Relaxed),
            remote_dropped: self.remote_dropped.load(Ordering::Relaxed),
            stale_dropped: self.stale_dropped.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
        }
    }
}

pub(crate) fn mirror_publish<T: datapod::DataPod + 'static>(
    alias: &mut PublisherAlias<T>,
    header_bytes: &[u8],
    payload_bytes: &[u8],
) -> Result<()> {
    let mut loan = alias.publisher.loan(payload_bytes.len())?;
    // Copy the header back into the alias slot. `bytemuck::bytes_of_mut`
    // gives us a writeable byte view of the same fixed-size header.
    bytemuck::bytes_of_mut(loan.header_mut()).copy_from_slice(header_bytes);
    loan.payload_mut().copy_from_slice(payload_bytes);
    alias.publisher.publish(loan)?;
    Ok(())
}

// ---- subscriber ----

/// Snapshot of a [`Subscriber`]'s lifetime counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct SubscriberStats {
    /// Samples successfully returned from `take()`.
    pub received: u64,
    /// `Error::Disconnected` events surfaced by `take()`. The
    /// reconnect loop may still be running in the background; this
    /// counter just records how many times the user-visible
    /// receiver observed the channel closing.
    pub disconnects: u64,
    /// Stale samples skipped by freshness policy.
    pub stale_dropped: u64,
    /// Incomplete chunked messages abandoned during reassembly.
    pub incomplete_dropped: u64,
    /// Bytes received from remote transport after reassembly.
    pub bytes_received: u64,
}

pub struct Subscriber<T: datapod::DataPod + 'static> {
    pub(crate) source: SubscriberSource<T>,
    pub(crate) received: Arc<AtomicU64>,
    pub(crate) disconnects: Arc<AtomicU64>,
    pub(crate) stale_dropped: Arc<AtomicU64>,
    pub(crate) incomplete_dropped: Arc<AtomicU64>,
    pub(crate) bytes_received: Arc<AtomicU64>,
}

pub(crate) enum SubscriberSource<T: datapod::DataPod + 'static> {
    Local {
        sub: LocalSubscriber<T>,
        qos: TopicQos,
        _svc: LocalService<T>,
    },
    Remote {
        rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    },
}

impl<T: datapod::DataPod + 'static> Subscriber<T> {
    pub fn take(&mut self) -> Result<Option<NodeSample<T>>> {
        let stale_dropped = self.stale_dropped.clone();
        let result = match &mut self.source {
            SubscriberSource::Local { sub, qos, .. } => {
                take_local_with_qos(sub, *qos, &stale_dropped)
            }
            SubscriberSource::Remote { rx } => match rx.try_recv() {
                Ok(bytes) => {
                    let header_size = std::mem::size_of::<T::Header>();
                    if bytes.len() < header_size {
                        return Err(Error::Remote(format!(
                            "frame too small: got {} bytes, expected at least {} (header)",
                            bytes.len(),
                            header_size,
                        )));
                    }
                    let header: T::Header = *bytemuck::from_bytes(&bytes[..header_size]);
                    let payload = bytes[header_size..].to_vec();
                    Ok(Some(NodeSample::Remote { header, payload }))
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    self.disconnects.fetch_add(1, Ordering::Relaxed);
                    Err(Error::Disconnected)
                }
            },
        };
        if matches!(&result, Ok(Some(_))) {
            self.received.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Block until a sample is available or `timeout` elapses.
    ///
    /// Unlike [`Subscriber::take`], which returns `Ok(None)` the instant
    /// the channel is empty, this polls the non-blocking `take` on a
    /// short interval (50 µs, matching the local req/res call loop)
    /// until a sample arrives or the deadline passes, then returns
    /// `Ok(None)`. A disconnected remote source still surfaces as
    /// `Err(Error::Disconnected)`. `take` semantics are unchanged.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<NodeSample<T>>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(sample) = self.take()? {
                return Ok(Some(sample));
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    /// Snapshot the subscriber's lifetime counters.
    pub fn stats(&self) -> SubscriberStats {
        SubscriberStats {
            received: self.received.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            stale_dropped: self.stale_dropped.load(Ordering::Relaxed),
            incomplete_dropped: self.incomplete_dropped.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

pub(crate) fn take_local_with_qos<T: datapod::DataPod + 'static>(
    sub: &mut LocalSubscriber<T>,
    qos: TopicQos,
    stale_dropped: &AtomicU64,
) -> Result<Option<NodeSample<T>>> {
    if !matches!(
        qos.delivery,
        DeliveryPolicy::Latest | DeliveryPolicy::BestEffort
    ) {
        return match sub.take() {
            Ok(sample) => Ok(sample.map(NodeSample::Local)),
            Err(Error::Lagged { dropped }) => {
                stale_dropped.fetch_add(dropped, Ordering::Relaxed);
                Err(Error::Lagged { dropped })
            }
            Err(err) => Err(err),
        };
    }

    let mut latest = None;
    loop {
        match sub.take() {
            Ok(Some(sample)) => {
                if latest.replace(sample).is_some() {
                    stale_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(None) => return Ok(latest.map(NodeSample::Local)),
            Err(Error::Lagged { dropped }) => {
                stale_dropped.fetch_add(dropped, Ordering::Relaxed);
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Unified sample. Exposes the wire-shape (`header()` + `payload()`)
/// regardless of whether the message arrived via SHM (Local) or
/// iroh (Remote). Reconstructing a full `T` from these is up to the
/// caller — fixed-Pod types just read the header; heap types pair
/// the header with `bytemuck::cast_slice` on the payload.
pub enum NodeSample<T: datapod::DataPod + 'static> {
    Local(Sample<T>),
    Remote { header: T::Header, payload: Vec<u8> },
}

impl<T: datapod::DataPod + 'static> NodeSample<T> {
    pub fn header(&self) -> &T::Header {
        match self {
            NodeSample::Local(s) => s.header(),
            NodeSample::Remote { header, .. } => header,
        }
    }

    pub fn payload(&self) -> &[u8] {
        match self {
            NodeSample::Local(s) => s.payload(),
            NodeSample::Remote { payload, .. } => payload,
        }
    }
}

// ---- req/res ----
