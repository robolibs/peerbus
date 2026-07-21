use super::*;

impl Drop for NodeInner {
    fn drop(&mut self) {
        // Stop accepting new inbound connections.
        if let Some(handle) =
            crate::trace::recover_poison(self.accept_handle.lock(), "Node::accept_handle").take()
        {
            handle.abort();
        }
        // Close the iroh endpoint best-effort. `close()` returns a
        // future; we cannot await it from a sync `Drop`, so we
        // spawn-detach onto the runtime. The runtime is shared
        // across all `Node`s — it outlives this drop.
        //
        // If the endpoint was already closed deterministically via
        // `Node::close`, skip re-closing so we never double-close.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let endpoint = self.endpoint.clone();
        self.rt.spawn(async move {
            endpoint.close().await;
        });
    }
}

// ---- core types ----

#[derive(Clone)]
pub struct Node {
    pub(crate) inner: Arc<NodeInner>,
    pub(crate) rt: Arc<Runtime>,
}

pub(crate) struct NodeInner {
    pub(crate) endpoint: Endpoint,
    pub(crate) endpoint_id: EndpointId,
    /// Optional cosmetic label used only in logs (never in routing).
    pub(crate) label: Option<String>,
    pub(crate) alpn: Vec<u8>,
    pub(crate) local_cfg: LocalConfig,
    pub(crate) publisher_topics: Mutex<HashMap<String, PublisherTopicState>>,
    pub(crate) request_topics: Mutex<HashMap<String, RequestTopicState>>,
    pub(crate) que_topics: Mutex<HashMap<String, QueTopicState>>,
    pub(crate) put_topics: Mutex<HashMap<String, PutTopicState>>,
    pub(crate) pip_topics: Mutex<HashMap<String, PipTopicState>>,
    /// Outbound iroh connections, keyed by peer endpoint id. Each
    /// slot holds the current connection (if any) and the
    /// most-recent dial address hint, so reconnect attempts can
    /// reuse direct addresses learned at first dial.
    pub(crate) peer_connections: Mutex<HashMap<[u8; 32], Arc<AsyncMutex<PeerSlot>>>>,
    /// Best-effort pub/sub datagram sinks, keyed by the publisher
    /// endpoint id plus a deterministic per-subscription session id.
    pub(crate) pubsub_datagram_routes: Mutex<HashMap<DatagramRouteKey, Arc<PubsubDatagramSink>>>,
    /// Peers that already have one connection-level datagram reader
    /// task. QUIC datagrams are connection scoped, so one reader
    /// demultiplexes all best-effort topic sessions for that peer.
    pub(crate) pubsub_datagram_readers: Mutex<HashMap<[u8; 32], usize>>,
    /// Who may open an inbound connection. Enforced in
    /// [`run_accept_loop`] right after the QUIC handshake completes,
    /// before a single bi stream is served — so it covers pub/sub and
    /// all five request modes at once. Defaults to
    /// [`InboundPolicy::DenyAll`].
    pub(crate) inbound_policy: InboundPolicy,
    /// Tokio runtime that drives the accept loop and per-subscriber
    /// tasks. Held so `Drop` can spawn `endpoint.close()` without
    /// reaching for the global singleton.
    pub(crate) rt: Arc<Runtime>,
    /// Accept loop handle; aborted on Drop to stop the inbound
    /// listener cleanly.
    pub(crate) accept_handle: Mutex<Option<JoinHandle<()>>>,
    /// Set once the endpoint has been closed deterministically via
    /// [`Node::close`]. `Drop` checks this so it never issues a second
    /// close on an already-closed endpoint.
    pub(crate) closed: AtomicBool,
}

pub(crate) struct PublisherTopicState {
    pub(crate) iroh_tx: broadcast::Sender<Vec<u8>>,
    pub(crate) type_hash: u64,
    pub(crate) payload_size: u32,
    pub(crate) qos: TopicQos,
    pub(crate) published: Arc<AtomicU64>,
    pub(crate) remote_dropped: Arc<AtomicU64>,
    pub(crate) stale_dropped: Arc<AtomicU64>,
    pub(crate) bytes_sent: Arc<AtomicU64>,
    pub(crate) send_errors: Arc<AtomicU64>,
}

pub(crate) struct RequestTopicState {
    pub(crate) tx: tokio::sync::mpsc::Sender<RemotePendingReq>,
    pub(crate) req_type_hash: u64,
    pub(crate) res_type_hash: u64,
    pub(crate) req_header_size: u32,
    pub(crate) res_header_size: u32,
    pub(crate) qos: TopicQos,
    pub(crate) stats: Arc<ItemStatsInner>,
}

pub(crate) struct QueTopicState {
    pub(crate) tx: tokio::sync::mpsc::Sender<RemotePendingQue>,
    pub(crate) que_type_hash: u64,
    pub(crate) ans_type_hash: u64,
    pub(crate) que_header_size: u32,
    pub(crate) ans_header_size: u32,
    pub(crate) qos: TopicQos,
    pub(crate) stats: Arc<ItemStatsInner>,
}

pub(crate) struct PutTopicState {
    pub(crate) tx: tokio::sync::mpsc::Sender<RemotePendingPuts>,
    pub(crate) put_type_hash: u64,
    pub(crate) ack_type_hash: u64,
    pub(crate) put_header_size: u32,
    pub(crate) ack_header_size: u32,
    pub(crate) qos: TopicQos,
    pub(crate) stats: Arc<ItemStatsInner>,
}

pub(crate) struct PipTopicState {
    pub(crate) tx: tokio::sync::mpsc::Sender<RemotePendingPip>,
    pub(crate) client_type_hash: u64,
    pub(crate) server_type_hash: u64,
    pub(crate) client_header_size: u32,
    pub(crate) server_header_size: u32,
    pub(crate) qos: TopicQos,
    pub(crate) stats: Arc<ItemStatsInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DatagramRouteKey {
    pub(crate) peer: [u8; 32],
    pub(crate) session_id: u64,
}

pub(crate) struct PubsubDatagramSink {
    pub(crate) tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub(crate) reassembler: Mutex<Reassembler>,
    pub(crate) stale_dropped: Arc<AtomicU64>,
    pub(crate) incomplete_dropped: Arc<AtomicU64>,
    pub(crate) bytes_received: Arc<AtomicU64>,
}

/// Per-peer entry in [`NodeInner::peer_connections`]. Tracks the
/// current outbound connection plus the address hint used to
/// establish it, so reconnects after a drop can target the same
/// direct address.
#[derive(Default)]
pub(crate) struct PeerSlot {
    pub(crate) conn: Option<Connection>,
    pub(crate) addr_hint: Option<EndpointAddr>,
}

/// Snapshot of `Node`-level counters returned by [`Node::stats`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NodeStats {
    /// Number of topics this node currently exposes as a publisher.
    pub publisher_topics: usize,
    /// Number of outbound peer connections currently cached. Stale
    /// (closed) entries are evicted on next use, so a value here
    /// includes both healthy and pending-redial slots.
    pub cached_peers: usize,
}

/// Snapshot of one iroh path for an already-cached peer connection.
#[derive(Debug, Clone)]
pub struct PathDiagnostic {
    /// Debug-format iroh path id. Kept as a string so peerbus does
    /// not expose noq's internal path-id representation as API.
    pub path_id: String,
    /// Debug-format remote transport address for this path.
    pub remote_addr: String,
    /// True when iroh currently selected this path for application data.
    pub selected: bool,
    /// True for direct IP paths.
    pub is_ip: bool,
    /// True for relay paths.
    pub is_relay: bool,
    /// Current round-trip time estimate for this path.
    pub rtt: Duration,
    /// Largest UDP payload size iroh currently believes this path supports.
    pub current_mtu: u16,
    /// Current congestion window for this path.
    pub cwnd: u64,
    /// Packets iroh reports lost on this path.
    pub lost_packets: u64,
}

/// Connection-level path diagnostics for one cached peer.
#[derive(Debug, Clone)]
pub struct PeerPathDiagnostics {
    pub peer: EndpointId,
    pub paths: Vec<PathDiagnostic>,
    /// Current maximum QUIC datagram size, when datagrams are available.
    pub max_datagram_size: Option<usize>,
    /// Bytes currently available in iroh's outgoing datagram buffer.
    pub datagram_send_buffer_space: usize,
}

impl Node {
    pub fn builder() -> NodeBuilder {
        NodeBuilder {
            secret: None,
            label: None,
            alpn: DEFAULT_ALPN.to_vec(),
            no_relay: false,
            local_cfg: LocalConfig::default(),
            allowed_peers: None,
            allow_any_peer: false,
        }
    }

    /// The optional cosmetic label set on the builder. Logs only; not
    /// part of any id or rendezvous name.
    pub fn label(&self) -> Option<&str> {
        self.inner.label.as_deref()
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.inner.endpoint_id
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.inner.endpoint.addr()
    }

    /// The topics this node currently hosts, across all modes, with the
    /// payload type-hash the higher-level crate needs to build and
    /// answer a `topic -> (type, id)` directory. Cheap; briefly locks
    /// each per-mode topic map to read it.
    pub fn hosted_topics(&self) -> Vec<HostedTopic> {
        let mut out = Vec::new();
        {
            let map = crate::trace::recover_poison(
                self.inner.publisher_topics.lock(),
                "Node::publisher_topics",
            );
            for (topic, st) in map.iter() {
                out.push(HostedTopic {
                    topic: topic.clone(),
                    mode: TopicMode::PubSub,
                    type_hash: st.type_hash,
                });
            }
        }
        {
            let map = crate::trace::recover_poison(
                self.inner.request_topics.lock(),
                "Node::request_topics",
            );
            for (topic, st) in map.iter() {
                out.push(HostedTopic {
                    topic: topic.clone(),
                    mode: TopicMode::ReqRes,
                    type_hash: st.req_type_hash,
                });
            }
        }
        {
            let map =
                crate::trace::recover_poison(self.inner.que_topics.lock(), "Node::que_topics");
            for (topic, st) in map.iter() {
                out.push(HostedTopic {
                    topic: topic.clone(),
                    mode: TopicMode::QueAns,
                    type_hash: st.que_type_hash,
                });
            }
        }
        {
            let map =
                crate::trace::recover_poison(self.inner.put_topics.lock(), "Node::put_topics");
            for (topic, st) in map.iter() {
                out.push(HostedTopic {
                    topic: topic.clone(),
                    mode: TopicMode::PutAck,
                    type_hash: st.put_type_hash,
                });
            }
        }
        {
            let map =
                crate::trace::recover_poison(self.inner.pip_topics.lock(), "Node::pip_topics");
            for (topic, st) in map.iter() {
                out.push(HostedTopic {
                    topic: topic.clone(),
                    mode: TopicMode::Pip,
                    type_hash: st.client_type_hash,
                });
            }
        }
        out
    }

    /// Snapshot of node-level operational counters. Cheap; takes
    /// the publisher/peer maps' locks briefly to read sizes.
    pub fn stats(&self) -> NodeStats {
        NodeStats {
            publisher_topics: crate::trace::recover_poison(
                self.inner.publisher_topics.lock(),
                "Node::publisher_topics",
            )
            .len(),
            cached_peers: crate::trace::recover_poison(
                self.inner.peer_connections.lock(),
                "Node::peer_connections",
            )
            .len(),
        }
    }

    /// Snapshot path diagnostics for an already-cached outbound peer
    /// connection.
    ///
    /// This does not dial by itself. It returns `Ok(None)` when this
    /// node has not connected to the peer yet, or when the cached
    /// connection is already closed. Callers can use this after a
    /// remote subscriber/client has connected to warn about high-rate
    /// topics running over relay paths, IPv6-only failures, low MTU,
    /// or high RTT.
    pub fn peer_path_diagnostics(
        &self,
        peer: impl IntoPeer,
    ) -> Result<Option<PeerPathDiagnostics>> {
        let peer = peer.into_peer();
        let peer_id = peer.endpoint_id;
        let key = *peer_id.as_bytes();
        let slot = {
            crate::trace::recover_poison(
                self.inner.peer_connections.lock(),
                "Node::peer_connections",
            )
            .get(&key)
            .cloned()
        };
        let Some(slot) = slot else {
            return Ok(None);
        };

        let conn = self.rt.block_on(async move {
            let guard = slot.lock().await;
            guard
                .conn
                .as_ref()
                .filter(|conn| conn.close_reason().is_none())
                .cloned()
        });
        Ok(conn.map(|conn| diagnose_peer_connection(peer_id, &conn)))
    }

    /// Block until the endpoint reports at least one transport
    /// address, or `timeout` elapses. The default of 5 s is enough
    /// for `bind()` to settle on a real machine; loopback usually
    /// reports an address inside one tick.
    pub fn wait_for_direct_addresses(&self, timeout: Duration) -> Result<()> {
        let endpoint = self.inner.endpoint.clone();
        let deadline = std::time::Instant::now() + timeout;
        self.rt.block_on(async move {
            while std::time::Instant::now() < deadline {
                if !endpoint.addr().is_empty() {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(Error::Timeout(timeout))
        })
    }

    /// Deterministically close this node's iroh endpoint and wait for
    /// the close to complete.
    ///
    /// Unlike dropping a `Node` — which spawn-detaches
    /// `endpoint.close()` and returns immediately, so in-flight remote
    /// sends may be lost — `close` blocks the calling thread until the
    /// endpoint has fully shut down. Use it for a graceful shutdown
    /// where outstanding sends must be flushed.
    ///
    /// This consumes the node. Because `Node` is `Clone`, any surviving
    /// clones keep the underlying endpoint alive; the close is still
    /// issued once and is idempotent, so a subsequent `Drop` of the
    /// last clone will not close a second time.
    pub fn close(self) -> Result<()> {
        // Stop accepting new inbound connections first.
        if let Some(handle) = crate::trace::recover_poison(
            self.inner.accept_handle.lock(),
            "Node::accept_handle",
        )
        .take()
        {
            handle.abort();
        }
        // Mark closed before issuing the close so a concurrent/late
        // `Drop` observes the flag and skips its own close.
        self.inner.closed.store(true, Ordering::Release);
        let endpoint = self.inner.endpoint.clone();
        self.rt.block_on(async move {
            endpoint.close().await;
        });
        Ok(())
    }

}

impl Node {
    pub(crate) fn remote_subscriber<T>(
        &self,
        peer_id: EndpointId,
        addr_hint: Option<EndpointAddr>,
        topic_owned: String,
        qos: TopicQos,
    ) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        // Remote path. Do one synchronous dial + handshake so the
        // caller sees a hard failure if the peer is unreachable
        // *at construction time*; after that, the background loop
        // owns reconnect.
        let inner = self.inner.clone();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;
        let stale_dropped = Arc::new(AtomicU64::new(0));
        let incomplete_dropped = Arc::new(AtomicU64::new(0));
        let bytes_received = Arc::new(AtomicU64::new(0));
        let loop_stale_dropped = stale_dropped.clone();
        let loop_incomplete_dropped = incomplete_dropped.clone();
        let loop_bytes_received = bytes_received.clone();
        let register_stale_dropped = stale_dropped.clone();
        let register_incomplete_dropped = incomplete_dropped.clone();
        let register_bytes_received = bytes_received.clone();
        let rx_handle = self.rt.block_on(async move {
            let open = subscribe_once(
                &inner,
                peer_id,
                addr_hint,
                &topic_owned,
                type_hash,
                payload_size,
                qos,
            )
            .await?;

            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(qos.subscriber_queue.max(1));
            let tx_loop = tx.clone();
            let inner_loop = inner.clone();
            let topic_loop = topic_owned.clone();
            maybe_register_pubsub_datagram_route(
                &inner,
                open.conn.clone(),
                peer_id,
                &topic_owned,
                type_hash,
                payload_size,
                qos,
                tx.clone(),
                register_stale_dropped.clone(),
                register_incomplete_dropped.clone(),
                register_bytes_received.clone(),
            );
            tokio::spawn(async move {
                // First iteration: pump the recv stream we already
                // opened on the synchronous dial. Subsequent
                // iterations re-dial with backoff via the loop —
                // the address hint persists in `peer_connections`.
                let _ = pump_recv_stream(
                    open.recv,
                    tx_loop.clone(),
                    qos,
                    loop_incomplete_dropped.clone(),
                    loop_bytes_received.clone(),
                )
                .await;
                run_subscriber_loop(
                    inner_loop,
                    peer_id,
                    topic_loop,
                    type_hash,
                    payload_size,
                    qos,
                    loop_stale_dropped,
                    loop_incomplete_dropped,
                    loop_bytes_received,
                    tx_loop,
                )
                .await;
            });
            Ok::<_, Error>(rx)
        })?;

        Ok(Subscriber {
            source: SubscriberSource::Remote { rx: rx_handle },
            received: Arc::new(AtomicU64::new(0)),
            disconnects: Arc::new(AtomicU64::new(0)),
            stale_dropped,
            incomplete_dropped,
            bytes_received,
        })
    }

    /// The iroh-side topic key. With group/system routing gone this is
    /// just the topic itself; kept as a thin helper so the per-mode
    /// call sites read uniformly.
    pub(crate) fn route_topic(&self, topic: &str) -> Result<String> {
        Ok(topic.to_string())
    }

    /// The shared-memory service name this node serves `topic` under,
    /// derived solely from `(endpoint_id, topic)`.
    pub(crate) fn primary_service_name(&self, topic: &str) -> Result<String> {
        Ok(service_name(self.inner.endpoint_id.as_bytes(), topic))
    }
}

/// One topic this node hosts, as reported by [`Node::hosted_topics`].
#[derive(Debug, Clone)]
pub struct HostedTopic {
    pub topic: String,
    pub mode: TopicMode,
    /// Payload type-hash (`transport::wire_type_hash`). For the
    /// two-type modes it is the client-facing type
    /// (req / que / put / pip-client message).
    pub type_hash: u64,
}

/// Which messaging mode a [`HostedTopic`] is served under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicMode {
    PubSub,
    ReqRes,
    QueAns,
    PutAck,
    Pip,
}

pub(crate) fn diagnose_peer_connection(peer: EndpointId, conn: &Connection) -> PeerPathDiagnostics {
    let paths = conn
        .paths()
        .iter()
        .map(|path| {
            let stats = path.stats();
            PathDiagnostic {
                path_id: format!("{:?}", path.id()),
                remote_addr: format!("{:?}", path.remote_addr()),
                selected: path.is_selected(),
                is_ip: path.is_ip(),
                is_relay: path.is_relay(),
                rtt: stats.rtt,
                current_mtu: stats.current_mtu,
                cwnd: stats.cwnd,
                lost_packets: stats.lost_packets,
            }
        })
        .collect();
    PeerPathDiagnostics {
        peer,
        paths,
        max_datagram_size: conn.max_datagram_size(),
        datagram_send_buffer_space: conn.datagram_send_buffer_space(),
    }
}
