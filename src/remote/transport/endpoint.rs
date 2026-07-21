use super::*;

pub(crate) type ErasedReqHandler =
    Arc<dyn Fn(&[u8]) -> std::result::Result<Vec<u8>, Error> + Send + Sync + 'static>;

/// Per-topic state for a registered request server.
pub(crate) struct RequestServerEntry {
    pub handler: ErasedReqHandler,
    pub req_type_hash: u64,
    pub resp_type_hash: u64,
    pub req_size: u32,
    pub resp_size: u32,
}

/// Default ALPN. Applications wanting protocol isolation can
/// override it via `RemoteTransportBuilder::alpn`.
pub const DEFAULT_ALPN: &[u8] = b"peerbus/1";

/// Channel depth for outgoing broadcast and incoming mpsc.
const CHANNEL_CAPACITY: usize = 256;

/// Bounded timeout for dialing a peer. A stalled dial would otherwise
/// hang a subscriber dispatcher (and any `call`) forever.
pub(crate) const DIAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Bounded timeout for opening a stream and reading the first
/// bytes/handshake off it. A peer that connects but never speaks would
/// otherwise park the accepting server task forever.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounded timeout for awaiting a request/response reply from a peer.
/// A server that accepts the stream but never answers would otherwise
/// hang the calling thread forever.
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

// --- builder ---

/// Builder for [`RemoteTransport`].
pub struct RemoteTransportBuilder {
    name: String,
    alpn: Vec<u8>,
    peer: Option<EndpointAddr>,
    relay_disabled: bool,
}

impl RemoteTransportBuilder {
    /// Override the ALPN. Defaults to `b"peerbus/1"`.
    pub fn alpn(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.alpn = alpn.into();
        self
    }

    /// Dial this peer for subscribers. Without a peer, the transport
    /// only accepts inbound connections and cannot act as the
    /// subscriber side.
    pub fn peer(mut self, peer: impl Into<EndpointAddr>) -> Self {
        self.peer = Some(peer.into());
        self
    }

    /// Disable the relay system (uses `presets::N0DisableRelay`).
    /// Required for loopback tests where no relay is available.
    pub fn no_relay(mut self) -> Self {
        self.relay_disabled = true;
        self
    }

    /// Synchronous build. Spawns the accept loop and (if `.peer()`
    /// was called) eagerly dials the peer so subscriber
    /// construction is non-blocking.
    pub fn build_blocking(self) -> Result<RemoteTransport> {
        let rt = runtime::shared()?;
        let alpn = self.alpn.clone();
        let name = self.name.clone();
        let relay_disabled = self.relay_disabled;
        let peer = self.peer.clone();

        let inner = rt.block_on(async move {
            let mut last_bind_err = None;
            let endpoint = {
                const BIND_ATTEMPTS: usize = 80;
                let mut bound = None;
                for attempt in 0..BIND_ATTEMPTS {
                    let result = if relay_disabled {
                        Endpoint::builder(presets::N0DisableRelay)
                            .alpns(vec![alpn.clone()])
                            .bind()
                            .await
                    } else {
                        Endpoint::builder(presets::N0)
                            .alpns(vec![alpn.clone()])
                            .bind()
                            .await
                    };
                    match result {
                        Ok(endpoint) => {
                            bound = Some(endpoint);
                            break;
                        }
                        Err(err) => {
                            last_bind_err = Some(err);
                            if attempt + 1 < BIND_ATTEMPTS {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                }
                bound.ok_or_else(|| {
                    Error::Remote(format!(
                        "bind: {}",
                        last_bind_err
                            .map(|err| err.to_string())
                            .unwrap_or_else(|| "exhausted bind attempts".to_string())
                    ))
                })?
            };

            let inner = Arc::new(InnerShared {
                name,
                alpn,
                endpoint: endpoint.clone(),
                publisher_topics: Mutex::new(HashMap::new()),
                subscriber_topics: Mutex::new(HashMap::new()),
                request_servers: Mutex::new(HashMap::new()),
                queans_servers: Mutex::new(HashMap::new()),
                putack_servers: Mutex::new(HashMap::new()),
                pip_servers: Mutex::new(HashMap::new()),
                peer,
                peer_conn: AsyncMutex::new(None),
            });

            // Accept loop.
            let accept_handle = {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let _ = run_accept_loop(inner).await;
                })
            };
            Ok::<_, Error>((inner, accept_handle))
        })?;

        let (shared, accept_handle) = inner;
        Ok(RemoteTransport {
            inner: Arc::new(InnerOwned {
                shared,
                accept: Some(accept_handle),
                rt: rt.clone(),
            }),
            rt,
        })
    }
}

// --- transport ---

/// QUIC pub/sub transport over iroh. Cheap to clone (`Arc`-backed).
#[derive(Clone)]
pub struct RemoteTransport {
    inner: Arc<InnerOwned>,
    rt: Arc<Runtime>,
}

/// Owned end of an `Arc<InnerOwned>`: holds the accept task and is
/// dropped when the last transport handle goes away. On `Drop` we
/// abort the accept loop and best-effort close the endpoint —
/// `JoinHandle::drop` only *detaches* the task, which would leak
/// it.
struct InnerOwned {
    shared: Arc<InnerShared>,
    accept: Option<JoinHandle<()>>,
    rt: Arc<Runtime>,
}

impl Drop for InnerOwned {
    fn drop(&mut self) {
        if let Some(handle) = self.accept.take() {
            handle.abort();
        }
        let endpoint = self.shared.endpoint.clone();
        self.rt.spawn(async move {
            endpoint.close().await;
        });
    }
}

/// State shared with the accept loop and subscriber tasks. Owned by
/// `InnerOwned`; cloning is by `Arc`.
pub(crate) struct InnerShared {
    pub(crate) name: String,
    pub(crate) alpn: Vec<u8>,
    pub(crate) endpoint: Endpoint,
    pub(crate) publisher_topics: Mutex<HashMap<String, PublisherTopic>>,
    pub(crate) subscriber_topics: Mutex<HashMap<String, SubscriberTopic>>,
    pub(crate) request_servers: Mutex<HashMap<String, RequestServerEntry>>,
    pub(crate) queans_servers: Mutex<HashMap<String, crate::remote::queans::QueServerEntry>>,
    pub(crate) putack_servers: Mutex<HashMap<String, crate::remote::putack::PutServerEntry>>,
    pub(crate) pip_servers: Mutex<HashMap<String, crate::remote::pip::PipServerEntry>>,
    /// Peer to dial as a subscriber. `None` means subscribe-only role
    /// is unavailable on this transport.
    pub(crate) peer: Option<EndpointAddr>,
    /// Lazily-established outbound connection to `peer`. Held as
    /// `Option<Connection>` so a dead handle (`close_reason()` is
    /// `Some(_)`) can be replaced by a fresh dial on next use.
    pub(crate) peer_conn: AsyncMutex<Option<Connection>>,
}

/// Per-topic broadcast queue for publishers. The accept loop creates
/// a new receiver each time a peer subscribes; the publisher's
/// `publish()` call is one `Sender::send`.
pub(crate) struct PublisherTopic {
    pub(crate) tx: broadcast::Sender<Arc<[u8]>>,
    pub(crate) type_hash: u64,
    pub(crate) payload_size: u32,
}

/// Per-topic broadcast queue for subscribers. The dispatcher task
/// pushes frames it reads off the wire onto `tx`; each subscriber
/// gets its own `Receiver` via `tx.subscribe()`, so a single topic
/// can fan out to multiple subscribers in the same process. A
/// dedicated flag tracks whether the receiver-side dispatcher
/// has been spawned yet (we only want one per topic, regardless of
/// how many subscriber handles the user creates).
pub(crate) struct SubscriberTopic {
    pub(crate) tx: broadcast::Sender<Arc<[u8]>>,
    pub(crate) dispatcher_started: bool,
    pub(crate) type_hash: u64,
    /// Tracked for future use (size validation against the peer's
    /// handshake); not consulted by the current take path.
    #[allow(dead_code)]
    pub(crate) payload_size: u32,
}

impl RemoteTransport {
    pub fn builder(name: impl Into<String>) -> RemoteTransportBuilder {
        RemoteTransportBuilder {
            name: name.into(),
            alpn: DEFAULT_ALPN.to_vec(),
            peer: None,
            relay_disabled: false,
        }
    }

    /// This endpoint's stable identity. Hand this to the other side
    /// so it can dial back.
    pub fn endpoint_id(&self) -> EndpointId {
        self.inner.shared.endpoint.id()
    }

    /// This endpoint's currently-advertised address (id + relay +
    /// direct addresses).
    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.inner.shared.endpoint.addr()
    }

    /// Block until the endpoint has at least one transport address
    /// (direct or relay), or `timeout` elapses. Useful for loopback
    /// tests that want to read the address before the peer dials
    /// in.
    pub fn wait_for_direct_addresses(&self, timeout: std::time::Duration) -> Result<()> {
        let endpoint = self.inner.shared.endpoint.clone();
        let deadline = std::time::Instant::now() + timeout;
        self.rt.block_on(async move {
            while std::time::Instant::now() < deadline {
                if !endpoint.addr().is_empty() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(Error::Timeout(timeout))
        })
    }

    pub fn name(&self) -> &str {
        &self.inner.shared.name
    }

    pub(crate) fn shared(&self) -> &Arc<InnerShared> {
        &self.inner.shared
    }

    pub(crate) fn runtime(&self) -> &Arc<Runtime> {
        &self.rt
    }
}

impl Transport for RemoteTransport {
    type Publisher<T: LocalPayload> = RemotePublisher<T>;
    type Subscriber<T: LocalPayload> = RemoteSubscriber<T>;

    fn publisher<T: LocalPayload>(&self) -> Result<Self::Publisher<T>> {
        let topic = self.inner.shared.name.clone();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;

        let mut topics = crate::trace::recover_poison(
            self.inner.shared.publisher_topics.lock(),
            "RemoteTransport::publisher_topics",
        );
        let entry = topics.entry(topic.clone()).or_insert_with(|| {
            let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
            PublisherTopic {
                tx,
                type_hash,
                payload_size,
            }
        });
        if entry.type_hash != type_hash {
            return Err(Error::TypeMismatch {
                expected: "<existing publisher type>",
                got: type_name::<T>().to_string(),
            });
        }
        Ok(RemotePublisher {
            tx: entry.tx.clone(),
            seq: 0,
            published: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            _phantom: PhantomData,
        })
    }

    fn subscriber<T: LocalPayload>(&self) -> Result<Self::Subscriber<T>> {
        if self.inner.shared.peer.is_none() {
            return Err(Error::invalid_argument(
                "RemoteTransport::subscriber requires a peer; \
                 set one with .peer(endpoint_id) on the builder",
            ));
        }
        let topic = self.inner.shared.name.clone();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;

        // Multi-subscriber: each call creates a fresh broadcast
        // receiver. The dispatcher task (which pumps frames off the
        // wire into the broadcast) is spawned at most once per
        // topic.
        let (receiver, needs_dispatcher) = {
            let mut topics = crate::trace::recover_poison(
                self.inner.shared.subscriber_topics.lock(),
                "RemoteTransport::subscriber_topics",
            );
            let entry = topics.entry(topic.clone()).or_insert_with(|| {
                let (tx, _initial_rx) = broadcast::channel(CHANNEL_CAPACITY);
                SubscriberTopic {
                    tx,
                    dispatcher_started: false,
                    type_hash,
                    payload_size,
                }
            });
            if entry.type_hash != type_hash {
                return Err(Error::TypeMismatch {
                    expected: "<existing subscriber type>",
                    got: type_name::<T>().to_string(),
                });
            }
            let rx = entry.tx.subscribe();
            let needs = !entry.dispatcher_started;
            entry.dispatcher_started = true;
            (rx, needs)
        };

        // Kick off the dispatcher task at most once per topic. It
        // dials the peer (or reuses the existing connection), opens
        // a bi stream, writes the handshake, and pumps frames into
        // the topic's broadcast channel — which fans out to every
        // subscriber on this transport.
        if needs_dispatcher {
            let shared = self.inner.shared.clone();
            let topic_for_task = topic.clone();
            self.rt.spawn(async move {
                run_subscriber(shared, topic_for_task, type_hash, payload_size).await;
            });
        }

        Ok(RemoteSubscriber {
            rx: receiver,
            received: AtomicU64::new(0),
            lagged: AtomicU64::new(0),
            disconnects: AtomicU64::new(0),
            _phantom: PhantomData,
        })
    }
}

