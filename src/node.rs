//! `Node` — the unified, topic-shaped entry point.
//!
//! One `Node` per process. Internally:
//!
//! * Owns a single [`iroh::Endpoint`] (cross-host transport).
//! * Hosts a single accept loop that dispatches incoming iroh bi
//!   streams to registered publishers' broadcast queues, by topic
//!   name.
//! * Caches outbound iroh `Connection`s, one per peer, reused
//!   across topics.
//! * For same-host routing, asks the local SHM backend whether a service
//!   exists for the topic on this host; if so, attaches locally
//!   via SHM. If not, dials the peer via iroh.
//!
//! User-facing API:
//!
//! ```no_run
//! use quicbit::Node;
//!
//! # fn run() -> quicbit::Result<()> {
//! # #[datapod::datapod]
//! # struct Pose { x: f32, y: f32, yaw: f32 }
//! let node = Node::builder().no_relay().bind()?;
//! let mut pubr = node.publisher::<Pose>("rover/pose")?;
//! pubr.send(&Pose { x: 0.0, y: 0.0, yaw: 0.0 })?;
//! # Ok(()) }
//! ```

use std::any::type_name;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::endpoint::{Connection, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::local::{Loan, Sample};
use crate::remote::runtime;
use crate::remote::{HANDSHAKE_MAGIC, HANDSHAKE_VERSION, parse_pubsub_handshake_tail};
use crate::transport::{fnv1a64, wire_type_hash};
use crate::{qb_debug, qb_info, qb_warn};

const DEFAULT_ALPN: &[u8] = b"quicbit/1";
const DEFAULT_BROADCAST_CAPACITY: usize = 256;
const IDENTITY_DERIVATION_TAG: &[u8] = b"quicbit/v1/identity";

/// Initial backoff between reconnect attempts on the subscriber
/// loop. Doubles up to [`RECONNECT_BACKOFF_MAX`].
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Cap on the reconnect backoff between attempts.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(10);

// ---- identity ----

#[derive(Debug, Clone)]
enum IdentitySource {
    Ephemeral,
    Name(String),
    File(PathBuf),
    Env(String),
}

// ---- peer addressing ----

/// A peer's address. We need an iroh `EndpointId` for the remote
/// path. The optional `name` lets us route same-host without any
/// discovery service: both publisher and subscriber name the
/// service the same way → local service open succeeds → SHM.
#[derive(Clone, Debug)]
pub struct Peer {
    pub endpoint_id: EndpointId,
    pub name: Option<String>,
    /// Full transport address. When set, used as the dial target;
    /// otherwise the node falls back to `EndpointAddr::new(id)`,
    /// which requires DNS / relay discovery.
    pub addr: Option<EndpointAddr>,
}

/// `&str` / `String` hashes to the same deterministic `EndpointId`
/// as [`NodeBuilder::identity`]. `EndpointId` and `EndpointAddr`
/// pass through.
pub trait IntoPeer {
    fn into_peer(self) -> Peer;
}

impl IntoPeer for Peer {
    fn into_peer(self) -> Peer {
        self
    }
}

impl IntoPeer for EndpointId {
    fn into_peer(self) -> Peer {
        Peer {
            endpoint_id: self,
            name: None,
            addr: None,
        }
    }
}

impl IntoPeer for EndpointAddr {
    fn into_peer(self) -> Peer {
        Peer {
            endpoint_id: self.id,
            name: None,
            addr: Some(self),
        }
    }
}

impl IntoPeer for &str {
    fn into_peer(self) -> Peer {
        // `did:key:z…` strings carry the literal ed25519 public key
        // (no derivation) and are routed locally by EndpointId
        // rather than identity name. Bare strings still hash to a
        // deterministic key by name for the trusted-LAN path.
        if crate::did_key::looks_like_did_key(self)
            && let Ok(id) = crate::did_key::did_key_to_endpoint_id(self)
        {
            return Peer {
                endpoint_id: id,
                name: None,
                addr: None,
            };
        }
        let secret = derive_secret_from_name(self);
        Peer {
            endpoint_id: secret.public(),
            name: Some(self.to_string()),
            addr: None,
        }
    }
}

impl IntoPeer for String {
    fn into_peer(self) -> Peer {
        if crate::did_key::looks_like_did_key(&self)
            && let Ok(id) = crate::did_key::did_key_to_endpoint_id(&self)
        {
            return Peer {
                endpoint_id: id,
                name: None,
                addr: None,
            };
        }
        let secret = derive_secret_from_name(&self);
        Peer {
            endpoint_id: secret.public(),
            name: Some(self),
            addr: None,
        }
    }
}

// ---- builder ----

pub struct NodeBuilder {
    identity: IdentitySource,
    system_did: Option<String>,
    alpn: Vec<u8>,
    no_relay: bool,
    local_cfg: LocalConfig,
    /// Allowlist of peers permitted to open inbound streams. `None`
    /// means "accept any peer" (back-compat default). `Some(set)`
    /// rejects every connection whose remote endpoint id is not in
    /// the set.
    allowed_peers: Option<std::collections::HashSet<[u8; 32]>>,
}

impl NodeBuilder {
    /// Join a logical multi-process system namespace identified by
    /// a `did:key:z...` URI. In system mode, high-level
    /// [`Node::publisher`] / [`Node::subscribe`] routes are keyed by
    /// `(system_did, topic)` instead of this process' transport id.
    pub fn system_did(mut self, did: impl Into<String>) -> Self {
        self.system_did = Some(did.into());
        self
    }

    /// Literal name → deterministic `SecretKey` via blake3.
    /// Same name on two machines → same `EndpointId`. Anyone with
    /// the string can impersonate — use only in trusted contexts.
    pub fn identity(mut self, name: impl Into<String>) -> Self {
        self.identity = IdentitySource::Name(name.into());
        self
    }

    /// Read 32 raw bytes as the `SecretKey`; generate + write on
    /// first run. Cryptographically meaningful; this is the
    /// production knob.
    pub fn identity_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.identity = IdentitySource::File(path.into());
        self
    }

    /// Read the env var `var` and feed its value through
    /// [`Self::identity`].
    pub fn identity_env(mut self, var: impl Into<String>) -> Self {
        self.identity = IdentitySource::Env(var.into());
        self
    }

    pub fn alpn(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.alpn = alpn.into();
        self
    }

    /// Add `peer` to the inbound allowlist. The first call switches
    /// the node from "accept any peer" (default) to "accept only
    /// allowlisted peers"; subsequent calls extend the list. Peers
    /// dial-out *from* this node (`subscriber(...)`) are not
    /// affected — only inbound accepts.
    pub fn allow_peer(mut self, peer: impl IntoPeer) -> Self {
        let p = peer.into_peer();
        self.allowed_peers
            .get_or_insert_with(std::collections::HashSet::new)
            .insert(*p.endpoint_id.as_bytes());
        self
    }

    /// Add many peers to the inbound allowlist at once.
    pub fn allow_peers<I, P>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: IntoPeer,
    {
        let set = self
            .allowed_peers
            .get_or_insert_with(std::collections::HashSet::new);
        for p in peers {
            set.insert(*p.into_peer().endpoint_id.as_bytes());
        }
        self
    }

    pub fn no_relay(mut self) -> Self {
        self.no_relay = true;
        self
    }

    /// Tune the default local SHM QoS used for publishers / subscribers
    /// this node creates. Per-`publisher`/`subscriber` overrides are
    /// not exposed yet.
    pub fn local_config(mut self, cfg: LocalConfig) -> Self {
        self.local_cfg = cfg;
        self
    }

    pub fn bind(self) -> Result<Node> {
        let rt = runtime::shared()?;
        let (secret, identity_name) = resolve_identity(&self.identity)?;
        let system_did = self
            .system_did
            .map(|did| validate_system_did(&did).map(|_| did))
            .transpose()?;
        let endpoint_id = secret.public();
        let secret_bytes = secret.to_bytes();
        let alpn = self.alpn.clone();
        let no_relay = self.no_relay;

        let mut last_bind_err = None;
        let endpoint: Endpoint = 'bind: loop {
            const BIND_ATTEMPTS: usize = 80;
            for attempt in 0..BIND_ATTEMPTS {
                let secret = SecretKey::from_bytes(&secret_bytes);
                let alpn = alpn.clone();
                let bind_result = rt.block_on(async move {
                    let builder = if no_relay {
                        Endpoint::builder(presets::N0DisableRelay)
                    } else {
                        Endpoint::builder(presets::N0)
                    };
                    builder
                        .secret_key(secret)
                        .alpns(vec![alpn])
                        .bind()
                        .await
                        .map_err(|e| Error::Remote(format!("Node::bind: {e}")))
                });
                match bind_result {
                    Ok(endpoint) => break 'bind endpoint,
                    Err(err) => {
                        last_bind_err = Some(err);
                        if attempt + 1 < BIND_ATTEMPTS {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            }
            return Err(last_bind_err.expect("bind loop always records an error"));
        };

        qb_info!(
            target: "quicbit::node",
            endpoint_id = %endpoint_id,
            identity = identity_name.as_deref().unwrap_or("<ephemeral>"),
            no_relay,
            "node bound"
        );

        let inner = Arc::new(NodeInner {
            endpoint,
            endpoint_id,
            identity_name,
            system_did,
            alpn: self.alpn,
            local_cfg: self.local_cfg,
            publisher_topics: Mutex::new(HashMap::new()),
            system_routes: Mutex::new(HashMap::new()),
            system_peers: Mutex::new(Vec::new()),
            peer_connections: Mutex::new(HashMap::new()),
            allowed_peers: self.allowed_peers,
            rt: rt.clone(),
            accept_handle: Mutex::new(None),
        });

        let accept_handle = {
            let inner = inner.clone();
            rt.spawn(async move {
                let _ = run_accept_loop(inner).await;
            })
        };
        *crate::trace::recover_poison(inner.accept_handle.lock(), "Node::accept_handle") =
            Some(accept_handle);

        Ok(Node { inner, rt })
    }
}

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
        let endpoint = self.endpoint.clone();
        self.rt.spawn(async move {
            endpoint.close().await;
        });
    }
}

// ---- core types ----

#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
    rt: Arc<Runtime>,
}

struct NodeInner {
    endpoint: Endpoint,
    endpoint_id: EndpointId,
    identity_name: Option<String>,
    system_did: Option<String>,
    alpn: Vec<u8>,
    local_cfg: LocalConfig,
    publisher_topics: Mutex<HashMap<String, PublisherTopicState>>,
    system_routes: Mutex<HashMap<String, EndpointAddr>>,
    /// Explicit remote peers that participate in this node's configured
    /// system DID. Used as a topic-agnostic fallback after local SHM and
    /// per-topic routes.
    system_peers: Mutex<Vec<EndpointAddr>>,
    /// Outbound iroh connections, keyed by peer endpoint id. Each
    /// slot holds the current connection (if any) and the
    /// most-recent dial address hint, so reconnect attempts can
    /// reuse direct addresses learned at first dial.
    peer_connections: Mutex<HashMap<[u8; 32], Arc<AsyncMutex<PeerSlot>>>>,
    /// If `Some`, only inbound connections from these peers are
    /// served; everything else is dropped immediately after the
    /// QUIC handshake completes.
    allowed_peers: Option<std::collections::HashSet<[u8; 32]>>,
    /// Tokio runtime that drives the accept loop and per-subscriber
    /// tasks. Held so `Drop` can spawn `endpoint.close()` without
    /// reaching for the global singleton.
    rt: Arc<Runtime>,
    /// Accept loop handle; aborted on Drop to stop the inbound
    /// listener cleanly.
    accept_handle: Mutex<Option<JoinHandle<()>>>,
}

struct PublisherTopicState {
    iroh_tx: broadcast::Sender<Vec<u8>>,
    type_hash: u64,
    payload_size: u32,
}

/// Per-peer entry in [`NodeInner::peer_connections`]. Tracks the
/// current outbound connection plus the address hint used to
/// establish it, so reconnects after a drop can target the same
/// direct address.
#[derive(Default)]
struct PeerSlot {
    conn: Option<Connection>,
    addr_hint: Option<EndpointAddr>,
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

impl Node {
    pub fn builder() -> NodeBuilder {
        NodeBuilder {
            identity: IdentitySource::Ephemeral,
            system_did: None,
            alpn: DEFAULT_ALPN.to_vec(),
            no_relay: false,
            local_cfg: LocalConfig::default(),
            allowed_peers: None,
        }
    }

    pub fn identity_name(&self) -> Option<&str> {
        self.inner.identity_name.as_deref()
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.inner.endpoint_id
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.inner.endpoint.addr()
    }

    /// This node's `EndpointId` formatted as a W3C `did:key:z6Mk…`
    /// URI. The same 32-byte ed25519 public key, just wrapped in
    /// the DID multibase encoding so it can be exchanged with
    /// DID-aware tooling.
    pub fn endpoint_did_key(&self) -> String {
        crate::did_key::endpoint_id_to_did_key(&self.inner.endpoint_id)
    }

    /// The logical system namespace this node joined, if any.
    ///
    /// In system mode, high-level publishers and subscribers route by
    /// `(system_did, topic)` so multiple processes can contribute to
    /// one machine/system bus without sharing a process identity.
    pub fn system_did(&self) -> Option<&str> {
        self.inner.system_did.as_deref()
    }

    /// Add an explicit remote route for this node's configured system:
    /// `(system_did, topic) -> endpoint address`.
    ///
    /// Local SHM is always tried first by [`subscribe`](Self::subscribe).
    /// This route is the first simple remote-discovery hook for when
    /// the topic is not present on this host.
    pub fn add_topic_route(&self, topic: &str, endpoint: EndpointAddr) -> Result<()> {
        validate_topic(topic)?;
        let route = self.system_route_topic(topic)?;
        crate::trace::recover_poison(self.inner.system_routes.lock(), "Node::system_routes")
            .insert(route, endpoint);
        Ok(())
    }

    /// Add a remote peer that participates in this node's configured
    /// system DID, independent of a particular topic key.
    ///
    /// [`subscribe`](Self::subscribe) still tries local SHM first and a
    /// per-topic route second; if neither exists, it dials these peers
    /// using the high-level `(system_did, topic)` route topic.
    pub fn add_system_peer(&self, endpoint: EndpointAddr) -> Result<()> {
        // Validate that the node is in system mode. The returned route is not
        // needed here; the check keeps misuse symmetric with add_topic_route.
        let _ = self.system_route_topic("__peer__")?;
        let mut peers =
            crate::trace::recover_poison(self.inner.system_peers.lock(), "Node::system_peers");
        if !peers.iter().any(|existing| existing.id == endpoint.id) {
            peers.push(endpoint);
        }
        Ok(())
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

    /// Build a publisher for `topic`. Writes go to both:
    /// * local SHM service named `<identity>__<topic>` (same-host
    ///   subscribers attach to this and read zero-copy);
    /// * a broadcast queue feeding the iroh accept loop, which
    ///   serves attached remote subscribers.
    pub fn publisher<T>(&self, topic: &str) -> Result<Publisher<T>>
    where
        T: datapod::DataPod + 'static,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let type_name = type_name::<T>();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;

        let iroh_tx = {
            let mut map = crate::trace::recover_poison(
                self.inner.publisher_topics.lock(),
                "Node::publisher_topics",
            );
            let entry = map.entry(route_topic.clone()).or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
                PublisherTopicState {
                    iroh_tx: tx,
                    type_hash,
                    payload_size,
                }
            });
            if entry.type_hash != type_hash || entry.payload_size != payload_size {
                return Err(Error::TypeMismatch {
                    expected: "<existing publisher type>",
                    got: type_name.to_string(),
                });
            }
            entry.iroh_tx.clone()
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
            published: Arc::new(AtomicU64::new(0)),
            remote_dropped: Arc::new(AtomicU64::new(0)),
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
                    _svc: svc,
                },
                received: AtomicU64::new(0),
                disconnects: AtomicU64::new(0),
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

        self.remote_subscriber::<T>(endpoint.id, Some(endpoint), route_topic)
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
                        _svc: svc,
                    },
                    received: AtomicU64::new(0),
                    disconnects: AtomicU64::new(0),
                });
            }
        }

        self.remote_subscriber::<T>(peer.endpoint_id, peer.addr.clone(), topic.to_string())
    }

    fn remote_subscriber<T>(
        &self,
        peer_id: EndpointId,
        addr_hint: Option<EndpointAddr>,
        topic_owned: String,
    ) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
    {
        // Remote path. Do one synchronous dial + handshake so the
        // caller sees a hard failure if the peer is unreachable
        // *at construction time*; after that, the background loop
        // owns reconnect.
        let inner = self.inner.clone();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;
        let rx_handle = self.rt.block_on(async move {
            let recv = subscribe_once(
                &inner,
                peer_id,
                addr_hint,
                &topic_owned,
                type_hash,
                payload_size,
            )
            .await?;

            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(DEFAULT_BROADCAST_CAPACITY);
            let tx_loop = tx.clone();
            let inner_loop = inner.clone();
            let topic_loop = topic_owned.clone();
            tokio::spawn(async move {
                // First iteration: pump the recv stream we already
                // opened on the synchronous dial. Subsequent
                // iterations re-dial with backoff via the loop —
                // the address hint persists in `peer_connections`.
                let _ = pump_recv_stream(recv, tx_loop.clone()).await;
                run_subscriber_loop(
                    inner_loop,
                    peer_id,
                    topic_loop,
                    type_hash,
                    payload_size,
                    tx_loop,
                )
                .await;
            });
            Ok::<_, Error>(rx)
        })?;

        Ok(Subscriber {
            source: SubscriberSource::Remote { rx: rx_handle },
            received: AtomicU64::new(0),
            disconnects: AtomicU64::new(0),
        })
    }

    fn route_topic(&self, topic: &str) -> Result<String> {
        match self.inner.system_did.as_deref() {
            Some(system_did) => Ok(system_route_topic(system_did, topic)),
            None => Ok(topic.to_string()),
        }
    }

    fn system_route_topic(&self, topic: &str) -> Result<String> {
        let system_did = self.inner.system_did.as_deref().ok_or_else(|| {
            Error::invalid_argument(
                "system_did is required for system topic subscribe/add_topic_route",
            )
        })?;
        Ok(system_route_topic(system_did, topic))
    }

    fn primary_service_name(&self, topic: &str) -> Result<String> {
        match self.inner.system_did.as_deref() {
            Some(system_did) => Ok(system_service_name(system_did, topic)),
            None => Ok(service_name(
                self.inner.identity_name.as_deref(),
                self.inner.endpoint_id.as_bytes(),
                topic,
            )),
        }
    }
}

/// Single dial + handshake attempt. Returns the publisher's
/// `RecvStream` ready for [`pump_recv_stream`].
async fn subscribe_once(
    inner: &Arc<NodeInner>,
    peer_id: EndpointId,
    addr_hint: Option<EndpointAddr>,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
) -> Result<iroh::endpoint::RecvStream> {
    let conn = ensure_peer_connection(inner, peer_id, addr_hint).await?;
    let (mut send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
    write_topic_handshake(&mut send, topic, type_hash, payload_size).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
    Ok(recv)
}

/// Reconnect loop. Repeatedly redials and re-issues the handshake
/// when the wire side disconnects. Exits cleanly when the
/// subscriber drops its `mpsc::Receiver` (detected via
/// `tx.is_closed()`).
#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn run_subscriber_loop(
    inner: Arc<NodeInner>,
    peer_id: EndpointId,
    topic: String,
    type_hash: u64,
    payload_size: u32,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
) {
    let mut backoff = RECONNECT_BACKOFF_MIN;
    loop {
        if tx.is_closed() {
            qb_debug!(
                target: "quicbit::node",
                topic = %topic,
                "subscriber receiver dropped, exiting reconnect loop"
            );
            return;
        }
        // Reconnect uses the addr_hint cached in `peer_connections`
        // by the initial dial — pass `None` so we don't override
        // it.
        match subscribe_once(&inner, peer_id, None, &topic, type_hash, payload_size).await {
            Ok(recv) => {
                qb_info!(
                    target: "quicbit::node",
                    topic = %topic,
                    peer = %peer_id,
                    "subscriber stream re-established"
                );
                backoff = RECONNECT_BACKOFF_MIN;
                let _ = pump_recv_stream(recv, tx.clone()).await;
                qb_warn!(
                    target: "quicbit::node",
                    topic = %topic,
                    peer = %peer_id,
                    "subscriber stream ended, will reconnect"
                );
            }
            Err(e) => {
                qb_debug!(
                    target: "quicbit::node",
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
}

/// Mirror publisher kept alive under the hex-EndpointId name when
/// the primary publisher used an identity-name. See `Publisher`.
struct PublisherAlias<T: datapod::DataPod + 'static> {
    publisher: LocalPublisher<T>,
    _service: LocalService<T>,
}

pub struct Publisher<T: datapod::DataPod + 'static> {
    local_publisher: LocalPublisher<T>,
    _local_service: LocalService<T>,
    alias: Option<PublisherAlias<T>>,
    iroh_tx: broadcast::Sender<Vec<u8>>,
    published: Arc<AtomicU64>,
    remote_dropped: Arc<AtomicU64>,
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
        let seq = self.local_publisher.publish(loan)?;
        if let Some(alias) = self.alias.as_mut() {
            // Best-effort mirror. A failed alias publish should not
            // break the primary path; downgrade to a warning so the
            // operator notices if the hex service falls behind.
            if let Err(e) = mirror_publish::<T>(alias, &header_bytes, &payload_bytes) {
                let _ = &e;
                qb_warn!(
                    target: "quicbit::node",
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
        }
    }
}

fn mirror_publish<T: datapod::DataPod + 'static>(
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
}

pub struct Subscriber<T: datapod::DataPod + 'static> {
    source: SubscriberSource<T>,
    received: AtomicU64,
    disconnects: AtomicU64,
}

enum SubscriberSource<T: datapod::DataPod + 'static> {
    Local {
        sub: LocalSubscriber<T>,
        _svc: LocalService<T>,
    },
    Remote {
        rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    },
}

impl<T: datapod::DataPod + 'static> Subscriber<T> {
    pub fn take(&mut self) -> Result<Option<NodeSample<T>>> {
        let result = match &mut self.source {
            SubscriberSource::Local { sub, .. } => Ok(sub.take()?.map(NodeSample::Local)),
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

    /// Snapshot the subscriber's lifetime counters.
    pub fn stats(&self) -> SubscriberStats {
        SubscriberStats {
            received: self.received.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
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

// ---- iroh accept side ----

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn run_accept_loop(inner: Arc<NodeInner>) -> Result<()> {
    qb_debug!(target: "quicbit::node", "accept loop started");
    while let Some(incoming) = inner.endpoint.accept().await {
        let inner = inner.clone();
        tokio::spawn(async move {
            let mut accepting = match incoming.accept() {
                Ok(a) => a,
                Err(e) => {
                    qb_warn!(target: "quicbit::node", error = %e, "incoming.accept failed");
                    return;
                }
            };
            let _alpn = match accepting.alpn().await {
                Ok(a) => a,
                Err(e) => {
                    qb_warn!(target: "quicbit::node", error = %e, "alpn negotiation failed");
                    return;
                }
            };
            let conn = match accepting.await {
                Ok(c) => c,
                Err(e) => {
                    qb_warn!(target: "quicbit::node", error = %e, "connection handshake failed");
                    return;
                }
            };
            let remote = conn.remote_id();
            if let Some(allow) = inner.allowed_peers.as_ref() {
                if !allow.contains(remote.as_bytes()) {
                    qb_warn!(
                        target: "quicbit::node",
                        remote = %remote,
                        "rejecting connection: peer not in allowlist"
                    );
                    conn.close(0u32.into(), b"peer not allowed");
                    return;
                }
            }
            qb_debug!(
                target: "quicbit::node",
                remote = %remote,
                "accepted connection"
            );
            let _ = serve_incoming_connection(inner, conn).await;
        });
    }
    qb_debug!(target: "quicbit::node", "accept loop exiting");
    Ok(())
}

async fn serve_incoming_connection(inner: Arc<NodeInner>, conn: Connection) -> Result<()> {
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let _ = serve_bi(inner, send, recv).await;
                });
            }
            Err(_) => return Ok(()),
        }
    }
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn serve_bi(
    inner: Arc<NodeInner>,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let (topic, type_hash, payload_size) = read_topic_handshake(&mut recv).await?;
    qb_debug!(
        target: "quicbit::node",
        topic = %topic,
        type_hash = format_args!("0x{type_hash:x}"),
        payload_size,
        "subscriber handshake received"
    );
    let mut rx = {
        let map =
            crate::trace::recover_poison(inner.publisher_topics.lock(), "Node::publisher_topics");
        match map.get(&topic) {
            Some(state) => {
                if state.type_hash != type_hash || state.payload_size != payload_size {
                    qb_warn!(
                        target: "quicbit::node",
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
                state.iroh_tx.subscribe()
            }
            None => {
                qb_debug!(
                    target: "quicbit::node",
                    topic = %topic,
                    "no local publisher for requested topic"
                );
                return Ok(());
            }
        }
    };

    loop {
        match rx.recv().await {
            Ok(bytes) => {
                if write_frame(&mut send, &bytes).await.is_err() {
                    return Ok(());
                }
            }
            Err(broadcast::error::RecvError::Closed) => {
                let _ = send.finish();
                return Ok(());
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                qb_warn!(
                    target: "quicbit::node",
                    topic = %topic,
                    dropped = n,
                    "broadcast lagged on serve path"
                );
                continue;
            }
        }
    }
}

async fn pump_recv_stream(
    mut recv: iroh::endpoint::RecvStream,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if recv.read_exact(&mut len_buf).await.is_err() {
            return Ok(());
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        const NODE_MAX_FRAME: usize = 16 * 1024 * 1024;
        if len > NODE_MAX_FRAME {
            return Err(Error::FrameTooLarge {
                actual: len as u64,
                limit: NODE_MAX_FRAME as u64,
            });
        }
        let mut buf = vec![0u8; len];
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| Error::Remote(format!("frame body: {e}")))?;
        if tx.send(buf).await.is_err() {
            return Ok(());
        }
    }
}

// ---- iroh wire helpers ----

async fn write_topic_handshake(
    send: &mut iroh::endpoint::SendStream,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
) -> Result<()> {
    const NODE_TOPIC_LEN_LIMIT: usize = 1024;
    let bytes = topic.as_bytes();
    if bytes.len() > NODE_TOPIC_LEN_LIMIT {
        return Err(Error::TopicNameTooLong {
            len: bytes.len(),
            limit: NODE_TOPIC_LEN_LIMIT,
        });
    }
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 4 + 2 + bytes.len());
    buf.extend_from_slice(&HANDSHAKE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&type_hash.to_le_bytes());
    buf.extend_from_slice(&payload_size.to_le_bytes());
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("topic handshake write: {e}")))?;
    Ok(())
}

async fn read_topic_handshake(recv: &mut iroh::endpoint::RecvStream) -> Result<(String, u64, u32)> {
    const NODE_TOPIC_LEN_LIMIT: usize = 1024;
    let mut magic = [0u8; 4];
    recv.read_exact(&mut magic)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("handshake magic: {e}")))?;
    let magic = u32::from_le_bytes(magic);
    if magic != HANDSHAKE_MAGIC {
        return Err(Error::HandshakeMalformed(format!(
            "unknown pub/sub stream magic 0x{magic:x}"
        )));
    }

    let mut fixed_tail = [0u8; 4 + 8 + 4 + 2];
    recv.read_exact(&mut fixed_tail)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("handshake tail: {e}")))?;
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
    parse_pubsub_handshake_tail(&tail)
}

async fn write_frame(send: &mut iroh::endpoint::SendStream, payload: &[u8]) -> Result<()> {
    const NODE_MAX_FRAME: usize = 16 * 1024 * 1024;
    if payload.len() > NODE_MAX_FRAME {
        return Err(Error::PayloadTooLarge {
            actual: payload.len(),
            capacity: NODE_MAX_FRAME,
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
async fn ensure_peer_connection(
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
            target: "quicbit::node",
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
        target: "quicbit::node",
        peer = %peer,
        "dialing peer"
    );
    let conn = inner
        .endpoint
        .connect(addr, &inner.alpn)
        .await
        .map_err(|e| {
            qb_warn!(
                target: "quicbit::node",
                peer = %peer,
                error = %e,
                "dial failed"
            );
            Error::ConnectFailed(format!("{e}"))
        })?;
    qb_info!(
        target: "quicbit::node",
        peer = %peer,
        "connected to peer"
    );
    guard.conn = Some(conn.clone());
    Ok(conn)
}

// ---- identity resolution ----

fn derive_secret_from_name(name: &str) -> SecretKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(IDENTITY_DERIVATION_TAG);
    hasher.update(b"\0");
    hasher.update(name.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(hasher.finalize().as_bytes());
    SecretKey::from_bytes(&out)
}

fn resolve_identity(src: &IdentitySource) -> Result<(SecretKey, Option<String>)> {
    match src {
        IdentitySource::Ephemeral => Ok((SecretKey::generate(), None)),
        IdentitySource::Name(name) => Ok((derive_secret_from_name(name), Some(name.clone()))),
        IdentitySource::Env(var) => {
            let name = std::env::var(var).map_err(|_| {
                Error::invalid_argument(format!("identity_env: env var '{var}' is not set"))
            })?;
            if name.is_empty() {
                return Err(Error::invalid_argument(format!(
                    "identity_env: '{var}' is empty"
                )));
            }
            Ok((derive_secret_from_name(&name), Some(name)))
        }
        IdentitySource::File(path) => {
            let key = load_or_generate_key(path)?;
            Ok((key, None))
        }
    }
}

fn load_or_generate_key(path: &Path) -> Result<SecretKey> {
    if path.exists() {
        let mut buf = [0u8; 32];
        let mut f =
            fs::File::open(path).map_err(|e| Error::Other(format!("open key file: {e}")))?;
        f.read_exact(&mut buf)
            .map_err(|e| Error::Other(format!("read key file: {e}")))?;
        // Re-enforce 0600 on every read. If an operator copied the
        // file without preserving mode, the next bind corrects it
        // and logs a warning. Failures (mounted read-only, foreign
        // FS) downgrade to a warn so the bind doesn't refuse on
        // pre-existing keys.
        enforce_key_perms(path);
        Ok(SecretKey::from_bytes(&buf))
    } else {
        let key = SecretKey::generate();
        let bytes: [u8; 32] = key.to_bytes();
        let mut f =
            fs::File::create(path).map_err(|e| Error::Other(format!("create key file: {e}")))?;
        f.write_all(&bytes)
            .map_err(|e| Error::Other(format!("write key file: {e}")))?;
        enforce_key_perms(path);
        Ok(key)
    }
}

#[cfg(unix)]
fn enforce_key_perms(path: &Path) {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let needs_chmod = match fs::metadata(path) {
        Ok(meta) => (meta.mode() & 0o777) != 0o600,
        Err(_) => true,
    };
    if needs_chmod {
        if let Err(e) = fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            let _ = &e;
            qb_warn!(
                target: "quicbit::node",
                path = %path.display(),
                error = %e,
                "could not enforce 0600 on identity key file; check permissions manually"
            );
        }
    }
}

#[cfg(not(unix))]
fn enforce_key_perms(_path: &Path) {}

// ---- service-name composition ----

/// Local service name = `<identity_name|hex_endpoint_id>__<sanitised_topic>`.
/// The SHM backend hashes this to an OS-safe id; we still sanitise spaces and a
/// few oddities so the logical name remains friendly in logs/tests.
pub fn service_name(identity_name: Option<&str>, endpoint_id: &[u8; 32], topic: &str) -> String {
    let topic = sanitise(topic);
    match identity_name {
        Some(name) => format!("{}__{topic}", sanitise(name)),
        None => {
            let mut hex = String::with_capacity(64);
            for b in endpoint_id.iter() {
                let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{b:02x}"));
            }
            format!("{hex}__{topic}")
        }
    }
}

fn sanitise(s: &str) -> String {
    s.replace([' '], "_")
}

fn validate_system_did(did: &str) -> Result<()> {
    if !crate::did_key::looks_like_did_key(did) {
        return Err(Error::invalid_argument(format!(
            "system_did must be did:key:z..., got '{did}'"
        )));
    }
    crate::did_key::did_key_to_endpoint_id(did)?;
    Ok(())
}

fn system_route_topic(system_did: &str, topic: &str) -> String {
    format!("{system_did}::{topic}")
}

fn system_service_name(system_did: &str, topic: &str) -> String {
    format!(
        "sys_{:016x}",
        fnv1a64(&system_route_topic(system_did, topic))
    )
}

/// Maximum byte length for a topic name. The local SHM backend uses the same
/// cap for direct services and for composed `<identity>__<topic>` names.
pub const MAX_TOPIC_BYTES: usize = 200;

/// Validate a user-supplied topic string. Allowed characters:
/// `A-Z`, `a-z`, `0-9`, and `._/-`. Empty strings and overlong strings
/// are also rejected.
pub(crate) fn validate_topic(topic: &str) -> Result<()> {
    if topic.is_empty() {
        return Err(Error::invalid_argument("topic name must not be empty"));
    }
    if topic.len() > MAX_TOPIC_BYTES {
        return Err(Error::invalid_argument(format!(
            "topic name '{topic}' is {} bytes; cap is {MAX_TOPIC_BYTES}",
            topic.len()
        )));
    }
    for c in topic.chars() {
        let ok = c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-');
        if !ok {
            return Err(Error::invalid_argument(format!(
                "topic name '{topic}' contains invalid character {c:?}; \
                 allowed: A-Z a-z 0-9 . _ / -"
            )));
        }
    }
    Ok(())
}
