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
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use iroh::endpoint::{Connection, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use tokio::task::JoinHandle;

use crate::chunk::{
    CHUNK_FRAME_FLAG, CHUNK_FRAME_LEN_MASK, CHUNK_HEADER_LEN, Reassembler, make_chunk_payload,
    parse_chunk_payload,
};
use crate::error::{Error, Result};
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::local::{Loan, Sample};
use crate::local::{
    LocalAckServer, LocalAnsServer, LocalPipServer, LocalPipService, LocalPutAckService,
    LocalQueAnsService, LocalReqResService, LocalReqServer,
};
use crate::pip::{PIP_KIND_DONE, PIP_KIND_ITEM};
use crate::putack::{PUT_KIND_DONE, PUT_KIND_ITEM};
use crate::qos::{DeliveryPolicy, TopicQos};
use crate::queans::{ANS_KIND_DONE, ANS_KIND_ITEM};
use crate::remote::runtime;
use crate::remote::{
    HANDSHAKE_MAGIC, HANDSHAKE_VERSION, ITEM_HANDSHAKE_VERSION_CHUNKED, MAX_PAYLOAD_LEN, PIP_MAGIC,
    PUBSUB_HANDSHAKE_VERSION_QOS, PUTACK_MAGIC, QUEANS_MAGIC, REQRESP_MAGIC,
    parse_pubsub_handshake_tail_qos,
};
use crate::transport::{fnv1a64, wire_type_hash};
use crate::{qb_debug, qb_info, qb_warn};

const DEFAULT_ALPN: &[u8] = b"quicbit/1";
const DEFAULT_BROADCAST_CAPACITY: usize = 256;
const IDENTITY_DERIVATION_TAG: &[u8] = b"quicbit/v1/identity";
const PUBSUB_DATAGRAM_MAGIC: &[u8; 4] = b"QBD1";
const PUBSUB_DATAGRAM_HEADER_LEN: usize = 4 + 8;
static NEXT_REMOTE_REQ_ID: AtomicU64 = AtomicU64::new(0);

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
    allowed_peers: Option<HashSet<[u8; 32]>>,
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

        let endpoint: Endpoint = {
            const BIND_ATTEMPTS: usize = 80;
            let mut last_bind_err = None;
            let mut bound = None;
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
                    Ok(endpoint) => {
                        bound = Some(endpoint);
                        break;
                    }
                    Err(err) => {
                        last_bind_err = Some(err);
                        if attempt + 1 < BIND_ATTEMPTS {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            }
            bound.ok_or_else(|| last_bind_err.expect("bind loop always records an error"))?
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
            request_topics: Mutex::new(HashMap::new()),
            que_topics: Mutex::new(HashMap::new()),
            put_topics: Mutex::new(HashMap::new()),
            pip_topics: Mutex::new(HashMap::new()),
            system_routes: Mutex::new(HashMap::new()),
            system_peers: Mutex::new(Vec::new()),
            peer_connections: Mutex::new(HashMap::new()),
            pubsub_datagram_routes: Mutex::new(HashMap::new()),
            pubsub_datagram_readers: Mutex::new(HashMap::new()),
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
    request_topics: Mutex<HashMap<String, RequestTopicState>>,
    que_topics: Mutex<HashMap<String, QueTopicState>>,
    put_topics: Mutex<HashMap<String, PutTopicState>>,
    pip_topics: Mutex<HashMap<String, PipTopicState>>,
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
    /// Best-effort pub/sub datagram sinks, keyed by the publisher
    /// endpoint id plus a deterministic per-subscription session id.
    pubsub_datagram_routes: Mutex<HashMap<DatagramRouteKey, Arc<PubsubDatagramSink>>>,
    /// Peers that already have one connection-level datagram reader
    /// task. QUIC datagrams are connection scoped, so one reader
    /// demultiplexes all best-effort topic sessions for that peer.
    pubsub_datagram_readers: Mutex<HashMap<[u8; 32], usize>>,
    /// If `Some`, only inbound connections from these peers are
    /// served; everything else is dropped immediately after the
    /// QUIC handshake completes.
    allowed_peers: Option<HashSet<[u8; 32]>>,
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
    qos: TopicQos,
    published: Arc<AtomicU64>,
    remote_dropped: Arc<AtomicU64>,
    stale_dropped: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
    send_errors: Arc<AtomicU64>,
}

struct RequestTopicState {
    tx: tokio::sync::mpsc::Sender<RemotePendingReq>,
    req_type_hash: u64,
    res_type_hash: u64,
    req_header_size: u32,
    res_header_size: u32,
    qos: TopicQos,
    stats: Arc<ItemStatsInner>,
}

struct QueTopicState {
    tx: tokio::sync::mpsc::Sender<RemotePendingQue>,
    que_type_hash: u64,
    ans_type_hash: u64,
    que_header_size: u32,
    ans_header_size: u32,
    qos: TopicQos,
    stats: Arc<ItemStatsInner>,
}

struct PutTopicState {
    tx: tokio::sync::mpsc::Sender<RemotePendingPuts>,
    put_type_hash: u64,
    ack_type_hash: u64,
    put_header_size: u32,
    ack_header_size: u32,
    qos: TopicQos,
    stats: Arc<ItemStatsInner>,
}

struct PipTopicState {
    tx: tokio::sync::mpsc::Sender<RemotePendingPip>,
    client_type_hash: u64,
    server_type_hash: u64,
    client_header_size: u32,
    server_header_size: u32,
    qos: TopicQos,
    stats: Arc<ItemStatsInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DatagramRouteKey {
    peer: [u8; 32],
    session_id: u64,
}

struct PubsubDatagramSink {
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    reassembler: Mutex<Reassembler>,
    stale_dropped: Arc<AtomicU64>,
    incomplete_dropped: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
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

/// Snapshot of one iroh path for an already-cached peer connection.
#[derive(Debug, Clone)]
pub struct PathDiagnostic {
    /// Debug-format iroh path id. Kept as a string so quicbit does
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

    /// Serve req/res calls for `topic`.
    ///
    /// The returned server polls both same-host SHM requests and
    /// remote iroh requests. In system-DID mode the local service and
    /// remote route are keyed by `(system_did, topic)`.
    pub fn req_server<Req, Res>(&self, topic: &str) -> Result<ReqServer<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.req_server_with_qos(topic, TopicQos::default())
    }

    /// Serve req/res calls for `topic` with explicit transport QoS.
    ///
    /// Only the byte-limit fields of [`TopicQos`] apply to req/res
    /// (`max_message_bytes`, `max_inflight_bytes`, `chunk_bytes`):
    /// large requests/responses are chunked and reassembled, lifting the
    /// 64 MiB single-frame cap. `delivery` is always treated as
    /// `Reliable` — req/res never drops messages.
    pub fn req_server_with_qos<Req, Res>(
        &self,
        topic: &str,
        qos: TopicQos,
    ) -> Result<ReqServer<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let req_type_hash = wire_type_hash::<Req>();
        let res_type_hash = wire_type_hash::<Res>();
        let req_header_size = std::mem::size_of::<Req::Header>() as u32;
        let res_header_size = std::mem::size_of::<Res::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map = crate::trace::recover_poison(
                self.inner.request_topics.lock(),
                "Node::request_topics",
            );
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a req/res server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                RequestTopicState {
                    tx: remote_tx,
                    req_type_hash,
                    res_type_hash,
                    req_header_size,
                    res_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalReqResService::<Req, Res>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalReqServerState {
            _service: primary_service,
            server: primary_server,
        });

        if self.inner.system_did.is_none() && self.inner.identity_name.is_some() {
            let hex_name = service_name(None, self.inner.endpoint_id.as_bytes(), topic);
            let alias_service = LocalReqResService::<Req, Res>::open_or_create(
                &hex_name,
                self.inner.local_cfg.clone(),
            )?;
            let alias_server = alias_service.server()?;
            local_servers.push(LocalReqServerState {
                _service: alias_service,
                server: alias_server,
            });
        }

        Ok(ReqServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_replies: HashMap::new(),
            next_pending_token: 0,
            stats,
        })
    }

    /// Build a req/res client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first using the same peer/topic naming
    /// rules as pub/sub; if no local req/res service exists, the call
    /// path dials the peer over iroh.
    pub fn req_client<Req, Res>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.req_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a req/res client for `peer` and `topic` with explicit QoS.
    ///
    /// QoS governs the remote (iroh) path only: the byte-limit fields
    /// enable chunking of large requests/responses. The local SHM path
    /// is unaffected. `delivery` is treated as `Reliable`.
    pub fn req_client_with_qos<Req, Res>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let mut candidates = Vec::with_capacity(2);
        if peer.name.is_some() {
            candidates.push(service_name(peer.name.as_deref(), &peer_bytes, topic));
        }
        candidates.push(service_name(None, &peer_bytes, topic));
        for svc_name in candidates {
            if let Ok(svc) = LocalReqResService::<Req, Res>::open_existing(&svc_name) {
                return Ok(ReqClient {
                    source: ReqClientSource::Local {
                        client: svc.client()?,
                    },
                });
            }
        }

        Ok(ReqClient {
            source: ReqClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: peer.endpoint_id,
                addr_hint: peer.addr,
                topic: topic.to_string(),
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
        })
    }

    /// Build a system-DID req/res client for `topic`.
    ///
    /// Resolution order mirrors [`subscribe`](Self::subscribe):
    /// local SHM first, explicit topic route second, system peer
    /// fallback third.
    pub fn req<Req, Res>(&self, topic: &str) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.req_with_qos(topic, TopicQos::default())
    }

    /// Build a system-DID req/res client for `topic` with explicit QoS.
    pub fn req_with_qos<Req, Res>(&self, topic: &str, qos: TopicQos) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
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

        if let Ok(svc) = LocalReqResService::<Req, Res>::open_existing(&svc_name) {
            return Ok(ReqClient {
                source: ReqClientSource::Local {
                    client: svc.client()?,
                },
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

        Ok(ReqClient {
            source: ReqClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: endpoint.id,
                addr_hint: Some(endpoint),
                topic: route_topic,
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
        })
    }

    /// Serve que/ans queries for `topic`.
    ///
    /// A que/ans server receives one query and may send zero or more
    /// answer items before calling `finish()`. In system-DID mode the
    /// service and remote route are keyed by `(system_did, topic)`.
    pub fn ans<Que, Ans>(&self, topic: &str) -> Result<AnsServer<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.ans_with_qos(topic, TopicQos::default())
    }

    /// Serve que/ans queries for `topic` with explicit transport QoS.
    ///
    /// The byte-limit fields of [`TopicQos`] chunk large queries and
    /// answers, lifting the 64 MiB single-frame cap. `delivery` is
    /// treated as `Reliable`; que/ans never drops answer items.
    pub fn ans_with_qos<Que, Ans>(&self, topic: &str, qos: TopicQos) -> Result<AnsServer<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let que_type_hash = wire_type_hash::<Que>();
        let ans_type_hash = wire_type_hash::<Ans>();
        let que_header_size = std::mem::size_of::<Que::Header>() as u32;
        let ans_header_size = std::mem::size_of::<Ans::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map =
                crate::trace::recover_poison(self.inner.que_topics.lock(), "Node::que_topics");
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a que/ans server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                QueTopicState {
                    tx: remote_tx,
                    que_type_hash,
                    ans_type_hash,
                    que_header_size,
                    ans_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalQueAnsService::<Que, Ans>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalAnsServerState {
            _service: primary_service,
            server: primary_server,
        });

        if self.inner.system_did.is_none() && self.inner.identity_name.is_some() {
            let hex_name = service_name(None, self.inner.endpoint_id.as_bytes(), topic);
            let alias_service = LocalQueAnsService::<Que, Ans>::open_or_create(
                &hex_name,
                self.inner.local_cfg.clone(),
            )?;
            let alias_server = alias_service.server()?;
            local_servers.push(LocalAnsServerState {
                _service: alias_service,
                server: alias_server,
            });
        }

        Ok(AnsServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_replies: HashMap::new(),
            next_pending_token: 0,
            stats,
        })
    }

    /// Build a que/ans client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first; if no local que/ans service is
    /// present, the client opens an iroh stream to the peer.
    pub fn que_client<Que, Ans>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.que_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a que/ans client for `peer` and `topic` with explicit QoS.
    pub fn que_client_with_qos<Que, Ans>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let mut candidates = Vec::with_capacity(2);
        if peer.name.is_some() {
            candidates.push(service_name(peer.name.as_deref(), &peer_bytes, topic));
        }
        candidates.push(service_name(None, &peer_bytes, topic));
        for svc_name in candidates {
            if let Ok(svc) = LocalQueAnsService::<Que, Ans>::open_existing(&svc_name) {
                return Ok(QueClient {
                    source: QueClientSource::Local {
                        client: svc.client()?,
                    },
                });
            }
        }

        Ok(QueClient {
            source: QueClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: peer.endpoint_id,
                addr_hint: peer.addr,
                topic: topic.to_string(),
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
        })
    }

    /// Build a system-DID que/ans client for `topic`.
    ///
    /// Resolution order mirrors [`req`](Self::req): local SHM,
    /// explicit topic route, then topic-agnostic system peer.
    pub fn que<Que, Ans>(&self, topic: &str) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.que_with_qos(topic, TopicQos::default())
    }

    /// Build a system-DID que/ans client for `topic` with explicit QoS.
    pub fn que_with_qos<Que, Ans>(&self, topic: &str, qos: TopicQos) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
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

        if let Ok(svc) = LocalQueAnsService::<Que, Ans>::open_existing(&svc_name) {
            return Ok(QueClient {
                source: QueClientSource::Local {
                    client: svc.client()?,
                },
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

        Ok(QueClient {
            source: QueClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: endpoint.id,
                addr_hint: Some(endpoint),
                topic: route_topic,
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
        })
    }

    /// Serve put/ack uploads for `topic`.
    ///
    /// A put/ack server receives zero or more put items, then sends
    /// one final ack. In system-DID mode the service and remote route
    /// are keyed by `(system_did, topic)`.
    pub fn ack<Put, Ack>(&self, topic: &str) -> Result<AckServer<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.ack_with_qos(topic, TopicQos::default())
    }

    /// Serve put/ack uploads for `topic` with explicit transport QoS.
    ///
    /// The byte-limit fields of [`TopicQos`] chunk large put items and
    /// acks, lifting the 64 MiB single-frame cap. `delivery` is treated
    /// as `Reliable`; put/ack never drops uploaded items.
    pub fn ack_with_qos<Put, Ack>(&self, topic: &str, qos: TopicQos) -> Result<AckServer<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let put_type_hash = wire_type_hash::<Put>();
        let ack_type_hash = wire_type_hash::<Ack>();
        let put_header_size = std::mem::size_of::<Put::Header>() as u32;
        let ack_header_size = std::mem::size_of::<Ack::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map =
                crate::trace::recover_poison(self.inner.put_topics.lock(), "Node::put_topics");
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a put/ack server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                PutTopicState {
                    tx: remote_tx,
                    put_type_hash,
                    ack_type_hash,
                    put_header_size,
                    ack_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalPutAckService::<Put, Ack>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalAckServerState {
            _service: primary_service,
            server: primary_server,
        });

        if self.inner.system_did.is_none() && self.inner.identity_name.is_some() {
            let hex_name = service_name(None, self.inner.endpoint_id.as_bytes(), topic);
            let alias_service = LocalPutAckService::<Put, Ack>::open_or_create(
                &hex_name,
                self.inner.local_cfg.clone(),
            )?;
            let alias_server = alias_service.server()?;
            local_servers.push(LocalAckServerState {
                _service: alias_service,
                server: alias_server,
            });
        }

        Ok(AckServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_puts: HashMap::new(),
            next_pending_token: 0,
            stats,
        })
    }

    /// Build a put/ack client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first; if no local put/ack service is
    /// present, the client opens an iroh stream to the peer.
    pub fn put_client<Put, Ack>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.put_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a put/ack client for `peer` and `topic` with explicit QoS.
    pub fn put_client_with_qos<Put, Ack>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let mut candidates = Vec::with_capacity(2);
        if peer.name.is_some() {
            candidates.push(service_name(peer.name.as_deref(), &peer_bytes, topic));
        }
        candidates.push(service_name(None, &peer_bytes, topic));
        for svc_name in candidates {
            if let Ok(svc) = LocalPutAckService::<Put, Ack>::open_existing(&svc_name) {
                return Ok(PutClient {
                    source: PutClientSource::Local {
                        client: svc.client()?,
                    },
                    pending_remote_uploads: HashMap::new(),
                    next_pending_upload: 0,
                });
            }
        }

        Ok(PutClient {
            source: PutClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: peer.endpoint_id,
                addr_hint: peer.addr,
                topic: topic.to_string(),
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
            pending_remote_uploads: HashMap::new(),
            next_pending_upload: 0,
        })
    }

    /// Build a system-DID put/ack client for `topic`.
    ///
    /// Resolution order mirrors [`req`](Self::req): local SHM,
    /// explicit topic route, then topic-agnostic system peer.
    pub fn put<Put, Ack>(&self, topic: &str) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.put_with_qos(topic, TopicQos::default())
    }

    /// Build a system-DID put/ack client for `topic` with explicit QoS.
    pub fn put_with_qos<Put, Ack>(&self, topic: &str, qos: TopicQos) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
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

        if let Ok(svc) = LocalPutAckService::<Put, Ack>::open_existing(&svc_name) {
            return Ok(PutClient {
                source: PutClientSource::Local {
                    client: svc.client()?,
                },
                pending_remote_uploads: HashMap::new(),
                next_pending_upload: 0,
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

        Ok(PutClient {
            source: PutClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: endpoint.id,
                addr_hint: Some(endpoint),
                topic: route_topic,
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
            pending_remote_uploads: HashMap::new(),
            next_pending_upload: 0,
        })
    }

    /// Serve pip sessions for `topic`.
    ///
    /// A pip session is bidirectional: clients send `ClientMsg`,
    /// servers send `ServerMsg`, and either direction may finish
    /// independently. In system-DID mode the service and remote route
    /// are keyed by `(system_did, topic)`.
    pub fn pip_server<ClientMsg, ServerMsg>(
        &self,
        topic: &str,
    ) -> Result<PipServer<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.pip_server_with_qos(topic, TopicQos::default())
    }

    /// Serve pip sessions for `topic` with explicit transport QoS.
    ///
    /// The byte-limit fields of [`TopicQos`] chunk large messages in
    /// both directions, lifting the 64 MiB single-frame cap. `delivery`
    /// is treated as `Reliable`; pip never drops session messages.
    pub fn pip_server_with_qos<ClientMsg, ServerMsg>(
        &self,
        topic: &str,
        qos: TopicQos,
    ) -> Result<PipServer<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let client_type_hash = wire_type_hash::<ClientMsg>();
        let server_type_hash = wire_type_hash::<ServerMsg>();
        let client_header_size = std::mem::size_of::<ClientMsg::Header>() as u32;
        let server_header_size = std::mem::size_of::<ServerMsg::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map =
                crate::trace::recover_poison(self.inner.pip_topics.lock(), "Node::pip_topics");
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a pip server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                PipTopicState {
                    tx: remote_tx,
                    client_type_hash,
                    server_type_hash,
                    client_header_size,
                    server_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalPipService::<ClientMsg, ServerMsg>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalPipServerState {
            _service: primary_service,
            server: primary_server,
        });

        if self.inner.system_did.is_none() && self.inner.identity_name.is_some() {
            let hex_name = service_name(None, self.inner.endpoint_id.as_bytes(), topic);
            let alias_service = LocalPipService::<ClientMsg, ServerMsg>::open_or_create(
                &hex_name,
                self.inner.local_cfg.clone(),
            )?;
            let alias_server = alias_service.server()?;
            local_servers.push(LocalPipServerState {
                _service: alias_service,
                server: alias_server,
            });
        }

        Ok(PipServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_sessions: HashMap::new(),
            next_pending_session: 0,
            stats,
        })
    }

    /// Build a pip client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first; if no local pip service is
    /// present, the client opens an iroh stream to the peer.
    pub fn pip_client<ClientMsg, ServerMsg>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.pip_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a pip client for `peer` and `topic` with explicit QoS.
    pub fn pip_client_with_qos<ClientMsg, ServerMsg>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let mut candidates = Vec::with_capacity(2);
        if peer.name.is_some() {
            candidates.push(service_name(peer.name.as_deref(), &peer_bytes, topic));
        }
        candidates.push(service_name(None, &peer_bytes, topic));
        for svc_name in candidates {
            if let Ok(svc) = LocalPipService::<ClientMsg, ServerMsg>::open_existing(&svc_name) {
                return Ok(PipClient {
                    source: PipClientSource::Local {
                        client: svc.client()?,
                    },
                    pending_remote_sessions: HashMap::new(),
                    next_pending_session: 0,
                });
            }
        }

        Ok(PipClient {
            source: PipClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: peer.endpoint_id,
                addr_hint: peer.addr,
                topic: topic.to_string(),
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
            pending_remote_sessions: HashMap::new(),
            next_pending_session: 0,
        })
    }

    /// Build a system-DID pip client for `topic`.
    ///
    /// Resolution order mirrors [`req`](Self::req): local SHM,
    /// explicit topic route, then topic-agnostic system peer.
    pub fn pip<ClientMsg, ServerMsg>(&self, topic: &str) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.pip_with_qos(topic, TopicQos::default())
    }

    /// Build a system-DID pip client for `topic` with explicit QoS.
    pub fn pip_with_qos<ClientMsg, ServerMsg>(
        &self,
        topic: &str,
        qos: TopicQos,
    ) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
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

        if let Ok(svc) = LocalPipService::<ClientMsg, ServerMsg>::open_existing(&svc_name) {
            return Ok(PipClient {
                source: PipClientSource::Local {
                    client: svc.client()?,
                },
                pending_remote_sessions: HashMap::new(),
                next_pending_session: 0,
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

        Ok(PipClient {
            source: PipClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: endpoint.id,
                addr_hint: Some(endpoint),
                topic: route_topic,
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
            pending_remote_sessions: HashMap::new(),
            next_pending_session: 0,
        })
    }

    fn remote_subscriber<T>(
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

struct RemotePubsubOpen {
    conn: Connection,
    recv: iroh::endpoint::RecvStream,
}

/// Single dial + handshake attempt. Returns the publisher connection and
/// `RecvStream` ready for [`pump_recv_stream`].
async fn subscribe_once(
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
async fn run_subscriber_loop(
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
                target: "quicbit::node",
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
                    target: "quicbit::node",
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
    messages_out: AtomicU64,
    messages_in: AtomicU64,
    bytes_out: AtomicU64,
    bytes_in: AtomicU64,
    errors: AtomicU64,
}

impl ItemStatsInner {
    fn snapshot(&self) -> ItemStats {
        ItemStats {
            messages_out: self.messages_out.load(Ordering::Relaxed),
            messages_in: self.messages_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    fn record_out(&self, bytes: usize) {
        self.messages_out.fetch_add(1, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_in(&self, bytes: usize) {
        self.messages_in.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
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
    qos: TopicQos,
    published: Arc<AtomicU64>,
    remote_dropped: Arc<AtomicU64>,
    stale_dropped: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
    send_errors: Arc<AtomicU64>,
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
            stale_dropped: self.stale_dropped.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
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
    /// Stale samples skipped by freshness policy.
    pub stale_dropped: u64,
    /// Incomplete chunked messages abandoned during reassembly.
    pub incomplete_dropped: u64,
    /// Bytes received from remote transport after reassembly.
    pub bytes_received: u64,
}

pub struct Subscriber<T: datapod::DataPod + 'static> {
    source: SubscriberSource<T>,
    received: Arc<AtomicU64>,
    disconnects: Arc<AtomicU64>,
    stale_dropped: Arc<AtomicU64>,
    incomplete_dropped: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
}

enum SubscriberSource<T: datapod::DataPod + 'static> {
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

fn take_local_with_qos<T: datapod::DataPod + 'static>(
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

struct LocalReqServerState<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    _service: LocalReqResService<Req, Res>,
    server: LocalReqServer<Req, Res>,
}

pub type PendingReq<'a, Req, Res> = (ReqSample<Req>, ReqReply<'a, Req, Res>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReqReplyToken {
    req_id: u64,
    source: ReplyTokenSource,
}

impl ReqReplyToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyTokenSource {
    Local { server_index: usize },
    Remote { token: u64 },
}

pub struct PendingReqMessage<Req>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    sample: ReqSample<Req>,
    reply: ReqReplyToken,
}

impl<Req> PendingReqMessage<Req>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn sample(&self) -> &ReqSample<Req> {
        &self.sample
    }

    pub fn into_parts(self) -> (ReqSample<Req>, ReqReplyToken) {
        (self.sample, self.reply)
    }
}

pub struct ReqServer<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    inner: Arc<NodeInner>,
    route_topic: String,
    local_servers: Vec<LocalReqServerState<Req, Res>>,
    remote_rx: tokio::sync::mpsc::Receiver<RemotePendingReq>,
    pending_remote_replies: HashMap<u64, RemoteReqReply>,
    next_pending_token: u64,
    stats: Arc<ItemStatsInner>,
}

impl<Req, Res> Drop for ReqServer<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.request_topics.lock(), "Node::request_topics")
            .remove(&self.route_topic);
    }
}

impl<Req, Res> ReqServer<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this req/res server.
    pub fn stats(&self) -> ReqStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<PendingReq<'_, Req, Res>>> {
        for local in &mut self.local_servers {
            if let Some((req, reply)) = local.server.take_request()? {
                let sample = ReqSample {
                    req_id: req.req_id(),
                    header: *req.header(),
                    payload: req.payload().to_vec(),
                };
                return Ok(Some((sample, ReqReply::Local(reply))));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                let sample = req_sample_from_frame::<Req>(pending.req_id, &pending.frame)?;
                Ok(Some((
                    sample,
                    ReqReply::Remote {
                        req_id: pending.req_id,
                        send: Some(pending.send),
                        rt: self.inner.rt.clone(),
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                        _phantom: PhantomData,
                    },
                )))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn take_message(&mut self) -> Result<Option<PendingReqMessage<Req>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((req, _reply)) = local.server.take_request()? {
                let req_id = req.req_id();
                let sample = ReqSample {
                    req_id,
                    header: *req.header(),
                    payload: req.payload().to_vec(),
                };
                return Ok(Some(PendingReqMessage {
                    sample,
                    reply: ReqReplyToken {
                        req_id,
                        source: ReplyTokenSource::Local { server_index },
                    },
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_token = self.next_pending_token.wrapping_add(1).max(1);
                let token = self.next_pending_token;
                let sample = req_sample_from_frame::<Req>(pending.req_id, &pending.frame)?;
                self.pending_remote_replies.insert(
                    token,
                    RemoteReqReply {
                        send: Some(pending.send),
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                    },
                );
                Ok(Some(PendingReqMessage {
                    sample,
                    reply: ReqReplyToken {
                        req_id: pending.req_id,
                        source: ReplyTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn respond_pending(&mut self, reply: ReqReplyToken, res: &Res) -> Result<()> {
        match reply.source {
            ReplyTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local req/res reply token"))?;
                local.server.respond_to(reply.req_id, res)
            }
            ReplyTokenSource::Remote { token } => {
                let mut reply = self.pending_remote_replies.remove(&token).ok_or_else(|| {
                    Error::invalid_argument("invalid or already used remote req/res reply token")
                })?;
                reply.respond(&self.inner.rt, res)
            }
        }
    }
}

pub struct ReqClient<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    source: ReqClientSource<Req, Res>,
}

enum ReqClientSource<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalReqClient<Req, Res>,
    },
    Remote {
        inner: Arc<NodeInner>,
        peer_id: EndpointId,
        addr_hint: Option<EndpointAddr>,
        topic: String,
        next_id: AtomicU64,
        qos: TopicQos,
        stats: Arc<ItemStatsInner>,
    },
}

impl<Req, Res> ReqClient<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this req/res client.
    pub fn stats(&self) -> ReqStats {
        match &self.source {
            ReqClientSource::Remote { stats, .. } => stats.snapshot(),
            ReqClientSource::Local { .. } => ReqStats::default(),
        }
    }

    pub fn call(&mut self, req: &Req) -> Result<ResSample<Res>> {
        match &mut self.source {
            ReqClientSource::Local { client } => {
                let sample = client.call(req)?;
                Ok(ResSample {
                    req_id: sample.req_id(),
                    header: *sample.header(),
                    payload: sample.payload().to_vec(),
                })
            }
            ReqClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let frame = frame_from_datapod(req);
                let req_len = frame.len();
                let stats = stats.clone();
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let req_type_hash = wire_type_hash::<Req>();
                let res_type_hash = wire_type_hash::<Res>();
                let req_header_size = std::mem::size_of::<Req::Header>() as u32;
                let res_header_size = std::mem::size_of::<Res::Header>() as u32;

                let rt = inner.rt.clone();
                let inner_for_call = inner.clone();
                let response = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner_for_call, peer_id, addr_hint).await?;
                    let (mut send, mut recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        REQRESP_MAGIC,
                        &topic,
                        req_type_hash,
                        res_type_hash,
                        req_header_size,
                        res_header_size,
                        qos,
                    )
                    .await?;
                    write_item_chunked(&mut send, &frame, qos.chunk_bytes, true).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    read_item_chunked(&mut recv, qos.max_message_bytes, qos.max_inflight_bytes)
                        .await?
                        .ok_or_else(|| {
                            Error::Remote("server closed without writing a response".to_string())
                        })
                });
                let response = match response {
                    Ok(r) => r,
                    Err(e) => {
                        stats.record_error();
                        return Err(e);
                    }
                };
                stats.record_out(req_len);
                stats.record_in(response.len());
                res_sample_from_frame::<Res>(req_id, &response)
            }
        }
    }
}

pub struct ReqSample<Req: datapod::DataPod + 'static> {
    req_id: u64,
    header: Req::Header,
    payload: Vec<u8>,
}

impl<Req: datapod::DataPod + 'static> ReqSample<Req> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Req::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub struct ResSample<Res: datapod::DataPod + 'static> {
    req_id: u64,
    header: Res::Header,
    payload: Vec<u8>,
}

impl<Res: datapod::DataPod + 'static> ResSample<Res> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Res::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum ReqReply<'a, Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::ReplyHandle<'a, Req, Res>),
    Remote {
        req_id: u64,
        send: Option<iroh::endpoint::SendStream>,
        rt: Arc<Runtime>,
        qos: TopicQos,
        peer_chunks: bool,
        stats: Arc<ItemStatsInner>,
        _phantom: PhantomData<fn() -> (Req, Res)>,
    },
}

impl<Req, Res> ReqReply<'_, Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        match self {
            ReqReply::Local(reply) => reply.req_id(),
            ReqReply::Remote { req_id, .. } => *req_id,
        }
    }

    pub fn respond(self, res: &Res) -> Result<()> {
        match self {
            ReqReply::Local(reply) => reply.respond(res),
            ReqReply::Remote {
                send: mut send_opt,
                rt,
                qos,
                peer_chunks,
                stats,
                ..
            } => {
                let mut send = send_opt
                    .take()
                    .ok_or_else(|| Error::Remote("response already sent".to_string()))?;
                let frame = frame_from_datapod(res);
                let res_len = frame.len();
                let result = rt.block_on(async move {
                    write_item_chunked(&mut send, &frame, qos.chunk_bytes, peer_chunks).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    Ok(())
                });
                match result {
                    Ok(()) => {
                        stats.record_out(res_len);
                        Ok(())
                    }
                    Err(e) => {
                        stats.record_error();
                        Err(e)
                    }
                }
            }
        }
    }
}

struct RemotePendingReq {
    req_id: u64,
    frame: Vec<u8>,
    send: iroh::endpoint::SendStream,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
}

struct RemoteReqReply {
    send: Option<iroh::endpoint::SendStream>,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
}

impl RemoteReqReply {
    fn respond<Res>(&mut self, rt: &Runtime, res: &Res) -> Result<()>
    where
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("response already sent".to_string()))?;
        let frame = frame_from_datapod(res);
        let res_len = frame.len();
        let qos = self.qos;
        let peer_chunks = self.peer_chunks;
        let result = rt.block_on(async move {
            write_item_chunked(&mut send, &frame, qos.chunk_bytes, peer_chunks).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok(())
        });
        match result {
            Ok(()) => {
                self.stats.record_out(res_len);
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }
}

fn frame_from_datapod<T>(value: &T) -> Vec<u8>
where
    T: datapod::DataPod + 'static,
    <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    <T as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    datapod::to_wire_message(value).bytes
}

fn req_sample_from_frame<Req>(req_id: u64, frame: &[u8]) -> Result<ReqSample<Req>>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Req::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "req frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(ReqSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

fn res_sample_from_frame<Res>(req_id: u64, frame: &[u8]) -> Result<ResSample<Res>>
where
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Res::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "res frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(ResSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

// ---- que/ans ----

struct LocalAnsServerState<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    _service: LocalQueAnsService<Que, Ans>,
    server: LocalAnsServer<Que, Ans>,
}

pub type PendingQue<'a, Que, Ans> = (QueSample<Que>, AnsReply<'a, Que, Ans>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnsReplyToken {
    req_id: u64,
    source: ReplyTokenSource,
}

impl AnsReplyToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

pub struct PendingQueMessage<Que>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    sample: QueSample<Que>,
    answers: AnsReplyToken,
}

impl<Que> PendingQueMessage<Que>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn sample(&self) -> &QueSample<Que> {
        &self.sample
    }

    pub fn into_parts(self) -> (QueSample<Que>, AnsReplyToken) {
        (self.sample, self.answers)
    }
}

pub struct AnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    inner: Arc<NodeInner>,
    route_topic: String,
    local_servers: Vec<LocalAnsServerState<Que, Ans>>,
    remote_rx: tokio::sync::mpsc::Receiver<RemotePendingQue>,
    pending_remote_replies: HashMap<u64, RemoteAnsReply>,
    next_pending_token: u64,
    stats: Arc<ItemStatsInner>,
}

impl<Que, Ans> Drop for AnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.que_topics.lock(), "Node::que_topics")
            .remove(&self.route_topic);
    }
}

impl<Que, Ans> AnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this que/ans server.
    pub fn stats(&self) -> QueStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<PendingQue<'_, Que, Ans>>> {
        for local in &mut self.local_servers {
            if let Some((que, reply)) = local.server.take()? {
                let sample = QueSample {
                    req_id: que.req_id(),
                    header: *que.header(),
                    payload: que.payload().to_vec(),
                };
                return Ok(Some((sample, AnsReply::Local(reply))));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                let sample = que_sample_from_frame::<Que>(pending.req_id, &pending.frame)?;
                Ok(Some((
                    sample,
                    AnsReply::Remote {
                        req_id: pending.req_id,
                        send: Some(pending.send),
                        rt: self.inner.rt.clone(),
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                        _phantom: PhantomData,
                    },
                )))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn take_message(&mut self) -> Result<Option<PendingQueMessage<Que>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((que, _reply)) = local.server.take()? {
                let sample = QueSample {
                    req_id: que.req_id(),
                    header: *que.header(),
                    payload: que.payload().to_vec(),
                };
                return Ok(Some(PendingQueMessage {
                    answers: AnsReplyToken {
                        req_id: sample.req_id,
                        source: ReplyTokenSource::Local { server_index },
                    },
                    sample,
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_token = self.next_pending_token.wrapping_add(1).max(1);
                let token = self.next_pending_token;
                let sample = que_sample_from_frame::<Que>(pending.req_id, &pending.frame)?;
                self.pending_remote_replies.insert(
                    token,
                    RemoteAnsReply {
                        send: Some(pending.send),
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                    },
                );
                Ok(Some(PendingQueMessage {
                    sample,
                    answers: AnsReplyToken {
                        req_id: pending.req_id,
                        source: ReplyTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn send_pending(&mut self, answers: AnsReplyToken, ans: &Ans) -> Result<()> {
        match answers.source {
            ReplyTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local que/ans token"))?;
                local.server.send_to(answers.req_id, ans)
            }
            ReplyTokenSource::Remote { token } => {
                let reply = self.pending_remote_replies.get_mut(&token).ok_or_else(|| {
                    Error::invalid_argument("invalid or finished remote que/ans token")
                })?;
                reply.send(&self.inner.rt, ans)
            }
        }
    }

    pub fn finish_pending(&mut self, answers: AnsReplyToken) -> Result<()> {
        match answers.source {
            ReplyTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local que/ans token"))?;
                local.server.finish_to(answers.req_id)
            }
            ReplyTokenSource::Remote { token } => {
                let mut reply = self.pending_remote_replies.remove(&token).ok_or_else(|| {
                    Error::invalid_argument("invalid or finished remote que/ans token")
                })?;
                reply.finish(&self.inner.rt)
            }
        }
    }
}

pub struct QueClient<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    source: QueClientSource<Que, Ans>,
}

enum QueClientSource<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalQueClient<Que, Ans>,
    },
    Remote {
        inner: Arc<NodeInner>,
        peer_id: EndpointId,
        addr_hint: Option<EndpointAddr>,
        topic: String,
        next_id: AtomicU64,
        qos: TopicQos,
        stats: Arc<ItemStatsInner>,
    },
}

impl<Que, Ans> QueClient<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this que/ans client.
    pub fn stats(&self) -> QueStats {
        match &self.source {
            QueClientSource::Remote { stats, .. } => stats.snapshot(),
            QueClientSource::Local { .. } => QueStats::default(),
        }
    }

    pub fn send(&mut self, que: &Que) -> Result<Answers<'_, Ans>> {
        match &mut self.source {
            QueClientSource::Local { client } => Ok(Answers::Local(client.send(que)?)),
            QueClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let frame = frame_from_datapod(que);
                let que_len = frame.len();
                let stats = stats.clone();
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let que_type_hash = wire_type_hash::<Que>();
                let ans_type_hash = wire_type_hash::<Ans>();
                let que_header_size = std::mem::size_of::<Que::Header>() as u32;
                let ans_header_size = std::mem::size_of::<Ans::Header>() as u32;

                let rt = inner.rt.clone();
                let recv = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        QUEANS_MAGIC,
                        &topic,
                        que_type_hash,
                        ans_type_hash,
                        que_header_size,
                        ans_header_size,
                        qos,
                    )
                    .await?;
                    write_item_chunked(&mut send, &frame, qos.chunk_bytes, true).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    Ok::<_, Error>(recv)
                });
                let recv = match recv {
                    Ok(r) => r,
                    Err(e) => {
                        stats.record_error();
                        return Err(e);
                    }
                };
                stats.record_out(que_len);

                Ok(Answers::Remote(RemoteAnswers {
                    req_id,
                    recv,
                    rt,
                    done: false,
                    qos,
                    stats,
                    _phantom: PhantomData,
                }))
            }
        }
    }
}

pub struct QueSample<Que: datapod::DataPod + 'static> {
    req_id: u64,
    header: Que::Header,
    payload: Vec<u8>,
}

impl<Que: datapod::DataPod + 'static> QueSample<Que> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Que::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub struct AnsSample<Ans: datapod::DataPod + 'static> {
    req_id: u64,
    header: Ans::Header,
    payload: Vec<u8>,
}

impl<Ans: datapod::DataPod + 'static> AnsSample<Ans> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Ans::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum AnsReply<'a, Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::AnsReply<'a, Que, Ans>),
    Remote {
        req_id: u64,
        send: Option<iroh::endpoint::SendStream>,
        rt: Arc<Runtime>,
        qos: TopicQos,
        peer_chunks: bool,
        stats: Arc<ItemStatsInner>,
        _phantom: PhantomData<fn() -> (Que, Ans)>,
    },
}

impl<Que, Ans> AnsReply<'_, Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        match self {
            AnsReply::Local(reply) => reply.req_id(),
            AnsReply::Remote { req_id, .. } => *req_id,
        }
    }

    pub fn send(&mut self, ans: &Ans) -> Result<()> {
        match self {
            AnsReply::Local(reply) => reply.send(ans),
            AnsReply::Remote {
                send,
                rt,
                qos,
                peer_chunks,
                stats,
                ..
            } => {
                let send = send
                    .as_mut()
                    .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
                let frame = frame_from_datapod(ans);
                let frame_len = frame.len();
                let chunk_bytes = qos.chunk_bytes;
                let peer_chunks = *peer_chunks;
                let result = rt.block_on(async move {
                    write_answer_item(send, &frame, chunk_bytes, peer_chunks).await
                });
                match result {
                    Ok(()) => {
                        stats.record_out(frame_len);
                        Ok(())
                    }
                    Err(e) => {
                        stats.record_error();
                        Err(e)
                    }
                }
            }
        }
    }

    pub fn finish(mut self) -> Result<()> {
        match &mut self {
            AnsReply::Local(reply) => reply.finish(),
            AnsReply::Remote { send, rt, .. } => {
                let mut send = send
                    .take()
                    .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
                rt.block_on(async move {
                    write_answer_done(&mut send).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    Ok(())
                })
            }
        }
    }
}

pub enum Answers<'a, Ans: datapod::DataPod + 'static> {
    Local(crate::local::LocalAnswers<'a, Ans>),
    Remote(RemoteAnswers<Ans>),
}

impl<Ans: datapod::DataPod + 'static> Answers<'_, Ans>
where
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<AnsSample<Ans>>> {
        match self {
            Answers::Local(answers) => Ok(answers.next()?.map(|sample| AnsSample {
                req_id: sample.req_id(),
                header: *sample.header(),
                payload: sample.payload().to_vec(),
            })),
            Answers::Remote(answers) => answers.next(),
        }
    }
}

pub struct RemoteAnswers<Ans: datapod::DataPod + 'static> {
    req_id: u64,
    recv: iroh::endpoint::RecvStream,
    rt: Arc<Runtime>,
    done: bool,
    qos: TopicQos,
    stats: Arc<ItemStatsInner>,
    _phantom: PhantomData<fn() -> Ans>,
}

impl<Ans: datapod::DataPod + 'static> RemoteAnswers<Ans>
where
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<AnsSample<Ans>>> {
        if self.done {
            return Ok(None);
        }
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let item = self.rt.block_on(async {
            read_answer_frame(&mut self.recv, max_message_bytes, max_inflight_bytes).await
        });
        let item = match item {
            Ok(i) => i,
            Err(e) => {
                self.stats.record_error();
                return Err(e);
            }
        };
        match item {
            Some(frame) => {
                self.stats.record_in(frame.len());
                Ok(Some(ans_sample_from_frame::<Ans>(self.req_id, &frame)?))
            }
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }
}

struct RemotePendingQue {
    req_id: u64,
    frame: Vec<u8>,
    send: iroh::endpoint::SendStream,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
}

struct RemoteAnsReply {
    send: Option<iroh::endpoint::SendStream>,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
}

impl RemoteAnsReply {
    fn send<Ans>(&mut self, rt: &Runtime, ans: &Ans) -> Result<()>
    where
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let send = self
            .send
            .as_mut()
            .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
        let frame = frame_from_datapod(ans);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let peer_chunks = self.peer_chunks;
        let result =
            rt.block_on(
                async move { write_answer_item(send, &frame, chunk_bytes, peer_chunks).await },
            );
        match result {
            Ok(()) => {
                self.stats.record_out(frame_len);
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    fn finish(&mut self, rt: &Runtime) -> Result<()> {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
        rt.block_on(async move {
            write_answer_done(&mut send).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok(())
        })
    }
}

fn que_sample_from_frame<Que>(req_id: u64, frame: &[u8]) -> Result<QueSample<Que>>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Que::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "que frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(QueSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

fn ans_sample_from_frame<Ans>(req_id: u64, frame: &[u8]) -> Result<AnsSample<Ans>>
where
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Ans::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "ans frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(AnsSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

// ---- put/ack ----

struct LocalAckServerState<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    _service: LocalPutAckService<Put, Ack>,
    server: LocalAckServer<Put, Ack>,
}

pub struct AckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    inner: Arc<NodeInner>,
    route_topic: String,
    local_servers: Vec<LocalAckServerState<Put, Ack>>,
    remote_rx: tokio::sync::mpsc::Receiver<RemotePendingPuts>,
    pending_remote_puts: HashMap<u64, RemotePuts<Put, Ack>>,
    next_pending_token: u64,
    stats: Arc<ItemStatsInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutAckToken {
    req_id: u64,
    source: PutAckTokenSource,
}

impl PutAckToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutAckTokenSource {
    Local { server_index: usize },
    Remote { token: u64 },
}

pub struct PendingPutMessage<Put>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    req_id: u64,
    first: Option<PutSample<Put>>,
    done: bool,
    token: PutAckToken,
}

impl<Put> PendingPutMessage<Put>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn first(&self) -> Option<&PutSample<Put>> {
        self.first.as_ref()
    }

    pub fn done(&self) -> bool {
        self.done
    }

    pub fn into_parts(self) -> (u64, Option<PutSample<Put>>, bool, PutAckToken) {
        (self.req_id, self.first, self.done, self.token)
    }
}

impl<Put, Ack> Drop for AckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.put_topics.lock(), "Node::put_topics")
            .remove(&self.route_topic);
    }
}

impl<Put, Ack> AckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this put/ack server.
    pub fn stats(&self) -> PutStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<Puts<'_, Put, Ack>>> {
        for local in &mut self.local_servers {
            if let Some(puts) = local.server.take()? {
                return Ok(Some(Puts::Local(puts)));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => Ok(Some(Puts::Remote(RemotePuts {
                req_id: pending.req_id,
                recv: pending.recv,
                send: Some(pending.send),
                rt: self.inner.rt.clone(),
                done: false,
                qos: pending.qos,
                peer_chunks: pending.peer_chunks,
                stats: pending.stats,
                _phantom: PhantomData,
            }))),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn take_message(&mut self) -> Result<Option<PendingPutMessage<Put>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((req_id, first, done)) = local.server.take_message()? {
                return Ok(Some(PendingPutMessage {
                    req_id,
                    first: first.map(|sample| PutSample {
                        req_id: sample.req_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }),
                    done,
                    token: PutAckToken {
                        req_id,
                        source: PutAckTokenSource::Local { server_index },
                    },
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_token = self.next_pending_token.wrapping_add(1).max(1);
                let token = self.next_pending_token;
                let req_id = pending.req_id;
                self.pending_remote_puts.insert(
                    token,
                    RemotePuts {
                        req_id,
                        recv: pending.recv,
                        send: Some(pending.send),
                        rt: self.inner.rt.clone(),
                        done: false,
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(Some(PendingPutMessage {
                    req_id,
                    first: None,
                    done: false,
                    token: PutAckToken {
                        req_id,
                        source: PutAckTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn next_pending(&mut self, token: PutAckToken) -> Result<Option<PutSample<Put>>> {
        match token.source {
            PutAckTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local put/ack token"))?;
                Ok(local
                    .server
                    .next_from(token.req_id)?
                    .map(|sample| PutSample {
                        req_id: sample.req_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }))
            }
            PutAckTokenSource::Remote { token } => {
                let puts = self
                    .pending_remote_puts
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote put/ack token"))?;
                puts.next()
            }
        }
    }

    pub fn ack_pending(&mut self, token: PutAckToken, ack: &Ack) -> Result<()> {
        match token.source {
            PutAckTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local put/ack token"))?;
                local.server.ack_to(token.req_id, ack)
            }
            PutAckTokenSource::Remote { token } => {
                let mut puts = self
                    .pending_remote_puts
                    .remove(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote put/ack token"))?;
                puts.ack(ack)
            }
        }
    }

    pub fn close_pending(&mut self, token: PutAckToken) {
        if let PutAckTokenSource::Remote { token } = token.source {
            self.pending_remote_puts.remove(&token);
        }
    }
}

pub struct PutClient<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    source: PutClientSource<Put, Ack>,
    pending_remote_uploads: HashMap<u64, RemotePutSender<Put, Ack>>,
    next_pending_upload: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutUploadToken {
    req_id: u64,
    source: PutUploadTokenSource,
}

impl PutUploadToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutUploadTokenSource {
    Local,
    Remote { token: u64 },
}

enum PutClientSource<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalPutClient<Put, Ack>,
    },
    Remote {
        inner: Arc<NodeInner>,
        peer_id: EndpointId,
        addr_hint: Option<EndpointAddr>,
        topic: String,
        next_id: AtomicU64,
        qos: TopicQos,
        stats: Arc<ItemStatsInner>,
    },
}

impl<Put, Ack> PutClient<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this put/ack client.
    pub fn stats(&self) -> PutStats {
        match &self.source {
            PutClientSource::Remote { stats, .. } => stats.snapshot(),
            PutClientSource::Local { .. } => PutStats::default(),
        }
    }

    pub fn open(&mut self) -> Result<PutSender<'_, Put, Ack>> {
        match &mut self.source {
            PutClientSource::Local { client } => Ok(PutSender::Local(client.open()?)),
            PutClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let put_type_hash = wire_type_hash::<Put>();
                let ack_type_hash = wire_type_hash::<Ack>();
                let put_header_size = std::mem::size_of::<Put::Header>() as u32;
                let ack_header_size = std::mem::size_of::<Ack::Header>() as u32;

                let rt = inner.rt.clone();
                let (send, recv) = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PUTACK_MAGIC,
                        &topic,
                        put_type_hash,
                        ack_type_hash,
                        put_header_size,
                        ack_header_size,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                Ok(PutSender::Remote(RemotePutSender {
                    req_id,
                    send: Some(send),
                    recv,
                    rt,
                    qos,
                    stats,
                    _phantom: PhantomData,
                }))
            }
        }
    }

    pub fn open_upload(&mut self) -> Result<PutUploadToken> {
        match &mut self.source {
            PutClientSource::Local { client } => {
                let req_id = client.open_req();
                Ok(PutUploadToken {
                    req_id,
                    source: PutUploadTokenSource::Local,
                })
            }
            PutClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let rt = inner.rt.clone();
                let send_rt = rt.clone();
                let (send, recv) = send_rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PUTACK_MAGIC,
                        &topic,
                        wire_type_hash::<Put>(),
                        wire_type_hash::<Ack>(),
                        std::mem::size_of::<Put::Header>() as u32,
                        std::mem::size_of::<Ack::Header>() as u32,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                self.next_pending_upload = self.next_pending_upload.wrapping_add(1).max(1);
                let token = self.next_pending_upload;
                self.pending_remote_uploads.insert(
                    token,
                    RemotePutSender {
                        req_id,
                        send: Some(send),
                        recv,
                        rt,
                        qos,
                        stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(PutUploadToken {
                    req_id,
                    source: PutUploadTokenSource::Remote { token },
                })
            }
        }
    }

    pub fn send_pending(&mut self, token: PutUploadToken, put: &Put) -> Result<()> {
        match token.source {
            PutUploadTokenSource::Local => match &mut self.source {
                PutClientSource::Local { client } => client.send_to(token.req_id, put),
                PutClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local put token used with remote put client",
                )),
            },
            PutUploadTokenSource::Remote { token } => self
                .pending_remote_uploads
                .get_mut(&token)
                .ok_or_else(|| Error::invalid_argument("invalid remote put token"))?
                .send(put),
        }
    }

    pub fn finish_pending(&mut self, token: PutUploadToken) -> Result<AckSample<Ack>> {
        match token.source {
            PutUploadTokenSource::Local => match &mut self.source {
                PutClientSource::Local { client } => {
                    let sample = client.finish_req(token.req_id)?;
                    Ok(AckSample {
                        req_id: sample.req_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    })
                }
                PutClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local put token used with remote put client",
                )),
            },
            PutUploadTokenSource::Remote { token } => self
                .pending_remote_uploads
                .remove(&token)
                .ok_or_else(|| Error::invalid_argument("invalid remote put token"))?
                .finish(),
        }
    }
}

pub struct PutSample<Put: datapod::DataPod + 'static> {
    req_id: u64,
    header: Put::Header,
    payload: Vec<u8>,
}

impl<Put: datapod::DataPod + 'static> PutSample<Put> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Put::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub struct AckSample<Ack: datapod::DataPod + 'static> {
    req_id: u64,
    header: Ack::Header,
    payload: Vec<u8>,
}

impl<Ack: datapod::DataPod + 'static> AckSample<Ack> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Ack::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum Puts<'a, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::LocalPuts<'a, Put, Ack>),
    Remote(RemotePuts<Put, Ack>),
}

impl<Put, Ack> Puts<'_, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> Option<u64> {
        match self {
            Puts::Local(puts) => puts.req_id(),
            Puts::Remote(puts) => Some(puts.req_id),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<PutSample<Put>>> {
        match self {
            Puts::Local(puts) => Ok(puts.next()?.map(|sample| PutSample {
                req_id: sample.req_id(),
                header: *sample.header(),
                payload: sample.payload().to_vec(),
            })),
            Puts::Remote(puts) => puts.next(),
        }
    }

    pub fn ack(&mut self, ack: &Ack) -> Result<()> {
        match self {
            Puts::Local(puts) => puts.ack(ack),
            Puts::Remote(puts) => puts.ack(ack),
        }
    }
}

pub enum PutSender<'a, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::LocalPutSender<'a, Put, Ack>),
    Remote(RemotePutSender<Put, Ack>),
}

impl<Put, Ack> PutSender<'_, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        match self {
            PutSender::Local(sender) => sender.req_id(),
            PutSender::Remote(sender) => sender.req_id,
        }
    }

    pub fn send(&mut self, put: &Put) -> Result<()> {
        match self {
            PutSender::Local(sender) => sender.send(put),
            PutSender::Remote(sender) => sender.send(put),
        }
    }

    pub fn finish(self) -> Result<AckSample<Ack>> {
        match self {
            PutSender::Local(sender) => {
                let sample = sender.finish()?;
                Ok(AckSample {
                    req_id: sample.req_id(),
                    header: *sample.header(),
                    payload: sample.payload().to_vec(),
                })
            }
            PutSender::Remote(sender) => sender.finish(),
        }
    }
}

pub struct RemotePuts<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    req_id: u64,
    recv: iroh::endpoint::RecvStream,
    send: Option<iroh::endpoint::SendStream>,
    rt: Arc<Runtime>,
    done: bool,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
    _phantom: PhantomData<fn() -> (Put, Ack)>,
}

impl<Put, Ack> RemotePuts<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn next(&mut self) -> Result<Option<PutSample<Put>>> {
        if self.done {
            return Ok(None);
        }
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let item = self.rt.block_on(async {
            read_put_frame(&mut self.recv, max_message_bytes, max_inflight_bytes).await
        });
        let item = match item {
            Ok(i) => i,
            Err(e) => {
                self.stats.record_error();
                return Err(e);
            }
        };
        match item {
            Some(frame) => {
                self.stats.record_in(frame.len());
                Ok(Some(put_sample_from_frame::<Put>(self.req_id, &frame)?))
            }
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }

    fn ack(&mut self, ack: &Ack) -> Result<()> {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("ack already sent".to_string()))?;
        let frame = frame_from_datapod(ack);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let peer_chunks = self.peer_chunks;
        let stats = self.stats.clone();
        self.rt.block_on(async move {
            write_item_chunked(&mut send, &frame, chunk_bytes, peer_chunks).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok::<(), Error>(())
        })?;
        stats.record_out(frame_len);
        Ok(())
    }
}

pub struct RemotePutSender<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    req_id: u64,
    send: Option<iroh::endpoint::SendStream>,
    recv: iroh::endpoint::RecvStream,
    rt: Arc<Runtime>,
    qos: TopicQos,
    stats: Arc<ItemStatsInner>,
    _phantom: PhantomData<fn() -> (Put, Ack)>,
}

impl<Put, Ack> RemotePutSender<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn send(&mut self, put: &Put) -> Result<()> {
        let send = self
            .send
            .as_mut()
            .ok_or_else(|| Error::Remote("put stream already finished".to_string()))?;
        let frame = frame_from_datapod(put);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let result = self
            .rt
            .block_on(async move { write_put_item(send, &frame, chunk_bytes, true).await });
        match result {
            Ok(()) => {
                self.stats.record_out(frame_len);
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    fn finish(mut self) -> Result<AckSample<Ack>> {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("put stream already finished".to_string()))?;
        let req_id = self.req_id;
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let frame = self.rt.block_on(async move {
            write_put_done(&mut send).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            read_item_chunked(&mut self.recv, max_message_bytes, max_inflight_bytes)
                .await?
                .ok_or_else(|| Error::Remote("server closed without writing an ack".to_string()))
        });
        let frame = match frame {
            Ok(f) => f,
            Err(e) => {
                self.stats.record_error();
                return Err(e);
            }
        };
        self.stats.record_in(frame.len());
        ack_sample_from_frame::<Ack>(req_id, &frame)
    }
}

struct RemotePendingPuts {
    req_id: u64,
    recv: iroh::endpoint::RecvStream,
    send: iroh::endpoint::SendStream,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
}

fn put_sample_from_frame<Put>(req_id: u64, frame: &[u8]) -> Result<PutSample<Put>>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Put::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "put frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(PutSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

fn ack_sample_from_frame<Ack>(req_id: u64, frame: &[u8]) -> Result<AckSample<Ack>>
where
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Ack::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "ack frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(AckSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

// ---- pip ----

struct LocalPipServerState<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    _service: LocalPipService<ClientMsg, ServerMsg>,
    server: LocalPipServer<ClientMsg, ServerMsg>,
}

pub struct PipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    inner: Arc<NodeInner>,
    route_topic: String,
    local_servers: Vec<LocalPipServerState<ClientMsg, ServerMsg>>,
    remote_rx: tokio::sync::mpsc::Receiver<RemotePendingPip>,
    pending_remote_sessions: HashMap<u64, RemotePip<ServerMsg, ClientMsg>>,
    next_pending_session: u64,
    stats: Arc<ItemStatsInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipServerToken {
    session_id: u64,
    source: PipServerTokenSource,
}

impl PipServerToken {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipServerTokenSource {
    Local { server_index: usize },
    Remote { token: u64 },
}

pub struct PendingPipMessage<ClientMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    session_id: u64,
    first: Option<PipSample<ClientMsg>>,
    incoming_done: bool,
    token: PipServerToken,
}

impl<ClientMsg> PendingPipMessage<ClientMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn first(&self) -> Option<&PipSample<ClientMsg>> {
        self.first.as_ref()
    }

    pub fn incoming_done(&self) -> bool {
        self.incoming_done
    }

    pub fn into_parts(self) -> (u64, Option<PipSample<ClientMsg>>, bool, PipServerToken) {
        (self.session_id, self.first, self.incoming_done, self.token)
    }
}

impl<ClientMsg, ServerMsg> Drop for PipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.pip_topics.lock(), "Node::pip_topics")
            .remove(&self.route_topic);
    }
}

impl<ClientMsg, ServerMsg> PipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this pip server.
    pub fn stats(&self) -> PipStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<Pip<'_, ServerMsg, ClientMsg>>> {
        for local in &mut self.local_servers {
            if let Some(pip) = local.server.take()? {
                return Ok(Some(Pip::Local(pip)));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => Ok(Some(Pip::Remote(RemotePip {
                session_id: pending.session_id,
                send: Some(pending.send),
                recv: pending.recv,
                rt: self.inner.rt.clone(),
                incoming_done: false,
                outgoing_done: false,
                qos: pending.qos,
                peer_chunks: pending.peer_chunks,
                stats: pending.stats,
                _phantom: PhantomData,
            }))),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn take_message(&mut self) -> Result<Option<PendingPipMessage<ClientMsg>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((session_id, first, incoming_done)) = local.server.take_message()? {
                return Ok(Some(PendingPipMessage {
                    session_id,
                    first: first.map(|sample| PipSample {
                        session_id: sample.session_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }),
                    incoming_done,
                    token: PipServerToken {
                        session_id,
                        source: PipServerTokenSource::Local { server_index },
                    },
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_session = self.next_pending_session.wrapping_add(1).max(1);
                let token = self.next_pending_session;
                let session_id = pending.session_id;
                self.pending_remote_sessions.insert(
                    token,
                    RemotePip {
                        session_id,
                        send: Some(pending.send),
                        recv: pending.recv,
                        rt: self.inner.rt.clone(),
                        incoming_done: false,
                        outgoing_done: false,
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(Some(PendingPipMessage {
                    session_id,
                    first: None,
                    incoming_done: false,
                    token: PipServerToken {
                        session_id,
                        source: PipServerTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn send_pending(&mut self, token: PipServerToken, msg: &ServerMsg) -> Result<()> {
        match token.source {
            PipServerTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local pip server token"))?;
                local.server.send_to(token.session_id, msg)
            }
            PipServerTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip server token"))?;
                pip.send(msg)
            }
        }
    }

    pub fn finish_send_pending(&mut self, token: PipServerToken) -> Result<()> {
        match token.source {
            PipServerTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local pip server token"))?;
                local.server.finish_send_to(token.session_id)
            }
            PipServerTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip server token"))?;
                pip.finish_send()
            }
        }
    }

    pub fn next_pending(&mut self, token: PipServerToken) -> Result<Option<PipSample<ClientMsg>>> {
        match token.source {
            PipServerTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local pip server token"))?;
                Ok(local
                    .server
                    .next_from(token.session_id)?
                    .map(|sample| PipSample {
                        session_id: sample.session_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }))
            }
            PipServerTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip server token"))?;
                pip.next()
            }
        }
    }

    pub fn close_pending(&mut self, token: PipServerToken) {
        if let PipServerTokenSource::Remote { token } = token.source {
            self.pending_remote_sessions.remove(&token);
        }
    }
}

pub struct PipClient<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    source: PipClientSource<ClientMsg, ServerMsg>,
    pending_remote_sessions: HashMap<u64, RemotePip<ClientMsg, ServerMsg>>,
    next_pending_session: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipSessionToken {
    session_id: u64,
    source: PipSessionTokenSource,
}

impl PipSessionToken {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipSessionTokenSource {
    Local,
    Remote { token: u64 },
}

enum PipClientSource<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalPipClient<ClientMsg, ServerMsg>,
    },
    Remote {
        inner: Arc<NodeInner>,
        peer_id: EndpointId,
        addr_hint: Option<EndpointAddr>,
        topic: String,
        next_id: AtomicU64,
        qos: TopicQos,
        stats: Arc<ItemStatsInner>,
    },
}

impl<ClientMsg, ServerMsg> PipClient<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this pip client.
    pub fn stats(&self) -> PipStats {
        match &self.source {
            PipClientSource::Remote { stats, .. } => stats.snapshot(),
            PipClientSource::Local { .. } => PipStats::default(),
        }
    }

    pub fn open(&mut self) -> Result<Pip<'_, ClientMsg, ServerMsg>> {
        match &mut self.source {
            PipClientSource::Local { client } => Ok(Pip::Local(client.open()?)),
            PipClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let session_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let client_type_hash = wire_type_hash::<ClientMsg>();
                let server_type_hash = wire_type_hash::<ServerMsg>();
                let client_header_size = std::mem::size_of::<ClientMsg::Header>() as u32;
                let server_header_size = std::mem::size_of::<ServerMsg::Header>() as u32;

                let rt = inner.rt.clone();
                let (send, recv) = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PIP_MAGIC,
                        &topic,
                        client_type_hash,
                        server_type_hash,
                        client_header_size,
                        server_header_size,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                Ok(Pip::Remote(RemotePip {
                    session_id,
                    send: Some(send),
                    recv,
                    rt,
                    incoming_done: false,
                    outgoing_done: false,
                    qos,
                    peer_chunks: true,
                    stats,
                    _phantom: PhantomData,
                }))
            }
        }
    }

    pub fn open_session(&mut self) -> Result<PipSessionToken> {
        match &mut self.source {
            PipClientSource::Local { client } => {
                let session_id = client.start_session();
                Ok(PipSessionToken {
                    session_id,
                    source: PipSessionTokenSource::Local,
                })
            }
            PipClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let session_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let client_type_hash = wire_type_hash::<ClientMsg>();
                let server_type_hash = wire_type_hash::<ServerMsg>();
                let client_header_size = std::mem::size_of::<ClientMsg::Header>() as u32;
                let server_header_size = std::mem::size_of::<ServerMsg::Header>() as u32;

                let rt = inner.rt.clone();
                let (send, recv) = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PIP_MAGIC,
                        &topic,
                        client_type_hash,
                        server_type_hash,
                        client_header_size,
                        server_header_size,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                self.next_pending_session = self.next_pending_session.wrapping_add(1).max(1);
                let token = self.next_pending_session;
                self.pending_remote_sessions.insert(
                    token,
                    RemotePip {
                        session_id,
                        send: Some(send),
                        recv,
                        rt,
                        incoming_done: false,
                        outgoing_done: false,
                        qos,
                        peer_chunks: true,
                        stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(PipSessionToken {
                    session_id,
                    source: PipSessionTokenSource::Remote { token },
                })
            }
        }
    }

    pub fn send_pending(&mut self, token: PipSessionToken, msg: &ClientMsg) -> Result<()> {
        match token.source {
            PipSessionTokenSource::Local => match &mut self.source {
                PipClientSource::Local { client } => client.send_to(token.session_id, msg),
                PipClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local pip token used with remote client",
                )),
            },
            PipSessionTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip token"))?;
                pip.send(msg)
            }
        }
    }

    pub fn finish_send_pending(&mut self, token: PipSessionToken) -> Result<()> {
        match token.source {
            PipSessionTokenSource::Local => match &mut self.source {
                PipClientSource::Local { client } => client.finish_send_to(token.session_id),
                PipClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local pip token used with remote client",
                )),
            },
            PipSessionTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip token"))?;
                pip.finish_send()
            }
        }
    }

    pub fn next_pending(&mut self, token: PipSessionToken) -> Result<Option<PipSample<ServerMsg>>> {
        match token.source {
            PipSessionTokenSource::Local => match &mut self.source {
                PipClientSource::Local { client } => {
                    Ok(client.next_from(token.session_id)?.map(|sample| PipSample {
                        session_id: sample.session_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }))
                }
                PipClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local pip token used with remote client",
                )),
            },
            PipSessionTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip token"))?;
                pip.next()
            }
        }
    }

    pub fn close_session(&mut self, token: PipSessionToken) {
        if let PipSessionTokenSource::Remote { token } = token.source {
            self.pending_remote_sessions.remove(&token);
        }
    }
}

pub struct PipSample<T: datapod::DataPod + 'static> {
    session_id: u64,
    header: T::Header,
    payload: Vec<u8>,
}

impl<T: datapod::DataPod + 'static> PipSample<T> {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn header(&self) -> &T::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum Pip<'a, Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::LocalPip<'a, Tx, Rx>),
    Remote(RemotePip<Tx, Rx>),
}

impl<Tx, Rx> Pip<'_, Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn session_id(&self) -> u64 {
        match self {
            Pip::Local(pip) => pip.session_id(),
            Pip::Remote(pip) => pip.session_id,
        }
    }

    pub fn send(&mut self, msg: &Tx) -> Result<()> {
        match self {
            Pip::Local(pip) => pip.send(msg),
            Pip::Remote(pip) => pip.send(msg),
        }
    }

    pub fn finish_send(&mut self) -> Result<()> {
        match self {
            Pip::Local(pip) => pip.finish_send(),
            Pip::Remote(pip) => pip.finish_send(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<PipSample<Rx>>> {
        match self {
            Pip::Local(pip) => Ok(pip.next()?.map(|sample| PipSample {
                session_id: sample.session_id(),
                header: *sample.header(),
                payload: sample.payload().to_vec(),
            })),
            Pip::Remote(pip) => pip.next(),
        }
    }
}

pub struct RemotePip<Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    session_id: u64,
    send: Option<iroh::endpoint::SendStream>,
    recv: iroh::endpoint::RecvStream,
    rt: Arc<Runtime>,
    incoming_done: bool,
    outgoing_done: bool,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
    _phantom: PhantomData<fn() -> (Tx, Rx)>,
}

impl<Tx, Rx> RemotePip<Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn send(&mut self, msg: &Tx) -> Result<()> {
        if self.outgoing_done {
            return Err(Error::invalid_argument("pip outgoing direction is done"));
        }
        let send = self
            .send
            .as_mut()
            .ok_or_else(|| Error::Remote("pip send stream is closed".to_string()))?;
        let frame = frame_from_datapod(msg);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let peer_chunks = self.peer_chunks;
        let result = self
            .rt
            .block_on(async move { write_pip_item(send, &frame, chunk_bytes, peer_chunks).await });
        match result {
            Ok(()) => {
                self.stats.record_out(frame_len);
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    fn finish_send(&mut self) -> Result<()> {
        if self.outgoing_done {
            return Ok(());
        }
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("pip send stream is closed".to_string()))?;
        self.rt.block_on(async move {
            write_pip_done(&mut send).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok::<_, Error>(())
        })?;
        self.outgoing_done = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<PipSample<Rx>>> {
        if self.incoming_done {
            return Ok(None);
        }
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let item = self.rt.block_on(async {
            read_pip_frame(&mut self.recv, max_message_bytes, max_inflight_bytes).await
        });
        let item = match item {
            Ok(i) => i,
            Err(e) => {
                self.stats.record_error();
                return Err(e);
            }
        };
        match item {
            Some(frame) => {
                self.stats.record_in(frame.len());
                Ok(Some(pip_sample_from_frame::<Rx>(self.session_id, &frame)?))
            }
            None => {
                self.incoming_done = true;
                Ok(None)
            }
        }
    }
}

struct RemotePendingPip {
    session_id: u64,
    recv: iroh::endpoint::RecvStream,
    send: iroh::endpoint::SendStream,
    qos: TopicQos,
    peer_chunks: bool,
    stats: Arc<ItemStatsInner>,
}

fn pip_sample_from_frame<T>(session_id: u64, frame: &[u8]) -> Result<PipSample<T>>
where
    T: datapod::DataPod + 'static,
    <T as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<T::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "pip frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(PipSample {
        session_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
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
                let conn = conn.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_bi(inner, conn, send, recv).await {
                        qb_warn!(target: "quicbit::node", error = %e, "subscriber stream failed");
                    }
                });
            }
            Err(_) => return Ok(()),
        }
    }
}

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn serve_bi(
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
async fn serve_pubsub_bi(
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
        target: "quicbit::node",
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
                        target: "quicbit::node",
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
                            target: "quicbit::node",
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
                        target: "quicbit::node",
                        topic = %topic,
                        dropped = n,
                        "reliable subscriber exceeded publisher queue capacity"
                    );
                    return Err(Error::Lagged { dropped: n });
                }
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

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn serve_reqres_bi(
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
                target: "quicbit::node",
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
                target: "quicbit::node",
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
async fn serve_queans_bi(
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
                target: "quicbit::node",
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
                target: "quicbit::node",
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
async fn serve_putack_bi(
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
                target: "quicbit::node",
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
                target: "quicbit::node",
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
async fn serve_pip_bi(
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
                target: "quicbit::node",
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
                target: "quicbit::node",
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

#[allow(clippy::too_many_arguments)]
fn maybe_register_pubsub_datagram_route(
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
async fn run_pubsub_datagram_reader(inner: Arc<NodeInner>, peer_id: EndpointId, conn: Connection) {
    let peer = *peer_id.as_bytes();
    let stable_id = conn.stable_id();
    loop {
        let datagram = match conn.read_datagram().await {
            Ok(datagram) => datagram,
            Err(e) => {
                qb_debug!(
                    target: "quicbit::node",
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
                    target: "quicbit::node",
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
                        target: "quicbit::node",
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

async fn pump_recv_stream(
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
async fn write_item_chunked(
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
async fn read_item_chunked(
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

async fn write_topic_handshake(
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

async fn read_topic_handshake_tail(
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

const ITEM_HS_V2_FIXED: usize = 4 + 8 + 8 + 4 + 4 + 2; // 30; topic_len at [28..30]
const ITEM_HS_QOS_EXTRA: usize = 8 + 8 + 4; // 20 (max_message, max_inflight, chunk_bytes)
const ITEM_HS_V3_FIXED: usize = ITEM_HS_V2_FIXED - 2 + ITEM_HS_QOS_EXTRA + 2; // 50; topic_len at [48..50]
const ITEM_TOPIC_LEN_LIMIT: usize = 1024;

/// Byte limits negotiated (or defaulted) for an item stream, plus
/// whether the peer advertised chunk support (handshake v3).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ItemHsQos {
    peer_chunks: bool,
    max_message_bytes: usize,
    max_inflight_bytes: usize,
    chunk_bytes: usize,
}

impl ItemHsQos {
    fn legacy() -> Self {
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
async fn write_item_handshake(
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
async fn read_item_handshake_tail(
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
        target: "quicbit::node",
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

async fn write_answer_item(
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

async fn write_answer_done(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[ANS_KIND_DONE])
        .await
        .map_err(|e| Error::Remote(format!("answer done write: {e}")))?;
    Ok(())
}

async fn read_answer_frame(
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

async fn write_put_item(
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

async fn write_put_done(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[PUT_KIND_DONE])
        .await
        .map_err(|e| Error::Remote(format!("put done write: {e}")))?;
    Ok(())
}

async fn read_put_frame(
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

async fn write_pip_item(
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

async fn write_pip_done(send: &mut iroh::endpoint::SendStream) -> Result<()> {
    send.write_all(&[PIP_KIND_DONE])
        .await
        .map_err(|e| Error::Remote(format!("pip done write: {e}")))?;
    Ok(())
}

async fn read_pip_frame(
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

async fn write_pubsub_message(
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

fn send_pubsub_datagrams(
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

fn make_pubsub_datagram(session_id: u64, chunk_payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PUBSUB_DATAGRAM_HEADER_LEN + chunk_payload.len());
    out.extend_from_slice(PUBSUB_DATAGRAM_MAGIC);
    out.extend_from_slice(&session_id.to_le_bytes());
    out.extend_from_slice(chunk_payload);
    out
}

fn parse_pubsub_datagram(bytes: &[u8]) -> Result<(u64, crate::chunk::ChunkFrame<'_>)> {
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

fn pubsub_datagram_session_id(
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

fn diagnose_peer_connection(peer: EndpointId, conn: &Connection) -> PeerPathDiagnostics {
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

async fn write_frame(send: &mut iroh::endpoint::SendStream, payload: &[u8]) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubsub_datagram_frame_round_trips() {
        let chunk = make_chunk_payload(7, 0, 1, 3, b"abc").unwrap();
        let datagram = make_pubsub_datagram(42, &chunk);
        let (session_id, frame) = parse_pubsub_datagram(&datagram).unwrap();
        assert_eq!(session_id, 42);
        assert_eq!(frame.message_id, 7);
        assert_eq!(frame.chunk_index, 0);
        assert_eq!(frame.chunk_count, 1);
        assert_eq!(frame.message_len, 3);
        assert_eq!(frame.chunk, b"abc");
    }

    #[test]
    fn pubsub_datagram_session_id_is_endpoint_ordered() {
        let publisher = SecretKey::generate().public();
        let subscriber = SecretKey::generate().public();
        let a = pubsub_datagram_session_id(publisher, subscriber, "demo/video", 0x12, 8);
        let b = pubsub_datagram_session_id(publisher, subscriber, "demo/video", 0x12, 8);
        let reversed = pubsub_datagram_session_id(subscriber, publisher, "demo/video", 0x12, 8);
        assert_eq!(a, b);
        assert_ne!(a, reversed);
    }

    fn item_tail_v2(topic: &str) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
        b.extend_from_slice(&0x1111_2222_3333_4444u64.to_le_bytes()); // hash_a
        b.extend_from_slice(&0x5555_6666_7777_8888u64.to_le_bytes()); // hash_b
        b.extend_from_slice(&12u32.to_le_bytes()); // size_a
        b.extend_from_slice(&34u32.to_le_bytes()); // size_b
        b.extend_from_slice(&(topic.len() as u16).to_le_bytes());
        b.extend_from_slice(topic.as_bytes());
        b
    }

    fn item_tail_v3(topic: &str) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&ITEM_HANDSHAKE_VERSION_CHUNKED.to_le_bytes());
        b.extend_from_slice(&0x1111_2222_3333_4444u64.to_le_bytes()); // hash_a
        b.extend_from_slice(&0x5555_6666_7777_8888u64.to_le_bytes()); // hash_b
        b.extend_from_slice(&12u32.to_le_bytes()); // size_a
        b.extend_from_slice(&34u32.to_le_bytes()); // size_b
        b.extend_from_slice(&(1024u64 * 1024).to_le_bytes()); // max_message_bytes
        b.extend_from_slice(&(8u64 * 1024 * 1024).to_le_bytes()); // max_inflight_bytes
        b.extend_from_slice(&(64u32 * 1024).to_le_bytes()); // chunk_bytes
        b.extend_from_slice(&(topic.len() as u16).to_le_bytes());
        b.extend_from_slice(topic.as_bytes());
        b
    }

    #[test]
    fn item_handshake_v2_tail_parses_without_chunking() {
        let (topic, a, b, sa, sb, qos) =
            parse_item_handshake_tail(&item_tail_v2("calc/add")).unwrap();
        assert_eq!(topic, "calc/add");
        assert_eq!(a, 0x1111_2222_3333_4444);
        assert_eq!(b, 0x5555_6666_7777_8888);
        assert_eq!(sa, 12);
        assert_eq!(sb, 34);
        assert!(!qos.peer_chunks, "v2 peers must not be told to chunk");
    }

    #[test]
    fn item_handshake_v3_tail_round_trips_byte_limits() {
        let (topic, _, _, _, _, qos) =
            parse_item_handshake_tail(&item_tail_v3("map/tiles")).unwrap();
        assert_eq!(topic, "map/tiles");
        assert!(qos.peer_chunks);
        assert_eq!(qos.max_message_bytes, 1024 * 1024);
        assert_eq!(qos.max_inflight_bytes, 8 * 1024 * 1024);
        assert_eq!(qos.chunk_bytes, 64 * 1024);
    }

    #[test]
    fn item_handshake_rejects_unknown_version() {
        let mut tail = item_tail_v2("x");
        tail[0..4].copy_from_slice(&99u32.to_le_bytes());
        assert!(matches!(
            parse_item_handshake_tail(&tail),
            Err(Error::HandshakeVersionMismatch { .. })
        ));
    }

    #[test]
    fn item_handshake_rejects_truncated() {
        assert!(parse_item_handshake_tail(&[]).is_err());
        let tail = item_tail_v3("topic");
        // Truncate inside the v3 byte-limit region.
        assert!(parse_item_handshake_tail(&tail[..32]).is_err());
    }
}
