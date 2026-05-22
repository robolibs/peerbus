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
//! * For same-host routing, asks iceoryx2 whether a service
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
use crate::transport::wire_type_hash;
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
/// service the same way → iceoryx2 service open succeeds → SHM.
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

    /// Tune the default iceoryx2 QoS used for publishers / subscribers
    /// this node creates. Per-`publisher`/`subscriber` overrides are
    /// not exposed yet.
    pub fn local_config(mut self, cfg: LocalConfig) -> Self {
        self.local_cfg = cfg;
        self
    }

    pub fn bind(self) -> Result<Node> {
        let rt = runtime::shared()?;
        let (secret, identity_name) = resolve_identity(&self.identity)?;
        let endpoint_id = secret.public();
        let alpn = self.alpn.clone();
        let no_relay = self.no_relay;

        let endpoint: Endpoint = rt.block_on(async move {
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
        })?;

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
            alpn: self.alpn,
            local_cfg: self.local_cfg,
            publisher_topics: Mutex::new(HashMap::new()),
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
        *inner
            .accept_handle
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(accept_handle);

        Ok(Node { inner, rt })
    }
}

impl Drop for NodeInner {
    fn drop(&mut self) {
        // Stop accepting new inbound connections.
        if let Some(handle) = self
            .accept_handle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
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
    alpn: Vec<u8>,
    local_cfg: LocalConfig,
    publisher_topics: Mutex<HashMap<String, PublisherTopicState>>,
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

    /// Snapshot of node-level operational counters. Cheap; takes
    /// the publisher/peer maps' locks briefly to read sizes.
    pub fn stats(&self) -> NodeStats {
        NodeStats {
            publisher_topics: self
                .inner
                .publisher_topics
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len(),
            cached_peers: self
                .inner
                .peer_connections
                .lock()
                .unwrap_or_else(|p| p.into_inner())
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
    /// * iceoryx2 service named `<identity>__<topic>` (same-host
    ///   subscribers attach to this and read zero-copy);
    /// * a broadcast queue feeding the iroh accept loop, which
    ///   serves attached remote subscribers.
    pub fn publisher<T>(&self, topic: &str) -> Result<Publisher<T>>
    where
        T: datapod::DataPod + 'static,
    {
        let type_name = type_name::<T>();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;

        let iroh_tx = {
            let mut map = self
                .inner
                .publisher_topics
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let entry = map.entry(topic.to_string()).or_insert_with(|| {
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

        let svc_name = service_name(
            self.inner.identity_name.as_deref(),
            self.inner.endpoint_id.as_bytes(),
            topic,
        );
        let service = LocalService::<T>::open_or_create(&svc_name, self.inner.local_cfg.clone())?;
        let local_publisher = service.publisher()?;

        Ok(Publisher {
            local_publisher,
            _local_service: service,
            iroh_tx,
            published: Arc::new(AtomicU64::new(0)),
            remote_dropped: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Subscribe to `peer`'s publication of `topic`.
    ///
    /// Routing:
    /// * If we can open an existing iceoryx2 service for the
    ///   peer + topic on this host → attach locally (zero copy).
    /// * Otherwise → dial `peer` over iroh, open a bi stream,
    ///   write the topic handshake, pump received frames to an
    ///   internal queue.
    pub fn subscriber<T>(&self, peer: impl IntoPeer, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
    {
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        // Try local first if we have a name (iceoryx2's open-only
        // call returns Err if the service hasn't been created
        // anywhere on the host).
        if peer.name.is_some() {
            let svc_name = service_name(peer.name.as_deref(), &peer_bytes, topic);
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

        // Remote path. Do one synchronous dial + handshake so the
        // caller sees a hard failure if the peer is unreachable
        // *at construction time*; after that, the background loop
        // owns reconnect.
        let inner = self.inner.clone();
        let topic_owned = topic.to_string();
        let type_hash = wire_type_hash::<T>();
        let payload_size = std::mem::size_of::<T>() as u32;
        let peer_id = peer.endpoint_id;
        let addr_hint = peer.addr.clone();

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

pub struct Publisher<T: datapod::DataPod + 'static> {
    local_publisher: LocalPublisher<T>,
    _local_service: LocalService<T>,
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
        // Snapshot header + payload bytes before iceoryx2 consumes
        // the loan; needed for the iroh broadcast.
        let header_bytes = bytemuck::bytes_of(loan.header()).to_vec();
        let payload_bytes = loan.payload().to_vec();
        let mut frame = Vec::with_capacity(header_bytes.len() + payload_bytes.len());
        frame.extend_from_slice(&header_bytes);
        frame.extend_from_slice(&payload_bytes);
        let seq = self.local_publisher.publish(loan)?;
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
/// regardless of whether the message arrived via iceoryx2 (Local) or
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
        let map = inner
            .publisher_topics
            .lock()
            .unwrap_or_else(|p| p.into_inner());
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
    let bytes = topic.as_bytes();
    let mut buf = Vec::with_capacity(4 + bytes.len() + 8 + 4);
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
    buf.extend_from_slice(&type_hash.to_le_bytes());
    buf.extend_from_slice(&payload_size.to_le_bytes());
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("topic handshake write: {e}")))?;
    Ok(())
}

async fn read_topic_handshake(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(String, u64, u32)> {
    const NODE_TOPIC_LEN_LIMIT: usize = 1024;
    let mut topic_len = [0u8; 4];
    recv.read_exact(&mut topic_len)
        .await
        .map_err(|e| Error::HandshakeMalformed(format!("handshake topic_len: {e}")))?;
    let n = u32::from_le_bytes(topic_len) as usize;
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
    let mut hash = [0u8; 8];
    recv.read_exact(&mut hash)
        .await
        .map_err(|e| Error::Remote(format!("handshake hash: {e}")))?;
    let mut size = [0u8; 4];
    recv.read_exact(&mut size)
        .await
        .map_err(|e| Error::Remote(format!("handshake size: {e}")))?;
    let topic = String::from_utf8(topic)
        .map_err(|e| Error::Remote(format!("handshake topic utf8: {e}")))?;
    Ok((
        topic,
        u64::from_le_bytes(hash),
        u32::from_le_bytes(size),
    ))
}

async fn write_frame(
    send: &mut iroh::endpoint::SendStream,
    payload: &[u8],
) -> Result<()> {
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
        let mut map = inner
            .peer_connections
            .lock()
            .unwrap_or_else(|p| p.into_inner());
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
        Ok(SecretKey::from_bytes(&buf))
    } else {
        let key = SecretKey::generate();
        let bytes: [u8; 32] = key.to_bytes();
        let mut f =
            fs::File::create(path).map_err(|e| Error::Other(format!("create key file: {e}")))?;
        f.write_all(&bytes)
            .map_err(|e| Error::Other(format!("write key file: {e}")))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ =
                fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(key)
    }
}

// ---- service-name composition ----

/// iceoryx2 service name = `<identity_name|hex_endpoint_id>__<sanitised_topic>`.
/// iceoryx2 accepts `/`, `.`, alphanumerics — we still sanitise spaces and a
/// few oddities so the name is friendly to look at via the iceoryx2 tooling.
pub fn service_name(
    identity_name: Option<&str>,
    endpoint_id: &[u8; 32],
    topic: &str,
) -> String {
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
