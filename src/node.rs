//! `Node` — the unified, topic-shaped entry point.
//!
//! One `Node` per process. Internally:
//!
//! * Owns a single [`iroh::Endpoint`] (cross-host transport).
//! * Owns a [`crate::registry::Registry`] handle (same-host
//!   discovery of peer `EndpointId`s).
//! * Hosts a single accept loop that dispatches incoming iroh
//!   bi streams to registered publishers' broadcast queues by
//!   topic name.
//! * Caches outbound iroh `Connection`s, one per peer, reused
//!   across topics.
//!
//! User-facing API:
//!
//! ```no_run
//! use bytemuck::{Pod, Zeroable};
//! use quicbit::Node;
//!
//! # async fn run() -> quicbit::Result<()> {
//! # #[repr(C)] #[derive(Clone, Copy, Pod, Zeroable)] struct Pose;
//! let node = Node::builder().no_relay().bind()?;
//! let mut pubr = node.publisher::<Pose>("rover/pose")?;
//! // *pubr.loan()?... pubr.publish(loan)?;
//! # Ok(()) }
//! ```
//!
//! The unified `Publisher` / `Subscriber` types abstract over
//! whether the peer is on the same host (SHM) or remote (iroh).
//! Subscribers learn the routing decision from the registry; if
//! the peer's `EndpointId` is present in the host registry, the
//! subscriber attaches to the publisher's SHM segment, otherwise
//! it dials over iroh.

use std::any::type_name;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytemuck::Pod;
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, OnceCell};

use crate::error::{Error, Result};
use crate::local::layout::fnv1a64;
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::local::{Loan, Sample};
use crate::registry::Registry;
use crate::remote::runtime;

const DEFAULT_ALPN: &[u8] = b"quicbit/1";
const DEFAULT_BROADCAST_CAPACITY: usize = 256;
/// Domain separation tag for the `.identity(name)` → `SecretKey`
/// derivation. Bump if we ever need to change the derivation.
const IDENTITY_DERIVATION_TAG: &[u8] = b"quicbit/v1/identity";

/// How a [`Node`] obtains its iroh `SecretKey`.
#[derive(Debug, Clone)]
enum IdentitySource {
    /// Generate a fresh random key on every `bind`.
    Ephemeral,
    /// Hash a human-readable name to a deterministic key. Same
    /// name on two machines → same `EndpointId`. Anyone with the
    /// string can impersonate; only use in trusted contexts.
    Name(String),
    /// Load 32 raw bytes from disk; generate + persist if missing.
    File(PathBuf),
    /// Read an env var, then derive a name-based key from its value.
    /// Same security shape as `Name`.
    Env(String),
}

// ---- peer addressing ----

/// A peer's address: always an `EndpointId`, optionally with the
/// human-readable name that derived it. The name (if present)
/// lets the subscriber find the publisher's SHM segment without
/// hitting the registry.
#[derive(Clone, Debug)]
pub struct Peer {
    pub endpoint_id: EndpointId,
    pub name: Option<String>,
}

/// Trait for "anything you can call a peer." `&str` / `String` →
/// hashes to an `EndpointId` via the same derivation that
/// [`NodeBuilder::identity`] uses. `EndpointId` → passes through.
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
        }
    }
}

impl IntoPeer for &str {
    fn into_peer(self) -> Peer {
        let secret = derive_secret_from_name(self);
        Peer {
            endpoint_id: secret.public(),
            name: Some(self.to_string()),
        }
    }
}

impl IntoPeer for String {
    fn into_peer(self) -> Peer {
        let secret = derive_secret_from_name(&self);
        Peer {
            endpoint_id: secret.public(),
            name: Some(self),
        }
    }
}

// ---- builder ----

/// Builder for [`Node`]. See [`Node::builder`].
pub struct NodeBuilder {
    identity: IdentitySource,
    alpn: Vec<u8>,
    no_relay: bool,
    slot_count: u32,
    slot_size: u32,
    history_depth: u32,
}

impl NodeBuilder {
    /// Use a literal name as the node's identity. The name hashes
    /// (blake3, domain-separated) to a deterministic `SecretKey`,
    /// and therefore to a stable `EndpointId`. Same name on two
    /// machines → same identity.
    ///
    /// **Trust model**: anyone with the source string IS this
    /// node. Use for dev, tests, trusted networks, fleet
    /// bootstrap. For real authentication, use
    /// [`Self::identity_file`].
    ///
    /// Also drives SHM segment naming: a publisher with
    /// `identity("rover-a")` writes its topic `"rover/pose"` to
    /// `quicbit.rover-a__rover_pose`. Same-host subscribers find
    /// it by name without any registry lookup.
    pub fn identity(mut self, name: impl Into<String>) -> Self {
        self.identity = IdentitySource::Name(name.into());
        self
    }

    /// Read 32 raw bytes from `path` as the `SecretKey`. Generate
    /// and write a fresh random key if the file doesn't exist
    /// (with `0o600` perms on Unix).
    ///
    /// This is the cryptographically meaningful path — the key is
    /// genuinely random and lives only in the file. Lose the file,
    /// lose the identity.
    pub fn identity_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.identity = IdentitySource::File(path.into());
        self
    }

    /// Read the env var `var` and use its value as a name (same
    /// hashing as [`Self::identity`]).
    pub fn identity_env(mut self, var: impl Into<String>) -> Self {
        self.identity = IdentitySource::Env(var.into());
        self
    }

    /// Backwards-compat alias for [`Self::identity_file`].
    #[doc(hidden)]
    pub fn secret_key_file(self, path: impl Into<PathBuf>) -> Self {
        self.identity_file(path)
    }

    /// Override the ALPN. Defaults to `b"quicbit/1"`.
    pub fn alpn(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.alpn = alpn.into();
        self
    }

    /// Disable iroh relays. Required for loopback / direct-only
    /// deployments.
    pub fn no_relay(mut self) -> Self {
        self.no_relay = true;
        self
    }

    /// SHM slot pool size for publishers created by this node.
    /// Defaults to 16.
    pub fn slot_count(mut self, count: u32) -> Self {
        self.slot_count = count;
        self
    }

    /// SHM per-slot payload size. Defaults to 256 bytes (rounded
    /// up to an 8-byte boundary in the segment).
    pub fn slot_size(mut self, size: u32) -> Self {
        self.slot_size = size;
        self
    }

    /// SHM ring history depth (samples retained for late
    /// subscribers). Defaults to 1.
    pub fn history_depth(mut self, depth: u32) -> Self {
        self.history_depth = depth;
        self
    }

    /// Build the node. Sync; internally blocks on iroh's `bind`
    /// future using the crate's shared tokio runtime.
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

        let mut registry = Registry::open()?;
        registry.claim(*endpoint_id.as_bytes())?;

        let inner = Arc::new(NodeInner {
            endpoint,
            endpoint_id,
            identity_name,
            alpn: self.alpn,
            registry: Mutex::new(registry),
            publisher_topics: Mutex::new(HashMap::new()),
            peer_connections: Mutex::new(HashMap::new()),
            cfg: LocalConfig {
                slot_count: self.slot_count,
                slot_size: self.slot_size,
                history_depth: self.history_depth,
            },
        });

        // Spawn the iroh accept loop.
        {
            let inner = inner.clone();
            rt.spawn(async move {
                let _ = run_accept_loop(inner).await;
            });
        }

        Ok(Node { inner, rt })
    }
}

// ---- core types ----

/// One-per-process anchor that owns the iroh endpoint, registry,
/// and per-topic publisher state. Cheap to clone (`Arc`-backed).
#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
    rt: Arc<Runtime>,
}

struct NodeInner {
    endpoint: Endpoint,
    endpoint_id: EndpointId,
    /// User-supplied identity name, if `.identity(...)` or
    /// `.identity_env(...)` was used. Drives SHM segment naming
    /// (`quicbit.<identity_name>__<topic>`); falls back to hex of
    /// the `EndpointId` when `None` (ephemeral / file-keyed nodes).
    identity_name: Option<String>,
    alpn: Vec<u8>,
    registry: Mutex<Registry>,
    /// Per-topic publisher state. The `broadcast::Sender` here is
    /// the iroh fan-out side: the accept loop subscribes new
    /// receivers when a remote peer opens a bi stream for this
    /// topic.
    publisher_topics: Mutex<HashMap<String, PublisherTopicState>>,
    /// Per-peer iroh `Connection`s, lazy-dialled and reused across
    /// topics.
    peer_connections: Mutex<HashMap<[u8; 32], Arc<OnceCell<Connection>>>>,
    /// Default SHM config for new publishers.
    cfg: LocalConfig,
}

struct PublisherTopicState {
    /// Broadcast for the iroh fan-out side. Each remote subscriber
    /// gets its own `Receiver`.
    iroh_tx: broadcast::Sender<Vec<u8>>,
    /// Type hash, validated against the handshake type_hash.
    type_hash: u64,
    /// Payload size for handshake validation.
    payload_size: u32,
}

impl Node {
    /// Entry point: returns a builder. See [`NodeBuilder`].
    pub fn builder() -> NodeBuilder {
        NodeBuilder {
            identity: IdentitySource::Ephemeral,
            alpn: DEFAULT_ALPN.to_vec(),
            no_relay: false,
            slot_count: 16,
            slot_size: 256,
            history_depth: 1,
        }
    }

    /// User-supplied identity name, if any.
    pub fn identity_name(&self) -> Option<&str> {
        self.inner.identity_name.as_deref()
    }

    /// This node's iroh `EndpointId`. Stable if the builder was
    /// pointed at a persistent `secret_key_file`; ephemeral
    /// otherwise.
    pub fn endpoint_id(&self) -> EndpointId {
        self.inner.endpoint_id
    }

    /// This node's currently advertised iroh `EndpointAddr`
    /// (EndpointId + relay URL + direct addresses).
    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.inner.endpoint.addr()
    }

    /// Block until the endpoint has at least one transport address.
    /// Useful for loopback tests that need to read the address
    /// before the peer dials in.
    pub fn wait_for_direct_addresses(&self) {
        let endpoint = self.inner.endpoint.clone();
        self.rt.block_on(async move {
            loop {
                if !endpoint.addr().is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
    }

    /// Build a publisher for `topic`. The publisher writes to:
    ///
    /// * an SHM segment named
    ///   `quicbit.<endpoint_id_hex>__<topic>` — same-host
    ///   subscribers attach to this and read with zero copy,
    /// * and a broadcast queue that fans out to whichever iroh
    ///   subscribers are currently attached to the topic.
    ///
    /// Both writes happen on every `publish()`; if no subscribers
    /// of a kind are attached, that path is a no-op (the SHM
    /// segment still records the publish; the broadcast send
    /// errors are swallowed when there are zero receivers).
    pub fn publisher<T: Pod>(&self, topic: &str) -> Result<Publisher<T>> {
        let type_name = type_name::<T>();
        let type_hash = fnv1a64(type_name);
        let payload_size = std::mem::size_of::<T>() as u32;

        // Register / fetch the topic's iroh fan-out broadcast tx.
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

        // Create the SHM segment for the local path.
        let segment_name = shm_segment_name(
            self.inner.identity_name.as_deref(),
            self.inner.endpoint_id.as_bytes(),
            topic,
        );
        let local =
            LocalService::<T>::open_or_create(&segment_name, self.inner.cfg.clone())?;
        let local_publisher = local.publisher();

        Ok(Publisher {
            local_publisher,
            _local_service: local,
            iroh_tx,
        })
    }

    /// Subscribe to `peer`'s publication of `topic`.
    ///
    /// Routing:
    /// * Registry hit on `peer` → attach to the publisher's SHM
    ///   segment (true zero-copy on the take path).
    /// * Registry miss → open (or reuse) an iroh `Connection` to
    ///   `peer`, open a bi stream, write the topic handshake, and
    ///   pump bytes into an internal queue.
    pub fn subscriber<T: Pod>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<Subscriber<T>> {
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        // Same-host detection: prefer the name-based segment if we
        // have a name (zero registry hit, just one `shm_open`).
        // Otherwise fall back to the registry → hex-named segment.
        let segment_name = shm_segment_name(peer.name.as_deref(), &peer_bytes, topic);
        let try_local = if peer.name.is_some() {
            true
        } else {
            let reg = self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            reg.lookup(&peer_bytes).is_some()
        };

        if try_local && let Ok(svc) = LocalService::<T>::attach(&segment_name) {
            let local_sub = svc.subscriber();
            return Ok(Subscriber {
                source: SubscriberSource::Local {
                    sub: local_sub,
                    _svc: svc,
                },
            });
        }
        // Either we didn't try local, or the attach failed —
        // publisher might be elsewhere with the same name. Fall
        // through to the iroh path.

        // Remote path: ensure a connection to the peer + open a bi
        // stream with the topic handshake.
        let inner = self.inner.clone();
        let alpn = self.inner.alpn.clone();
        let topic_owned = topic.to_string();
        let type_hash = fnv1a64(type_name::<T>());
        let payload_size = std::mem::size_of::<T>() as u32;
        let peer_id = peer.endpoint_id;

        let (rx_handle, _bg_task) = self.rt.block_on(async move {
            let conn = ensure_peer_connection(&inner, peer_id).await?;
            let (mut send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
            write_topic_handshake(&mut send, &topic_owned, type_hash, payload_size).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;

            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(DEFAULT_BROADCAST_CAPACITY);
            let task = tokio::spawn(async move {
                let _ = pump_recv_stream(recv, tx).await;
            });
            Ok::<_, Error>((rx, task))
        })?;

        let _ = alpn; // ALPN already wired into Endpoint::builder
        Ok(Subscriber {
            source: SubscriberSource::Remote { rx: rx_handle },
        })
    }
}

// ---- publisher ----

/// Unified publisher returned by [`Node::publisher`].
pub struct Publisher<T: Pod> {
    local_publisher: LocalPublisher<T>,
    _local_service: LocalService<T>,
    iroh_tx: broadcast::Sender<Vec<u8>>,
}

impl<T: Pod> Publisher<T> {
    /// Reserve a slot for in-place writes. Same semantics as the
    /// local transport's `Loan<T>`.
    pub fn loan(&mut self) -> Result<Loan<T>> {
        self.local_publisher.loan()
    }

    /// Publish the loan. Writes go to BOTH:
    /// * the SHM segment (local subscribers read here),
    /// * the iroh fan-out broadcast (remote subscribers read here).
    pub fn publish(&mut self, loan: Loan<T>) -> Result<u64> {
        // Snapshot the payload bytes before we hand the loan off
        // to the local publisher (which transfers the slot to the
        // SHM ring). Cheap copy of size_of::<T>() bytes.
        let bytes: Vec<u8> = bytemuck::bytes_of(&*loan).to_vec();
        let seq = self.local_publisher.publish(loan)?;
        // No-op if zero subscribers; we don't care.
        let _ = self.iroh_tx.send(bytes);
        Ok(seq)
    }

    /// Shortcut: loan + write + publish.
    pub fn send(&mut self, value: T) -> Result<u64> {
        let mut loan = self.loan()?;
        *loan = value;
        self.publish(loan)
    }
}

// ---- subscriber ----

/// Unified subscriber returned by [`Node::subscriber`].
pub struct Subscriber<T: Pod> {
    source: SubscriberSource<T>,
}

enum SubscriberSource<T: Pod> {
    Local {
        sub: LocalSubscriber<T>,
        _svc: LocalService<T>,
    },
    Remote {
        rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    },
}

impl<T: Pod> Subscriber<T> {
    /// Non-blocking take. Returns:
    /// * `Ok(Some(sample))` — sample available.
    /// * `Ok(None)` — no new sample yet.
    /// * `Err(Lagged { .. })` — subscriber fell behind.
    pub fn take(&mut self) -> Result<Option<NodeSample<T>>> {
        match &mut self.source {
            SubscriberSource::Local { sub, .. } => Ok(sub.take()?.map(NodeSample::Local)),
            SubscriberSource::Remote { rx } => match rx.try_recv() {
                Ok(bytes) => {
                    if bytes.len() != std::mem::size_of::<T>() {
                        return Err(Error::Remote(format!(
                            "frame size mismatch: got {} bytes, expected {}",
                            bytes.len(),
                            std::mem::size_of::<T>()
                        )));
                    }
                    // SAFETY: T: Pod + length match guarantees a
                    // valid value of T for any bytes of the right
                    // size.
                    let value: T = *bytemuck::from_bytes(&bytes);
                    Ok(Some(NodeSample::Remote { value }))
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
            },
        }
    }
}

/// Unified sample. Derefs to `T` regardless of transport.
pub enum NodeSample<T: Pod> {
    Local(Sample<T>),
    Remote { value: T },
}

impl<T: Pod> std::ops::Deref for NodeSample<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match self {
            NodeSample::Local(s) => s,
            NodeSample::Remote { value } => value,
        }
    }
}

// ---- iroh accept side ----

async fn run_accept_loop(inner: Arc<NodeInner>) -> Result<()> {
    while let Some(incoming) = inner.endpoint.accept().await {
        let inner = inner.clone();
        tokio::spawn(async move {
            let mut accepting = match incoming.accept() {
                Ok(a) => a,
                Err(_) => return,
            };
            let _alpn = match accepting.alpn().await {
                Ok(a) => a,
                Err(_) => return,
            };
            let conn = match accepting.await {
                Ok(c) => c,
                Err(_) => return,
            };
            let _ = serve_incoming_connection(inner, conn).await;
        });
    }
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

async fn serve_bi(
    inner: Arc<NodeInner>,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let (topic, type_hash, payload_size) = read_topic_handshake(&mut recv).await?;
    let mut rx = {
        let map = inner
            .publisher_topics
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        match map.get(&topic) {
            Some(state) => {
                if state.type_hash != type_hash || state.payload_size != payload_size {
                    return Err(Error::TypeMismatch {
                        expected: "<publisher type>",
                        got: format!("hash=0x{type_hash:x} size={payload_size}"),
                    });
                }
                state.iroh_tx.subscribe()
            }
            None => return Ok(()), // no publisher registered, drop quietly
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
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
        }
    }
}

async fn pump_recv_stream(
    mut recv: iroh::endpoint::RecvStream,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        match recv.read_exact(&mut len_buf).await {
            Ok(()) => {}
            Err(_) => return Ok(()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 16 * 1024 * 1024 {
            return Err(Error::Remote(format!("frame too large: {len}")));
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

// ---- iroh outbound side ----

async fn ensure_peer_connection(
    inner: &Arc<NodeInner>,
    peer: EndpointId,
) -> Result<Connection> {
    let peer_bytes: [u8; 32] = *peer.as_bytes();
    let cell = {
        let mut map = inner
            .peer_connections
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        map.entry(peer_bytes)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    };
    let alpn = inner.alpn.clone();
    let endpoint = inner.endpoint.clone();
    cell.get_or_try_init(|| async move {
        endpoint
            .connect(EndpointAddr::new(peer), &alpn)
            .await
            .map_err(|e| Error::Remote(format!("connect: {e}")))
    })
    .await
    .cloned()
}

// ---- handshake helpers ----

const TOPIC_HANDSHAKE_MAGIC: u32 = 0x3354_4251; // "QBT3"
const TOPIC_HANDSHAKE_VERSION: u32 = 1;

async fn write_topic_handshake(
    send: &mut iroh::endpoint::SendStream,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
) -> Result<()> {
    if topic.len() > 1024 {
        return Err(Error::invalid_argument("topic too long"));
    }
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 4 + 2 + topic.len());
    buf.extend_from_slice(&TOPIC_HANDSHAKE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&TOPIC_HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&type_hash.to_le_bytes());
    buf.extend_from_slice(&payload_size.to_le_bytes());
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("handshake: {e}")))
}

async fn read_topic_handshake(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(String, u64, u32)> {
    let mut header = [0u8; 4 + 4 + 8 + 4 + 2];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| Error::Remote(format!("handshake: {e}")))?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != TOPIC_HANDSHAKE_MAGIC {
        return Err(Error::Remote(format!("bad magic 0x{magic:x}")));
    }
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
    if version != TOPIC_HANDSHAKE_VERSION {
        return Err(Error::Remote(format!("version {version}")));
    }
    let type_hash = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let payload_size = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let topic_len = u16::from_le_bytes(header[20..22].try_into().unwrap());
    let mut topic_buf = vec![0u8; topic_len as usize];
    recv.read_exact(&mut topic_buf)
        .await
        .map_err(|e| Error::Remote(format!("topic: {e}")))?;
    let topic = String::from_utf8(topic_buf)
        .map_err(|_| Error::Remote("topic not UTF-8".to_string()))?;
    Ok((topic, type_hash, payload_size))
}

async fn write_frame(send: &mut iroh::endpoint::SendStream, bytes: &[u8]) -> Result<()> {
    send.write_all(&(bytes.len() as u32).to_le_bytes())
        .await
        .map_err(|e| Error::Remote(format!("frame header: {e}")))?;
    send.write_all(bytes)
        .await
        .map_err(|e| Error::Remote(format!("frame body: {e}")))
}

// ---- segment naming + key persistence ----

/// Convert an identity name to a deterministic 32-byte
/// `SecretKey` via blake3, with a domain-separation tag so this
/// derivation can never collide with someone else's
/// `blake3("rover-a")`.
fn derive_secret_from_name(name: &str) -> SecretKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(IDENTITY_DERIVATION_TAG);
    hasher.update(b"\0");
    hasher.update(name.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(hasher.finalize().as_bytes());
    SecretKey::from_bytes(&out)
}

/// Resolve an [`IdentitySource`] to a `SecretKey` and (optionally)
/// the user-supplied name that drives SHM segment naming. The
/// name is only `Some` for `Name`/`Env` variants — file and
/// ephemeral keys have no public name to share.
fn resolve_identity(src: &IdentitySource) -> Result<(SecretKey, Option<String>)> {
    match src {
        IdentitySource::Ephemeral => Ok((SecretKey::generate(), None)),
        IdentitySource::Name(name) => {
            Ok((derive_secret_from_name(name), Some(name.clone())))
        }
        IdentitySource::Env(var) => {
            let name = std::env::var(var).map_err(|_| {
                Error::invalid_argument(format!(
                    "identity_env: env var '{var}' is not set"
                ))
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

/// SHM segment name for a topic. Uses the user-supplied identity
/// name if present (`quicbit.<name>__<topic>`), otherwise the
/// 64-hex `EndpointId` (`quicbit.<hex>__<topic>`).
pub fn shm_segment_name(identity_name: Option<&str>, endpoint_id: &[u8; 32], topic: &str) -> String {
    let sanitised_topic = sanitise(topic);
    match identity_name {
        Some(name) => format!("{}__{sanitised_topic}", sanitise(name)),
        None => {
            let mut hex = String::with_capacity(64);
            for b in endpoint_id.iter() {
                let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{b:02x}"));
            }
            format!("{hex}__{sanitised_topic}")
        }
    }
}

fn sanitise(s: &str) -> String {
    s.replace(['/', '.', ' '], "_")
}

fn load_or_generate_key(path: &Path) -> Result<SecretKey> {
    match fs::File::open(path) {
        Ok(mut f) => {
            let mut bytes = [0u8; 32];
            f.read_exact(&mut bytes)
                .map_err(|e| Error::invalid_argument(format!("read key: {e}")))?;
            Ok(SecretKey::from_bytes(&bytes))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretKey::generate();
            let bytes = key.to_bytes();
            let mut f = fs::File::create(path)
                .map_err(|e| Error::invalid_argument(format!("create key file: {e}")))?;
            f.write_all(&bytes)
                .map_err(|e| Error::invalid_argument(format!("write key: {e}")))?;
            // Best-effort secure perms (0o600). Failure is non-fatal.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o600);
                let _ = fs::set_permissions(path, perms);
            }
            Ok(key)
        }
        Err(e) => Err(Error::invalid_argument(format!("open key file: {e}"))),
    }
}
