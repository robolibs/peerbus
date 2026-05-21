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
//! use bytemuck::{Pod, Zeroable};
//! use iceoryx2::prelude::ZeroCopySend;
//! use quicbit::Node;
//!
//! # fn run() -> quicbit::Result<()> {
//! # #[repr(C)]
//! # #[derive(Clone, Copy, Debug, Pod, Zeroable, ZeroCopySend)] struct Pose;
//! let node = Node::builder().no_relay().bind()?;
//! let mut pubr = node.publisher::<Pose>("rover/pose")?;
//! // *pubr.loan()?... pubr.publish(loan)?;
//! # Ok(()) }
//! ```

use core::fmt::Debug;
use std::any::type_name;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytemuck::Pod;
use iceoryx2::prelude::ZeroCopySend;
use iroh::endpoint::{Connection, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::runtime::Runtime;
use tokio::sync::{OnceCell, broadcast};

use crate::error::{Error, Result};
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::local::{Loan, Sample};
use crate::remote::runtime;

const DEFAULT_ALPN: &[u8] = b"quicbit/1";
const DEFAULT_BROADCAST_CAPACITY: usize = 256;
const IDENTITY_DERIVATION_TAG: &[u8] = b"quicbit/v1/identity";

/// Small, stable type-name hash used to gate iroh handshake
/// compatibility. Identical algorithm publisher and subscriber
/// must use; FNV-1a (64-bit) is the historical choice.
fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

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
}

/// `&str` / `String` hashes to the same deterministic `EndpointId`
/// as [`NodeBuilder::identity`]. `EndpointId` passes through.
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

pub struct NodeBuilder {
    identity: IdentitySource,
    alpn: Vec<u8>,
    no_relay: bool,
    local_cfg: LocalConfig,
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

        let inner = Arc::new(NodeInner {
            endpoint,
            endpoint_id,
            identity_name,
            alpn: self.alpn,
            local_cfg: self.local_cfg,
            publisher_topics: Mutex::new(HashMap::new()),
            peer_connections: Mutex::new(HashMap::new()),
        });

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
    peer_connections: Mutex<HashMap<[u8; 32], Arc<OnceCell<Connection>>>>,
}

struct PublisherTopicState {
    iroh_tx: broadcast::Sender<Vec<u8>>,
    type_hash: u64,
    payload_size: u32,
}

impl Node {
    pub fn builder() -> NodeBuilder {
        NodeBuilder {
            identity: IdentitySource::Ephemeral,
            alpn: DEFAULT_ALPN.to_vec(),
            no_relay: false,
            local_cfg: LocalConfig::default(),
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

    /// Build a publisher for `topic`. Writes go to both:
    /// * iceoryx2 service named `<identity>__<topic>` (same-host
    ///   subscribers attach to this and read zero-copy);
    /// * a broadcast queue feeding the iroh accept loop, which
    ///   serves attached remote subscribers.
    pub fn publisher<T>(&self, topic: &str) -> Result<Publisher<T>>
    where
        T: Pod + ZeroCopySend + Debug + 'static,
    {
        let type_name = type_name::<T>();
        let type_hash = fnv1a64(type_name);
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
        T: Pod + ZeroCopySend + Debug + 'static,
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
                });
            }
        }

        // Remote path.
        let inner = self.inner.clone();
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

        Ok(Subscriber {
            source: SubscriberSource::Remote { rx: rx_handle },
        })
    }
}

// ---- publisher ----

pub struct Publisher<T: Pod + ZeroCopySend + Debug + 'static> {
    local_publisher: LocalPublisher<T>,
    _local_service: LocalService<T>,
    iroh_tx: broadcast::Sender<Vec<u8>>,
}

impl<T: Pod + ZeroCopySend + Debug + 'static> Publisher<T> {
    pub fn loan(&mut self) -> Result<Loan<T>> {
        self.local_publisher.loan()
    }

    pub fn publish(&mut self, loan: Loan<T>) -> Result<u64> {
        // Snapshot bytes before the loan transfers ownership into
        // iceoryx2's send path; cheap copy of size_of::<T>() bytes.
        let bytes: Vec<u8> = bytemuck::bytes_of(&*loan).to_vec();
        let seq = self.local_publisher.publish(loan)?;
        let _ = self.iroh_tx.send(bytes);
        Ok(seq)
    }

    pub fn send(&mut self, value: T) -> Result<u64> {
        let mut loan = self.loan()?;
        *loan = value;
        self.publish(loan)
    }
}

// ---- subscriber ----

pub struct Subscriber<T: Pod + ZeroCopySend + Debug + 'static> {
    source: SubscriberSource<T>,
}

enum SubscriberSource<T: Pod + ZeroCopySend + Debug + 'static> {
    Local {
        sub: LocalSubscriber<T>,
        _svc: LocalService<T>,
    },
    Remote {
        rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    },
}

impl<T: Pod + ZeroCopySend + Debug + 'static> Subscriber<T> {
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
                    let value: T = *bytemuck::from_bytes(&bytes);
                    Ok(Some(NodeSample::Remote { value }))
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
            },
        }
    }
}

/// Unified sample, derefs to `T` regardless of transport.
pub enum NodeSample<T: Pod + ZeroCopySend + Debug + 'static> {
    Local(Sample<T>),
    Remote { value: T },
}

impl<T: Pod + ZeroCopySend + Debug + 'static> std::ops::Deref for NodeSample<T> {
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
            None => return Ok(()),
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
        if recv.read_exact(&mut len_buf).await.is_err() {
            return Ok(());
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
    let mut topic_len = [0u8; 4];
    recv.read_exact(&mut topic_len)
        .await
        .map_err(|e| Error::Remote(format!("handshake topic_len: {e}")))?;
    let n = u32::from_le_bytes(topic_len) as usize;
    if n > 1024 {
        return Err(Error::Remote(format!("topic name too long: {n}")));
    }
    let mut topic = vec![0u8; n];
    recv.read_exact(&mut topic)
        .await
        .map_err(|e| Error::Remote(format!("handshake topic: {e}")))?;
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

async fn ensure_peer_connection(inner: &Arc<NodeInner>, peer: EndpointId) -> Result<Connection> {
    let cell = {
        let mut map = inner
            .peer_connections
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        map.entry(*peer.as_bytes())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    };
    let conn = cell
        .get_or_try_init(|| async {
            let addr = EndpointAddr::new(peer);
            inner
                .endpoint
                .connect(addr, &inner.alpn)
                .await
                .map_err(|e| Error::Remote(format!("connect: {e}")))
        })
        .await?;
    Ok(conn.clone())
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
