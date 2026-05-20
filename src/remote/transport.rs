//! [`RemoteTransport`] — pub/sub on top of an [`iroh::Endpoint`].
//!
//! Architectural shape, mirroring the local transport's
//! "one transport = one topic" rule:
//!
//! * A `RemoteTransport` owns exactly one `iroh::Endpoint` and is
//!   bound to one topic name. Multiple transports can coexist in
//!   one process; each opens its own endpoint.
//! * The transport runs a background **accept loop** that handles
//!   inbound subscriber connections. Each incoming bi stream
//!   starts with the [`handshake`] bytes; if the topic matches
//!   the transport's, we hook the stream's send half up to the
//!   topic's broadcast queue and start forwarding.
//! * Subscribers are *active*: at construction time, the subscriber
//!   task dials a configured peer (`builder.peer(id)`), opens a
//!   bi stream, writes the handshake, then reads length-prefixed
//!   frames off the recv half into a broadcast that each
//!   subscriber's `take()` polls (fan-out: one wire stream serves
//!   N in-process subscribers on the same topic).
//!
//! Payloads are `bytemuck::Pod`, sent as raw bytes. Frames carry a
//! length prefix only because QUIC streams don't preserve write
//! boundaries; the wire format is `[u32_le length][raw bytes]`.

use std::any::type_name;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use bytemuck::Pod;
use iroh::endpoint::presets;
use iroh::endpoint::Connection;
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, OnceCell};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::local::layout::fnv1a64;
use crate::remote::handshake::{
    HANDSHAKE_MAGIC, HANDSHAKE_VERSION, MAX_PAYLOAD_LEN, MAX_TOPIC_LEN, REQRESP_MAGIC,
};
use crate::remote::runtime;
use crate::transport::{LocalPayload, PublisherOps, SubscriberOps, Transport};

/// Type-erased boxed request handler used internally.
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
pub const DEFAULT_ALPN: &[u8] = b"quicbit/1";

/// Channel depth for outgoing broadcast and incoming mpsc.
const CHANNEL_CAPACITY: usize = 256;

// --- builder ---

/// Builder for [`RemoteTransport`].
pub struct RemoteTransportBuilder {
    name: String,
    alpn: Vec<u8>,
    peer: Option<EndpointAddr>,
    relay_disabled: bool,
}

impl RemoteTransportBuilder {
    /// Override the ALPN. Defaults to [`DEFAULT_ALPN`].
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
            let endpoint = if relay_disabled {
                Endpoint::builder(presets::N0DisableRelay)
                    .alpns(vec![alpn.clone()])
                    .bind()
                    .await
                    .map_err(|e| Error::Remote(format!("bind: {e}")))?
            } else {
                Endpoint::builder(presets::N0)
                    .alpns(vec![alpn.clone()])
                    .bind()
                    .await
                    .map_err(|e| Error::Remote(format!("bind: {e}")))?
            };

            let inner = Arc::new(InnerShared {
                name,
                alpn,
                endpoint: endpoint.clone(),
                publisher_topics: Mutex::new(HashMap::new()),
                subscriber_topics: Mutex::new(HashMap::new()),
                request_servers: Mutex::new(HashMap::new()),
                peer,
                peer_conn: OnceCell::new(),
            });

            // Accept loop.
            let accept_handle = {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let _ = run_accept_loop(inner).await;
                })
            };
            Ok::<_, Error>(InnerOwned {
                shared: inner,
                _accept: accept_handle,
            })
        })?;

        Ok(RemoteTransport {
            inner: Arc::new(inner),
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
/// dropped when the last transport handle goes away.
struct InnerOwned {
    shared: Arc<InnerShared>,
    _accept: JoinHandle<()>,
}

/// State shared with the accept loop and subscriber tasks. Owned by
/// `InnerOwned`; cloning is by `Arc`.
pub(crate) struct InnerShared {
    pub(crate) name: String,
    pub(crate) alpn: Vec<u8>,
    pub(crate) endpoint: Endpoint,
    publisher_topics: Mutex<HashMap<String, PublisherTopic>>,
    subscriber_topics: Mutex<HashMap<String, SubscriberTopic>>,
    pub(crate) request_servers: Mutex<HashMap<String, RequestServerEntry>>,
    /// Peer to dial as a subscriber. `None` means subscribe-only role
    /// is unavailable on this transport.
    pub(crate) peer: Option<EndpointAddr>,
    /// Lazily-established outbound connection to `peer`.
    pub(crate) peer_conn: OnceCell<Connection>,
}

/// Per-topic broadcast queue for publishers. The accept loop creates
/// a new receiver each time a peer subscribes; the publisher's
/// `publish()` call is one `Sender::send`.
struct PublisherTopic {
    tx: broadcast::Sender<Arc<[u8]>>,
    type_hash: u64,
    payload_size: u32,
}

/// Per-topic broadcast queue for subscribers. The dispatcher task
/// pushes frames it reads off the wire onto `tx`; each subscriber
/// gets its own `Receiver` via `tx.subscribe()`, so a single topic
/// can fan out to multiple subscribers in the same process. A
/// dedicated flag tracks whether the receiver-side dispatcher
/// has been spawned yet (we only want one per topic, regardless of
/// how many subscriber handles the user creates).
struct SubscriberTopic {
    tx: broadcast::Sender<Arc<[u8]>>,
    dispatcher_started: bool,
    type_hash: u64,
    /// Tracked for future use (size validation against the peer's
    /// handshake); not consulted by the current take path.
    #[allow(dead_code)]
    payload_size: u32,
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
    /// (direct or relay). Useful for loopback tests that want to
    /// read the address before the peer dials in.
    pub fn wait_for_direct_addresses(&self) -> Result<()> {
        let endpoint = self.inner.shared.endpoint.clone();
        self.rt.block_on(async move {
            // Poll until `addr()` reports at least one TransportAddr.
            // The endpoint publishes addresses fairly quickly after
            // `bind`, so this loop iterates only a handful of times.
            loop {
                if !endpoint.addr().is_empty() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        });
        Ok(())
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
        let type_hash = fnv1a64(type_name::<T>());
        let payload_size = std::mem::size_of::<T>() as u32;

        let mut topics = self.inner.shared.publisher_topics.lock().unwrap_or_else(|p| p.into_inner());
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
        let type_hash = fnv1a64(type_name::<T>());
        let payload_size = std::mem::size_of::<T>() as u32;

        // Multi-subscriber: each call creates a fresh broadcast
        // receiver. The dispatcher task (which pumps frames off the
        // wire into the broadcast) is spawned at most once per
        // topic.
        let (receiver, needs_dispatcher) = {
            let mut topics = self
                .inner
                .shared
                .subscriber_topics
                .lock()
                .unwrap_or_else(|p| p.into_inner());
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
                if let Err(_e) =
                    run_subscriber(shared, topic_for_task, type_hash, payload_size).await
                {
                    // Connection failures show up as `take()` returning
                    // `None` once the channel closes; nothing else to do.
                }
            });
        }

        Ok(RemoteSubscriber {
            rx: receiver,
            _phantom: PhantomData,
        })
    }
}

// --- publisher ---

/// Publisher handle returned by [`RemoteTransport::publisher`].
pub struct RemotePublisher<T> {
    tx: broadcast::Sender<Arc<[u8]>>,
    seq: u64,
    _phantom: PhantomData<fn() -> T>,
}

/// Owned writable handle the publisher fills before `publish`.
pub struct RemoteLoan<T: Pod> {
    value: T,
}

impl<T: Pod> RemoteLoan<T> {
    fn new() -> Self {
        Self {
            // SAFETY: `T: Pod` implies all-zeros is a valid value.
            value: bytemuck::Zeroable::zeroed(),
        }
    }
}

impl<T: Pod> Deref for RemoteLoan<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Pod> DerefMut for RemoteLoan<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: LocalPayload> PublisherOps<T> for RemotePublisher<T> {
    type Loan = RemoteLoan<T>;

    fn loan(&mut self) -> Result<Self::Loan> {
        Ok(RemoteLoan::new())
    }

    fn publish(&mut self, loan: Self::Loan) -> Result<u64> {
        let bytes: Arc<[u8]> = Arc::from(bytemuck::bytes_of(&loan.value).to_vec().into_boxed_slice());
        // `broadcast::send` returns Err only when there are no
        // subscribers; treat that as a no-op rather than an error.
        let _ = self.tx.send(bytes);
        self.seq = self.seq.wrapping_add(1);
        Ok(self.seq)
    }
}

// --- subscriber ---

/// Subscriber handle returned by [`RemoteTransport::subscriber`].
///
/// Multiple subscribers can coexist on the same topic; each has its
/// own broadcast receiver, so a slow subscriber doesn't stall its
/// peers. Each receiver has a fixed-size buffer; if a subscriber
/// falls behind by more than the buffer depth, its next `take()`
/// returns [`Error::Lagged`](crate::Error::Lagged).
pub struct RemoteSubscriber<T> {
    rx: broadcast::Receiver<Arc<[u8]>>,
    _phantom: PhantomData<fn() -> T>,
}

/// Owned sample handed back from the subscriber. Decoded once on
/// receive; `Deref` is a plain field read.
pub struct RemoteSample<T: Pod> {
    value: T,
}

impl<T: Pod> Deref for RemoteSample<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: LocalPayload> SubscriberOps<T> for RemoteSubscriber<T> {
    type Sample = RemoteSample<T>;

    fn take(&mut self) -> Result<Option<Self::Sample>> {
        match self.rx.try_recv() {
            Ok(bytes) => {
                if bytes.len() != std::mem::size_of::<T>() {
                    return Err(Error::Remote(format!(
                        "frame size mismatch: got {} bytes, expected {}",
                        bytes.len(),
                        std::mem::size_of::<T>()
                    )));
                }
                // SAFETY: `T: Pod` guarantees the byte representation
                // is valid; the size check above ensures alignment-
                // free transmute is in-bounds.
                let value: T = *bytemuck::from_bytes(&bytes);
                Ok(Some(RemoteSample { value }))
            }
            Err(broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(broadcast::error::TryRecvError::Closed) => Ok(None),
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                Err(Error::Lagged { dropped: n })
            }
        }
    }
}

// --- accept side (publisher endpoint) ---

async fn run_accept_loop(inner: Arc<InnerShared>) -> Result<()> {
    while let Some(accept) = inner.endpoint.accept().await {
        let inner = inner.clone();
        tokio::spawn(async move {
            let mut iconn = match accept.accept() {
                Ok(c) => c,
                Err(_) => return,
            };
            let _alpn = match iconn.alpn().await {
                Ok(a) => a,
                Err(_) => return,
            };
            let conn = match iconn.await {
                Ok(c) => c,
                Err(_) => return,
            };
            let _ = serve_incoming_connection(inner, conn).await;
        });
    }
    Ok(())
}

async fn serve_incoming_connection(inner: Arc<InnerShared>, conn: Connection) -> Result<()> {
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
    inner: Arc<InnerShared>,
    send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    // Peek the magic so we can dispatch pub/sub vs req/resp.
    let mut magic_buf = [0u8; 4];
    recv.read_exact(&mut magic_buf)
        .await
        .map_err(|e| Error::Remote(format!("magic: {e}")))?;
    let magic = u32::from_le_bytes(magic_buf);
    match magic {
        HANDSHAKE_MAGIC => serve_pubsub_bi(inner, send, recv).await,
        REQRESP_MAGIC => crate::remote::reqresp::serve_request_bi(inner, send, recv).await,
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
        let map = inner.publisher_topics.lock().unwrap_or_else(|p| p.into_inner());
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
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
        }
    }
}

// --- subscriber side ---

async fn run_subscriber(
    inner: Arc<InnerShared>,
    topic: String,
    type_hash: u64,
    payload_size: u32,
) -> Result<()> {
    let conn = ensure_peer_connection(&inner).await?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;

    // Subscriber writes the handshake, then FIN's its send side.
    write_handshake(&mut send, &topic, type_hash, payload_size).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;

    // Find the topic's broadcast tx and pump frames onto it. Each
    // subscriber on this transport holds its own `Receiver`, so the
    // dispatcher is fan-out — one send reaches every subscriber.
    let sender = {
        let map = inner
            .subscriber_topics
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        map.get(&topic).map(|t| t.tx.clone()).ok_or_else(|| {
            Error::Remote(format!("subscriber for topic '{}' vanished", topic))
        })?
    };

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

pub(crate) async fn ensure_peer_connection(inner: &Arc<InnerShared>) -> Result<Connection> {
    inner
        .peer_conn
        .get_or_try_init(|| async {
            let peer = inner
                .peer
                .clone()
                .ok_or_else(|| Error::invalid_argument("no peer configured"))?;
            inner
                .endpoint
                .connect(peer, &inner.alpn)
                .await
                .map_err(|e| Error::Remote(format!("connect: {e}")))
        })
        .await
        .cloned()
}

// --- wire helpers ---

async fn write_handshake(
    send: &mut SendStream,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
) -> Result<()> {
    if topic.len() > MAX_TOPIC_LEN as usize {
        return Err(Error::invalid_argument("topic name too long"));
    }
    let mut buf = Vec::with_capacity(4 + 4 + 8 + 4 + 2 + topic.len());
    buf.extend_from_slice(&HANDSHAKE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&type_hash.to_le_bytes());
    buf.extend_from_slice(&payload_size.to_le_bytes());
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());
    send.write_all(&buf)
        .await
        .map_err(|e| Error::Remote(format!("write handshake: {e}")))
}

/// Read the pub/sub handshake tail (everything after the magic).
/// The magic itself has already been consumed by [`serve_bi`].
async fn read_pubsub_handshake_tail(recv: &mut RecvStream) -> Result<(String, u64, u32)> {
    let mut header = [0u8; 4 + 8 + 4 + 2];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| Error::Remote(format!("handshake tail: {e}")))?;
    let version = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if version != HANDSHAKE_VERSION {
        return Err(Error::Remote(format!(
            "handshake version mismatch: peer={version} local={HANDSHAKE_VERSION}"
        )));
    }
    let type_hash = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let payload_size = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let topic_len = u16::from_le_bytes(header[16..18].try_into().unwrap());
    if topic_len > MAX_TOPIC_LEN {
        return Err(Error::Remote(format!("topic too long: {topic_len}")));
    }
    let mut topic_buf = vec![0u8; topic_len as usize];
    recv.read_exact(&mut topic_buf)
        .await
        .map_err(|e| Error::Remote(format!("topic name: {e}")))?;
    let topic = String::from_utf8(topic_buf)
        .map_err(|_| Error::Remote("topic name is not UTF-8".to_string()))?;
    Ok((topic, type_hash, payload_size))
}

pub(crate) async fn write_frame(send: &mut SendStream, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 > MAX_PAYLOAD_LEN as u64 {
        return Err(Error::PayloadTooLarge {
            actual: bytes.len(),
            capacity: MAX_PAYLOAD_LEN as usize,
        });
    }
    send.write_all(&(bytes.len() as u32).to_le_bytes())
        .await
        .map_err(|e| Error::Remote(format!("frame header: {e}")))?;
    send.write_all(bytes)
        .await
        .map_err(|e| Error::Remote(format!("frame body: {e}")))
}

pub(crate) async fn read_frame(recv: &mut RecvStream) -> Result<Option<Arc<[u8]>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        // Quinn-style: a clean FIN on read_exact for 0 bytes is the
        // graceful end-of-stream signal.
        Err(e) => {
            // Treat any read error after the publisher closes as
            // end-of-stream; surfacing it as `Ok(None)` lets
            // subscribers complete cleanly.
            let _ = e;
            return Ok(None);
        }
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_PAYLOAD_LEN {
        return Err(Error::Remote(format!("frame too large: {len}")));
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| Error::Remote(format!("frame body: {e}")))?;
    Ok(Some(Arc::from(buf.into_boxed_slice())))
}
