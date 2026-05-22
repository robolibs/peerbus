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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use iroh::endpoint::presets;
use iroh::endpoint::Connection;
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::transport::wire_type_hash;
use crate::{qb_debug, qb_info, qb_warn};
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
    /// Override the ALPN. Defaults to `b"quicbit/1"`.
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
    publisher_topics: Mutex<HashMap<String, PublisherTopic>>,
    subscriber_topics: Mutex<HashMap<String, SubscriberTopic>>,
    pub(crate) request_servers: Mutex<HashMap<String, RequestServerEntry>>,
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

// --- publisher ---

/// Snapshot of a [`RemotePublisher`]'s lifetime counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct RemotePublisherStats {
    /// Successful `publish()` calls.
    pub published: u64,
    /// Frames the broadcast queue refused (no subscriber attached
    /// at that moment).
    pub dropped: u64,
}

/// Publisher handle returned by [`RemoteTransport::publisher`].
pub struct RemotePublisher<T> {
    tx: broadcast::Sender<Arc<[u8]>>,
    seq: u64,
    published: AtomicU64,
    dropped: AtomicU64,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> RemotePublisher<T> {
    /// Snapshot lifetime counters.
    pub fn stats(&self) -> RemotePublisherStats {
        RemotePublisherStats {
            published: self.published.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

/// Owned writable handle the publisher fills before `publish`.
///
/// Mirrors the local transport's header+payload split: `header` is
/// the small Pod metadata (a `T::Header`), `payload` is the
/// variable-length byte buffer.
pub struct RemoteLoan<T: LocalPayload> {
    pub header: T::Header,
    pub payload: Vec<u8>,
}

impl<T: LocalPayload> RemoteLoan<T> {
    fn new(byte_count: usize) -> Self {
        Self {
            header: <T::Header as bytemuck::Zeroable>::zeroed(),
            payload: vec![0u8; byte_count],
        }
    }
}

impl<T: LocalPayload> PublisherOps<T> for RemotePublisher<T> {
    type Loan = RemoteLoan<T>;

    fn loan(&mut self, byte_count: usize) -> Result<Self::Loan> {
        Ok(RemoteLoan::new(byte_count))
    }

    fn publish(&mut self, loan: Self::Loan) -> Result<u64> {
        let header_bytes = bytemuck::bytes_of(&loan.header);
        let mut frame = Vec::with_capacity(header_bytes.len() + loan.payload.len());
        frame.extend_from_slice(header_bytes);
        frame.extend_from_slice(&loan.payload);
        let bytes: Arc<[u8]> = Arc::from(frame.into_boxed_slice());
        // `broadcast::send` returns Err only when there are no
        // subscribers; treat that as a no-op rather than an error,
        // but count the drop so operators can see it.
        if self.tx.send(bytes).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.published.fetch_add(1, Ordering::Relaxed);
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
    received: AtomicU64,
    lagged: AtomicU64,
    disconnects: AtomicU64,
    _phantom: PhantomData<fn() -> T>,
}

/// Snapshot of a [`RemoteSubscriber`]'s lifetime counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct RemoteSubscriberStats {
    /// Samples successfully returned from `take()`.
    pub received: u64,
    /// Total samples dropped by broadcast lag events (sum of `n`
    /// across every `Error::Lagged { dropped: n }` observed).
    pub lagged: u64,
    /// Times the channel was observed closed via
    /// `Error::Disconnected`.
    pub disconnects: u64,
}

impl<T> RemoteSubscriber<T> {
    /// Snapshot lifetime counters.
    pub fn stats(&self) -> RemoteSubscriberStats {
        RemoteSubscriberStats {
            received: self.received.load(Ordering::Relaxed),
            lagged: self.lagged.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
        }
    }
}

/// Owned sample handed back from the subscriber. The wire frame
/// is split into the Pod header (decoded eagerly via
/// `bytemuck::from_bytes`) and the trailing variable-length byte
/// payload (kept as `Vec<u8>` for the caller to reinterpret).
pub struct RemoteSample<T: LocalPayload> {
    pub header: T::Header,
    pub payload: Vec<u8>,
}

impl<T: LocalPayload> RemoteSample<T> {
    pub fn header(&self) -> &T::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl<T: LocalPayload> SubscriberOps<T> for RemoteSubscriber<T> {
    type Sample = RemoteSample<T>;

    fn take(&mut self) -> Result<Option<Self::Sample>> {
        match self.rx.try_recv() {
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
                self.received.fetch_add(1, Ordering::Relaxed);
                Ok(Some(RemoteSample { header, payload }))
            }
            Err(broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(broadcast::error::TryRecvError::Closed) => {
                self.disconnects.fetch_add(1, Ordering::Relaxed);
                Err(Error::Disconnected)
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                self.lagged.fetch_add(n, Ordering::Relaxed);
                Err(Error::Lagged { dropped: n })
            }
        }
    }
}

// --- accept side (publisher endpoint) ---

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn run_accept_loop(inner: Arc<InnerShared>) -> Result<()> {
    qb_debug!(target: "quicbit::remote", name = %inner.name, "accept loop started");
    while let Some(accept) = inner.endpoint.accept().await {
        let inner = inner.clone();
        tokio::spawn(async move {
            let mut iconn = match accept.accept() {
                Ok(c) => c,
                Err(e) => {
                    qb_warn!(target: "quicbit::remote", error = %e, "incoming.accept failed");
                    return;
                }
            };
            let _alpn = match iconn.alpn().await {
                Ok(a) => a,
                Err(e) => {
                    qb_warn!(target: "quicbit::remote", error = %e, "alpn negotiation failed");
                    return;
                }
            };
            let conn = match iconn.await {
                Ok(c) => c,
                Err(e) => {
                    qb_warn!(target: "quicbit::remote", error = %e, "connection handshake failed");
                    return;
                }
            };
            qb_debug!(
                target: "quicbit::remote",
                remote = %conn.remote_id(),
                "accepted connection"
            );
            let _ = serve_incoming_connection(inner, conn).await;
        });
    }
    qb_debug!(target: "quicbit::remote", "accept loop exiting");
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
                    target: "quicbit::remote",
                    dropped = n,
                    "broadcast lagged on publisher serve path"
                );
                continue;
            }
        }
    }
}

// --- subscriber side ---

/// Initial backoff between reconnect attempts on the subscriber
/// dispatcher loop. Doubles up to [`RECONNECT_BACKOFF_MAX`].
const RECONNECT_BACKOFF_MIN: std::time::Duration = std::time::Duration::from_millis(100);
const RECONNECT_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(10);

/// Dispatcher loop for a topic. Repeatedly dials the peer,
/// re-issues the handshake, and pumps frames into the topic's
/// broadcast channel. Exits when the broadcast sender is dropped
/// (i.e. the transport itself is gone).
#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
async fn run_subscriber(
    inner: Arc<InnerShared>,
    topic: String,
    type_hash: u64,
    payload_size: u32,
) {
    let mut backoff = RECONNECT_BACKOFF_MIN;
    loop {
        let sender = {
            let map = inner
                .subscriber_topics
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match map.get(&topic) {
                Some(t) => t.tx.clone(),
                // Topic state is gone — the transport is being
                // torn down. Exit cleanly.
                None => {
                    qb_debug!(
                        target: "quicbit::remote",
                        topic = %topic,
                        "dispatcher exiting: topic state gone"
                    );
                    return;
                }
            }
        };
        // Bail out if every subscriber has dropped — no point
        // re-establishing the wire just to feed nobody. A new
        // `subscriber()` call will spawn a fresh dispatcher.
        if sender.receiver_count() == 0 {
            qb_debug!(
                target: "quicbit::remote",
                topic = %topic,
                "dispatcher exiting: no remaining receivers"
            );
            return;
        }

        match subscribe_pump_once(&inner, &topic, type_hash, payload_size, &sender).await {
            Ok(()) => {
                qb_warn!(
                    target: "quicbit::remote",
                    topic = %topic,
                    "subscriber stream ended, will reconnect"
                );
                backoff = RECONNECT_BACKOFF_MIN;
            }
            Err(e) => {
                qb_debug!(
                    target: "quicbit::remote",
                    topic = %topic,
                    error = %e,
                    backoff_ms = backoff.as_millis() as u64,
                    "subscriber reconnect attempt failed"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }
}

async fn subscribe_pump_once(
    inner: &Arc<InnerShared>,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
    sender: &broadcast::Sender<Arc<[u8]>>,
) -> Result<()> {
    let conn = ensure_peer_connection(inner).await?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;

    write_handshake(&mut send, topic, type_hash, payload_size).await?;
    send.finish()
        .map_err(|e| Error::Remote(format!("finish: {e}")))?;

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

#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub(crate) async fn ensure_peer_connection(inner: &Arc<InnerShared>) -> Result<Connection> {
    let mut guard = inner.peer_conn.lock().await;
    if let Some(conn) = guard.as_ref() {
        if conn.close_reason().is_none() {
            return Ok(conn.clone());
        }
        qb_warn!(
            target: "quicbit::remote",
            reason = ?conn.close_reason(),
            "cached connection is dead, re-dialing"
        );
        *guard = None;
    }
    let peer = inner
        .peer
        .clone()
        .ok_or_else(|| Error::invalid_argument("no peer configured"))?;
    qb_info!(
        target: "quicbit::remote",
        peer = %peer.id,
        "dialing peer"
    );
    let conn = inner
        .endpoint
        .connect(peer.clone(), &inner.alpn)
        .await
        .map_err(|e| {
            qb_warn!(
                target: "quicbit::remote",
                peer = %peer.id,
                error = %e,
                "dial failed"
            );
            Error::ConnectFailed(format!("{e}"))
        })?;
    qb_info!(
        target: "quicbit::remote",
        peer = %peer.id,
        "connected to peer"
    );
    *guard = Some(conn.clone());
    Ok(conn)
}

// --- wire helpers ---

async fn write_handshake(
    send: &mut SendStream,
    topic: &str,
    type_hash: u64,
    payload_size: u32,
) -> Result<()> {
    if topic.len() > MAX_TOPIC_LEN as usize {
        return Err(Error::TopicNameTooLong {
            len: topic.len(),
            limit: MAX_TOPIC_LEN as usize,
        });
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
        .map_err(|e| Error::HandshakeMalformed(format!("handshake tail: {e}")))?;
    let topic_len = u16::from_le_bytes(header[16..18].try_into().unwrap()) as usize;
    let mut topic_buf = vec![0u8; topic_len];
    recv.read_exact(&mut topic_buf)
        .await
        .map_err(|e| Error::Remote(format!("topic name: {e}")))?;

    // Concatenate header + topic and feed to the pure parser so
    // wire / fuzz tests exercise the exact same logic.
    let mut buf = Vec::with_capacity(header.len() + topic_buf.len());
    buf.extend_from_slice(&header);
    buf.extend_from_slice(&topic_buf);
    parse_pubsub_handshake_tail(&buf)
}

/// Pure-byte parser for the pub/sub handshake tail (the part after
/// the 4-byte `HANDSHAKE_MAGIC`). Public so fuzz targets and tests
/// can hammer it without driving an actual `RecvStream`.
///
/// Layout: `[u32 version][u64 type_hash][u32 payload_size][u16
/// topic_len][topic_bytes]`.
pub fn parse_pubsub_handshake_tail(bytes: &[u8]) -> Result<(String, u64, u32)> {
    if bytes.len() < 4 + 8 + 4 + 2 {
        return Err(Error::HandshakeMalformed(format!(
            "handshake tail truncated: {} bytes",
            bytes.len()
        )));
    }
    let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if version != HANDSHAKE_VERSION {
        return Err(Error::HandshakeVersionMismatch {
            local: HANDSHAKE_VERSION,
            peer: version,
        });
    }
    let type_hash = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let payload_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let topic_len = u16::from_le_bytes(bytes[16..18].try_into().unwrap());
    if topic_len > MAX_TOPIC_LEN {
        return Err(Error::TopicNameTooLong {
            len: topic_len as usize,
            limit: MAX_TOPIC_LEN as usize,
        });
    }
    let topic_end = 18usize.saturating_add(topic_len as usize);
    if bytes.len() < topic_end {
        return Err(Error::HandshakeMalformed(format!(
            "topic name truncated: declared {} bytes, have {}",
            topic_len,
            bytes.len().saturating_sub(18)
        )));
    }
    let topic = std::str::from_utf8(&bytes[18..topic_end])
        .map_err(|_| Error::HandshakeMalformed("topic name is not UTF-8".to_string()))?
        .to_string();
    Ok((topic, type_hash, payload_size))
}

/// Pure-byte parser for the framed payload header (`[u32 length]`)
/// plus body. Returns the body slice. Exposed for fuzz tests.
pub fn parse_frame(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        return Err(Error::HandshakeMalformed(format!(
            "frame header truncated: {} bytes",
            bytes.len()
        )));
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if len > MAX_PAYLOAD_LEN {
        return Err(Error::FrameTooLarge {
            actual: len as u64,
            limit: MAX_PAYLOAD_LEN as u64,
        });
    }
    let end = 4usize.saturating_add(len as usize);
    if bytes.len() < end {
        return Err(Error::HandshakeMalformed(format!(
            "frame body truncated: declared {} bytes, have {}",
            len,
            bytes.len().saturating_sub(4)
        )));
    }
    Ok(&bytes[4..end])
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
        return Err(Error::FrameTooLarge {
            actual: len as u64,
            limit: MAX_PAYLOAD_LEN as u64,
        });
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| Error::Remote(format!("frame body: {e}")))?;
    Ok(Some(Arc::from(buf.into_boxed_slice())))
}
