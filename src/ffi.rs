//! C ABI for peerbus.
//!
//! A thin, `extern "C"` surface over the high-level [`Node`] API,
//! speaking opaque byte messages ([`crate::RawMsg`]) so callers in any
//! language can publish/subscribe and do request/response over both
//! transports (shared memory on the same host, iroh QUIC across hosts).
//!
//! Conventions (matching the sibling `maptrax` C ABI):
//!
//! * Handles are opaque pointers from `Box::into_raw`; free them with
//!   the matching `*_free` and never dereference them in C.
//! * Fallible calls return `bool`/`int` status; on failure the reason is
//!   stashed in a thread-local and read via [`peerbus_last_error_message`].
//! * Returned byte views ([`PeerbusBytes`]) borrow memory owned by the
//!   handle they came from; copy out before freeing the handle.
//!
//! # Lifetime and threading contract
//!
//! * A parent client/server handle and every child handle derived from it
//!   (pending/upload/pip/puts/batch) share ownership of the parent's
//!   transport state through an `Arc<Mutex<..>>`. They may therefore be
//!   freed in any order: each child keeps the shared state alive, which is
//!   dropped only when the last handle (parent or child) is freed. Freeing
//!   the parent before its children is safe — no use-after-free.
//! * Each handle is single-threaded: one handle must not be used from two
//!   threads concurrently; the caller synchronizes. In particular, do not
//!   re-enter peerbus on the same parent handle from inside a `serve_one`
//!   handler callback (the parent's lock is held for the callback's
//!   duration, so a re-entrant call would deadlock).
//! * Free each handle exactly once; never use a handle after freeing it.
//!
//! See `include/peerbus.h` for the C declarations.

// These `extern "C"` functions take raw pointers from C and dereference
// them by design; the safety contract lives in the C header, not in a
// Rust `unsafe fn` signature. The lint is noise for a C ABI.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use iroh::EndpointAddr;

use crate::{
    AckServer, AnsReplyToken, AnsServer, DatapodMsg, DeliveryPolicy, LocalConfig, Node, NodeSample,
    PipClient, PipServer, PipServerToken, PipSessionToken, Publisher, PutAckToken, PutClient,
    PutUploadToken, QueClient, RawMsg, ReqClient, ReqReplyToken, ReqServer, Subscriber, TopicQos,
};

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
}

fn set_last_error(message: impl Into<String>) {
    let message = message.into().replace('\0', " ");
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(
            CString::new(message).unwrap_or_else(|_| CString::new("peerbus ffi error").unwrap()),
        );
    });
}

/// Locks a shared handle mutex, recovering from poisoning.
///
/// A parent client/server and every child handle derived from it share one
/// `Arc<Mutex<Inner>>`; this is how a child keeps the parent's transport state
/// alive even if the C caller frees the parent first (no use-after-free). If a
/// previous call panicked while holding the lock the mutex is poisoned; because
/// the panic was already caught at the C boundary and reported, we recover the
/// inner value rather than propagate a second panic across `extern "C"`.
fn lock_inner<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runs an FFI entry-point body and converts any Rust panic into a normal
/// error return: the panic is caught at the C boundary (unwinding across
/// `extern "C"` is undefined behavior), the reason is stashed via
/// [`set_last_error`], and `fallback` (the function's error sentinel —
/// `-1`, `false`, NULL, or a zeroed value) is returned instead.
fn ffi_guard<T>(fallback: T, body: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(_) => {
            set_last_error("peerbus ffi call panicked");
            fallback
        }
    }
}

/// Returns the last error message on this thread, or NULL if the most
/// recent call succeeded. The pointer is valid until the next peerbus
/// call on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_last_error_message() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|m| m.as_ptr())
            .unwrap_or(ptr::null())
    })
}

/// A borrowed view of contiguous bytes owned by a handle.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusBytes {
    pub ptr: *const u8,
    pub len: usize,
}

impl PeerbusBytes {
    fn empty() -> Self {
        Self {
            ptr: ptr::null(),
            len: 0,
        }
    }
}

// ---- handles ----

pub struct PeerbusNode {
    node: Node,
}

/// Optional node construction settings. NULL string pointers mean "unset";
/// zero numeric limits mean "use the Rust default".
///
/// Inbound connections are denied by default: unless `allowed_peers` is
/// non-empty or `allow_any_peer` is true, every incoming connection is
/// refused right after the QUIC handshake.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusNodeConfig {
    pub identity: *const c_char,
    pub no_relay: bool,
    pub system_did: *const c_char,
    pub allowed_peers: *const *const c_char,
    pub allowed_peers_len: usize,
    /// Accept connections from ANY peer that knows the ALPN. Insecure;
    /// only appropriate on a trusted network. Logs a WARN at bind.
    pub allow_any_peer: bool,
    pub max_payload_bytes: usize,
    pub history_depth: u32,
    pub subscriber_buffer: u32,
    pub max_publishers: u32,
    pub max_subscribers: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusNodeStats {
    pub publisher_topics: usize,
    pub cached_peers: usize,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusPublisherStats {
    pub published: u64,
    pub remote_dropped: u64,
    pub stale_dropped: u64,
    pub bytes_sent: u64,
    pub send_errors: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusSubscriberStats {
    pub received: u64,
    pub disconnects: u64,
    pub stale_dropped: u64,
    pub incomplete_dropped: u64,
    pub bytes_received: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusItemStats {
    pub messages_out: u64,
    pub messages_in: u64,
    pub bytes_out: u64,
    pub bytes_in: u64,
    pub errors: u64,
}

pub struct PeerbusPublisher {
    publisher: Publisher<RawMsg>,
}

pub struct PeerbusSubscriber {
    subscriber: Subscriber<RawMsg>,
}

pub struct PeerbusSample {
    sample: NodeSample<RawMsg>,
}

pub struct PeerbusDatapodPublisher {
    publisher: Publisher<DatapodMsg>,
}

pub struct PeerbusDatapodSubscriber {
    subscriber: Subscriber<DatapodMsg>,
}

pub struct PeerbusDatapodSample {
    sample: NodeSample<DatapodMsg>,
}

pub struct PeerbusDatapodMessage {
    type_hash: u64,
    wire: Vec<u8>,
}

pub struct PeerbusDatapodMessages {
    messages: Vec<PeerbusDatapodMessage>,
}

/// Preferred three-letter generic-datapod que/ans answer list handle name.
///
/// `PeerbusDatapodMessages` remains the shared finite-list storage type for
/// compatibility with earlier binding code.
pub type PeerbusDatapodAnswers = PeerbusDatapodMessages;

pub struct PeerbusPeerPathDiagnostics {
    diag: crate::PeerPathDiagnostics,
}

/// An owned, received message (kind tag + payload bytes).
pub struct PeerbusMessage {
    kind: u64,
    data: Vec<u8>,
}

pub struct PeerbusReqClient {
    client: ReqClient<RawMsg, RawMsg>,
}

pub struct PeerbusReqServer {
    server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusDatapodReqClient {
    client: ReqClient<DatapodMsg, DatapodMsg>,
}

pub struct PeerbusDatapodReqServer {
    server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodQueClient {
    client: QueClient<DatapodMsg, DatapodMsg>,
}

pub struct PeerbusDatapodAnsServer {
    server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodPutClient {
    client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodAckServer {
    server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodPipClient {
    client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodPipServer {
    server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusQueClient {
    client: QueClient<RawMsg, RawMsg>,
}

pub struct PeerbusAnsServer {
    server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusPutClient {
    client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
}

pub struct PeerbusAckServer {
    server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusPuts {
    server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
    token: Option<PutAckToken>,
    first: Option<PeerbusMessage>,
    done: bool,
}

pub struct PeerbusDatapodPuts {
    server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
    token: Option<PutAckToken>,
    first: Option<PeerbusDatapodMessage>,
    done: bool,
}

pub struct PeerbusDatapodPutUpload {
    client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
    token: Option<PutUploadToken>,
}

pub struct PeerbusPipClient {
    client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
}

pub struct PeerbusPipServer {
    server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusPendingReq {
    server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
    reply: Option<ReqReplyToken>,
    request: PeerbusMessage,
}

pub struct PeerbusPendingDatapodReq {
    server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
    reply: Option<ReqReplyToken>,
    request: PeerbusDatapodMessage,
}

pub struct PeerbusPendingDatapodQue {
    server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
    answers: Option<AnsReplyToken>,
    request: PeerbusDatapodMessage,
}

pub struct PeerbusPendingQue {
    server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
    answers: Option<AnsReplyToken>,
    request: PeerbusMessage,
}

pub struct PeerbusPutUpload {
    client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
    token: Option<PutUploadToken>,
}

/// Preferred three-letter put/ack sender handle name.
///
/// `PeerbusPutUpload` remains as a compatibility alias in the C ABI.
pub type PeerbusPutSender = PeerbusPutUpload;

/// Preferred three-letter generic-datapod put/ack sender handle name.
///
/// `PeerbusDatapodPutUpload` remains as a compatibility alias in the C ABI.
pub type PeerbusDatapodPutSender = PeerbusDatapodPutUpload;

pub struct PeerbusPip {
    client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
    token: Option<PipSessionToken>,
    incoming_done: bool,
    outgoing_done: bool,
}

pub struct PeerbusDatapodPip {
    client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
    token: Option<PipSessionToken>,
    incoming_done: bool,
    outgoing_done: bool,
}

pub struct PeerbusPendingPip {
    server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
    token: Option<PipServerToken>,
    first: Option<PeerbusMessage>,
    incoming_done: bool,
    outgoing_done: bool,
}

pub struct PeerbusPendingDatapodPip {
    server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
    token: Option<PipServerToken>,
    first: Option<PeerbusDatapodMessage>,
    incoming_done: bool,
    outgoing_done: bool,
}

/// Passed to a request handler so it can set the response.
pub struct PeerbusResponder {
    kind: u64,
    data: Vec<u8>,
    set: bool,
}

/// Request handler callback: receives the request `kind` + bytes and the
/// `responder` to fill via [`peerbus_responder_set`].
pub type PeerbusReqHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        kind: u64,
        data: *const u8,
        len: usize,
        responder: *mut PeerbusResponder,
    ),
>;

/// Delivery behavior for C QoS: reliable, in-order delivery.
pub const PEERBUS_DELIVERY_RELIABLE: u32 = 0;
/// Delivery behavior for C QoS: keep only the latest value.
pub const PEERBUS_DELIVERY_LATEST: u32 = 1;
/// Delivery behavior for C QoS: best-effort, may drop.
pub const PEERBUS_DELIVERY_BEST_EFFORT: u32 = 2;

/// C mirror of [`TopicQos`]. Zero fields are allowed; use
/// [`peerbus_topic_qos_reliable`], [`peerbus_topic_qos_latest`], or
/// [`peerbus_topic_qos_best_effort`] for canonical defaults.
///
/// `delivery` is a plain integer (not a Rust enum) so that an out-of-range
/// value from C is never undefined behavior: it is validated at every use
/// (`0..=2`, see [`PEERBUS_DELIVERY_RELIABLE`] and friends) and an invalid
/// value fails the call via [`peerbus_last_error_message`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusTopicQos {
    pub delivery: u32,
    pub max_message_bytes: usize,
    pub max_inflight_bytes: usize,
    pub chunk_bytes: usize,
    pub subscriber_queue: usize,
    pub priority: u8,
}

impl From<TopicQos> for PeerbusTopicQos {
    fn from(value: TopicQos) -> Self {
        let delivery = match value.delivery {
            DeliveryPolicy::Reliable => PEERBUS_DELIVERY_RELIABLE,
            DeliveryPolicy::Latest => PEERBUS_DELIVERY_LATEST,
            DeliveryPolicy::BestEffort => PEERBUS_DELIVERY_BEST_EFFORT,
        };
        Self {
            delivery,
            max_message_bytes: value.max_message_bytes,
            max_inflight_bytes: value.max_inflight_bytes,
            chunk_bytes: value.chunk_bytes,
            subscriber_queue: value.subscriber_queue,
            priority: value.priority,
        }
    }
}

/// Fallible conversion from the C QoS mirror to [`TopicQos`]. Rejects an
/// out-of-range `delivery` discriminant (must be `0..=2`) rather than
/// materializing an invalid enum, which would be undefined behavior.
fn topic_qos_from_c(value: PeerbusTopicQos) -> Result<TopicQos, ()> {
    let mut qos = match value.delivery {
        PEERBUS_DELIVERY_RELIABLE => TopicQos::reliable(),
        PEERBUS_DELIVERY_LATEST => TopicQos::latest(),
        PEERBUS_DELIVERY_BEST_EFFORT => TopicQos::best_effort(),
        _ => {
            set_last_error(
                "invalid QoS delivery policy (expected 0=RELIABLE, 1=LATEST, 2=BEST_EFFORT)",
            );
            return Err(());
        }
    };
    if value.max_message_bytes != 0 {
        qos.max_message_bytes = value.max_message_bytes;
    }
    if value.max_inflight_bytes != 0 {
        qos.max_inflight_bytes = value.max_inflight_bytes;
    }
    if value.chunk_bytes != 0 {
        qos.chunk_bytes = value.chunk_bytes;
    }
    if value.subscriber_queue != 0 {
        qos.subscriber_queue = value.subscriber_queue;
    }
    qos.priority = value.priority;
    Ok(qos)
}

/// Test-only shim exposing the private [`topic_qos_from_c`] conversion so the
/// FFI test suite can assert the zero-fill-to-policy-defaults contract and the
/// out-of-range rejection without standing up a live transport. Returns
/// `None` for an invalid `delivery`. Not part of the stable C ABI (it is a
/// plain Rust `fn`, so cbindgen does not emit it).
#[doc(hidden)]
pub fn __peerbus_topic_qos_from_c(value: PeerbusTopicQos) -> Option<TopicQos> {
    topic_qos_from_c(value).ok()
}

/// Borrowed message input used by finite C convenience APIs.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusRawMessage {
    pub kind: u64,
    pub data: PeerbusBytes,
}

/// Borrowed datapod message input used by generic datapod C convenience APIs.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusDatapodRawMessage {
    pub type_hash: u64,
    pub wire: PeerbusBytes,
}

/// Owned message list returned by que/ans and pip convenience calls.
pub struct PeerbusMessages {
    messages: Vec<PeerbusMessage>,
}

/// Preferred three-letter que/ans answer list handle name.
///
/// `PeerbusMessages` remains the shared finite-list storage type for
/// compatibility with earlier binding code.
pub type PeerbusAnswers = PeerbusMessages;

/// Passed to a que/ans handler so it can append answer items.
pub struct PeerbusAnsResponder {
    messages: Vec<RawMsg>,
}

/// Passed to put/ack or pip handlers so they can build the final reply list.
pub struct PeerbusMessageResponder {
    messages: Vec<RawMsg>,
}

pub type PeerbusAnsHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        kind: u64,
        data: *const u8,
        len: usize,
        responder: *mut PeerbusAnsResponder,
    ),
>;

pub type PeerbusAckHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        items: *const PeerbusMessages,
        responder: *mut PeerbusResponder,
    ),
>;

pub type PeerbusPipHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        items: *const PeerbusMessages,
        responder: *mut PeerbusMessageResponder,
    ),
>;

// ---- helpers ----

unsafe fn cstr<'a>(ptr: *const c_char) -> Result<&'a str, ()> {
    if ptr.is_null() {
        set_last_error("null string argument");
        return Err(());
    }
    // SAFETY: caller promises a valid NUL-terminated C string.
    match unsafe { CStr::from_ptr(ptr) }.to_str() {
        Ok(s) => Ok(s),
        Err(_) => {
            set_last_error("string argument is not valid UTF-8");
            Err(())
        }
    }
}

unsafe fn bytes_in<'a>(data: *const u8, len: usize) -> Result<&'a [u8], ()> {
    if len == 0 {
        Ok(&[])
    } else if data.is_null() {
        set_last_error("null byte buffer with non-zero length");
        Err(())
    } else {
        // SAFETY: caller promises `len` valid bytes at `data`.
        Ok(unsafe { std::slice::from_raw_parts(data, len) })
    }
}

unsafe fn raw_messages_in<'a>(
    items: *const PeerbusRawMessage,
    len: usize,
) -> Result<&'a [PeerbusRawMessage], ()> {
    if items.is_null() {
        if len == 0 {
            Ok(&[])
        } else {
            set_last_error("null message array with non-zero length");
            Err(())
        }
    } else {
        // SAFETY: caller promises `len` valid PeerbusRawMessage values.
        Ok(unsafe { std::slice::from_raw_parts(items, len) })
    }
}

unsafe fn datapod_raw_messages_in<'a>(
    items: *const PeerbusDatapodRawMessage,
    len: usize,
) -> Result<&'a [PeerbusDatapodRawMessage], ()> {
    if items.is_null() {
        if len == 0 {
            Ok(&[])
        } else {
            set_last_error("null datapod message array with non-zero length");
            Err(())
        }
    } else {
        Ok(unsafe { std::slice::from_raw_parts(items, len) })
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        set_last_error("endpoint address hex has odd length");
        return Err(());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = match nibble(pair[0]) {
            Some(value) => value,
            None => {
                set_last_error("invalid endpoint address hex");
                return Err(());
            }
        };
        let lo = match nibble(pair[1]) {
            Some(value) => value,
            None => {
                set_last_error("invalid endpoint address hex");
                return Err(());
            }
        };
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn encode_endpoint_addr(addr: &EndpointAddr) -> Result<CString, ()> {
    let bytes = postcard::to_stdvec(addr).map_err(|e| {
        set_last_error(format!("encode endpoint addr: {e}"));
    })?;
    CString::new(hex_encode(&bytes)).map_err(|_| {
        set_last_error("encoded endpoint addr contained NUL");
    })
}

unsafe fn endpoint_addr_in(ptr: *const c_char) -> Result<EndpointAddr, ()> {
    let encoded = unsafe { cstr(ptr) }?;
    let bytes = hex_decode(encoded)?;
    postcard::from_bytes(&bytes).map_err(|e| {
        set_last_error(format!("decode endpoint addr: {e}"));
    })
}

unsafe fn peer_arg(peer: *const c_char) -> Result<Result<EndpointAddr, String>, ()> {
    let value = unsafe { cstr(peer) }?;
    let looks_hex = value.len() % 2 == 0 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit());
    if looks_hex
        && let Ok(bytes) = hex_decode(value)
        && let Ok(addr) = postcard::from_bytes::<EndpointAddr>(&bytes)
    {
        return Ok(Ok(addr));
    }
    Ok(Err(value.to_string()))
}

fn item_stats_out(stats: crate::ItemStats) -> PeerbusItemStats {
    PeerbusItemStats {
        messages_out: stats.messages_out,
        messages_in: stats.messages_in,
        bytes_out: stats.bytes_out,
        bytes_in: stats.bytes_in,
        errors: stats.errors,
    }
}

fn message_from_raw(raw: PeerbusRawMessage) -> RawMsg {
    // Element pointers come from an already-validated array; treat a null or
    // empty element as an empty payload (the array-level check rejects a null
    // array with non-zero length).
    let bytes: &[u8] = if raw.data.ptr.is_null() || raw.data.len == 0 {
        &[]
    } else {
        // SAFETY: caller promises `len` valid bytes at `ptr`.
        unsafe { std::slice::from_raw_parts(raw.data.ptr, raw.data.len) }
    };
    RawMsg::new(raw.kind, bytes)
}

fn message_from_datapod_raw(raw: PeerbusDatapodRawMessage) -> DatapodMsg {
    let wire: &[u8] = if raw.wire.ptr.is_null() || raw.wire.len == 0 {
        &[]
    } else {
        // SAFETY: caller promises `len` valid bytes at `ptr`.
        unsafe { std::slice::from_raw_parts(raw.wire.ptr, raw.wire.len) }
    };
    DatapodMsg::new(raw.type_hash, wire)
}

fn owned_message(kind: u64, payload: &[u8]) -> PeerbusMessage {
    PeerbusMessage {
        kind,
        data: payload.to_vec(),
    }
}

fn owned_datapod_message(type_hash: u64, wire: &[u8]) -> PeerbusDatapodMessage {
    PeerbusDatapodMessage {
        type_hash,
        wire: wire.to_vec(),
    }
}

fn messages_from_raw(values: Vec<RawMsg>) -> PeerbusMessages {
    PeerbusMessages {
        messages: values
            .into_iter()
            .map(|msg| owned_message(msg.kind, &msg.data))
            .collect(),
    }
}

fn build_node_from_config(cfg: PeerbusNodeConfig) -> Result<Node, ()> {
    let mut builder = Node::builder();
    if !cfg.identity.is_null() {
        let identity = unsafe { cstr(cfg.identity) }?;
        builder = builder.identity(identity);
    }
    if cfg.no_relay {
        builder = builder.no_relay();
    }
    if !cfg.system_did.is_null() {
        let system_did = unsafe { cstr(cfg.system_did) }?;
        builder = builder.system_did(system_did);
    }
    if cfg.allowed_peers_len != 0 {
        if cfg.allowed_peers.is_null() {
            set_last_error("allowed_peers is null but allowed_peers_len is non-zero");
            return Err(());
        }
        // SAFETY: caller promises `allowed_peers_len` valid C string pointers.
        let peers = unsafe { std::slice::from_raw_parts(cfg.allowed_peers, cfg.allowed_peers_len) };
        for peer in peers {
            let peer = unsafe { cstr(*peer) }?;
            builder = builder.allow_peer(peer);
        }
    }
    if cfg.allow_any_peer {
        builder = builder.allow_any_peer();
    }
    if cfg.max_payload_bytes != 0
        || cfg.history_depth != 0
        || cfg.subscriber_buffer != 0
        || cfg.max_publishers != 0
        || cfg.max_subscribers != 0
    {
        let mut local_cfg = LocalConfig::default();
        if cfg.max_payload_bytes != 0 {
            local_cfg.max_payload_bytes = cfg.max_payload_bytes;
        }
        if cfg.history_depth != 0 {
            local_cfg.history_depth = cfg.history_depth;
        }
        if cfg.subscriber_buffer != 0 {
            local_cfg.subscriber_buffer = cfg.subscriber_buffer;
        }
        if cfg.max_publishers != 0 {
            local_cfg.max_publishers = cfg.max_publishers;
        }
        if cfg.max_subscribers != 0 {
            local_cfg.max_subscribers = cfg.max_subscribers;
        }
        builder = builder.local_config(local_cfg);
    }
    builder.bind().map_err(|e| {
        set_last_error(e.to_string());
    })
}

// ---- node ----

/// Create a node. `identity` may be NULL for an ephemeral key. Returns
/// NULL on failure (see [`peerbus_last_error_message`]).
///
/// The node this creates denies every inbound connection (no allowlist, no
/// `allow_any_peer`). To serve remote peers, use
/// [`peerbus_node_new_with_config`] and set `allowed_peers` (preferred) or
/// `allow_any_peer`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_new(identity: *const c_char, no_relay: bool) -> *mut PeerbusNode {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let cfg = PeerbusNodeConfig {
        identity,
        no_relay,
        system_did: ptr::null(),
        allowed_peers: ptr::null(),
        allowed_peers_len: 0,
        allow_any_peer: false,
        max_payload_bytes: 0,
        history_depth: 0,
        subscriber_buffer: 0,
        max_publishers: 0,
        max_subscribers: 0,
    };
    match build_node_from_config(cfg) {
        Ok(node) => Box::into_raw(Box::new(PeerbusNode { node })),
        Err(()) => ptr::null_mut(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_config_default() -> PeerbusNodeConfig {
    PeerbusNodeConfig {
        identity: ptr::null(),
        no_relay: false,
        system_did: ptr::null(),
        allowed_peers: ptr::null(),
        allowed_peers_len: 0,
        allow_any_peer: false,
        max_payload_bytes: 0,
        history_depth: 0,
        subscriber_buffer: 0,
        max_publishers: 0,
        max_subscribers: 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_new_with_config(cfg: PeerbusNodeConfig) -> *mut PeerbusNode {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    match build_node_from_config(cfg) {
        Ok(node) => Box::into_raw(Box::new(PeerbusNode { node })),
        Err(()) => ptr::null_mut(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_free(node: *mut PeerbusNode) {
    ffi_guard((), move || {
    if node.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw in peerbus_node_new.
    unsafe { drop(Box::from_raw(node)) };
})
}

/// This node's identity as a `did:key:z6Mk…` string. Caller owns the
/// returned C string and must free it with [`peerbus_string_free`].
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_did_key(node: *const PeerbusNode) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    match CString::new(node.node.endpoint_did_key()) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("did:key contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_endpoint_addr(node: *const PeerbusNode) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    match encode_endpoint_addr(&node.node.endpoint_addr()) {
        Ok(addr) => addr.into_raw(),
        Err(()) => ptr::null_mut(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_add_topic_route(
    node: *const PeerbusNode,
    topic: *const c_char,
    endpoint_addr: *const c_char,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return false;
    }
    let topic = match unsafe { cstr(topic) } {
        Ok(topic) => topic,
        Err(()) => return false,
    };
    let endpoint_addr = match unsafe { endpoint_addr_in(endpoint_addr) } {
        Ok(addr) => addr,
        Err(()) => return false,
    };
    let node = unsafe { &*node };
    match node.node.add_topic_route(topic, endpoint_addr) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_add_system_peer(
    node: *const PeerbusNode,
    endpoint_addr: *const c_char,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return false;
    }
    let endpoint_addr = match unsafe { endpoint_addr_in(endpoint_addr) } {
        Ok(addr) => addr,
        Err(()) => return false,
    };
    let node = unsafe { &*node };
    match node.node.add_system_peer(endpoint_addr) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_stats(node: *const PeerbusNode) -> PeerbusNodeStats {
    ffi_guard(PeerbusNodeStats::default(), move || {
    if node.is_null() {
        return PeerbusNodeStats::default();
    }
    let stats = unsafe { &*node }.node.stats();
    PeerbusNodeStats {
        publisher_topics: stats.publisher_topics,
        cached_peers: stats.cached_peers,
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_peer_path_diagnostics(
    node: *const PeerbusNode,
    endpoint_addr: *const c_char,
) -> *mut PeerbusPeerPathDiagnostics {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let endpoint_addr = match unsafe { endpoint_addr_in(endpoint_addr) } {
        Ok(addr) => addr,
        Err(()) => return ptr::null_mut(),
    };
    let node = unsafe { &*node };
    match node.node.peer_path_diagnostics(endpoint_addr) {
        Ok(Some(diag)) => Box::into_raw(Box::new(PeerbusPeerPathDiagnostics { diag })),
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_free(diag: *mut PeerbusPeerPathDiagnostics) {
    ffi_guard((), move || {
    if diag.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(diag)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_peer(
    diag: *const PeerbusPeerPathDiagnostics,
) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if diag.is_null() {
        set_last_error("null peer path diagnostics handle");
        return ptr::null_mut();
    }
    let did = crate::did_key::endpoint_id_to_did_key(&unsafe { &*diag }.diag.peer);
    match CString::new(did) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("peer did:key contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_count(
    diag: *const PeerbusPeerPathDiagnostics,
) -> usize {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }.diag.paths.len()
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_max_datagram_size(
    diag: *const PeerbusPeerPathDiagnostics,
    out: *mut usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() || out.is_null() {
        return false;
    }
    let Some(value) = unsafe { &*diag }.diag.max_datagram_size else {
        return false;
    };
    unsafe { *out = value };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_datagram_send_buffer_space(
    diag: *const PeerbusPeerPathDiagnostics,
) -> usize {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }.diag.datagram_send_buffer_space
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_id(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if diag.is_null() {
        set_last_error("null peer path diagnostics handle");
        return ptr::null_mut();
    }
    let Some(path) = unsafe { &*diag }.diag.paths.get(index) else {
        set_last_error("path index out of range");
        return ptr::null_mut();
    };
    match CString::new(path.path_id.as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("path id contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_remote_addr(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if diag.is_null() {
        set_last_error("null peer path diagnostics handle");
        return ptr::null_mut();
    }
    let Some(path) = unsafe { &*diag }.diag.paths.get(index) else {
        set_last_error("path index out of range");
        return ptr::null_mut();
    };
    match CString::new(path.remote_addr.as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("path remote addr contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_selected(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() {
        return false;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .is_some_and(|path| path.selected)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_is_ip(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() {
        return false;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .is_some_and(|path| path.is_ip)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_is_relay(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() {
        return false;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .is_some_and(|path| path.is_relay)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_rtt_ms(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> f64 {
    ffi_guard(0.0, move || {
    if diag.is_null() {
        return 0.0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0.0, |path| path.rtt.as_secs_f64() * 1000.0)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_current_mtu(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> u16 {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0, |path| path.current_mtu)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_cwnd(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0, |path| path.cwnd)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_lost_packets(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0, |path| path.lost_packets)
})
}

/// Free a string returned by peerbus (e.g. [`peerbus_node_did_key`]).
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_string_free(s: *mut c_char) {
    ffi_guard((), move || {
    if s.is_null() {
        return;
    }
    // SAFETY: originated from CString::into_raw.
    unsafe { drop(CString::from_raw(s)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_topic_qos_reliable() -> PeerbusTopicQos {
    TopicQos::reliable().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_topic_qos_latest() -> PeerbusTopicQos {
    TopicQos::latest().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_topic_qos_best_effort() -> PeerbusTopicQos {
    TopicQos::best_effort().into()
}

// ---- pub/sub ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusPublisher {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_publisher_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPublisher {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.publisher_with_qos::<RawMsg>(topic, qos) {
        Ok(publisher) => Box::into_raw(Box::new(PeerbusPublisher { publisher })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_free(publisher: *mut PeerbusPublisher) {
    ffi_guard((), move || {
    if publisher.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(publisher)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_stats(
    publisher: *const PeerbusPublisher,
) -> PeerbusPublisherStats {
    ffi_guard(PeerbusPublisherStats::default(), move || {
    if publisher.is_null() {
        return PeerbusPublisherStats::default();
    }
    let stats = unsafe { &*publisher }.publisher.stats();
    PeerbusPublisherStats {
        published: stats.published,
        remote_dropped: stats.remote_dropped,
        stale_dropped: stats.stale_dropped,
        bytes_sent: stats.bytes_sent,
        send_errors: stats.send_errors,
    }
})
}

/// Publish `data` (`len` bytes) with user tag `kind`. Returns false on
/// failure.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_send(
    publisher: *mut PeerbusPublisher,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if publisher.is_null() {
        set_last_error("null publisher handle");
        return false;
    }
    // SAFETY: validated non-null.
    let publisher = unsafe { &mut *publisher };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match publisher.publisher.send(&RawMsg::new(kind, bytes)) {
        Ok(_) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_subscriber_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node.node.subscriber_with_qos::<RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .subscriber_with_qos::<RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(subscriber) => Box::into_raw(Box::new(PeerbusSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscribe_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.subscribe_with_qos::<RawMsg>(topic, qos) {
        Ok(subscriber) => Box::into_raw(Box::new(PeerbusSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_free(subscriber: *mut PeerbusSubscriber) {
    ffi_guard((), move || {
    if subscriber.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(subscriber)) };
})
}

// ---- generic datapod pub/sub ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPublisher {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .publisher_with_qos::<DatapodMsg>(topic, qos)
    {
        Ok(publisher) => Box::into_raw(Box::new(PeerbusDatapodPublisher { publisher })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscribe_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_subscribe_new_with_qos(node, topic, qos)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_free(publisher: *mut PeerbusDatapodPublisher) {
    ffi_guard((), move || {
    if publisher.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(publisher)) };
})
}

/// Publish a datapod wire message: `type_hash` plus `header || payload` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_send(
    publisher: *mut PeerbusDatapodPublisher,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if publisher.is_null() {
        set_last_error("null publisher handle");
        return false;
    }
    let publisher = unsafe { &mut *publisher };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match publisher.publisher.send(&DatapodMsg::new(type_hash, wire)) {
        Ok(_) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .subscriber_with_qos::<DatapodMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .subscriber_with_qos::<DatapodMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(subscriber) => Box::into_raw(Box::new(PeerbusDatapodSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscribe_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .subscribe_with_qos::<DatapodMsg>(topic, qos)
    {
        Ok(subscriber) => Box::into_raw(Box::new(PeerbusDatapodSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscribe_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_datapod_subscribe_new_with_qos(node, topic, qos)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_free(subscriber: *mut PeerbusDatapodSubscriber) {
    ffi_guard((), move || {
    if subscriber.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(subscriber)) };
})
}

/// Poll for a datapod sample without copying the wire bytes.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_take_sample(
    subscriber: *mut PeerbusDatapodSubscriber,
    out_sample: *mut *mut PeerbusDatapodSample,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_sample.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            unsafe { *out_sample = Box::into_raw(Box::new(PeerbusDatapodSample { sample })) };
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_sample_type_hash(sample: *const PeerbusDatapodSample) -> u64 {
    ffi_guard(0, move || {
    if sample.is_null() {
        return 0;
    }
    unsafe { &*sample }.sample.header().type_hash
})
}

/// Borrowed zero-copy view of datapod `header || payload` wire bytes.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_sample_wire(sample: *const PeerbusDatapodSample) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if sample.is_null() {
        return PeerbusBytes::empty();
    }
    let sample = unsafe { &*sample };
    PeerbusBytes {
        ptr: sample.sample.payload().as_ptr(),
        len: sample.sample.payload().len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_sample_free(sample: *mut PeerbusDatapodSample) {
    ffi_guard((), move || {
    if sample.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(sample)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_message_type_hash(message: *const PeerbusDatapodMessage) -> u64 {
    ffi_guard(0, move || {
    if message.is_null() {
        return 0;
    }
    unsafe { &*message }.type_hash
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_message_wire(
    message: *const PeerbusDatapodMessage,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if message.is_null() {
        return PeerbusBytes::empty();
    }
    let message = unsafe { &*message };
    PeerbusBytes {
        ptr: message.wire.as_ptr(),
        len: message.wire.len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_message_free(message: *mut PeerbusDatapodMessage) {
    ffi_guard((), move || {
    if message.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(message)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_len(messages: *const PeerbusDatapodMessages) -> usize {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }.messages.len()
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_type_hash_at(
    messages: *const PeerbusDatapodMessages,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map_or(0, |message| message.type_hash)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_wire_at(
    messages: *const PeerbusDatapodMessages,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if messages.is_null() {
        return PeerbusBytes::empty();
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map_or_else(PeerbusBytes::empty, |message| PeerbusBytes {
            ptr: message.wire.as_ptr(),
            len: message.wire.len(),
        })
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_free(messages: *mut PeerbusDatapodMessages) {
    ffi_guard((), move || {
    if messages.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(messages)) };
})
}

// Preferred que/ans answer-list aliases. These wrap the shared finite-list
// storage used by the older `messages` names so callers can use the public
// three-letter `ans` terminology without a second ownership model.

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_len(answers: *const PeerbusDatapodAnswers) -> usize {
    ffi_guard(0, move || {
    peerbus_datapod_messages_len(answers)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_type_hash_at(
    answers: *const PeerbusDatapodAnswers,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    peerbus_datapod_messages_type_hash_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_wire_at(
    answers: *const PeerbusDatapodAnswers,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    peerbus_datapod_messages_wire_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_free(answers: *mut PeerbusDatapodAnswers) {
    ffi_guard((), move || {
    peerbus_datapod_messages_free(answers);
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_stats(
    subscriber: *const PeerbusSubscriber,
) -> PeerbusSubscriberStats {
    ffi_guard(PeerbusSubscriberStats::default(), move || {
    if subscriber.is_null() {
        return PeerbusSubscriberStats::default();
    }
    let stats = unsafe { &*subscriber }.subscriber.stats();
    PeerbusSubscriberStats {
        received: stats.received,
        disconnects: stats.disconnects,
        stale_dropped: stats.stale_dropped,
        incomplete_dropped: stats.incomplete_dropped,
        bytes_received: stats.bytes_received,
    }
})
}

/// Poll for the next sample. Returns `1` and writes an owned message to
/// `*out_message` when one is available, `0` when none is ready, and
/// `-1` on error. A returned message must be freed with
/// [`peerbus_message_free`].
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_take(
    subscriber: *mut PeerbusSubscriber,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_message.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    // SAFETY: validated non-null.
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            let msg = PeerbusMessage {
                kind: sample.header().kind,
                data: sample.payload().to_vec(),
            };
            // SAFETY: out_message validated non-null.
            unsafe { *out_message = Box::into_raw(Box::new(msg)) };
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

/// Poll for the next sample without copying payload bytes.
///
/// Returns `1` and writes a borrowed sample handle to `*out_sample` when one is
/// available, `0` when none is ready, and `-1` on error. A returned sample must
/// be freed with [`peerbus_sample_free`]. The byte view returned from
/// [`peerbus_sample_data`] is valid until that free call.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_take_sample(
    subscriber: *mut PeerbusSubscriber,
    out_sample: *mut *mut PeerbusSample,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_sample.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            unsafe { *out_sample = Box::into_raw(Box::new(PeerbusSample { sample })) };
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_sample_kind(sample: *const PeerbusSample) -> u64 {
    ffi_guard(0, move || {
    if sample.is_null() {
        return 0;
    }
    unsafe { &*sample }.sample.header().kind
})
}

/// Borrowed zero-copy view of a sample payload.
///
/// For local SHM this points directly into the shared-memory slot and pins that
/// slot until [`peerbus_sample_free`] is called. Copy it if you need to keep the
/// data longer.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_sample_data(sample: *const PeerbusSample) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if sample.is_null() {
        return PeerbusBytes::empty();
    }
    let sample = unsafe { &*sample };
    PeerbusBytes {
        ptr: sample.sample.payload().as_ptr(),
        len: sample.sample.payload().len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_sample_free(sample: *mut PeerbusSample) {
    ffi_guard((), move || {
    if sample.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(sample)) };
})
}

// ---- message accessors ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_new(
    kind: u64,
    data: *const u8,
    len: usize,
) -> *mut PeerbusMessage {
    ffi_guard(ptr::null_mut(), move || {
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return ptr::null_mut(),
    };
    Box::into_raw(Box::new(owned_message(kind, bytes)))
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_kind(message: *const PeerbusMessage) -> u64 {
    ffi_guard(0, move || {
    if message.is_null() {
        return 0;
    }
    // SAFETY: validated non-null.
    unsafe { (*message).kind }
})
}

/// Borrowed view of the message payload, valid until the message is
/// freed.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_data(message: *const PeerbusMessage) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if message.is_null() {
        return PeerbusBytes::empty();
    }
    // SAFETY: validated non-null.
    let message = unsafe { &*message };
    PeerbusBytes {
        ptr: message.data.as_ptr(),
        len: message.data.len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_free(message: *mut PeerbusMessage) {
    ffi_guard((), move || {
    if message.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(message)) };
})
}

// ---- generic datapod req/res client ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodReqClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .req_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos),
        Err(name) => {
            node.node
                .req_client_with_qos::<DatapodMsg, DatapodMsg>(name.as_str(), topic, qos)
        }
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusDatapodReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodReqClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .req_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(client) => Box::into_raw(Box::new(PeerbusDatapodReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_free(client: *mut PeerbusDatapodReqClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_stats(
    client: *const PeerbusDatapodReqClient,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_call(
    client: *mut PeerbusDatapodReqClient,
    type_hash: u64,
    wire: *const u8,
    len: usize,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null datapod req client or out pointer");
        return false;
    }
    let client = unsafe { &mut *client };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match client.client.call(&DatapodMsg::new(type_hash, wire)) {
        Ok(res) => {
            let msg = owned_datapod_message(res.header().type_hash, res.payload());
            unsafe { *out_message = Box::into_raw(Box::new(msg)) };
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

// ---- generic datapod req/res server ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodReqServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .req_server_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodReqServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_free(server: *mut PeerbusDatapodReqServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_stats(
    server: *const PeerbusDatapodReqServer,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_take(
    server: *mut PeerbusDatapodReqServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingDatapodReq,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null datapod req server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (req, reply) = pending.into_parts();
                let request = owned_datapod_message(req.header().type_hash, req.payload());
                drop(guard);
                let handle = PeerbusPendingDatapodReq {
                    server: arc.clone(),
                    reply: Some(reply),
                    request,
                };
                unsafe { *out_pending = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_pending = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_req_request(
    pending: *const PeerbusPendingDatapodReq,
) -> *const PeerbusDatapodMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusDatapodMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_req_reply(
    pending: *mut PeerbusPendingDatapodReq,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending datapod req handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(reply) = pending.reply.take() else {
        set_last_error("pending datapod req already replied");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).respond_pending(reply, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_req_free(pending: *mut PeerbusPendingDatapodReq) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

// ---- generic datapod que/ans ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodQueClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .que_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos),
        Err(name) => {
            node.node
                .que_client_with_qos::<DatapodMsg, DatapodMsg>(name.as_str(), topic, qos)
        }
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusDatapodQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodQueClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .que_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(client) => Box::into_raw(Box::new(PeerbusDatapodQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_free(client: *mut PeerbusDatapodQueClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_stats(
    client: *const PeerbusDatapodQueClient,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_send(
    client: *mut PeerbusDatapodQueClient,
    type_hash: u64,
    wire: *const u8,
    len: usize,
    out_messages: *mut *mut PeerbusDatapodMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null datapod que client or out pointer");
        return false;
    }
    let client = unsafe { &mut *client };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    let mut answers = match client.client.send(&DatapodMsg::new(type_hash, wire)) {
        Ok(answers) => answers,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    let mut messages = Vec::new();
    loop {
        match answers.next() {
            Ok(Some(ans)) => {
                messages.push(owned_datapod_message(ans.header().type_hash, ans.payload()))
            }
            Ok(None) => break,
            Err(e) => {
                set_last_error(e.to_string());
                return false;
            }
        }
    }
    unsafe { *out_messages = Box::into_raw(Box::new(PeerbusDatapodMessages { messages })) };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ans_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodAnsServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .ans_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodAnsServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ans_server_free(server: *mut PeerbusDatapodAnsServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ans_server_stats(
    server: *const PeerbusDatapodAnsServer,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ans_server_take(
    server: *mut PeerbusDatapodAnsServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingDatapodQue,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null datapod ans server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (que, answers) = pending.into_parts();
                let handle = PeerbusPendingDatapodQue {
                    server: arc.clone(),
                    answers: Some(answers),
                    request: owned_datapod_message(que.header().type_hash, que.payload()),
                };
                unsafe { *out_pending = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_pending = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_que_request(
    pending: *const PeerbusPendingDatapodQue,
) -> *const PeerbusDatapodMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusDatapodMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_que_send(
    pending: *mut PeerbusPendingDatapodQue,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending datapod que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers else {
        set_last_error("pending datapod que already finished");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).send_pending(answers, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_que_finish(
    pending: *mut PeerbusPendingDatapodQue,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending datapod que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers.take() else {
        return true;
    };
    match lock_inner(&pending.server).finish_pending(answers) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_que_free(pending: *mut PeerbusPendingDatapodQue) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

// ---- generic datapod put/ack ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPutClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .put_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos),
        Err(name) => {
            node.node
                .put_client_with_qos::<DatapodMsg, DatapodMsg>(name.as_str(), topic, qos)
        }
    };
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPutClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPutClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .put_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPutClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_free(client: *mut PeerbusDatapodPutClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_stats(
    client: *const PeerbusDatapodPutClient,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*client }.client).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_upload(
    client: *mut PeerbusDatapodPutClient,
    items: *const PeerbusDatapodRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null datapod put client or out pointer");
        return false;
    }
    let raw_items = match unsafe { datapod_raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &*client };
    let mut guard = lock_inner(&client.client);
    let mut sender = match guard.open() {
        Ok(sender) => sender,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    for raw in raw_items {
        if let Err(e) = sender.send(&message_from_datapod_raw(*raw)) {
            set_last_error(e.to_string());
            return false;
        }
    }
    match sender.finish() {
        Ok(ack) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    ack.header().type_hash,
                    ack.payload(),
                )));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_put(
    client: *mut PeerbusDatapodPutClient,
    items: *const PeerbusDatapodRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_client_upload(client, items, len, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_open(
    client: *mut PeerbusDatapodPutClient,
    out_upload: *mut *mut PeerbusDatapodPutUpload,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_upload.is_null() {
        set_last_error("null datapod put client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_upload() {
        Ok(token) => {
            unsafe {
                *out_upload = Box::into_raw(Box::new(PeerbusDatapodPutUpload {
                    client: client_ref.client.clone(),
                    token: Some(token),
                }));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_open_sender(
    client: *mut PeerbusDatapodPutClient,
    out_sender: *mut *mut PeerbusDatapodPutSender,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_client_open(client, out_sender)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_upload_send(
    upload: *mut PeerbusDatapodPutUpload,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() {
        set_last_error("null datapod put upload handle");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token else {
        set_last_error("datapod put upload already finished");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&upload.client).send_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_sender_send(
    sender: *mut PeerbusDatapodPutSender,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_upload_send(sender, type_hash, wire, len)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_upload_finish(
    upload: *mut PeerbusDatapodPutUpload,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() || out_message.is_null() {
        set_last_error("null datapod put upload or out pointer");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token.take() else {
        set_last_error("datapod put upload already finished");
        return false;
    };
    match lock_inner(&upload.client).finish_pending(token) {
        Ok(ack) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    ack.header().type_hash,
                    ack.payload(),
                )));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_sender_finish(
    sender: *mut PeerbusDatapodPutSender,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_upload_finish(sender, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_upload_free(upload: *mut PeerbusDatapodPutUpload) {
    ffi_guard((), move || {
    if upload.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(upload)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_sender_free(sender: *mut PeerbusDatapodPutSender) {
    ffi_guard((), move || {
    peerbus_datapod_put_upload_free(sender);
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodAckServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .ack_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodAckServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_free(server: *mut PeerbusDatapodAckServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_stats(
    server: *const PeerbusDatapodAckServer,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_take(
    server: *mut PeerbusDatapodAckServer,
    timeout_ms: u64,
    out_puts: *mut *mut PeerbusDatapodPuts,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_puts.is_null() {
        set_last_error("null datapod ack server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_req_id, first, done, token) = pending.into_parts();
                let handle = PeerbusDatapodPuts {
                    server: arc.clone(),
                    token: Some(token),
                    first: first
                        .map(|msg| owned_datapod_message(msg.header().type_hash, msg.payload())),
                    done,
                };
                unsafe { *out_puts = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_puts = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_puts_next(
    puts: *mut PeerbusDatapodPuts,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if puts.is_null() || out_message.is_null() {
        set_last_error("null datapod puts or out pointer");
        return -1;
    }
    let puts = unsafe { &mut *puts };
    if let Some(first) = puts.first.take() {
        unsafe { *out_message = Box::into_raw(Box::new(first)) };
        return 1;
    }
    if puts.done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = puts.token else {
        set_last_error("datapod puts handle already acked or closed");
        return -1;
    };
    let result = lock_inner(&puts.server).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    msg.header().type_hash,
                    msg.payload(),
                )));
            }
            1
        }
        Ok(None) => {
            puts.done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_puts_ack(
    puts: *mut PeerbusDatapodPuts,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if puts.is_null() {
        set_last_error("null datapod puts handle");
        return false;
    }
    let puts = unsafe { &mut *puts };
    let Some(token) = puts.token.take() else {
        set_last_error("datapod puts handle already acked or closed");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&puts.server).ack_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_puts_free(puts: *mut PeerbusDatapodPuts) {
    ffi_guard((), move || {
    if puts.is_null() {
        return;
    }
    let puts_ref = unsafe { &mut *puts };
    if let Some(token) = puts_ref.token.take() {
        lock_inner(&puts_ref.server).close_pending(token);
    }
    unsafe { drop(Box::from_raw(puts)) };
})
}

// ---- generic datapod pip ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPipClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .pip_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos),
        Err(name) => {
            node.node
                .pip_client_with_qos::<DatapodMsg, DatapodMsg>(name.as_str(), topic, qos)
        }
    };
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPipClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPipClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .pip_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPipClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_free(client: *mut PeerbusDatapodPipClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_stats(
    client: *const PeerbusDatapodPipClient,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*client }.client).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_exchange(
    client: *mut PeerbusDatapodPipClient,
    items: *const PeerbusDatapodRawMessage,
    len: usize,
    out_messages: *mut *mut PeerbusDatapodMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null datapod pip client or out pointer");
        return false;
    }
    let raw_items = match unsafe { datapod_raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &*client };
    let mut guard = lock_inner(&client.client);
    let mut pip = match guard.open() {
        Ok(pip) => pip,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    for raw in raw_items {
        if let Err(e) = pip.send(&message_from_datapod_raw(*raw)) {
            set_last_error(e.to_string());
            return false;
        }
    }
    if let Err(e) = pip.finish_send() {
        set_last_error(e.to_string());
        return false;
    }
    let mut messages = Vec::new();
    loop {
        match pip.next() {
            Ok(Some(msg)) => {
                messages.push(owned_datapod_message(msg.header().type_hash, msg.payload()))
            }
            Ok(None) => break,
            Err(e) => {
                set_last_error(e.to_string());
                return false;
            }
        }
    }
    unsafe { *out_messages = Box::into_raw(Box::new(PeerbusDatapodMessages { messages })) };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_open(
    client: *mut PeerbusDatapodPipClient,
    out_pip: *mut *mut PeerbusDatapodPip,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_pip.is_null() {
        set_last_error("null datapod pip client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_session() {
        Ok(token) => {
            unsafe {
                *out_pip = Box::into_raw(Box::new(PeerbusDatapodPip {
                    client: client_ref.client.clone(),
                    token: Some(token),
                    incoming_done: false,
                    outgoing_done: false,
                }));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_send(
    pip: *mut PeerbusDatapodPip,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("datapod pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("datapod pip session is closed");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.client).send_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_finish_send(pip: *mut PeerbusDatapodPip) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("datapod pip session is closed");
        return false;
    };
    let result = lock_inner(&pip.client).finish_send_pending(token);
    match result {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_next(
    pip: *mut PeerbusDatapodPip,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null datapod pip handle or out pointer");
        return -1;
    }
    let pip = unsafe { &mut *pip };
    if pip.incoming_done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = pip.token else {
        set_last_error("datapod pip session is closed");
        return -1;
    };
    let result = lock_inner(&pip.client).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    msg.header().type_hash,
                    msg.payload(),
                )));
            }
            1
        }
        Ok(None) => {
            pip.incoming_done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_close(pip: *mut PeerbusDatapodPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take() {
        lock_inner(&pip_ref.client).close_session(token);
    }
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_free(pip: *mut PeerbusDatapodPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_datapod_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPipServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .pip_server_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodPipServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_free(server: *mut PeerbusDatapodPipServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_stats(
    server: *const PeerbusDatapodPipServer,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_take(
    server: *mut PeerbusDatapodPipServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingDatapodPip,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null datapod pip server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_session_id, first, incoming_done, token) = pending.into_parts();
                let handle = PeerbusPendingDatapodPip {
                    server: arc.clone(),
                    token: Some(token),
                    first: first
                        .map(|msg| owned_datapod_message(msg.header().type_hash, msg.payload())),
                    incoming_done,
                    outgoing_done: false,
                };
                unsafe { *out_pending = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_pending = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_pip_next(
    pip: *mut PeerbusPendingDatapodPip,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null pending datapod pip or out pointer");
        return -1;
    }
    let pip = unsafe { &mut *pip };
    if let Some(first) = pip.first.take() {
        unsafe { *out_message = Box::into_raw(Box::new(first)) };
        return 1;
    }
    if pip.incoming_done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = pip.token else {
        set_last_error("pending datapod pip is closed");
        return -1;
    };
    let result = lock_inner(&pip.server).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    msg.header().type_hash,
                    msg.payload(),
                )));
            }
            1
        }
        Ok(None) => {
            pip.incoming_done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_pip_send(
    pip: *mut PeerbusPendingDatapodPip,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("pending datapod pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("pending datapod pip is closed");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.server).send_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_pip_finish_send(
    pip: *mut PeerbusPendingDatapodPip,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("pending datapod pip is closed");
        return false;
    };
    let result = lock_inner(&pip.server).finish_send_pending(token);
    match result {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_pip_close(pip: *mut PeerbusPendingDatapodPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take() {
        lock_inner(&pip_ref.server).close_pending(token);
    }
    pip_ref.first = None;
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_pip_free(pip: *mut PeerbusPendingDatapodPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_pending_datapod_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

// ---- req/res client ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusReqClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_req_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusReqClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .req_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .req_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusReqClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.req_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(client) => Box::into_raw(Box::new(PeerbusReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_free(client: *mut PeerbusReqClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_stats(client: *const PeerbusReqClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

/// Send a request and block for the response. Returns false on failure;
/// on success writes an owned response message to `*out_message` (free
/// with [`peerbus_message_free`]).
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_call(
    client: *mut PeerbusReqClient,
    kind: u64,
    data: *const u8,
    len: usize,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null client or out pointer");
        return false;
    }
    // SAFETY: validated non-null.
    let client = unsafe { &mut *client };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match client.client.call(&RawMsg::new(kind, bytes)) {
        Ok(res) => {
            let msg = PeerbusMessage {
                kind: res.header().kind,
                data: res.payload().to_vec(),
            };
            // SAFETY: out_message validated non-null.
            unsafe { *out_message = Box::into_raw(Box::new(msg)) };
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

// ---- req/res server ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusReqServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_req_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusReqServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .req_server_with_qos::<RawMsg, RawMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusReqServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_free(server: *mut PeerbusReqServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_stats(server: *const PeerbusReqServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

/// Set the response on a responder passed to a request handler. Copies
/// `data` immediately; safe to call once per handler invocation.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_responder_set(
    responder: *mut PeerbusResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    ffi_guard((), move || {
    if responder.is_null() {
        return;
    }
    // SAFETY: validated non-null; lives on serve_one's stack.
    let responder = unsafe { &mut *responder };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return,
    };
    responder.kind = kind;
    responder.data = bytes.to_vec();
    responder.set = true;
})
}

/// Serve at most one request, waiting up to `timeout_ms`. Invokes
/// `handler` with the request and a responder; whatever the handler sets
/// (via [`peerbus_responder_set`]) is sent back. Returns `1` if a request
/// was served, `0` on timeout, `-1` on error.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_serve_one(
    server: *mut PeerbusReqServer,
    timeout_ms: u64,
    handler: PeerbusReqHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null request handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some((req, reply))) => {
                let kind = req.header().kind;
                let payload = req.payload();
                let mut responder = PeerbusResponder {
                    kind: 0,
                    data: Vec::new(),
                    set: false,
                };
                // SAFETY: handler is a valid fn pointer; responder lives
                // for the duration of this call.
                unsafe {
                    handler(
                        ctx,
                        kind,
                        payload.as_ptr(),
                        payload.len(),
                        &mut responder as *mut PeerbusResponder,
                    )
                };
                let response = RawMsg::new(responder.kind, &responder.data);
                return match reply.respond(&response) {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_take(
    server: *mut PeerbusReqServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingReq,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null req server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (req, reply) = pending.into_parts();
                let handle = PeerbusPendingReq {
                    server: arc.clone(),
                    reply: Some(reply),
                    request: owned_message(req.header().kind, req.payload()),
                };
                unsafe { *out_pending = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_pending = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_req_request(
    pending: *const PeerbusPendingReq,
) -> *const PeerbusMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_req_reply(
    pending: *mut PeerbusPendingReq,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending req handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(reply) = pending.reply.take() else {
        set_last_error("pending req already replied");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).respond_pending(reply, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_req_free(pending: *mut PeerbusPendingReq) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

// ---- message-list accessors ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_len(messages: *const PeerbusMessages) -> usize {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }.messages.len()
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_kind_at(messages: *const PeerbusMessages, index: usize) -> u64 {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map(|msg| msg.kind)
        .unwrap_or(0)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_data_at(
    messages: *const PeerbusMessages,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if messages.is_null() {
        return PeerbusBytes::empty();
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map(|msg| PeerbusBytes {
            ptr: msg.data.as_ptr(),
            len: msg.data.len(),
        })
        .unwrap_or_else(PeerbusBytes::empty)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_free(messages: *mut PeerbusMessages) {
    ffi_guard((), move || {
    if messages.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(messages)) };
})
}

// Preferred que/ans answer-list aliases. These wrap `PeerbusMessages` so old
// `messages` accessors and new `answers` accessors remain ownership-compatible.

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_len(answers: *const PeerbusAnswers) -> usize {
    ffi_guard(0, move || {
    peerbus_messages_len(answers)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_kind_at(answers: *const PeerbusAnswers, index: usize) -> u64 {
    ffi_guard(0, move || {
    peerbus_messages_kind_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_data_at(
    answers: *const PeerbusAnswers,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    peerbus_messages_data_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_free(answers: *mut PeerbusAnswers) {
    ffi_guard((), move || {
    peerbus_messages_free(answers);
})
}

// ---- que/ans ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusQueClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .que_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .que_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusQueClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_que_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusQueClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.que_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(client) => Box::into_raw(Box::new(PeerbusQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_free(client: *mut PeerbusQueClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_stats(client: *const PeerbusQueClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_send(
    client: *mut PeerbusQueClient,
    kind: u64,
    data: *const u8,
    len: usize,
    out_messages: *mut *mut PeerbusMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null que client or out pointer");
        return false;
    }
    let client = unsafe { &mut *client };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    let mut answers = match client.client.send(&RawMsg::new(kind, bytes)) {
        Ok(answers) => answers,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    let mut messages = Vec::new();
    loop {
        match answers.next() {
            Ok(Some(ans)) => messages.push(owned_message(ans.header().kind, ans.payload())),
            Ok(None) => break,
            Err(e) => {
                set_last_error(e.to_string());
                return false;
            }
        }
    }
    unsafe { *out_messages = Box::into_raw(Box::new(PeerbusMessages { messages })) };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusAnsServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.ans_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusAnsServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusAnsServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_ans_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_free(server: *mut PeerbusAnsServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_stats(server: *const PeerbusAnsServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_responder_send(
    responder: *mut PeerbusAnsResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    ffi_guard((), move || {
    if responder.is_null() {
        return;
    }
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return,
    };
    unsafe { &mut *responder }
        .messages
        .push(RawMsg::new(kind, bytes));
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_serve_one(
    server: *mut PeerbusAnsServer,
    timeout_ms: u64,
    handler: PeerbusAnsHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null ans server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ans handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some((que, mut ans))) => {
                let mut responder = PeerbusAnsResponder {
                    messages: Vec::new(),
                };
                unsafe {
                    handler(
                        ctx,
                        que.header().kind,
                        que.payload().as_ptr(),
                        que.payload().len(),
                        &mut responder,
                    )
                };
                for msg in responder.messages {
                    if let Err(e) = ans.send(&msg) {
                        set_last_error(e.to_string());
                        return -1;
                    }
                }
                return match ans.finish() {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_take(
    server: *mut PeerbusAnsServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingQue,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null ans server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (que, answers) = pending.into_parts();
                let handle = PeerbusPendingQue {
                    server: arc.clone(),
                    answers: Some(answers),
                    request: owned_message(que.header().kind, que.payload()),
                };
                unsafe { *out_pending = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_pending = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_que_request(
    pending: *const PeerbusPendingQue,
) -> *const PeerbusMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_que_send(
    pending: *mut PeerbusPendingQue,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers else {
        set_last_error("pending que already finished");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).send_pending(answers, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_que_finish(pending: *mut PeerbusPendingQue) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers.take() else {
        return true;
    };
    match lock_inner(&pending.server).finish_pending(answers) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_que_free(pending: *mut PeerbusPendingQue) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

// ---- put/ack ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPutClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .put_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .put_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusPutClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusPutClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_put_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPutClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.put_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusPutClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_free(client: *mut PeerbusPutClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_stats(client: *const PeerbusPutClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*client }.client).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_upload(
    client: *mut PeerbusPutClient,
    items: *const PeerbusRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null put client or out pointer");
        return false;
    }
    let raw_items = match unsafe { raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &*client };
    let mut guard = lock_inner(&client.client);
    let mut sender = match guard.open() {
        Ok(sender) => sender,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    for raw in raw_items {
        if let Err(e) = sender.send(&message_from_raw(*raw)) {
            set_last_error(e.to_string());
            return false;
        }
    }
    match sender.finish() {
        Ok(ack) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(ack.header().kind, ack.payload())));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_put(
    client: *mut PeerbusPutClient,
    items: *const PeerbusRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_client_upload(client, items, len, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_open(
    client: *mut PeerbusPutClient,
    out_upload: *mut *mut PeerbusPutUpload,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_upload.is_null() {
        set_last_error("null put client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_upload() {
        Ok(token) => {
            unsafe {
                *out_upload = Box::into_raw(Box::new(PeerbusPutUpload {
                    client: client_ref.client.clone(),
                    token: Some(token),
                }));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_open_sender(
    client: *mut PeerbusPutClient,
    out_sender: *mut *mut PeerbusPutSender,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_client_open(client, out_sender)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_upload_send(
    upload: *mut PeerbusPutUpload,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() {
        set_last_error("null put upload handle");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token else {
        set_last_error("put upload already finished");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&upload.client).send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_sender_send(
    sender: *mut PeerbusPutSender,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_upload_send(sender, kind, data, len)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_upload_finish(
    upload: *mut PeerbusPutUpload,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() || out_message.is_null() {
        set_last_error("null put upload or out pointer");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token.take() else {
        set_last_error("put upload already finished");
        return false;
    };
    match lock_inner(&upload.client).finish_pending(token) {
        Ok(ack) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(ack.header().kind, ack.payload())));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_sender_finish(
    sender: *mut PeerbusPutSender,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_upload_finish(sender, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_upload_free(upload: *mut PeerbusPutUpload) {
    ffi_guard((), move || {
    if upload.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(upload)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_sender_free(sender: *mut PeerbusPutSender) {
    ffi_guard((), move || {
    peerbus_put_upload_free(sender);
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusAckServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.ack_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusAckServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusAckServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_ack_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_free(server: *mut PeerbusAckServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_stats(server: *const PeerbusAckServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_serve_one(
    server: *mut PeerbusAckServer,
    timeout_ms: u64,
    handler: PeerbusAckHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null ack server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ack handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some(mut puts)) => {
                let mut values = Vec::new();
                loop {
                    match puts.next() {
                        Ok(Some(put)) => values.push(RawMsg::new(put.header().kind, put.payload())),
                        Ok(None) => break,
                        Err(e) => {
                            set_last_error(e.to_string());
                            return -1;
                        }
                    }
                }
                let messages = messages_from_raw(values);
                let mut responder = PeerbusResponder {
                    kind: 0,
                    data: Vec::new(),
                    set: false,
                };
                unsafe { handler(ctx, &messages, &mut responder) };
                let ack = RawMsg::new(responder.kind, &responder.data);
                return match puts.ack(&ack) {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_take(
    server: *mut PeerbusAckServer,
    timeout_ms: u64,
    out_puts: *mut *mut PeerbusPuts,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_puts.is_null() {
        set_last_error("null ack server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_req_id, first, done, token) = pending.into_parts();
                let handle = PeerbusPuts {
                    server: arc.clone(),
                    token: Some(token),
                    first: first.map(|msg| owned_message(msg.header().kind, msg.payload())),
                    done,
                };
                unsafe { *out_puts = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_puts = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_puts_next(
    puts: *mut PeerbusPuts,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if puts.is_null() || out_message.is_null() {
        set_last_error("null puts or out pointer");
        return -1;
    }
    let puts = unsafe { &mut *puts };
    if let Some(first) = puts.first.take() {
        unsafe { *out_message = Box::into_raw(Box::new(first)) };
        return 1;
    }
    if puts.done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = puts.token else {
        set_last_error("puts handle already acked or closed");
        return -1;
    };
    let result = lock_inner(&puts.server).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(msg.header().kind, msg.payload())));
            }
            1
        }
        Ok(None) => {
            puts.done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_puts_ack(
    puts: *mut PeerbusPuts,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if puts.is_null() {
        set_last_error("null puts handle");
        return false;
    }
    let puts = unsafe { &mut *puts };
    let Some(token) = puts.token.take() else {
        set_last_error("puts handle already acked or closed");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&puts.server).ack_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_puts_free(puts: *mut PeerbusPuts) {
    ffi_guard((), move || {
    if puts.is_null() {
        return;
    }
    let puts_ref = unsafe { &mut *puts };
    if let Some(token) = puts_ref.token.take() {
        lock_inner(&puts_ref.server).close_pending(token);
    }
    unsafe { drop(Box::from_raw(puts)) };
})
}

// ---- pip ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPipClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .pip_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .pip_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusPipClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusPipClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_pip_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPipClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.pip_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusPipClient {
                client: Arc::new(Mutex::new(client)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_free(client: *mut PeerbusPipClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_stats(client: *const PeerbusPipClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*client }.client).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_exchange(
    client: *mut PeerbusPipClient,
    items: *const PeerbusRawMessage,
    len: usize,
    out_messages: *mut *mut PeerbusMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null pip client or out pointer");
        return false;
    }
    let raw_items = match unsafe { raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &*client };
    let mut guard = lock_inner(&client.client);
    let mut pip = match guard.open() {
        Ok(pip) => pip,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    for raw in raw_items {
        if let Err(e) = pip.send(&message_from_raw(*raw)) {
            set_last_error(e.to_string());
            return false;
        }
    }
    if let Err(e) = pip.finish_send() {
        set_last_error(e.to_string());
        return false;
    }
    let mut messages = Vec::new();
    loop {
        match pip.next() {
            Ok(Some(msg)) => messages.push(owned_message(msg.header().kind, msg.payload())),
            Ok(None) => break,
            Err(e) => {
                set_last_error(e.to_string());
                return false;
            }
        }
    }
    unsafe { *out_messages = Box::into_raw(Box::new(PeerbusMessages { messages })) };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_open(
    client: *mut PeerbusPipClient,
    out_pip: *mut *mut PeerbusPip,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_pip.is_null() {
        set_last_error("null pip client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_session() {
        Ok(token) => {
            unsafe {
                *out_pip = Box::into_raw(Box::new(PeerbusPip {
                    client: client_ref.client.clone(),
                    token: Some(token),
                    incoming_done: false,
                    outgoing_done: false,
                }));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_send(
    pip: *mut PeerbusPip,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("pip session is closed");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.client).send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_finish_send(pip: *mut PeerbusPip) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("pip session is closed");
        return false;
    };
    let result = lock_inner(&pip.client).finish_send_pending(token);
    match result {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_next(
    pip: *mut PeerbusPip,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null pip handle or out pointer");
        return -1;
    }
    let pip = unsafe { &mut *pip };
    if pip.incoming_done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = pip.token else {
        set_last_error("pip session is closed");
        return -1;
    };
    let result = lock_inner(&pip.client).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(msg.header().kind, msg.payload())));
            }
            1
        }
        Ok(None) => {
            pip.incoming_done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_close(pip: *mut PeerbusPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take() {
        lock_inner(&pip_ref.client).close_session(token);
    }
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_free(pip: *mut PeerbusPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPipServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .pip_server_with_qos::<RawMsg, RawMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusPipServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusPipServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_pip_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_free(server: *mut PeerbusPipServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_stats(server: *const PeerbusPipServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_responder_send(
    responder: *mut PeerbusMessageResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    ffi_guard((), move || {
    if responder.is_null() {
        return;
    }
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return,
    };
    unsafe { &mut *responder }
        .messages
        .push(RawMsg::new(kind, bytes));
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_serve_one(
    server: *mut PeerbusPipServer,
    timeout_ms: u64,
    handler: PeerbusPipHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null pip server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null pip handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some(mut pip)) => {
                let mut values = Vec::new();
                loop {
                    match pip.next() {
                        Ok(Some(msg)) => values.push(RawMsg::new(msg.header().kind, msg.payload())),
                        Ok(None) => break,
                        Err(e) => {
                            set_last_error(e.to_string());
                            return -1;
                        }
                    }
                }
                let messages = messages_from_raw(values);
                let mut responder = PeerbusMessageResponder {
                    messages: Vec::new(),
                };
                unsafe { handler(ctx, &messages, &mut responder) };
                for msg in responder.messages {
                    if let Err(e) = pip.send(&msg) {
                        set_last_error(e.to_string());
                        return -1;
                    }
                }
                return match pip.finish_send() {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_take(
    server: *mut PeerbusPipServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingPip,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null pip server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_session_id, first, incoming_done, token) = pending.into_parts();
                let handle = PeerbusPendingPip {
                    server: arc.clone(),
                    token: Some(token),
                    first: first.map(|msg| owned_message(msg.header().kind, msg.payload())),
                    incoming_done,
                    outgoing_done: false,
                };
                unsafe { *out_pending = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_pending = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_next(
    pip: *mut PeerbusPendingPip,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null pending pip or out pointer");
        return -1;
    }
    let pip = unsafe { &mut *pip };
    if let Some(first) = pip.first.take() {
        unsafe { *out_message = Box::into_raw(Box::new(first)) };
        return 1;
    }
    if pip.incoming_done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = pip.token else {
        set_last_error("pending pip is closed");
        return -1;
    };
    let result = lock_inner(&pip.server).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(msg.header().kind, msg.payload())));
            }
            1
        }
        Ok(None) => {
            pip.incoming_done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_send(
    pip: *mut PeerbusPendingPip,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("pending pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("pending pip is closed");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.server).send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_finish_send(pip: *mut PeerbusPendingPip) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("pending pip is closed");
        return false;
    };
    let result = lock_inner(&pip.server).finish_send_pending(token);
    match result {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_close(pip: *mut PeerbusPendingPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take() {
        lock_inner(&pip_ref.server).close_pending(token);
    }
    pip_ref.first = None;
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_free(pip: *mut PeerbusPendingPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_pending_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

// ---- datapod pub/sub parity with the raw family ----

/// No-QoS datapod publisher, mirroring [`peerbus_publisher_new`].
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusDatapodPublisher {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_datapod_publisher_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_stats(
    publisher: *const PeerbusDatapodPublisher,
) -> PeerbusPublisherStats {
    ffi_guard(PeerbusPublisherStats::default(), move || {
    if publisher.is_null() {
        return PeerbusPublisherStats::default();
    }
    let stats = unsafe { &*publisher }.publisher.stats();
    PeerbusPublisherStats {
        published: stats.published,
        remote_dropped: stats.remote_dropped,
        stale_dropped: stats.stale_dropped,
        bytes_sent: stats.bytes_sent,
        send_errors: stats.send_errors,
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_stats(
    subscriber: *const PeerbusDatapodSubscriber,
) -> PeerbusSubscriberStats {
    ffi_guard(PeerbusSubscriberStats::default(), move || {
    if subscriber.is_null() {
        return PeerbusSubscriberStats::default();
    }
    let stats = unsafe { &*subscriber }.subscriber.stats();
    PeerbusSubscriberStats {
        received: stats.received,
        disconnects: stats.disconnects,
        stale_dropped: stats.stale_dropped,
        incomplete_dropped: stats.incomplete_dropped,
        bytes_received: stats.bytes_received,
    }
})
}

/// Poll for the next datapod sample as an owned message, mirroring the raw
/// [`peerbus_subscriber_take`]. Returns `1` and writes an owned message to
/// `*out_message` when one is available, `0` when none is ready, and `-1`
/// on error. Free the message with [`peerbus_datapod_message_free`].
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_take(
    subscriber: *mut PeerbusDatapodSubscriber,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_message.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            let msg = owned_datapod_message(sample.header().type_hash, sample.payload());
            unsafe { *out_message = Box::into_raw(Box::new(msg)) };
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

// ---- datapod server serve_one parity with the raw family ----

/// Datapod req/res `serve_one`, mirroring [`peerbus_req_server_serve_one`].
/// The handler's `kind` argument carries the request `type_hash`, and the
/// responder's `kind` becomes the response `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_serve_one(
    server: *mut PeerbusDatapodReqServer,
    timeout_ms: u64,
    handler: PeerbusReqHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod req server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null request handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some((req, reply))) => {
                let type_hash = req.header().type_hash;
                let payload = req.payload();
                let mut responder = PeerbusResponder {
                    kind: 0,
                    data: Vec::new(),
                    set: false,
                };
                unsafe {
                    handler(
                        ctx,
                        type_hash,
                        payload.as_ptr(),
                        payload.len(),
                        &mut responder as *mut PeerbusResponder,
                    )
                };
                let response = DatapodMsg::new(responder.kind, &*responder.data);
                return match reply.respond(&response) {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

/// Datapod que/ans `serve_one`, mirroring [`peerbus_ans_server_serve_one`].
/// Answer items pushed via [`peerbus_ans_responder_send`] use their `kind`
/// argument as the answer `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ans_server_serve_one(
    server: *mut PeerbusDatapodAnsServer,
    timeout_ms: u64,
    handler: PeerbusAnsHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod ans server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ans handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some((que, mut ans))) => {
                let mut responder = PeerbusAnsResponder {
                    messages: Vec::new(),
                };
                unsafe {
                    handler(
                        ctx,
                        que.header().type_hash,
                        que.payload().as_ptr(),
                        que.payload().len(),
                        &mut responder,
                    )
                };
                for msg in responder.messages {
                    if let Err(e) = ans.send(&DatapodMsg::new(msg.kind, &*msg.data)) {
                        set_last_error(e.to_string());
                        return -1;
                    }
                }
                return match ans.finish() {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

/// Datapod put/ack `serve_one`, mirroring [`peerbus_ack_server_serve_one`].
/// Uploaded items are exposed via `items` with each element's `kind`
/// carrying the `type_hash`; the responder's `kind` becomes the ack
/// `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_serve_one(
    server: *mut PeerbusDatapodAckServer,
    timeout_ms: u64,
    handler: PeerbusAckHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod ack server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ack handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some(mut puts)) => {
                let mut values = Vec::new();
                loop {
                    match puts.next() {
                        Ok(Some(put)) => {
                            values.push(RawMsg::new(put.header().type_hash, put.payload()))
                        }
                        Ok(None) => break,
                        Err(e) => {
                            set_last_error(e.to_string());
                            return -1;
                        }
                    }
                }
                let messages = messages_from_raw(values);
                let mut responder = PeerbusResponder {
                    kind: 0,
                    data: Vec::new(),
                    set: false,
                };
                unsafe { handler(ctx, &messages, &mut responder) };
                let ack = DatapodMsg::new(responder.kind, &*responder.data);
                return match puts.ack(&ack) {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

/// Datapod pip `serve_one`, mirroring [`peerbus_pip_server_serve_one`].
/// Incoming items are exposed via `items` with each element's `kind`
/// carrying the `type_hash`; reply items pushed via
/// [`peerbus_message_responder_send`] use their `kind` as the `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_serve_one(
    server: *mut PeerbusDatapodPipServer,
    timeout_ms: u64,
    handler: PeerbusPipHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod pip server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null pip handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some(mut pip)) => {
                let mut values = Vec::new();
                loop {
                    match pip.next() {
                        Ok(Some(msg)) => {
                            values.push(RawMsg::new(msg.header().type_hash, msg.payload()))
                        }
                        Ok(None) => break,
                        Err(e) => {
                            set_last_error(e.to_string());
                            return -1;
                        }
                    }
                }
                let messages = messages_from_raw(values);
                let mut responder = PeerbusMessageResponder {
                    messages: Vec::new(),
                };
                unsafe { handler(ctx, &messages, &mut responder) };
                for msg in responder.messages {
                    if let Err(e) = pip.send(&DatapodMsg::new(msg.kind, &*msg.data)) {
                        set_last_error(e.to_string());
                        return -1;
                    }
                }
                return match pip.finish_send() {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}
