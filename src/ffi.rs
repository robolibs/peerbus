//! C ABI for quicbit.
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
//!   stashed in a thread-local and read via [`quicbit_last_error_message`].
//! * Returned byte views ([`QuicbitBytes`]) borrow memory owned by the
//!   handle they came from; copy out before freeing the handle.
//!
//! See `include/quicbit.h` for the C declarations.

// These `extern "C"` functions take raw pointers from C and dereference
// them by design; the safety contract lives in the C header, not in a
// Rust `unsafe fn` signature. The lint is noise for a C ABI.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
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
            CString::new(message).unwrap_or_else(|_| CString::new("quicbit ffi error").unwrap()),
        );
    });
}

/// Returns the last error message on this thread, or NULL if the most
/// recent call succeeded. The pointer is valid until the next quicbit
/// call on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_last_error_message() -> *const c_char {
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
pub struct QuicbitBytes {
    pub ptr: *const u8,
    pub len: usize,
}

impl QuicbitBytes {
    fn empty() -> Self {
        Self {
            ptr: ptr::null(),
            len: 0,
        }
    }
}

// ---- handles ----

pub struct QuicbitNode {
    node: Node,
}

/// Optional node construction settings. NULL string pointers mean "unset";
/// zero numeric limits mean "use the Rust default".
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct QuicbitNodeConfig {
    pub identity: *const c_char,
    pub no_relay: bool,
    pub system_did: *const c_char,
    pub max_payload_bytes: usize,
    pub history_depth: u32,
    pub subscriber_buffer: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct QuicbitNodeStats {
    pub publisher_topics: usize,
    pub cached_peers: usize,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct QuicbitPublisherStats {
    pub published: u64,
    pub remote_dropped: u64,
    pub stale_dropped: u64,
    pub bytes_sent: u64,
    pub send_errors: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct QuicbitSubscriberStats {
    pub received: u64,
    pub disconnects: u64,
    pub stale_dropped: u64,
    pub incomplete_dropped: u64,
    pub bytes_received: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct QuicbitItemStats {
    pub messages_out: u64,
    pub messages_in: u64,
    pub bytes_out: u64,
    pub bytes_in: u64,
    pub errors: u64,
}

pub struct QuicbitPublisher {
    publisher: Publisher<RawMsg>,
}

pub struct QuicbitSubscriber {
    subscriber: Subscriber<RawMsg>,
}

pub struct QuicbitSample {
    sample: NodeSample<RawMsg>,
}

pub struct QuicbitDatapodPublisher {
    publisher: Publisher<DatapodMsg>,
}

pub struct QuicbitDatapodSubscriber {
    subscriber: Subscriber<DatapodMsg>,
}

pub struct QuicbitDatapodSample {
    sample: NodeSample<DatapodMsg>,
}

/// An owned, received message (kind tag + payload bytes).
pub struct QuicbitMessage {
    kind: u64,
    data: Vec<u8>,
}

pub struct QuicbitReqClient {
    client: ReqClient<RawMsg, RawMsg>,
}

pub struct QuicbitReqServer {
    server: ReqServer<RawMsg, RawMsg>,
}

pub struct QuicbitQueClient {
    client: QueClient<RawMsg, RawMsg>,
}

pub struct QuicbitAnsServer {
    server: AnsServer<RawMsg, RawMsg>,
}

pub struct QuicbitPutClient {
    client: PutClient<RawMsg, RawMsg>,
}

pub struct QuicbitAckServer {
    server: AckServer<RawMsg, RawMsg>,
}

pub struct QuicbitPuts {
    server: *mut QuicbitAckServer,
    token: Option<PutAckToken>,
    first: Option<QuicbitMessage>,
    done: bool,
}

pub struct QuicbitPipClient {
    client: PipClient<RawMsg, RawMsg>,
}

pub struct QuicbitPipServer {
    server: PipServer<RawMsg, RawMsg>,
}

pub struct QuicbitPendingReq {
    server: *mut QuicbitReqServer,
    reply: Option<ReqReplyToken>,
    request: QuicbitMessage,
}

pub struct QuicbitPendingQue {
    server: *mut QuicbitAnsServer,
    answers: Option<AnsReplyToken>,
    request: QuicbitMessage,
}

pub struct QuicbitPutUpload {
    client: *mut QuicbitPutClient,
    token: Option<PutUploadToken>,
}

pub struct QuicbitPip {
    client: *mut QuicbitPipClient,
    token: Option<PipSessionToken>,
    incoming_done: bool,
    outgoing_done: bool,
}

pub struct QuicbitPendingPip {
    server: *mut QuicbitPipServer,
    token: Option<PipServerToken>,
    first: Option<QuicbitMessage>,
    incoming_done: bool,
    outgoing_done: bool,
}

/// Passed to a request handler so it can set the response.
pub struct QuicbitResponder {
    kind: u64,
    data: Vec<u8>,
    set: bool,
}

/// Request handler callback: receives the request `kind` + bytes and the
/// `responder` to fill via [`quicbit_responder_set`].
pub type QuicbitReqHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        kind: u64,
        data: *const u8,
        len: usize,
        responder: *mut QuicbitResponder,
    ),
>;

/// Delivery behavior for C QoS.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum QuicbitDeliveryPolicy {
    Reliable = 0,
    Latest = 1,
    BestEffort = 2,
}

/// C mirror of [`TopicQos`]. Zero fields are allowed; use
/// [`quicbit_topic_qos_reliable`], [`quicbit_topic_qos_latest`], or
/// [`quicbit_topic_qos_best_effort`] for canonical defaults.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct QuicbitTopicQos {
    pub delivery: QuicbitDeliveryPolicy,
    pub max_message_bytes: usize,
    pub max_inflight_bytes: usize,
    pub chunk_bytes: usize,
    pub subscriber_queue: usize,
    pub priority: u8,
}

impl From<QuicbitDeliveryPolicy> for DeliveryPolicy {
    fn from(value: QuicbitDeliveryPolicy) -> Self {
        match value {
            QuicbitDeliveryPolicy::Reliable => DeliveryPolicy::Reliable,
            QuicbitDeliveryPolicy::Latest => DeliveryPolicy::Latest,
            QuicbitDeliveryPolicy::BestEffort => DeliveryPolicy::BestEffort,
        }
    }
}

impl From<TopicQos> for QuicbitTopicQos {
    fn from(value: TopicQos) -> Self {
        let delivery = match value.delivery {
            DeliveryPolicy::Reliable => QuicbitDeliveryPolicy::Reliable,
            DeliveryPolicy::Latest => QuicbitDeliveryPolicy::Latest,
            DeliveryPolicy::BestEffort => QuicbitDeliveryPolicy::BestEffort,
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

impl From<QuicbitTopicQos> for TopicQos {
    fn from(value: QuicbitTopicQos) -> Self {
        Self {
            delivery: value.delivery.into(),
            max_message_bytes: value.max_message_bytes,
            max_inflight_bytes: value.max_inflight_bytes,
            chunk_bytes: value.chunk_bytes,
            subscriber_queue: value.subscriber_queue,
            priority: value.priority,
        }
    }
}

/// Borrowed message input used by finite C convenience APIs.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct QuicbitRawMessage {
    pub kind: u64,
    pub data: QuicbitBytes,
}

/// Owned message list returned by que/ans and pip convenience calls.
pub struct QuicbitMessages {
    messages: Vec<QuicbitMessage>,
}

/// Passed to a que/ans handler so it can append answer items.
pub struct QuicbitAnsResponder {
    messages: Vec<RawMsg>,
}

/// Passed to put/ack or pip handlers so they can build the final reply list.
pub struct QuicbitMessageResponder {
    messages: Vec<RawMsg>,
}

pub type QuicbitAnsHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        kind: u64,
        data: *const u8,
        len: usize,
        responder: *mut QuicbitAnsResponder,
    ),
>;

pub type QuicbitAckHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        items: *const QuicbitMessages,
        responder: *mut QuicbitResponder,
    ),
>;

pub type QuicbitPipHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        items: *const QuicbitMessages,
        responder: *mut QuicbitMessageResponder,
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

unsafe fn bytes_in<'a>(data: *const u8, len: usize) -> &'a [u8] {
    if data.is_null() || len == 0 {
        &[]
    } else {
        // SAFETY: caller promises `len` valid bytes at `data`.
        unsafe { std::slice::from_raw_parts(data, len) }
    }
}

unsafe fn raw_messages_in<'a>(
    items: *const QuicbitRawMessage,
    len: usize,
) -> Result<&'a [QuicbitRawMessage], ()> {
    if items.is_null() {
        if len == 0 {
            Ok(&[])
        } else {
            set_last_error("null message array with non-zero length");
            Err(())
        }
    } else {
        // SAFETY: caller promises `len` valid QuicbitRawMessage values.
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

fn item_stats_out(stats: crate::ItemStats) -> QuicbitItemStats {
    QuicbitItemStats {
        messages_out: stats.messages_out,
        messages_in: stats.messages_in,
        bytes_out: stats.bytes_out,
        bytes_in: stats.bytes_in,
        errors: stats.errors,
    }
}

fn message_from_raw(raw: QuicbitRawMessage) -> RawMsg {
    let bytes = unsafe { bytes_in(raw.data.ptr, raw.data.len) };
    RawMsg::new(raw.kind, bytes)
}

fn owned_message(kind: u64, payload: &[u8]) -> QuicbitMessage {
    QuicbitMessage {
        kind,
        data: payload.to_vec(),
    }
}

fn messages_from_raw(values: Vec<RawMsg>) -> QuicbitMessages {
    QuicbitMessages {
        messages: values
            .into_iter()
            .map(|msg| owned_message(msg.kind, &msg.data))
            .collect(),
    }
}

fn build_node_from_config(cfg: QuicbitNodeConfig) -> Result<Node, ()> {
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
    if cfg.max_payload_bytes != 0 || cfg.history_depth != 0 || cfg.subscriber_buffer != 0 {
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
        builder = builder.local_config(local_cfg);
    }
    builder.bind().map_err(|e| {
        set_last_error(e.to_string());
    })
}

// ---- node ----

/// Create a node. `identity` may be NULL for an ephemeral key. Returns
/// NULL on failure (see [`quicbit_last_error_message`]).
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_new(identity: *const c_char, no_relay: bool) -> *mut QuicbitNode {
    clear_last_error();
    let cfg = QuicbitNodeConfig {
        identity,
        no_relay,
        system_did: ptr::null(),
        max_payload_bytes: 0,
        history_depth: 0,
        subscriber_buffer: 0,
    };
    match build_node_from_config(cfg) {
        Ok(node) => Box::into_raw(Box::new(QuicbitNode { node })),
        Err(()) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_config_default() -> QuicbitNodeConfig {
    QuicbitNodeConfig {
        identity: ptr::null(),
        no_relay: false,
        system_did: ptr::null(),
        max_payload_bytes: 0,
        history_depth: 0,
        subscriber_buffer: 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_new_with_config(cfg: QuicbitNodeConfig) -> *mut QuicbitNode {
    clear_last_error();
    match build_node_from_config(cfg) {
        Ok(node) => Box::into_raw(Box::new(QuicbitNode { node })),
        Err(()) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_free(node: *mut QuicbitNode) {
    if node.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw in quicbit_node_new.
    unsafe { drop(Box::from_raw(node)) };
}

/// This node's identity as a `did:key:z6Mk…` string. Caller owns the
/// returned C string and must free it with [`quicbit_string_free`].
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_did_key(node: *const QuicbitNode) -> *mut c_char {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_endpoint_addr(node: *const QuicbitNode) -> *mut c_char {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_add_topic_route(
    node: *const QuicbitNode,
    topic: *const c_char,
    endpoint_addr: *const c_char,
) -> bool {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_add_system_peer(
    node: *const QuicbitNode,
    endpoint_addr: *const c_char,
) -> bool {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_stats(node: *const QuicbitNode) -> QuicbitNodeStats {
    if node.is_null() {
        return QuicbitNodeStats::default();
    }
    let stats = unsafe { &*node }.node.stats();
    QuicbitNodeStats {
        publisher_topics: stats.publisher_topics,
        cached_peers: stats.cached_peers,
    }
}

/// Free a string returned by quicbit (e.g. [`quicbit_node_did_key`]).
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    // SAFETY: originated from CString::into_raw.
    unsafe { drop(CString::from_raw(s)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_topic_qos_reliable() -> QuicbitTopicQos {
    TopicQos::reliable().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_topic_qos_latest() -> QuicbitTopicQos {
    TopicQos::latest().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_topic_qos_best_effort() -> QuicbitTopicQos {
    TopicQos::best_effort().into()
}

// ---- pub/sub ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_publisher_new(
    node: *const QuicbitNode,
    topic: *const c_char,
) -> *mut QuicbitPublisher {
    quicbit_publisher_new_with_qos(node, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_publisher_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitPublisher {
    clear_last_error();
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
    match node.node.publisher_with_qos::<RawMsg>(topic, qos.into()) {
        Ok(publisher) => Box::into_raw(Box::new(QuicbitPublisher { publisher })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_publisher_free(publisher: *mut QuicbitPublisher) {
    if publisher.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(publisher)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_publisher_stats(
    publisher: *const QuicbitPublisher,
) -> QuicbitPublisherStats {
    if publisher.is_null() {
        return QuicbitPublisherStats::default();
    }
    let stats = unsafe { &*publisher }.publisher.stats();
    QuicbitPublisherStats {
        published: stats.published,
        remote_dropped: stats.remote_dropped,
        stale_dropped: stats.stale_dropped,
        bytes_sent: stats.bytes_sent,
        send_errors: stats.send_errors,
    }
}

/// Publish `data` (`len` bytes) with user tag `kind`. Returns false on
/// failure.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_publisher_send(
    publisher: *mut QuicbitPublisher,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    clear_last_error();
    if publisher.is_null() {
        set_last_error("null publisher handle");
        return false;
    }
    // SAFETY: validated non-null.
    let publisher = unsafe { &mut *publisher };
    let bytes = unsafe { bytes_in(data, len) };
    match publisher.publisher.send(&RawMsg::new(kind, bytes)) {
        Ok(_) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_subscriber_new(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut QuicbitSubscriber {
    quicbit_subscriber_new_with_qos(node, peer, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_subscriber_new_with_qos(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitSubscriber {
    clear_last_error();
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
    let qos = qos.into();
    let result = match peer {
        Ok(addr) => node.node.subscriber_with_qos::<RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .subscriber_with_qos::<RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(subscriber) => Box::into_raw(Box::new(QuicbitSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_subscribe_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitSubscriber {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.subscribe_with_qos::<RawMsg>(topic, qos.into()) {
        Ok(subscriber) => Box::into_raw(Box::new(QuicbitSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_subscriber_free(subscriber: *mut QuicbitSubscriber) {
    if subscriber.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(subscriber)) };
}

// ---- generic datapod pub/sub ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_publisher_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitDatapodPublisher {
    clear_last_error();
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
        .publisher_with_qos::<DatapodMsg>(topic, qos.into())
    {
        Ok(publisher) => Box::into_raw(Box::new(QuicbitDatapodPublisher { publisher })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_publisher_free(publisher: *mut QuicbitDatapodPublisher) {
    if publisher.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(publisher)) };
}

/// Publish a datapod wire message: `type_hash` plus `header || payload` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_publisher_send(
    publisher: *mut QuicbitDatapodPublisher,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    clear_last_error();
    if publisher.is_null() {
        set_last_error("null publisher handle");
        return false;
    }
    let publisher = unsafe { &mut *publisher };
    let wire = unsafe { bytes_in(wire, len) };
    match publisher.publisher.send(&DatapodMsg::new(type_hash, wire)) {
        Ok(_) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_subscriber_new_with_qos(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitDatapodSubscriber {
    clear_last_error();
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
    let qos = qos.into();
    let result = match peer {
        Ok(addr) => node
            .node
            .subscriber_with_qos::<DatapodMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .subscriber_with_qos::<DatapodMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(subscriber) => Box::into_raw(Box::new(QuicbitDatapodSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_subscriber_free(subscriber: *mut QuicbitDatapodSubscriber) {
    if subscriber.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(subscriber)) };
}

/// Poll for a datapod sample without copying the wire bytes.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_subscriber_take_sample(
    subscriber: *mut QuicbitDatapodSubscriber,
    out_sample: *mut *mut QuicbitDatapodSample,
) -> i32 {
    clear_last_error();
    if subscriber.is_null() || out_sample.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            unsafe { *out_sample = Box::into_raw(Box::new(QuicbitDatapodSample { sample })) };
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_sample_type_hash(sample: *const QuicbitDatapodSample) -> u64 {
    if sample.is_null() {
        return 0;
    }
    unsafe { &*sample }.sample.header().type_hash
}

/// Borrowed zero-copy view of datapod `header || payload` wire bytes.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_sample_wire(sample: *const QuicbitDatapodSample) -> QuicbitBytes {
    if sample.is_null() {
        return QuicbitBytes::empty();
    }
    let sample = unsafe { &*sample };
    QuicbitBytes {
        ptr: sample.sample.payload().as_ptr(),
        len: sample.sample.payload().len(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_datapod_sample_free(sample: *mut QuicbitDatapodSample) {
    if sample.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(sample)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_subscriber_stats(
    subscriber: *const QuicbitSubscriber,
) -> QuicbitSubscriberStats {
    if subscriber.is_null() {
        return QuicbitSubscriberStats::default();
    }
    let stats = unsafe { &*subscriber }.subscriber.stats();
    QuicbitSubscriberStats {
        received: stats.received,
        disconnects: stats.disconnects,
        stale_dropped: stats.stale_dropped,
        incomplete_dropped: stats.incomplete_dropped,
        bytes_received: stats.bytes_received,
    }
}

/// Poll for the next sample. Returns `1` and writes an owned message to
/// `*out_message` when one is available, `0` when none is ready, and
/// `-1` on error. A returned message must be freed with
/// [`quicbit_message_free`].
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_subscriber_take(
    subscriber: *mut QuicbitSubscriber,
    out_message: *mut *mut QuicbitMessage,
) -> i32 {
    clear_last_error();
    if subscriber.is_null() || out_message.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    // SAFETY: validated non-null.
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            let msg = QuicbitMessage {
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
}

/// Poll for the next sample without copying payload bytes.
///
/// Returns `1` and writes a borrowed sample handle to `*out_sample` when one is
/// available, `0` when none is ready, and `-1` on error. A returned sample must
/// be freed with [`quicbit_sample_free`]. The byte view returned from
/// [`quicbit_sample_data`] is valid until that free call.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_subscriber_take_sample(
    subscriber: *mut QuicbitSubscriber,
    out_sample: *mut *mut QuicbitSample,
) -> i32 {
    clear_last_error();
    if subscriber.is_null() || out_sample.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            unsafe { *out_sample = Box::into_raw(Box::new(QuicbitSample { sample })) };
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_sample_kind(sample: *const QuicbitSample) -> u64 {
    if sample.is_null() {
        return 0;
    }
    unsafe { &*sample }.sample.header().kind
}

/// Borrowed zero-copy view of a sample payload.
///
/// For local SHM this points directly into the shared-memory slot and pins that
/// slot until [`quicbit_sample_free`] is called. Copy it if you need to keep the
/// data longer.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_sample_data(sample: *const QuicbitSample) -> QuicbitBytes {
    if sample.is_null() {
        return QuicbitBytes::empty();
    }
    let sample = unsafe { &*sample };
    QuicbitBytes {
        ptr: sample.sample.payload().as_ptr(),
        len: sample.sample.payload().len(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_sample_free(sample: *mut QuicbitSample) {
    if sample.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(sample)) };
}

// ---- message accessors ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_message_new(
    kind: u64,
    data: *const u8,
    len: usize,
) -> *mut QuicbitMessage {
    let bytes = unsafe { bytes_in(data, len) };
    Box::into_raw(Box::new(owned_message(kind, bytes)))
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_message_kind(message: *const QuicbitMessage) -> u64 {
    if message.is_null() {
        return 0;
    }
    // SAFETY: validated non-null.
    unsafe { (*message).kind }
}

/// Borrowed view of the message payload, valid until the message is
/// freed.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_message_data(message: *const QuicbitMessage) -> QuicbitBytes {
    if message.is_null() {
        return QuicbitBytes::empty();
    }
    // SAFETY: validated non-null.
    let message = unsafe { &*message };
    QuicbitBytes {
        ptr: message.data.as_ptr(),
        len: message.data.len(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_message_free(message: *mut QuicbitMessage) {
    if message.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(message)) };
}

// ---- req/res client ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_client_new(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut QuicbitReqClient {
    quicbit_req_client_new_with_qos(node, peer, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_client_new_with_qos(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitReqClient {
    clear_last_error();
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
    let qos = qos.into();
    let result = match peer {
        Ok(addr) => node
            .node
            .req_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .req_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(QuicbitReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_system_client_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitReqClient {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.req_with_qos::<RawMsg, RawMsg>(topic, qos.into()) {
        Ok(client) => Box::into_raw(Box::new(QuicbitReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_client_free(client: *mut QuicbitReqClient) {
    if client.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(client)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_client_stats(client: *const QuicbitReqClient) -> QuicbitItemStats {
    if client.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
}

/// Send a request and block for the response. Returns false on failure;
/// on success writes an owned response message to `*out_message` (free
/// with [`quicbit_message_free`]).
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_client_call(
    client: *mut QuicbitReqClient,
    kind: u64,
    data: *const u8,
    len: usize,
    out_message: *mut *mut QuicbitMessage,
) -> bool {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null client or out pointer");
        return false;
    }
    // SAFETY: validated non-null.
    let client = unsafe { &mut *client };
    let bytes = unsafe { bytes_in(data, len) };
    match client.client.call(&RawMsg::new(kind, bytes)) {
        Ok(res) => {
            let msg = QuicbitMessage {
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
}

// ---- req/res server ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_server_new(
    node: *const QuicbitNode,
    topic: *const c_char,
) -> *mut QuicbitReqServer {
    quicbit_req_server_new_with_qos(node, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_server_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitReqServer {
    clear_last_error();
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
        .req_server_with_qos::<RawMsg, RawMsg>(topic, qos.into())
    {
        Ok(server) => Box::into_raw(Box::new(QuicbitReqServer { server })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_server_free(server: *mut QuicbitReqServer) {
    if server.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(server)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_server_stats(server: *const QuicbitReqServer) -> QuicbitItemStats {
    if server.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*server }.server.stats())
}

/// Set the response on a responder passed to a request handler. Copies
/// `data` immediately; safe to call once per handler invocation.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_responder_set(
    responder: *mut QuicbitResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    if responder.is_null() {
        return;
    }
    // SAFETY: validated non-null; lives on serve_one's stack.
    let responder = unsafe { &mut *responder };
    let bytes = unsafe { bytes_in(data, len) };
    responder.kind = kind;
    responder.data = bytes.to_vec();
    responder.set = true;
}

/// Serve at most one request, waiting up to `timeout_ms`. Invokes
/// `handler` with the request and a responder; whatever the handler sets
/// (via [`quicbit_responder_set`]) is sent back. Returns `1` if a request
/// was served, `0` on timeout, `-1` on error.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_server_serve_one(
    server: *mut QuicbitReqServer,
    timeout_ms: u64,
    handler: QuicbitReqHandler,
    ctx: *mut c_void,
) -> i32 {
    clear_last_error();
    if server.is_null() {
        set_last_error("null server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null request handler");
        return -1;
    };
    // SAFETY: validated non-null.
    let server = unsafe { &mut *server };
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        match server.server.take() {
            Ok(Some((req, reply))) => {
                let kind = req.header().kind;
                let payload = req.payload();
                let mut responder = QuicbitResponder {
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
                        &mut responder as *mut QuicbitResponder,
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_req_server_take(
    server: *mut QuicbitReqServer,
    timeout_ms: u64,
    out_pending: *mut *mut QuicbitPendingReq,
) -> i32 {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null req server or out pointer");
        return -1;
    }
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let server_ref = unsafe { &mut *server };
        match server_ref.server.take_message() {
            Ok(Some(pending)) => {
                let (req, reply) = pending.into_parts();
                let handle = QuicbitPendingReq {
                    server,
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_req_request(
    pending: *const QuicbitPendingReq,
) -> *const QuicbitMessage {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const QuicbitMessage
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_req_reply(
    pending: *mut QuicbitPendingReq,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
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
    if pending.server.is_null() {
        set_last_error("pending req has null server");
        return false;
    }
    let bytes = unsafe { bytes_in(data, len) };
    let server = unsafe { &mut *pending.server };
    match server
        .server
        .respond_pending(reply, &RawMsg::new(kind, bytes))
    {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_req_free(pending: *mut QuicbitPendingReq) {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
}

// ---- message-list accessors ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_messages_len(messages: *const QuicbitMessages) -> usize {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }.messages.len()
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_messages_kind_at(messages: *const QuicbitMessages, index: usize) -> u64 {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map(|msg| msg.kind)
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_messages_data_at(
    messages: *const QuicbitMessages,
    index: usize,
) -> QuicbitBytes {
    if messages.is_null() {
        return QuicbitBytes::empty();
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map(|msg| QuicbitBytes {
            ptr: msg.data.as_ptr(),
            len: msg.data.len(),
        })
        .unwrap_or_else(QuicbitBytes::empty)
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_messages_free(messages: *mut QuicbitMessages) {
    if messages.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(messages)) };
}

// ---- que/ans ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_que_client_new_with_qos(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitQueClient {
    clear_last_error();
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
    let qos = qos.into();
    let result = match peer {
        Ok(addr) => node
            .node
            .que_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .que_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(QuicbitQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_que_client_new(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut QuicbitQueClient {
    quicbit_que_client_new_with_qos(node, peer, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_que_system_client_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitQueClient {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.que_with_qos::<RawMsg, RawMsg>(topic, qos.into()) {
        Ok(client) => Box::into_raw(Box::new(QuicbitQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_que_client_free(client: *mut QuicbitQueClient) {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_que_client_stats(client: *const QuicbitQueClient) -> QuicbitItemStats {
    if client.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_que_client_send(
    client: *mut QuicbitQueClient,
    kind: u64,
    data: *const u8,
    len: usize,
    out_messages: *mut *mut QuicbitMessages,
) -> bool {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null que client or out pointer");
        return false;
    }
    let client = unsafe { &mut *client };
    let bytes = unsafe { bytes_in(data, len) };
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
    unsafe { *out_messages = Box::into_raw(Box::new(QuicbitMessages { messages })) };
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ans_server_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitAnsServer {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.ans_with_qos::<RawMsg, RawMsg>(topic, qos.into()) {
        Ok(server) => Box::into_raw(Box::new(QuicbitAnsServer { server })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ans_server_new(
    node: *const QuicbitNode,
    topic: *const c_char,
) -> *mut QuicbitAnsServer {
    quicbit_ans_server_new_with_qos(node, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ans_server_free(server: *mut QuicbitAnsServer) {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ans_server_stats(server: *const QuicbitAnsServer) -> QuicbitItemStats {
    if server.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*server }.server.stats())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ans_responder_send(
    responder: *mut QuicbitAnsResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    if responder.is_null() {
        return;
    }
    let bytes = unsafe { bytes_in(data, len) };
    unsafe { &mut *responder }
        .messages
        .push(RawMsg::new(kind, bytes));
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ans_server_serve_one(
    server: *mut QuicbitAnsServer,
    timeout_ms: u64,
    handler: QuicbitAnsHandler,
    ctx: *mut c_void,
) -> i32 {
    clear_last_error();
    if server.is_null() {
        set_last_error("null ans server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ans handler");
        return -1;
    };
    let server = unsafe { &mut *server };
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        match server.server.take() {
            Ok(Some((que, mut ans))) => {
                let mut responder = QuicbitAnsResponder {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ans_server_take(
    server: *mut QuicbitAnsServer,
    timeout_ms: u64,
    out_pending: *mut *mut QuicbitPendingQue,
) -> i32 {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null ans server or out pointer");
        return -1;
    }
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let server_ref = unsafe { &mut *server };
        match server_ref.server.take_message() {
            Ok(Some(pending)) => {
                let (que, answers) = pending.into_parts();
                let handle = QuicbitPendingQue {
                    server,
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_que_request(
    pending: *const QuicbitPendingQue,
) -> *const QuicbitMessage {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const QuicbitMessage
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_que_send(
    pending: *mut QuicbitPendingQue,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
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
    if pending.server.is_null() {
        set_last_error("pending que has null server");
        return false;
    }
    let bytes = unsafe { bytes_in(data, len) };
    let server = unsafe { &mut *pending.server };
    match server
        .server
        .send_pending(answers, &RawMsg::new(kind, bytes))
    {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_que_finish(pending: *mut QuicbitPendingQue) -> bool {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers.take() else {
        return true;
    };
    if pending.server.is_null() {
        set_last_error("pending que has null server");
        return false;
    }
    let server = unsafe { &mut *pending.server };
    match server.server.finish_pending(answers) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_que_free(pending: *mut QuicbitPendingQue) {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
}

// ---- put/ack ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_client_new_with_qos(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitPutClient {
    clear_last_error();
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
    let qos = qos.into();
    let result = match peer {
        Ok(addr) => node
            .node
            .put_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .put_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(QuicbitPutClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_client_new(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut QuicbitPutClient {
    quicbit_put_client_new_with_qos(node, peer, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_system_client_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitPutClient {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.put_with_qos::<RawMsg, RawMsg>(topic, qos.into()) {
        Ok(client) => Box::into_raw(Box::new(QuicbitPutClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_client_free(client: *mut QuicbitPutClient) {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_client_stats(client: *const QuicbitPutClient) -> QuicbitItemStats {
    if client.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_client_upload(
    client: *mut QuicbitPutClient,
    items: *const QuicbitRawMessage,
    len: usize,
    out_message: *mut *mut QuicbitMessage,
) -> bool {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null put client or out pointer");
        return false;
    }
    let raw_items = match unsafe { raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &mut *client };
    let mut sender = match client.client.open() {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_client_open(
    client: *mut QuicbitPutClient,
    out_upload: *mut *mut QuicbitPutUpload,
) -> bool {
    clear_last_error();
    if client.is_null() || out_upload.is_null() {
        set_last_error("null put client or out pointer");
        return false;
    }
    let client_ref = unsafe { &mut *client };
    match client_ref.client.open_upload() {
        Ok(token) => {
            unsafe {
                *out_upload = Box::into_raw(Box::new(QuicbitPutUpload {
                    client,
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_upload_send(
    upload: *mut QuicbitPutUpload,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
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
    if upload.client.is_null() {
        set_last_error("put upload has null client");
        return false;
    }
    let bytes = unsafe { bytes_in(data, len) };
    let client = unsafe { &mut *upload.client };
    match client.client.send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_upload_finish(
    upload: *mut QuicbitPutUpload,
    out_message: *mut *mut QuicbitMessage,
) -> bool {
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
    if upload.client.is_null() {
        set_last_error("put upload has null client");
        return false;
    }
    let client = unsafe { &mut *upload.client };
    match client.client.finish_pending(token) {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_put_upload_free(upload: *mut QuicbitPutUpload) {
    if upload.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(upload)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ack_server_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitAckServer {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.ack_with_qos::<RawMsg, RawMsg>(topic, qos.into()) {
        Ok(server) => Box::into_raw(Box::new(QuicbitAckServer { server })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ack_server_new(
    node: *const QuicbitNode,
    topic: *const c_char,
) -> *mut QuicbitAckServer {
    quicbit_ack_server_new_with_qos(node, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ack_server_free(server: *mut QuicbitAckServer) {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ack_server_stats(server: *const QuicbitAckServer) -> QuicbitItemStats {
    if server.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*server }.server.stats())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ack_server_serve_one(
    server: *mut QuicbitAckServer,
    timeout_ms: u64,
    handler: QuicbitAckHandler,
    ctx: *mut c_void,
) -> i32 {
    clear_last_error();
    if server.is_null() {
        set_last_error("null ack server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ack handler");
        return -1;
    };
    let server = unsafe { &mut *server };
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        match server.server.take() {
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
                let mut responder = QuicbitResponder {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_ack_server_take(
    server: *mut QuicbitAckServer,
    timeout_ms: u64,
    out_puts: *mut *mut QuicbitPuts,
) -> i32 {
    clear_last_error();
    if server.is_null() || out_puts.is_null() {
        set_last_error("null ack server or out pointer");
        return -1;
    }
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let server_ref = unsafe { &mut *server };
        match server_ref.server.take_message() {
            Ok(Some(pending)) => {
                let (_req_id, first, done, token) = pending.into_parts();
                let handle = QuicbitPuts {
                    server,
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_puts_next(
    puts: *mut QuicbitPuts,
    out_message: *mut *mut QuicbitMessage,
) -> i32 {
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
    if puts.server.is_null() {
        set_last_error("puts handle has null server");
        return -1;
    }
    let server = unsafe { &mut *puts.server };
    match server.server.next_pending(token) {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_puts_ack(
    puts: *mut QuicbitPuts,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
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
    if puts.server.is_null() {
        set_last_error("puts handle has null server");
        return false;
    }
    let bytes = unsafe { bytes_in(data, len) };
    let server = unsafe { &mut *puts.server };
    match server.server.ack_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_puts_free(puts: *mut QuicbitPuts) {
    if puts.is_null() {
        return;
    }
    let puts_ref = unsafe { &mut *puts };
    if let Some(token) = puts_ref.token.take()
        && !puts_ref.server.is_null()
    {
        let server = unsafe { &mut *puts_ref.server };
        server.server.close_pending(token);
    }
    unsafe { drop(Box::from_raw(puts)) };
}

// ---- pip ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_client_new_with_qos(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitPipClient {
    clear_last_error();
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
    let qos = qos.into();
    let result = match peer {
        Ok(addr) => node
            .node
            .pip_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .pip_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(QuicbitPipClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_client_new(
    node: *const QuicbitNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut QuicbitPipClient {
    quicbit_pip_client_new_with_qos(node, peer, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_system_client_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitPipClient {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.pip_with_qos::<RawMsg, RawMsg>(topic, qos.into()) {
        Ok(client) => Box::into_raw(Box::new(QuicbitPipClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_client_free(client: *mut QuicbitPipClient) {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_client_stats(client: *const QuicbitPipClient) -> QuicbitItemStats {
    if client.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_client_exchange(
    client: *mut QuicbitPipClient,
    items: *const QuicbitRawMessage,
    len: usize,
    out_messages: *mut *mut QuicbitMessages,
) -> bool {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null pip client or out pointer");
        return false;
    }
    let raw_items = match unsafe { raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &mut *client };
    let mut pip = match client.client.open() {
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
    unsafe { *out_messages = Box::into_raw(Box::new(QuicbitMessages { messages })) };
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_client_open(
    client: *mut QuicbitPipClient,
    out_pip: *mut *mut QuicbitPip,
) -> bool {
    clear_last_error();
    if client.is_null() || out_pip.is_null() {
        set_last_error("null pip client or out pointer");
        return false;
    }
    let client_ref = unsafe { &mut *client };
    match client_ref.client.open_session() {
        Ok(token) => {
            unsafe {
                *out_pip = Box::into_raw(Box::new(QuicbitPip {
                    client,
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_send(
    pip: *mut QuicbitPip,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
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
    if pip.client.is_null() {
        set_last_error("pip session has null client");
        return false;
    }
    let bytes = unsafe { bytes_in(data, len) };
    let client = unsafe { &mut *pip.client };
    match client.client.send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_finish_send(pip: *mut QuicbitPip) -> bool {
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
    if pip.client.is_null() {
        set_last_error("pip session has null client");
        return false;
    }
    let client = unsafe { &mut *pip.client };
    match client.client.finish_send_pending(token) {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_next(
    pip: *mut QuicbitPip,
    out_message: *mut *mut QuicbitMessage,
) -> i32 {
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
    if pip.client.is_null() {
        set_last_error("pip session has null client");
        return -1;
    }
    let client = unsafe { &mut *pip.client };
    match client.client.next_pending(token) {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_close(pip: *mut QuicbitPip) {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take()
        && !pip_ref.client.is_null()
    {
        let client = unsafe { &mut *pip_ref.client };
        client.client.close_session(token);
    }
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_free(pip: *mut QuicbitPip) {
    if pip.is_null() {
        return;
    }
    quicbit_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_server_new_with_qos(
    node: *const QuicbitNode,
    topic: *const c_char,
    qos: QuicbitTopicQos,
) -> *mut QuicbitPipServer {
    clear_last_error();
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
        .pip_server_with_qos::<RawMsg, RawMsg>(topic, qos.into())
    {
        Ok(server) => Box::into_raw(Box::new(QuicbitPipServer { server })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_server_new(
    node: *const QuicbitNode,
    topic: *const c_char,
) -> *mut QuicbitPipServer {
    quicbit_pip_server_new_with_qos(node, topic, quicbit_topic_qos_reliable())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_server_free(server: *mut QuicbitPipServer) {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_server_stats(server: *const QuicbitPipServer) -> QuicbitItemStats {
    if server.is_null() {
        return QuicbitItemStats::default();
    }
    item_stats_out(unsafe { &*server }.server.stats())
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_message_responder_send(
    responder: *mut QuicbitMessageResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    if responder.is_null() {
        return;
    }
    let bytes = unsafe { bytes_in(data, len) };
    unsafe { &mut *responder }
        .messages
        .push(RawMsg::new(kind, bytes));
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_server_serve_one(
    server: *mut QuicbitPipServer,
    timeout_ms: u64,
    handler: QuicbitPipHandler,
    ctx: *mut c_void,
) -> i32 {
    clear_last_error();
    if server.is_null() {
        set_last_error("null pip server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null pip handler");
        return -1;
    };
    let server = unsafe { &mut *server };
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        match server.server.take() {
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
                let mut responder = QuicbitMessageResponder {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pip_server_take(
    server: *mut QuicbitPipServer,
    timeout_ms: u64,
    out_pending: *mut *mut QuicbitPendingPip,
) -> i32 {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null pip server or out pointer");
        return -1;
    }
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let server_ref = unsafe { &mut *server };
        match server_ref.server.take_message() {
            Ok(Some(pending)) => {
                let (_session_id, first, incoming_done, token) = pending.into_parts();
                let handle = QuicbitPendingPip {
                    server,
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_pip_next(
    pip: *mut QuicbitPendingPip,
    out_message: *mut *mut QuicbitMessage,
) -> i32 {
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
    if pip.server.is_null() {
        set_last_error("pending pip has null server");
        return -1;
    }
    let server = unsafe { &mut *pip.server };
    match server.server.next_pending(token) {
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
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_pip_send(
    pip: *mut QuicbitPendingPip,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
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
    if pip.server.is_null() {
        set_last_error("pending pip has null server");
        return false;
    }
    let bytes = unsafe { bytes_in(data, len) };
    let server = unsafe { &mut *pip.server };
    match server.server.send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_pip_finish_send(pip: *mut QuicbitPendingPip) -> bool {
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
    if pip.server.is_null() {
        set_last_error("pending pip has null server");
        return false;
    }
    let server = unsafe { &mut *pip.server };
    match server.server.finish_send_pending(token) {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_pip_close(pip: *mut QuicbitPendingPip) {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take()
        && !pip_ref.server.is_null()
    {
        let server = unsafe { &mut *pip_ref.server };
        server.server.close_pending(token);
    }
    pip_ref.first = None;
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
}

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_pending_pip_free(pip: *mut QuicbitPendingPip) {
    if pip.is_null() {
        return;
    }
    quicbit_pending_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
}
