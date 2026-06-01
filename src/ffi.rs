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

use crate::{Node, Publisher, RawMsg, ReqClient, ReqServer, Subscriber};

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

pub struct QuicbitPublisher {
    publisher: Publisher<RawMsg>,
}

pub struct QuicbitSubscriber {
    subscriber: Subscriber<RawMsg>,
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

// ---- node ----

/// Create a node. `identity` may be NULL for an ephemeral key. Returns
/// NULL on failure (see [`quicbit_last_error_message`]).
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_node_new(identity: *const c_char, no_relay: bool) -> *mut QuicbitNode {
    clear_last_error();
    let mut builder = Node::builder();
    if !identity.is_null() {
        // SAFETY: validated non-null; caller promises a valid C string.
        match unsafe { cstr(identity) } {
            Ok(s) => builder = builder.identity(s),
            Err(()) => return ptr::null_mut(),
        }
    }
    if no_relay {
        builder = builder.no_relay();
    }
    match builder.bind() {
        Ok(node) => Box::into_raw(Box::new(QuicbitNode { node })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
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

/// Free a string returned by quicbit (e.g. [`quicbit_node_did_key`]).
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    // SAFETY: originated from CString::into_raw.
    unsafe { drop(CString::from_raw(s)) };
}

// ---- pub/sub ----

#[unsafe(no_mangle)]
pub extern "C" fn quicbit_publisher_new(
    node: *const QuicbitNode,
    topic: *const c_char,
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
    match node.node.publisher::<RawMsg>(topic) {
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
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let peer = match unsafe { cstr(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.subscriber::<RawMsg>(peer, topic) {
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

// ---- message accessors ----

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
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let peer = match unsafe { cstr(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.req_client::<RawMsg, RawMsg>(peer, topic) {
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
    match node.node.req_server::<RawMsg, RawMsg>(topic) {
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
