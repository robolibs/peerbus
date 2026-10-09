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

mod datapod_pip;
mod datapod_putack;
mod datapod_queans;
mod datapod_reqres;
mod handles;
mod helpers;
mod node;
mod parity;
mod pip;
mod pubsub;
mod putack;
mod queans;
mod reqres;

// Re-export the whole surface so `peerbus::ffi::*` (used by tests and any
// Rust caller) resolves every handle type, config/stats struct, constant,
// and `extern "C"` entry point exactly as when this was one flat module.
pub use datapod_pip::*;
pub use datapod_putack::*;
pub use datapod_queans::*;
pub use datapod_reqres::*;
pub use handles::*;
pub(crate) use helpers::*;
pub use node::*;
pub use parity::*;
pub use pip::*;
pub use pubsub::*;
pub use putack::*;
pub use queans::*;
pub use reqres::*;
