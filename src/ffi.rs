//! C FFI — handle-based API mirroring `wirebit.h` / `agent47.h`.
//!
//! The FFI is byte-oriented: the typed-payload generics that the
//! Rust API uses (`LocalService<T>`, `Loan<T>`, `Sample<T>`) don't
//! cross the language boundary cleanly. Instead, the FFI gives the
//! caller a raw `*mut u8` of `slot_size` bytes for each loan, plus
//! a `*const u8` of `slot_size` bytes for each sample.
//!
//! All return-int functions use these conventions:
//!
//! * `0`  — success.
//! * `1`  — non-error "nothing to do" (e.g. `take` with empty queue).
//! * `<0` — error; consult `quicbit_last_error()`.
//!
//! Handles are owning pointers. Free them with the matching
//! `_free` call when done.

#![allow(non_camel_case_types)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;

use crate::error::{Error, Result};
use crate::local::segment::{Segment, SegmentParams};

// --- error code constants ---

const STATUS_OK: c_int = 0;
const STATUS_EMPTY: c_int = 1;
const ERR_INVALID_ARGUMENT: c_int = -1;
const ERR_NOT_FOUND: c_int = -2;
const ERR_ALREADY_EXISTS: c_int = -3;
const ERR_NO_FREE_SLOT: c_int = -4;
const ERR_LAGGED: c_int = -5;
const ERR_TYPE_MISMATCH: c_int = -6;
const ERR_IO: c_int = -7;
const ERR_INTERNAL: c_int = -99;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(e: &Error) -> c_int {
    let msg = e.to_string();
    let code = match e {
        Error::ServiceNotFound(_) => ERR_NOT_FOUND,
        Error::ServiceAlreadyExists(_) => ERR_ALREADY_EXISTS,
        Error::NoFreeSlot { .. } => ERR_NO_FREE_SLOT,
        Error::Lagged { .. } => ERR_LAGGED,
        Error::TypeMismatch { .. } => ERR_TYPE_MISMATCH,
        Error::PayloadTooLarge { .. } => ERR_INVALID_ARGUMENT,
        Error::IncompatibleShm(_) => ERR_TYPE_MISMATCH,
        Error::Io(_) => ERR_IO,
        Error::InvalidArgument(_) => ERR_INVALID_ARGUMENT,
        _ => ERR_INTERNAL,
    };
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new(msg).ok();
    });
    code
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
}

/// Returns a pointer to a thread-local NUL-terminated error string
/// for the most recent failure on this thread, or `NULL` if there
/// is no last error. The pointer is valid until the next FFI call
/// from this thread.
#[unsafe(no_mangle)]
pub extern "C" fn quicbit_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match &*slot.borrow() {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    })
}

// --- opaque handles ---

pub struct quicbit_service {
    segment: Segment,
}

pub struct quicbit_publisher {
    segment: Segment,
}

pub struct quicbit_subscriber {
    segment: Segment,
    next_seq: u64,
}

pub struct quicbit_loan {
    segment: Segment,
    slot_idx: u32,
    published: bool,
}

impl Drop for quicbit_loan {
    fn drop(&mut self) {
        if !self.published {
            // Same rollback semantics as `Loan::Drop`: the slot
            // was popped exclusively (gen bumped), so refcount is
            // already 0 and stale subscribers' CAS-bump will fail
            // their generation match. Push back to the free list.
            self.segment.push_free(self.slot_idx);
        }
    }
}

pub struct quicbit_sample {
    segment: Segment,
    slot_idx: u32,
}

impl Drop for quicbit_sample {
    fn drop(&mut self) {
        self.segment.release(self.slot_idx);
    }
}

unsafe fn cstr_to_str<'a>(p: *const c_char) -> Result<&'a str> {
    if p.is_null() {
        return Err(Error::invalid_argument("null string"));
    }
    unsafe {
        CStr::from_ptr(p)
            .to_str()
            .map_err(|_| Error::invalid_argument("non-UTF-8 string"))
    }
}

fn build_params(slot_count: u32, slot_size: u32, history_depth: u32) -> SegmentParams {
    SegmentParams {
        slot_count,
        slot_size,
        history_depth,
        type_name: "<quicbit ffi>",
    }
}

/// Create a brand-new local service.
///
/// # Safety
///
/// `name` must point to a NUL-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_service_create(
    name: *const c_char,
    slot_count: u32,
    slot_size: u32,
    history_depth: u32,
) -> *mut quicbit_service {
    clear_last_error();
    let name = match unsafe { cstr_to_str(name) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(&e);
            return ptr::null_mut();
        }
    };
    match Segment::create(name, build_params(slot_count, slot_size, history_depth)) {
        Ok(segment) => Box::into_raw(Box::new(quicbit_service { segment })),
        Err(e) => {
            set_last_error(&e);
            ptr::null_mut()
        }
    }
}

/// Attach to an existing local service.
///
/// # Safety
///
/// `name` must point to a NUL-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_service_attach(name: *const c_char) -> *mut quicbit_service {
    clear_last_error();
    let name = match unsafe { cstr_to_str(name) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(&e);
            return ptr::null_mut();
        }
    };
    let expected_hash = crate::local::layout::fnv1a64("<quicbit ffi>");
    match Segment::attach(name, expected_hash) {
        Ok(segment) => Box::into_raw(Box::new(quicbit_service { segment })),
        Err(e) => {
            set_last_error(&e);
            ptr::null_mut()
        }
    }
}

/// Create-or-attach.
///
/// # Safety
///
/// `name` must point to a NUL-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_service_open_or_create(
    name: *const c_char,
    slot_count: u32,
    slot_size: u32,
    history_depth: u32,
) -> *mut quicbit_service {
    let created = unsafe { quicbit_service_create(name, slot_count, slot_size, history_depth) };
    if !created.is_null() {
        return created;
    }
    unsafe { quicbit_service_attach(name) }
}

/// Free a service handle. Safe to call with `NULL`.
///
/// # Safety
///
/// `svc` must have come from one of the `_create` / `_attach`
/// functions and must not have been freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_service_free(svc: *mut quicbit_service) {
    if svc.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(svc) });
}

/// Build a publisher.
///
/// # Safety
///
/// `svc` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_publisher_new(
    svc: *mut quicbit_service,
) -> *mut quicbit_publisher {
    if svc.is_null() {
        set_last_error(&Error::invalid_argument("null service"));
        return ptr::null_mut();
    }
    let svc = unsafe { &*svc };
    Box::into_raw(Box::new(quicbit_publisher {
        segment: svc.segment.clone(),
    }))
}

/// Free a publisher.
///
/// # Safety
///
/// `p` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_publisher_free(p: *mut quicbit_publisher) {
    if p.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(p) });
}

/// Reserve a slot. On success, `*out_loan`, `*out_ptr`, `*out_len`
/// are set; the pointer is writable until publish.
///
/// # Safety
///
/// All output pointers must be non-null and point to writable u64-
/// aligned storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_publisher_loan(
    p: *mut quicbit_publisher,
    out_loan: *mut *mut quicbit_loan,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    clear_last_error();
    if p.is_null() || out_loan.is_null() || out_ptr.is_null() || out_len.is_null() {
        return set_last_error(&Error::invalid_argument("null pointer"));
    }
    let p = unsafe { &*p };
    let slot_idx = match p.segment.pop_free() {
        Some(i) => i,
        None => {
            return set_last_error(&Error::NoFreeSlot {
                service: p.segment.name().to_string(),
            });
        }
    };
    let ptr_u8 = p.segment.slot_payload(slot_idx);
    let len = p.segment.slot_size() as usize;
    unsafe { std::ptr::write_bytes(ptr_u8, 0u8, len) };
    let loan = Box::new(quicbit_loan {
        segment: p.segment.clone(),
        slot_idx,
        published: false,
    });
    unsafe {
        *out_loan = Box::into_raw(loan);
        *out_ptr = ptr_u8;
        *out_len = len;
    }
    STATUS_OK
}

/// Publish a loan. Consumes the loan handle.
///
/// # Safety
///
/// `loan` must be a valid handle from `quicbit_publisher_loan`
/// and not have been published / aborted yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_publisher_publish(
    p: *mut quicbit_publisher,
    loan: *mut quicbit_loan,
) -> c_int {
    clear_last_error();
    if p.is_null() || loan.is_null() {
        return set_last_error(&Error::invalid_argument("null pointer"));
    }
    let mut loan = unsafe { Box::from_raw(loan) };
    let p = unsafe { &*p };
    let slot_idx = loan.slot_idx;
    loan.published = true;
    drop(loan);
    p.segment.publish_slot(slot_idx);
    STATUS_OK
}

/// Abort a loan without publishing.
///
/// # Safety
///
/// `loan` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_loan_abort(loan: *mut quicbit_loan) -> c_int {
    clear_last_error();
    if loan.is_null() {
        return set_last_error(&Error::invalid_argument("null pointer"));
    }
    drop(unsafe { Box::from_raw(loan) });
    STATUS_OK
}

/// Build a subscriber.
///
/// # Safety
///
/// `svc` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_subscriber_new(
    svc: *mut quicbit_service,
) -> *mut quicbit_subscriber {
    if svc.is_null() {
        set_last_error(&Error::invalid_argument("null service"));
        return ptr::null_mut();
    }
    let svc = unsafe { &*svc };
    let next_seq = svc.segment.latest_seq() + 1;
    Box::into_raw(Box::new(quicbit_subscriber {
        segment: svc.segment.clone(),
        next_seq,
    }))
}

/// Free a subscriber.
///
/// # Safety
///
/// `s` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_subscriber_free(s: *mut quicbit_subscriber) {
    if s.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(s) });
}

/// Non-blocking take.
///
/// # Safety
///
/// All output pointers must be non-null and writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_subscriber_take(
    s: *mut quicbit_subscriber,
    out_sample: *mut *mut quicbit_sample,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> c_int {
    clear_last_error();
    if s.is_null() || out_sample.is_null() || out_ptr.is_null() || out_len.is_null() {
        return set_last_error(&Error::invalid_argument("null pointer"));
    }
    let s = unsafe { &mut *s };
    let wanted = s.next_seq;
    match s.segment.try_acquire(wanted) {
        Ok(Some((slot_idx, seq))) => {
            s.next_seq = seq + 1;
            let ptr_u8 = s.segment.slot_payload(slot_idx) as *const u8;
            let len = s.segment.slot_size() as usize;
            let sample = Box::new(quicbit_sample {
                segment: s.segment.clone(),
                slot_idx,
            });
            unsafe {
                *out_sample = Box::into_raw(sample);
                *out_ptr = ptr_u8;
                *out_len = len;
            }
            STATUS_OK
        }
        Ok(None) => STATUS_EMPTY,
        Err(Error::Lagged { dropped }) => {
            let latest = s.segment.latest_seq();
            let depth = s.segment.history_depth() as u64;
            s.next_seq = if latest > depth { latest - depth + 1 } else { 1 };
            set_last_error(&Error::Lagged { dropped })
        }
        Err(e) => set_last_error(&e),
    }
}

/// Free a sample.
///
/// # Safety
///
/// `sample` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn quicbit_sample_free(sample: *mut quicbit_sample) {
    if sample.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(sample) });
}
