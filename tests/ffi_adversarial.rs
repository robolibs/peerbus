//! Adversarial input tests for the C ABI.
//!
//! Verify that hostile / sloppy callers can't crash the library:
//!
//! * NULL handles → graceful error, last_error populated.
//! * `_free(NULL)` → no-op (matches `free(NULL)`).
//! * Non-UTF-8 / NUL-embedded strings → InvalidArgument.
//! * Empty / too-large numeric params → InvalidArgument.
//! * Double `_free` is *not* tested here — that's caller UB by
//!   convention; we don't try to detect it.
//! * Output pointer args of NULL → graceful error.
//!
//! No assertion that errors carry specific text; just that the
//! library returns a sensible status without crashing.

use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

use quicbit::ffi::*;

fn unique_name(stem: &str) -> CString {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    CString::new(format!("adv-{stem}-{pid}-{nanos}")).unwrap()
}

// --- service ---

#[test]
fn create_with_null_name_does_not_crash() {
    unsafe {
        let svc = quicbit_service_create(ptr::null(), 4, 16, 1);
        assert!(svc.is_null(), "null name should fail");
        let err = quicbit_last_error();
        assert!(!err.is_null(), "last_error populated");
    }
}

#[test]
fn create_with_zero_slot_count_fails() {
    unsafe {
        let name = unique_name("zeroslots");
        let svc = quicbit_service_create(name.as_ptr(), 0, 16, 1);
        assert!(svc.is_null());
    }
}

#[test]
fn create_with_zero_slot_size_fails() {
    unsafe {
        let name = unique_name("zerosize");
        let svc = quicbit_service_create(name.as_ptr(), 4, 0, 1);
        assert!(svc.is_null());
    }
}

#[test]
fn attach_to_nonexistent_returns_null() {
    unsafe {
        let name = unique_name("absent");
        let svc = quicbit_service_attach(name.as_ptr());
        assert!(svc.is_null());
        let err = quicbit_last_error();
        assert!(!err.is_null());
    }
}

#[test]
fn service_free_null_is_noop() {
    unsafe {
        quicbit_service_free(ptr::null_mut());
    }
}

// --- publisher ---

#[test]
fn publisher_new_with_null_service_fails() {
    unsafe {
        let p = quicbit_publisher_new(ptr::null_mut());
        assert!(p.is_null());
    }
}

#[test]
fn publisher_free_null_is_noop() {
    unsafe {
        quicbit_publisher_free(ptr::null_mut());
    }
}

#[test]
fn publisher_loan_with_null_out_pointers_fails() {
    unsafe {
        let name = unique_name("loan_null_out");
        let svc = quicbit_service_create(name.as_ptr(), 4, 16, 1);
        assert!(!svc.is_null());
        let pubr = quicbit_publisher_new(svc);
        assert!(!pubr.is_null());

        let mut loan: *mut quicbit_loan = ptr::null_mut();
        let mut data: *mut u8 = ptr::null_mut();
        let mut len: usize = 0;

        // Each of the 4 out-pointers must be non-null. NULL p should fail.
        let rc1 = quicbit_publisher_loan(ptr::null_mut(), &mut loan, &mut data, &mut len);
        assert!(rc1 < 0, "null publisher → error");
        let rc2 = quicbit_publisher_loan(pubr, ptr::null_mut(), &mut data, &mut len);
        assert!(rc2 < 0, "null out_loan → error");
        let rc3 = quicbit_publisher_loan(pubr, &mut loan, ptr::null_mut(), &mut len);
        assert!(rc3 < 0, "null out_ptr → error");
        let rc4 = quicbit_publisher_loan(pubr, &mut loan, &mut data, ptr::null_mut());
        assert!(rc4 < 0, "null out_len → error");

        quicbit_publisher_free(pubr);
        quicbit_service_free(svc);
    }
}

#[test]
fn publisher_publish_null_loan_fails() {
    unsafe {
        let name = unique_name("publish_null");
        let svc = quicbit_service_create(name.as_ptr(), 4, 16, 1);
        let pubr = quicbit_publisher_new(svc);
        let rc = quicbit_publisher_publish(pubr, ptr::null_mut());
        assert!(rc < 0);
        let rc2 = quicbit_publisher_publish(ptr::null_mut(), ptr::null_mut());
        assert!(rc2 < 0);
        quicbit_publisher_free(pubr);
        quicbit_service_free(svc);
    }
}

#[test]
fn loan_abort_null_fails() {
    unsafe {
        let rc = quicbit_loan_abort(ptr::null_mut());
        assert!(rc < 0);
    }
}

// --- subscriber ---

#[test]
fn subscriber_new_null_service_fails() {
    unsafe {
        let s = quicbit_subscriber_new(ptr::null_mut());
        assert!(s.is_null());
    }
}

#[test]
fn subscriber_take_with_null_out_pointers_fails() {
    unsafe {
        let name = unique_name("take_null_out");
        let svc = quicbit_service_create(name.as_ptr(), 4, 16, 1);
        let sub = quicbit_subscriber_new(svc);

        let mut sample: *mut quicbit_sample = ptr::null_mut();
        let mut data: *const u8 = ptr::null();
        let mut len: usize = 0;

        let rc1 = quicbit_subscriber_take(ptr::null_mut(), &mut sample, &mut data, &mut len);
        assert!(rc1 < 0);
        let rc2 = quicbit_subscriber_take(sub, ptr::null_mut(), &mut data, &mut len);
        assert!(rc2 < 0);
        let rc3 = quicbit_subscriber_take(sub, &mut sample, ptr::null_mut(), &mut len);
        assert!(rc3 < 0);
        let rc4 = quicbit_subscriber_take(sub, &mut sample, &mut data, ptr::null_mut());
        assert!(rc4 < 0);

        quicbit_subscriber_free(sub);
        quicbit_service_free(svc);
    }
}

#[test]
fn subscriber_free_null_noop() {
    unsafe {
        quicbit_subscriber_free(ptr::null_mut());
    }
}

#[test]
fn sample_free_null_noop() {
    unsafe {
        quicbit_sample_free(ptr::null_mut());
    }
}

// --- last_error contract ---

#[test]
fn last_error_clears_on_successful_call() {
    unsafe {
        // Provoke an error first.
        let s = quicbit_subscriber_new(ptr::null_mut());
        assert!(s.is_null());
        let after_fail = quicbit_last_error();
        assert!(!after_fail.is_null(), "error populated after failure");

        // Now do a successful call. last_error should be cleared.
        let name = unique_name("clear");
        let svc = quicbit_service_create(name.as_ptr(), 4, 16, 1);
        assert!(!svc.is_null());
        let after_ok = quicbit_last_error();
        assert!(
            after_ok.is_null(),
            "last_error should be cleared after a successful call"
        );

        quicbit_service_free(svc);
    }
}

// --- non-UTF-8 names ---

#[test]
fn non_utf8_name_rejected() {
    unsafe {
        // CString of arbitrary bytes; UTF-8 validation happens
        // inside the FFI when converting to &str.
        let bytes = [0xff, 0xfe, 0xfd, 0u8]; // not valid UTF-8 + NUL terminator
        let name = bytes.as_ptr() as *const c_char;
        let svc = quicbit_service_create(name, 4, 16, 1);
        assert!(svc.is_null(), "non-UTF-8 name should be rejected");
    }
}
