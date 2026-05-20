//! End-to-end smoke test for the C FFI.
//!
//! Calls the same functions a C client would, verifying the
//! handle-based ABI works in-process (publishing one message, taking
//! it back, freeing handles).

use std::ffi::CString;
use std::os::raw::c_int;
use std::ptr;

use quicbit::ffi::*;

fn unique_name(stem: &str) -> CString {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    CString::new(format!("ffi-{stem}-{pid}-{nanos}")).unwrap()
}

#[test]
fn ffi_loan_publish_take_roundtrip() {
    unsafe {
        let name = unique_name("rt");
        let svc = quicbit_service_create(name.as_ptr(), 8, 32, 1);
        assert!(!svc.is_null(), "create should succeed");

        let pubr = quicbit_publisher_new(svc);
        assert!(!pubr.is_null());
        let sub = quicbit_subscriber_new(svc);
        assert!(!sub.is_null());

        // Loan a slot, write a known pattern, publish.
        let mut loan: *mut quicbit_loan = ptr::null_mut();
        let mut ptr_w: *mut u8 = ptr::null_mut();
        let mut len: usize = 0;
        let rc = quicbit_publisher_loan(pubr, &mut loan, &mut ptr_w, &mut len);
        assert_eq!(rc, 0);
        assert!(!loan.is_null() && !ptr_w.is_null());
        assert_eq!(len, 32);
        for i in 0..len {
            *ptr_w.add(i) = (i as u8) ^ 0xA5;
        }
        let rc = quicbit_publisher_publish(pubr, loan);
        assert_eq!(rc, 0);

        // Take the sample back.
        let mut sample: *mut quicbit_sample = ptr::null_mut();
        let mut ptr_r: *const u8 = ptr::null();
        let mut rlen: usize = 0;
        let rc: c_int = quicbit_subscriber_take(sub, &mut sample, &mut ptr_r, &mut rlen);
        assert_eq!(rc, 0, "take should return a sample");
        assert!(!sample.is_null() && !ptr_r.is_null());
        assert_eq!(rlen, 32);
        for i in 0..rlen {
            assert_eq!(*ptr_r.add(i), (i as u8) ^ 0xA5, "byte {i} mismatch");
        }
        quicbit_sample_free(sample);

        // Empty queue → status 1.
        let rc = quicbit_subscriber_take(sub, &mut sample, &mut ptr_r, &mut rlen);
        assert_eq!(rc, 1, "take on empty queue should return STATUS_EMPTY");

        quicbit_subscriber_free(sub);
        quicbit_publisher_free(pubr);
        quicbit_service_free(svc);
    }
}

#[test]
fn ffi_create_collision_reports_already_exists() {
    unsafe {
        let name = unique_name("collision");
        let svc1 = quicbit_service_create(name.as_ptr(), 4, 16, 1);
        assert!(!svc1.is_null());

        let svc2 = quicbit_service_create(name.as_ptr(), 4, 16, 1);
        assert!(
            svc2.is_null(),
            "second create with same name must return NULL"
        );
        let err = quicbit_last_error();
        assert!(!err.is_null(), "last_error should be populated");

        quicbit_service_free(svc1);
    }
}

#[test]
fn ffi_loan_abort_returns_slot() {
    unsafe {
        let name = unique_name("abort");
        let svc = quicbit_service_create(name.as_ptr(), 1, 8, 1);
        assert!(!svc.is_null());
        let pubr = quicbit_publisher_new(svc);

        // Loan the only slot, abort, loan again — should succeed.
        let mut loan: *mut quicbit_loan = ptr::null_mut();
        let mut p: *mut u8 = ptr::null_mut();
        let mut n: usize = 0;
        assert_eq!(quicbit_publisher_loan(pubr, &mut loan, &mut p, &mut n), 0);
        assert_eq!(quicbit_loan_abort(loan), 0);
        assert_eq!(quicbit_publisher_loan(pubr, &mut loan, &mut p, &mut n), 0);
        assert_eq!(quicbit_loan_abort(loan), 0);

        quicbit_publisher_free(pubr);
        quicbit_service_free(svc);
    }
}
