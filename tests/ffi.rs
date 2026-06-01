//! Exercises the C ABI (`quicbit::ffi`) end-to-end from Rust, so the
//! foreign-language surface is covered by `cargo test` / CI rather than
//! only by the hand-run `bindings/c/pubsub.c` demo.

use std::ffi::{CString, c_void};
use std::ptr;
use std::time::{Duration, Instant};

use quicbit::ffi::*;

fn cstr(s: &str) -> CString {
    CString::new(s).unwrap()
}

#[test]
fn c_abi_pubsub_round_trip() {
    let identity = cstr("ffi-pubsub");
    let topic = cstr("ffi/topic");

    let node = quicbit_node_new(identity.as_ptr(), true);
    assert!(!node.is_null(), "node_new failed");

    // did:key is a non-empty owned string.
    let did = quicbit_node_did_key(node);
    assert!(!did.is_null());
    quicbit_string_free(did);

    let publisher = quicbit_publisher_new(node, topic.as_ptr());
    let subscriber = quicbit_subscriber_new(node, identity.as_ptr(), topic.as_ptr());
    assert!(!publisher.is_null() && !subscriber.is_null());

    let payload = [0xDEu8, 0xAD, 0xBE, 0xEF];
    assert!(quicbit_publisher_send(
        publisher,
        7,
        payload.as_ptr(),
        payload.len()
    ));

    let mut msg: *mut QuicbitMessage = ptr::null_mut();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut rc = 0;
    while rc == 0 && Instant::now() < deadline {
        rc = quicbit_subscriber_take(subscriber, &mut msg as *mut *mut QuicbitMessage);
        if rc == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(rc, 1, "expected a message");
    assert!(!msg.is_null());

    assert_eq!(quicbit_message_kind(msg), 7);
    let bytes = quicbit_message_data(msg);
    // SAFETY: view valid until quicbit_message_free.
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &payload);

    quicbit_message_free(msg);
    quicbit_subscriber_free(subscriber);
    quicbit_publisher_free(publisher);
    quicbit_node_free(node);
}

unsafe extern "C" fn double_handler(
    _ctx: *mut c_void,
    kind: u64,
    data: *const u8,
    len: usize,
    responder: *mut QuicbitResponder,
) {
    // SAFETY: ffi contract — `len` valid bytes at `data`.
    let input = if data.is_null() || len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };
    let doubled: Vec<u8> = input.iter().map(|b| b.wrapping_mul(2)).collect();
    quicbit_responder_set(responder, kind, doubled.as_ptr(), doubled.len());
}

#[test]
fn c_abi_req_res_round_trip() {
    let identity = cstr("ffi-calc");
    let topic = cstr("ffi/double");

    let node = quicbit_node_new(identity.as_ptr(), true);
    assert!(!node.is_null());

    let server = quicbit_req_server_new(node, topic.as_ptr());
    let client = quicbit_req_client_new(node, identity.as_ptr(), topic.as_ptr());
    assert!(!server.is_null() && !client.is_null());

    // Move the server pointer into the serve thread (raw ptrs aren't Send;
    // the test owns it exclusively, so a usize hop is sound here).
    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut QuicbitReqServer;
        quicbit_req_server_serve_one(server, 3000, Some(double_handler), ptr::null_mut())
    });

    let request = [1u8, 2, 3];
    let mut response: *mut QuicbitMessage = ptr::null_mut();
    let ok = quicbit_req_client_call(
        client,
        9,
        request.as_ptr(),
        request.len(),
        &mut response as *mut *mut QuicbitMessage,
    );
    assert!(ok, "call failed");
    assert!(!response.is_null());

    assert_eq!(quicbit_message_kind(response), 9);
    let bytes = quicbit_message_data(response);
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &[2u8, 4, 6]);

    let served = handle.join().unwrap();
    assert_eq!(served, 1, "server should have served one request");

    quicbit_message_free(response);
    quicbit_req_client_free(client);
    quicbit_req_server_free(server);
    quicbit_node_free(node);
}
