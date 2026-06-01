//! Exercises the C ABI (`quicbit::ffi`) end-to-end from Rust, so the
//! foreign-language surface is covered by `cargo test` / CI rather than
//! only by the hand-run `examples/c_abi/pubsub.c` demo.

use std::ffi::{CString, c_void};
use std::ptr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use quicbit::ffi::*;

static FFI_TEST_LOCK: Mutex<()> = Mutex::new(());

fn cstr(s: &str) -> CString {
    CString::new(s).unwrap()
}

fn unique(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{stem}-{pid}-{nanos}")
}

fn last_error() -> String {
    let ptr = quicbit_last_error_message();
    if ptr.is_null() {
        "<no ffi error>".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

fn message_bytes(message: *const QuicbitMessage) -> Vec<u8> {
    let bytes = quicbit_message_data(message);
    if bytes.ptr.is_null() || bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    }
}

#[test]
fn c_abi_pubsub_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let identity = cstr(&unique("ffi-pubsub"));
    let topic = cstr("ffi/topic");

    let node = quicbit_node_new(identity.as_ptr(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

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
    assert_eq!(quicbit_publisher_stats(publisher).published, 1);
    assert_eq!(quicbit_subscriber_stats(subscriber).received, 1);

    quicbit_message_free(msg);
    quicbit_subscriber_free(subscriber);
    quicbit_publisher_free(publisher);
    quicbit_node_free(node);
}

#[test]
fn c_abi_system_did_pubsub_with_qos_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let seed = quicbit_node_new(ptr::null(), true);
    assert!(!seed.is_null(), "seed node_new failed: {}", last_error());
    let did = quicbit_node_did_key(seed);
    assert!(!did.is_null());
    let system_did = unsafe { std::ffi::CStr::from_ptr(did) }
        .to_string_lossy()
        .into_owned();
    quicbit_string_free(did);
    quicbit_node_free(seed);

    let system = cstr(&system_did);
    let topic = cstr("ffi/system_topic");
    let cfg = QuicbitNodeConfig {
        identity: ptr::null(),
        no_relay: true,
        system_did: system.as_ptr(),
        max_payload_bytes: 0,
        history_depth: 8,
        subscriber_buffer: 8,
    };
    let pub_node = quicbit_node_new_with_config(cfg);
    let sub_node = quicbit_node_new_with_config(cfg);
    assert!(!pub_node.is_null(), "pub node failed: {}", last_error());
    assert!(!sub_node.is_null(), "sub node failed: {}", last_error());

    let qos = quicbit_topic_qos_latest();
    let publisher = quicbit_publisher_new_with_qos(pub_node, topic.as_ptr(), qos);
    let subscriber = quicbit_subscribe_new_with_qos(sub_node, topic.as_ptr(), qos);
    assert!(
        !publisher.is_null() && !subscriber.is_null(),
        "system setup failed: {}",
        last_error()
    );

    let payload = b"system";
    assert!(quicbit_publisher_send(
        publisher,
        55,
        payload.as_ptr(),
        payload.len()
    ));
    let mut msg: *mut QuicbitMessage = ptr::null_mut();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut rc = 0;
    while rc == 0 && Instant::now() < deadline {
        rc = quicbit_subscriber_take(subscriber, &mut msg);
        if rc == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(rc, 1, "expected system message: {}", last_error());
    assert_eq!(quicbit_message_kind(msg), 55);
    let bytes = quicbit_message_data(msg);
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, payload);

    quicbit_message_free(msg);
    quicbit_subscriber_free(subscriber);
    quicbit_publisher_free(publisher);
    quicbit_node_free(sub_node);
    quicbit_node_free(pub_node);
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

unsafe extern "C" fn range_handler(
    _ctx: *mut c_void,
    kind: u64,
    data: *const u8,
    len: usize,
    responder: *mut QuicbitAnsResponder,
) {
    let input = if data.is_null() || len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };
    let start = input.first().copied().unwrap_or(0);
    let count = input.get(1).copied().unwrap_or(0);
    for offset in 0..count {
        let value = [start.wrapping_add(offset)];
        quicbit_ans_responder_send(responder, kind, value.as_ptr(), value.len());
    }
}

#[test]
fn c_abi_que_ans_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let identity = cstr(&unique("ffi-search"));
    let topic = cstr("ffi/range");
    let node = quicbit_node_new(identity.as_ptr(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let server = quicbit_ans_server_new(node, topic.as_ptr());
    let client = quicbit_que_client_new(node, identity.as_ptr(), topic.as_ptr());
    assert!(
        !server.is_null() && !client.is_null(),
        "que setup failed: {}",
        last_error()
    );

    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut QuicbitAnsServer;
        quicbit_ans_server_serve_one(server, 3000, Some(range_handler), ptr::null_mut())
    });

    let request = [10u8, 3];
    let mut answers: *mut QuicbitMessages = ptr::null_mut();
    assert!(quicbit_que_client_send(
        client,
        12,
        request.as_ptr(),
        request.len(),
        &mut answers,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert!(!answers.is_null());
    assert_eq!(quicbit_messages_len(answers), 3);
    for i in 0..3 {
        assert_eq!(quicbit_messages_kind_at(answers, i), 12);
        let bytes = quicbit_messages_data_at(answers, i);
        let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
        assert_eq!(got, &[10 + i as u8]);
    }
    quicbit_messages_free(answers);
    quicbit_que_client_free(client);
    quicbit_ans_server_free(server);
    quicbit_node_free(node);
}

unsafe extern "C" fn upload_handler(
    _ctx: *mut c_void,
    items: *const QuicbitMessages,
    responder: *mut QuicbitResponder,
) {
    let mut sum = 0u8;
    for i in 0..quicbit_messages_len(items) {
        let bytes = quicbit_messages_data_at(items, i);
        let data = if bytes.ptr.is_null() || bytes.len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }
        };
        sum = sum.wrapping_add(data.iter().copied().sum::<u8>());
    }
    let ack = [sum];
    quicbit_responder_set(responder, 99, ack.as_ptr(), ack.len());
}

#[test]
fn c_abi_put_ack_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let identity = cstr(&unique("ffi-sink"));
    let topic = cstr("ffi/upload");
    let node = quicbit_node_new(identity.as_ptr(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());
    let server = quicbit_ack_server_new(node, topic.as_ptr());
    let client = quicbit_put_client_new(node, identity.as_ptr(), topic.as_ptr());
    assert!(
        !server.is_null() && !client.is_null(),
        "put setup failed: {}",
        last_error()
    );
    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut QuicbitAckServer;
        quicbit_ack_server_serve_one(server, 3000, Some(upload_handler), ptr::null_mut())
    });

    let a = [1u8, 2];
    let b = [3u8, 4];
    let items = [
        QuicbitRawMessage {
            kind: 1,
            data: QuicbitBytes {
                ptr: a.as_ptr(),
                len: a.len(),
            },
        },
        QuicbitRawMessage {
            kind: 1,
            data: QuicbitBytes {
                ptr: b.as_ptr(),
                len: b.len(),
            },
        },
    ];
    let mut ack: *mut QuicbitMessage = ptr::null_mut();
    assert!(quicbit_put_client_upload(
        client,
        items.as_ptr(),
        items.len(),
        &mut ack,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert_eq!(quicbit_message_kind(ack), 99);
    let bytes = quicbit_message_data(ack);
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &[10]);

    quicbit_message_free(ack);
    quicbit_put_client_free(client);
    quicbit_ack_server_free(server);
    quicbit_node_free(node);
}

unsafe extern "C" fn pip_handler(
    _ctx: *mut c_void,
    items: *const QuicbitMessages,
    responder: *mut QuicbitMessageResponder,
) {
    for i in 0..quicbit_messages_len(items) {
        let kind = quicbit_messages_kind_at(items, i);
        let bytes = quicbit_messages_data_at(items, i);
        let data = if bytes.ptr.is_null() || bytes.len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }
        };
        let doubled: Vec<u8> = data.iter().map(|b| b.wrapping_mul(2)).collect();
        quicbit_message_responder_send(responder, kind, doubled.as_ptr(), doubled.len());
    }
}

#[test]
fn c_abi_pip_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let identity = cstr(&unique("ffi-pip"));
    let topic = cstr("ffi/session");
    let node = quicbit_node_new(identity.as_ptr(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());
    let server = quicbit_pip_server_new(node, topic.as_ptr());
    let client = quicbit_pip_client_new(node, identity.as_ptr(), topic.as_ptr());
    assert!(
        !server.is_null() && !client.is_null(),
        "pip setup failed: {}",
        last_error()
    );
    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut QuicbitPipServer;
        quicbit_pip_server_serve_one(server, 3000, Some(pip_handler), ptr::null_mut())
    });

    let a = [2u8, 4];
    let b = [5u8];
    let items = [
        QuicbitRawMessage {
            kind: 7,
            data: QuicbitBytes {
                ptr: a.as_ptr(),
                len: a.len(),
            },
        },
        QuicbitRawMessage {
            kind: 8,
            data: QuicbitBytes {
                ptr: b.as_ptr(),
                len: b.len(),
            },
        },
    ];
    let mut replies: *mut QuicbitMessages = ptr::null_mut();
    assert!(quicbit_pip_client_exchange(
        client,
        items.as_ptr(),
        items.len(),
        &mut replies,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert_eq!(quicbit_messages_len(replies), 2);
    assert_eq!(quicbit_messages_kind_at(replies, 0), 7);
    let first = quicbit_messages_data_at(replies, 0);
    let got = unsafe { std::slice::from_raw_parts(first.ptr, first.len) };
    assert_eq!(got, &[4, 8]);

    quicbit_messages_free(replies);
    quicbit_pip_client_free(client);
    quicbit_pip_server_free(server);
    quicbit_node_free(node);
}

#[test]
fn c_abi_req_res_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let identity = cstr(&unique("ffi-calc"));
    let topic = cstr("ffi/double");

    let node = quicbit_node_new(identity.as_ptr(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

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

#[test]
fn c_abi_endpoint_addr_peer_and_stats_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let server_identity = cstr(&unique("ffi-addr-server"));
    let client_identity = cstr(&unique("ffi-addr-client"));
    let topic = cstr("ffi/addr_double");

    let server_node = quicbit_node_new(server_identity.as_ptr(), true);
    let client_node = quicbit_node_new(client_identity.as_ptr(), true);
    assert!(
        !server_node.is_null(),
        "server node failed: {}",
        last_error()
    );
    assert!(
        !client_node.is_null(),
        "client node failed: {}",
        last_error()
    );

    let addr = quicbit_node_endpoint_addr(server_node);
    assert!(!addr.is_null(), "endpoint addr failed: {}", last_error());
    let node_stats = quicbit_node_stats(server_node);
    assert_eq!(node_stats.cached_peers, 0);

    let server = quicbit_req_server_new(server_node, topic.as_ptr());
    let client = quicbit_req_client_new(client_node, addr, topic.as_ptr());
    assert!(
        !server.is_null() && !client.is_null(),
        "endpoint setup failed: {}",
        last_error()
    );

    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut QuicbitReqServer;
        quicbit_req_server_serve_one(server, 3000, Some(double_handler), ptr::null_mut())
    });

    let request = [4u8, 5];
    let mut response: *mut QuicbitMessage = ptr::null_mut();
    assert!(quicbit_req_client_call(
        client,
        11,
        request.as_ptr(),
        request.len(),
        &mut response,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert_eq!(quicbit_message_kind(response), 11);
    let bytes = quicbit_message_data(response);
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &[8, 10]);
    let _client_stats = quicbit_req_client_stats(client);
    let _server_stats = quicbit_req_server_stats(server);

    quicbit_message_free(response);
    quicbit_req_client_free(client);
    quicbit_req_server_free(server);
    quicbit_string_free(addr);
    quicbit_node_free(client_node);
    quicbit_node_free(server_node);
}

#[test]
fn c_abi_polling_and_session_handles_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();

    // req/res explicit polling.
    let req_identity = cstr(&unique("ffi-req-poll"));
    let req_topic = cstr("ffi/poll_req");
    let req_node = quicbit_node_new(req_identity.as_ptr(), true);
    assert!(!req_node.is_null(), "req node failed: {}", last_error());
    let req_server = quicbit_req_server_new(req_node, req_topic.as_ptr());
    let req_client = quicbit_req_client_new(req_node, req_identity.as_ptr(), req_topic.as_ptr());
    assert!(!req_server.is_null() && !req_client.is_null());
    let req_server_addr = req_server as usize;
    let req_thread = std::thread::spawn(move || {
        let server = req_server_addr as *mut QuicbitReqServer;
        let mut pending: *mut QuicbitPendingReq = ptr::null_mut();
        let rc = quicbit_req_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "req take failed: {}", last_error());
        let request = quicbit_pending_req_request(pending);
        assert_eq!(quicbit_message_kind(request), 70);
        assert_eq!(message_bytes(request), b"\x03");
        let reply = [6u8];
        assert!(quicbit_pending_req_reply(
            pending,
            71,
            reply.as_ptr(),
            reply.len()
        ));
        quicbit_pending_req_free(pending);
    });
    let mut response: *mut QuicbitMessage = ptr::null_mut();
    let request = [3u8];
    assert!(quicbit_req_client_call(
        req_client,
        70,
        request.as_ptr(),
        request.len(),
        &mut response,
    ));
    req_thread.join().unwrap();
    assert_eq!(quicbit_message_kind(response), 71);
    assert_eq!(message_bytes(response), b"\x06");
    quicbit_message_free(response);
    quicbit_req_client_free(req_client);
    quicbit_req_server_free(req_server);
    quicbit_node_free(req_node);

    // que/ans explicit polling.
    let que_identity = cstr(&unique("ffi-que-poll"));
    let que_topic = cstr("ffi/poll_que");
    let que_node = quicbit_node_new(que_identity.as_ptr(), true);
    assert!(!que_node.is_null(), "que node failed: {}", last_error());
    let ans_server = quicbit_ans_server_new(que_node, que_topic.as_ptr());
    let que_client = quicbit_que_client_new(que_node, que_identity.as_ptr(), que_topic.as_ptr());
    assert!(!ans_server.is_null() && !que_client.is_null());
    let ans_server_addr = ans_server as usize;
    let que_thread = std::thread::spawn(move || {
        let server = ans_server_addr as *mut QuicbitAnsServer;
        let mut pending: *mut QuicbitPendingQue = ptr::null_mut();
        let rc = quicbit_ans_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "ans take failed: {}", last_error());
        let request = quicbit_pending_que_request(pending);
        assert_eq!(quicbit_message_kind(request), 80);
        assert_eq!(message_bytes(request), b"\x05");
        let a = [5u8];
        let b = [6u8];
        assert!(quicbit_pending_que_send(pending, 81, a.as_ptr(), a.len()));
        assert!(quicbit_pending_que_send(pending, 81, b.as_ptr(), b.len()));
        assert!(quicbit_pending_que_finish(pending));
        quicbit_pending_que_free(pending);
    });
    let mut answers: *mut QuicbitMessages = ptr::null_mut();
    let query = [5u8];
    assert!(quicbit_que_client_send(
        que_client,
        80,
        query.as_ptr(),
        query.len(),
        &mut answers,
    ));
    que_thread.join().unwrap();
    assert_eq!(quicbit_messages_len(answers), 2);
    assert_eq!(quicbit_messages_kind_at(answers, 0), 81);
    assert_eq!(quicbit_messages_kind_at(answers, 1), 81);
    quicbit_messages_free(answers);
    quicbit_que_client_free(que_client);
    quicbit_ans_server_free(ans_server);
    quicbit_node_free(que_node);

    // put/ack interactive client upload.
    let put_identity = cstr(&unique("ffi-put-open"));
    let put_topic = cstr("ffi/open_put");
    let put_node = quicbit_node_new(put_identity.as_ptr(), true);
    assert!(!put_node.is_null(), "put node failed: {}", last_error());
    let ack_server = quicbit_ack_server_new(put_node, put_topic.as_ptr());
    let put_client = quicbit_put_client_new(put_node, put_identity.as_ptr(), put_topic.as_ptr());
    assert!(!ack_server.is_null() && !put_client.is_null());
    let ack_server_addr = ack_server as usize;
    let put_thread = std::thread::spawn(move || {
        let server = ack_server_addr as *mut QuicbitAckServer;
        let mut puts: *mut QuicbitPuts = ptr::null_mut();
        let rc = quicbit_ack_server_take(server, 3000, &mut puts);
        assert_eq!(rc, 1, "ack take failed: {}", last_error());
        let mut sum = 0u8;
        let mut msg: *mut QuicbitMessage = ptr::null_mut();
        while quicbit_puts_next(puts, &mut msg) == 1 {
            for byte in message_bytes(msg) {
                sum = sum.wrapping_add(byte);
            }
            quicbit_message_free(msg);
        }
        let ack = [sum];
        assert!(quicbit_puts_ack(puts, 99, ack.as_ptr(), ack.len()));
        quicbit_puts_free(puts);
    });
    let mut upload: *mut QuicbitPutUpload = ptr::null_mut();
    assert!(quicbit_put_client_open(put_client, &mut upload));
    let a = [1u8, 2];
    let b = [3u8];
    assert!(quicbit_put_upload_send(upload, 90, a.as_ptr(), a.len()));
    assert!(quicbit_put_upload_send(upload, 90, b.as_ptr(), b.len()));
    let mut ack: *mut QuicbitMessage = ptr::null_mut();
    assert!(quicbit_put_upload_finish(upload, &mut ack));
    put_thread.join().unwrap();
    assert_eq!(quicbit_message_kind(ack), 99);
    assert_eq!(message_bytes(ack), b"\x06");
    quicbit_message_free(ack);
    quicbit_put_upload_free(upload);
    quicbit_put_client_free(put_client);
    quicbit_ack_server_free(ack_server);
    quicbit_node_free(put_node);

    // pip interactive client and server sessions.
    let pip_identity = cstr(&unique("ffi-pip-open"));
    let pip_topic = cstr("ffi/open_pip");
    let pip_node = quicbit_node_new(pip_identity.as_ptr(), true);
    assert!(!pip_node.is_null(), "pip node failed: {}", last_error());
    let pip_server = quicbit_pip_server_new(pip_node, pip_topic.as_ptr());
    let pip_client = quicbit_pip_client_new(pip_node, pip_identity.as_ptr(), pip_topic.as_ptr());
    assert!(!pip_server.is_null() && !pip_client.is_null());
    let pip_server_addr = pip_server as usize;
    let pip_thread = std::thread::spawn(move || {
        let server = pip_server_addr as *mut QuicbitPipServer;
        let mut pending: *mut QuicbitPendingPip = ptr::null_mut();
        let rc = quicbit_pip_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "pip server take failed: {}", last_error());
        let mut msg: *mut QuicbitMessage = ptr::null_mut();
        assert_eq!(quicbit_pending_pip_next(pending, &mut msg), 1);
        assert_eq!(quicbit_message_kind(msg), 100);
        assert_eq!(message_bytes(msg), b"\x04");
        let reply = [8u8];
        assert!(quicbit_pending_pip_send(
            pending,
            101,
            reply.as_ptr(),
            reply.len(),
        ));
        quicbit_message_free(msg);
        assert_eq!(quicbit_pending_pip_next(pending, &mut msg), 0);
        assert!(quicbit_pending_pip_finish_send(pending));
        quicbit_pending_pip_free(pending);
    });
    let mut pip: *mut QuicbitPip = ptr::null_mut();
    assert!(quicbit_pip_client_open(pip_client, &mut pip));
    let input = [4u8];
    assert!(quicbit_pip_send(pip, 100, input.as_ptr(), input.len()));
    assert!(quicbit_pip_finish_send(pip));
    let mut reply: *mut QuicbitMessage = ptr::null_mut();
    assert_eq!(quicbit_pip_next(pip, &mut reply), 1);
    assert_eq!(quicbit_message_kind(reply), 101);
    assert_eq!(message_bytes(reply), b"\x08");
    quicbit_message_free(reply);
    assert_eq!(quicbit_pip_next(pip, &mut reply), 0);
    pip_thread.join().unwrap();
    quicbit_pip_free(pip);
    quicbit_pip_client_free(pip_client);
    quicbit_pip_server_free(pip_server);
    quicbit_node_free(pip_node);
}
