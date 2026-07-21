//! Exercises the C ABI (`peerbus::ffi`) end-to-end from Rust, so the
//! foreign-language surface is covered by `cargo test` / CI rather than
//! only by the hand-run `examples/c_abi/pubsub.c` demo.

use std::ffi::{CString, c_void};
use std::ptr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use peerbus::ffi::*;

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
    let ptr = peerbus_last_error_message();
    if ptr.is_null() {
        "<no ffi error>".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

fn message_bytes(message: *const PeerbusMessage) -> Vec<u8> {
    let bytes = peerbus_message_data(message);
    if bytes.ptr.is_null() || bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    }
}

fn datapod_message_wire(message: *const PeerbusDatapodMessage) -> Vec<u8> {
    let bytes = peerbus_datapod_message_wire(message);
    if bytes.ptr.is_null() || bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    }
}

fn sample_bytes(sample: *const PeerbusSample) -> Vec<u8> {
    let bytes = peerbus_sample_data(sample);
    if bytes.ptr.is_null() || bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    }
}

fn datapod_sample_wire(sample: *const PeerbusDatapodSample) -> Vec<u8> {
    let bytes = peerbus_datapod_sample_wire(sample);
    if bytes.ptr.is_null() || bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    }
}

fn datapod_messages_wire_at(messages: *const PeerbusDatapodMessages, index: usize) -> Vec<u8> {
    let bytes = peerbus_datapod_messages_wire_at(messages, index);
    if bytes.ptr.is_null() || bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    }
}

fn datapod_answers_wire_at(answers: *const PeerbusDatapodAnswers, index: usize) -> Vec<u8> {
    let bytes = peerbus_datapod_answers_wire_at(answers, index);
    if bytes.ptr.is_null() || bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec()
    }
}

fn grid_wire(rows: u32, cols: u32, payload: Vec<u8>) -> datapod::WireMessage {
    datapod::to_wire_message(&datapod::Grid::new(
        rows,
        cols,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        payload,
    ))
}

fn assert_grid_message(type_hash: u64, wire: &[u8], rows: u32, cols: u32, payload: &[u8]) {
    let view = datapod::dynamic::view_message(type_hash, wire).unwrap();
    assert_eq!(view.get_u32("rows").unwrap(), rows);
    assert_eq!(view.get_u32("cols").unwrap(), cols);
    assert_eq!(view.payload(), payload);
}

#[test]
fn c_header_exposes_plan_qos_delivery_constants() {
    let header = include_str!("../include/peerbus.h");
    assert!(header.contains("#define PEERBUS_DELIVERY_RELIABLE 0"));
    assert!(header.contains("#define PEERBUS_DELIVERY_LATEST 1"));
    assert!(header.contains("#define PEERBUS_DELIVERY_BEST_EFFORT 2"));
    // The QoS delivery field is a plain integer, not a repr(C) enum: an
    // out-of-range value from C must be validated rather than materialized as
    // an invalid enum discriminant (UB). Guard against regressing to the enum.
    assert!(!header.contains("PeerbusDeliveryPolicy"));
    assert!(header.contains("uint32_t delivery;"));
    assert!(header.contains("uint32_t max_publishers;"));
    assert!(header.contains("uint32_t max_subscribers;"));
}

#[test]
fn c_qos_zero_numeric_fields_keep_policy_defaults() {
    // Zero numeric fields mean "use the policy default"; only `delivery` and
    // `priority` are carried through. Exercises the new fallible u32-based
    // conversion (`topic_qos_from_c`) via its `#[doc(hidden)]` test shim.
    let qos = __peerbus_topic_qos_from_c(PeerbusTopicQos {
        delivery: PEERBUS_DELIVERY_LATEST,
        max_message_bytes: 0,
        max_inflight_bytes: 0,
        chunk_bytes: 0,
        subscriber_queue: 0,
        priority: 7,
    })
    .expect("a valid delivery policy converts");

    assert_eq!(qos.delivery, peerbus::DeliveryPolicy::Latest);
    assert_eq!(
        qos.max_message_bytes,
        peerbus::TopicQos::latest().max_message_bytes
    );
    assert_eq!(
        qos.max_inflight_bytes,
        peerbus::TopicQos::latest().max_inflight_bytes
    );
    assert_eq!(qos.chunk_bytes, peerbus::TopicQos::latest().chunk_bytes);
    assert_eq!(
        qos.subscriber_queue,
        peerbus::TopicQos::latest().subscriber_queue
    );
    assert_eq!(qos.priority, 7);

    // An out-of-range delivery discriminant is rejected instead of being
    // materialized as an invalid enum (which would be UB).
    assert!(
        __peerbus_topic_qos_from_c(PeerbusTopicQos {
            delivery: 7,
            max_message_bytes: 0,
            max_inflight_bytes: 0,
            chunk_bytes: 0,
            subscriber_queue: 0,
            priority: 0,
        })
        .is_none()
    );
}

#[test]
fn c_abi_null_timeout_and_ownership_edges() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();

    let empty = peerbus_message_data(ptr::null());
    assert!(empty.ptr.is_null());
    assert_eq!(empty.len, 0);
    assert_eq!(peerbus_message_kind(ptr::null()), 0);

    let mut owned_input = [1_u8, 2, 3];
    let message = peerbus_message_new(77, owned_input.as_ptr(), owned_input.len());
    assert!(!message.is_null());
    owned_input.fill(9);
    assert_eq!(peerbus_message_kind(message), 77);
    assert_eq!(message_bytes(message), [1, 2, 3]);
    peerbus_message_free(message);
    peerbus_message_free(ptr::null_mut());

    let topic = cstr(&unique("ffi-null-timeout-topic"));
    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    assert!(peerbus_publisher_new(ptr::null(), topic.as_ptr()).is_null());
    assert!(
        last_error().contains("null node"),
        "expected null-node publisher error, got: {}",
        last_error()
    );

    let server = peerbus_req_server_new(node, topic.as_ptr());
    assert!(!server.is_null(), "req server failed: {}", last_error());
    let mut pending: *mut PeerbusPendingReq = ptr::null_mut();
    assert_eq!(peerbus_req_server_take(server, 1, &mut pending), 0);
    assert!(pending.is_null());

    assert_eq!(
        peerbus_req_server_take(ptr::null_mut(), 1, &mut pending),
        -1
    );
    assert!(
        last_error().contains("null req server"),
        "expected null server error, got: {}",
        last_error()
    );

    let mut out: *mut PeerbusMessage = ptr::null_mut();
    assert!(!peerbus_req_client_call(
        ptr::null_mut(),
        1,
        ptr::null(),
        0,
        &mut out
    ));
    assert!(
        last_error().contains("null client"),
        "expected null client error, got: {}",
        last_error()
    );

    peerbus_req_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_pubsub_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/topic");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    // Peers are addressed only by their hex postcard-encoded endpoint addr.
    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let publisher = peerbus_publisher_new(node, topic.as_ptr());
    let subscriber = peerbus_subscriber_new(node, peer, topic.as_ptr());
    peerbus_string_free(peer);
    assert!(!publisher.is_null() && !subscriber.is_null());

    let payload = [0xDEu8, 0xAD, 0xBE, 0xEF];
    assert!(peerbus_publisher_send(
        publisher,
        7,
        payload.as_ptr(),
        payload.len()
    ));

    let mut msg: *mut PeerbusMessage = ptr::null_mut();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut rc = 0;
    while rc == 0 && Instant::now() < deadline {
        rc = peerbus_subscriber_take(subscriber, &mut msg as *mut *mut PeerbusMessage);
        if rc == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(rc, 1, "expected a message");
    assert!(!msg.is_null());

    assert_eq!(peerbus_message_kind(msg), 7);
    let bytes = peerbus_message_data(msg);
    // SAFETY: view valid until peerbus_message_free.
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &payload);
    assert_eq!(peerbus_publisher_stats(publisher).published, 1);
    assert_eq!(peerbus_subscriber_stats(subscriber).received, 1);

    peerbus_message_free(msg);
    peerbus_subscriber_free(subscriber);
    peerbus_publisher_free(publisher);
    peerbus_node_free(node);
}

#[test]
fn c_abi_pubsub_zero_copy_sample_view_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/topic_sample");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let publisher = peerbus_publisher_new(node, topic.as_ptr());
    let subscriber = peerbus_subscriber_new(node, peer, topic.as_ptr());
    peerbus_string_free(peer);
    assert!(!publisher.is_null() && !subscriber.is_null());

    let payload = b"borrowed-sample";
    assert!(peerbus_publisher_send(
        publisher,
        77,
        payload.as_ptr(),
        payload.len()
    ));

    let mut sample: *mut PeerbusSample = ptr::null_mut();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut rc = 0;
    while rc == 0 && Instant::now() < deadline {
        rc = peerbus_subscriber_take_sample(subscriber, &mut sample);
        if rc == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(rc, 1, "expected a borrowed sample: {}", last_error());
    assert!(!sample.is_null());
    assert_eq!(peerbus_sample_kind(sample), 77);
    assert_eq!(sample_bytes(sample), payload);

    peerbus_sample_free(sample);
    peerbus_subscriber_free(subscriber);
    peerbus_publisher_free(publisher);
    peerbus_node_free(node);
}

#[test]
fn c_abi_datapod_pubsub_zero_copy_sample_view_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/datapod_sample");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let qos = peerbus_topic_qos_reliable();
    let publisher = peerbus_datapod_publisher_new_with_qos(node, topic.as_ptr(), qos);
    let subscriber =
        peerbus_datapod_subscriber_new_with_qos(node, peer, topic.as_ptr(), qos);
    peerbus_string_free(peer);
    assert!(!publisher.is_null() && !subscriber.is_null());

    let wire = grid_wire(1, 2, vec![9; 8]);
    assert!(peerbus_datapod_publisher_send(
        publisher,
        wire.type_hash,
        wire.bytes.as_ptr(),
        wire.bytes.len()
    ));

    let mut sample: *mut PeerbusDatapodSample = ptr::null_mut();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut rc = 0;
    while rc == 0 && Instant::now() < deadline {
        rc = peerbus_datapod_subscriber_take_sample(subscriber, &mut sample);
        if rc == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(
        rc,
        1,
        "expected a borrowed datapod sample: {}",
        last_error()
    );
    assert!(!sample.is_null());
    assert_eq!(peerbus_datapod_sample_type_hash(sample), wire.type_hash);
    let borrowed_wire = datapod_sample_wire(sample);
    assert_eq!(borrowed_wire, wire.bytes);
    assert_grid_message(
        peerbus_datapod_sample_type_hash(sample),
        &borrowed_wire,
        1,
        2,
        &[9; 8],
    );

    peerbus_datapod_sample_free(sample);
    peerbus_datapod_subscriber_free(subscriber);
    peerbus_datapod_publisher_free(publisher);
    peerbus_node_free(node);
}

unsafe extern "C" fn double_handler(
    _ctx: *mut c_void,
    kind: u64,
    data: *const u8,
    len: usize,
    responder: *mut PeerbusResponder,
) {
    // SAFETY: ffi contract — `len` valid bytes at `data`.
    let input = if data.is_null() || len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };
    let doubled: Vec<u8> = input.iter().map(|b| b.wrapping_mul(2)).collect();
    peerbus_responder_set(responder, kind, doubled.as_ptr(), doubled.len());
}

unsafe extern "C" fn range_handler(
    _ctx: *mut c_void,
    kind: u64,
    data: *const u8,
    len: usize,
    responder: *mut PeerbusAnsResponder,
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
        peerbus_ans_responder_send(responder, kind, value.as_ptr(), value.len());
    }
}

#[test]
fn c_abi_que_ans_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/range");
    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let server = peerbus_ans_server_new(node, topic.as_ptr());
    let client = peerbus_que_client_new(node, peer, topic.as_ptr());
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "que setup failed: {}",
        last_error()
    );

    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusAnsServer;
        peerbus_ans_server_serve_one(server, 3000, Some(range_handler), ptr::null_mut())
    });

    let request = [10u8, 3];
    let mut answers: *mut PeerbusAnswers = ptr::null_mut();
    assert!(peerbus_que_client_send(
        client,
        12,
        request.as_ptr(),
        request.len(),
        &mut answers,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert!(!answers.is_null());
    assert_eq!(peerbus_answers_len(answers), 3);
    for i in 0..3 {
        assert_eq!(peerbus_answers_kind_at(answers, i), 12);
        let bytes = peerbus_answers_data_at(answers, i);
        let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
        assert_eq!(got, &[10 + i as u8]);
    }
    peerbus_answers_free(answers);
    peerbus_que_client_free(client);
    peerbus_ans_server_free(server);
    peerbus_node_free(node);
}

unsafe extern "C" fn upload_handler(
    _ctx: *mut c_void,
    items: *const PeerbusMessages,
    responder: *mut PeerbusResponder,
) {
    let mut sum = 0u8;
    for i in 0..peerbus_messages_len(items) {
        let bytes = peerbus_messages_data_at(items, i);
        let data = if bytes.ptr.is_null() || bytes.len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }
        };
        sum = sum.wrapping_add(data.iter().copied().sum::<u8>());
    }
    let ack = [sum];
    peerbus_responder_set(responder, 99, ack.as_ptr(), ack.len());
}

#[test]
fn c_abi_put_ack_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/upload");
    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());
    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());
    let server = peerbus_ack_server_new(node, topic.as_ptr());
    let client = peerbus_put_client_new(node, peer, topic.as_ptr());
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "put setup failed: {}",
        last_error()
    );
    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusAckServer;
        peerbus_ack_server_serve_one(server, 3000, Some(upload_handler), ptr::null_mut())
    });

    let a = [1u8, 2];
    let b = [3u8, 4];
    let items = [
        PeerbusRawMessage {
            kind: 1,
            data: PeerbusBytes {
                ptr: a.as_ptr(),
                len: a.len(),
            },
        },
        PeerbusRawMessage {
            kind: 1,
            data: PeerbusBytes {
                ptr: b.as_ptr(),
                len: b.len(),
            },
        },
    ];
    let mut ack: *mut PeerbusMessage = ptr::null_mut();
    assert!(peerbus_put_client_put(
        client,
        items.as_ptr(),
        items.len(),
        &mut ack,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert_eq!(peerbus_message_kind(ack), 99);
    let bytes = peerbus_message_data(ack);
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &[10]);

    peerbus_message_free(ack);
    peerbus_put_client_free(client);
    peerbus_ack_server_free(server);
    peerbus_node_free(node);
}

unsafe extern "C" fn pip_handler(
    _ctx: *mut c_void,
    items: *const PeerbusMessages,
    responder: *mut PeerbusMessageResponder,
) {
    for i in 0..peerbus_messages_len(items) {
        let kind = peerbus_messages_kind_at(items, i);
        let bytes = peerbus_messages_data_at(items, i);
        let data = if bytes.ptr.is_null() || bytes.len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }
        };
        let doubled: Vec<u8> = data.iter().map(|b| b.wrapping_mul(2)).collect();
        peerbus_message_responder_send(responder, kind, doubled.as_ptr(), doubled.len());
    }
}

#[test]
fn c_abi_pip_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/session");
    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());
    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());
    let server = peerbus_pip_server_new(node, topic.as_ptr());
    let client = peerbus_pip_client_new(node, peer, topic.as_ptr());
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "pip setup failed: {}",
        last_error()
    );
    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusPipServer;
        peerbus_pip_server_serve_one(server, 3000, Some(pip_handler), ptr::null_mut())
    });

    let a = [2u8, 4];
    let b = [5u8];
    let items = [
        PeerbusRawMessage {
            kind: 7,
            data: PeerbusBytes {
                ptr: a.as_ptr(),
                len: a.len(),
            },
        },
        PeerbusRawMessage {
            kind: 8,
            data: PeerbusBytes {
                ptr: b.as_ptr(),
                len: b.len(),
            },
        },
    ];
    let mut replies: *mut PeerbusMessages = ptr::null_mut();
    assert!(peerbus_pip_client_exchange(
        client,
        items.as_ptr(),
        items.len(),
        &mut replies,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert_eq!(peerbus_messages_len(replies), 2);
    assert_eq!(peerbus_messages_kind_at(replies, 0), 7);
    let first = peerbus_messages_data_at(replies, 0);
    let got = unsafe { std::slice::from_raw_parts(first.ptr, first.len) };
    assert_eq!(got, &[4, 8]);

    peerbus_messages_free(replies);
    peerbus_pip_client_free(client);
    peerbus_pip_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_req_res_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/double");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let server = peerbus_req_server_new(node, topic.as_ptr());
    let client = peerbus_req_client_new(node, peer, topic.as_ptr());
    peerbus_string_free(peer);
    assert!(!server.is_null() && !client.is_null());

    // Move the server pointer into the serve thread (raw ptrs aren't Send;
    // the test owns it exclusively, so a usize hop is sound here).
    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusReqServer;
        peerbus_req_server_serve_one(server, 3000, Some(double_handler), ptr::null_mut())
    });

    let request = [1u8, 2, 3];
    let mut response: *mut PeerbusMessage = ptr::null_mut();
    let ok = peerbus_req_client_call(
        client,
        4,
        request.as_ptr(),
        request.len(),
        &mut response as *mut *mut PeerbusMessage,
    );
    assert!(ok, "call failed");
    assert!(!response.is_null());

    assert_eq!(peerbus_message_kind(response), 4);
    let bytes = peerbus_message_data(response);
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &[2u8, 4, 6]);

    let served = handle.join().unwrap();
    assert_eq!(served, 1, "server should have served one request");

    peerbus_message_free(response);
    peerbus_req_client_free(client);
    peerbus_req_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_datapod_req_res_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/datapod_req");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let server =
        peerbus_datapod_req_server_new_with_qos(node, topic.as_ptr(), peerbus_topic_qos_reliable());
    let client = peerbus_datapod_req_client_new_with_qos(
        node,
        peer,
        topic.as_ptr(),
        peerbus_topic_qos_reliable(),
    );
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "datapod req setup failed: {}",
        last_error()
    );

    let response = datapod::to_wire_message(&datapod::Grid::new(
        1,
        2,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (10_u8..18).collect(),
    ));

    let server_addr = server as usize;
    let response_type_hash = response.type_hash;
    let response_bytes = response.bytes.clone();
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusDatapodReqServer;
        let mut pending: *mut PeerbusPendingDatapodReq = ptr::null_mut();
        let rc = peerbus_datapod_req_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "datapod req server take failed");
        assert!(!pending.is_null());
        let request = peerbus_pending_datapod_req_request(pending);
        assert!(!request.is_null());
        let type_hash = peerbus_datapod_message_type_hash(request);
        let wire = datapod_message_wire(request);
        let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
        assert_eq!(view.get_u32("rows").unwrap(), 2);
        assert_eq!(view.get_u32("cols").unwrap(), 3);
        assert!(peerbus_pending_datapod_req_reply(
            pending,
            response_type_hash,
            response_bytes.as_ptr(),
            response_bytes.len(),
        ));
        peerbus_pending_datapod_req_free(pending);
    });

    let request = datapod::to_wire_message(&datapod::Grid::new(
        2,
        3,
        datapod::Encoding::Rgba8,
        0.25,
        false,
        datapod::Pose::default(),
        (0_u8..24).collect(),
    ));
    let mut response: *mut PeerbusDatapodMessage = ptr::null_mut();
    assert!(peerbus_datapod_req_client_call(
        client,
        request.type_hash,
        request.bytes.as_ptr(),
        request.bytes.len(),
        &mut response,
    ));
    assert!(!response.is_null());
    let type_hash = peerbus_datapod_message_type_hash(response);
    let wire = datapod_message_wire(response);
    let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
    assert_eq!(view.get_u32("rows").unwrap(), 1);
    assert_eq!(view.get_u32("cols").unwrap(), 2);
    assert_eq!(view.payload(), &(10_u8..18).collect::<Vec<_>>());

    handle.join().unwrap();
    peerbus_datapod_message_free(response);
    peerbus_datapod_req_client_free(client);
    peerbus_datapod_req_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_datapod_que_ans_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/datapod_que");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let server =
        peerbus_datapod_ans_server_new_with_qos(node, topic.as_ptr(), peerbus_topic_qos_reliable());
    let client = peerbus_datapod_que_client_new_with_qos(
        node,
        peer,
        topic.as_ptr(),
        peerbus_topic_qos_reliable(),
    );
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "datapod que setup failed: {}",
        last_error()
    );

    let ans_a = datapod::to_wire_message(&datapod::Grid::new(
        1,
        1,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (0_u8..4).collect(),
    ));
    let ans_b = datapod::to_wire_message(&datapod::Grid::new(
        1,
        2,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (10_u8..18).collect(),
    ));

    let server_addr = server as usize;
    let ans_a_hash = ans_a.type_hash;
    let ans_a_bytes = ans_a.bytes.clone();
    let ans_b_hash = ans_b.type_hash;
    let ans_b_bytes = ans_b.bytes.clone();
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusDatapodAnsServer;
        let mut pending: *mut PeerbusPendingDatapodQue = ptr::null_mut();
        let rc = peerbus_datapod_ans_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "datapod ans server take failed");
        assert!(!pending.is_null());
        let request = peerbus_pending_datapod_que_request(pending);
        assert!(!request.is_null());
        let type_hash = peerbus_datapod_message_type_hash(request);
        let wire = datapod_message_wire(request);
        let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
        assert_eq!(view.get_u32("rows").unwrap(), 2);
        assert_eq!(view.get_u32("cols").unwrap(), 3);
        assert!(peerbus_pending_datapod_que_send(
            pending,
            ans_a_hash,
            ans_a_bytes.as_ptr(),
            ans_a_bytes.len(),
        ));
        assert!(peerbus_pending_datapod_que_send(
            pending,
            ans_b_hash,
            ans_b_bytes.as_ptr(),
            ans_b_bytes.len(),
        ));
        assert!(peerbus_pending_datapod_que_finish(pending));
        peerbus_pending_datapod_que_free(pending);
    });

    let que = datapod::to_wire_message(&datapod::Grid::new(
        2,
        3,
        datapod::Encoding::Rgba8,
        0.25,
        false,
        datapod::Pose::default(),
        (0_u8..24).collect(),
    ));
    let mut answers: *mut PeerbusDatapodAnswers = ptr::null_mut();
    assert!(peerbus_datapod_que_client_send(
        client,
        que.type_hash,
        que.bytes.as_ptr(),
        que.bytes.len(),
        &mut answers,
    ));
    assert!(!answers.is_null());
    assert_eq!(peerbus_datapod_answers_len(answers), 2);
    let first_hash = peerbus_datapod_answers_type_hash_at(answers, 0);
    let second_hash = peerbus_datapod_answers_type_hash_at(answers, 1);
    let first_wire = datapod_answers_wire_at(answers, 0);
    let second_wire = datapod_answers_wire_at(answers, 1);
    let first = datapod::dynamic::view_message(first_hash, &first_wire).unwrap();
    let second = datapod::dynamic::view_message(second_hash, &second_wire).unwrap();
    assert_eq!(first.get_u32("cols").unwrap(), 1);
    assert_eq!(second.get_u32("cols").unwrap(), 2);
    assert_eq!(second.payload(), &(10_u8..18).collect::<Vec<_>>());

    handle.join().unwrap();
    peerbus_datapod_answers_free(answers);
    peerbus_datapod_que_client_free(client);
    peerbus_datapod_ans_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_datapod_put_ack_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/datapod_put");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let server =
        peerbus_datapod_ack_server_new_with_qos(node, topic.as_ptr(), peerbus_topic_qos_reliable());
    let client = peerbus_datapod_put_client_new_with_qos(
        node,
        peer,
        topic.as_ptr(),
        peerbus_topic_qos_reliable(),
    );
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "datapod put setup failed: {}",
        last_error()
    );

    let ack = datapod::to_wire_message(&datapod::Grid::new(
        4,
        1,
        datapod::Encoding::U8,
        1.0,
        false,
        datapod::Pose::default(),
        (200_u8..204).collect(),
    ));
    let server_addr = server as usize;
    let ack_hash = ack.type_hash;
    let ack_bytes = ack.bytes.clone();
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusDatapodAckServer;
        let mut puts: *mut PeerbusDatapodPuts = ptr::null_mut();
        let rc = peerbus_datapod_ack_server_take(server, 3000, &mut puts);
        assert_eq!(rc, 1, "datapod ack server take failed");
        assert!(!puts.is_null());

        let mut got_cols = Vec::new();
        loop {
            let mut msg: *mut PeerbusDatapodMessage = ptr::null_mut();
            let rc = peerbus_datapod_puts_next(puts, &mut msg);
            if rc == 0 {
                break;
            }
            assert_eq!(rc, 1);
            assert!(!msg.is_null());
            let type_hash = peerbus_datapod_message_type_hash(msg);
            let wire = datapod_message_wire(msg);
            let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
            got_cols.push(view.get_u32("cols").unwrap());
            peerbus_datapod_message_free(msg);
        }
        assert_eq!(got_cols, vec![1, 2]);
        assert!(peerbus_datapod_puts_ack(
            puts,
            ack_hash,
            ack_bytes.as_ptr(),
            ack_bytes.len(),
        ));
        peerbus_datapod_puts_free(puts);
    });

    let put_a = datapod::to_wire_message(&datapod::Grid::new(
        1,
        1,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (0_u8..4).collect(),
    ));
    let put_b = datapod::to_wire_message(&datapod::Grid::new(
        1,
        2,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (10_u8..18).collect(),
    ));
    let raw = [
        PeerbusDatapodRawMessage {
            type_hash: put_a.type_hash,
            wire: PeerbusBytes {
                ptr: put_a.bytes.as_ptr(),
                len: put_a.bytes.len(),
            },
        },
        PeerbusDatapodRawMessage {
            type_hash: put_b.type_hash,
            wire: PeerbusBytes {
                ptr: put_b.bytes.as_ptr(),
                len: put_b.bytes.len(),
            },
        },
    ];
    let mut response: *mut PeerbusDatapodMessage = ptr::null_mut();
    assert!(peerbus_datapod_put_client_put(
        client,
        raw.as_ptr(),
        raw.len(),
        &mut response,
    ));
    assert!(!response.is_null());
    let type_hash = peerbus_datapod_message_type_hash(response);
    let wire = datapod_message_wire(response);
    let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
    assert_eq!(view.get_u32("rows").unwrap(), 4);
    assert_eq!(view.payload(), &(200_u8..204).collect::<Vec<_>>());

    handle.join().unwrap();
    peerbus_datapod_message_free(response);
    peerbus_datapod_put_client_free(client);
    peerbus_datapod_ack_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_datapod_put_sender_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/datapod_put_sender");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let server =
        peerbus_datapod_ack_server_new_with_qos(node, topic.as_ptr(), peerbus_topic_qos_reliable());
    let client = peerbus_datapod_put_client_new_with_qos(
        node,
        peer,
        topic.as_ptr(),
        peerbus_topic_qos_reliable(),
    );
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "datapod put sender setup failed: {}",
        last_error()
    );

    let ack = datapod::to_wire_message(&datapod::Grid::new(
        1,
        1,
        datapod::Encoding::U8,
        1.0,
        false,
        datapod::Pose::default(),
        vec![42],
    ));
    let ack_hash = ack.type_hash;
    let ack_bytes = ack.bytes.clone();
    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusDatapodAckServer;
        let mut puts: *mut PeerbusDatapodPuts = ptr::null_mut();
        let rc = peerbus_datapod_ack_server_take(server, 3000, &mut puts);
        assert_eq!(rc, 1, "datapod ack take failed: {}", last_error());
        assert!(!puts.is_null());

        let mut rows = Vec::new();
        loop {
            let mut msg: *mut PeerbusDatapodMessage = ptr::null_mut();
            let rc = peerbus_datapod_puts_next(puts, &mut msg);
            if rc == 0 {
                break;
            }
            assert_eq!(rc, 1);
            assert!(!msg.is_null());
            let type_hash = peerbus_datapod_message_type_hash(msg);
            let wire = datapod_message_wire(msg);
            let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
            rows.push(view.get_u32("rows").unwrap());
            peerbus_datapod_message_free(msg);
        }
        assert_eq!(rows, vec![2, 3]);
        assert!(peerbus_datapod_puts_ack(
            puts,
            ack_hash,
            ack_bytes.as_ptr(),
            ack_bytes.len(),
        ));
        peerbus_datapod_puts_free(puts);
    });

    let put_a = datapod::to_wire_message(&datapod::Grid::new(
        2,
        1,
        datapod::Encoding::U8,
        1.0,
        false,
        datapod::Pose::default(),
        vec![1, 2],
    ));
    let put_b = datapod::to_wire_message(&datapod::Grid::new(
        3,
        1,
        datapod::Encoding::U8,
        1.0,
        false,
        datapod::Pose::default(),
        vec![3, 4, 5],
    ));
    let mut sender: *mut PeerbusDatapodPutSender = ptr::null_mut();
    assert!(peerbus_datapod_put_client_open_sender(client, &mut sender));
    assert!(!sender.is_null());
    assert!(peerbus_datapod_put_sender_send(
        sender,
        put_a.type_hash,
        put_a.bytes.as_ptr(),
        put_a.bytes.len(),
    ));
    assert!(peerbus_datapod_put_sender_send(
        sender,
        put_b.type_hash,
        put_b.bytes.as_ptr(),
        put_b.bytes.len(),
    ));
    let mut response: *mut PeerbusDatapodMessage = ptr::null_mut();
    assert!(peerbus_datapod_put_sender_finish(sender, &mut response));
    assert!(!response.is_null());
    let type_hash = peerbus_datapod_message_type_hash(response);
    let wire = datapod_message_wire(response);
    let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
    assert_eq!(view.get_u32("rows").unwrap(), 1);
    assert_eq!(view.payload(), &[42]);

    handle.join().unwrap();
    peerbus_datapod_message_free(response);
    peerbus_datapod_put_sender_free(sender);
    peerbus_datapod_put_client_free(client);
    peerbus_datapod_ack_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_datapod_pip_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/datapod_pip");

    let node = peerbus_node_new(ptr::null(), true);
    assert!(!node.is_null(), "node_new failed: {}", last_error());

    let peer = peerbus_node_endpoint_addr(node);
    assert!(!peer.is_null(), "endpoint addr failed: {}", last_error());

    let server =
        peerbus_datapod_pip_server_new_with_qos(node, topic.as_ptr(), peerbus_topic_qos_reliable());
    let client = peerbus_datapod_pip_client_new_with_qos(
        node,
        peer,
        topic.as_ptr(),
        peerbus_topic_qos_reliable(),
    );
    peerbus_string_free(peer);
    assert!(
        !server.is_null() && !client.is_null(),
        "datapod pip setup failed: {}",
        last_error()
    );

    let reply_a = datapod::to_wire_message(&datapod::Grid::new(
        1,
        1,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (20_u8..24).collect(),
    ));
    let reply_b = datapod::to_wire_message(&datapod::Grid::new(
        1,
        2,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (30_u8..38).collect(),
    ));
    let server_addr = server as usize;
    let reply_a_hash = reply_a.type_hash;
    let reply_a_bytes = reply_a.bytes.clone();
    let reply_b_hash = reply_b.type_hash;
    let reply_b_bytes = reply_b.bytes.clone();
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusDatapodPipServer;
        let mut pending: *mut PeerbusPendingDatapodPip = ptr::null_mut();
        let rc = peerbus_datapod_pip_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "datapod pip server take failed");
        assert!(!pending.is_null());

        let mut got_cols = Vec::new();
        loop {
            let mut msg: *mut PeerbusDatapodMessage = ptr::null_mut();
            let rc = peerbus_pending_datapod_pip_next(pending, &mut msg);
            if rc == 0 {
                break;
            }
            assert_eq!(rc, 1);
            assert!(!msg.is_null());
            let type_hash = peerbus_datapod_message_type_hash(msg);
            let wire = datapod_message_wire(msg);
            let view = datapod::dynamic::view_message(type_hash, &wire).unwrap();
            got_cols.push(view.get_u32("cols").unwrap());
            peerbus_datapod_message_free(msg);
        }
        assert_eq!(got_cols, vec![1, 2]);
        assert!(peerbus_pending_datapod_pip_send(
            pending,
            reply_a_hash,
            reply_a_bytes.as_ptr(),
            reply_a_bytes.len(),
        ));
        assert!(peerbus_pending_datapod_pip_send(
            pending,
            reply_b_hash,
            reply_b_bytes.as_ptr(),
            reply_b_bytes.len(),
        ));
        assert!(peerbus_pending_datapod_pip_finish_send(pending));
        peerbus_pending_datapod_pip_free(pending);
    });

    let request_a = datapod::to_wire_message(&datapod::Grid::new(
        1,
        1,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (0_u8..4).collect(),
    ));
    let request_b = datapod::to_wire_message(&datapod::Grid::new(
        1,
        2,
        datapod::Encoding::Rgba8,
        0.5,
        false,
        datapod::Pose::default(),
        (10_u8..18).collect(),
    ));
    let raw = [
        PeerbusDatapodRawMessage {
            type_hash: request_a.type_hash,
            wire: PeerbusBytes {
                ptr: request_a.bytes.as_ptr(),
                len: request_a.bytes.len(),
            },
        },
        PeerbusDatapodRawMessage {
            type_hash: request_b.type_hash,
            wire: PeerbusBytes {
                ptr: request_b.bytes.as_ptr(),
                len: request_b.bytes.len(),
            },
        },
    ];
    let mut responses: *mut PeerbusDatapodMessages = ptr::null_mut();
    assert!(peerbus_datapod_pip_client_exchange(
        client,
        raw.as_ptr(),
        raw.len(),
        &mut responses,
    ));
    assert!(!responses.is_null());
    assert_eq!(peerbus_datapod_messages_len(responses), 2);
    let first_wire = datapod_messages_wire_at(responses, 0);
    let second_wire = datapod_messages_wire_at(responses, 1);
    let first = datapod::dynamic::view_message(
        peerbus_datapod_messages_type_hash_at(responses, 0),
        &first_wire,
    )
    .unwrap();
    let second = datapod::dynamic::view_message(
        peerbus_datapod_messages_type_hash_at(responses, 1),
        &second_wire,
    )
    .unwrap();
    assert_eq!(first.get_u32("cols").unwrap(), 1);
    assert_eq!(second.get_u32("cols").unwrap(), 2);
    assert_eq!(second.payload(), &(30_u8..38).collect::<Vec<_>>());

    handle.join().unwrap();
    peerbus_datapod_messages_free(responses);
    peerbus_datapod_pip_client_free(client);
    peerbus_datapod_pip_server_free(server);
    peerbus_node_free(node);
}

#[test]
fn c_abi_endpoint_addr_peer_and_stats_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();
    let topic = cstr("ffi/addr_double");

    // Create the client node first so the server can allowlist it by its
    // hex postcard-encoded endpoint addr (peers are addressed only by id).
    let client_node = peerbus_node_new(ptr::null(), true);
    assert!(
        !client_node.is_null(),
        "client node failed: {}",
        last_error()
    );
    let client_addr = peerbus_node_endpoint_addr(client_node);
    assert!(
        !client_addr.is_null(),
        "client endpoint addr failed: {}",
        last_error()
    );

    let allowed_peers = [client_addr.cast_const()];
    let server_cfg = PeerbusNodeConfig {
        secret_key: ptr::null(),
        no_relay: true,
        allowed_peers: allowed_peers.as_ptr(),
        allowed_peers_len: allowed_peers.len(),
        allow_any_peer: false,
        max_payload_bytes: 0,
        history_depth: 0,
        subscriber_buffer: 0,
        max_publishers: 0,
        max_subscribers: 0,
    };
    let server_node = peerbus_node_new_with_config(server_cfg);
    peerbus_string_free(client_addr);
    assert!(
        !server_node.is_null(),
        "server node failed: {}",
        last_error()
    );

    let addr = peerbus_node_endpoint_addr(server_node);
    assert!(!addr.is_null(), "endpoint addr failed: {}", last_error());
    let node_stats = peerbus_node_stats(server_node);
    assert_eq!(node_stats.cached_peers, 0);

    let server = peerbus_req_server_new(server_node, topic.as_ptr());
    let client = peerbus_req_client_new(client_node, addr, topic.as_ptr());
    assert!(
        !server.is_null() && !client.is_null(),
        "endpoint setup failed: {}",
        last_error()
    );

    let server_addr = server as usize;
    let handle = std::thread::spawn(move || {
        let server = server_addr as *mut PeerbusReqServer;
        peerbus_req_server_serve_one(server, 3000, Some(double_handler), ptr::null_mut())
    });

    let request = [4u8, 5];
    let mut response: *mut PeerbusMessage = ptr::null_mut();
    assert!(peerbus_req_client_call(
        client,
        11,
        request.as_ptr(),
        request.len(),
        &mut response,
    ));
    assert_eq!(handle.join().unwrap(), 1);
    assert_eq!(peerbus_message_kind(response), 11);
    let bytes = peerbus_message_data(response);
    let got = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) };
    assert_eq!(got, &[8, 10]);

    // Path diagnostics are reported for cached Node pub/sub connections.
    // Create a peer-addressed subscriber before the publisher so this same-host
    // test is forced onto the iroh path instead of the local SHM fast path.
    let diag_topic = cstr("ffi/addr_diag_pubsub");
    let diag_sub = peerbus_subscriber_new(client_node, addr, diag_topic.as_ptr());
    let diag_pub = peerbus_publisher_new(server_node, diag_topic.as_ptr());
    assert!(
        !diag_sub.is_null() && !diag_pub.is_null(),
        "diag pub/sub setup failed: {}",
        last_error()
    );
    let diag_payload = b"diag";
    assert!(peerbus_publisher_send(
        diag_pub,
        91,
        diag_payload.as_ptr(),
        diag_payload.len()
    ));
    let mut diag_msg: *mut PeerbusMessage = ptr::null_mut();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut rc = 0;
    while rc == 0 && Instant::now() < deadline {
        rc = peerbus_subscriber_take(diag_sub, &mut diag_msg);
        if rc == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(rc, 1, "expected diag pub/sub message: {}", last_error());
    assert_eq!(peerbus_message_kind(diag_msg), 91);
    assert_eq!(message_bytes(diag_msg), diag_payload);

    let diag = peerbus_node_peer_path_diagnostics(client_node, addr);
    assert!(
        !diag.is_null(),
        "peer path diagnostics missing: {}",
        last_error()
    );
    let peer_ptr = peerbus_peer_path_diagnostics_peer(diag);
    assert!(
        !peer_ptr.is_null(),
        "peer diagnostics did failed: {}",
        last_error()
    );
    let peer = unsafe { std::ffi::CStr::from_ptr(peer_ptr) }
        .to_string_lossy()
        .into_owned();
    // The peer is now reported as the hex-encoded 32-byte endpoint id.
    assert_eq!(peer.len(), 64, "unexpected peer id encoding: {peer}");
    assert!(
        peer.bytes().all(|b| b.is_ascii_hexdigit()),
        "peer id is not hex: {peer}"
    );
    peerbus_string_free(peer_ptr);

    let path_count = peerbus_peer_path_diagnostics_path_count(diag);
    assert!(path_count > 0, "expected at least one iroh path");
    let mut max_datagram_size = 0usize;
    let _has_datagrams =
        peerbus_peer_path_diagnostics_max_datagram_size(diag, &mut max_datagram_size);
    let _send_buffer = peerbus_peer_path_diagnostics_datagram_send_buffer_space(diag);
    let path_id = peerbus_peer_path_diagnostics_path_id(diag, 0);
    assert!(!path_id.is_null(), "path id missing: {}", last_error());
    let remote_addr = peerbus_peer_path_diagnostics_remote_addr(diag, 0);
    assert!(
        !remote_addr.is_null(),
        "path remote addr missing: {}",
        last_error()
    );
    let _selected = peerbus_peer_path_diagnostics_path_selected(diag, 0);
    let _is_ip = peerbus_peer_path_diagnostics_path_is_ip(diag, 0);
    let _is_relay = peerbus_peer_path_diagnostics_path_is_relay(diag, 0);
    let _rtt_ms = peerbus_peer_path_diagnostics_path_rtt_ms(diag, 0);
    let _mtu = peerbus_peer_path_diagnostics_path_current_mtu(diag, 0);
    let _cwnd = peerbus_peer_path_diagnostics_path_cwnd(diag, 0);
    let _lost = peerbus_peer_path_diagnostics_path_lost_packets(diag, 0);
    peerbus_string_free(path_id);
    peerbus_string_free(remote_addr);
    peerbus_peer_path_diagnostics_free(diag);
    peerbus_message_free(diag_msg);
    peerbus_subscriber_free(diag_sub);
    peerbus_publisher_free(diag_pub);

    let _client_stats = peerbus_req_client_stats(client);
    let _server_stats = peerbus_req_server_stats(server);

    peerbus_message_free(response);
    peerbus_req_client_free(client);
    peerbus_req_server_free(server);
    peerbus_string_free(addr);
    peerbus_node_free(client_node);
    peerbus_node_free(server_node);
}

#[test]
fn c_abi_polling_and_session_handles_round_trip() {
    let _guard = FFI_TEST_LOCK.lock().unwrap();

    // req/res explicit polling.
    let req_topic = cstr("ffi/poll_req");
    let req_node = peerbus_node_new(ptr::null(), true);
    assert!(!req_node.is_null(), "req node failed: {}", last_error());
    let req_peer = peerbus_node_endpoint_addr(req_node);
    assert!(!req_peer.is_null(), "req endpoint addr failed: {}", last_error());
    let req_server = peerbus_req_server_new(req_node, req_topic.as_ptr());
    let req_client = peerbus_req_client_new(req_node, req_peer, req_topic.as_ptr());
    peerbus_string_free(req_peer);
    assert!(!req_server.is_null() && !req_client.is_null());
    let req_server_addr = req_server as usize;
    let req_thread = std::thread::spawn(move || {
        let server = req_server_addr as *mut PeerbusReqServer;
        let mut pending: *mut PeerbusPendingReq = ptr::null_mut();
        let rc = peerbus_req_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "req take failed: {}", last_error());
        let request = peerbus_pending_req_request(pending);
        assert_eq!(peerbus_message_kind(request), 70);
        assert_eq!(message_bytes(request), b"\x03");
        let reply = [6u8];
        assert!(peerbus_pending_req_reply(
            pending,
            71,
            reply.as_ptr(),
            reply.len()
        ));
        peerbus_pending_req_free(pending);
    });
    let mut response: *mut PeerbusMessage = ptr::null_mut();
    let request = [3u8];
    assert!(peerbus_req_client_call(
        req_client,
        70,
        request.as_ptr(),
        request.len(),
        &mut response,
    ));
    req_thread.join().unwrap();
    assert_eq!(peerbus_message_kind(response), 71);
    assert_eq!(message_bytes(response), b"\x06");
    peerbus_message_free(response);
    peerbus_req_client_free(req_client);
    peerbus_req_server_free(req_server);
    peerbus_node_free(req_node);

    // que/ans explicit polling.
    let que_topic = cstr("ffi/poll_que");
    let que_node = peerbus_node_new(ptr::null(), true);
    assert!(!que_node.is_null(), "que node failed: {}", last_error());
    let que_peer = peerbus_node_endpoint_addr(que_node);
    assert!(!que_peer.is_null(), "que endpoint addr failed: {}", last_error());
    let ans_server = peerbus_ans_server_new(que_node, que_topic.as_ptr());
    let que_client = peerbus_que_client_new(que_node, que_peer, que_topic.as_ptr());
    peerbus_string_free(que_peer);
    assert!(!ans_server.is_null() && !que_client.is_null());
    let ans_server_addr = ans_server as usize;
    let que_thread = std::thread::spawn(move || {
        let server = ans_server_addr as *mut PeerbusAnsServer;
        let mut pending: *mut PeerbusPendingQue = ptr::null_mut();
        let rc = peerbus_ans_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "ans take failed: {}", last_error());
        let request = peerbus_pending_que_request(pending);
        assert_eq!(peerbus_message_kind(request), 80);
        assert_eq!(message_bytes(request), b"\x05");
        let a = [5u8];
        let b = [6u8];
        assert!(peerbus_pending_que_send(pending, 81, a.as_ptr(), a.len()));
        assert!(peerbus_pending_que_send(pending, 81, b.as_ptr(), b.len()));
        assert!(peerbus_pending_que_finish(pending));
        peerbus_pending_que_free(pending);
    });
    let mut answers: *mut PeerbusMessages = ptr::null_mut();
    let query = [5u8];
    assert!(peerbus_que_client_send(
        que_client,
        80,
        query.as_ptr(),
        query.len(),
        &mut answers,
    ));
    que_thread.join().unwrap();
    assert_eq!(peerbus_answers_len(answers), 2);
    assert_eq!(peerbus_answers_kind_at(answers, 0), 81);
    assert_eq!(peerbus_answers_kind_at(answers, 1), 81);
    peerbus_answers_free(answers);
    peerbus_que_client_free(que_client);
    peerbus_ans_server_free(ans_server);
    peerbus_node_free(que_node);

    // put/ack interactive client upload.
    let put_topic = cstr("ffi/open_put");
    let put_node = peerbus_node_new(ptr::null(), true);
    assert!(!put_node.is_null(), "put node failed: {}", last_error());
    let put_peer = peerbus_node_endpoint_addr(put_node);
    assert!(!put_peer.is_null(), "put endpoint addr failed: {}", last_error());
    let ack_server = peerbus_ack_server_new(put_node, put_topic.as_ptr());
    let put_client = peerbus_put_client_new(put_node, put_peer, put_topic.as_ptr());
    peerbus_string_free(put_peer);
    assert!(!ack_server.is_null() && !put_client.is_null());
    let ack_server_addr = ack_server as usize;
    let put_thread = std::thread::spawn(move || {
        let server = ack_server_addr as *mut PeerbusAckServer;
        let mut puts: *mut PeerbusPuts = ptr::null_mut();
        let rc = peerbus_ack_server_take(server, 3000, &mut puts);
        assert_eq!(rc, 1, "ack take failed: {}", last_error());
        let mut sum = 0u8;
        let mut msg: *mut PeerbusMessage = ptr::null_mut();
        while peerbus_puts_next(puts, &mut msg) == 1 {
            for byte in message_bytes(msg) {
                sum = sum.wrapping_add(byte);
            }
            peerbus_message_free(msg);
        }
        let ack = [sum];
        assert!(peerbus_puts_ack(puts, 99, ack.as_ptr(), ack.len()));
        peerbus_puts_free(puts);
    });
    let mut put_sender: *mut PeerbusPutSender = ptr::null_mut();
    assert!(peerbus_put_client_open_sender(put_client, &mut put_sender));
    let a = [1u8, 2];
    let b = [3u8];
    assert!(peerbus_put_sender_send(put_sender, 90, a.as_ptr(), a.len()));
    assert!(peerbus_put_sender_send(put_sender, 90, b.as_ptr(), b.len()));
    let mut ack: *mut PeerbusMessage = ptr::null_mut();
    assert!(peerbus_put_sender_finish(put_sender, &mut ack));
    put_thread.join().unwrap();
    assert_eq!(peerbus_message_kind(ack), 99);
    assert_eq!(message_bytes(ack), b"\x06");
    peerbus_message_free(ack);
    peerbus_put_sender_free(put_sender);
    peerbus_put_client_free(put_client);
    peerbus_ack_server_free(ack_server);
    peerbus_node_free(put_node);

    // pip interactive client and server sessions.
    let pip_topic = cstr("ffi/open_pip");
    let pip_node = peerbus_node_new(ptr::null(), true);
    assert!(!pip_node.is_null(), "pip node failed: {}", last_error());
    let pip_peer = peerbus_node_endpoint_addr(pip_node);
    assert!(!pip_peer.is_null(), "pip endpoint addr failed: {}", last_error());
    let pip_server = peerbus_pip_server_new(pip_node, pip_topic.as_ptr());
    let pip_client = peerbus_pip_client_new(pip_node, pip_peer, pip_topic.as_ptr());
    peerbus_string_free(pip_peer);
    assert!(!pip_server.is_null() && !pip_client.is_null());
    let pip_server_addr = pip_server as usize;
    let pip_thread = std::thread::spawn(move || {
        let server = pip_server_addr as *mut PeerbusPipServer;
        let mut pending: *mut PeerbusPendingPip = ptr::null_mut();
        let rc = peerbus_pip_server_take(server, 3000, &mut pending);
        assert_eq!(rc, 1, "pip server take failed: {}", last_error());
        let mut msg: *mut PeerbusMessage = ptr::null_mut();
        assert_eq!(peerbus_pending_pip_next(pending, &mut msg), 1);
        assert_eq!(peerbus_message_kind(msg), 100);
        assert_eq!(message_bytes(msg), b"\x04");
        let reply = [8u8];
        assert!(peerbus_pending_pip_send(
            pending,
            101,
            reply.as_ptr(),
            reply.len(),
        ));
        peerbus_message_free(msg);
        assert_eq!(peerbus_pending_pip_next(pending, &mut msg), 0);
        assert!(peerbus_pending_pip_finish_send(pending));
        peerbus_pending_pip_free(pending);
    });
    let mut pip: *mut PeerbusPip = ptr::null_mut();
    assert!(peerbus_pip_client_open(pip_client, &mut pip));
    let input = [4u8];
    assert!(peerbus_pip_send(pip, 100, input.as_ptr(), input.len()));
    assert!(peerbus_pip_finish_send(pip));
    let mut reply: *mut PeerbusMessage = ptr::null_mut();
    assert_eq!(peerbus_pip_next(pip, &mut reply), 1);
    assert_eq!(peerbus_message_kind(reply), 101);
    assert_eq!(message_bytes(reply), b"\x08");
    peerbus_message_free(reply);
    assert_eq!(peerbus_pip_next(pip, &mut reply), 0);
    pip_thread.join().unwrap();
    peerbus_pip_free(pip);
    peerbus_pip_client_free(pip_client);
    peerbus_pip_server_free(pip_server);
    peerbus_node_free(pip_node);
}
