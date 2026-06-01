//! End-to-end tests for `Node`.
//!
//! With the SHM local backend, same-host routing keys off
//! the service name we compose from
//! `(identity, topic)`. The subscriber asks the backend whether
//! that service exists locally; if yes → SHM, if no → dial via
//! iroh. So all the local tests below use `.identity(...)` so the
//! routing has something to match.

use std::time::{Duration, Instant};

use quicbit::did_key::endpoint_id_to_did_key;
use quicbit::transport::{PublisherOps, SubscriberOps};
use quicbit::{LocalConfig, Node, RemoteTransport, TopicQos, Transport};

static NODE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[datapod::datapod]
struct Tick {
    seq: u32,
    payload: u32,
}

#[datapod::datapod]
struct Add {
    a: i32,
    b: i32,
}

#[datapod::datapod]
struct Sum {
    value: i32,
}

#[datapod::datapod]
struct RangeQue {
    start: u32,
    count: u32,
}

#[datapod::datapod]
struct Hit {
    value: u32,
}

#[datapod::datapod]
struct LogChunk {
    value: u32,
}

#[datapod::datapod]
struct UploadAck {
    count: u32,
    sum: u32,
}

/// Fixed-POD type larger than a small `chunk_bytes`, so the Node req/res
/// client chunks it on the wire. Used to verify iroh chunking +
/// reassembly end-to-end against a standalone `RemoteTransport` server
/// (which has no SHM, forcing the iroh path).
#[datapod::datapod]
struct Big64 {
    data: [u8; 65536],
}

#[datapod::datapod]
struct ClientMsg {
    value: u32,
}

#[datapod::datapod]
struct ServerMsg {
    value: u32,
}

fn poll_for<R>(timeout: Duration, mut f: impl FnMut() -> Option<R>) -> Option<R> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(r) = f() {
            return Some(r);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{stem}_{pid}_{nanos}")
}

fn unique_system_did() -> String {
    endpoint_id_to_did_key(&iroh::SecretKey::generate().public())
}

fn node_test_guard() -> std::sync::MutexGuard<'static, ()> {
    NODE_TEST_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

#[test]
fn local_routing_two_nodes_same_process() {
    let _guard = node_test_guard();
    let pub_node = Node::builder()
        .no_relay()
        .identity("local_pub")
        .bind()
        .expect("publisher node");

    let sub_node = Node::builder()
        .no_relay()
        .identity("local_sub")
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>("rover/pose").unwrap();
    let mut sub = sub_node
        .subscriber::<Tick>("local_pub", "rover/pose")
        .expect("local subscribe");

    pubr.send(&Tick {
        seq: 1,
        payload: 42,
    })
    .unwrap();

    let sample = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive");
    assert_eq!(
        *sample.header(),
        Tick {
            seq: 1,
            payload: 42
        }
    );
}

#[test]
fn publisher_stats_track_no_remote_subscriber_drop() {
    let _guard = node_test_guard();
    let node = Node::builder()
        .no_relay()
        .identity(unique_name("stats_pub"))
        .bind()
        .expect("node");
    let topic = unique_name("stats/topic");
    let mut pubr = node.publisher::<Tick>(&topic).unwrap();

    pubr.send(&Tick { seq: 1, payload: 2 }).unwrap();
    let stats = pubr.stats();
    assert_eq!(stats.published, 1);
    assert_eq!(stats.remote_dropped, 1);
    assert_eq!(stats.stale_dropped, 0);
    assert_eq!(stats.bytes_sent, 0);
    assert_eq!(stats.send_errors, 0);
}

#[test]
fn local_latest_qos_skips_stale_samples() {
    let _guard = node_test_guard();
    let cfg = LocalConfig {
        history_depth: 8,
        subscriber_buffer: 8,
        ..LocalConfig::default()
    };

    let pub_identity = unique_name("local_latest_pub");
    let topic = unique_name("local/latest");
    let pub_node = Node::builder()
        .no_relay()
        .identity(&pub_identity)
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .identity(unique_name("local_latest_sub"))
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node
        .subscriber_with_qos::<Tick>(pub_identity.as_str(), &topic, TopicQos::latest())
        .unwrap();

    pubr.send(&Tick {
        seq: 1,
        payload: 10,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 2,
        payload: 20,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 3,
        payload: 30,
    })
    .unwrap();

    let sample = sub.take().unwrap().expect("latest sample");
    assert_eq!(sample.header().seq, 3);
    assert_eq!(sample.header().payload, 30);
    assert_eq!(sub.stats().stale_dropped, 2);
    assert!(sub.take().unwrap().is_none());
}

#[test]
fn local_reliable_qos_preserves_backlog_under_capacity() {
    let _guard = node_test_guard();
    let cfg = LocalConfig {
        history_depth: 8,
        subscriber_buffer: 8,
        ..LocalConfig::default()
    };

    let pub_identity = unique_name("local_reliable_pub");
    let topic = unique_name("local/reliable");
    let pub_node = Node::builder()
        .no_relay()
        .identity(&pub_identity)
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .identity(unique_name("local_reliable_sub"))
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node
        .subscriber_with_qos::<Tick>(pub_identity.as_str(), &topic, TopicQos::reliable())
        .unwrap();

    pubr.send(&Tick {
        seq: 1,
        payload: 10,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 2,
        payload: 20,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 3,
        payload: 30,
    })
    .unwrap();

    assert_eq!(sub.take().unwrap().unwrap().header().seq, 1);
    assert_eq!(sub.take().unwrap().unwrap().header().seq, 2);
    assert_eq!(sub.take().unwrap().unwrap().header().seq, 3);
    assert_eq!(sub.stats().stale_dropped, 0);
}

#[test]
fn local_reliable_qos_reports_lag_when_capacity_exceeded() {
    let _guard = node_test_guard();
    let cfg = LocalConfig {
        max_subscribers: 1,
        subscriber_buffer: 1,
        history_depth: 1,
        ..LocalConfig::default()
    };

    let pub_identity = unique_name("local_lag_pub");
    let topic = unique_name("local/reliable_lag");
    let pub_node = Node::builder()
        .no_relay()
        .identity(&pub_identity)
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .identity(unique_name("local_lag_sub"))
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node
        .subscriber_with_qos::<Tick>(pub_identity.as_str(), &topic, TopicQos::reliable())
        .unwrap();

    pubr.send(&Tick {
        seq: 1,
        payload: 10,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 2,
        payload: 20,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 3,
        payload: 30,
    })
    .unwrap();

    assert!(matches!(
        sub.take(),
        Err(quicbit::Error::Lagged { dropped: 2 })
    ));
    assert_eq!(sub.stats().stale_dropped, 2);
    assert_eq!(sub.take().unwrap().unwrap().header().seq, 3);
}

#[test]
fn local_latest_qos_subscribers_have_independent_drop_stats() {
    let _guard = node_test_guard();
    let cfg = LocalConfig {
        history_depth: 8,
        subscriber_buffer: 8,
        ..LocalConfig::default()
    };

    let pub_identity = unique_name("local_multi_latest_pub");
    let topic = unique_name("local/latest_multi");
    let pub_node = Node::builder()
        .no_relay()
        .identity(&pub_identity)
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node_a = Node::builder()
        .no_relay()
        .identity(unique_name("local_multi_latest_sub_a"))
        .bind()
        .expect("subscriber node a");
    let sub_node_b = Node::builder()
        .no_relay()
        .identity(unique_name("local_multi_latest_sub_b"))
        .bind()
        .expect("subscriber node b");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub_a = sub_node_a
        .subscriber_with_qos::<Tick>(pub_identity.as_str(), &topic, TopicQos::latest())
        .unwrap();
    let mut sub_b = sub_node_b
        .subscriber_with_qos::<Tick>(pub_identity.as_str(), &topic, TopicQos::latest())
        .unwrap();

    pubr.send(&Tick {
        seq: 1,
        payload: 10,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 2,
        payload: 20,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 3,
        payload: 30,
    })
    .unwrap();

    assert_eq!(sub_a.take().unwrap().unwrap().header().seq, 3);
    assert_eq!(sub_a.stats().stale_dropped, 2);
    assert_eq!(sub_b.stats().stale_dropped, 0);
    assert_eq!(sub_b.take().unwrap().unwrap().header().seq, 3);
    assert_eq!(sub_b.stats().stale_dropped, 2);
}

#[test]
fn system_did_subscribe_with_qos_routes_locally() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/qos_local");
    let cfg = LocalConfig {
        max_subscribers: 2,
        history_depth: 8,
        subscriber_buffer: 8,
        ..LocalConfig::default()
    };

    let pub_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .local_config(cfg)
        .bind()
        .expect("system publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system subscriber node");

    let mut pubr = pub_node
        .publisher_with_qos::<Tick>(&topic, TopicQos::latest())
        .unwrap();
    let mut sub = sub_node
        .subscribe_with_qos::<Tick>(&topic, TopicQos::latest())
        .unwrap();

    pubr.send(&Tick {
        seq: 1,
        payload: 10,
    })
    .unwrap();
    pubr.send(&Tick {
        seq: 2,
        payload: 20,
    })
    .unwrap();
    let sample = sub.take().unwrap().expect("latest system sample");
    assert_eq!(sample.header().seq, 2);
    assert_eq!(sub.stats().stale_dropped, 1);
}

#[test]
fn node_req_res_routes_locally_by_name() {
    let _guard = node_test_guard();
    let server_identity = unique_name("calc_local");
    let client_identity = unique_name("calc_client");
    let topic = unique_name("calc/add");

    let server_node = Node::builder()
        .no_relay()
        .identity(&server_identity)
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .identity(client_identity)
        .bind()
        .expect("client node");

    let mut server = server_node.req_server::<Add, Sum>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let (req, reply) = server.take().unwrap()?;
            reply
                .respond(&Sum {
                    value: req.header().a + req.header().b,
                })
                .unwrap();
            Some(())
        })
        .expect("server should receive req");
    });

    let mut client = client_node
        .req_client::<Add, Sum>(server_identity.as_str(), &topic)
        .unwrap();
    let res = client.call(&Add { a: 2, b: 40 }).unwrap();
    assert_eq!(res.header().value, 42);
    handle.join().unwrap();
}

#[test]
fn node_req_res_routes_remotely_by_endpoint_addr() {
    let _guard = node_test_guard();
    let topic = unique_name("calc/remote_add");

    let server_node = Node::builder()
        .no_relay()
        .identity(unique_name("calc_remote_server"))
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .identity(unique_name("calc_remote_client"))
        .bind()
        .expect("client node");

    let mut server = server_node.req_server::<Add, Sum>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let (req, reply) = server.take().unwrap()?;
            reply
                .respond(&Sum {
                    value: req.header().a + req.header().b,
                })
                .unwrap();
            Some(())
        })
        .expect("server should receive remote req");
    });

    let mut client = client_node
        .req_client::<Add, Sum>(server_node.endpoint_addr(), &topic)
        .unwrap();
    let res = client.call(&Add { a: 3, b: 39 }).unwrap();
    assert_eq!(res.header().value, 42);
    handle.join().unwrap();
}

#[test]
fn system_did_req_res_routes_locally_without_peer_argument() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/calc_add");

    let server_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system server node");
    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system client node");

    let mut server = server_node.req_server::<Add, Sum>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let (req, reply) = server.take().unwrap()?;
            reply
                .respond(&Sum {
                    value: req.header().a + req.header().b,
                })
                .unwrap();
            Some(())
        })
        .expect("system server should receive req");
    });

    let mut client = client_node.req::<Add, Sum>(&topic).unwrap();
    let res = client.call(&Add { a: 4, b: 38 }).unwrap();
    assert_eq!(res.header().value, 42);
    handle.join().unwrap();
}

#[test]
fn system_did_req_res_uses_topic_route_when_not_local() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/remote_calc_add");
    let route_topic = format!("{system_did}::{topic}");

    let remote_server = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .expect("remote req/res server transport");
    remote_server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("remote server addresses");
    remote_server
        .serve_requests::<Add, Sum, _>(|req| Sum {
            value: req.a + req.b,
        })
        .unwrap();

    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system client node");
    client_node
        .add_topic_route(&topic, remote_server.endpoint_addr())
        .unwrap();

    let mut client = client_node.req::<Add, Sum>(&topic).unwrap();
    let res = client.call(&Add { a: 5, b: 37 }).unwrap();
    assert_eq!(res.header().value, 42);
}

#[test]
fn system_did_req_res_can_use_topic_agnostic_system_peer() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/peer_calc_add");
    let route_topic = format!("{system_did}::{topic}");

    let remote_server = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .expect("remote req/res server transport");
    remote_server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("remote server addresses");
    remote_server
        .serve_requests::<Add, Sum, _>(|req| Sum {
            value: req.a + req.b,
        })
        .unwrap();

    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system client node");
    client_node
        .add_system_peer(remote_server.endpoint_addr())
        .unwrap();

    let mut client = client_node.req::<Add, Sum>(&topic).unwrap();
    let res = client.call(&Add { a: 6, b: 36 }).unwrap();
    assert_eq!(res.header().value, 42);
}

#[test]
fn req_res_client_stats_track_remote_calls() {
    // Node req/res client dialing a RemoteTransport server goes over
    // iroh (the RemoteTransport peer has no local SHM service), so the
    // client's remote-path counters move. Two same-host Nodes would
    // resolve through SHM and report zeros.
    let _guard = node_test_guard();
    let topic = unique_name("calc/stats_add");

    let remote_server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .expect("remote req/res server transport");
    remote_server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("remote server addresses");
    remote_server
        .serve_requests::<Add, Sum, _>(|req| Sum {
            value: req.a + req.b,
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().expect("client node");
    let mut client = client_node
        .req_client::<Add, Sum>(remote_server.endpoint_addr(), &topic)
        .unwrap();

    let before = client.stats();
    assert_eq!(before.messages_out, 0);
    assert_eq!(before.messages_in, 0);

    for _ in 0..3 {
        let res = client.call(&Add { a: 2, b: 3 }).unwrap();
        assert_eq!(res.header().value, 5);
    }

    let after = client.stats();
    assert_eq!(after.messages_out, 3, "three requests sent");
    assert_eq!(after.messages_in, 3, "three responses received");
    assert!(after.bytes_out > 0, "request bytes counted");
    assert!(after.bytes_in > 0, "response bytes counted");
    assert_eq!(after.errors, 0);
}

#[test]
fn req_res_chunks_large_payload_over_iroh() {
    // Genuine iroh chunking: a Node req/res client dialing a standalone
    // RemoteTransport server (no SHM) goes over the wire. With a small
    // `chunk_bytes`, the 64 KiB request is split into chunk frames and
    // reassembled server-side — the path that lifts the 64 MiB cap.
    let _guard = node_test_guard();
    let topic = unique_name("calc/big_echo");

    let remote_server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .expect("remote server transport");
    remote_server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("remote server addresses");
    // Echo the request straight back.
    remote_server
        .serve_requests::<Big64, Big64, _>(|req| req)
        .unwrap();

    let qos = TopicQos::reliable()
        .with_chunk_bytes(16 * 1024)
        .with_max_message_bytes(8 * 1024 * 1024)
        .with_max_inflight_bytes(8 * 1024 * 1024);

    let client_node = Node::builder().no_relay().bind().expect("client node");
    let mut client = client_node
        .req_client_with_qos::<Big64, Big64>(remote_server.endpoint_addr(), &topic, qos)
        .unwrap();

    let mut req = Big64 { data: [0u8; 65536] };
    for (i, b) in req.data.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let res = client.call(&req).unwrap();
    assert!(res.header().data == req.data, "echoed payload must match");

    // The request (64 KiB) exceeded chunk_bytes (16 KiB), so the client
    // chunked it; stats count one logical request out and one in.
    let stats = client.stats();
    assert_eq!(stats.messages_out, 1);
    assert_eq!(stats.messages_in, 1);
    assert!(stats.bytes_out >= 65536, "chunked request bytes counted");
}

// ---- standalone RemoteTransport <-> Node interop (over iroh) ----
// A RemoteTransport has no SHM service, so these force the iroh path
// and exercise the standalone que/ans, put/ack, pip servers/clients.

#[test]
fn standalone_queans_server_serves_node_client() {
    let _guard = node_test_guard();
    let topic = unique_name("search/standalone_srv");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_ques::<RangeQue, Hit, _>(|q| {
            (0..q.count).map(|o| Hit { value: q.start + o }).collect()
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .que_client::<RangeQue, Hit>(server.endpoint_addr(), &topic)
        .unwrap();
    let mut answers = client
        .send(&RangeQue {
            start: 100,
            count: 3,
        })
        .unwrap();
    let mut got = Vec::new();
    while let Some(a) = answers.next().unwrap() {
        got.push(a.header().value);
    }
    assert_eq!(got, vec![100, 101, 102]);
}

#[test]
fn node_queans_server_serves_standalone_client() {
    let _guard = node_test_guard();
    let topic = unique_name("search/standalone_cli");
    let server_node = Node::builder()
        .no_relay()
        .identity(unique_name("ans_srv"))
        .bind()
        .unwrap();
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    let mut server = server_node.ans::<RangeQue, Hit>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let (que, mut ans) = server.take().unwrap()?;
            let q = *que.header();
            for o in 0..q.count {
                ans.send(&Hit { value: q.start + o }).unwrap();
            }
            ans.finish().unwrap();
            Some(())
        })
        .expect("server served");
    });

    let client_tp = RemoteTransport::builder(&topic)
        .no_relay()
        .peer(server_node.endpoint_addr())
        .build_blocking()
        .unwrap();
    let mut client = client_tp.que_client::<RangeQue, Hit>().unwrap();
    let answers = client.send(RangeQue { start: 5, count: 2 }).unwrap();
    assert_eq!(
        answers.iter().map(|h| h.value).collect::<Vec<_>>(),
        vec![5, 6]
    );
    handle.join().unwrap();
}

#[test]
fn standalone_putack_server_serves_node_client() {
    let _guard = node_test_guard();
    let topic = unique_name("logs/standalone_srv");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_puts::<LogChunk, UploadAck, _>(|puts| UploadAck {
            count: puts.len() as u32,
            sum: puts.iter().map(|p| p.value).sum(),
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .put_client::<LogChunk, UploadAck>(server.endpoint_addr(), &topic)
        .unwrap();
    let mut upload = client.open().unwrap();
    for value in [10, 20, 30] {
        upload.send(&LogChunk { value }).unwrap();
    }
    let ack = upload.finish().unwrap();
    assert_eq!(ack.header().count, 3);
    assert_eq!(ack.header().sum, 60);
}

#[test]
fn node_putack_server_serves_standalone_client() {
    let _guard = node_test_guard();
    let topic = unique_name("logs/standalone_cli");
    let server_node = Node::builder()
        .no_relay()
        .identity(unique_name("ack_srv"))
        .bind()
        .unwrap();
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    let mut server = server_node.ack::<LogChunk, UploadAck>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let mut puts = server.take().unwrap()?;
            let mut count = 0;
            let mut sum = 0;
            while let Some(p) = puts.next().unwrap() {
                count += 1;
                sum += p.header().value;
            }
            puts.ack(&UploadAck { count, sum }).unwrap();
            Some(())
        })
        .expect("server served");
    });

    let client_tp = RemoteTransport::builder(&topic)
        .no_relay()
        .peer(server_node.endpoint_addr())
        .build_blocking()
        .unwrap();
    let mut client = client_tp.put_client::<LogChunk, UploadAck>().unwrap();
    let ack = client
        .upload(&[LogChunk { value: 7 }, LogChunk { value: 8 }])
        .unwrap();
    assert_eq!(ack.count, 2);
    assert_eq!(ack.sum, 15);
    handle.join().unwrap();
}

#[test]
fn standalone_pip_server_serves_node_client() {
    let _guard = node_test_guard();
    let topic = unique_name("session/standalone_srv");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_pips::<ClientMsg, ServerMsg, _>(|msgs| {
            msgs.iter()
                .map(|m| ServerMsg { value: m.value * 2 })
                .collect()
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .pip_client::<ClientMsg, ServerMsg>(server.endpoint_addr(), &topic)
        .unwrap();
    let mut pip = client.open().unwrap();
    for value in [1, 2, 3] {
        pip.send(&ClientMsg { value }).unwrap();
    }
    pip.finish_send().unwrap();
    let mut got = Vec::new();
    while let Some(reply) = pip.next().unwrap() {
        got.push(reply.header().value);
    }
    assert_eq!(got, vec![2, 4, 6]);
}

#[test]
fn node_pip_server_serves_standalone_client() {
    let _guard = node_test_guard();
    let topic = unique_name("session/standalone_cli");
    let server_node = Node::builder()
        .no_relay()
        .identity(unique_name("pip_srv"))
        .bind()
        .unwrap();
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    let mut server = server_node
        .pip_server::<ClientMsg, ServerMsg>(&topic)
        .unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let mut pip = server.take().unwrap()?;
            while let Some(msg) = pip.next().unwrap() {
                pip.send(&ServerMsg {
                    value: msg.header().value + 1,
                })
                .unwrap();
            }
            pip.finish_send().unwrap();
            Some(())
        })
        .expect("server served");
    });

    let client_tp = RemoteTransport::builder(&topic)
        .no_relay()
        .peer(server_node.endpoint_addr())
        .build_blocking()
        .unwrap();
    let mut client = client_tp.pip_client::<ClientMsg, ServerMsg>().unwrap();
    let replies = client
        .exchange(&[ClientMsg { value: 40 }, ClientMsg { value: 41 }])
        .unwrap();
    assert_eq!(
        replies.iter().map(|m| m.value).collect::<Vec<_>>(),
        vec![41, 42]
    );
    handle.join().unwrap();
}

#[test]
fn node_que_ans_routes_locally_by_name() {
    let _guard = node_test_guard();
    let server_identity = unique_name("search_local");
    let client_identity = unique_name("search_client");
    let topic = unique_name("search/local");

    let server_node = Node::builder()
        .no_relay()
        .identity(&server_identity)
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .identity(client_identity)
        .bind()
        .expect("client node");

    let mut server = server_node.ans::<RangeQue, Hit>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let (que, mut ans) = server.take().unwrap()?;
            for offset in 0..que.header().count {
                ans.send(&Hit {
                    value: que.header().start + offset,
                })
                .unwrap();
            }
            ans.finish().unwrap();
            Some(())
        })
        .expect("server should receive que");
    });

    let mut client = client_node
        .que_client::<RangeQue, Hit>(server_identity.as_str(), &topic)
        .unwrap();
    let mut answers = client
        .send(&RangeQue {
            start: 10,
            count: 3,
        })
        .unwrap();
    let mut got = Vec::new();
    while let Some(ans) = answers.next().unwrap() {
        got.push(ans.header().value);
    }
    assert_eq!(got, vec![10, 11, 12]);
    handle.join().unwrap();
}

#[test]
fn node_que_ans_routes_remotely_by_endpoint_addr() {
    let _guard = node_test_guard();
    let topic = unique_name("search/remote");

    let server_node = Node::builder()
        .no_relay()
        .identity(unique_name("search_remote_server"))
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .identity(unique_name("search_remote_client"))
        .bind()
        .expect("client node");

    let mut server = server_node.ans::<RangeQue, Hit>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let (que, mut ans) = server.take().unwrap()?;
            for offset in 0..que.header().count {
                ans.send(&Hit {
                    value: que.header().start + offset,
                })
                .unwrap();
            }
            ans.finish().unwrap();
            Some(())
        })
        .expect("server should receive remote que");
    });

    let mut client = client_node
        .que_client::<RangeQue, Hit>(server_node.endpoint_addr(), &topic)
        .unwrap();
    let mut answers = client
        .send(&RangeQue {
            start: 20,
            count: 4,
        })
        .unwrap();
    let mut got = Vec::new();
    while let Some(ans) = answers.next().unwrap() {
        got.push(ans.header().value);
    }
    assert_eq!(got, vec![20, 21, 22, 23]);
    handle.join().unwrap();
}

#[test]
fn system_did_que_ans_routes_locally_without_peer_argument() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/search");

    let server_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system server node");
    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system client node");

    let mut server = server_node.ans::<RangeQue, Hit>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let (que, mut ans) = server.take().unwrap()?;
            for offset in 0..que.header().count {
                ans.send(&Hit {
                    value: que.header().start + offset,
                })
                .unwrap();
            }
            ans.finish().unwrap();
            Some(())
        })
        .expect("system server should receive que");
    });

    let mut client = client_node.que::<RangeQue, Hit>(&topic).unwrap();
    let mut answers = client
        .send(&RangeQue {
            start: 30,
            count: 2,
        })
        .unwrap();
    let mut got = Vec::new();
    while let Some(ans) = answers.next().unwrap() {
        got.push(ans.header().value);
    }
    assert_eq!(got, vec![30, 31]);
    handle.join().unwrap();
}

#[test]
fn node_put_ack_routes_locally_by_name() {
    let _guard = node_test_guard();
    let server_identity = unique_name("sink_local");
    let client_identity = unique_name("sink_client");
    let topic = unique_name("logs/local");

    let server_node = Node::builder()
        .no_relay()
        .identity(&server_identity)
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .identity(client_identity)
        .bind()
        .expect("client node");

    let mut server = server_node.ack::<LogChunk, UploadAck>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let mut puts = server.take().unwrap()?;
            let mut count = 0;
            let mut sum = 0;
            loop {
                match puts.next().unwrap() {
                    Some(chunk) => {
                        count += 1;
                        sum += chunk.header().value;
                    }
                    None if count == 0 => return None,
                    None => break,
                }
            }
            puts.ack(&UploadAck { count, sum }).unwrap();
            Some(())
        })
        .expect("server should receive puts");
    });

    let mut client = client_node
        .put_client::<LogChunk, UploadAck>(server_identity.as_str(), &topic)
        .unwrap();
    let mut put = client.open().unwrap();
    put.send(&LogChunk { value: 10 }).unwrap();
    put.send(&LogChunk { value: 20 }).unwrap();
    put.send(&LogChunk { value: 30 }).unwrap();
    let ack = put.finish().unwrap();
    assert_eq!(ack.header().count, 3);
    assert_eq!(ack.header().sum, 60);
    handle.join().unwrap();
}

#[test]
fn node_put_ack_routes_remotely_by_endpoint_addr() {
    let _guard = node_test_guard();
    let topic = unique_name("logs/remote");

    let server_node = Node::builder()
        .no_relay()
        .identity(unique_name("sink_remote_server"))
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .identity(unique_name("sink_remote_client"))
        .bind()
        .expect("client node");

    let mut server = server_node.ack::<LogChunk, UploadAck>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let mut puts = server.take().unwrap()?;
            let mut count = 0;
            let mut sum = 0;
            loop {
                match puts.next().unwrap() {
                    Some(chunk) => {
                        count += 1;
                        sum += chunk.header().value;
                    }
                    None if count == 0 => return None,
                    None => break,
                }
            }
            puts.ack(&UploadAck { count, sum }).unwrap();
            Some(())
        })
        .expect("server should receive remote puts");
    });

    let mut client = client_node
        .put_client::<LogChunk, UploadAck>(server_node.endpoint_addr(), &topic)
        .unwrap();
    let mut put = client.open().unwrap();
    put.send(&LogChunk { value: 7 }).unwrap();
    put.send(&LogChunk { value: 8 }).unwrap();
    let ack = put.finish().unwrap();
    assert_eq!(ack.header().count, 2);
    assert_eq!(ack.header().sum, 15);
    handle.join().unwrap();
}

#[test]
fn system_did_put_ack_routes_locally_without_peer_argument() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/logs");

    let server_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system server node");
    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system client node");

    let mut server = server_node.ack::<LogChunk, UploadAck>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let mut puts = server.take().unwrap()?;
            let mut count = 0;
            let mut sum = 0;
            loop {
                match puts.next().unwrap() {
                    Some(chunk) => {
                        count += 1;
                        sum += chunk.header().value;
                    }
                    None if count == 0 => return None,
                    None => break,
                }
            }
            puts.ack(&UploadAck { count, sum }).unwrap();
            Some(())
        })
        .expect("system server should receive puts");
    });

    let mut client = client_node.put::<LogChunk, UploadAck>(&topic).unwrap();
    let mut put = client.open().unwrap();
    put.send(&LogChunk { value: 100 }).unwrap();
    put.send(&LogChunk { value: 23 }).unwrap();
    let ack = put.finish().unwrap();
    assert_eq!(ack.header().count, 2);
    assert_eq!(ack.header().sum, 123);
    handle.join().unwrap();
}

#[test]
fn node_pip_routes_locally_by_name() {
    let _guard = node_test_guard();
    let server_identity = unique_name("pip_local");
    let client_identity = unique_name("pip_client");
    let topic = unique_name("session/local");

    let server_node = Node::builder()
        .no_relay()
        .identity(&server_identity)
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .identity(client_identity)
        .bind()
        .expect("client node");

    let mut server = server_node
        .pip_server::<ClientMsg, ServerMsg>(&topic)
        .unwrap();
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut pip = loop {
            if let Some(pip) = server.take().unwrap() {
                break pip;
            }
            assert!(
                Instant::now() < deadline,
                "server should receive pip session"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let msg = pip.next().unwrap().expect("server should receive msg");
        pip.send(&ServerMsg {
            value: msg.header().value + 1,
        })
        .unwrap();
        pip.finish_send().unwrap();
    });

    let mut client = client_node
        .pip_client::<ClientMsg, ServerMsg>(server_identity.as_str(), &topic)
        .unwrap();
    let mut pip = client.open().unwrap();
    pip.send(&ClientMsg { value: 41 }).unwrap();
    let msg = pip.next().unwrap().expect("client should receive msg");
    assert_eq!(msg.header().value, 42);
    assert!(pip.next().unwrap().is_none());
    handle.join().unwrap();
}

#[test]
fn node_pip_routes_remotely_by_endpoint_addr() {
    let _guard = node_test_guard();
    let topic = unique_name("session/remote");

    let server_node = Node::builder()
        .no_relay()
        .identity(unique_name("pip_remote_server"))
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .identity(unique_name("pip_remote_client"))
        .bind()
        .expect("client node");

    let mut server = server_node
        .pip_server::<ClientMsg, ServerMsg>(&topic)
        .unwrap();
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut pip = loop {
            if let Some(pip) = server.take().unwrap() {
                break pip;
            }
            assert!(
                Instant::now() < deadline,
                "server should receive remote pip session"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let msg = pip.next().unwrap().expect("server should receive msg");
        pip.send(&ServerMsg {
            value: msg.header().value + 2,
        })
        .unwrap();
        pip.finish_send().unwrap();
    });

    let mut client = client_node
        .pip_client::<ClientMsg, ServerMsg>(server_node.endpoint_addr(), &topic)
        .unwrap();
    let mut pip = client.open().unwrap();
    pip.send(&ClientMsg { value: 40 }).unwrap();
    let msg = pip.next().unwrap().expect("client should receive msg");
    assert_eq!(msg.header().value, 42);
    assert!(pip.next().unwrap().is_none());
    handle.join().unwrap();
}

#[test]
fn system_did_pip_routes_locally_without_peer_argument() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/session");

    let server_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system server node");
    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system client node");

    let mut server = server_node
        .pip_server::<ClientMsg, ServerMsg>(&topic)
        .unwrap();
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut pip = loop {
            if let Some(pip) = server.take().unwrap() {
                break pip;
            }
            assert!(
                Instant::now() < deadline,
                "system server should receive pip session"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let msg = pip.next().unwrap().expect("server should receive msg");
        pip.send(&ServerMsg {
            value: msg.header().value * 2,
        })
        .unwrap();
        pip.finish_send().unwrap();
    });

    let mut client = client_node.pip::<ClientMsg, ServerMsg>(&topic).unwrap();
    let mut pip = client.open().unwrap();
    pip.send(&ClientMsg { value: 21 }).unwrap();
    let msg = pip.next().unwrap().expect("client should receive msg");
    assert_eq!(msg.header().value, 42);
    assert!(pip.next().unwrap().is_none());
    handle.join().unwrap();
}

#[test]
fn endpoint_id_is_stable_with_key_file() {
    let _guard = node_test_guard();
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().join("rover.key");

    let id1 = {
        let node = Node::builder()
            .no_relay()
            .identity_file(&path)
            .bind()
            .expect("first bind");
        node.endpoint_id()
    };

    let id2 = {
        let node = Node::builder()
            .no_relay()
            .identity_file(&path)
            .bind()
            .expect("second bind");
        node.endpoint_id()
    };

    assert_eq!(id1, id2, "EndpointId should persist across reloads");
}

#[test]
fn identity_string_yields_deterministic_endpoint_id() {
    let _guard = node_test_guard();
    let id1 = Node::builder()
        .no_relay()
        .identity("rover-a")
        .bind()
        .unwrap()
        .endpoint_id();
    let id2 = Node::builder()
        .no_relay()
        .identity("rover-a")
        .bind()
        .unwrap()
        .endpoint_id();
    assert_eq!(id1, id2);

    let other = Node::builder()
        .no_relay()
        .identity("rover-b")
        .bind()
        .unwrap()
        .endpoint_id();
    assert_ne!(id1, other);
}

#[test]
fn identity_env_round_trip() {
    let _guard = node_test_guard();
    let var = format!("QUICBIT_TEST_ID_{}", std::process::id());
    // SAFETY: tests modify process env, single-threaded read here.
    unsafe { std::env::set_var(&var, "rover-c") };

    let id1 = Node::builder()
        .no_relay()
        .identity_env(&var)
        .bind()
        .unwrap()
        .endpoint_id();
    let id2 = Node::builder()
        .no_relay()
        .identity("rover-c") // same name, derived directly
        .bind()
        .unwrap()
        .endpoint_id();
    assert_eq!(id1, id2);

    unsafe { std::env::remove_var(&var) };
}

#[test]
fn name_based_local_routing() {
    let _guard = node_test_guard();
    let pub_node = Node::builder()
        .no_relay()
        .identity("sensors")
        .bind()
        .unwrap();
    let sub_node = Node::builder()
        .no_relay()
        .identity("planner")
        .bind()
        .unwrap();

    let mut pubr = pub_node.publisher::<Tick>("imu/raw").unwrap();
    // Subscribe by NAME — local service name is composed from
    // it, and `open_existing` succeeds because the publisher is
    // already up on this host.
    let mut sub = sub_node.subscriber::<Tick>("sensors", "imu/raw").unwrap();

    pubr.send(&Tick {
        seq: 7,
        payload: 700,
    })
    .unwrap();
    let s = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive");
    assert_eq!(
        *s.header(),
        Tick {
            seq: 7,
            payload: 700
        }
    );
}

#[test]
fn node_publisher_feeds_remote_transport_subscriber() {
    let _guard = node_test_guard();
    let identity = unique_name("node_pub_remote_sub");
    let topic = unique_name("interop/node_to_remote");

    let pub_node = Node::builder()
        .no_relay()
        .identity(&identity)
        .bind()
        .expect("publisher node");
    pub_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("publisher addresses");

    let remote_sub_side = RemoteTransport::builder(&topic)
        .no_relay()
        .peer(pub_node.endpoint_addr())
        .build_blocking()
        .expect("remote subscriber transport");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = remote_sub_side.subscriber::<Tick>().unwrap();

    let got = poll_for(Duration::from_secs(5), || {
        pubr.send(&Tick {
            seq: 1,
            payload: 11_001,
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(50));
        while let Some(sample) = sub.take().unwrap() {
            if sample.header().payload == 11_001 {
                return Some(*sample.header());
            }
        }
        None
    })
    .expect("RemoteTransport subscriber should receive from Node publisher");

    assert_eq!(got.payload, 11_001);
}

#[test]
fn remote_transport_publisher_feeds_node_subscriber() {
    let _guard = node_test_guard();
    let topic = unique_name("interop/remote_to_node");

    let remote_pub_side = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .expect("remote publisher transport");
    remote_pub_side
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("remote publisher addresses");

    let sub_node = Node::builder()
        .no_relay()
        .identity(unique_name("node_sub_remote_pub"))
        .bind()
        .expect("subscriber node");

    let mut pubr = remote_pub_side.publisher::<Tick>().unwrap();
    let mut sub = sub_node
        .subscriber::<Tick>(remote_pub_side.endpoint_addr(), &topic)
        .unwrap();

    let got = poll_for(Duration::from_secs(5), || {
        let mut loan = pubr.loan(0).unwrap();
        loan.header = Tick {
            seq: 1,
            payload: 22_002,
        };
        pubr.publish(loan).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        while let Some(sample) = sub.take().unwrap() {
            if sample.header().payload == 22_002 {
                return Some(*sample.header());
            }
        }
        None
    })
    .expect("Node subscriber should receive from RemoteTransport publisher");

    assert_eq!(got.payload, 22_002);
}

#[test]
fn node_best_effort_pubsub_uses_datagram_path_when_available() {
    let _guard = node_test_guard();
    let topic = unique_name("interop/best_effort_datagram");
    let qos = TopicQos::best_effort()
        .with_chunk_bytes(4)
        .with_max_inflight_bytes(1024);

    let pub_node = Node::builder()
        .no_relay()
        .identity(unique_name("best_effort_pub"))
        .bind()
        .expect("publisher node");
    pub_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("publisher addresses");

    let sub_node = Node::builder()
        .no_relay()
        .identity(unique_name("best_effort_sub"))
        .bind()
        .expect("subscriber node");

    // Subscribe before creating the local SHM service so this same-host test
    // is forced onto the iroh path instead of the local fast path.
    let mut sub = sub_node
        .subscriber_with_qos::<Tick>(pub_node.endpoint_addr(), &topic, qos)
        .expect("best-effort subscriber");
    let mut pubr = pub_node
        .publisher_with_qos::<Tick>(&topic, qos)
        .expect("best-effort publisher");

    let diag = sub_node
        .peer_path_diagnostics(pub_node.endpoint_addr())
        .expect("path diagnostics")
        .expect("cached subscriber connection");
    assert!(
        diag.max_datagram_size.is_some(),
        "iroh loopback should negotiate QUIC datagrams"
    );

    let got = poll_for(Duration::from_secs(5), || {
        pubr.send(&Tick {
            seq: 1,
            payload: 44_004,
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(50));
        while let Some(sample) = sub.take().unwrap() {
            if sample.header().payload == 44_004 {
                return Some(*sample.header());
            }
        }
        None
    })
    .expect("best-effort datagram subscriber should receive");

    assert_eq!(got.payload, 44_004);
    assert!(pubr.stats().bytes_sent > 0);
    assert!(sub.stats().bytes_received > 0);
}

#[test]
fn node_publisher_fans_out_to_local_shm_and_remote_iroh() {
    let _guard = node_test_guard();
    let identity = unique_name("node_pub_dual");
    let local_sub_identity = unique_name("node_local_sub_dual");
    let topic = unique_name("interop/dual");

    let pub_node = Node::builder()
        .no_relay()
        .identity(&identity)
        .bind()
        .expect("publisher node");
    pub_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("publisher addresses");

    let local_sub_node = Node::builder()
        .no_relay()
        .identity(local_sub_identity)
        .bind()
        .expect("local subscriber node");
    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut local_sub = local_sub_node
        .subscriber::<Tick>(identity.as_str(), &topic)
        .unwrap();

    let remote_sub_side = RemoteTransport::builder(&topic)
        .no_relay()
        .peer(pub_node.endpoint_addr())
        .build_blocking()
        .expect("remote subscriber transport");
    let mut remote_sub = remote_sub_side.subscriber::<Tick>().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_local = false;
    let mut got_remote = false;
    while Instant::now() < deadline && !(got_local && got_remote) {
        pubr.send(&Tick {
            seq: 1,
            payload: 33_003,
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(50));

        while let Some(sample) = local_sub.take().unwrap() {
            if sample.header().payload == 33_003 {
                got_local = true;
                break;
            }
        }
        while let Some(sample) = remote_sub.take().unwrap() {
            if sample.header().payload == 33_003 {
                got_remote = true;
                break;
            }
        }
    }

    assert!(got_local, "local SHM subscriber should receive");
    assert!(got_remote, "remote iroh subscriber should receive");
}

#[test]
fn system_did_routes_topic_locally_without_peer_argument() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/pose");

    let pub_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system subscriber node");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node.subscribe::<Tick>(&topic).unwrap();

    pubr.send(&Tick {
        seq: 1,
        payload: 44_004,
    })
    .unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("system subscriber should receive via local SHM");
    assert_eq!(got.header().payload, 44_004);
}

#[test]
fn system_did_namespace_is_independent_from_process_identity() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/shared_topic");

    let pub_node = Node::builder()
        .no_relay()
        .identity(unique_name("system_process_a"))
        .system_did(&system_did)
        .bind()
        .expect("system publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .identity(unique_name("system_process_b"))
        .system_did(&system_did)
        .bind()
        .expect("system subscriber node");

    assert_ne!(
        pub_node.endpoint_id(),
        sub_node.endpoint_id(),
        "process transport identities stay independent"
    );
    assert_eq!(pub_node.system_did(), Some(system_did.as_str()));
    assert_eq!(sub_node.system_did(), Some(system_did.as_str()));

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node.subscribe::<Tick>(&topic).unwrap();

    pubr.send(&Tick {
        seq: 1,
        payload: 66_006,
    })
    .unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("same system DID should share a topic namespace locally");
    assert_eq!(got.header().payload, 66_006);
}

#[test]
fn different_system_dids_isolate_the_same_topic_key() {
    let _guard = node_test_guard();
    let system_a = unique_system_did();
    let system_b = unique_system_did();
    let topic = unique_name("system/same_topic_key");

    let pub_node = Node::builder()
        .no_relay()
        .system_did(&system_a)
        .bind()
        .expect("system A publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .system_did(&system_b)
        .bind()
        .expect("system B subscriber node");

    let _pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let err = match sub_node.subscribe::<Tick>(&topic) {
        Ok(_) => panic!("same topic key in a different system DID must not attach locally"),
        Err(err) => err,
    };
    assert!(
        matches!(err, quicbit::Error::ServiceNotFound(_)),
        "got {err:?}"
    );
}

#[test]
fn system_did_subscribe_falls_back_to_iroh_route_when_not_local() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/remote_pose");
    let route_topic = format!("{system_did}::{topic}");

    let remote_pub_side = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .expect("remote publisher transport");
    remote_pub_side
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("remote publisher addresses");

    let sub_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system subscriber node");
    sub_node
        .add_topic_route(&topic, remote_pub_side.endpoint_addr())
        .unwrap();

    let mut pubr = remote_pub_side.publisher::<Tick>().unwrap();
    let mut sub = sub_node.subscribe::<Tick>(&topic).unwrap();

    let got = poll_for(Duration::from_secs(5), || {
        let mut loan = pubr.loan(0).unwrap();
        loan.header = Tick {
            seq: 1,
            payload: 55_005,
        };
        pubr.publish(loan).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        while let Some(sample) = sub.take().unwrap() {
            if sample.header().payload == 55_005 {
                return Some(*sample.header());
            }
        }
        None
    })
    .expect("system subscriber should receive via iroh route");

    assert_eq!(got.payload, 55_005);
}

#[test]
fn system_did_subscribe_can_use_topic_agnostic_system_peer() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/peer_pose");
    let route_topic = format!("{system_did}::{topic}");

    let remote_pub_side = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .expect("remote publisher transport");
    remote_pub_side
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("remote publisher addresses");

    let sub_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .expect("system subscriber node");
    sub_node
        .add_system_peer(remote_pub_side.endpoint_addr())
        .unwrap();

    let mut pubr = remote_pub_side.publisher::<Tick>().unwrap();
    let mut sub = sub_node.subscribe::<Tick>(&topic).unwrap();

    let got = poll_for(Duration::from_secs(5), || {
        let mut loan = pubr.loan(0).unwrap();
        loan.header = Tick {
            seq: 1,
            payload: 77_007,
        };
        pubr.publish(loan).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        while let Some(sample) = sub.take().unwrap() {
            if sample.header().payload == 77_007 {
                return Some(*sample.header());
            }
        }
        None
    })
    .expect("system subscriber should receive through system peer fallback");

    assert_eq!(got.payload, 77_007);
}

#[test]
fn system_did_requires_did_key() {
    let _guard = node_test_guard();
    let err = match Node::builder()
        .no_relay()
        .system_did("did:name:not-yet")
        .bind()
    {
        Ok(_) => panic!("system_did should require did:key for now"),
        Err(err) => err,
    };
    assert!(
        matches!(err, quicbit::Error::InvalidArgument(_)),
        "got {err:?}"
    );
}

#[test]
fn system_topic_helpers_require_system_did() {
    let _guard = node_test_guard();
    let node = Node::builder()
        .no_relay()
        .identity(unique_name("plain_node"))
        .bind()
        .unwrap();
    let topic = unique_name("system/requires_did");

    let sub_err = match node.subscribe::<Tick>(&topic) {
        Ok(_) => panic!("Node::subscribe(topic) is only for system DID mode"),
        Err(err) => err,
    };
    assert!(
        matches!(sub_err, quicbit::Error::InvalidArgument(_)),
        "got {sub_err:?}"
    );

    let route_err = node
        .add_topic_route(&topic, node.endpoint_addr())
        .expect_err("topic routes require system DID mode");
    assert!(
        matches!(route_err, quicbit::Error::InvalidArgument(_)),
        "got {route_err:?}"
    );

    let peer_err = node
        .add_system_peer(node.endpoint_addr())
        .expect_err("system peers require system DID mode");
    assert!(
        matches!(peer_err, quicbit::Error::InvalidArgument(_)),
        "got {peer_err:?}"
    );
}

#[test]
fn ephemeral_key_changes_each_bind() {
    let _guard = node_test_guard();
    let id1 = Node::builder().no_relay().bind().unwrap().endpoint_id();
    let id2 = Node::builder().no_relay().bind().unwrap().endpoint_id();
    assert_ne!(id1, id2, "fresh keys each time when no path is supplied");
}

#[test]
fn topic_validation_rejects_bad_chars() {
    let _guard = node_test_guard();
    use quicbit::Error;
    let node = Node::builder().no_relay().identity("v").bind().unwrap();

    // Empty topic.
    assert!(matches!(
        node.publisher::<Tick>(""),
        Err(Error::InvalidArgument(_))
    ));
    // Disallowed char (space).
    assert!(matches!(
        node.publisher::<Tick>("rover pose"),
        Err(Error::InvalidArgument(_))
    ));
    // Disallowed char (colon).
    assert!(matches!(
        node.subscriber::<Tick>("v", "rover:pose"),
        Err(Error::InvalidArgument(_))
    ));
    // The allowed set still passes.
    assert!(node.publisher::<Tick>("rover/pose.v2-final_1").is_ok());
}

/// Connection from an un-allowlisted peer must be rejected by the
/// accept loop before any data flows. We exercise this by binding a
/// publisher with an empty allowlist (`.allow_peer(<unrelated>)`)
/// and then subscribing from a node whose endpoint id is NOT in
/// the list. The subscriber's `take()` should never see a sample
/// because no stream is served.
#[test]
fn rejects_unallowlisted_peer() {
    let _guard = node_test_guard();
    use quicbit::Error;

    // An "intended" peer whose key won't actually dial us — we
    // just need *some* allowlisted id so the publisher is in
    // closed-not-open mode.
    let stranger_id = Node::builder().no_relay().bind().unwrap().endpoint_id();

    let pub_node = Node::builder()
        .no_relay()
        .identity("rejector")
        .allow_peer(stranger_id)
        .bind()
        .expect("publisher node");

    pub_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("publisher addresses");

    let sub_node = Node::builder()
        .no_relay()
        .identity("attacker")
        .bind()
        .expect("subscriber node");

    let _pubr = pub_node.publisher::<Tick>("blocked/topic").unwrap();

    // The dial succeeds at the QUIC layer; quicbit then closes the
    // connection because the subscriber's endpoint id is not in
    // the allowlist. Subsequent take() observes the disconnect.
    let mut sub = sub_node
        .subscriber::<Tick>(pub_node.endpoint_addr(), "blocked/topic")
        .expect("subscribe handshake (over wire)");

    // Spin a little to give the publisher time to send/close.
    let _ = poll_for(Duration::from_millis(300), || match sub.take() {
        Err(Error::Disconnected) => Some(()),
        Ok(Some(_)) => panic!("attacker should not receive any sample"),
        _ => None,
    });
    // Either Disconnected or no sample is acceptable; the
    // contract is that no Tick samples reach the attacker.
    if let Ok(Some(_)) = sub.take() {
        panic!("attacker received a Tick despite ACL");
    }
}

// ---- system-DID routing parity for the streaming modes ----
// que/ans, put/ack, pip now mirror req/res: local SHM, then explicit
// topic route, then topic-agnostic system peer. A standalone
// RemoteTransport server (no SHM) stands in for the remote endpoint.

#[test]
fn system_did_que_ans_uses_topic_route_when_not_local() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/search_route");
    let route_topic = format!("{system_did}::{topic}");

    let server = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_ques::<RangeQue, Hit, _>(|q| {
            (0..q.count).map(|o| Hit { value: q.start + o }).collect()
        })
        .unwrap();

    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .unwrap();
    client_node
        .add_topic_route(&topic, server.endpoint_addr())
        .unwrap();

    let mut client = client_node.que::<RangeQue, Hit>(&topic).unwrap();
    let mut answers = client.send(&RangeQue { start: 1, count: 3 }).unwrap();
    let mut got = Vec::new();
    while let Some(a) = answers.next().unwrap() {
        got.push(a.header().value);
    }
    assert_eq!(got, vec![1, 2, 3]);
}

#[test]
fn system_did_que_ans_can_use_topic_agnostic_system_peer() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/search_peer");
    let route_topic = format!("{system_did}::{topic}");

    let server = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_ques::<RangeQue, Hit, _>(|q| {
            (0..q.count).map(|o| Hit { value: q.start + o }).collect()
        })
        .unwrap();

    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .unwrap();
    client_node.add_system_peer(server.endpoint_addr()).unwrap();

    let mut client = client_node.que::<RangeQue, Hit>(&topic).unwrap();
    let mut answers = client.send(&RangeQue { start: 7, count: 2 }).unwrap();
    let mut got = Vec::new();
    while let Some(a) = answers.next().unwrap() {
        got.push(a.header().value);
    }
    assert_eq!(got, vec![7, 8]);
}

#[test]
fn system_did_put_ack_uses_topic_route_when_not_local() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/upload_route");
    let route_topic = format!("{system_did}::{topic}");

    let server = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_puts::<LogChunk, UploadAck, _>(|puts| UploadAck {
            count: puts.len() as u32,
            sum: puts.iter().map(|p| p.value).sum(),
        })
        .unwrap();

    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .unwrap();
    client_node
        .add_topic_route(&topic, server.endpoint_addr())
        .unwrap();

    let mut client = client_node.put::<LogChunk, UploadAck>(&topic).unwrap();
    let mut upload = client.open().unwrap();
    for value in [5, 6, 7] {
        upload.send(&LogChunk { value }).unwrap();
    }
    let ack = upload.finish().unwrap();
    assert_eq!(ack.header().count, 3);
    assert_eq!(ack.header().sum, 18);
}

#[test]
fn system_did_put_ack_can_use_topic_agnostic_system_peer() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/upload_peer");
    let route_topic = format!("{system_did}::{topic}");

    let server = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_puts::<LogChunk, UploadAck, _>(|puts| UploadAck {
            count: puts.len() as u32,
            sum: puts.iter().map(|p| p.value).sum(),
        })
        .unwrap();

    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .unwrap();
    client_node.add_system_peer(server.endpoint_addr()).unwrap();

    let mut client = client_node.put::<LogChunk, UploadAck>(&topic).unwrap();
    let mut upload = client.open().unwrap();
    upload.send(&LogChunk { value: 100 }).unwrap();
    let ack = upload.finish().unwrap();
    assert_eq!(ack.header().count, 1);
    assert_eq!(ack.header().sum, 100);
}

#[test]
fn system_did_pip_uses_topic_route_when_not_local() {
    let _guard = node_test_guard();
    let system_did = unique_system_did();
    let topic = unique_name("system/session_route");
    let route_topic = format!("{system_did}::{topic}");

    let server = RemoteTransport::builder(route_topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_pips::<ClientMsg, ServerMsg, _>(|msgs| {
            msgs.iter()
                .map(|m| ServerMsg {
                    value: m.value * 10,
                })
                .collect()
        })
        .unwrap();

    let client_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .bind()
        .unwrap();
    client_node
        .add_topic_route(&topic, server.endpoint_addr())
        .unwrap();

    let mut client = client_node.pip::<ClientMsg, ServerMsg>(&topic).unwrap();
    let mut pip = client.open().unwrap();
    for value in [1, 2] {
        pip.send(&ClientMsg { value }).unwrap();
    }
    pip.finish_send().unwrap();
    let mut got = Vec::new();
    while let Some(reply) = pip.next().unwrap() {
        got.push(reply.header().value);
    }
    assert_eq!(got, vec![10, 20]);
}

// ---- empty-stream edge cases (local SHM) ----

#[test]
fn que_ans_empty_answer_stream() {
    let _guard = node_test_guard();
    let server_node = Node::builder()
        .no_relay()
        .identity("empty_ans_srv")
        .bind()
        .unwrap();
    let client_node = Node::builder()
        .no_relay()
        .identity("empty_ans_cli")
        .bind()
        .unwrap();
    let topic = unique_name("empty/que");

    let mut server = server_node.ans::<RangeQue, Hit>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(3), || {
            let (_que, ans) = server.take().unwrap()?;
            ans.finish().unwrap(); // zero answers
            Some(())
        })
        .expect("served");
    });

    let mut client = client_node
        .que_client::<RangeQue, Hit>("empty_ans_srv", &topic)
        .unwrap();
    let mut answers = client.send(&RangeQue { start: 0, count: 0 }).unwrap();
    assert!(answers.next().unwrap().is_none(), "no answers expected");
    handle.join().unwrap();
}

#[test]
fn put_ack_empty_upload() {
    let _guard = node_test_guard();
    let server_node = Node::builder()
        .no_relay()
        .identity("empty_put_srv")
        .bind()
        .unwrap();
    let client_node = Node::builder()
        .no_relay()
        .identity("empty_put_cli")
        .bind()
        .unwrap();
    let topic = unique_name("empty/put");

    let mut server = server_node.ack::<LogChunk, UploadAck>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(3), || {
            let mut puts = server.take().unwrap()?;
            let mut count = 0;
            while puts.next().unwrap().is_some() {
                count += 1;
            }
            puts.ack(&UploadAck { count, sum: 0 }).unwrap();
            Some(())
        })
        .expect("served");
    });

    let mut client = client_node
        .put_client::<LogChunk, UploadAck>("empty_put_srv", &topic)
        .unwrap();
    let upload = client.open().unwrap();
    let ack = upload.finish().unwrap(); // zero puts
    assert_eq!(ack.header().count, 0);
    handle.join().unwrap();
}

#[test]
fn pip_empty_session() {
    let _guard = node_test_guard();
    let server_node = Node::builder()
        .no_relay()
        .identity("empty_pip_srv")
        .bind()
        .unwrap();
    let client_node = Node::builder()
        .no_relay()
        .identity("empty_pip_cli")
        .bind()
        .unwrap();
    let topic = unique_name("empty/pip");

    let mut server = server_node
        .pip_server::<ClientMsg, ServerMsg>(&topic)
        .unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(3), || {
            let mut pip = server.take().unwrap()?;
            while pip.next().unwrap().is_some() {}
            pip.finish_send().unwrap(); // zero replies
            Some(())
        })
        .expect("served");
    });

    let mut client = client_node
        .pip_client::<ClientMsg, ServerMsg>("empty_pip_srv", &topic)
        .unwrap();
    let mut pip = client.open().unwrap();
    pip.finish_send().unwrap(); // zero messages
    assert!(pip.next().unwrap().is_none(), "no replies expected");
    handle.join().unwrap();
}

// ---- per-mode stats over iroh (Node client + standalone server) ----

#[test]
fn que_ans_client_stats_over_iroh() {
    let _guard = node_test_guard();
    let topic = unique_name("stats/queans");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_ques::<RangeQue, Hit, _>(|q| {
            (0..q.count).map(|o| Hit { value: q.start + o }).collect()
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .que_client::<RangeQue, Hit>(server.endpoint_addr(), &topic)
        .unwrap();
    let mut answers = client.send(&RangeQue { start: 0, count: 4 }).unwrap();
    while answers.next().unwrap().is_some() {}
    drop(answers);

    let stats = client.stats();
    assert_eq!(stats.messages_out, 1, "one que sent");
    assert_eq!(stats.messages_in, 4, "four answers received");
    assert!(stats.bytes_out > 0 && stats.bytes_in > 0);
}

#[test]
fn put_ack_client_stats_over_iroh() {
    let _guard = node_test_guard();
    let topic = unique_name("stats/putack");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_puts::<LogChunk, UploadAck, _>(|puts| UploadAck {
            count: puts.len() as u32,
            sum: puts.iter().map(|p| p.value).sum(),
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .put_client::<LogChunk, UploadAck>(server.endpoint_addr(), &topic)
        .unwrap();
    let mut upload = client.open().unwrap();
    for value in [1, 2, 3, 4, 5] {
        upload.send(&LogChunk { value }).unwrap();
    }
    let _ack = upload.finish().unwrap();

    let stats = client.stats();
    assert_eq!(stats.messages_out, 5, "five puts sent");
    assert_eq!(stats.messages_in, 1, "one ack received");
}

#[test]
fn pip_client_stats_over_iroh() {
    let _guard = node_test_guard();
    let topic = unique_name("stats/pip");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_pips::<ClientMsg, ServerMsg, _>(|msgs| {
            msgs.iter().map(|m| ServerMsg { value: m.value }).collect()
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .pip_client::<ClientMsg, ServerMsg>(server.endpoint_addr(), &topic)
        .unwrap();
    let mut pip = client.open().unwrap();
    for value in [1, 2, 3] {
        pip.send(&ClientMsg { value }).unwrap();
    }
    pip.finish_send().unwrap();
    while pip.next().unwrap().is_some() {}
    drop(pip);

    let stats = client.stats();
    assert_eq!(stats.messages_out, 3, "three client messages sent");
    assert_eq!(stats.messages_in, 3, "three server messages received");
}

// ---- req/res: sequential calls + type-mismatch rejection ----

#[test]
fn req_res_sequential_calls_preserve_req_ids() {
    let _guard = node_test_guard();
    let topic = unique_name("calc/seq");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    server
        .serve_requests::<Add, Sum, _>(|req| Sum {
            value: req.a + req.b,
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .req_client::<Add, Sum>(server.endpoint_addr(), &topic)
        .unwrap();

    let mut last_id = 0;
    for i in 0..5 {
        let res = client.call(&Add { a: i, b: 1 }).unwrap();
        assert_eq!(res.header().value, i + 1);
        assert!(res.req_id() > last_id, "req_id must strictly increase");
        last_id = res.req_id();
    }
    assert_eq!(client.stats().messages_out, 5);
}

#[test]
fn req_res_type_mismatch_is_rejected() {
    let _guard = node_test_guard();
    let topic = unique_name("calc/mismatch");
    let server = RemoteTransport::builder(&topic)
        .no_relay()
        .build_blocking()
        .unwrap();
    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    // Server expects Add (8 bytes); client will request with Sum
    // (4 bytes) — different size → different type hash → rejected.
    server
        .serve_requests::<Add, Sum, _>(|req| Sum {
            value: req.a + req.b,
        })
        .unwrap();

    let client_node = Node::builder().no_relay().bind().unwrap();
    let mut client = client_node
        .req_client::<Sum, Sum>(server.endpoint_addr(), &topic)
        .unwrap();
    let result = client.call(&Sum { value: 1 });
    assert!(result.is_err(), "type mismatch must surface as an error");
}
