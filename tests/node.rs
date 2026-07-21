//! End-to-end tests for `Node`.
//!
//! With the SHM local backend, same-host routing keys off the
//! service name we compose from `(endpoint_id, topic)`. The
//! subscriber asks the backend whether that service exists
//! locally; if yes → SHM, if no → dial via iroh. Peers are
//! addressed only by id, so the local tests below create the
//! publisher/server on one node and address it from the other
//! by that node's `endpoint_id()`.

use std::time::{Duration, Instant};

use peerbus::transport::{PublisherOps, SubscriberOps};
use peerbus::{LocalConfig, Node, RemoteTransport, TopicQos, Transport};

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

fn node_test_guard() -> std::sync::MutexGuard<'static, ()> {
    NODE_TEST_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

#[test]
fn local_routing_two_nodes_same_process() {
    let _guard = node_test_guard();
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("publisher node");

    let sub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>("rover/pose").unwrap();
    let mut sub = sub_node
        .subscriber::<Tick>(pub_node.endpoint_id(), "rover/pose")
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
        .ephemeral()
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

    let topic = unique_name("local/latest");
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node
        .subscriber_with_qos::<Tick>(pub_node.endpoint_id(), &topic, TopicQos::latest())
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

    let topic = unique_name("local/reliable");
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node
        .subscriber_with_qos::<Tick>(pub_node.endpoint_id(), &topic, TopicQos::reliable())
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

    let topic = unique_name("local/reliable_lag");
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub = sub_node
        .subscriber_with_qos::<Tick>(pub_node.endpoint_id(), &topic, TopicQos::reliable())
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
        Err(peerbus::Error::Lagged { dropped: 2 })
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

    let topic = unique_name("local/latest_multi");
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .local_config(cfg)
        .bind()
        .expect("publisher node");
    let sub_node_a = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("subscriber node a");
    let sub_node_b = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("subscriber node b");

    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut sub_a = sub_node_a
        .subscriber_with_qos::<Tick>(pub_node.endpoint_id(), &topic, TopicQos::latest())
        .unwrap();
    let mut sub_b = sub_node_b
        .subscriber_with_qos::<Tick>(pub_node.endpoint_id(), &topic, TopicQos::latest())
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
fn node_req_res_routes_locally_by_name() {
    let _guard = node_test_guard();
    let topic = unique_name("calc/add");

    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
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
        .req_client::<Add, Sum>(server_node.endpoint_id(), &topic)
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
        .ephemeral()
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
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

    let client_node = Node::builder().ephemeral().no_relay().bind().expect("client node");
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

    let client_node = Node::builder().ephemeral().no_relay().bind().expect("client node");
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
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
    // `allow_any_peer` rather than a real allowlist: the inbound peer is a
    // `RemoteTransport`, whose endpoint key is ephemeral — its id does not
    // exist until it is built, and building it needs this node's address.
    // There is nothing to allowlist at builder time.
    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_any_peer()
        .bind()
        .unwrap();
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    let mut server = server_node.que_server::<RangeQue, Hit>(&topic).unwrap();
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
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
    // See `node_queans_server_serves_standalone_client`: the RemoteTransport
    // client's ephemeral id is unknowable before this node binds.
    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_any_peer()
        .bind()
        .unwrap();
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    let mut server = server_node.put_server::<LogChunk, UploadAck>(&topic).unwrap();
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
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
    // See `node_queans_server_serves_standalone_client`: the RemoteTransport
    // client's ephemeral id is unknowable before this node binds.
    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_any_peer()
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
    let topic = unique_name("search/local");

    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("client node");

    let mut server = server_node.que_server::<RangeQue, Hit>(&topic).unwrap();
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
        .que_client::<RangeQue, Hit>(server_node.endpoint_id(), &topic)
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
        .ephemeral()
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("client node");

    let mut server = server_node.que_server::<RangeQue, Hit>(&topic).unwrap();
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
fn node_put_ack_routes_locally_by_name() {
    let _guard = node_test_guard();
    let topic = unique_name("logs/local");

    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("client node");

    let mut server = server_node.put_server::<LogChunk, UploadAck>(&topic).unwrap();
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
        .put_client::<LogChunk, UploadAck>(server_node.endpoint_id(), &topic)
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
        .ephemeral()
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("client node");

    let mut server = server_node.put_server::<LogChunk, UploadAck>(&topic).unwrap();
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
fn node_pip_routes_locally_by_name() {
    let _guard = node_test_guard();
    let topic = unique_name("session/local");

    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
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
        .pip_client::<ClientMsg, ServerMsg>(server_node.endpoint_id(), &topic)
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
        .ephemeral()
        .bind()
        .expect("server node");
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
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
fn endpoint_id_is_stable_with_secret_key() {
    let _guard = node_test_guard();
    let secret = iroh::SecretKey::generate();

    let id1 = {
        let node = Node::builder()
            .no_relay()
            .secret_key(secret.clone())
            .bind()
            .expect("first bind");
        node.endpoint_id()
    };

    let id2 = {
        let node = Node::builder()
            .no_relay()
            .secret_key(secret.clone())
            .bind()
            .expect("second bind");
        node.endpoint_id()
    };

    assert_eq!(id1, id2, "same secret key yields the same EndpointId");
}

#[test]
fn local_routing_by_endpoint_id() {
    let _guard = node_test_guard();
    let pub_node = Node::builder().no_relay().ephemeral().bind().unwrap();
    let sub_node = Node::builder().no_relay().ephemeral().bind().unwrap();

    let mut pubr = pub_node.publisher::<Tick>("imu/raw").unwrap();
    // Subscribe by the publisher node's `endpoint_id()` — the local
    // service name is composed from it, and `open_existing` succeeds
    // because the publisher is already up on this host.
    let mut sub = sub_node
        .subscriber::<Tick>(pub_node.endpoint_id(), "imu/raw")
        .unwrap();

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
    let topic = unique_name("interop/node_to_remote");

    // The inbound peer is a RemoteTransport with an ephemeral key that only
    // exists once it is built — and building it needs this node's address —
    // so there is no id to allowlist. Opt out explicitly instead.
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_any_peer()
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
        .ephemeral()
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

    // Both sides are Nodes. Bind the subscriber first so the publisher can
    // allowlist it by its real `endpoint_id()`. This is the secure path.
    let sub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("subscriber node");

    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_peer(sub_node.endpoint_id())
        .bind()
        .expect("publisher node");
    pub_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("publisher addresses");

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
    let topic = unique_name("interop/dual");

    // The remote leg of the fan-out is a RemoteTransport (ephemeral id,
    // built after this node), so there is nothing to allowlist.
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_any_peer()
        .bind()
        .expect("publisher node");
    pub_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("publisher addresses");

    let local_sub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("local subscriber node");
    let mut pubr = pub_node.publisher::<Tick>(&topic).unwrap();
    let mut local_sub = local_sub_node
        .subscriber::<Tick>(pub_node.endpoint_id(), &topic)
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
fn ephemeral_key_changes_each_bind() {
    let _guard = node_test_guard();
    let id1 = Node::builder().ephemeral().no_relay().bind().unwrap().endpoint_id();
    let id2 = Node::builder().ephemeral().no_relay().bind().unwrap().endpoint_id();
    assert_ne!(id1, id2, "fresh keys each time when no path is supplied");
}

#[test]
fn topic_validation_rejects_bad_chars() {
    let _guard = node_test_guard();
    use peerbus::Error;
    let node = Node::builder().no_relay().ephemeral().bind().unwrap();

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
        node.subscriber::<Tick>(node.endpoint_id(), "rover:pose"),
        Err(Error::InvalidArgument(_))
    ));
    // The allowed set still passes.
    assert!(node.publisher::<Tick>("rover/pose.v2-final_1").is_ok());
}

/// Connection from an un-allowlisted peer must be rejected by the
/// accept loop before any data flows.
///
/// All three ACL tests below share one shape, and the ordering in them
/// is load-bearing: the client's `req_client` is built *before* the
/// server registers its req/res service, so the client cannot attach to
/// the server's same-host SHM service and is forced onto the iroh path —
/// which is where the peer ACL lives. The server is then wired up to
/// answer, so the only thing that can make the call fail is the ACL.
///
/// Runs an `Add` call against `server_node` from `client_node` over
/// iroh and returns whether the call succeeded. Asserts the server never
/// observes the request when the connection was refused.
fn remote_call_is_served(server_node: &Node, client_node: &Node, topic: &str) -> bool {
    server_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");

    // Built before `req_server` below exists → no local SHM service to
    // attach to → genuine iroh path.
    let mut client = client_node
        .req_client::<Add, Sum>(server_node.endpoint_addr(), topic)
        .expect("remote req client");

    let mut server = server_node.req_server::<Add, Sum>(topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(3), || {
            let (req, reply) = server.take().unwrap()?;
            reply
                .respond(&Sum {
                    value: req.header().a + req.header().b,
                })
                .unwrap();
            Some(())
        })
        .is_some()
    });

    let call = client.call(&Add { a: 2, b: 40 });
    let served = handle.join().unwrap();
    match call {
        Ok(res) => {
            assert_eq!(res.header().value, 42);
            assert!(served, "a served call must have reached the server");
            true
        }
        Err(_) => {
            assert!(
                !served,
                "a rejected connection must never reach the req/res server"
            );
            false
        }
    }
}

/// Regression test for the deny-by-default flip: a node that configures
/// neither `.allow_peer(...)` nor `.allow_any_peer()` must REJECT every
/// inbound peer. Before the fix, this call succeeded.
#[test]
fn deny_by_default_rejects_inbound_peer_without_allowlist() {
    let _guard = node_test_guard();
    let topic = unique_name("acl/deny_by_default");

    // No allowlist, no `allow_any_peer` → deny all inbound.
    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("client node");

    assert!(
        !remote_call_is_served(&server_node, &client_node, &topic),
        "deny-by-default: an un-allowlisted peer must be refused, not served"
    );
}

/// A node WITH an allowlist rejects peers that are not on it.
#[test]
fn rejects_unallowlisted_peer() {
    let _guard = node_test_guard();
    let topic = unique_name("acl/not_on_list");

    // Some unrelated peer is allowlisted; the client below is not.
    let stranger_id = Node::builder().ephemeral().no_relay().bind().unwrap().endpoint_id();

    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_peer(stranger_id)
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("client node");

    assert!(
        !remote_call_is_served(&server_node, &client_node, &topic),
        "a peer missing from the allowlist must be refused"
    );
}

/// Positive control for the two rejection tests: the very same wire path
/// succeeds once the client's endpoint id IS on the allowlist. Without
/// this, the rejections above could be passing for the wrong reason.
#[test]
fn allowlisted_peer_is_accepted_over_iroh() {
    let _guard = node_test_guard();
    let topic = unique_name("acl/on_list");

    // Bind the client first so the server can allowlist its real id.
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .bind()
        .expect("client node");

    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .allow_peer(client_node.endpoint_id())
        .bind()
        .expect("server node");

    assert!(
        remote_call_is_served(&server_node, &client_node, &topic),
        "an allowlisted peer must be served over iroh"
    );
}

// ---- empty-stream edge cases (local SHM) ----

#[test]
fn que_ans_empty_answer_stream() {
    let _guard = node_test_guard();
    let server_node = Node::builder().no_relay().ephemeral().bind().unwrap();
    let client_node = Node::builder().no_relay().ephemeral().bind().unwrap();
    let topic = unique_name("empty/que");

    let mut server = server_node.que_server::<RangeQue, Hit>(&topic).unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(3), || {
            let (_que, ans) = server.take().unwrap()?;
            ans.finish().unwrap(); // zero answers
            Some(())
        })
        .expect("served");
    });

    let mut client = client_node
        .que_client::<RangeQue, Hit>(server_node.endpoint_id(), &topic)
        .unwrap();
    let mut answers = client.send(&RangeQue { start: 0, count: 0 }).unwrap();
    assert!(answers.next().unwrap().is_none(), "no answers expected");
    handle.join().unwrap();
}

#[test]
fn put_ack_empty_upload() {
    let _guard = node_test_guard();
    let server_node = Node::builder().no_relay().ephemeral().bind().unwrap();
    let client_node = Node::builder().no_relay().ephemeral().bind().unwrap();
    let topic = unique_name("empty/put");

    let mut server = server_node.put_server::<LogChunk, UploadAck>(&topic).unwrap();
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
        .put_client::<LogChunk, UploadAck>(server_node.endpoint_id(), &topic)
        .unwrap();
    let upload = client.open().unwrap();
    let ack = upload.finish().unwrap(); // zero puts
    assert_eq!(ack.header().count, 0);
    handle.join().unwrap();
}

#[test]
fn pip_empty_session() {
    let _guard = node_test_guard();
    let server_node = Node::builder().no_relay().ephemeral().bind().unwrap();
    let client_node = Node::builder().no_relay().ephemeral().bind().unwrap();
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
        .pip_client::<ClientMsg, ServerMsg>(server_node.endpoint_id(), &topic)
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
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

    let client_node = Node::builder().ephemeral().no_relay().bind().unwrap();
    let mut client = client_node
        .req_client::<Sum, Sum>(server.endpoint_addr(), &topic)
        .unwrap();
    let result = client.call(&Sum { value: 1 });
    assert!(result.is_err(), "type mismatch must surface as an error");
}
