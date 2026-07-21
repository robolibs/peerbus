//! Phase 1 — `*_with_qos` constructors + large-payload round-trips for
//! req/res, que/ans, put/ack, pip.
//!
//! These drive the new `*_with_qos` constructors end-to-end with a small
//! `chunk_bytes`, confirming the QoS plumbing and that multi-hundred-KiB
//! payloads round-trip byte-exact.
//!
//! NOTE on transport: two `Node`s on the same host always resolve a
//! topic through shared memory (the SHM service is host-global), so
//! these in-process tests exercise the local SHM path. The iroh chunk
//! codec they configure (`write_item_chunked`/`read_item_chunked`) is
//! shared with pub/sub — whose iroh chunking/reassembly is covered in
//! `wire_parsers.rs` — and the v3 item handshake is unit-tested in
//! `node::tests`. Genuine end-to-end *iroh* chunking for these four
//! modes (including a payload past the old 64 MiB single-frame cap) is
//! verified in Phase 3, once their standalone `RemoteTransport` servers
//! provide a no-SHM peer to force the iroh leg.

use std::time::{Duration, Instant};

use peerbus::{Node, TopicQos};

static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[datapod::datapod]
struct Blob {
    id: u32,
    #[dp(bytes)]
    data: Vec<u8>,
}

#[datapod::datapod]
struct Tiny {
    seq: u32,
}

#[datapod::datapod]
struct BlobAck {
    count: u32,
    total_len: u64,
    checksum: u64,
}

fn guard() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

fn unique(stem: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("{stem}_{pid}_{n}")
}

/// Deterministic byte pattern so the receiver can verify content.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut h = 1469598103934665603u64; // FNV-1a offset basis
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
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

/// QoS with a small chunk size so a few hundred KiB spans many chunks.
fn chunky_qos() -> TopicQos {
    TopicQos::reliable()
        .with_chunk_bytes(64 * 1024)
        .with_max_message_bytes(32 * 1024 * 1024)
        .with_max_inflight_bytes(64 * 1024 * 1024)
}

fn server_node(stem: &str) -> Node {
    let node = Node::builder()
        .no_relay()
        .ephemeral()
        .label(unique(stem))
        .bind()
        .expect("server node");
    node.wait_for_direct_addresses(Duration::from_secs(5))
        .expect("server addresses");
    node
}

fn client_node(stem: &str) -> Node {
    Node::builder()
        .no_relay()
        .ephemeral()
        .label(unique(stem))
        .bind()
        .expect("client node")
}

#[test]
fn reqres_chunks_large_payload_both_directions() {
    let _g = guard();
    let topic = unique("chunk/reqres");
    let server = server_node("reqres_chunk_server");
    let client = client_node("reqres_chunk_client");

    let mut srv = server
        .req_server_with_qos::<Blob, Blob>(&topic, chunky_qos())
        .unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let (req, reply) = srv.take().unwrap()?;
            // Echo the payload straight back (also chunked on the way out).
            reply
                .respond(&Blob {
                    id: req.header().id,
                    data: req.payload().to_vec(),
                })
                .unwrap();
            Some(())
        })
        .expect("server received request");
    });

    let payload = pattern(300 * 1024);
    let mut cli = client
        .req_client_with_qos::<Blob, Blob>(server.endpoint_addr(), &topic, chunky_qos())
        .unwrap();
    let res = cli
        .call(&Blob {
            id: 7,
            data: payload.clone(),
        })
        .unwrap();
    assert_eq!(res.header().id, 7);
    assert_eq!(res.payload(), payload.as_slice());
    handle.join().unwrap();
}

#[test]
fn queans_chunks_large_answers() {
    let _g = guard();
    let topic = unique("chunk/queans");
    let server = server_node("queans_chunk_server");
    let client = client_node("queans_chunk_client");

    let answers = [pattern(200 * 1024), pattern(150 * 1024), pattern(90 * 1024)];
    let server_answers = answers.clone();
    let mut srv = server
        .ans_with_qos::<Tiny, Blob>(&topic, chunky_qos())
        .unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let (_que, mut ans) = srv.take().unwrap()?;
            for (i, a) in server_answers.iter().enumerate() {
                ans.send(&Blob {
                    id: i as u32,
                    data: a.clone(),
                })
                .unwrap();
            }
            ans.finish().unwrap();
            Some(())
        })
        .expect("server received que");
    });

    let mut cli = client
        .que_client_with_qos::<Tiny, Blob>(server.endpoint_addr(), &topic, chunky_qos())
        .unwrap();
    let mut stream = cli.send(&Tiny { seq: 1 }).unwrap();
    let mut got = Vec::new();
    while let Some(ans) = stream.next().unwrap() {
        got.push((ans.header().id, ans.payload().to_vec()));
    }
    assert_eq!(got.len(), answers.len());
    for (i, a) in answers.iter().enumerate() {
        assert_eq!(got[i].0, i as u32);
        assert_eq!(&got[i].1, a, "answer {i} payload mismatch");
    }
    handle.join().unwrap();
}

#[test]
fn putack_chunks_large_puts() {
    let _g = guard();
    let topic = unique("chunk/putack");
    let server = server_node("putack_chunk_server");
    let client = client_node("putack_chunk_client");

    let puts = [pattern(220 * 1024), pattern(130 * 1024)];
    let mut srv = server
        .ack_with_qos::<Blob, BlobAck>(&topic, chunky_qos())
        .unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let mut puts = srv.take().unwrap()?;
            let mut count = 0u32;
            let mut total_len = 0u64;
            let mut all = Vec::new();
            while let Some(item) = puts.next().unwrap() {
                count += 1;
                total_len += item.payload().len() as u64;
                all.extend_from_slice(item.payload());
            }
            puts.ack(&BlobAck {
                count,
                total_len,
                checksum: checksum(&all),
            })
            .unwrap();
            Some(())
        })
        .expect("server received puts");
    });

    let mut cli = client
        .put_client_with_qos::<Blob, BlobAck>(server.endpoint_addr(), &topic, chunky_qos())
        .unwrap();
    let mut sender = cli.open().unwrap();
    let mut expected = Vec::new();
    for (i, p) in puts.iter().enumerate() {
        sender
            .send(&Blob {
                id: i as u32,
                data: p.clone(),
            })
            .unwrap();
        expected.extend_from_slice(p);
    }
    let ack = sender.finish().unwrap();
    assert_eq!(ack.header().count, puts.len() as u32);
    assert_eq!(ack.header().total_len, expected.len() as u64);
    assert_eq!(ack.header().checksum, checksum(&expected));
    handle.join().unwrap();
}

#[test]
fn pip_chunks_large_messages_both_directions() {
    let _g = guard();
    let topic = unique("chunk/pip");
    let server = server_node("pip_chunk_server");
    let client = client_node("pip_chunk_client");

    let mut srv = server
        .pip_server_with_qos::<Blob, Blob>(&topic, chunky_qos())
        .unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(5), || {
            let mut pip = srv.take().unwrap()?;
            // Echo each client message back, then close.
            while let Some(msg) = pip.next().unwrap() {
                pip.send(&Blob {
                    id: msg.header().id,
                    data: msg.payload().to_vec(),
                })
                .unwrap();
            }
            pip.finish_send().unwrap();
            Some(())
        })
        .expect("server accepted pip");
    });

    let payload = pattern(256 * 1024);
    let mut cli = client
        .pip_client_with_qos::<Blob, Blob>(server.endpoint_addr(), &topic, chunky_qos())
        .unwrap();
    let mut pip = cli.open().unwrap();
    pip.send(&Blob {
        id: 9,
        data: payload.clone(),
    })
    .unwrap();
    pip.finish_send().unwrap();
    let echoed = pip.next().unwrap().expect("echo message");
    assert_eq!(echoed.header().id, 9);
    assert_eq!(echoed.payload(), payload.as_slice());
    handle.join().unwrap();
}
