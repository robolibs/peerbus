//! End-to-end tests for `Node`.
//!
//! With iceoryx2 as the local backend, same-host routing keys off
//! the iceoryx2 service name we compose from
//! `(identity, topic)`. The subscriber asks iceoryx2 whether
//! that service exists locally; if yes → SHM, if no → dial via
//! iroh. So all the local tests below use `.identity(...)` so the
//! routing has something to match.


use std::time::{Duration, Instant};

use bytemuck::{Pod, Zeroable};
use iceoryx2::prelude::ZeroCopySend;
use quicbit::Node;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq, ZeroCopySend)]
struct Tick {
    seq: u32,
    payload: u32,
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

#[test]
fn local_routing_two_nodes_same_process() {
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

    pubr.send(Tick { seq: 1, payload: 42 }).unwrap();

    let sample = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive");
    assert_eq!(*sample, Tick { seq: 1, payload: 42 });
}

#[test]
fn endpoint_id_is_stable_with_key_file() {
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
    // Subscribe by NAME — iceoryx2 service name is composed from
    // it, and `open_existing` succeeds because the publisher is
    // already up on this host.
    let mut sub = sub_node
        .subscriber::<Tick>("sensors", "imu/raw")
        .unwrap();

    pubr.send(Tick { seq: 7, payload: 700 }).unwrap();
    let s = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive");
    assert_eq!(*s, Tick { seq: 7, payload: 700 });
}

#[test]
fn ephemeral_key_changes_each_bind() {
    let id1 = Node::builder().no_relay().bind().unwrap().endpoint_id();
    let id2 = Node::builder().no_relay().bind().unwrap().endpoint_id();
    assert_ne!(id1, id2, "fresh keys each time when no path is supplied");
}
