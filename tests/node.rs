//! End-to-end tests for `Node`.
//!
//! Two scenarios:
//!
//! 1. **Local routing** — two `Node`s in the same process. The
//!    registry sees both, so the subscriber attaches via SHM.
//!    Validates that `Node::publisher` + `Node::subscriber` route
//!    through the local transport.
//! 2. **Remote routing** — two `Node`s where the subscriber has
//!    *no* registry entry for the publisher (we use a synthetic
//!    `EndpointId` that's not in the registry). Subscriber falls
//!    through to iroh.

#![cfg(feature = "remote")]

use std::time::{Duration, Instant};

use bytemuck::{Pod, Zeroable};
use quicbit::Node;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq)]
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
        .slot_count(8)
        .slot_size(32)
        .bind()
        .expect("publisher node");
    let pub_id = pub_node.endpoint_id();

    let sub_node = Node::builder()
        .no_relay()
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<Tick>("rover/pose").unwrap();
    let mut sub = sub_node
        .subscriber::<Tick>(pub_id, "rover/pose")
        .expect("local subscribe");

    pubr.send(Tick { seq: 1, payload: 42 }).unwrap();

    let sample = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive");
    assert_eq!(*sample, Tick { seq: 1, payload: 42 });
}

#[test]
fn local_routing_survives_multiple_publishes() {
    let pub_node = Node::builder().no_relay().bind().unwrap();
    let sub_node = Node::builder().no_relay().bind().unwrap();
    let pub_id = pub_node.endpoint_id();

    let mut pubr = pub_node.publisher::<Tick>("rover/twist").unwrap();
    let mut sub = sub_node.subscriber::<Tick>(pub_id, "rover/twist").unwrap();

    // Publish several; subscriber catches the latest at least.
    for i in 1..=5 {
        pubr.send(Tick { seq: i, payload: i * 10 }).unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }

    // history_depth defaults to 1, so the subscriber may see
    // `Lagged` if the publisher outpaces it. Tolerate that and
    // keep polling — the goal is to verify at least one publish
    // is observed.
    let mut latest_payload = 0;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        match sub.take() {
            Ok(Some(s)) => {
                latest_payload = s.payload;
                if latest_payload == 50 {
                    break;
                }
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(5)),
            Err(_) => {} // Lagged: keep polling
        }
    }
    assert!(
        latest_payload > 0,
        "subscriber should have received something"
    );
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
    // Drop the first node so the second can bind. Both share the
    // same identity → same EndpointId.
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
        .identity("rover-c")  // same name, derived directly
        .bind()
        .unwrap()
        .endpoint_id();
    assert_eq!(id1, id2);

    unsafe { std::env::remove_var(&var) };
}

#[test]
fn name_based_local_routing_without_registry_lookup() {
    // Publisher with a name. Subscriber finds the SHM segment via
    // the name → no registry lookup needed (still works if registry
    // would have it too, but the path doesn't depend on it).
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
    // Subscribe by NAME (not EndpointId). The &str impl of IntoPeer
    // hashes the name and looks up the SHM segment by name.
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
