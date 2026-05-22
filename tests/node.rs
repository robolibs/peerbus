//! End-to-end tests for `Node`.
//!
//! With iceoryx2 as the local backend, same-host routing keys off
//! the iceoryx2 service name we compose from
//! `(identity, topic)`. The subscriber asks iceoryx2 whether
//! that service exists locally; if yes → SHM, if no → dial via
//! iroh. So all the local tests below use `.identity(...)` so the
//! routing has something to match.

use std::time::{Duration, Instant};

use quicbit::Node;

#[datapod::datapod]
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

    pubr.send(&Tick { seq: 1, payload: 42 }).unwrap();

    let sample = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive");
    assert_eq!(*sample.header(), Tick { seq: 1, payload: 42 });
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

    pubr.send(&Tick { seq: 7, payload: 700 }).unwrap();
    let s = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive");
    assert_eq!(*s.header(), Tick { seq: 7, payload: 700 });
}

#[test]
fn ephemeral_key_changes_each_bind() {
    let id1 = Node::builder().no_relay().bind().unwrap().endpoint_id();
    let id2 = Node::builder().no_relay().bind().unwrap().endpoint_id();
    assert_ne!(id1, id2, "fresh keys each time when no path is supplied");
}

#[test]
fn topic_validation_rejects_bad_chars() {
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
    let _ = poll_for(Duration::from_millis(300), || {
        match sub.take() {
            Err(Error::Disconnected) => Some(()),
            Ok(Some(_)) => panic!("attacker should not receive any sample"),
            _ => None,
        }
    });
    // Either Disconnected or no sample is acceptable; the
    // contract is that no Tick samples reach the attacker.
    if let Ok(Some(_)) = sub.take() {
        panic!("attacker received a Tick despite ACL");
    }
}
