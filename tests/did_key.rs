//! Round-trip tests for the `did:key` <-> `EndpointId` adapter.

use std::time::Duration;

use quicbit::did_key::{
    DID_KEY_PREFIX, did_key_to_endpoint_id, endpoint_id_to_did_key, looks_like_did_key,
};
use quicbit::Node;

#[datapod::datapod]
struct Tick {
    seq: u32,
}

fn poll_for<R>(timeout: Duration, mut f: impl FnMut() -> Option<R>) -> Option<R> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Some(r) = f() {
            return Some(r);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

#[test]
fn did_key_round_trip() {
    let node = Node::builder().no_relay().identity("did-rt").bind().unwrap();

    let did = node.endpoint_did_key();
    assert!(did.starts_with(DID_KEY_PREFIX), "got: {did}");
    assert!(looks_like_did_key(&did));

    let parsed = did_key_to_endpoint_id(&did).expect("parse own did:key");
    assert_eq!(parsed, node.endpoint_id());

    // Encode round-trip from a known-good iroh EndpointId is also stable.
    let re_encoded = endpoint_id_to_did_key(&parsed);
    assert_eq!(re_encoded, did);
}

#[test]
fn did_key_rejects_garbage() {
    assert!(did_key_to_endpoint_id("did:key:not-base58").is_err());
    assert!(did_key_to_endpoint_id("did:web:example.com").is_err());
    assert!(did_key_to_endpoint_id("rover-a").is_err()); // plain name
}

/// Even when the publisher uses an identity-name (the
/// `.identity("rover-a")` ergonomic path), the auto-alias should
/// still let a DID:KEY-only subscriber attach to a local iceoryx2
/// service rather than fall back to iroh loopback.
#[test]
fn subscribe_by_did_key_routes_locally_with_named_publisher() {
    let pub_node = Node::builder()
        .no_relay()
        .identity("aliased-pub")
        .bind()
        .unwrap();
    let sub_node = Node::builder().no_relay().bind().unwrap();

    let did = pub_node.endpoint_did_key();
    let mut pubr = pub_node.publisher::<Tick>("aliased/topic").unwrap();
    let mut sub = sub_node
        .subscriber::<Tick>(did.as_str(), "aliased/topic")
        .expect("subscribe by did:key string");

    pubr.send(&Tick { seq: 7 }).unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive via did:key-routed hex-alias SHM");
    assert_eq!(got.header().seq, 7);
}

/// `IntoPeer` should route a publisher's `did:key:` string to the
/// same local iceoryx2 service as direct EndpointId subscription —
/// proves the parsed peer is bit-identical to the publisher's id
/// AND that the hex-based service-name composition matches on both
/// sides when neither side has a named identity.
#[test]
fn subscribe_by_did_key_routes_locally() {
    let dir = tempfile::tempdir().unwrap();

    // identity_file gives the publisher a stable EndpointId but no
    // identity_name; service-name composition falls back to the hex
    // EndpointId, which the DID:KEY-only subscriber can reproduce.
    let pub_node = Node::builder()
        .no_relay()
        .identity_file(dir.path().join("pub.key"))
        .bind()
        .unwrap();
    let sub_node = Node::builder().no_relay().bind().unwrap();

    let did = pub_node.endpoint_did_key();
    let mut pubr = pub_node.publisher::<Tick>("did/route").unwrap();
    let mut sub = sub_node
        .subscriber::<Tick>(did.as_str(), "did/route")
        .expect("subscribe by did:key string");

    pubr.send(&Tick { seq: 42 }).unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive via did:key-routed local SHM");
    assert_eq!(got.header().seq, 42);
}
