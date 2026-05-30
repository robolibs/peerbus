//! Round-trip tests for the `did:key` <-> `EndpointId` adapter.

use std::time::Duration;

use quicbit::did_key::{
    DID_KEY_PREFIX, did_key_to_endpoint_id, endpoint_id_to_did_key, looks_like_did_key,
};
use quicbit::node::service_name;
use quicbit::{LocalConfig, LocalService};

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

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{stem}_{pid}_{nanos}")
}

#[test]
fn did_key_round_trip() {
    let endpoint_id = iroh::SecretKey::generate().public();

    let did = endpoint_id_to_did_key(&endpoint_id);
    assert!(did.starts_with(DID_KEY_PREFIX), "got: {did}");
    assert!(looks_like_did_key(&did));

    let parsed = did_key_to_endpoint_id(&did).expect("parse own did:key");
    assert_eq!(parsed, endpoint_id);

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

/// Even when a Node publisher uses an identity-name, its auto-alias is
/// the same hex-EndpointId service that a DID:KEY-only subscriber can
/// reconstruct from the public DID. This test exercises that service-name
/// equivalence without binding an iroh endpoint.
#[test]
fn did_key_reconstructs_named_publisher_hex_alias() {
    let endpoint_id = iroh::SecretKey::generate().public();
    let did = endpoint_id_to_did_key(&endpoint_id);
    let parsed = did_key_to_endpoint_id(&did).expect("did:key parses");
    let topic = unique_name("aliased/topic");
    let alias_name = service_name(None, parsed.as_bytes(), &topic);

    let svc = LocalService::<Tick>::create(&alias_name, LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub =
        LocalService::<Tick>::open_existing(&service_name(None, parsed.as_bytes(), &topic))
            .unwrap()
            .subscriber()
            .unwrap();

    pubr.send(&Tick { seq: 7 }).unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive via did:key-routed hex-alias SHM");
    assert_eq!(got.header().seq, 7);
}

/// A `did:key:` string parses to the same local SHM service name as
/// direct EndpointId addressing when the publisher composes by hex id.
#[test]
fn did_key_reconstructs_endpoint_id_service_name() {
    let endpoint_id = iroh::SecretKey::generate().public();
    let did = endpoint_id_to_did_key(&endpoint_id);
    let parsed = did_key_to_endpoint_id(&did).expect("did:key parses");
    let topic = unique_name("did/route");
    let svc_name = service_name(None, endpoint_id.as_bytes(), &topic);

    let svc = LocalService::<Tick>::create(&svc_name, LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub =
        LocalService::<Tick>::open_existing(&service_name(None, parsed.as_bytes(), &topic))
            .unwrap()
            .subscriber()
            .unwrap();

    pubr.send(&Tick { seq: 42 }).unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap())
        .expect("subscriber should receive via did:key-routed local SHM");
    assert_eq!(got.header().seq, 42);
}
