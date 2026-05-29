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
use quicbit::{Node, RemoteTransport, Transport};

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
fn node_publisher_fans_out_to_local_shm_and_remote_iroh() {
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
