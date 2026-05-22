//! Loopback smoke test for the iroh-backed remote transport.
//!
//! Spins up two `RemoteTransport`s on `127.0.0.1` with relays
//! disabled, has one publish, the other subscribe, and asserts the
//! sample round-trips. Verifies the Phase 3 wire protocol end-to-end
//! without relying on external relay infrastructure.

use std::time::{Duration, Instant};

use quicbit::transport::{PublisherOps, SubscriberOps};
use quicbit::{Node, RemoteTransport, Transport};

#[datapod::datapod]
struct Tick {
    seq: u32,
    payload: u32,
}

#[test]
fn remote_loopback_pub_sub_roundtrip() {
    // Publisher side: bind a relay-free endpoint, no peer needed.
    let publisher_side = RemoteTransport::builder("test/loopback")
        .no_relay()
        .build_blocking()
        .expect("publisher endpoint should bind");
    publisher_side
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("addresses should resolve");
    let publisher_addr = publisher_side.endpoint_addr();

    // Subscriber side: bind, dial the publisher's endpoint id.
    let subscriber_side = RemoteTransport::builder("test/loopback")
        .no_relay()
        .peer(publisher_addr)
        .build_blocking()
        .expect("subscriber endpoint should bind");

    let mut pubr = publisher_side
        .publisher::<Tick>()
        .expect("publisher::<Tick>");
    let mut sub = subscriber_side
        .subscriber::<Tick>()
        .expect("subscriber::<Tick>");

    // Give the bi stream time to handshake before we publish.
    // (Publishers buffer through the broadcast channel, so an early
    // publish would just be missed if no subscriber stream is open
    // yet.)
    let connect_deadline = Instant::now() + Duration::from_secs(5);
    let mut received: Option<Tick> = None;
    while Instant::now() < connect_deadline && received.is_none() {
        let mut loan = pubr.loan(0).unwrap();
        loan.header = Tick {
            seq: 1,
            payload: 9999,
        };
        pubr.publish(loan).unwrap();

        std::thread::sleep(Duration::from_millis(50));
        // The subscriber's mpsc may have multiple messages buffered;
        // drain until we find one matching our payload.
        while let Some(s) = sub.take().unwrap() {
            if s.header().payload == 9999 {
                received = Some(*s.header());
                break;
            }
        }
    }
    let got = received.expect("subscriber should receive the published Tick");
    assert_eq!(got.payload, 9999);
}

#[test]
fn remote_loopback_multi_subscriber_fanout() {
    let publisher_side = RemoteTransport::builder("test/loopback-multi")
        .no_relay()
        .build_blocking()
        .expect("publisher endpoint");
    publisher_side
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("addresses");
    let publisher_addr = publisher_side.endpoint_addr();

    // One subscriber-side transport hosting two independent subscribers.
    let subscriber_side = RemoteTransport::builder("test/loopback-multi")
        .no_relay()
        .peer(publisher_addr)
        .build_blocking()
        .expect("subscriber endpoint");

    let mut pubr = publisher_side.publisher::<Tick>().unwrap();
    let mut sub_a = subscriber_side.subscriber::<Tick>().unwrap();
    let mut sub_b = subscriber_side.subscriber::<Tick>().unwrap();

    // Keep publishing the same payload until both subscribers see it.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_a = false;
    let mut got_b = false;
    while Instant::now() < deadline && !(got_a && got_b) {
        let mut loan = pubr.loan(0).unwrap();
        loan.header = Tick { seq: 1, payload: 7777 };
        pubr.publish(loan).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        while let Some(s) = sub_a.take().unwrap() {
            if s.header().payload == 7777 {
                got_a = true;
                break;
            }
        }
        while let Some(s) = sub_b.take().unwrap() {
            if s.header().payload == 7777 {
                got_b = true;
                break;
            }
        }
    }
    assert!(got_a, "subscriber A should have received the payload");
    assert!(got_b, "subscriber B should have received the payload");
}

/// Validates the C.2 + C.3 path: when a publisher Node is dropped,
/// the subscriber's reconnect loop survives (no panics, no
/// deadlock) and continues to retry the dial silently in the
/// background. This is the "caller stays oblivious" half of the
/// contract; surfacing `Error::Disconnected` would require an
/// explicit give-up policy, which we have not introduced yet.
#[test]
fn node_subscriber_survives_publisher_drop() {
    let pub_node = Node::builder()
        .no_relay()
        .identity("droppable_pub")
        .bind()
        .expect("publisher node");
    pub_node
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("addresses");
    let pub_addr = pub_node.endpoint_addr();

    let sub_node = Node::builder()
        .no_relay()
        .identity("drop_observer")
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node
        .publisher::<Tick>("drop/topic")
        .expect("publisher");
    let mut sub = sub_node
        .subscriber::<Tick>(pub_addr, "drop/topic")
        .expect("subscriber");

    // Confirm the happy path: publisher sends until subscriber
    // receives. The first few sends may race the publisher-side
    // broadcast attach; the second `serve_bi` round-trip across
    // the network needs a moment to wire up.
    let mut got = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while !got && Instant::now() < deadline {
        pubr.send(&Tick { seq: 1, payload: 100 }).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        while let Some(s) = sub.take().expect("take should not error during normal op") {
            if s.header().payload == 100 {
                got = true;
                break;
            }
        }
    }
    assert!(got, "subscriber should have received the initial sample");

    // Drop the publisher. Endpoint::close() is spawned best-effort.
    drop(pubr);
    drop(pub_node);

    // Subscriber should not deadlock, panic, or stop polling. We
    // accept any of Ok(None), Ok(Some(stale)), or one-off transport
    // errors as long as the loop keeps going for at least a second.
    let drain_deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < drain_deadline {
        let _ = sub.take(); // must not panic
        std::thread::sleep(Duration::from_millis(50));
    }
}
