//! In-process smoke tests for the iceoryx2-backed local transport.
//!
//! What we test now:
//! * loan / publish / take round-trip
//! * fan-out to multiple subscribers
//! * empty queue → `Ok(None)`
//!
//! What we deliberately don't test (left to iceoryx2's own suite):
//! * slot exhaustion / refcount races / ABA — iceoryx2 owns the
//!   allocator; we don't need to re-test theirs.
//! * sequence-number monotonicity — iceoryx2 doesn't surface a
//!   per-publish seq number through its public API.
//! * history-depth replay — iceoryx2's semantics differ from our
//!   old SHM ring and need their own dedicated tests in a later
//!   pass.

use quicbit::{LocalConfig, LocalService};

#[datapod::datapod]
struct Pose {
    x: f32,
    y: f32,
    yaw: f32,
}

#[datapod::datapod]
struct U32Box {
    value: u32,
}

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // iceoryx2 service names allow / and _ but not most punctuation.
    format!("quicbit_test_{stem}_{pid}_{nanos}")
}

#[test]
fn loan_publish_take_roundtrip() {
    let svc =
        LocalService::<Pose>::create(&unique_name("rt"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    let mut loan = pubr.loan(0).unwrap();
    *loan.header_mut() = Pose { x: 1.0, y: 2.0, yaw: 0.5 };
    pubr.publish(loan).unwrap();

    let sample = sub.take().unwrap().expect("a sample should be available");
    assert_eq!(*sample.header(), Pose { x: 1.0, y: 2.0, yaw: 0.5 });
}

#[test]
fn empty_take_returns_none() {
    let svc =
        LocalService::<Pose>::create(&unique_name("empty"), LocalConfig::default()).unwrap();
    let mut sub = svc.subscriber().unwrap();
    assert!(sub.take().unwrap().is_none());
}

#[test]
fn fanout_to_two_subscribers() {
    let svc =
        LocalService::<Pose>::create(&unique_name("fanout"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub_a = svc.subscriber().unwrap();
    let mut sub_b = svc.subscriber().unwrap();

    pubr.send(&Pose { x: 1.0, y: 2.0, yaw: 3.0 }).unwrap();

    let a = sub_a
        .take()
        .unwrap()
        .expect("subscriber A should see the publish");
    let b = sub_b
        .take()
        .unwrap()
        .expect("subscriber B should see the publish");
    assert_eq!(*a.header(), *b.header());
    assert_eq!(*a.header(), Pose { x: 1.0, y: 2.0, yaw: 3.0 });
}

#[test]
fn late_subscriber_eventually_sees_new_publishes() {
    // With iceoryx2 the late subscriber may see up to
    // `history_depth` retained samples (default 1). Either way it
    // MUST also see whatever the publisher emits afterwards.
    let svc =
        LocalService::<U32Box>::create(&unique_name("late"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();

    pubr.send(&U32Box { value: 1 }).unwrap();
    pubr.send(&U32Box { value: 2 }).unwrap();

    let mut sub = svc.subscriber().unwrap();
    // Drain any retained historical samples first.
    while sub.take().unwrap().is_some() {}

    pubr.send(&U32Box { value: 99 }).unwrap();

    // Poll briefly for the post-attach publish.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if let Some(s) = sub.take().unwrap() && s.header().value == 99 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("subscriber should have seen the post-attach publish (99)");
}
