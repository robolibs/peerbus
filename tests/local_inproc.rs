//! In-process smoke tests for the local SHM transport.
//!
//! Verifies the loan/publish/consume lifecycle, fan-out, slot
//! exhaustion, and history-based lag detection in a single process.
//! Cross-process tests live in `local_xproc.rs`.

use bytemuck::{Pod, Zeroable};
use quicbit::{Error, LocalConfig, LocalService};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq)]
struct Pose {
    x: f32,
    y: f32,
    yaw: f32,
}

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("test-{stem}-{pid}-{nanos}")
}

#[test]
fn loan_publish_take_roundtrip() {
    let svc =
        LocalService::<Pose>::create(&unique_name("rt"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher();
    let mut sub = svc.subscriber();

    let mut loan = pubr.loan().unwrap();
    *loan = Pose { x: 1.0, y: 2.0, yaw: 0.5 };
    let seq = pubr.publish(loan).unwrap();
    assert_eq!(seq, 1);

    let sample = sub.take().unwrap().expect("a sample should be available");
    assert_eq!(*sample, Pose { x: 1.0, y: 2.0, yaw: 0.5 });
    assert_eq!(sample.sequence(), 1);
}

#[test]
fn empty_take_returns_none() {
    let svc =
        LocalService::<Pose>::create(&unique_name("empty"), LocalConfig::default()).unwrap();
    let mut sub = svc.subscriber();
    assert!(sub.take().unwrap().is_none());
}

#[test]
fn slot_exhaustion_returns_no_free_slot() {
    let svc = LocalService::<Pose>::create(
        &unique_name("exhaust"),
        LocalConfig {
            slot_count: 2,
            slot_size: std::mem::size_of::<Pose>() as u32,
            history_depth: 1,
        },
    )
    .unwrap();
    let mut pubr = svc.publisher();

    // Keep two loans alive without publishing. With slot_count=2,
    // a third loan must fail with `NoFreeSlot`.
    let _l1 = pubr.loan().unwrap();
    let _l2 = pubr.loan().unwrap();
    match pubr.loan() {
        Err(Error::NoFreeSlot { .. }) => {}
        Err(e) => panic!("expected NoFreeSlot, got {e:?}"),
        Ok(_) => panic!("expected NoFreeSlot, got Ok"),
    }
}

#[test]
fn dropped_loan_returns_to_pool() {
    let svc = LocalService::<Pose>::create(
        &unique_name("rollback"),
        LocalConfig {
            slot_count: 1,
            slot_size: std::mem::size_of::<Pose>() as u32,
            history_depth: 1,
        },
    )
    .unwrap();
    let mut pubr = svc.publisher();

    // Loan, then drop without publishing.
    {
        let _l = pubr.loan().unwrap();
    }
    // The slot must be reusable.
    let l = pubr.loan().unwrap();
    drop(l);
}

#[test]
fn fanout_to_two_subscribers() {
    let svc =
        LocalService::<Pose>::create(&unique_name("fanout"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher();
    let mut sub_a = svc.subscriber();
    let mut sub_b = svc.subscriber();

    pubr.send(Pose { x: 1.0, y: 2.0, yaw: 3.0 }).unwrap();

    let a = sub_a.take().unwrap().unwrap();
    let b = sub_b.take().unwrap().unwrap();
    assert_eq!(*a, *b);
    assert_eq!(*a, Pose { x: 1.0, y: 2.0, yaw: 3.0 });
}

#[test]
fn history_depth_one_drops_old_samples() {
    let svc = LocalService::<u32>::create(
        &unique_name("hist1"),
        LocalConfig {
            slot_count: 8,
            slot_size: 4,
            history_depth: 1,
        },
    )
    .unwrap();
    let mut pubr = svc.publisher();

    // Publish three values. With history=1, a fresh subscriber
    // started after the third publish should only see the third.
    pubr.send(10).unwrap();
    pubr.send(20).unwrap();
    pubr.send(30).unwrap();

    let mut sub = svc.subscriber_from_start();
    let first = sub.take().unwrap().unwrap();
    assert_eq!(*first, 30);
    assert!(sub.take().unwrap().is_none());
}

#[test]
fn fresh_subscriber_does_not_see_past_traffic() {
    let svc =
        LocalService::<u32>::create(&unique_name("fresh"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher();

    pubr.send(1).unwrap();
    pubr.send(2).unwrap();

    // Subscriber attached AFTER publishes — should see nothing.
    let mut sub = svc.subscriber();
    assert!(sub.take().unwrap().is_none());

    // But it should see the next publish.
    pubr.send(99).unwrap();
    let sample = sub.take().unwrap().unwrap();
    assert_eq!(*sample, 99);
}

#[test]
fn type_mismatch_rejects_attach() {
    let name = unique_name("typecheck");
    let _svc = LocalService::<Pose>::create(&name, LocalConfig::default()).unwrap();

    #[repr(C)]
    #[derive(Clone, Copy, Pod, Zeroable)]
    struct WrongType {
        a: u64,
    }

    let r = LocalService::<WrongType>::attach(&name);
    assert!(matches!(r, Err(Error::TypeMismatch { .. })));
}
