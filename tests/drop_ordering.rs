//! Drop-ordering safety tests.
//!
//! Verify the segment + handle network survives every reasonable
//! drop order without UAF / leaking SHM segments:
//!
//! * Sample dropped AFTER its subscriber.
//! * Subscriber dropped AFTER its service.
//! * Loan dropped after its publisher (uncommon but legal).
//! * Cloned services share the segment; last clone drops it.
//! * Long-lived Sample across multiple `take()` calls.
//!
//! Every test ends with a `unique_name` re-creation check: after
//! all handles drop, `Segment::create` with the same name must
//! succeed (segment was unlinked).

use bytemuck::{Pod, Zeroable};
use quicbit::{Error, LocalConfig, LocalService};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq)]
struct Tick {
    seq: u32,
    payload: u32,
}

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("drop-{stem}-{pid}-{nanos}")
}

/// After every reasonable drop, the SHM segment should be unlinked.
/// Verify by re-creating with the same name — `create` must
/// succeed (no `ServiceAlreadyExists`).
fn assert_segment_was_unlinked(name: &str) {
    let recreated = LocalService::<Tick>::create(name, LocalConfig::default());
    match recreated {
        Ok(_svc) => {} // Good; drop will clean up.
        Err(Error::ServiceAlreadyExists(_)) => {
            panic!("segment '{name}' not unlinked after handles dropped");
        }
        Err(e) => panic!("unexpected create error: {e:?}"),
    }
}

#[test]
fn sample_outlives_subscriber() {
    let name = unique_name("sample_outlives_sub");
    let svc = LocalService::<Tick>::create(&name, LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher();
    let mut sub = svc.subscriber();
    pubr.send(Tick { seq: 1, payload: 11 }).unwrap();
    let sample = sub.take().unwrap().expect("a sample");

    // Drop subscriber first.
    drop(sub);

    // Sample still readable.
    assert_eq!(*sample, Tick { seq: 1, payload: 11 });

    // Drop the rest.
    drop(sample);
    drop(pubr);
    drop(svc);

    assert_segment_was_unlinked(&name);
}

#[test]
fn sample_outlives_service() {
    let name = unique_name("sample_outlives_svc");
    let svc = LocalService::<Tick>::create(&name, LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher();
    let mut sub = svc.subscriber();
    pubr.send(Tick { seq: 2, payload: 22 }).unwrap();
    let sample = sub.take().unwrap().expect("a sample");

    // Drop the user-facing service handle. The Sample, Publisher,
    // and Subscriber all still hold internal `Arc<Segment>`s, so the
    // mapping stays alive.
    drop(svc);
    drop(pubr);
    drop(sub);

    assert_eq!(*sample, Tick { seq: 2, payload: 22 });

    drop(sample);

    assert_segment_was_unlinked(&name);
}

#[test]
fn loan_dropped_without_publish_returns_slot() {
    let name = unique_name("loan_drop");
    let svc = LocalService::<Tick>::create(
        &name,
        LocalConfig {
            slot_count: 1,
            slot_size: std::mem::size_of::<Tick>() as u32,
            history_depth: 1,
        },
    )
    .unwrap();
    let mut pubr = svc.publisher();

    // Loan the only slot, then drop without publish.
    {
        let _loan = pubr.loan().unwrap();
    }
    // Slot should be reusable.
    let _loan2 = pubr.loan().unwrap();
    drop(_loan2); // explicit drop for clarity

    drop(pubr);
    drop(svc);
    assert_segment_was_unlinked(&name);
}

#[test]
fn cloned_service_keeps_segment_alive() {
    let name = unique_name("clone_alive");
    let svc1 = LocalService::<Tick>::create(&name, LocalConfig::default()).unwrap();
    let svc2 = svc1.clone();

    // Drop one clone — segment should still be present (re-create
    // must fail with AlreadyExists because svc2 keeps it alive).
    drop(svc1);
    let collision = LocalService::<Tick>::create(&name, LocalConfig::default());
    assert!(
        matches!(collision, Err(Error::ServiceAlreadyExists(_))),
        "segment unlinked before all clones dropped"
    );

    drop(svc2);
    assert_segment_was_unlinked(&name);
}

#[test]
fn long_lived_sample_across_more_takes() {
    let name = unique_name("long_sample");
    let svc = LocalService::<Tick>::create(
        &name,
        LocalConfig {
            slot_count: 4,
            slot_size: std::mem::size_of::<Tick>() as u32,
            history_depth: 2,
        },
    )
    .unwrap();
    let mut pubr = svc.publisher();
    let mut sub = svc.subscriber_from_start();

    pubr.send(Tick { seq: 1, payload: 100 }).unwrap();
    let s1 = sub.take().unwrap().expect("s1");

    pubr.send(Tick { seq: 2, payload: 200 }).unwrap();
    let s2 = sub.take().unwrap().expect("s2");

    // Two outstanding Samples; both must still read their bytes.
    assert_eq!(*s1, Tick { seq: 1, payload: 100 });
    assert_eq!(*s2, Tick { seq: 2, payload: 200 });

    drop(s1);
    drop(s2);
    drop(sub);
    drop(pubr);
    drop(svc);
    assert_segment_was_unlinked(&name);
}

#[test]
fn dropping_in_reverse_order_is_safe() {
    let name = unique_name("rev_order");
    let svc = LocalService::<Tick>::create(&name, LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher();
    let mut sub = svc.subscriber();

    pubr.send(Tick { seq: 9, payload: 99 }).unwrap();
    let sample = sub.take().unwrap().expect("sample");

    // Drop in the WORST order: service first, then publisher, then
    // subscriber, then sample.
    drop(svc);
    drop(pubr);
    drop(sub);
    assert_eq!(*sample, Tick { seq: 9, payload: 99 });
    drop(sample);

    assert_segment_was_unlinked(&name);
}
