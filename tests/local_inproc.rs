//! Smoke and integration tests for the local SHM transport.
//!
//! What we test now:
//! * loan / publish / take round-trip
//! * fan-out to multiple subscribers
//! * empty queue → `Ok(None)`
//! * cross-process create/open/publish/subscribe
//!
//! The lower-level `local::shm` unit tests cover create/open and
//! compatibility checks; this file stays focused on the public API.

use peerbus::{LocalConfig, LocalService};

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
    // Local logical names allow / and _ but not most punctuation.
    format!("peerbus_test_{stem}_{pid}_{nanos}")
}

fn poll_for<R>(
    timeout: std::time::Duration,
    mut f: impl FnMut() -> peerbus::Result<Option<R>>,
) -> R {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Some(value) = f().unwrap() {
            return value;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("timed out waiting for local SHM sample");
}

#[test]
fn loan_publish_take_roundtrip() {
    let svc = LocalService::<Pose>::create(&unique_name("rt"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    let mut loan = pubr.loan(0).unwrap();
    *loan.header_mut() = Pose {
        x: 1.0,
        y: 2.0,
        yaw: 0.5,
    };
    pubr.publish(loan).unwrap();

    let sample = sub.take().unwrap().expect("a sample should be available");
    assert_eq!(
        *sample.header(),
        Pose {
            x: 1.0,
            y: 2.0,
            yaw: 0.5
        }
    );
}

#[test]
fn empty_take_returns_none() {
    let svc = LocalService::<Pose>::create(&unique_name("empty"), LocalConfig::default()).unwrap();
    let mut sub = svc.subscriber().unwrap();
    assert!(sub.take().unwrap().is_none());
}

#[test]
fn fanout_to_two_subscribers() {
    let svc = LocalService::<Pose>::create(&unique_name("fanout"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub_a = svc.subscriber().unwrap();
    let mut sub_b = svc.subscriber().unwrap();

    pubr.send(&Pose {
        x: 1.0,
        y: 2.0,
        yaw: 3.0,
    })
    .unwrap();

    let a = sub_a
        .take()
        .unwrap()
        .expect("subscriber A should see the publish");
    let b = sub_b
        .take()
        .unwrap()
        .expect("subscriber B should see the publish");
    assert_eq!(*a.header(), *b.header());
    assert_eq!(
        *a.header(),
        Pose {
            x: 1.0,
            y: 2.0,
            yaw: 3.0
        }
    );
}

#[test]
fn sequence_numbers_are_monotonic() {
    let svc = LocalService::<U32Box>::create(&unique_name("seq"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    assert_eq!(pubr.send(&U32Box { value: 1 }).unwrap(), 1);
    assert_eq!(pubr.send(&U32Box { value: 2 }).unwrap(), 2);

    let first = sub.take().unwrap().expect("first sample");
    let second = sub.take().unwrap().expect("second sample");
    assert_eq!(first.sequence(), 1);
    assert_eq!(second.sequence(), 2);
}

#[test]
fn multi_publisher_one_subscriber() {
    // Two publishers attached to the same local service; one
    // subscriber must observe samples from both. The backend caps
    // attached publishers via `max_publishers` (default 2); raise
    // it slightly here to leave headroom for any cross-thread races.
    let cfg = LocalConfig {
        max_publishers: 4,
        ..LocalConfig::default()
    };
    let svc = LocalService::<U32Box>::create(&unique_name("multipub"), cfg).unwrap();

    let mut pub_a = svc.publisher().unwrap();
    let mut pub_b = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    pub_a.send(&U32Box { value: 1 }).unwrap();
    pub_b.send(&U32Box { value: 2 }).unwrap();

    // Drain until we've seen both values or we time out.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut saw_a = false;
    let mut saw_b = false;
    while std::time::Instant::now() < deadline && !(saw_a && saw_b) {
        if let Some(s) = sub.take().unwrap() {
            match s.header().value {
                1 => saw_a = true,
                2 => saw_b = true,
                _ => {}
            }
        } else {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    assert!(saw_a, "subscriber should have seen publisher A's sample");
    assert!(saw_b, "subscriber should have seen publisher B's sample");
}

#[test]
fn multi_publisher_threaded_stress_no_drops_when_history_covers_burst() {
    const PUBLISHERS: u32 = 4;
    const PER_PUBLISHER: u32 = 100;
    let cfg = LocalConfig {
        max_publishers: PUBLISHERS,
        max_subscribers: 1,
        subscriber_buffer: PUBLISHERS * PER_PUBLISHER,
        history_depth: PUBLISHERS * PER_PUBLISHER,
        max_payload_bytes: 0,
    };
    let svc = LocalService::<U32Box>::create(&unique_name("multipub_stress"), cfg).unwrap();

    let mut handles = Vec::new();
    for publisher_id in 0..PUBLISHERS {
        let svc = svc.clone();
        handles.push(std::thread::spawn(move || {
            let mut pubr = svc.publisher().unwrap();
            for i in 0..PER_PUBLISHER {
                pubr.send(&U32Box {
                    value: publisher_id * 1_000 + i,
                })
                .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.join().expect("publisher thread panicked");
    }

    let mut sub = svc.subscriber().unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let mut last_sequence = 0;
    let expected = (PUBLISHERS * PER_PUBLISHER) as usize;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline && seen.len() < expected {
        if let Some(sample) = sub.take().unwrap() {
            assert!(
                sample.sequence() > last_sequence,
                "sample sequences must be monotonic"
            );
            last_sequence = sample.sequence();
            seen.insert(sample.header().value);
        } else {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    assert_eq!(seen.len(), expected, "subscriber should see the full burst");
    for publisher_id in 0..PUBLISHERS {
        for i in 0..PER_PUBLISHER {
            assert!(seen.contains(&(publisher_id * 1_000 + i)));
        }
    }
}

#[test]
fn late_subscriber_eventually_sees_new_publishes() {
    // A late subscriber may see up to `history_depth` retained samples
    // (default 1). Either way it MUST also see whatever the publisher
    // emits afterwards.
    let svc = LocalService::<U32Box>::create(&unique_name("late"), LocalConfig::default()).unwrap();
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
        if let Some(s) = sub.take().unwrap()
            && s.header().value == 99
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("subscriber should have seen the post-attach publish (99)");
}

#[test]
fn caps_publishers_and_subscribers() {
    let cfg = LocalConfig {
        max_publishers: 1,
        max_subscribers: 1,
        ..LocalConfig::default()
    };
    let svc = LocalService::<U32Box>::create(&unique_name("caps"), cfg).unwrap();

    let pub_a = svc.publisher().unwrap();
    assert!(svc.publisher().is_err(), "second publisher should hit cap");
    drop(pub_a);
    let _pub_b = svc
        .publisher()
        .expect("publisher slot should be released on drop");

    let sub_a = svc.subscriber().unwrap();
    assert!(
        svc.subscriber().is_err(),
        "second subscriber should hit cap"
    );
    drop(sub_a);
    let _sub_b = svc
        .subscriber()
        .expect("subscriber slot should be released on drop");
}

#[test]
fn lagged_subscriber_reports_dropped_count_and_recovers() {
    let cfg = LocalConfig {
        max_publishers: 1,
        max_subscribers: 1,
        subscriber_buffer: 1,
        history_depth: 1,
        max_payload_bytes: 0,
    };
    let svc = LocalService::<U32Box>::create(&unique_name("lag"), cfg).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    pubr.send(&U32Box { value: 1 }).unwrap();
    pubr.send(&U32Box { value: 2 }).unwrap();

    let err = match sub.take() {
        Ok(_) => panic!("first take should report lag"),
        Err(err) => err,
    };
    assert!(
        matches!(err, peerbus::Error::Lagged { dropped: 1 }),
        "got {err:?}"
    );

    let sample = sub
        .take()
        .unwrap()
        .expect("latest sample should remain readable");
    assert_eq!(sample.header().value, 2);
}

#[test]
fn local_service_cross_process_publish_subscribe() {
    let name = unique_name("cross_process");
    let svc = LocalService::<U32Box>::create(&name, LocalConfig::default()).unwrap();
    let mut sub = svc.subscriber().unwrap();

    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("local_service_cross_process_child_publish")
        .arg("--ignored")
        .arg("--nocapture")
        .env("PEERBUS_SHM_CHILD", "1")
        .env("PEERBUS_SHM_SERVICE", &name)
        .env("PEERBUS_SHM_VALUE", "12345")
        .status()
        .expect("spawn child test process");
    assert!(child.success(), "child publisher failed: {child:?}");

    let sample = poll_for(std::time::Duration::from_secs(2), || sub.take());
    assert_eq!(sample.header().value, 12_345);
}

#[test]
fn local_service_reclaims_sample_held_by_dead_process() {
    let name = unique_name("dead_sample");
    let cfg = LocalConfig {
        max_publishers: 2,
        max_subscribers: 1,
        subscriber_buffer: 1,
        history_depth: 1,
        max_payload_bytes: 0,
    };
    let svc = LocalService::<U32Box>::create(&name, cfg).unwrap();
    let mut pubr = svc.publisher().unwrap();
    pubr.send(&U32Box { value: 1 }).unwrap();

    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("local_service_cross_process_child_hold_sample_and_abort")
        .arg("--ignored")
        .arg("--nocapture")
        .env("PEERBUS_SHM_CHILD", "1")
        .env("PEERBUS_SHM_SERVICE", &name)
        .env("PEERBUS_SHM_VALUE", "1")
        .status()
        .expect("spawn child holder");
    assert!(
        !child.success(),
        "child should abort while holding the sample: {child:?}"
    );

    pubr.send(&U32Box { value: 2 })
        .expect("dead reader bit should be reaped, not pin the slot forever");

    let mut sub = svc.subscriber().unwrap();
    let sample = poll_for(std::time::Duration::from_secs(2), || sub.take());
    assert_eq!(sample.header().value, 2);
}

#[test]
fn local_service_reclaims_loan_held_by_dead_process() {
    let name = unique_name("dead_writer");
    let cfg = LocalConfig {
        max_publishers: 1,
        max_subscribers: 1,
        subscriber_buffer: 1,
        history_depth: 1,
        max_payload_bytes: 0,
    };
    let svc = LocalService::<U32Box>::create(&name, cfg).unwrap();

    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("local_service_cross_process_child_hold_loan_and_abort")
        .arg("--ignored")
        .arg("--nocapture")
        .env("PEERBUS_SHM_CHILD", "1")
        .env("PEERBUS_SHM_SERVICE", &name)
        .env("PEERBUS_SHM_VALUE", "77")
        .status()
        .expect("spawn child writer");
    assert!(
        !child.success(),
        "child should abort while holding the loan: {child:?}"
    );

    let mut pubr = svc.publisher().unwrap();
    pubr.send(&U32Box { value: 88 })
        .expect("dead writer ownership should be reaped");
    let mut sub = svc.subscriber().unwrap();
    let sample = poll_for(std::time::Duration::from_secs(2), || sub.take());
    assert_eq!(sample.header().value, 88);
}

#[test]
#[ignore = "helper spawned by local_service_cross_process_publish_subscribe"]
fn local_service_cross_process_child_publish() {
    if std::env::var_os("PEERBUS_SHM_CHILD").is_none() {
        return;
    }
    let name = std::env::var("PEERBUS_SHM_SERVICE").expect("PEERBUS_SHM_SERVICE");
    let value = std::env::var("PEERBUS_SHM_VALUE")
        .expect("PEERBUS_SHM_VALUE")
        .parse::<u32>()
        .expect("u32 value");

    let svc = LocalService::<U32Box>::open_existing(&name).expect("open parent SHM service");
    let mut pubr = svc.publisher().expect("child publisher");
    pubr.send(&U32Box { value }).expect("child publish");
}

#[test]
#[ignore = "helper spawned by local_service_reclaims_sample_held_by_dead_process"]
fn local_service_cross_process_child_hold_sample_and_abort() {
    if std::env::var_os("PEERBUS_SHM_CHILD").is_none() {
        return;
    }
    let name = std::env::var("PEERBUS_SHM_SERVICE").expect("PEERBUS_SHM_SERVICE");
    let value = std::env::var("PEERBUS_SHM_VALUE")
        .expect("PEERBUS_SHM_VALUE")
        .parse::<u32>()
        .expect("u32 value");

    let svc = LocalService::<U32Box>::open_existing(&name).expect("open parent SHM service");
    let mut sub = svc.subscriber().expect("child subscriber");
    let sample = poll_for(std::time::Duration::from_secs(2), || sub.take());
    assert_eq!(sample.header().value, value);
    // Simulate a dead process without running Rust destructors. Unlike
    // `abort()`, this avoids slow coredump handling during the full test run.
    std::process::exit(42);
}

#[test]
#[ignore = "helper spawned by local_service_reclaims_loan_held_by_dead_process"]
fn local_service_cross_process_child_hold_loan_and_abort() {
    if std::env::var_os("PEERBUS_SHM_CHILD").is_none() {
        return;
    }
    let name = std::env::var("PEERBUS_SHM_SERVICE").expect("PEERBUS_SHM_SERVICE");
    let value = std::env::var("PEERBUS_SHM_VALUE")
        .expect("PEERBUS_SHM_VALUE")
        .parse::<u32>()
        .expect("u32 value");

    let svc = LocalService::<U32Box>::open_existing(&name).expect("open parent SHM service");
    let mut pubr = svc.publisher().expect("child publisher");
    let mut loan = pubr.loan(0).expect("child loan");
    loan.header_mut().value = value;
    // Simulate a dead process without running Rust destructors. Unlike
    // `abort()`, this avoids slow coredump handling during the full test run.
    std::process::exit(42);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        failure_persistence: None,
        ..proptest::test_runner::Config::default()
    })]

    #[test]
    fn random_local_publish_take_hold_preserves_sequences(
        actions in proptest::collection::vec(0u8..4, 1..64)
    ) {
        let cfg = LocalConfig {
            max_publishers: 1,
            max_subscribers: 1,
            subscriber_buffer: 128,
            history_depth: 128,
            max_payload_bytes: 0,
        };
        let svc = LocalService::<U32Box>::create(&unique_name("prop"), cfg).unwrap();
        let mut pubr = svc.publisher().unwrap();
        let mut sub = svc.subscriber().unwrap();
        let mut held = Vec::new();
        let mut next_value = 1u32;
        let mut last_sequence = 0u64;

        for action in actions {
            match action {
                0 => {
                    pubr.send(&U32Box { value: next_value }).unwrap();
                    next_value += 1;
                }
                1 | 2 => {
                    if let Some(sample) = sub.take().unwrap() {
                        proptest::prop_assert!(sample.sequence() > last_sequence);
                        last_sequence = sample.sequence();
                        proptest::prop_assert!(sample.header().value < next_value);
                        if action == 2 {
                            held.push(sample);
                        }
                    }
                }
                _ => {
                    let _ = held.pop();
                }
            }
        }

        drop(held);
        pubr.send(&U32Box { value: next_value }).unwrap();
    }
}
