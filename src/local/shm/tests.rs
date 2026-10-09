    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
    struct Header {
        value: u32,
    }

    fn unique_name(stem: &str) -> String {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("peerbus_shm_{stem}_{pid}_{nanos}")
    }

    /// Env var read by `create`'s test hook: when set to a millisecond count,
    /// the calling process stalls in the unstamped window (pid == 0) that long
    /// before stamping. Lets the stalled-live-creator test drive the *real*
    /// `create` path into the exact state that used to cause split brain.
    const STALL_ENV: &str = "PEERBUS_TEST_STALL_BEFORE_STAMP_MS";

    /// Called from `create` under `#[cfg(test)]` only.
    pub(super) fn stall_before_stamp_hook() {
        if let Ok(ms) = std::env::var(STALL_ENV) {
            if let Ok(ms) = ms.parse::<u64>() {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
        }
    }

    // When set, `capture_mapped_identity` returns `None` for the *current
    // thread only*, simulating identity-capture failure (e.g. fd exhaustion)
    // deterministically without actually exhausting fds. Thread-local so it
    // cannot leak into other tests running in parallel.
    thread_local! {
        static FORCE_STAT_FAIL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        static INJECTED_STAT: std::cell::Cell<Option<Option<(u64, u64)>>> =
            const { std::cell::Cell::new(None) };
    }

    /// Read by `capture_mapped_identity` under `#[cfg(test)]` only.
    pub(super) fn force_stat_fail() -> bool {
        FORCE_STAT_FAIL.with(|c| c.get())
    }

    fn set_force_stat_fail(on: bool) {
        FORCE_STAT_FAIL.with(|c| c.set(on));
    }

    /// Read by `stat_shm` under `#[cfg(test)]` only: when `Some(r)`, `stat_shm`
    /// returns `r` instead of resolving the name — lets a test inject a *by-name*
    /// stat that diverges from the mapped inode, proving capture ignores it.
    pub(super) fn injected_stat() -> Option<Option<(u64, u64)>> {
        INJECTED_STAT.with(|c| c.get())
    }

    fn set_injected_stat(v: Option<Option<(u64, u64)>>) {
        INJECTED_STAT.with(|c| c.set(v));
    }

    #[test]
    fn create_open_round_trip() {
        let name = unique_name("round_trip");
        let segment = Segment::<Header>::create(&name, 0xfeed, LocalConfig::default()).unwrap();
        let opened = Segment::<Header>::open_existing(&name, 0xfeed).unwrap();
        assert_eq!(segment.layout.slot_count, opened.layout.slot_count);
        assert_eq!(opened.publisher_count(), 0);
    }

    #[test]
    fn open_absent_errors() {
        let err = match Segment::<Header>::open_existing(&unique_name("absent"), 0xfeed) {
            Ok(_) => panic!("absent segment should not open"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::ServiceNotFound(_)));
    }

    #[test]
    fn type_hash_mismatch_is_rejected() {
        let name = unique_name("type_mismatch");
        let _segment = Segment::<Header>::create(&name, 0xaaaa, LocalConfig::default()).unwrap();
        let err = match Segment::<Header>::open_existing(&name, 0xbbbb) {
            Ok(_) => panic!("wrong type hash should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::IncompatibleShm(_)));
    }

    #[test]
    fn publisher_registration_count_tracks_drops() {
        let name = unique_name("counts");
        let segment = Segment::<Header>::create(&name, 0xfeed, LocalConfig::default()).unwrap();
        assert_eq!(segment.publisher_count(), 0);

        let producer = segment.producer().unwrap();
        assert_eq!(segment.publisher_count(), 1);

        drop(producer);
        assert_eq!(segment.publisher_count(), 0);
    }

    #[test]
    fn payload_cap_is_enforced_before_sequence_claim() {
        let name = unique_name("payload_cap");
        let cfg = LocalConfig {
            max_payload_bytes: 4,
            ..LocalConfig::default()
        };
        let segment = Segment::<Header>::create(&name, 0xfeed, cfg).unwrap();
        let mut producer = segment.producer().unwrap();

        let err = match producer.loan(5) {
            Ok(_) => panic!("oversized loan should fail"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            Error::PayloadTooLarge {
                actual: 5,
                capacity: 4
            }
        ));
        assert_eq!(segment.control().write_seq.load(Ordering::Acquire), 0);
    }

    #[test]
    fn dropped_loan_does_not_publish_sample() {
        let name = unique_name("drop_loan");
        let segment = Segment::<Header>::create(&name, 0xfeed, LocalConfig::default()).unwrap();
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();

        {
            let mut loan = producer.loan(0).unwrap();
            loan.header_mut().value = 42;
        }

        assert!(consumer.take().unwrap().is_none());
    }

    #[test]
    fn pinned_slot_returns_no_free_slot_without_sequence_hole() {
        let name = unique_name("pinned");
        let cfg = LocalConfig {
            max_publishers: 1,
            max_subscribers: 1,
            subscriber_buffer: 1,
            history_depth: 1,
            max_payload_bytes: 0,
        };
        let segment = Segment::<Header>::create(&name, 0xfeed, cfg).unwrap();
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();

        let mut loan = producer.loan(0).unwrap();
        loan.header_mut().value = 1;
        assert_eq!(producer.publish(loan).unwrap(), 1);

        let held = consumer.take().unwrap().expect("sample should be present");
        assert_eq!(held.header().value, 1);

        let err = match producer.loan(0) {
            Ok(_) => panic!("one-slot ring should be pinned by held sample"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::NoFreeSlot { .. }));
        assert_eq!(segment.control().write_seq.load(Ordering::Acquire), 1);

        drop(held);
        let mut loan = producer.loan(0).unwrap();
        loan.header_mut().value = 2;
        assert_eq!(producer.publish(loan).unwrap(), 2);
    }

    /// Deterministically hammer the concurrent create / open / drop
    /// lifecycle on a *single* segment from many threads.
    ///
    /// Before the backing-store fix, on a constrained `/dev/shm` (small
    /// containers, or a per-user tmpfs quota) the creator's full-segment
    /// zeroing faulted as SIGBUS the moment tmpfs could not allocate a
    /// page, killing the whole test binary. After the fix `create`
    /// pre-commits the segment with `fallocate`, so exhaustion is reported
    /// as a clean `Error::ShmExhausted` and this loop can never SIGBUS.
    ///
    /// It also exercises the create/open race directly: threads all race
    /// on one name, the owner's `Drop` unlinks the OS object, and the next
    /// `open_or_create` must re-create and converge — never observe an
    /// unsized mapping or wedge. A small payload keeps peak RAM/tmpfs
    /// bounded so the test is safe to run anywhere while still driving the
    /// exact code path.
    #[test]
    fn concurrent_open_create_drop_never_faults() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering as O};

        // Small enough to be safe on any machine, large enough that the
        // creator still zeroes real pages (the operation that faulted).
        let cfg = LocalConfig {
            max_publishers: 2,
            max_subscribers: 2,
            subscriber_buffer: 4,
            history_depth: 1,
            max_payload_bytes: 64 * 1024,
        };
        let name = Arc::new(unique_name("race"));
        let created = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let name = Arc::clone(&name);
            let cfg = cfg.clone();
            let created = Arc::clone(&created);
            handles.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    // `open_or_create` must always converge (create or
                    // attach) despite peers racing and unlinking.
                    let seg = Segment::<Header>::open_or_create(&name, 0xfeed, cfg.clone())
                        .expect("open_or_create must converge, not fault or error");
                    created.fetch_add(1, O::Relaxed);

                    // Touch the control block and, capacity permitting,
                    // round-trip a sample through a real slot. Publisher/
                    // consumer capacity errors are expected under
                    // contention and are not failures.
                    let _ = seg.control().write_seq.load(O::Acquire);
                    if let Ok(mut producer) = seg.producer() {
                        if let Ok(mut loan) = producer.loan(8) {
                            loan.header_mut().value = 0xABCD;
                            loan.payload_mut().copy_from_slice(&[1u8; 8]);
                            let _ = producer.publish(loan);
                        }
                    }
                    if let Ok(mut consumer) = seg.consumer() {
                        let _ = consumer.take();
                    }
                    // `seg` drops here; when this thread owns the object the
                    // crate unlinks it, forcing the next racer to re-create.
                }
            }));
        }
        for h in handles {
            h.join().expect("worker thread panicked");
        }
        assert_eq!(created.load(O::Relaxed), 8 * 200);
    }

    /// Env var used to drive `poison_helper_child` as a real child process.
    const POISON_ENV: &str = "PEERBUS_TEST_POISON_SEGMENT";

    fn poison_cfg() -> LocalConfig {
        LocalConfig {
            max_publishers: 2,
            max_subscribers: 2,
            subscriber_buffer: 4,
            history_depth: 1,
            max_payload_bytes: 64 * 1024,
        }
    }

    /// Reproduce exactly the state a creator leaves behind when it dies
    /// mid-initialisation: the OS object exists and is *sized*, the creator
    /// stamp names this (about to die) process, but `MAGIC` is never stored.
    fn poison_segment(name: &str) {
        let key = os_key(name);
        let layout = Layout::new::<Header>(&poison_cfg()).unwrap();
        let shmem = ShmemConf::new()
            .os_id(&key)
            .size(layout.total_size)
            .create()
            .expect("child must create the segment");
        let ptr = NonNull::new(shmem.as_ptr()).unwrap();
        reserve_shm_backing(&key, std::mem::size_of::<ControlBlock>()).unwrap();

        let control = unsafe_control(ptr);
        control
            .creator_token
            .store(current_process_token(), Ordering::Relaxed);
        control.creator_pid.store(current_pid(), Ordering::Release);

        // Leak the mapping: the OS object must outlive us. (`process::exit`
        // below skips destructors anyway; this makes it explicit.)
        std::mem::forget(shmem);
    }

    /// Child half of `dead_creator_segment_is_reclaimed`. Inert unless driven
    /// with `POISON_ENV` set, so it is a no-op in a normal test run.
    #[test]
    fn poison_helper_child() {
        let Ok(name) = std::env::var(POISON_ENV) else {
            return;
        };
        poison_segment(&name);
        // Die without ever storing MAGIC and without running destructors —
        // precisely the crash that used to poison the name forever.
        std::process::exit(0);
    }

    /// Finding #4: a creator that dies after `ftruncate` but before storing
    /// `MAGIC` used to poison the service name permanently — every later
    /// `open_or_create` returned `IncompatibleShm` until a human ran
    /// `rm /dev/shm/qb_*`. It must now recover automatically.
    #[test]
    fn dead_creator_segment_is_reclaimed() {
        let name = unique_name("poisoned");

        // Spawn a real child process that half-initialises the segment and
        // dies, leaving a genuinely dead PID + start token in the stamp.
        let status = std::process::Command::new(
            std::env::current_exe().expect("test binary path"),
        )
        .args(["--exact", "local::shm::tests::poison_helper_child"])
        .env(POISON_ENV, &name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("spawn poisoning child");
        assert!(status.success(), "poisoning child failed: {status:?}");

        // The segment now exists, is sized, and names a dead creator, but has
        // no MAGIC. A bare attach must still report it as broken...
        assert!(
            matches!(
                Segment::<Header>::open_existing(&name, 0xfeed),
                Err(Error::IncompatibleShm(_))
            ),
            "a poisoned segment must not attach as healthy"
        );

        // ...while `open_or_create` reclaims it and rebuilds a working one.
        let segment = Segment::<Header>::open_or_create(&name, 0xfeed, poison_cfg())
            .expect("open_or_create must reclaim a segment whose creator died mid-init");

        // And the reclaimed segment must actually work end to end.
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();
        let mut loan = producer.loan(4).unwrap();
        loan.header_mut().value = 0xC0DE;
        loan.payload_mut().copy_from_slice(&[7u8; 4]);
        producer.publish(loan).unwrap();

        let sample = consumer
            .take()
            .unwrap()
            .expect("reclaimed segment must deliver the sample");
        assert_eq!(sample.header().value, 0xC0DE);
        assert_eq!(sample.payload(), &[7u8; 4]);
    }

    /// Plant a sized, un-`MAGIC`ed segment whose responsible-party stamp is
    /// `(pid, token)`, then drop the mapping WITHOUT unlinking (owner=false),
    /// so the OS name persists with no live process owning it — an orphaned
    /// poisoned name. Returns its inode identity.
    fn plant_orphan_segment(name: &str, pid: u32, token: u64) -> (u64, u64) {
        let key = os_key(name);
        let layout = Layout::new::<Header>(&poison_cfg()).unwrap();
        let mut shmem = ShmemConf::new()
            .os_id(&key)
            .size(layout.total_size)
            .create()
            .expect("plant must create the segment");
        // Do NOT unlink on drop: the poisoned name must outlive this scope.
        shmem.set_owner(false);
        let ptr = NonNull::new(shmem.as_ptr()).unwrap();
        reserve_shm_backing(&key, std::mem::size_of::<ControlBlock>()).unwrap();
        let control = unsafe_control(ptr);
        control.creator_token.store(token, Ordering::Relaxed);
        control.creator_pid.store(pid, Ordering::Release);
        let id = stat_shm(&key).expect("planted segment must stat");
        drop(shmem); // munmap+close only; name persists (orphaned)
        id
    }

    /// Trigger (a): a segment stuck with a *dead* responsible party and no live
    /// process about to unlink it (e.g. the reclaimer crashed between taking
    /// over and unlinking) must AUTO-RECOVER. Under the old fixed-`CONDEMNED`
    /// sentinel this state was permanent poison — every opener span 12 s then
    /// returned `IncompatibleShm`, requiring a manual `rm /dev/shm/qb_*`.
    #[test]
    fn orphaned_dead_reclaimer_segment_auto_recovers() {
        let name = unique_name("orphan");
        let key = os_key(&name);

        // Stamp a nonexistent PID (u32::MAX — the value the old code used as
        // the permanent `CONDEMNED` sentinel) with a reclaimer token (0). No
        // live process owns or will unlink this name.
        let planted = plant_orphan_segment(&name, u32::MAX, 0);
        assert_eq!(stat_shm(&key), Some(planted), "planted name must exist");

        // A bare attach still reports it broken (no rebuild capability)...
        assert!(matches!(
            Segment::<Header>::open_existing(&name, 0xfeed),
            Err(Error::IncompatibleShm(_))
        ));

        // ...but `open_or_create` now takes over the dead inode, unlinks it,
        // and rebuilds a working segment — quickly, not after the 12 s deadline.
        let began = Instant::now();
        let segment = Segment::<Header>::open_or_create(&name, 0xfeed, poison_cfg())
            .expect("open_or_create must auto-recover an orphaned dead-reclaimer segment");
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "recovery must be prompt, not a deadline timeout"
        );
        // The name now points at a fresh, healthy inode (not the planted one).
        assert_ne!(
            (segment.dev, segment.ino),
            planted,
            "recovery must rebuild onto a fresh inode"
        );

        // And it works end to end.
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();
        let mut loan = producer.loan(4).unwrap();
        loan.header_mut().value = 0x1234;
        loan.payload_mut().copy_from_slice(&[5u8; 4]);
        producer.publish(loan).unwrap();
        let sample = consumer.take().unwrap().expect("recovered segment delivers");
        assert_eq!(sample.header().value, 0x1234);
    }

    /// Trigger (b): a reclaim attempt that cannot capture the inode identity
    /// (simulated fd exhaustion → `stat_shm` returns `None`) must NOT take over
    /// the segment — taking over without the ability to unlink would strand the
    /// name as permanent poison. It must leave the stamp untouched and let a
    /// later, better-resourced attempt reclaim it.
    #[test]
    fn reclaim_without_identity_does_not_permanently_poison() {
        let name = unique_name("noident");
        let key = os_key(&name);

        // Plant a poisoned segment with a genuinely-dead responsible party.
        let planted = plant_orphan_segment(&name, u32::MAX, 0);

        // Simulate fd exhaustion during the reclaim attach: `stat_shm` -> None,
        // so identity capture fails.
        set_force_stat_fail(true);
        let outcome = Segment::<Header>::attach(&name, 0xfeed, true);
        set_force_stat_fail(false);

        // The attach must report a transient Retry (not take over, not error),
        // and must NOT have altered the stamp — the inode is untouched, so it
        // is still exactly as reclaimable as before (no condemn-without-unlink).
        assert!(
            matches!(outcome, Ok(Attach::Retry)),
            "identity-less reclaim must back off with Retry, not take over or error"
        );
        assert_eq!(
            stat_shm(&key),
            Some(planted),
            "the poisoned inode must be left intact (name not unlinked, not repurposed)"
        );
        // Confirm the stamp was not advanced to some un-unlinkable state.
        {
            let mut shmem = ShmemConf::new().os_id(&key).open().unwrap();
            shmem.set_owner(false);
            let ptr = NonNull::new(shmem.as_ptr()).unwrap();
            let control = unsafe_control(ptr);
            assert_eq!(
                control.creator_pid.load(Ordering::Acquire),
                u32::MAX,
                "stamp must be untouched after an identity-less reclaim attempt"
            );
        }

        // Once identity capture works again, recovery proceeds normally.
        let segment = Segment::<Header>::open_or_create(&name, 0xfeed, poison_cfg())
            .expect("segment must reclaim once fds (identity capture) are available again");
        assert_ne!((segment.dev, segment.ino), planted);
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();
        let mut loan = producer.loan(0).unwrap();
        loan.header_mut().value = 0x77;
        producer.publish(loan).unwrap();
        assert_eq!(
            consumer.take().unwrap().expect("delivers").header().value,
            0x77
        );
    }

    /// Latent split-brain window: identity must be the inode we *mapped and
    /// arbitrated on*, never a by-name stat that a repurposed name could point
    /// at a different, healthy inode. Two independent checks:
    ///
    ///   1. Capture ignores a divergent by-name stat (uses the mapped inode).
    ///   2. An inode-verified unlink carrying a *foreign* identity spares the
    ///      healthy inode the name currently resolves to (safe no-op) — so even
    ///      if a reclaimer arbitrated on some orphan, it cannot destroy the
    ///      healthy segment now under the name.
    #[test]
    fn identity_is_the_mapped_inode_not_a_byname_stat() {
        let name = unique_name("mapident");
        let key = os_key(&name);
        let segment = Segment::<Header>::create(&name, 0xfeed, poison_cfg()).unwrap();
        let mapped = (segment.dev, segment.ino);
        // Sanity: with no divergence the name resolves to the mapped inode.
        assert_eq!(stat_shm(&key), Some(mapped));

        // (1) Force `stat_shm` to report a DIFFERENT inode (as a repurposed
        // name would). Capture must still return the MAPPED inode, proving it
        // does not depend on the by-name lookup.
        set_injected_stat(Some(Some((mapped.0, mapped.1 ^ 0x5AA5))));
        let captured = capture_mapped_identity(segment.ptr, &key);
        set_injected_stat(None);
        assert_eq!(
            captured,
            Some(mapped),
            "capture must use the mapped inode, not the (diverged) by-name stat"
        );

        // (2) A reclaimer that (correctly) carries a FOREIGN orphan identity
        // must not destroy the healthy inode the name now resolves to.
        let foreign = (mapped.0, mapped.1 ^ 0x1); // some other inode
        unlink_if_matches(&key, foreign.0, foreign.1);
        assert_eq!(
            stat_shm(&key),
            Some(mapped),
            "unlink carrying a foreign identity must be a no-op, sparing the healthy inode"
        );

        // The healthy segment is untouched and still works end to end.
        let opened = Segment::<Header>::open_existing(&name, 0xfeed)
            .expect("healthy segment survived the foreign-identity unlink");
        assert_eq!((opened.dev, opened.ino), mapped);
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();
        let mut loan = producer.loan(0).unwrap();
        loan.header_mut().value = 0x2468;
        producer.publish(loan).unwrap();
        assert_eq!(
            consumer.take().unwrap().expect("delivers").header().value,
            0x2468
        );
    }

    /// Child half of `stalled_live_creator_causes_no_split_brain`. Drives the
    /// *real* `create` path, which — because the parent sets `STALL_ENV` in
    /// this child's environment — stalls in the unstamped window long enough
    /// for the parent to (wrongly) condemn a still-live creator.
    #[test]
    fn stalled_live_creator_child() {
        let Ok(name) = std::env::var(POISON_ENV) else {
            return;
        };
        match Segment::<Header>::create(&name, 0xfeed, poison_cfg()) {
            // Expected on the fixed code: while we stalled, the parent
            // condemned this inode, so our sticky-stamp CAS failed and we
            // abandoned without building a segment or unlinking anything.
            Err(_) => {}
            // Only reachable if the parent did not condemn in time. Hold the
            // segment briefly, then let it DROP: on the *buggy* code this drop
            // blind-`shm_unlink`s the name (destroying whatever inode it now
            // points at); on the fixed code the drop is inode-verified.
            Ok(seg) => {
                std::thread::sleep(std::time::Duration::from_millis(200));
                drop(seg);
            }
        }
    }

    /// CRITICAL regression: a live-but-slow creator must never cause split
    /// brain. If a creator stalls in the unstamped window past the grace, a
    /// peer condemns it, unlinks the name, and builds a fresh healthy segment.
    /// The stalled creator then wakes and eventually drops its handle — and on
    /// the pre-fix code the `shared_memory` crate's unconditional owner-`shm_
    /// unlink(name)` destroyed the *peer's* healthy segment (two live segments
    /// under one name). The fix disables the crate's blind unlink, makes every
    /// unlink inode-verified, and makes the creator abandon (not fight) once
    /// condemned. This test must fail before the fix and pass after.
    #[test]
    fn stalled_live_creator_causes_no_split_brain() {
        let name = unique_name("stalled");
        let key = os_key(&name);
        // Stall the child well past the 1 s grace so the parent deterministically
        // condemns it while it is still alive.
        let stall_ms = (CREATOR_STAMP_GRACE.as_millis() as u64) + 1_000;

        let mut child = std::process::Command::new(
            std::env::current_exe().expect("test binary path"),
        )
        .args(["--exact", "local::shm::tests::stalled_live_creator_child"])
        .env(POISON_ENV, &name)
        .env(STALL_ENV, stall_ms.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn stalled child");

        // Wait until the child has created (and sized) the OS object, so our
        // `open_or_create` below sees it and takes the attach→condemn path.
        let waited_start = Instant::now();
        while stat_shm(&key).is_none() {
            assert!(
                waited_start.elapsed() < Duration::from_secs(5),
                "child never created the segment"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        // The parent reclaims the stalled creator's segment and rebuilds a
        // fresh healthy one.
        let segment = Segment::<Header>::open_or_create(&name, 0xfeed, poison_cfg())
            .expect("parent must reclaim the stalled creator's segment");
        let healthy = (segment.dev, segment.ino);
        // Wire up producer/consumer up front so the consumer's cursor starts
        // before any publish and we read exactly what we write.
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();

        // Let the stalled creator wake, discover it was condemned, and exit
        // (dropping any handle it may hold). On the pre-fix code its exit
        // blind-unlinked the name, destroying this healthy segment.
        let status = child.wait().expect("await stalled child");
        assert!(status.success(), "stalled child failed: {status:?}");

        // No split brain: the name must still resolve to the *same* healthy
        // inode the parent built — the child's exit must not have unlinked or
        // repurposed it.
        assert_eq!(
            stat_shm(&key),
            Some(healthy),
            "split brain: stalled creator's exit destroyed/repurposed the peer's segment"
        );

        // And the parent's segment is still coherent end to end.
        {
            let mut loan = producer.loan(4).unwrap();
            loan.header_mut().value = 0xF00D;
            loan.payload_mut().copy_from_slice(&[3u8; 4]);
            producer.publish(loan).unwrap();
        }
        let sample = consumer
            .take()
            .unwrap()
            .expect("healthy segment must still deliver samples");
        assert_eq!(sample.header().value, 0xF00D);
        assert_eq!(sample.payload(), &[3u8; 4]);
    }

    #[test]
    fn process_liveness_token_matches_current_process() {
        let pid = current_pid();
        let token = current_process_token();
        assert!(process_alive(pid, token));
        #[cfg(target_os = "linux")]
        if token != u64::MAX {
            assert!(
                !process_alive(pid, token + 1),
                "a mismatched start token must not be treated as the same process"
            );
        }
    }
