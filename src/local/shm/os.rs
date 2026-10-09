use super::*;


pub(crate) fn validate_name(name: &str) -> Result<()> {
    let len = name.len();
    if len == 0 {
        return Err(Error::invalid_argument("service name must not be empty"));
    }
    if len > MAX_SERVICE_NAME_BYTES {
        return Err(Error::TopicNameTooLong {
            len,
            limit: MAX_SERVICE_NAME_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn os_key(name: &str) -> String {
    format!("qb_{:016x}", fnv1a64(name))
}

/// Identity `(st_dev, st_ino)` of the OS object currently *named* `key`, or
/// `None` if it does not exist or cannot be stat'd. This resolves the NAME, so
/// it is used at unlink time (to check the name still points at a given inode)
/// — NOT to capture a handle's identity (see `capture_mapped_identity`, which
/// records the inode actually mapped, independent of the name).
#[cfg(unix)]
pub(crate) fn stat_shm(key: &str) -> Option<(u64, u64)> {
    // Test-only hook to inject a divergent by-name stat result, so a test can
    // prove that identity capture does NOT depend on this by-name lookup.
    #[cfg(test)]
    if let Some(injected) = tests::injected_stat() {
        return injected;
    }
    use std::ffi::CString;
    let cname = CString::new(key).ok()?;
    // SAFETY: FFI; `cname` is a valid NUL-terminated string for the call.
    let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDONLY, 0) };
    if fd < 0 {
        return None;
    }
    // SAFETY: `fd` is a valid descriptor; `st` is fully written by `fstat`.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstat(fd, &mut st) };
    // SAFETY: closing our own descriptor.
    unsafe {
        libc::close(fd);
    }
    if rc != 0 {
        return None;
    }
    Some((st.st_dev as u64, st.st_ino as u64))
}

#[cfg(not(unix))]
pub(crate) fn stat_shm(_key: &str) -> Option<(u64, u64)> {
    None
}

/// Identity `(dev, ino)` of the inode a mapping actually covers at address
/// `ptr`, read from `/proc/self/maps`.
///
/// This is the inode `ShmemConf` mapped and that arbitration
/// (`wait_until_initialised`) runs on — captured directly from the mapping,
/// NOT by re-resolving the name. That closes the last split-brain window: a
/// name repurposed in the sub-µs gap between `ShmemConf::open` and a by-name
/// stat can no longer make a reclaimer carry a *different* (healthy) inode's
/// identity; the identity is always the mapped-and-arbitrated inode, so a
/// repurposed name can only FAIL the `unlink_if_matches` check (safe no-op).
#[cfg(target_os = "linux")]
pub(crate) fn mapped_inode_identity(ptr: NonNull<u8>) -> Option<(u64, u64)> {
    let target = ptr.as_ptr() as usize;
    let maps = std::fs::read_to_string("/proc/self/maps").ok()?;
    for line in maps.lines() {
        // Format: "start-end perms offset maj:min inode pathname".
        let mut fields = line.split_whitespace();
        let range = fields.next()?;
        let _perms = fields.next()?;
        let _offset = fields.next()?;
        let dev = fields.next()?;
        let inode = fields.next()?;
        let (start_hex, end_hex) = range.split_once('-')?;
        let start = usize::from_str_radix(start_hex, 16).ok()?;
        let end = usize::from_str_radix(end_hex, 16).ok()?;
        if target < start || target >= end {
            continue;
        }
        let (maj, min) = dev.split_once(':')?;
        let maj = u32::from_str_radix(maj, 16).ok()?;
        let min = u32::from_str_radix(min, 16).ok()?;
        let ino: u64 = inode.parse().ok()?;
        if ino == 0 {
            // Anonymous mapping — no backing inode. Not what we mapped.
            return None;
        }
        // `makedev` yields the same `dev_t` encoding as `stat`'s `st_dev`, so
        // the pair is directly comparable with `stat_shm` at unlink time.
        return Some((libc::makedev(maj, min) as u64, ino));
    }
    None
}

/// Capture the identity of the inode a handle actually mapped at `ptr`.
///
/// Primary source is `mapped_inode_identity` (the inode `ShmemConf` mapped and
/// that arbitration runs on), guaranteeing the invariant "recorded identity ==
/// mapped-and-arbitrated inode" unconditionally. Fallback (non-Linux, or if
/// `/proc/self/maps` is unreadable) is the by-name `stat_shm`, which
/// reintroduces the tiny map/stat-divergence window only where the exact
/// source is unavailable.
pub(crate) fn capture_mapped_identity(ptr: NonNull<u8>, key: &str) -> Option<(u64, u64)> {
    // Test-only hook to simulate identity-capture failure (e.g. fd exhaustion).
    #[cfg(test)]
    if tests::force_stat_fail() {
        return None;
    }
    #[cfg(target_os = "linux")]
    if let Some(id) = mapped_inode_identity(ptr) {
        return Some(id);
    }
    let _ = ptr;
    stat_shm(key)
}

/// Unlink the OS name `key` **only if** it still resolves to inode
/// `(dev, ino)`.
///
/// This is the single unlink authority in peerbus (the `shared_memory` crate's
/// blind owner-unlink is disabled via `set_owner(false)`). Two callers use it:
/// the creating handle's `Drop`, and the reclaim path in `open_or_create`.
///
/// Why inode verification matters: `shm_unlink` operates on the *name*, and a
/// name can be repurposed to a different inode (a peer reclaimed the old one
/// and rebuilt a healthy segment). Verifying `(dev, ino)` first ensures we
/// never destroy that healthy repurposed segment.
///
/// The invariant that makes this sound: `(dev, ino)` is always the inode the
/// caller *mapped and arbitrated on* (`capture_mapped_identity`, read from the
/// mapping itself — never a by-name stat that could point at a different
/// inode). So a repurposed name can only make this check FAIL (safe no-op); it
/// can never coincidentally match a different, healthy inode.
///
/// Race-safety and its documented limit: the stat and the unlink are two
/// syscalls (no POSIX atomic inode-checked unlink exists), so this is
/// TOCTOU-*narrowed*, not atomic. The narrowing is closed in practice because
/// unlinking of a given inode is serialised elsewhere: a healthy segment is
/// unlinked only by its own creating handle on drop, and a dead one only by
/// the party that won the take-over CAS on that inode's `creator_pid`. At any
/// instant at most one *live* party holds that responsibility (a dead one is
/// superseded by the next opener, never resurrected to unlink), so no two
/// parties unlink the same inode concurrently. Hence while this check holds,
/// no other party is unlinking `(dev, ino)` and the name cannot be repurposed
/// in the gap between our stat and our unlink.
#[cfg(unix)]
pub(crate) fn unlink_if_matches(key: &str, dev: u64, ino: u64) {
    // A handle that could not capture its identity (dev==ino==0) must never
    // unlink: it cannot prove the name still points at its inode.
    if dev == 0 && ino == 0 {
        return;
    }
    if stat_shm(key) != Some((dev, ino)) {
        // Name is gone or now points at a different inode — leave it alone.
        return;
    }
    use std::ffi::CString;
    let Ok(cname) = CString::new(key) else {
        return;
    };
    // SAFETY: FFI with a valid NUL-terminated name. ENOENT (already removed)
    // is benign and ignored.
    unsafe {
        libc::shm_unlink(cname.as_ptr());
    }
}

#[cfg(not(unix))]
pub(crate) fn unlink_if_matches(_key: &str, _dev: u64, _ino: u64) {}

/// Eagerly commit backing store for a freshly created SHM object so an
/// exhausted tmpfs surfaces as a catchable error instead of a later
/// SIGBUS on first page touch. Best-effort: if `fallocate` is not
/// supported by the kernel/filesystem the segment keeps the previous
/// lazy-backing behaviour rather than failing a create that might still
/// succeed.
///
/// SAFETY/ordering: `key` names an object this process just created via
/// `shared_memory`'s exclusive `shm_open(O_CREAT|O_EXCL)`, so reopening
/// it by name yields a second descriptor to the *same* inode. `fallocate`
/// on that descriptor allocates blocks for the shared inode; the crate's
/// own mapping observes them immediately. The extra descriptor is closed
/// before returning; the crate retains its own descriptor and mapping.
#[cfg(target_os = "linux")]
pub(crate) fn reserve_shm_backing(key: &str, total_size: usize) -> Result<()> {
    use std::ffi::CString;

    let cname = CString::new(key).map_err(|_| Error::Other(format!("invalid shm key {key}")))?;
    // SAFETY: FFI call with a valid NUL-terminated name; the object was
    // just created by this process so the open is expected to succeed.
    let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0) };
    if fd < 0 {
        // Could not reopen (unexpected right after a successful create —
        // typically fd exhaustion, EMFILE/ENFILE). We cannot pre-reserve
        // without a descriptor, so we fall back to the legacy lazy-backing
        // behaviour and still attempt the create — but that leaves the
        // first page touch able to SIGBUS if the tmpfs is exhausted, so
        // surface the degraded path instead of hiding it.
        crate::qb_warn!(
            target: "peerbus::local::shm",
            key = %key,
            error = %std::io::Error::last_os_error(),
            "could not reopen shm object to pre-reserve backing; \
             skipping reservation — a later allocation failure may abort via SIGBUS"
        );
        return Ok(());
    }

    // `fallocate` over a large range on tmpfs can be interrupted (the
    // kernel bails on `fatal_signal_pending`, surfacing as EINTR); retry a
    // bounded number of times before giving up so a stray signal does not
    // silently drop us onto the faulting fallback path.
    let mut errno = 0;
    for _ in 0..8 {
        // SAFETY: `fd` is a valid, open descriptor to the segment inode.
        // `mode == 0` allocates (and zero-fills, on tmpfs) `total_size`
        // bytes.
        let rc = unsafe { libc::fallocate(fd, 0, 0, total_size as libc::off_t) };
        if rc == 0 {
            errno = 0;
            break;
        }
        errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(0);
        if errno != libc::EINTR {
            break;
        }
    }
    // SAFETY: closing our own extra descriptor; the crate keeps its own.
    unsafe {
        libc::close(fd);
    }

    match errno {
        // Fully reserved (or a residual EINTR after the bounded retries —
        // treat as best-effort success and let the write proceed).
        0 | libc::EINTR => Ok(()),
        // Genuine exhaustion (space or per-user quota): report cleanly so
        // the caller never reaches the faulting write.
        libc::ENOSPC | libc::EDQUOT => Err(Error::ShmExhausted(format!(
            "cannot reserve {total_size} bytes for {key}: {}",
            std::io::Error::from_raw_os_error(errno)
        ))),
        // `fallocate` is genuinely unsupported here (non-tmpfs/hugetlbfs,
        // or a pre-3.5 kernel: EOPNOTSUPP/ENOSYS) — or some other errno we
        // cannot interpret as exhaustion. Keep the previous behaviour and
        // still attempt the create, but make the degraded path observable:
        // the subsequent write can SIGBUS if the backing store is short.
        _ => {
            crate::qb_warn!(
                target: "peerbus::local::shm",
                key = %key,
                error = %std::io::Error::from_raw_os_error(errno),
                "fallocate could not pre-reserve shm backing (unsupported here); \
                 skipping reservation — a later allocation failure may abort via SIGBUS"
            );
            Ok(())
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn reserve_shm_backing(_key: &str, _total_size: usize) -> Result<()> {
    // No portable pre-reservation primitive here; retain lazy backing.
    Ok(())
}

pub(crate) fn unsafe_control(ptr: NonNull<u8>) -> &'static ControlBlock {
    // SAFETY: every segment mapping starts with a `ControlBlock`.
    unsafe { &*(ptr.as_ptr().cast::<ControlBlock>()) }
}

/// Block until an opener may safely read the control block, or fail with a
/// clean error.
///
/// SIGBUS-safety: on tmpfs (`/dev/shm`) a segment can be *sized* (the crate
/// `ftruncate`d it) yet not *backed* — a creator is still mid-setup, or its
/// backing reservation failed and it is about to unlink a doomed object. A
/// direct load of `magic` on such a mapping page-faults, and when the tmpfs
/// (or a quota) is exhausted that fault is delivered as SIGBUS — fatal and
/// uncatchable (proven: a hole read under an exhausted `/dev/shm` quota
/// SIGBUSes). So before *every* read of the mapping we first force the
/// control page resident with `MADV_POPULATE_READ`, which faults the page
/// in eagerly and returns an ordinary error instead of raising a signal
/// when it cannot be backed. Only once the page is confirmed backed do we
/// read `magic`.
///
/// Ordering/lifetime: a creator publishes `magic` (Release) only *after* it
/// has fully reserved and zeroed the whole segment (see `create`), so
/// observing `magic == MAGIC` (Acquire) here establishes happens-before
/// with that setup and guarantees every page of the segment is backed —
/// making all later slot access SIGBUS-free too. A creator that never
/// finishes (crashed mid-setup, Finding #4) leaves `magic` unset; the
/// bounded deadline then returns a clean error rather than hanging or
/// faulting.
/// Poisoned-name recovery (Finding #4): a creator that dies between
/// `ftruncate` and the `magic` store leaves a sized-but-uninitialised object.
/// `magic` never appears, so before this machinery every later
/// `open_or_create` failed forever and the name needed a manual
/// `rm /dev/shm/qb_*`. We use the responsible-party stamp (published *before*
/// the long init, see `create`) to tell the cases apart, and — critically —
/// the recovery is itself self-healing:
///
///   * responsible PID is alive -> it is still working; keep waiting.
///   * responsible PID is dead  -> abandoned; *take over* (install our own PID
///     via CAS) and reclaim: exactly one live party can win the CAS.
///   * PID still 0 past [`CREATOR_STAMP_GRACE`] -> the creator died in the
///     microsecond window before it could stamp; take over and reclaim.
///
/// Race-safety and self-healing: the take-over is a single `compare_exchange`
/// on `creator_pid` *in the doomed inode's own control block*, so it is bound
/// to that inode, not the name. At any instant at most one *live* party holds
/// the responsibility, and only that party unlinks — so a concurrent
/// detection can never unlink a healthy segment a peer rebuilt (that is why
/// the single-unlinker guarantee the split-brain fix relies on still holds).
/// Unlike the old fixed `CONDEMNED` sentinel, a responsible party that *dies*
/// (crashes between the CAS and the unlink, trigger a) leaves its own now-dead
/// PID behind, so the next opener observes it dead and takes over in turn —
/// no permanent poison. And when the reclaimer has no inode identity to unlink
/// with (`have_identity == false`, e.g. fd exhaustion, trigger b) it does NOT
/// take over — it returns `Retry`, leaving the inode untouched for a later,
/// better-resourced attempt, rather than installing a responsibility it cannot
/// discharge.
pub(crate) fn wait_until_initialised(
    ptr: NonNull<u8>,
    name: &str,
    reclaim: bool,
    have_identity: bool,
) -> Result<InitState> {
    let control = unsafe_control(ptr);
    let start = Instant::now();
    let deadline = start + ATTACH_DEADLINE;
    loop {
        // Fault-free gate: don't touch the mapping unless its first page is
        // (or can be) backed. `NotBacked` means "still a hole" — treat as
        // not-yet-initialised and keep waiting. `Backed`/`Unsupported`
        // (older kernel / non-Linux) fall through to the loads, matching the
        // prior behaviour on platforms without the probe.
        if !matches!(probe_page_backed(ptr), PageBacking::NotBacked) {
            if control.magic.load(Ordering::Acquire) == MAGIC {
                return Ok(InitState::Ready);
            }

            // `magic` is not set. Consult the responsible-party stamp to
            // decide whether to wait for a live party or take over a dead one.
            //
            // Ordering: `creator_pid` is published with `Release` *after*
            // `creator_token`, so an `Acquire` load of a real PID here always
            // pairs with the matching token below — the liveness check can
            // never be fooled by a token left over from a prior incarnation.
            let responsible = control.creator_pid.load(Ordering::Acquire);
            let abandoned = if responsible == 0 {
                // Unstamped. The creator stamps within microseconds of sizing
                // the object, so past the grace period this means it died
                // before it could stamp.
                start.elapsed() >= CREATOR_STAMP_GRACE
            } else {
                let token = control.creator_token.load(Ordering::Acquire);
                !process_alive(responsible, token)
            };

            if abandoned {
                if !reclaim {
                    // Caller cannot rebuild the segment, so it must not take
                    // it over either; report as before.
                    return Err(Error::incompatible_shm(format!(
                        "service {name}: creator died before finishing initialisation"
                    )));
                }
                if !have_identity {
                    // We could not capture this inode's identity (e.g. fd
                    // exhaustion), so we cannot verify/perform the unlink.
                    // Never take over a segment we cannot clean up — that would
                    // strand the name. Back off; a later attempt reclaims it.
                    return Ok(InitState::Retry);
                }
                // Take over responsibility for this dead inode. Publish our
                // reclaimer identity — token `0` means "kill(pid,0) liveness"
                // and, because every reclaimer writes the *same* value, this
                // store is free of any pairing race with the CAS below. Then
                // CAS the dead PID to ours: exactly one live party wins and
                // becomes the sole unlinker.
                control.creator_token.store(0, Ordering::Relaxed);
                return match control.creator_pid.compare_exchange(
                    responsible,
                    current_pid(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        crate::qb_warn!(
                            target: "peerbus::local::shm",
                            service = %name,
                            dead_pid = responsible,
                            "shm segment's responsible process died mid-initialisation; reclaiming"
                        );
                        Ok(InitState::Reclaim)
                    }
                    // Lost the race: a peer took over. Abandon this mapping and
                    // retry by name (it will resolve to the fresh incarnation).
                    Err(_) => Ok(InitState::Retry),
                };
            }
            // Responsible party is alive and still initialising: keep waiting.
        }

        if Instant::now() >= deadline {
            return Err(Error::incompatible_shm(format!(
                "service {name} did not finish initialising"
            )));
        }
        std::thread::yield_now();
    }
}

/// What an attach found, once the segment's initialisation state is known.
pub(crate) enum Attach<H: Pod + Zeroable + Copy + 'static> {
    /// Fully initialised and validated; ready to use.
    Ready(Arc<Segment<H>>),
    /// The responsible party died mid-initialisation and *we* took over the
    /// inode: the caller must inode-verified-unlink the name (only if it still
    /// resolves to `(dev, ino)`) and retry. `(dev, ino)` is the inode we
    /// mapped and arbitrated on (from `capture_mapped_identity`), so a name
    /// repurposed to a healthy inode simply fails the match — never destroyed.
    Reclaim { dev: u64, ino: u64 },
    /// A peer is reclaiming this segment. Abandon the mapping and retry by
    /// name, which will resolve to the fresh incarnation.
    Retry,
}

/// Initialisation state of a mapped segment, as seen by an opener.
pub(crate) enum InitState {
    Ready,
    Reclaim,
    Retry,
}

pub(crate) enum PageBacking {
    /// The probed page is resident and safe to read.
    Backed,
    /// The page is an unbacked hole and reading it may fault; do not read.
    NotBacked,
    /// No fault-free probe is available here; caller may read as before.
    Unsupported,
}

/// Force the control page resident without risking a fault. On Linux
/// `MADV_POPULATE_READ` faults the range in and reports failure via `errno`
/// (EFAULT/ENOMEM/EIO) instead of SIGBUS; `EINVAL` means the kernel is too
/// old for the advice, in which case we report `Unsupported`.
#[cfg(target_os = "linux")]
pub(crate) fn probe_page_backed(ptr: NonNull<u8>) -> PageBacking {
    // Only the first page (holding `magic` and the rest of the control
    // block) needs to be resident to read `magic`; a matched `magic` then
    // implies the whole segment is backed.
    let len = std::mem::size_of::<ControlBlock>();
    // SAFETY: `ptr` is the page-aligned base of a live mapping of at least
    // `size_of::<ControlBlock>()` bytes; `madvise` only reads/populates.
    let rc = unsafe { libc::madvise(ptr.as_ptr().cast(), len, libc::MADV_POPULATE_READ) };
    if rc == 0 {
        return PageBacking::Backed;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::EINVAL) => PageBacking::Unsupported,
        _ => PageBacking::NotBacked,
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn probe_page_backed(_ptr: NonNull<u8>) -> PageBacking {
    PageBacking::Unsupported
}

pub(crate) fn validate_control<H: Pod + Zeroable + Copy + 'static>(
    control: &ControlBlock,
    expected_type_hash: u64,
) -> Result<()> {
    let version = control.version.load(Ordering::Acquire);
    if version != VERSION {
        return Err(Error::incompatible_shm(format!(
            "version mismatch: expected {VERSION}, got {version}"
        )));
    }

    let actual_type_hash = ((control.type_hash_hi.load(Ordering::Acquire) as u64) << 32)
        | control.type_hash_lo.load(Ordering::Acquire) as u64;
    if actual_type_hash != expected_type_hash {
        return Err(Error::incompatible_shm(format!(
            "type hash mismatch: expected {expected_type_hash:#x}, got {actual_type_hash:#x}"
        )));
    }

    let actual_header_size = control.header_size.load(Ordering::Acquire) as usize;
    let expected_header_size = std::mem::size_of::<H>();
    if actual_header_size != expected_header_size {
        return Err(Error::incompatible_shm(format!(
            "header size mismatch: expected {expected_header_size}, got {actual_header_size}"
        )));
    }

    Ok(())
}

pub(crate) fn validate_layout(control: &ControlBlock, expected: &Layout) -> Result<()> {
    let slot_count = control.slot_count.load(Ordering::Acquire) as usize;
    let slot_stride = control.slot_stride.load(Ordering::Acquire) as usize;
    let payload_cap = control.payload_cap.load(Ordering::Acquire) as usize;
    if slot_count != expected.slot_count
        || slot_stride != expected.slot_stride
        || payload_cap != expected.payload_cap
    {
        return Err(Error::incompatible_shm(format!(
            "layout mismatch: slots {slot_count}/{}, stride {slot_stride}/{}, payload {payload_cap}/{}",
            expected.slot_count, expected.slot_stride, expected.payload_cap
        )));
    }
    Ok(())
}

