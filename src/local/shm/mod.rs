//! Pure-Rust shared-memory ring used by the local transport.
//!
//! This module is intentionally small and POD-only: one named
//! `shared_memory` mapping per service, a fixed control block, and a
//! fixed-size ring of slots. The public local API still exposes
//! loan/fill/publish and non-blocking take; the implementation details
//! stay private to `src/local`.

use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytemuck::{Pod, Zeroable};
use shared_memory::{Shmem, ShmemConf};

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::transport::fnv1a64;

const MAGIC: u64 = 0x5155_4943_4249_5431; // "PEERBUS1"
// Bumped from 3: `ControlBlock` gained the creator-identity stamp
// (`creator_pid` / `creator_token`), so the layout differs from segments
// written by older peers.
const VERSION: u32 = 4;
const MAX_SERVICE_NAME_BYTES: usize = 200;
const MAX_TRACKED_PUBLISHERS: usize = 64;
const MAX_TRACKED_SUBSCRIBERS: usize = 31;
const WRITER_STATE: u32 = u32::MAX;

/// How long an opener tolerates an *unstamped* control block (`creator_pid`
/// still 0) before declaring the segment poisoned. `create` stamps its pid
/// within microseconds of sizing the object (the stamp is the very first
/// thing it does after `ftruncate`), so a live creator is essentially never
/// seen unstamped past this window. It is deliberately far longer than any
/// plausible scheduling stall (preemption, cgroup CPU throttle, brief
/// SIGSTOP), so eagerly condemning a *live* creator is rare. Crucially,
/// *correctness does not depend on this value*: the common "creator died
/// after stamping" case is caught immediately and precisely by the pid+token
/// liveness check, which never uses this timer, and even if a live creator
/// were wrongly condemned via this backstop, its sticky-stamp CAS in `create`
/// then fails and it abandons the segment WITHOUT unlinking — so no split
/// brain results. The grace only trades a longer wait against needless
/// reclaim work.
const CREATOR_STAMP_GRACE: Duration = Duration::from_secs(1);

/// Overall deadline a single attach waits for a segment to become usable
/// (creator finishes, or an abandoned one is condemned). Must exceed
/// [`CREATOR_STAMP_GRACE`] so the unstamped backstop can actually fire.
const ATTACH_DEADLINE: Duration = Duration::from_secs(2);

/// Overall deadline for `open_or_create`, spanning any create/attach retries
/// and reclaim rounds. Must comfortably exceed [`ATTACH_DEADLINE`].
const OPEN_OR_CREATE_DEADLINE: Duration = Duration::from_secs(12);

/// Bound on how many poisoned segments a single `open_or_create` will
/// reclaim before giving up, so a pathological churn of dying creators can
/// never spin here forever.
const MAX_RECLAIM_ATTEMPTS: u32 = 8;

#[repr(C, align(64))]
struct ControlBlock {
    magic: AtomicU64,
    version: AtomicU32,
    /// PID of the process currently *responsible* for driving this segment to
    /// `MAGIC` (or for tearing it down if it turns out abandoned).
    ///
    ///   * `0` — not stamped yet (the creator is in the microsecond window
    ///     between sizing the object and stamping).
    ///   * original creator's PID — set once by `create` (see `creator_token`
    ///     for how its liveness is checked).
    ///   * a reclaimer's PID — a peer found the previous responsible party
    ///     dead and *took over* via a `compare_exchange` on this field, making
    ///     itself the one party that will unlink the name. If that reclaimer
    ///     then dies before unlinking, the next opener finds *its* PID dead and
    ///     takes over in turn — so cleanup is self-healing, and there is never
    ///     more than one *live* responsible party (hence never two concurrent
    ///     unlinkers of the same inode).
    ///
    /// There is deliberately no separate "condemned" sentinel: a dead
    /// responsible party IS the condemnation, and it can always be superseded.
    ///
    /// Ordering: the original creator stores `creator_token` *first* and
    /// `creator_pid` *last* with `Release`, so a reader that `Acquire`-loads a
    /// real PID always observes that process' matching token — the liveness
    /// check can never be fooled by a token from a previous incarnation.
    creator_pid: AtomicU32,
    /// Start-time token paired with `creator_pid` for reuse-proof liveness.
    ///
    /// `0` is a distinguished value meaning "check liveness by `kill(pid, 0)`
    /// only" (no `/proc` token). The *original creator* stores its real token
    /// (so a recycled PID can never masquerade as it over a long-lived
    /// poison). A *reclaimer* that takes over instead stores `0`: it is
    /// short-lived (it unlinks within microseconds), so PID reuse cannot
    /// realistically bite, and — crucially — every reclaimer writing the *same*
    /// value (`0`) makes concurrent take-over attempts free of any
    /// token/PID pairing race.
    creator_token: AtomicU64,
    type_hash_hi: AtomicU32,
    type_hash_lo: AtomicU32,
    header_size: AtomicU32,
    payload_cap: AtomicU32,
    slot_count: AtomicU32,
    slot_stride: AtomicU32,
    write_seq: AtomicU64,
    publishers: AtomicU32,
    subscribers: AtomicU32,
    max_publishers: AtomicU32,
    max_subscribers: AtomicU32,
    subscriber_buffer: AtomicU32,
    history_depth: AtomicU32,
    publisher_pids: [AtomicU32; MAX_TRACKED_PUBLISHERS],
    publisher_tokens: [AtomicU64; MAX_TRACKED_PUBLISHERS],
    subscriber_pids: [AtomicU32; MAX_TRACKED_SUBSCRIBERS],
    subscriber_tokens: [AtomicU64; MAX_TRACKED_SUBSCRIBERS],
}

#[repr(C, align(8))]
struct SlotHeader {
    seq: AtomicU64,
    /// Reader bitmask, or [`WRITER_STATE`] while a producer owns the slot.
    ///
    /// Each subscriber gets one bit for the life of the subscriber plus
    /// any samples derived from it. That makes dead-reader cleanup
    /// possible: if a process dies while holding a sample, the next
    /// publisher can clear only that process' bit instead of pinning the
    /// slot forever.
    refcount: AtomicU32,
    len: AtomicU32,
    writer_pid: AtomicU32,
    writer_token: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct Layout {
    total_size: usize,
    slots_offset: usize,
    slot_stride: usize,
    header_offset: usize,
    payload_offset: usize,
    payload_cap: usize,
    slot_count: usize,
}

impl Layout {
    fn new<H>(cfg: &LocalConfig) -> Result<Self> {
        validate_config(cfg)?;
        let payload_cap = cfg.max_payload_bytes;
        if payload_cap > u32::MAX as usize {
            return Err(Error::invalid_argument(format!(
                "max_payload_bytes too large: {payload_cap}"
            )));
        }

        let history = cfg.history_depth.max(1) as usize;
        let subscribers = cfg.max_subscribers.max(1) as usize;
        let slot_count = history
            .saturating_mul(subscribers)
            .max(cfg.subscriber_buffer.max(1) as usize)
            .max(1);
        if slot_count > u32::MAX as usize {
            return Err(Error::invalid_argument(format!(
                "slot count too large: {slot_count}"
            )));
        }

        let header_align = std::mem::align_of::<H>().max(1);
        let header_offset = align_up(std::mem::size_of::<SlotHeader>(), header_align);
        let payload_offset = header_offset
            .checked_add(std::mem::size_of::<H>())
            .ok_or_else(|| Error::invalid_argument("slot layout overflow"))?;
        // Every slot base (`slots_offset + index * slot_stride`) must satisfy
        // both `SlotHeader`'s alignment and `H`'s alignment: the header lives
        // at `header_offset` (a multiple of `header_align`) from the base, so
        // the base must be aligned to `align_of::<H>()` for `header_ptr(index)`
        // to be well-aligned at *every* index, and to `align_of::<SlotHeader>()`
        // for the leading header. Align the stride and the first slot's offset
        // to this shared alignment (never below 8).
        let slot_align = std::mem::align_of::<SlotHeader>()
            .max(std::mem::align_of::<H>())
            .max(8);
        let slot_stride = align_up(
            payload_offset
                .checked_add(payload_cap)
                .ok_or_else(|| Error::invalid_argument("slot layout overflow"))?,
            slot_align,
        );
        let slots_offset = align_up(std::mem::size_of::<ControlBlock>(), slot_align.max(64));
        let total_size = slots_offset
            .checked_add(
                slot_stride
                    .checked_mul(slot_count)
                    .ok_or_else(|| Error::invalid_argument("segment layout overflow"))?,
            )
            .ok_or_else(|| Error::invalid_argument("segment layout overflow"))?;

        Ok(Self {
            total_size,
            slots_offset,
            slot_stride,
            header_offset,
            payload_offset,
            payload_cap,
            slot_count,
        })
    }
}

fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (value + align - 1) & !(align - 1)
}

/// A named SHM segment containing one typed ring.
pub(crate) struct Segment<H: Pod + Zeroable + Copy + 'static> {
    _shmem: Shmem,
    ptr: NonNull<u8>,
    layout: Layout,
    key: String,
    type_hash: u64,
    /// Identity (`dev`, `ino`) of the inode this segment actually maps, read
    /// from the mapping itself (`capture_mapped_identity`), not by re-resolving
    /// the name. Used to make the drop-time `shm_unlink` *inode-verified*: we
    /// remove the name only while it still resolves to the very inode we map,
    /// never a repurposed one. Because it is the mapped inode (not a by-name
    /// stat), a repurposed name can only fail the match, never match a
    /// different healthy inode.
    dev: u64,
    ino: u64,
    /// Whether this handle is responsible for unlinking the OS name when the
    /// last local reference drops. Only the creating handle sets this; every
    /// opener leaves the name alone (an opener that later becomes a reclaimer
    /// unlinks through the coordinated reclaim path, not on drop).
    owns_name: bool,
    _header: PhantomData<H>,
}

// SAFETY: `Segment` points at a shared mapping whose interior mutation is
// coordinated by atomics in the mapping. `H` is POD and copied as bytes.
unsafe impl<H: Pod + Zeroable + Copy + 'static> Send for Segment<H> {}
// SAFETY: Shared access to the mapping is safe under the same atomic
// protocol; mutable user access is only handed out through a claimed `Loan`.
unsafe impl<H: Pod + Zeroable + Copy + 'static> Sync for Segment<H> {}

impl<H: Pod + Zeroable + Copy + 'static> Drop for Segment<H> {
    fn drop(&mut self) {
        // Normal teardown. Because we disabled the `shared_memory` crate's
        // blind owner-unlink (`set_owner(false)`), peerbus must remove the OS
        // name itself when the creating handle's last local reference drops —
        // otherwise `/dev/shm` leaks. Only the creating handle owns the name;
        // openers leave it alone (the inode stays live for them regardless).
        //
        // Inode-verified: `unlink_if_matches` removes the name only while it
        // still resolves to the inode this handle created, so a name a peer has
        // meanwhile repurposed to a healthy segment is never destroyed. The
        // `_shmem` field's `Drop` (which unmaps + closes, but no longer
        // unlinks) runs after this, so the mapping is still valid here.
        if self.owns_name {
            unlink_if_matches(&self.key, self.dev, self.ino);
        }
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Segment<H> {
    pub(crate) fn open_or_create(
        name: &str,
        type_hash: u64,
        cfg: LocalConfig,
    ) -> Result<Arc<Self>> {
        // Two callers racing on the same name must converge: exactly one wins
        // the exclusive `create` (`shm_open(O_CREAT|O_EXCL)`); the others
        // attach. This loop rescues two distinct failures.
        //
        // 1. The narrow *unsized* window: the winner has created the OS name
        //    (so a racer's `create` returns "already exists") but has not yet
        //    `ftruncate`d it, so the racer's attach fails with
        //    `ServiceNotFound` (a zero-length object cannot be `mmap`ed).
        //    Retry until the winner finishes sizing or the deadline elapses.
        //
        // 2. A *poisoned* name: the party responsible for a sized segment died
        //    before storing `MAGIC`. The object exists and is sized, so
        //    `O_EXCL` create always fails and the attach can never see `MAGIC`
        //    — the service used to be dead forever, needing a manual
        //    `rm /dev/shm/qb_*`. Now the attach proves the responsible party
        //    is dead and *takes over* the inode (installs our PID via CAS);
        //    exactly one live racer wins and becomes the sole reclaimer,
        //    unlinking the name here so the next `create` makes a fresh
        //    segment. If that reclaimer dies before unlinking, the next opener
        //    finds *its* PID dead and takes over in turn — self-healing.
        //
        // Any other error (a genuine `IncompatibleShm` mismatch, `ShmExhausted`,
        // bad config) is permanent and returned at once.
        let deadline = Instant::now() + OPEN_OR_CREATE_DEADLINE;
        let mut backoff = Duration::from_micros(50);
        let mut reclaims = 0u32;
        loop {
            match Self::create(name, type_hash, cfg.clone()) {
                Ok(segment) => return Ok(segment),
                // Permanent errors: bad config, oversized name, or the
                // host cannot back the segment. Never retry these.
                Err(err @ Error::InvalidArgument(_))
                | Err(err @ Error::TopicNameTooLong { .. })
                | Err(err @ Error::ShmExhausted(_)) => return Err(err),
                // Otherwise the segment already exists (or a peer is mid
                // creation): attach to it.
                Err(_) => match Self::attach(name, type_hash, true) {
                    Ok(Attach::Ready(segment)) => return Ok(segment),
                    // We won the take-over of a dead party's inode, so we are
                    // the only live party that may unlink it. `unlink_if_matches`
                    // removes the name only while it still resolves to
                    // `(dev, ino)` — the very inode we took over — so we can
                    // never destroy a healthy segment a peer rebuilt under the
                    // same name. Then loop: we, or a peer, will win a fresh
                    // `create`.
                    Ok(Attach::Reclaim { dev, ino }) => {
                        reclaims += 1;
                        if reclaims > MAX_RECLAIM_ATTEMPTS {
                            return Err(Error::incompatible_shm(format!(
                                "service {name}: gave up after reclaiming {} poisoned segments",
                                reclaims - 1
                            )));
                        }
                        unlink_if_matches(&os_key(name), dev, ino);
                        backoff = Duration::from_micros(50);
                    }
                    // Someone else is reclaiming this segment (or already
                    // condemned it). Our mapping is of a doomed inode that
                    // will never be initialised — abandon it and retry by
                    // name, which will resolve to the fresh incarnation.
                    Ok(Attach::Retry) if Instant::now() < deadline => {
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(Duration::from_millis(2));
                    }
                    Ok(Attach::Retry) => {
                        return Err(Error::incompatible_shm(format!(
                            "service {name}: timed out waiting for a peer to reclaim it"
                        )));
                    }
                    Err(Error::ServiceNotFound(_)) if Instant::now() < deadline => {
                        // Sleep with a small capped backoff rather than a
                        // hot `yield_now()` spin: a stuck peer would
                        // otherwise churn shm_open/mmap/munmap for the full
                        // 2 s. Capped so convergence stays sub-millisecond
                        // once the winner finishes sizing.
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(Duration::from_millis(2));
                    }
                    Err(err) => return Err(err),
                },
            }
        }
    }

    pub(crate) fn create(name: &str, type_hash: u64, cfg: LocalConfig) -> Result<Arc<Self>> {
        validate_name(name)?;
        let key = os_key(name);
        let layout = Layout::new::<H>(&cfg)?;
        let mut shmem = ShmemConf::new()
            .os_id(&key)
            .size(layout.total_size)
            .create()
            .map_err(|e| Error::Other(format!("shared_memory create {key}: {e}")))?;

        // Take over the OS name's lifetime from the `shared_memory` crate.
        //
        // The crate's owner-drop calls `shm_unlink(name)` *unconditionally* on
        // the NAME, ignoring which inode that name currently resolves to. If a
        // live-but-slow creator were ever condemned (see below) and its name
        // rebuilt onto a fresh inode by a peer, that blind unlink would destroy
        // the peer's healthy segment — split brain. So peerbus becomes the sole
        // unlink authority and only ever unlinks *inode-verified* (see
        // `unlink_if_matches` / `Drop for Segment`).
        shmem.set_owner(false);

        let ptr = NonNull::new(shmem.as_ptr()).ok_or_else(|| {
            Error::Other(format!("shared_memory create {key}: returned null mapping"))
        })?;

        // Record the identity of the inode we just mapped, read from the
        // mapping itself (not a by-name lookup), so the identity we later
        // unlink against is unconditionally the mapped-and-arbitrated inode. If
        // capture fails (only plausibly under fd exhaustion / no `/proc`), we
        // must not risk a blind unlink of a future occupant of this name, so we
        // mark the handle non-owning: the segment then relies on the reclaim
        // path for eventual cleanup.
        let (dev, ino, owns_name) = match capture_mapped_identity(ptr, &key) {
            Some((dev, ino)) => (dev, ino, true),
            None => {
                crate::qb_warn!(
                    target: "peerbus::local::shm",
                    key = %key,
                    "could not capture freshly created shm inode identity; \
                     leaving it unowned so we never blind-unlink a repurposed name"
                );
                (0, 0, false)
            }
        };

        // Reserve backing store *before* touching any page. `shared_memory`'s
        // create does `shm_open` + `ftruncate` + `mmap`; on tmpfs (`/dev/shm`)
        // `ftruncate` only sets the file *size* and leaves it sparse, so pages
        // are allocated lazily on first write. If the tmpfs (or the process'
        // tmpfs quota) is exhausted, that first write faults as SIGBUS —
        // process-fatal and uncatchable. `fallocate` commits the blocks up
        // front and reports exhaustion as an ordinary `ENOSPC`/`EDQUOT` error
        // instead. On failure `shmem` drops here (owner=false, so only unmap).
        //
        // Two steps, so the creator stamp below lands on backed memory as early
        // as possible: first the control page (cheap), then the whole segment.
        reserve_shm_backing(&key, std::mem::size_of::<ControlBlock>())?;

        // SAFETY: the control page is now backed, and the object is a brand
        // new inode (`O_CREAT|O_EXCL`) so it reads as zero — but zero the
        // control block explicitly before stamping so the identity we publish
        // sits on known-clean storage.
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr(), 0, std::mem::size_of::<ControlBlock>());
        }

        // Stamp our identity NOW — the very first thing after sizing — so an
        // opener racing this create can tell "creator still working" from
        // "creator died mid-init". Without it, a creator that dies after
        // `ftruncate` but before `MAGIC` poisons the name forever.
        //
        // Ordering: token first (Relaxed), pid last (Release). A reader that
        // Acquire-loads a real pid therefore always observes the matching
        // token, so its liveness check can never be fooled by a stale one.
        //
        // Sticky CAS `0 -> pid`: it must only stamp an *unstamped* control
        // block. If a peer already took responsibility for this inode (found us
        // unstamped past the grace and installed its own reclaimer PID — only
        // reachable if we somehow stalled in the microsecond pre-stamp window),
        // `creator_pid` is non-zero and the CAS fails. We then ABANDON: return
        // without storing `MAGIC` and without unlinking (the reclaimer owns
        // cleanup of this inode). This is what makes a wrongly-reclaimed live
        // creator safe — it never fights the reclaimer over the name.
        //
        // Test-only hook: hold the segment in the unstamped window (pid still
        // 0) so a peer can attempt to (wrongly) reclaim a *live* creator. Zero
        // cost and entirely absent in non-test builds.
        #[cfg(test)]
        tests::stall_before_stamp_hook();

        let control = unsafe_control(ptr);
        control
            .creator_token
            .store(current_process_token(), Ordering::Relaxed);
        if control
            .creator_pid
            .compare_exchange(0, current_pid(), Ordering::Release, Ordering::Acquire)
            .is_err()
        {
            // `shmem` drops here with owner=false → unmap only, no unlink.
            return Err(Error::Other(format!(
                "shm segment {key}: superseded by a reclaimer during initialisation; retrying"
            )));
        }

        // Now the long part: back and zero the rest of the segment. An opener
        // that shows up during this sees `magic == 0` but a *live* creator
        // stamp, and correctly waits instead of reclaiming.
        reserve_shm_backing(&key, layout.total_size)?;

        // SAFETY: `ptr` is a valid writable mapping of `layout.total_size`
        // bytes, now fully backed. Zero only the slot region: `slots_offset`
        // is always >= `size_of::<ControlBlock>()`, so this cannot clobber the
        // creator stamp we just published. The control block was zeroed above
        // and every scalar in it is written by `init_control`; the pid/token
        // arrays stay zero, as required.
        unsafe {
            std::ptr::write_bytes(
                ptr.as_ptr().add(layout.slots_offset),
                0,
                layout.total_size - layout.slots_offset,
            );
        }

        let segment = Arc::new(Self {
            _shmem: shmem,
            ptr,
            layout,
            key,
            type_hash,
            dev,
            ino,
            owns_name,
            _header: PhantomData,
        });
        segment.init_control(&cfg)?;
        Ok(segment)
    }

    pub(crate) fn open_existing(name: &str, type_hash: u64) -> Result<Arc<Self>> {
        // A bare attach has no `LocalConfig` and therefore cannot re-create a
        // reclaimed segment, so it must not condemn one either: pass
        // `reclaim = false` and report a poisoned segment as the same
        // `IncompatibleShm` error callers already handle. Only
        // `open_or_create`, which *can* rebuild the segment, drives reclaim.
        match Self::attach(name, type_hash, false)? {
            Attach::Ready(segment) => Ok(segment),
            // Unreachable with `reclaim = false` (`attach` returns
            // `IncompatibleShm` instead), but stay total rather than panic.
            Attach::Reclaim { .. } | Attach::Retry => Err(Error::incompatible_shm(format!(
                "service {name}: creator died before finishing initialisation"
            ))),
        }
    }

    /// Map an existing segment and decide what state it is in.
    ///
    /// `reclaim` enables condemnation of a segment whose creator died
    /// mid-initialisation; see [`Attach`] and `wait_until_initialised`.
    fn attach(name: &str, type_hash: u64, reclaim: bool) -> Result<Attach<H>> {
        validate_name(name)?;
        let key = os_key(name);
        let mut shmem = ShmemConf::new()
            .os_id(&key)
            .open()
            .map_err(|e| Error::ServiceNotFound(format!("{name} ({key}): {e}")))?;
        // Openers are never owners in the crate's model, but be explicit: no
        // handle other than the coordinated create/reclaim path unlinks.
        shmem.set_owner(false);
        let ptr = NonNull::new(shmem.as_ptr())
            .ok_or_else(|| Error::Other(format!("shared_memory open {key}: null mapping")))?;

        // Identity of the inode we ACTUALLY mapped, read from the mapping
        // itself (not a by-name lookup). This is exactly the inode arbitration
        // runs on below, so the identity we later unlink against is
        // unconditionally the mapped-and-arbitrated inode: a name repurposed in
        // the gap between `ShmemConf::open` and here can only make
        // `unlink_if_matches` FAIL (safe no-op), never match a *different*
        // healthy inode. See `capture_mapped_identity` / `unlink_if_matches`.
        //
        // If capture failed (e.g. fd exhaustion → `(0, 0)`), we have no
        // identity to verify an unlink against, so we must NOT take over /
        // reclaim: doing so would install a responsibility we cannot discharge
        // (condemn-without-unlink → permanent poison). Pass `have_identity =
        // false` so the segment is left intact for a later, better-resourced
        // attempt to reclaim it.
        let (dev, ino) = capture_mapped_identity(ptr, &key).unwrap_or((0, 0));
        let have_identity = dev != 0 || ino != 0;

        let control = unsafe_control(ptr);
        match wait_until_initialised(ptr, name, reclaim, have_identity)? {
            InitState::Ready => {}
            // We took over responsibility for this dead inode; hand the
            // decision (with the identity to verify) up. `shmem` (owner=false)
            // drops on return, unmapping only — the caller performs the
            // inode-verified unlink.
            InitState::Reclaim => return Ok(Attach::Reclaim { dev, ino }),
            InitState::Retry => return Ok(Attach::Retry),
        }
        validate_control::<H>(control, type_hash)?;

        let cfg = LocalConfig {
            max_publishers: control.max_publishers.load(Ordering::Acquire),
            max_subscribers: control.max_subscribers.load(Ordering::Acquire),
            subscriber_buffer: control.subscriber_buffer.load(Ordering::Acquire),
            history_depth: control.history_depth.load(Ordering::Acquire),
            max_payload_bytes: control.payload_cap.load(Ordering::Acquire) as usize,
        };
        let layout = Layout::new::<H>(&cfg)?;
        validate_layout(control, &layout)?;

        Ok(Attach::Ready(Arc::new(Self {
            _shmem: shmem,
            ptr,
            layout,
            key,
            type_hash,
            dev,
            ino,
            // Openers never unlink the name on drop; only the creating handle
            // does. A reclaimer unlinks through `open_or_create`, not here.
            owns_name: false,
            _header: PhantomData,
        })))
    }

    fn init_control(&self, cfg: &LocalConfig) -> Result<()> {
        let control = self.control();
        control.version.store(VERSION, Ordering::Relaxed);
        control
            .type_hash_hi
            .store((self.type_hash >> 32) as u32, Ordering::Relaxed);
        control
            .type_hash_lo
            .store(self.type_hash as u32, Ordering::Relaxed);
        control
            .header_size
            .store(std::mem::size_of::<H>() as u32, Ordering::Relaxed);
        control
            .payload_cap
            .store(self.layout.payload_cap as u32, Ordering::Relaxed);
        control
            .slot_count
            .store(self.layout.slot_count as u32, Ordering::Relaxed);
        control
            .slot_stride
            .store(self.layout.slot_stride as u32, Ordering::Relaxed);
        control.write_seq.store(0, Ordering::Relaxed);
        control.publishers.store(0, Ordering::Relaxed);
        control.subscribers.store(0, Ordering::Relaxed);
        control
            .max_publishers
            .store(cfg.max_publishers, Ordering::Relaxed);
        control
            .max_subscribers
            .store(cfg.max_subscribers, Ordering::Relaxed);
        control
            .subscriber_buffer
            .store(cfg.subscriber_buffer, Ordering::Relaxed);
        control
            .history_depth
            .store(cfg.history_depth, Ordering::Relaxed);
        control.magic.store(MAGIC, Ordering::Release);
        Ok(())
    }

    pub(crate) fn publisher_count(&self) -> usize {
        self.reap_dead_publishers();
        self.control().publishers.load(Ordering::Acquire) as usize
    }

    pub(crate) fn producer(self: &Arc<Self>) -> Result<Producer<H>> {
        let lease = Arc::new(ProducerLease {
            segment: Arc::clone(self),
            slot: self.register_publisher()?,
            pid: current_pid(),
            token: current_process_token(),
        });
        Ok(Producer { lease })
    }

    pub(crate) fn consumer(self: &Arc<Self>) -> Result<Consumer<H>> {
        let consumer_id = self.register_subscriber()?;

        let control = self.control();
        let latest = control.write_seq.load(Ordering::Acquire);
        let history = control.history_depth.load(Ordering::Acquire).max(1) as u64;
        let read_cursor = latest.saturating_sub(history);

        Ok(Consumer {
            lease: Arc::new(ConsumerLease {
                segment: Arc::clone(self),
                slot: consumer_id,
                pid: current_pid(),
                token: current_process_token(),
            }),
            read_cursor,
        })
    }

    fn register_publisher(&self) -> Result<usize> {
        self.reap_dead_publishers();
        let control = self.control();
        let max = control.max_publishers.load(Ordering::Acquire) as usize;
        let pid = current_pid();
        let token = current_process_token();
        // Clamp to the fixed array bound: a peer that corrupts the shared
        // `max_publishers` above the tracked cap must not drive an OOB index
        // (or an oversized `1u32 << index` elsewhere). The reap paths clamp
        // identically. The returned index is therefore always < the cap.
        for index in 0..max.min(MAX_TRACKED_PUBLISHERS) {
            if control.publisher_pids[index]
                .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                control.publisher_tokens[index].store(token, Ordering::Release);
                control.publishers.fetch_add(1, Ordering::AcqRel);
                return Ok(index);
            }
        }
        self.reap_dead_publishers();
        Err(Error::Other(format!(
            "too many publishers on {}: max {}",
            self.key, max
        )))
    }

    fn deregister_publisher(&self, index: usize, pid: u32, token: u64) {
        let control = self.control();
        if control.publisher_tokens[index].load(Ordering::Acquire) != token {
            return;
        }
        if control.publisher_pids[index]
            .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            control.publisher_tokens[index].store(0, Ordering::Release);
            decrement_counter(&control.publishers);
        }
    }

    fn reap_dead_publishers(&self) {
        let control = self.control();
        let max = control.max_publishers.load(Ordering::Acquire) as usize;
        for index in 0..max.min(MAX_TRACKED_PUBLISHERS) {
            let pid = control.publisher_pids[index].load(Ordering::Acquire);
            let token = control.publisher_tokens[index].load(Ordering::Acquire);
            if pid != 0
                && !process_alive(pid, token)
                && control.publisher_pids[index]
                    .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                control.publisher_tokens[index].store(0, Ordering::Release);
                decrement_counter(&control.publishers);
            }
        }
    }

    fn register_subscriber(&self) -> Result<usize> {
        self.reap_dead_subscribers();
        let control = self.control();
        let max = control.max_subscribers.load(Ordering::Acquire) as usize;
        let pid = current_pid();
        let token = current_process_token();
        // Clamp to the fixed array bound so a corrupted `max_subscribers`
        // cannot force an OOB index or an oversized `1u32 << index` shift in
        // the reader-bit paths. The returned index is therefore always < the
        // cap, keeping `1u32 << slot` well-defined.
        for index in 0..max.min(MAX_TRACKED_SUBSCRIBERS) {
            if control.subscriber_pids[index]
                .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                control.subscriber_tokens[index].store(token, Ordering::Release);
                control.subscribers.fetch_add(1, Ordering::AcqRel);
                return Ok(index);
            }
        }
        self.reap_dead_subscribers();
        Err(Error::Other(format!(
            "too many subscribers on {}: max {}",
            self.key, max
        )))
    }

    fn deregister_subscriber(&self, index: usize, pid: u32, token: u64) {
        let control = self.control();
        if control.subscriber_tokens[index].load(Ordering::Acquire) != token {
            return;
        }
        if control.subscriber_pids[index]
            .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            control.subscriber_tokens[index].store(0, Ordering::Release);
            decrement_counter(&control.subscribers);
        }
    }

    fn reap_dead_subscribers(&self) {
        let control = self.control();
        let max = control.max_subscribers.load(Ordering::Acquire) as usize;
        for index in 0..max.min(MAX_TRACKED_SUBSCRIBERS) {
            let pid = control.subscriber_pids[index].load(Ordering::Acquire);
            let token = control.subscriber_tokens[index].load(Ordering::Acquire);
            if pid != 0
                && !process_alive(pid, token)
                && control.subscriber_pids[index]
                    .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                control.subscriber_tokens[index].store(0, Ordering::Release);
                decrement_counter(&control.subscribers);
            }
        }
    }

    fn reap_dead_slot_holders(&self, slot: &SlotHeader) {
        let state = slot.refcount.load(Ordering::Acquire);
        if state == 0 {
            return;
        }
        if state == WRITER_STATE {
            let pid = slot.writer_pid.load(Ordering::Acquire);
            let token = slot.writer_token.load(Ordering::Acquire);
            if pid != 0
                && !process_alive(pid, token)
                && slot
                    .refcount
                    .compare_exchange(WRITER_STATE, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                slot.writer_pid.store(0, Ordering::Release);
                slot.writer_token.store(0, Ordering::Release);
                slot.seq.store(0, Ordering::Release);
            }
            return;
        }

        let control = self.control();
        let max = control.max_subscribers.load(Ordering::Acquire) as usize;
        for index in 0..max.min(MAX_TRACKED_SUBSCRIBERS) {
            let bit = 1u32 << index;
            if state & bit == 0 {
                continue;
            }
            let pid = control.subscriber_pids[index].load(Ordering::Acquire);
            let token = control.subscriber_tokens[index].load(Ordering::Acquire);
            if pid == 0 || !process_alive(pid, token) {
                slot.refcount.fetch_and(!bit, Ordering::AcqRel);
                if pid != 0
                    && control.subscriber_pids[index]
                        .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    control.subscriber_tokens[index].store(0, Ordering::Release);
                    decrement_counter(&control.subscribers);
                }
            }
        }
    }

    fn control(&self) -> &ControlBlock {
        unsafe_control(self.ptr)
    }

    fn slot_header(&self, index: usize) -> &SlotHeader {
        debug_assert!(index < self.layout.slot_count);
        let offset = self.layout.slots_offset + index * self.layout.slot_stride;
        // SAFETY: layout construction keeps every slot within the mapping
        // and aligned to at least 8 bytes for `SlotHeader`.
        unsafe { &*(self.ptr.as_ptr().add(offset).cast::<SlotHeader>()) }
    }

    fn header_ptr(&self, index: usize) -> *mut H {
        let offset =
            self.layout.slots_offset + index * self.layout.slot_stride + self.layout.header_offset;
        // SAFETY: caller uses the pointer according to the slot protocol.
        unsafe { self.ptr.as_ptr().add(offset).cast::<H>() }
    }

    fn payload_ptr(&self, index: usize) -> *mut u8 {
        let offset =
            self.layout.slots_offset + index * self.layout.slot_stride + self.layout.payload_offset;
        // SAFETY: caller bounds slices by `payload_cap`/recorded `len`.
        unsafe { self.ptr.as_ptr().add(offset) }
    }

    fn slot_index(&self, seq: u64) -> usize {
        ((seq - 1) as usize) % self.layout.slot_count
    }
}

pub(crate) struct Producer<H: Pod + Zeroable + Copy + 'static> {
    lease: Arc<ProducerLease<H>>,
}

struct ProducerLease<H: Pod + Zeroable + Copy + 'static> {
    segment: Arc<Segment<H>>,
    slot: usize,
    pid: u32,
    token: u64,
}

impl<H: Pod + Zeroable + Copy + 'static> Producer<H> {
    pub(crate) fn loan(&mut self, byte_count: usize) -> Result<Loan<H>> {
        if byte_count > self.lease.segment.layout.payload_cap {
            return Err(Error::PayloadTooLarge {
                actual: byte_count,
                capacity: self.lease.segment.layout.payload_cap,
            });
        }

        let mut spin_attempts = 0u32;
        let (seq, index, slot) = loop {
            let current = self
                .lease
                .segment
                .control()
                .write_seq
                .load(Ordering::Acquire);
            let seq = current + 1;
            let index = self.lease.segment.slot_index(seq);
            let slot = self.lease.segment.slot_header(index);

            let mut state = slot.refcount.load(Ordering::Acquire);
            if state != 0 {
                self.lease.segment.reap_dead_slot_holders(slot);
                state = slot.refcount.load(Ordering::Acquire);
            }
            if state != 0 {
                if state == WRITER_STATE && spin_attempts < 1024 {
                    spin_attempts += 1;
                    std::thread::yield_now();
                    continue;
                }
                return Err(Error::NoFreeSlot {
                    service: self.lease.segment.key.clone(),
                });
            }
            if let Err(actual) =
                slot.refcount
                    .compare_exchange(0, WRITER_STATE, Ordering::AcqRel, Ordering::Acquire)
            {
                if actual == WRITER_STATE && spin_attempts < 1024 {
                    spin_attempts += 1;
                    std::thread::yield_now();
                    continue;
                }
                return Err(Error::NoFreeSlot {
                    service: self.lease.segment.key.clone(),
                });
            }
            slot.writer_token
                .store(current_process_token(), Ordering::Release);
            slot.writer_pid.store(current_pid(), Ordering::Release);

            match self.lease.segment.control().write_seq.compare_exchange(
                current,
                seq,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break (seq, index, slot),
                Err(_) => {
                    slot.writer_pid.store(0, Ordering::Release);
                    slot.writer_token.store(0, Ordering::Release);
                    slot.refcount.store(0, Ordering::Release);
                    std::thread::yield_now();
                }
            }
        };

        slot.seq.store(0, Ordering::Release);
        slot.len.store(byte_count as u32, Ordering::Release);

        // SAFETY: writer ownership is marked by `WRITER_STATE`, so no
        // reader can acquire this slot while we zero/copy into it.
        unsafe {
            self.lease.segment.header_ptr(index).write(H::zeroed());
            std::ptr::write_bytes(
                self.lease.segment.payload_ptr(index),
                0,
                self.lease.segment.layout.payload_cap,
            );
        }

        Ok(Loan {
            segment: Arc::clone(&self.lease.segment),
            index,
            seq,
            len: byte_count,
            published: false,
            _header: PhantomData,
        })
    }

    pub(crate) fn publish(&mut self, mut loan: Loan<H>) -> Result<u64> {
        loan.commit()
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for ProducerLease<H> {
    fn drop(&mut self) {
        self.segment
            .deregister_publisher(self.slot, self.pid, self.token);
    }
}

pub(crate) struct Loan<H: Pod + Zeroable + Copy + 'static> {
    segment: Arc<Segment<H>>,
    index: usize,
    seq: u64,
    len: usize,
    published: bool,
    _header: PhantomData<H>,
}

impl<H: Pod + Zeroable + Copy + 'static> Loan<H> {
    pub(crate) fn header(&self) -> &H {
        // SAFETY: a live loan owns the slot for writing; shared access from
        // `&self` is read-only and bounded by the loan lifetime.
        unsafe { &*self.segment.header_ptr(self.index).cast_const() }
    }

    pub(crate) fn header_mut(&mut self) -> &mut H {
        // SAFETY: the loan owns this slot (`WRITER_STATE`) and `&mut self`
        // gives unique access to the header value.
        unsafe { &mut *self.segment.header_ptr(self.index) }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        // SAFETY: `len` was validated at loan time.
        unsafe { std::slice::from_raw_parts(self.segment.payload_ptr(self.index), self.len) }
    }

    pub(crate) fn payload_mut(&mut self) -> &mut [u8] {
        // SAFETY: the loan owns this slot (`WRITER_STATE`) and `len` was
        // validated at loan time.
        unsafe { std::slice::from_raw_parts_mut(self.segment.payload_ptr(self.index), self.len) }
    }

    fn commit(&mut self) -> Result<u64> {
        let slot = self.segment.slot_header(self.index);
        slot.len.store(self.len as u32, Ordering::Release);
        slot.seq.store(self.seq, Ordering::Release);
        slot.writer_pid.store(0, Ordering::Release);
        slot.writer_token.store(0, Ordering::Release);
        slot.refcount.store(0, Ordering::Release);
        self.published = true;
        Ok(self.seq)
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for Loan<H> {
    fn drop(&mut self) {
        if !self.published {
            let slot = self.segment.slot_header(self.index);
            slot.seq.store(0, Ordering::Release);
            slot.writer_pid.store(0, Ordering::Release);
            slot.writer_token.store(0, Ordering::Release);
            slot.refcount.store(0, Ordering::Release);
        }
    }
}

pub(crate) struct Consumer<H: Pod + Zeroable + Copy + 'static> {
    lease: Arc<ConsumerLease<H>>,
    read_cursor: u64,
}

struct ConsumerLease<H: Pod + Zeroable + Copy + 'static> {
    segment: Arc<Segment<H>>,
    slot: usize,
    pid: u32,
    token: u64,
}

impl<H: Pod + Zeroable + Copy + 'static> Consumer<H> {
    pub(crate) fn take(&mut self) -> Result<Option<Sample<H>>> {
        let latest = self
            .lease
            .segment
            .control()
            .write_seq
            .load(Ordering::Acquire);
        if self.read_cursor >= latest {
            return Ok(None);
        }

        let next = self.read_cursor + 1;
        let index = self.lease.segment.slot_index(next);
        let slot = self.lease.segment.slot_header(index);
        let observed = slot.seq.load(Ordering::Acquire);

        if observed == 0 {
            // `next`'s slot is empty. Two cases:
            //   (a) a writer is still legitimately filling `next` (leave it —
            //       report `None` and let the caller retry), or
            //   (b) a permanent hole: the loan for `next` was dropped before
            //       commit, or the producer died between claiming `write_seq`
            //       and committing. `write_seq` is advanced at claim time and
            //       never rolled back, so an abandoned slot keeps seq==0 while
            //       `write_seq` moves on — in-order `take()` would otherwise
            //       stall here forever.
            // Only skip the hole when a strictly newer sequence is already
            // visible (`latest > next`, i.e. the producers moved past it) AND
            // no *live* writer owns the slot. A slot is owned by a live writer
            // when its refcount is `WRITER_STATE` and the recorded writer
            // process is still alive; in that case we must wait, never skip.
            // These loads are sequenced after the `latest`/`write_seq` load, so
            // if `latest` already reflects the claim of `next`, refcount is
            // visible as `WRITER_STATE` (or as committed-0, handled below).
            let writer_live = slot.refcount.load(Ordering::Acquire) == WRITER_STATE
                && process_alive(
                    slot.writer_pid.load(Ordering::Acquire),
                    slot.writer_token.load(Ordering::Acquire),
                );
            // Re-read `seq` after inspecting the writer: `commit()` stores
            // `seq` (Release) before clearing `refcount` (Release), so a slot
            // that just committed to `next` is guaranteed visible here and is
            // not treated as a hole (avoids skipping a fresh sample).
            if latest > next && !writer_live && slot.seq.load(Ordering::Acquire) == 0 {
                self.read_cursor = next;
                return Err(Error::Lagged { dropped: 1 });
            }
            return Ok(None);
        }

        if observed != next {
            if observed > next {
                let dropped = observed - next;
                self.read_cursor = observed - 1;
                return Err(Error::Lagged { dropped });
            }
            // `write_seq` is claimed before the slot is committed. Seeing an
            // older sequence here means the next sample is still in progress;
            // do not advance the cursor or we would silently skip it.
            return Ok(None);
        }

        let bit = 1u32 << self.lease.slot;
        loop {
            let refs = slot.refcount.load(Ordering::Acquire);
            if refs == WRITER_STATE {
                return Ok(None);
            }
            let next_refs = refs | bit;
            match slot.refcount.compare_exchange_weak(
                refs,
                next_refs,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }

        let reread = slot.seq.load(Ordering::Acquire);
        if reread != next {
            slot.refcount.fetch_and(!bit, Ordering::AcqRel);
            if reread > next {
                let dropped = reread - next;
                self.read_cursor = reread - 1;
                return Err(Error::Lagged { dropped });
            }
            // Writer claimed `next` but has not committed it yet.
            return Ok(None);
        }

        let len = slot.len.load(Ordering::Acquire) as usize;
        if len > self.lease.segment.layout.payload_cap {
            slot.refcount.fetch_and(!bit, Ordering::AcqRel);
            // Advance past the poisoned slot before surfacing the error;
            // otherwise every subsequent poll re-hits this same corrupt slot
            // and the consumer can never make progress.
            self.read_cursor = next;
            return Err(Error::incompatible_shm(format!(
                "slot payload len {len} exceeds cap {}",
                self.lease.segment.layout.payload_cap
            )));
        }

        self.read_cursor = next;
        Ok(Some(Sample {
            lease: Arc::clone(&self.lease),
            index,
            seq: next,
            len,
        }))
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for ConsumerLease<H> {
    fn drop(&mut self) {
        self.segment
            .deregister_subscriber(self.slot, self.pid, self.token);
    }
}

pub(crate) struct Sample<H: Pod + Zeroable + Copy + 'static> {
    lease: Arc<ConsumerLease<H>>,
    index: usize,
    seq: u64,
    len: usize,
}

impl<H: Pod + Zeroable + Copy + 'static> Sample<H> {
    pub(crate) fn header(&self) -> &H {
        // SAFETY: the consumer acquired a refcount and verified `seq`.
        unsafe { &*self.lease.segment.header_ptr(self.index).cast_const() }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        // SAFETY: `len` was read from the slot after acquiring a refcount.
        unsafe { std::slice::from_raw_parts(self.lease.segment.payload_ptr(self.index), self.len) }
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.seq
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for Sample<H> {
    fn drop(&mut self) {
        let bit = 1u32 << self.lease.slot;
        self.lease
            .segment
            .slot_header(self.index)
            .refcount
            .fetch_and(!bit, Ordering::AcqRel);
    }
}

fn validate_config(cfg: &LocalConfig) -> Result<()> {
    if cfg.max_publishers as usize > MAX_TRACKED_PUBLISHERS {
        return Err(Error::invalid_argument(format!(
            "max_publishers {} exceeds tracked-process cap {}",
            cfg.max_publishers, MAX_TRACKED_PUBLISHERS
        )));
    }
    if cfg.max_subscribers as usize > MAX_TRACKED_SUBSCRIBERS {
        return Err(Error::invalid_argument(format!(
            "max_subscribers {} exceeds tracked-process cap {}",
            cfg.max_subscribers, MAX_TRACKED_SUBSCRIBERS
        )));
    }
    Ok(())
}

fn decrement_counter(counter: &AtomicU32) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_sub(1))
    });
}

fn current_pid() -> u32 {
    std::process::id()
}

fn current_process_token() -> u64 {
    process_start_token(current_pid()).unwrap_or(0)
}

/// Read a process' start-time token from `/proc/<pid>/stat`, distinguishing
/// "the process is gone" (`NotFound`) from "we couldn't read it right now"
/// (any other error — e.g. `EMFILE`/`ENFILE` fd exhaustion, `EACCES`, a
/// malformed line). The distinction is load-bearing: mapping a *transient*
/// read failure to "dead" would let a fully-alive process be reaped or, worse,
/// its half-initialised segment condemned (the split-brain trigger).
#[cfg(target_os = "linux")]
fn read_start_token(pid: u32) -> std::io::Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let after_comm = stat.rsplit_once(") ").ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed /proc stat")
    })?;
    // Field 22 (`starttime`) is index 19 after stripping fields 1 and 2
    // (`pid` and `comm`). See `proc_pid_stat(5)`.
    after_comm
        .1
        .split_whitespace()
        .nth(19)
        .and_then(|f| f.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing starttime field")
        })
}

#[cfg(target_os = "linux")]
fn process_start_token(pid: u32) -> Option<u64> {
    read_start_token(pid).ok()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_start_token(_pid: u32) -> Option<u64> {
    None
}

#[cfg(not(unix))]
fn process_start_token(_pid: u32) -> Option<u64> {
    None
}

#[cfg(unix)]
fn process_alive(pid: u32, token: u64) -> bool {
    // A value that cannot be a valid PID is never a live process — and must
    // NEVER reach `kill()`, whose non-positive arguments mean "signal a process
    // group / every process" (`0`, `-1`, `< -1`). A `pid` of 0 or one that
    // would be negative as `pid_t` (high bit set — e.g. corruption, or the
    // `u32::MAX` sentinel older peers stored) is therefore treated as dead.
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    if token != 0 {
        #[cfg(target_os = "linux")]
        match read_start_token(pid) {
            // Definitive: the token either matches this incarnation or a
            // different process reused the pid (mismatch => original is dead).
            Ok(observed) => return observed == token,
            // The process is genuinely gone.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
            // Inconclusive (fd exhaustion, permissions, parse): never conclude
            // "dead" from a transient failure — fall through to the robust
            // `kill(pid, 0)` probe below, which errs toward "alive".
            Err(_) => {}
        }
    }
    // Untokened, or an inconclusive token read: ask the kernel directly.
    // SAFETY: `kill(pid, 0)` does not deliver a signal; it only asks whether
    // the process exists and is visible to us.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    )
}

#[cfg(not(unix))]
fn process_alive(pid: u32, token: u64) -> bool {
    // Conservative fallback for platforms where this module has not grown a
    // native liveness probe yet: never reap another process' slots.
    pid == current_pid() && (token == 0 || token == current_process_token())
}

fn validate_name(name: &str) -> Result<()> {
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

fn os_key(name: &str) -> String {
    format!("qb_{:016x}", fnv1a64(name))
}

/// Identity `(st_dev, st_ino)` of the OS object currently *named* `key`, or
/// `None` if it does not exist or cannot be stat'd. This resolves the NAME, so
/// it is used at unlink time (to check the name still points at a given inode)
/// — NOT to capture a handle's identity (see `capture_mapped_identity`, which
/// records the inode actually mapped, independent of the name).
#[cfg(unix)]
fn stat_shm(key: &str) -> Option<(u64, u64)> {
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
fn stat_shm(_key: &str) -> Option<(u64, u64)> {
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
fn mapped_inode_identity(ptr: NonNull<u8>) -> Option<(u64, u64)> {
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
fn capture_mapped_identity(ptr: NonNull<u8>, key: &str) -> Option<(u64, u64)> {
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
fn unlink_if_matches(key: &str, dev: u64, ino: u64) {
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
fn unlink_if_matches(_key: &str, _dev: u64, _ino: u64) {}

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
fn reserve_shm_backing(key: &str, total_size: usize) -> Result<()> {
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
fn reserve_shm_backing(_key: &str, _total_size: usize) -> Result<()> {
    // No portable pre-reservation primitive here; retain lazy backing.
    Ok(())
}

fn unsafe_control(ptr: NonNull<u8>) -> &'static ControlBlock {
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
fn wait_until_initialised(
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
enum Attach<H: Pod + Zeroable + Copy + 'static> {
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
enum InitState {
    Ready,
    Reclaim,
    Retry,
}

enum PageBacking {
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
fn probe_page_backed(ptr: NonNull<u8>) -> PageBacking {
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
fn probe_page_backed(_ptr: NonNull<u8>) -> PageBacking {
    PageBacking::Unsupported
}

fn validate_control<H: Pod + Zeroable + Copy + 'static>(
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

fn validate_layout(control: &ControlBlock, expected: &Layout) -> Result<()> {
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

#[cfg(test)]
mod tests {
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
}
