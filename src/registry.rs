//! Same-host process registry.
//!
//! A fixed-size POSIX SHM segment, `/quicbit-registry.v1`, that
//! every quicbit `Node` on the host writes itself into on bind.
//! Lookup answers "is this `EndpointId` reachable via SHM right
//! now?" — if yes, the `Node` shortcuts to the local SHM
//! transport; if no, the `Node` dials over iroh.
//!
//! Layout:
//!
//! ```text
//! ┌────────────────────────────────────────────────────┐
//! │ RegistryHeader  (magic, version, capacity)         │
//! ├────────────────────────────────────────────────────┤
//! │ RegistryEntry  ×  REGISTRY_CAPACITY (= 256)        │
//! └────────────────────────────────────────────────────┘
//! ```
//!
//! Each [`RegistryEntry`] has a *claim word* — an `AtomicU64`
//! that's `0` when the slot is empty and holds the entry's `pid`
//! (low 32 bits) plus a generation counter (high 32 bits) when
//! claimed. CAS on the claim word is the only synchronisation we
//! need: the `endpoint_id` and `heartbeat_nanos` fields are
//! published before the slot is "advertised" via the claim word's
//! non-zero write, and stale-entry GC re-CASes the claim word back
//! to 0 before any other process can write to the slot.
//!
//! Stale entries are GC'd lazily on lookup: if `heartbeat_nanos`
//! is older than [`STALE_AFTER`] and the PID is no longer alive
//! (`kill(pid, 0)` returns `ESRCH`), the slot is cleared.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::local::layout::CONTROL_PAGE_SIZE;
use crate::local::shm::ShmMapping;

// ---- constants ----

/// Process-wide name for the registry SHM segment. Versioned so a
/// future incompatible layout change can coexist with v1 on the
/// same host.
pub const REGISTRY_NAME: &str = "registry.v1";

/// Number of slots in the registry. Each quicbit process consumes
/// one. 256 is plenty for any realistic robot.
pub const REGISTRY_CAPACITY: usize = 256;

/// ASCII "QBR1" little-endian — registry segment magic.
const REGISTRY_MAGIC: u32 = 0x3152_5851;

/// Registry layout version.
const REGISTRY_VERSION: u32 = 1;

/// An entry's heartbeat is considered stale after this duration.
/// Stale entries are GC'd only if the PID is also dead.
pub const STALE_AFTER: Duration = Duration::from_secs(10);

/// Header at the start of the registry segment. Cache-line padded
/// so claim-word atomics don't share lines with the header.
#[repr(C)]
pub struct RegistryHeader {
    /// [`REGISTRY_MAGIC`].
    pub magic: u32,
    /// [`REGISTRY_VERSION`].
    pub version: u32,
    /// [`REGISTRY_CAPACITY`].
    pub capacity: u32,
    pub _pad: [u32; 13],
}

/// One slot in the registry.
///
/// `endpoint_id` and `heartbeat_nanos` are written before
/// `claim_word` is CAS'd from 0 to non-zero; therefore a non-zero
/// claim word published with `Release` semantics implies the rest
/// of the entry's fields are observable. Readers do an `Acquire`
/// load of `claim_word`, then read the other fields plain (the
/// claim word's acquire fence covers them).
#[repr(C)]
pub struct RegistryEntry {
    /// Packed `(generation:u32 << 32) | pid:u32`. `0` means empty.
    /// We CAS this to claim or release a slot.
    pub claim_word: AtomicU64,
    /// Raw 32 bytes of the ed25519 public key.
    pub endpoint_id: [u8; 32],
    /// Unix-epoch nanos at last refresh; updated by the owner.
    pub heartbeat_nanos: AtomicU64,
    /// Padding to a round size.
    pub _pad: [u8; 16],
}

const _: () = assert!(std::mem::size_of::<RegistryEntry>() == 64);

// ---- helpers ----

#[inline]
fn pack_claim(generation: u32, pid: u32) -> u64 {
    ((generation as u64) << 32) | (pid as u64)
}

#[inline]
fn unpack_claim(c: u64) -> (u32, u32) {
    ((c >> 32) as u32, c as u32)
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Returns true if the given PID currently exists on this host.
/// Uses `kill(pid, 0)`: a return of `0` means the PID is valid;
/// `ESRCH` means dead. Any other error (e.g. `EPERM`) is treated
/// as "still alive, just not our process" to be conservative —
/// we don't want to accidentally evict another user's slot.
pub fn pid_alive(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    let err = std::io::Error::last_os_error();
    err.raw_os_error() != Some(libc::ESRCH)
}

// ---- segment sizing ----

fn registry_segment_size() -> usize {
    CONTROL_PAGE_SIZE + std::mem::size_of::<RegistryEntry>() * REGISTRY_CAPACITY
}

// ---- public handle ----

/// Owned handle to the host-wide registry. One per `Node`.
///
/// On `Registry::open`, the process either creates the segment
/// (first quicbit user on the host) or attaches to the existing
/// one. On drop, the owner's slot is released.
pub struct Registry {
    mapping: ShmMapping,
    /// Index of the slot this `Registry` owns. `None` if we
    /// haven't claimed one yet.
    owned_slot: Option<u32>,
    /// Our process id, cached.
    pid: u32,
}

unsafe impl Send for Registry {}
unsafe impl Sync for Registry {}

impl Registry {
    /// Create or attach to the registry segment.
    pub fn open() -> Result<Self> {
        let size = registry_segment_size();
        // Try create first; on collision, attach.
        let mapping = match ShmMapping::create(REGISTRY_NAME, size) {
            Ok(m) => {
                // Initialize header.
                unsafe {
                    let hdr = &mut *(m.as_ptr() as *mut RegistryHeader);
                    hdr.magic = REGISTRY_MAGIC;
                    hdr.version = REGISTRY_VERSION;
                    hdr.capacity = REGISTRY_CAPACITY as u32;
                    // Entries are already zeroed by `ftruncate`.
                }
                m.release_ownership();
                m
            }
            Err(Error::ServiceAlreadyExists(_)) => ShmMapping::open(REGISTRY_NAME, size)?,
            Err(e) => return Err(e),
        };
        // Validate header on attach.
        unsafe {
            let hdr = &*(mapping.as_ptr() as *const RegistryHeader);
            if hdr.magic != REGISTRY_MAGIC {
                return Err(Error::incompatible_shm(format!(
                    "registry: bad magic 0x{:x}",
                    hdr.magic
                )));
            }
            if hdr.version != REGISTRY_VERSION {
                return Err(Error::incompatible_shm(format!(
                    "registry: version {}",
                    hdr.version
                )));
            }
            if hdr.capacity as usize != REGISTRY_CAPACITY {
                return Err(Error::incompatible_shm(format!(
                    "registry: capacity {}",
                    hdr.capacity
                )));
            }
        }

        Ok(Self {
            mapping,
            owned_slot: None,
            pid: std::process::id(),
        })
    }

    /// Insert this process's `endpoint_id` into the registry.
    /// Idempotent: if we already own a slot, refresh the heartbeat
    /// and return `Ok(())`.
    pub fn claim(&mut self, endpoint_id: [u8; 32]) -> Result<()> {
        if let Some(idx) = self.owned_slot {
            self.touch_heartbeat(idx);
            return Ok(());
        }

        // Find an empty slot (claim_word == 0) and CAS it to ours.
        for idx in 0..(REGISTRY_CAPACITY as u32) {
            let entry = self.entry(idx);
            let cur = entry.claim_word.load(Ordering::Acquire);
            if cur != 0 {
                // Already claimed; consider GC if stale.
                if let Some(after_gc) = self.gc_if_stale(idx, cur) {
                    if after_gc != 0 {
                        continue; // someone re-claimed it; skip
                    }
                    // Slot was GC'd, fall through to try claiming.
                } else {
                    continue;
                }
            }
            // Slot looks empty. Write our fields BEFORE publishing
            // the claim word so any reader doing an Acquire load
            // sees our endpoint_id + heartbeat.
            //
            // SAFETY: we have exclusive access to this slot via the
            // pending CAS — if another thread races us, our CAS
            // will fail and we retry. Reading the unclaimed fields
            // is fine because they're plain bytes.
            unsafe {
                let ptr = entry as *const _ as *mut RegistryEntry;
                (*ptr).endpoint_id = endpoint_id;
            }
            entry.heartbeat_nanos.store(now_nanos(), Ordering::Release);
            let new_claim = pack_claim(0, self.pid);
            if entry
                .claim_word
                .compare_exchange(0, new_claim, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.owned_slot = Some(idx);
                return Ok(());
            }
            // CAS failed: someone else got there first. Try next.
        }
        Err(Error::Other(format!(
            "registry full: {REGISTRY_CAPACITY} slots in use"
        )))
    }

    /// Find the slot for `endpoint_id`. Returns `Some((idx, pid))`
    /// if a live slot exists for that key. Stale entries are GC'd
    /// during the scan.
    pub fn lookup(&self, endpoint_id: &[u8; 32]) -> Option<(u32, u32)> {
        for idx in 0..(REGISTRY_CAPACITY as u32) {
            let entry = self.entry(idx);
            let cur = entry.claim_word.load(Ordering::Acquire);
            if cur == 0 {
                continue;
            }
            // SAFETY: claim word non-zero with Acquire ordering;
            // the writer published these fields before the claim.
            let id = unsafe { (*(entry as *const RegistryEntry)).endpoint_id };
            if &id != endpoint_id {
                continue;
            }
            // Match. Check liveness; GC if dead+stale.
            if let Some(0) = self.gc_if_stale(idx, cur) {
                return None; // we GC'd it
            }
            let (_generation, pid) = unpack_claim(cur);
            return Some((idx, pid));
        }
        None
    }

    /// Refresh our own heartbeat. Call periodically (e.g. once a
    /// few seconds) so stale-entry GC doesn't reap us.
    pub fn heartbeat(&self) {
        if let Some(idx) = self.owned_slot {
            self.touch_heartbeat(idx);
        }
    }

    /// Read-only access to the i'th entry.
    #[inline]
    fn entry(&self, idx: u32) -> &RegistryEntry {
        unsafe {
            let base = self.mapping.as_ptr().add(CONTROL_PAGE_SIZE);
            &*(base as *const RegistryEntry).add(idx as usize)
        }
    }

    fn touch_heartbeat(&self, idx: u32) {
        let entry = self.entry(idx);
        entry
            .heartbeat_nanos
            .store(now_nanos(), Ordering::Release);
    }

    /// If the entry at `idx` is stale (heartbeat too old AND the
    /// pid is dead), CAS-clear its claim word and return `Some(0)`
    /// (slot is now free). If the slot is still alive, return
    /// `Some(cur)` (current claim word). If the slot was already
    /// empty (or got changed mid-check), return `None`.
    fn gc_if_stale(&self, idx: u32, cur: u64) -> Option<u64> {
        let entry = self.entry(idx);
        let now = now_nanos();
        let hb = entry.heartbeat_nanos.load(Ordering::Acquire);
        let age_ns = now.saturating_sub(hb);
        if age_ns < STALE_AFTER.as_nanos() as u64 {
            return Some(cur);
        }
        let (_generation, pid) = unpack_claim(cur);
        if pid_alive(pid) {
            return Some(cur);
        }
        // Stale + dead. CAS-clear; if someone else GC'd or refreshed,
        // they win.
        match entry
            .claim_word
            .compare_exchange(cur, 0, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Some(0),
            Err(_) => None,
        }
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        // Release our own slot by CAS-clearing the claim word.
        // We do NOT zero the endpoint_id field: a later writer
        // will overwrite it before publishing a new claim.
        if let Some(idx) = self.owned_slot.take() {
            let entry = self.entry(idx);
            let expect = pack_claim(0, self.pid);
            // Read the live generation bits — they may have been
            // set non-zero by future code; we still CAS-clear
            // strictly by-pid for safety.
            let cur = entry.claim_word.load(Ordering::Acquire);
            let (generation, pid) = unpack_claim(cur);
            if pid == self.pid {
                let _ = entry.claim_word.compare_exchange(
                    pack_claim(generation, pid),
                    0,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            let _ = expect; // silence unused warning in non-debug
        }
        // We do NOT shm_unlink the registry segment on drop: other
        // processes may still need it. Last process out by host
        // shutdown / reboot is fine.
    }
}

