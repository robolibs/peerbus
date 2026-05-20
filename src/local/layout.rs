//! Wire layout of the shared-memory segment.
//!
//! Layout:
//!
//! ```text
//! ┌────────────────────────────┐  offset 0
//! │   ControlPage  (4 KiB)     │  magic, version, ring metadata,
//! │                            │  free-list head, attached refcount,
//! │                            │  history ring of slot offsets
//! ├────────────────────────────┤  offset = CONTROL_PAGE_SIZE
//! │   Slot 0                   │  16 B header + payload
//! ├────────────────────────────┤
//! │   Slot 1                   │
//! ├────────────────────────────┤
//! │   ...                      │
//! └────────────────────────────┘
//! ```
//!
//! All layouts are `#[repr(C)]` with named atomic types so cross-
//! process attaches see the same bytes regardless of compiler.

use std::sync::atomic::{AtomicU32, AtomicU64};

/// Identifies a quicbit SHM segment. ASCII "QBP1" little-endian.
pub const MAGIC: u32 = 0x3150_4251;

/// Current segment layout version. Bump on any breaking change to
/// [`ControlPage`] or [`SlotHeader`].
///
/// History:
/// * v1 — initial.
/// * v2 — free-list head is now `AtomicU64` packing
///   `(generation << 32) | slot_idx` to defeat ABA on the
///   Treiber-stack CAS pop.
/// * v3 — per-slot `(generation << 32) | refcount` is one
///   `AtomicU64` so subscribers can CAS-bump refcount only if
///   the slot hasn't been recycled since they snapshotted the
///   generation. Removes the unsound "bump then undo" race
///   that v2 had under contention.
pub const VERSION: u32 = 3;

/// Control page is one host page; 4 KiB is universal across the
/// platforms we target (Linux x86_64, aarch64).
pub const CONTROL_PAGE_SIZE: usize = 4096;

/// Sentinel for "no slot" / "end of free list".
pub const NULL_SLOT: u32 = u32::MAX;

/// Maximum ring history. The on-disk format reserves a fixed slot
/// here so changing this requires a [`VERSION`] bump.
pub const MAX_HISTORY: usize = 64;

/// Per-slot header that precedes the payload. Sized to 32 B so that
/// for any reasonable payload alignment (<= 32 B), the payload starts
/// at a naturally aligned address relative to the slot.
pub const SLOT_HEADER_SIZE: usize = 32;

#[repr(C)]
pub struct ControlPage {
    /// [`MAGIC`].
    pub magic: u32,
    /// [`VERSION`].
    pub version: u32,
    /// Number of slots (fixed at create).
    pub slot_count: u32,
    /// Per-slot payload capacity (bytes, fixed at create).
    pub slot_size: u32,
    /// FNV-1a hash of the rust type name. Cross-process attaches
    /// reject mismatches.
    pub type_hash: u64,
    /// Ring history depth (1..=[`MAX_HISTORY`]).
    pub history_depth: u32,
    /// Padding so the next field starts on a fresh cache line.
    pub _pad0: [u32; 9],

    // ---- cache line 1: free list ----
    /// Packed `(generation << 32) | slot_idx`. The generation
    /// increments on every successful push/pop, so CAS on the
    /// composite value detects ABA: a slot that was popped, pushed
    /// back, and popped again will have a different generation
    /// even if the slot index recurs.
    pub free_list_head: AtomicU64,
    pub _pad1: [u32; 14],

    // ---- cache line 2: publish state ----
    /// Monotonically increasing publish sequence (0 = none yet).
    pub publish_seq: AtomicU64,
    /// Encoded (`(seq:u32) | (slot_idx:u32) << 32`) of the latest
    /// published entry — handy for the history=1 fast path.
    pub latest_entry: AtomicU64,
    pub _pad2: [u64; 6],

    // ---- cache line 3: lifetime ----
    /// Number of processes with this segment mapped. The process
    /// that decrements this to zero is responsible for calling
    /// `shm_unlink`.
    pub attached: AtomicU32,
    pub _pad3: [u32; 15],

    // ---- ring: history_depth slots, each 8 B ----
    /// Encoded `(seq:u32) | (slot_idx:u32) << 32`. The seq field
    /// matches the publish sequence at the time the entry was
    /// written; subscribers compare seqs to detect overruns.
    pub ring: [AtomicU64; MAX_HISTORY],
}

const _: () = assert!(std::mem::size_of::<ControlPage>() <= CONTROL_PAGE_SIZE);

#[repr(C)]
pub struct SlotHeader {
    /// Packed `(generation:u32 << 32) | refcount:u32`. Atomic
    /// CAS on this word lets a subscriber bump `refcount` only if
    /// `generation` hasn't changed — the only race-free way to
    /// claim a live slot.
    pub state: AtomicU64,
    /// Next slot in the free list, or [`NULL_SLOT`].
    pub next_free: AtomicU32,
    pub _pad: u32,
    /// Last publish sequence written to this slot. Diagnostic only.
    pub last_seq: AtomicU64,
    pub _reserved: [u8; 8],
}

const _: () = assert!(std::mem::size_of::<SlotHeader>() == SLOT_HEADER_SIZE);

/// Pack a `(seq, slot_idx)` pair into the 64-bit ring entry encoding.
#[inline]
pub fn pack_entry(seq: u32, slot_idx: u32) -> u64 {
    (seq as u64) | ((slot_idx as u64) << 32)
}

/// Unpack a ring entry into `(seq, slot_idx)`.
#[inline]
pub fn unpack_entry(entry: u64) -> (u32, u32) {
    (entry as u32, (entry >> 32) as u32)
}

/// Pack a `(generation, slot_idx)` pair into the 64-bit free-list
/// head encoding. The generation is in the high 32 bits so CAS on
/// the composite catches the ABA case.
#[inline]
pub fn pack_free_head(generation: u32, slot_idx: u32) -> u64 {
    (slot_idx as u64) | ((generation as u64) << 32)
}

/// Unpack a free-list head into `(generation, slot_idx)`.
#[inline]
pub fn unpack_free_head(head: u64) -> (u32, u32) {
    ((head >> 32) as u32, head as u32)
}

/// Pack a `(generation, refcount)` pair into the per-slot 64-bit
/// state word.
#[inline]
pub fn pack_state(generation: u32, refcount: u32) -> u64 {
    (refcount as u64) | ((generation as u64) << 32)
}

/// Unpack a slot state into `(generation, refcount)`.
#[inline]
pub fn unpack_state(state: u64) -> (u32, u32) {
    ((state >> 32) as u32, state as u32)
}

/// Total bytes required for a segment with the given parameters.
pub fn segment_size(slot_count: u32, slot_size: u32) -> usize {
    let per_slot = SLOT_HEADER_SIZE + slot_size as usize;
    CONTROL_PAGE_SIZE + per_slot * slot_count as usize
}

/// Byte offset of slot `idx` from the start of the segment.
pub fn slot_offset(idx: u32, slot_size: u32) -> usize {
    CONTROL_PAGE_SIZE + (SLOT_HEADER_SIZE + slot_size as usize) * idx as usize
}

/// FNV-1a 64-bit hash of a string. Used for the segment's type hash
/// so attachers can fail fast on a payload-type mismatch.
pub fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}
