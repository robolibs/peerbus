//! Typed view onto a mapped SHM segment.
//!
//! Glues [`super::shm::ShmMapping`] to the on-disk layout in
//! [`super::layout`] and exposes the operations the rest of the
//! crate uses: pop/push the free list, write/read the publish ring,
//! and resolve slot indices to payload pointers.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::Error;
use crate::local::layout::{
    fnv1a64, pack_entry, pack_free_head, pack_state, segment_size, slot_offset, unpack_entry,
    unpack_free_head, unpack_state, ControlPage, SlotHeader, CONTROL_PAGE_SIZE, MAGIC, MAX_HISTORY,
    NULL_SLOT, VERSION,
};
use crate::local::shm::ShmMapping;

/// Builder-style parameters for creating a service segment.
#[derive(Debug, Clone)]
pub struct SegmentParams {
    pub slot_count: u32,
    pub slot_size: u32,
    pub history_depth: u32,
    pub type_name: &'static str,
}

impl SegmentParams {
    pub fn type_hash(&self) -> u64 {
        fnv1a64(self.type_name)
    }
}

/// Mapped segment + decoded layout. Cloning is cheap (just bumps
/// an `Arc`); when the last clone drops, the mapping is torn down
/// and the segment is unlinked if this process held the last attach.
#[derive(Clone)]
pub struct Segment {
    inner: Arc<SegmentInner>,
}

struct SegmentInner {
    mapping: ShmMapping,
    slot_size: u32,
    slot_count: u32,
    history_depth: u32,
    name: String,
    /// PID of the process that constructed this `SegmentInner`. If
    /// the segment is inherited across `fork()`, the child sees the
    /// same struct but a different live PID — we use this on drop
    /// to skip the per-segment refcount decrement (the inherited
    /// mapping was never `attach()`ed, so it never incremented).
    owner_pid: u32,
}

impl Segment {
    /// Create a new segment for `name` with the given parameters.
    /// Caller must guarantee uniqueness — collisions return
    /// [`Error::ServiceAlreadyExists`].
    pub fn create(name: &str, params: SegmentParams) -> Result<Self, Error> {
        if params.slot_count == 0 {
            return Err(Error::invalid_argument("slot_count must be >= 1"));
        }
        if params.slot_size == 0 {
            return Err(Error::invalid_argument("slot_size must be >= 1"));
        }
        if params.history_depth == 0 || params.history_depth as usize > MAX_HISTORY {
            return Err(Error::invalid_argument(format!(
                "history_depth must be in 1..={}",
                MAX_HISTORY
            )));
        }
        let params = round_slot_size(params);
        let size = segment_size(params.slot_count, params.slot_size);
        let mapping = ShmMapping::create(name, size)?;
        unsafe { init_segment_in_place(&mapping, &params) };

        // After init, hand off ownership to the per-segment attached
        // refcount: only the last detacher will unlink.
        mapping.release_ownership();

        Ok(Self {
            inner: Arc::new(SegmentInner {
                mapping,
                slot_size: params.slot_size,
                slot_count: params.slot_count,
                history_depth: params.history_depth,
                name: name.to_string(),
                owner_pid: std::process::id(),
            }),
        })
    }

    /// Attach to an existing segment, validating magic / version /
    /// type hash. The expected type hash is the FNV-1a of the same
    /// `type_name` the creator passed.
    pub fn attach(name: &str, expected_type_hash: u64) -> Result<Self, Error> {
        // Read just the control page to discover the real layout.
        let header_only = ShmMapping::open(name, CONTROL_PAGE_SIZE)?;
        let (slot_count, slot_size, history_depth) = unsafe {
            let ctrl = &*(header_only.as_ptr() as *const ControlPage);
            if ctrl.magic != MAGIC {
                return Err(Error::incompatible_shm(format!(
                    "bad magic: 0x{:x}",
                    ctrl.magic
                )));
            }
            if ctrl.version != VERSION {
                return Err(Error::incompatible_shm(format!(
                    "version mismatch: segment={} crate={}",
                    ctrl.version, VERSION
                )));
            }
            if ctrl.type_hash != expected_type_hash {
                return Err(Error::TypeMismatch {
                    expected: "<segment payload>",
                    got: format!("hash=0x{:x}", ctrl.type_hash),
                });
            }
            (ctrl.slot_count, ctrl.slot_size, ctrl.history_depth)
        };
        drop(header_only); // unmap the small mapping, then re-open at full size

        let full_size = segment_size(slot_count, slot_size);
        let mapping = ShmMapping::open(name, full_size)?;
        mapping.release_ownership();

        // SAFETY: segment is initialized and validated above; the
        // attached counter is an atomic integer.
        unsafe {
            let ctrl = &*(mapping.as_ptr() as *const ControlPage);
            ctrl.attached.fetch_add(1, Ordering::AcqRel);
        }

        Ok(Self {
            inner: Arc::new(SegmentInner {
                mapping,
                slot_size,
                slot_count,
                history_depth,
                name: name.to_string(),
                owner_pid: std::process::id(),
            }),
        })
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn slot_size(&self) -> u32 {
        self.inner.slot_size
    }

    pub fn slot_count(&self) -> u32 {
        self.inner.slot_count
    }

    pub fn history_depth(&self) -> u32 {
        self.inner.history_depth
    }

    #[inline]
    fn base(&self) -> *mut u8 {
        self.inner.mapping.as_ptr()
    }

    #[inline]
    pub(crate) fn control(&self) -> &ControlPage {
        unsafe { &*(self.base() as *const ControlPage) }
    }

    #[inline]
    pub(crate) fn slot_header(&self, idx: u32) -> &SlotHeader {
        unsafe { &*slot_header_ptr(self.base(), idx, self.inner.slot_size) }
    }

    /// Raw pointer to the payload region of slot `idx`. Length is
    /// [`Segment::slot_size`] bytes.
    #[inline]
    pub fn slot_payload(&self, idx: u32) -> *mut u8 {
        unsafe {
            self.base()
                .add(slot_offset(idx, self.inner.slot_size) + std::mem::size_of::<SlotHeader>())
        }
    }

    /// Pop one slot off the free list. Returns `None` if the pool is
    /// exhausted. Multi-producer safe.
    ///
    /// The composite `(generation, slot_idx)` head defeats ABA: if
    /// another thread popped, pushed, and popped a slot in between
    /// our `load` and `compare_exchange`, the generation will have
    /// changed and the CAS fails — we retry.
    ///
    /// On success, the slot's own per-slot generation is bumped so
    /// any subscriber holding a stale ring entry that pointed at
    /// this slot will notice on its CAS-bump verify.
    pub fn pop_free(&self) -> Option<u32> {
        let ctrl = self.control();
        loop {
            let head_raw = ctrl.free_list_head.load(Ordering::Acquire);
            let (head_gen, head_idx) = unpack_free_head(head_raw);
            if head_idx == NULL_SLOT {
                return None;
            }
            let next_idx = self
                .slot_header(head_idx)
                .next_free
                .load(Ordering::Acquire);
            let new_head = pack_free_head(head_gen.wrapping_add(1), next_idx);
            if ctrl
                .free_list_head
                .compare_exchange(head_raw, new_head, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Bump the slot's per-slot generation, atomically
                // with refcount (which must be 0 since the slot
                // was on the free list).
                let header = self.slot_header(head_idx);
                let mut cur = header.state.load(Ordering::Acquire);
                loop {
                    let (cur_gen, cur_rc) = unpack_state(cur);
                    // Refcount should be 0 here; if it isn't, some
                    // earlier balance got off — preserve it anyway
                    // so we don't paper over the inconsistency
                    // (debug assert is harmless).
                    debug_assert_eq!(cur_rc, 0, "popped slot with nonzero refcount");
                    let new_state = pack_state(cur_gen.wrapping_add(1), cur_rc);
                    match header.state.compare_exchange_weak(
                        cur,
                        new_state,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(observed) => cur = observed,
                    }
                }
                return Some(head_idx);
            }
        }
    }

    /// Return a slot to the free list. Caller must guarantee the
    /// slot's refcount is zero.
    pub fn push_free(&self, idx: u32) {
        let ctrl = self.control();
        let header = self.slot_header(idx);
        loop {
            let head_raw = ctrl.free_list_head.load(Ordering::Acquire);
            let (generation, head_idx) = unpack_free_head(head_raw);
            // Link the new node to the existing head.
            header.next_free.store(head_idx, Ordering::Release);
            let new_head = pack_free_head(generation.wrapping_add(1), idx);
            if ctrl
                .free_list_head
                .compare_exchange(head_raw, new_head, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Publish a slot. Allocates a fresh sequence number, writes the
    /// ring entry, and decrements any displaced slot's refcount.
    /// Returns the assigned sequence.
    pub fn publish_slot(&self, idx: u32) -> u64 {
        let ctrl = self.control();
        let header = self.slot_header(idx);
        // Ring presence is one logical refcount holder; bump
        // atomically without touching the generation half.
        bump_refcount(header);

        // Allocate seq. We use a CAS on publish_seq so that
        // single-publisher remains fast; under multi-publisher this
        // serializes the ring writes (still correct).
        let seq = ctrl.publish_seq.fetch_add(1, Ordering::AcqRel) + 1;
        header.last_seq.store(seq, Ordering::Release);

        let entry = pack_entry(seq as u32, idx);
        let ring_pos = (seq as usize - 1) % self.inner.history_depth as usize;
        let old = ctrl.ring[ring_pos].swap(entry, Ordering::AcqRel);

        // Update the latest-entry fast path.
        ctrl.latest_entry.store(entry, Ordering::Release);

        // Evict the displaced entry, if any.
        //
        // Special case: if the displaced entry was THIS SAME slot
        // at an earlier sequence, the bump we just did and the
        // dec we'd do here net to zero, but the slot is still in
        // the ring (we just rewrote it at a newer seq). Skipping
        // the dec is the correct accounting.
        if old != 0 {
            let (_old_seq, old_idx) = unpack_entry(old);
            if old_idx != idx {
                let old_header = self.slot_header(old_idx);
                if dec_refcount(old_header) == 1 {
                    self.push_free(old_idx);
                }
            }
        }
        seq
    }

    /// Read the latest published sequence (0 = none yet).
    pub fn latest_seq(&self) -> u64 {
        self.control().publish_seq.load(Ordering::Acquire)
    }

    /// Try to acquire a sample at `wanted_seq`. Returns:
    /// * `Ok(Some((slot_idx, observed_seq)))` on success — caller now
    ///   holds one refcount on the slot.
    /// * `Ok(None)` if `wanted_seq` is in the future (no message
    ///   published there yet).
    /// * `Err(Lagged { dropped })` if `wanted_seq` is already too far
    ///   behind the writer's window.
    pub fn try_acquire(&self, wanted_seq: u64) -> Result<Option<(u32, u64)>, Error> {
        let ctrl = self.control();
        let depth = self.inner.history_depth as u64;

        let global = ctrl.publish_seq.load(Ordering::Acquire);
        if wanted_seq > global {
            return Ok(None);
        }
        if global > wanted_seq && global - wanted_seq >= depth {
            return Err(Error::Lagged {
                dropped: global - wanted_seq + 1 - depth,
            });
        }

        let ring_pos = (wanted_seq as usize - 1) % self.inner.history_depth as usize;
        let entry = ctrl.ring[ring_pos].load(Ordering::Acquire);
        if entry == 0 {
            return Ok(None);
        }
        let (entry_seq, slot_idx) = unpack_entry(entry);
        if entry_seq as u64 != wanted_seq {
            // The writer has wrapped past us between the publish_seq
            // load and the ring read. Treat as a lag.
            return Err(Error::Lagged { dropped: 1 });
        }

        let header = self.slot_header(slot_idx);
        let snapshot = header.state.load(Ordering::Acquire);
        let (gen_before, _rc_before) = unpack_state(snapshot);

        // Single CAS bump: succeed only if both
        //   (a) generation is still `gen_before` (slot is the same
        //       one we observed in the ring entry), AND
        //   (b) refcount > 0 (slot is genuinely live, not on its
        //       way back to the free list).
        //
        // Atomic on both halves means we never bump the wrong
        // slot's lifecycle, so we never need a fragile "undo" path.
        let mut cur = snapshot;
        loop {
            let (cur_gen, cur_rc) = unpack_state(cur);
            if cur_gen != gen_before || cur_rc == 0 {
                return Err(Error::Lagged { dropped: 1 });
            }
            let new_state = pack_state(cur_gen, cur_rc + 1);
            match header.state.compare_exchange_weak(
                cur,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Some((slot_idx, wanted_seq))),
                Err(observed) => cur = observed,
            }
        }
    }

    /// Drop a subscriber-held reference. Returns the slot to the
    /// free list when the count reaches zero.
    pub fn release(&self, idx: u32) {
        let header = self.slot_header(idx);
        if dec_refcount(header) == 1 {
            self.push_free(idx);
        }
    }
}

/// Atomically increment the refcount half of `header.state`,
/// leaving the generation untouched.
fn bump_refcount(header: &SlotHeader) {
    let mut cur = header.state.load(Ordering::Acquire);
    loop {
        let (g, rc) = unpack_state(cur);
        let new = pack_state(g, rc.wrapping_add(1));
        match header.state.compare_exchange_weak(
            cur,
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(observed) => cur = observed,
        }
    }
}

/// Atomically decrement the refcount half of `header.state` and
/// return the value BEFORE the decrement (i.e., 1 if we just hit 0).
fn dec_refcount(header: &SlotHeader) -> u32 {
    let mut cur = header.state.load(Ordering::Acquire);
    loop {
        let (g, rc) = unpack_state(cur);
        debug_assert!(rc > 0, "decrement of zero refcount");
        let new = pack_state(g, rc - 1);
        match header.state.compare_exchange_weak(
            cur,
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return rc,
            Err(observed) => cur = observed,
        }
    }
}

impl Drop for SegmentInner {
    fn drop(&mut self) {
        // Skip the per-segment refcount if we're a fork-inherited
        // copy: the child never called `attach`, so it must not
        // `detach` either. The kernel will unmap the inherited
        // pages when the child exits.
        if self.owner_pid != std::process::id() {
            return;
        }
        let ctrl = unsafe { &*(self.mapping.as_ptr() as *const ControlPage) };
        let prev = ctrl.attached.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.mapping.claim_ownership();
        }
    }
}

unsafe fn slot_header_ptr(base: *mut u8, idx: u32, slot_size: u32) -> *mut SlotHeader {
    unsafe { base.add(slot_offset(idx, slot_size)) as *mut SlotHeader }
}

unsafe fn slot_header_mut<'a>(base: *mut u8, idx: u32, slot_size: u32) -> &'a mut SlotHeader {
    unsafe { &mut *slot_header_ptr(base, idx, slot_size) }
}

/// Round `slot_size` up to 8 bytes so each slot's [`SlotHeader`]
/// (which contains `AtomicU64` fields) starts at an 8-byte aligned
/// offset. Shared between `Segment::create` and the test-only heap
/// constructor.
pub(crate) fn round_slot_size(params: SegmentParams) -> SegmentParams {
    SegmentParams {
        slot_size: (params.slot_size + 7) & !7,
        ..params
    }
}

/// Initialize the control page + free list of a freshly-allocated
/// (zero-filled) [`ShmMapping`]. Shared between `Segment::create`
/// and the test-only heap constructor.
///
/// # Safety
///
/// `mapping.as_ptr()` must point to at least
/// `segment_size(params.slot_count, params.slot_size)` writable
/// bytes, freshly zeroed.
pub(crate) unsafe fn init_segment_in_place(mapping: &ShmMapping, params: &SegmentParams) {
    unsafe {
        let ctrl = &mut *(mapping.as_ptr() as *mut ControlPage);
        ctrl.magic = MAGIC;
        ctrl.version = VERSION;
        ctrl.slot_count = params.slot_count;
        ctrl.slot_size = params.slot_size;
        ctrl.type_hash = params.type_hash();
        ctrl.history_depth = params.history_depth;
        ctrl.publish_seq.store(0, Ordering::Release);
        ctrl.latest_entry.store(0, Ordering::Release);
        ctrl.attached.store(1, Ordering::Release);

        // Build the free list as a singly linked list 0 → 1 → ... → N-1.
        for i in 0..params.slot_count {
            let header = slot_header_mut(mapping.as_ptr(), i, params.slot_size);
            // Initial state: generation=0, refcount=0.
            header.state.store(pack_state(0, 0), Ordering::Release);
            let next = if i + 1 == params.slot_count {
                NULL_SLOT
            } else {
                i + 1
            };
            header.next_free.store(next, Ordering::Release);
            header.last_seq.store(0, Ordering::Release);
        }
        // Initial head: generation=0, slot_idx=0. Subsequent
        // pushes / pops bump the generation.
        ctrl.free_list_head
            .store(pack_free_head(0, 0), Ordering::Release);
    }
}

impl Segment {
    /// Test-only heap-backed segment. Bypasses `shm_open` so miri
    /// (and tests on platforms without POSIX SHM) can exercise the
    /// allocator's atomics + pointer arithmetic.
    #[doc(hidden)]
    pub fn test_from_heap(params: SegmentParams) -> Self {
        let params = round_slot_size(params);
        let size = segment_size(params.slot_count, params.slot_size);
        let mapping = ShmMapping::test_heap(params.type_name, size);
        unsafe { init_segment_in_place(&mapping, &params) };
        Self {
            inner: Arc::new(SegmentInner {
                mapping,
                slot_size: params.slot_size,
                slot_count: params.slot_count,
                history_depth: params.history_depth,
                name: format!("test:{}", params.type_name),
                owner_pid: std::process::id(),
            }),
        }
    }
}

// --- miri-friendly unit tests ---
//
// These exercise the slot allocator and publish ring on a heap-backed
// segment so `cargo miri test --lib local::segment::tests` can run
// without needing `shm_open`.

#[cfg(test)]
mod tests {
    use super::*;

    fn make_segment(slot_count: u32, slot_size: u32, history_depth: u32) -> Segment {
        Segment::test_from_heap(SegmentParams {
            slot_count,
            slot_size,
            history_depth,
            type_name: "miri_test",
        })
    }

    #[test]
    fn pop_free_returns_each_slot_exactly_once() {
        let seg = make_segment(4, 16, 1);
        let mut seen = [false; 4];
        for _ in 0..4 {
            let idx = seg.pop_free().expect("should have a free slot");
            assert!((idx as usize) < 4, "out-of-range slot index");
            assert!(!seen[idx as usize], "slot {idx} popped twice");
            seen[idx as usize] = true;
        }
        assert!(seg.pop_free().is_none(), "pool should be exhausted");
    }

    #[test]
    fn push_then_pop_round_trips() {
        let seg = make_segment(4, 16, 1);
        // Drain.
        let mut popped = Vec::new();
        while let Some(idx) = seg.pop_free() {
            popped.push(idx);
        }
        // Push them back.
        for &idx in &popped {
            seg.push_free(idx);
        }
        // Pop again — should be the same population, order may differ.
        let mut second = Vec::new();
        while let Some(idx) = seg.pop_free() {
            second.push(idx);
        }
        popped.sort();
        second.sort();
        assert_eq!(popped, second);
    }

    #[test]
    fn publish_then_acquire_returns_same_slot() {
        let seg = make_segment(4, 16, 1);
        let idx = seg.pop_free().unwrap();
        let seq = seg.publish_slot(idx);
        assert_eq!(seq, 1);
        let (acquired_idx, acquired_seq) = seg
            .try_acquire(1)
            .expect("try_acquire")
            .expect("a sample at seq=1");
        assert_eq!(acquired_idx, idx);
        assert_eq!(acquired_seq, 1);
        seg.release(acquired_idx);
    }

    #[test]
    fn try_acquire_future_seq_is_none() {
        let seg = make_segment(4, 16, 1);
        let r = seg.try_acquire(7).unwrap();
        assert!(r.is_none(), "future seq should be None, got {r:?}");
    }

    #[test]
    fn try_acquire_too_old_seq_is_lagged() {
        let seg = make_segment(4, 16, 1);
        let a = seg.pop_free().unwrap();
        seg.publish_slot(a);
        let b = seg.pop_free().unwrap();
        seg.publish_slot(b);
        // history_depth=1 → seq=1 is now too old.
        match seg.try_acquire(1) {
            Err(Error::Lagged { .. }) => {}
            other => panic!("expected Lagged, got {other:?}"),
        }
    }

    #[test]
    fn published_slot_returns_to_free_after_eviction_and_release() {
        let seg = make_segment(2, 16, 1);
        // Publish slot A → ring holds 1, refcount(A)=1.
        let a = seg.pop_free().unwrap();
        seg.publish_slot(a);
        // Publish slot B → evicts A; refcount(A) → 0 → free list.
        let b = seg.pop_free().unwrap();
        seg.publish_slot(b);
        // Now we should be able to pop again — A came back.
        let recycled = seg.pop_free().expect("A should have been recycled");
        // It's whichever index was first to evict; with slot_count=2,
        // only A is back on the free list at this point.
        assert!(
            recycled == a || recycled == b,
            "recycled slot {recycled} should match a={a} or b={b}"
        );
    }

    #[test]
    fn sequence_numbers_are_monotonic() {
        // history_depth=1 so each publish evicts the previous slot
        // back to the free list (after our acquire/release).
        let seg = make_segment(4, 16, 1);
        let mut last = 0;
        for _ in 0..10 {
            let idx = seg.pop_free().expect("free slot");
            let seq = seg.publish_slot(idx);
            assert!(seq > last, "seq must be monotonic: {seq} <= {last}");
            last = seq;
            if let Some((s, _)) = seg.try_acquire(seq).unwrap() {
                seg.release(s);
            }
        }
    }

    #[test]
    fn pop_then_publish_recycles_via_eviction() {
        // slot_count=2, history=1: each new publish evicts the
        // previous from the ring. Verify the evicted slot returns
        // to the free list when the subscriber has already released.
        let seg = make_segment(2, 16, 1);
        for _ in 0..5 {
            let a = seg.pop_free().expect("free slot a");
            seg.publish_slot(a);
            // Subscriber acquires + releases immediately.
            if let Some((s, _)) = seg.try_acquire(seg.latest_seq()).unwrap() {
                seg.release(s);
            }
            let b = seg.pop_free().expect("free slot b");
            seg.publish_slot(b);
            if let Some((s, _)) = seg.try_acquire(seg.latest_seq()).unwrap() {
                seg.release(s);
            }
        }
    }
}
