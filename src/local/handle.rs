//! RAII handles for borrowed SHM slots.
//!
//! [`Loan<T>`] is the publisher's writable view of a fresh slot:
//! `DerefMut<Target = T>` writes go straight into shared memory. If
//! the loan is dropped without
//! [`LocalPublisher::publish`](super::service::LocalPublisher::publish),
//! the slot is returned to the free list.
//!
//! [`Sample<T>`] is the subscriber's read-only view of a published
//! slot: `Deref<Target = T>` reads borrow the SHM bytes. Drop
//! decrements the refcount; the last drop returns the slot to the
//! free list.

use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use bytemuck::Pod;

use crate::local::segment::Segment;

/// Writable handle to a freshly loaned slot. The publisher writes
/// directly into SHM via `DerefMut`. On drop without
/// [`LocalPublisher::publish`](super::service::LocalPublisher::publish),
/// the slot returns to the free list.
///
/// `Loan` owns a cheap `Segment` clone so it does not borrow the
/// publisher's `&mut self`.
pub struct Loan<T: Pod> {
    segment: Segment,
    slot_idx: u32,
    /// True once the publisher has transferred ownership to the ring.
    /// Suppresses the rollback in `Drop` but still lets the
    /// `segment` field drop normally (so the `Arc<SegmentInner>`
    /// refcount decrements as expected).
    published: bool,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Pod> Loan<T> {
    pub(crate) fn new(segment: Segment, slot_idx: u32) -> Self {
        Self {
            segment,
            slot_idx,
            published: false,
            _phantom: PhantomData,
        }
    }

    /// Mark as published, transferring slot ownership to the ring +
    /// subscribers. The returned `slot_idx` is the slot the caller
    /// hands to `Segment::publish_slot`.
    ///
    /// After this returns, `self` drops normally — the slot rollback
    /// is suppressed via the `published` flag, while the `segment`
    /// field continues to decrement its `Arc` properly.
    pub(crate) fn mark_published(mut self) -> u32 {
        self.published = true;
        self.slot_idx
    }

    fn payload_ptr(&self) -> *mut T {
        self.segment.slot_payload(self.slot_idx) as *mut T
    }
}

impl<T: Pod> Deref for Loan<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: the slot's payload region is at least
        // `size_of::<T>()` bytes (enforced at loan-time), aligned to
        // an 8-byte boundary (segment rounds slot_size up), which is
        // sufficient for the alignments of any `Pod` type we accept
        // here, and is exclusively owned by this Loan handle until
        // publish.
        unsafe { &*self.payload_ptr() }
    }
}

impl<T: Pod> DerefMut for Loan<T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: same as `Deref`; exclusive access until publish.
        unsafe { &mut *self.payload_ptr() }
    }
}

impl<T: Pod> Drop for Loan<T> {
    fn drop(&mut self) {
        if !self.published {
            // The slot is exclusive to this Loan via the free-list
            // pop. Refcount is already 0 (set when the slot was on
            // the free list and the bumped generation prevents
            // stale subscribers from interfering — they CAS-bump
            // refcount with a generation match required). Just
            // return it to the pool.
            self.segment.push_free(self.slot_idx);
        }
        // `segment` field drops normally below, decrementing the Arc.
    }
}

/// Read-only handle to a published slot. The subscriber reads
/// directly from SHM via `Deref`. On drop, the slot's refcount is
/// decremented and the slot returns to the free list if this was
/// the last reference.
pub struct Sample<T: Pod> {
    segment: Segment,
    slot_idx: u32,
    seq: u64,
    /// `fn() -> T` rather than `*const T` so `Sample<T>` is `Send +
    /// Sync` when `T: Pod` (which it is). The slot itself lives in
    /// shared memory, and `Segment` is already `Send + Sync`; the
    /// `Sample` just carries an integer index and an `Arc`.
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Pod> Sample<T> {
    pub(crate) fn new(segment: Segment, slot_idx: u32, seq: u64) -> Self {
        Self {
            segment,
            slot_idx,
            seq,
            _phantom: PhantomData,
        }
    }

    /// Publish sequence this sample was taken from.
    pub fn sequence(&self) -> u64 {
        self.seq
    }
}

impl<T: Pod> Deref for Sample<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: refcount >= 1 (we hold one ref), so the slot is
        // not on the free list and the payload bytes are stable for
        // the duration of `&self`. Alignment is satisfied as in
        // `Loan::deref`.
        unsafe { &*(self.segment.slot_payload(self.slot_idx) as *const T) }
    }
}

impl<T: Pod> Drop for Sample<T> {
    fn drop(&mut self) {
        self.segment.release(self.slot_idx);
    }
}
