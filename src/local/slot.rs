//! `Slot<H>` — a `#[repr(transparent)]` wrapper around `T::Header`
//! that adds a `Default` impl backed by `bytemuck::Zeroable::zeroed()`.
//!
//! iceoryx2's `loan_*_uninit` APIs zero-initialise the `user_header`
//! via `UserHeader::default()`. `datapod::DataPod::Header` is `Pod +
//! Zeroable + Copy`, but `Default` is not part of that bound — a Pod
//! struct generated for an arbitrary user type doesn't automatically
//! derive `Default`. Wrapping in `Slot<H>` (transparent, same layout)
//! gives us the missing `Default` without touching the wire format.

use core::fmt::Debug;

use bytemuck::{Pod, Zeroable};
use iceoryx2::prelude::ZeroCopySend;

/// Transparent newtype around a Pod header that supplies `Default`
/// via `Zeroable`. Used internally as the iceoryx2 `user_header`
/// type so we can call `loan_slice_uninit` regardless of whether
/// `H: Default`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct Slot<H>(pub H)
where
    H: Pod + Zeroable + ZeroCopySend + Debug + Copy + 'static;

// SAFETY: `#[repr(transparent)]` means `Slot<H>` has the exact same
// layout as `H`. All `Pod`/`Zeroable` invariants carry over.
unsafe impl<H> Pod for Slot<H> where H: Pod + Zeroable + ZeroCopySend + Debug + Copy + 'static {}
unsafe impl<H> Zeroable for Slot<H> where H: Pod + Zeroable + ZeroCopySend + Debug + Copy + 'static {}
unsafe impl<H> ZeroCopySend for Slot<H> where H: Pod + Zeroable + ZeroCopySend + Debug + Copy + 'static
{}

impl<H> Default for Slot<H>
where
    H: Pod + Zeroable + ZeroCopySend + Debug + Copy + 'static,
{
    fn default() -> Self {
        Self(H::zeroed())
    }
}
