//! RAII handles for borrowed iceoryx2 samples.
//!
//! [`Loan<T>`] wraps an `iceoryx2::SampleMut`. The publisher fills
//! the payload in place via `DerefMut` and hands it back to the
//! publisher's `publish()`, which calls `send()` under the hood.
//!
//! [`Sample<T>`] wraps an `iceoryx2::Sample`. The subscriber reads
//! the payload via `Deref`. Drop releases the underlying iceoryx2
//! sample, which returns the SHM slot to the publisher's pool.

use core::fmt::Debug;
use std::ops::{Deref, DerefMut};

use bytemuck::Pod;
use iceoryx2::prelude::*;
use iceoryx2::sample::Sample as IoxSample;
use iceoryx2::sample_mut::SampleMut as IoxSampleMut;

/// Writable handle to an in-flight iceoryx2 sample. Derefs mutably
/// to `T`. Hand back to `LocalPublisher::publish` to send.
///
/// On `Drop` without publish, the underlying iceoryx2 sample is
/// released and the slot returns to the publisher's pool — same
/// semantics as our old SHM allocator's rollback path.
pub struct Loan<T: Pod + ZeroCopySend + Debug + 'static> {
    pub(crate) inner: IoxSampleMut<ipc_threadsafe::Service, T, ()>,
}

impl<T: Pod + ZeroCopySend + Debug + 'static> Deref for Loan<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner.payload()
    }
}

impl<T: Pod + ZeroCopySend + Debug + 'static> DerefMut for Loan<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner.payload_mut()
    }
}

/// Read-only handle to a received iceoryx2 sample. Derefs to `T`.
/// On `Drop`, the iceoryx2 sample is released; the publisher's
/// slot becomes available again once every subscriber has dropped
/// its view.
pub struct Sample<T: Pod + ZeroCopySend + Debug + 'static> {
    pub(crate) inner: IoxSample<ipc_threadsafe::Service, T, ()>,
}

impl<T: Pod + ZeroCopySend + Debug + 'static> Sample<T> {
    /// iceoryx2 doesn't surface a per-publish sequence number on
    /// the sample's `Header` in v0.7. We provide a placeholder for
    /// API parity with the old SHM allocator's `Sample::sequence`.
    pub fn sequence(&self) -> u64 {
        0
    }
}

impl<T: Pod + ZeroCopySend + Debug + 'static> Deref for Sample<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner.payload()
    }
}
