//! Transport abstraction.
//!
//! A [`Transport`] is the seam between the messaging layer (this
//! crate) and the bytes-in-flight layer (SHM slots locally, QUIC
//! streams remotely). The trait is intentionally small: each
//! implementation defines its own associated `Publisher` /
//! `Subscriber` types so the same `Service<T>` user code works
//! against every backend with no boxing on the hot path.
//!
//! See [`crate::local::LocalTransport`] and (with the `remote`
//! feature) [`crate::remote::RemoteTransport`] for concrete impls.

use std::ops::{Deref, DerefMut};

use crate::error::Result;

/// FNV-1a (64-bit) — used to hash `std::any::type_name::<T>()`
/// across publisher and subscriber so the iroh handshake can
/// detect a payload-type mismatch quickly. Kept here because the
/// historical home (`local::layout`) is gone after the iceoryx2
/// migration.
pub fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Marker trait for types you can send/receive locally.
///
/// Three constraints:
/// * `bytemuck::Pod` — fixed memory layout, valid bit pattern for
///   any byte sequence of the right size. We use this for the iroh
///   remote path's byte-level (de)serialisation.
/// * `iceoryx2::ZeroCopySend` — iceoryx2's marker that a type may
///   ride in shared memory between processes. In practice it
///   requires `#[repr(C)]` + no pointers / references / heap.
///   `Pod` satisfies the safety contract, but the trait must be
///   `unsafe impl`'d (or `#[derive(ZeroCopySend)]`) for each user
///   type because the orphan rules prevent us from doing it
///   automatically.
/// * `Debug` — iceoryx2's `Sample` / `SampleMut` types require it.
pub trait LocalPayload:
    bytemuck::Pod + iceoryx2::prelude::ZeroCopySend + core::fmt::Debug + 'static
{
}
impl<T> LocalPayload for T where
    T: bytemuck::Pod + iceoryx2::prelude::ZeroCopySend + core::fmt::Debug + 'static
{
}

/// Marker trait for types you can send/receive across the network.
///
/// Adds `serde::Serialize + DeserializeOwned` on top of [`LocalPayload`].
/// `Pod` is *not* required — the remote transport serializes through
/// `postcard`, which can handle non-POD types.
pub trait RemotePayload:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

impl<T> RemotePayload for T where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

/// Operations a publisher handle must support.
pub trait PublisherOps<T>: Send {
    /// RAII handle that derefs mutably to `T`. The publisher writes
    /// in-place and then hands it back via [`publish`].
    ///
    /// [`publish`]: PublisherOps::publish
    type Loan: DerefMut<Target = T>;

    /// Reserve a slot for in-place writes.
    fn loan(&mut self) -> Result<Self::Loan>;

    /// Hand the loan over to subscribers. Returns a transport-defined
    /// sequence number (monotonically increasing).
    fn publish(&mut self, loan: Self::Loan) -> Result<u64>;
}

/// Operations a subscriber handle must support.
pub trait SubscriberOps<T>: Send {
    /// RAII handle that derefs to `T`. Borrows shared bytes on the
    /// local path and an owned (deserialized) value on the remote
    /// path; both expose the same `Deref` surface.
    type Sample: Deref<Target = T>;

    /// Non-blocking take. `Ok(None)` means "no new sample".
    fn take(&mut self) -> Result<Option<Self::Sample>>;
}

/// A transport — the thing that owns slots/streams and hands out
/// typed publishers and subscribers.
pub trait Transport: Send + Sync {
    type Publisher<T: LocalPayload>: PublisherOps<T>;
    type Subscriber<T: LocalPayload>: SubscriberOps<T>;

    fn publisher<T: LocalPayload>(&self) -> Result<Self::Publisher<T>>;
    fn subscriber<T: LocalPayload>(&self) -> Result<Self::Subscriber<T>>;
}
