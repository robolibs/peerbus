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

/// Marker trait for types you can send/receive locally.
///
/// `Pod` is required because the local transport places the bytes
/// directly into a shared-memory slot and reads them back through a
/// typed pointer — there is no serialization step. `Send + Sync`
/// is automatic for `Pod`.
pub trait LocalPayload: bytemuck::Pod + 'static {}
impl<T: bytemuck::Pod + 'static> LocalPayload for T {}

/// Marker trait for types you can send/receive across the network.
///
/// Adds `serde::Serialize + DeserializeOwned` on top of [`LocalPayload`].
/// `Pod` is *not* required — the remote transport serializes through
/// `postcard`, which can handle non-POD types.
#[cfg(feature = "remote")]
pub trait RemotePayload:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

#[cfg(feature = "remote")]
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
