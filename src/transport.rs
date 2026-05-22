//! Transport abstractions.
//!
//! quicbit ships two transports today: [`crate::local::LocalTransport`]
//! (iceoryx2 SHM) and [`crate::remote::RemoteTransport`] (iroh QUIC).
//! Both implement the [`Transport`] trait so higher layers can be
//! generic over them.
//!
//! Every payload `T` shipped over a quicbit transport implements
//! [`datapod::DataPod`]. That trait provides:
//!
//! * A Pod **header** `T::Header` — fixed size, lives in the wire's
//!   metadata slot.
//! * An optional byte **payload** `T::Payload` (= `()` or `[u8]`) —
//!   variable-length data carrying the heap bytes for types like
//!   `Polygon`, `Grid`, `Linestring`, etc.
//!
//! For fixed-Pod types (e.g. `Point`, `Pose`, `Joint`) `T::Header = T`,
//! so the entire value rides in the header and the payload is empty.

use crate::error::Result;

/// FNV-1a (64-bit). Used internally to hash type-layout descriptors
/// for the wire handshake.
pub fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Hash a type's wire identity from its memory layout (size + align).
/// Stable across rustc versions, unlike `core::any::type_name`.
pub fn wire_type_hash<T>() -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for byte in (std::mem::size_of::<T>() as u64).to_le_bytes() {
        h ^= byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    for byte in (std::mem::align_of::<T>() as u64).to_le_bytes() {
        h ^= byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Marker trait for the local-transport payload bound. Adds a
/// `Send + Sync` bound to `T::Header` so loans/samples carrying a
/// `T::Header` value across threads (e.g. via `tokio::spawn_blocking`)
/// type-check. `Pod` types are always thread-safe in practice;
/// stating it here lets us be generic over `T: LocalPayload` without
/// repeating the bound at every impl site.
pub trait LocalPayload: datapod::DataPod<Header: Send + Sync> + 'static {}
impl<T> LocalPayload for T
where
    T: datapod::DataPod + 'static,
    T::Header: Send + Sync,
{
}

/// Marker trait for the remote-transport (iroh) payload bound.
pub trait RemotePayload:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

impl<T> RemotePayload for T where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

/// Operations a publisher handle must support. Loans are
/// fixed-shape "header + variable-byte payload" containers; the
/// publisher writes both halves in place and hands the loan back
/// via [`publish`](PublisherOps::publish).
pub trait PublisherOps<T: LocalPayload>: Send {
    type Loan: Send;

    /// Reserve a slot with `byte_count` payload bytes. Pass 0 for
    /// fixed-Pod types (`T::Payload = ()`); pass the cast-byte
    /// length for heap-bearing types (`T::Payload = [u8]`).
    fn loan(&mut self, byte_count: usize) -> Result<Self::Loan>;

    /// Hand the loan over to subscribers. Returns a transport-defined
    /// sequence number (monotonically increasing).
    fn publish(&mut self, loan: Self::Loan) -> Result<u64>;
}

/// Operations a subscriber handle must support.
pub trait SubscriberOps<T: LocalPayload>: Send {
    type Sample: Send;

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
