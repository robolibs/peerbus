//! Cross-cutting types for request/response.
//!
//! The local req/resp implementation lives in [`crate::local`];
//! the remote (iroh) implementation in [`crate::remote`]. Both
//! share the [`Envelope`] payload shape so messages on the wire
//! are bit-for-bit identical between transports.
//!
//! Correlation is by `req_id`, a monotonically increasing `u64`
//! handed out by the client. The server echoes the same `req_id`
//! in its response so multiple concurrent clients can disambiguate
//! their replies on a shared response channel.

use bytemuck::{Pod, Zeroable};
use iceoryx2::prelude::ZeroCopySend;

/// `#[repr(C)]` wrapper carrying a request/response id alongside a
/// `Pod` payload. The local transport places `Envelope<T>` directly
/// into iceoryx2 SHM slots; the remote transport serializes it
/// as bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Envelope<T: Pod + ZeroCopySend> {
    pub req_id: u64,
    pub payload: T,
}

// SAFETY: `Envelope<T>` is `#[repr(C)]`, contains only a `u64` and a
// `Pod + ZeroCopySend` payload, and has no padding when `T`'s
// alignment is ≤ 8 (the common case for the types we accept).
unsafe impl<T: Pod + ZeroCopySend> Pod for Envelope<T> {}
unsafe impl<T: Pod + ZeroCopySend> Zeroable for Envelope<T> {}
unsafe impl<T: Pod + ZeroCopySend> ZeroCopySend for Envelope<T> {}

/// Default timeout for `Client::call` if the user does not override.
pub const DEFAULT_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
