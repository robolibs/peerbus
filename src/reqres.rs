//! Public **req/res** API — the canonical module for peerbus's
//! request/response primitives.
//!
//! Prefer `peerbus::reqres` and the `ReqRes*` type names in new code and
//! docs. This module re-exports the local and remote req/res building
//! blocks under their canonical `ReqRes*` names plus the shared
//! [`Envelope`] and [`DEFAULT_CALL_TIMEOUT`].
//!
//! The lower-level local/remote implementation lives in
//! [`crate::local::reqresp`] / [`crate::remote`]; the shared
//! [`Envelope`] and [`DEFAULT_CALL_TIMEOUT`] are defined here.
//! [`crate::reqresp`] remains as a pre-1.0 `req/resp` compatibility
//! alias that re-exports these names so existing callers keep
//! compiling. It will be removed only on a breaking release.
//!
//! `Envelope<H>` is the user_header used by the req/res services:
//! a `u64` correlation id plus a Pod metadata header `H`. For
//! fixed-Pod req/res types `T`, `H = T` and the entire
//! value rides in this header. For heap-bearing types, `H = T::Header`
//! and the bytes ride in the variable-length payload alongside.

use bytemuck::{Pod, Zeroable};
use datapod::ZeroCopySend;

/// Req/res envelope. The `H` parameter is `T::Header` for
/// whichever `T: datapod::DataPod` the service ships.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Envelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    pub req_id: u64,
    pub header: H,
}

// SAFETY: `Envelope<H>` is `#[repr(C)]`, contains only a `u64` and a
// `Pod` header `H`. When `H`'s alignment is ≤ 8 (the common case)
// the struct has no internal padding.
unsafe impl<H> Pod for Envelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> Zeroable for Envelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> ZeroCopySend for Envelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}

// The local SHM loan path zero-initialises the fixed header. We can't
// derive `Default` (H may not be Default), but H is `Zeroable`, so an
// all-zeros envelope is valid.
impl<H> Default for Envelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    fn default() -> Self {
        Self {
            req_id: 0,
            header: H::zeroed(),
        }
    }
}

/// Default timeout for `Client::call` if the user does not override.
pub const DEFAULT_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// Local (same-host) req/res, exposed under the canonical `ReqRes*`
// names. These are aliases of the existing `LocalReqResp*`/`Local*`
// implementation types in [`crate::local::reqresp`].
pub use crate::local::reqresp::{
    LocalClient, LocalReqClient, LocalReqResService, LocalReqServer, LocalRequestServer, PendingReq,
    PendingRequest, ReplyHandle, RequestSample, ResSample, ResponseSample,
};

// Remote (cross-host) req/res client, under the canonical name.
pub use crate::remote::RemoteReqClient;
