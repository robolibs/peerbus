//! Cross-cutting types for req/res.
//!
//! This module is the pre-1.0 `req/resp` compatibility home for the
//! shared req/res types. The canonical public module is
//! [`crate::reqres`]; prefer `peerbus::reqres` and the `ReqRes*` names
//! in new code. These types are re-exported unchanged from `reqres`.
//!
//! `Envelope<H>` is the user_header used by the reqresp services:
//! a `u64` correlation id plus a Pod metadata header `H`. For
//! fixed-Pod req/res types `T`, `H = T` and the entire
//! value rides in this header. For heap-bearing types, `H = T::Header`
//! and the bytes ride in the variable-length payload alongside.

pub use crate::reqres::{DEFAULT_CALL_TIMEOUT, Envelope};
