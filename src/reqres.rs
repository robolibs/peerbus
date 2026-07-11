//! Public req/res compatibility module.
//!
//! The original lower-level implementation lives in [`crate::reqresp`] so old
//! callers keep compiling. New public docs and imports should prefer
//! `peerbus::reqres`.

pub use crate::reqresp::{DEFAULT_CALL_TIMEOUT, Envelope};
