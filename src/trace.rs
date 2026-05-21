//! Feature-gated logging shims.
//!
//! When `feature = "tracing"` is on, these macros forward to
//! `tracing::*`. When off, they discard all tokens and expand to
//! nothing — no dependency on `tracing`, no runtime cost. Callers
//! use them like the `tracing` macros: `qb_info!(target: "...",
//! k = v, "message")`.
//!
//! The off-path is a token-discarding `{}` rather than a syntax-
//! checked `format_args!` shim because `tracing`'s macro syntax is
//! a superset (`target: ...`, structured fields) that `format_args`
//! doesn't accept. The CI matrix builds both feature combinations
//! so typos in tracing call sites do not silently rot.

#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! qb_error {
    ($($t:tt)*) => { ::tracing::error!($($t)*) };
}
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! qb_error {
    ($($t:tt)*) => {{}};
}

#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! qb_warn {
    ($($t:tt)*) => { ::tracing::warn!($($t)*) };
}
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! qb_warn {
    ($($t:tt)*) => {{}};
}

#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! qb_info {
    ($($t:tt)*) => { ::tracing::info!($($t)*) };
}
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! qb_info {
    ($($t:tt)*) => {{}};
}

#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! qb_debug {
    ($($t:tt)*) => { ::tracing::debug!($($t)*) };
}
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! qb_debug {
    ($($t:tt)*) => {{}};
}

#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! qb_trace {
    ($($t:tt)*) => { ::tracing::trace!($($t)*) };
}
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! qb_trace {
    ($($t:tt)*) => {{}};
}
