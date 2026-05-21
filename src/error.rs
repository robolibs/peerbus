//! Crate-level error type.
//!
//! One `#[non_exhaustive]` enum so adding variants later is not a
//! breaking change. Per-module `thiserror` sub-enums may be added
//! if this grows past ~15 variants (compare `wirebit::Error`).

use thiserror::Error;

pub type Result<T, E = Error> = core::result::Result<T, E>;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    #[error("service not found: {0}")]
    ServiceNotFound(String),

    #[error("service already exists: {0}")]
    ServiceAlreadyExists(String),

    #[error("no free SHM slot on service {service}")]
    NoFreeSlot { service: String },

    #[error("subscriber lagged; {dropped} samples dropped")]
    Lagged { dropped: u64 },

    #[error("peer disconnected")]
    Disconnected,

    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("payload too large: {actual} bytes > slot capacity {capacity}")]
    PayloadTooLarge { actual: usize, capacity: usize },

    #[error("payload type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: &'static str, got: String },

    #[error("incompatible SHM segment: {0}")]
    IncompatibleShm(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("connect failed: {0}")]
    ConnectFailed(String),

    #[error("handshake version mismatch: local={local} peer={peer}")]
    HandshakeVersionMismatch { local: u32, peer: u32 },

    #[error("handshake malformed: {0}")]
    HandshakeMalformed(String),

    #[error("frame too large: {actual} bytes > limit {limit}")]
    FrameTooLarge { actual: u64, limit: u64 },

    #[error("topic name too long: {len} bytes > limit {limit}")]
    TopicNameTooLong { len: usize, limit: usize },

    /// Catch-all for transport-layer failures (iroh send/recv, etc.)
    /// where the specific variant isn't load-bearing for callers.
    #[error("remote transport error: {0}")]
    Remote(String),

    /// Last-resort escape hatch. Prefer one of the structured
    /// variants above for any error users may want to match on.
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }

    pub fn incompatible_shm(msg: impl Into<String>) -> Self {
        Self::IncompatibleShm(msg.into())
    }
}
