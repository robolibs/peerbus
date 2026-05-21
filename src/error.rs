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

    #[error("remote transport error: {0}")]
    Remote(String),

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
