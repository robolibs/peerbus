//! Local transport — pure-Rust shared-memory publish/subscribe.
//!
//! [`service`] exposes the typed user-facing API:
//! [`LocalService`], [`LocalPublisher`], [`LocalSubscriber`]. The
//! RAII handles live in [`handle`]. Request/response support sits
//! in [`reqresp`].
//!
//! The current backend uses `shared_memory` plus a small POD ring in
//! the private `shm` module. It intentionally keeps delivery poll-based so the async
//! layer can continue wrapping the sync API without a cross-process
//! wakeup primitive.

pub mod handle;
pub mod pip;
pub mod putack;
pub mod queans;
pub mod reqresp;
pub mod service;
pub(crate) mod shm;
pub mod transport;

pub use handle::{Loan, Sample};
pub use pip::{
    LocalPip, LocalPipClient, LocalPipServer, LocalPipService, PipSample as LocalPipSample,
};
pub use putack::{
    AckSample as LocalAckSample, LocalAckServer, LocalPutAckService, LocalPutClient,
    LocalPutSender, LocalPuts, PutSample as LocalPutSample,
};
pub use queans::{
    AnsReply, AnsSample, LocalAnsServer, LocalAnswers, LocalQueAnsService, LocalQueClient,
    PendingQue, QueSample,
};
pub use reqresp::{
    LocalClient, LocalReqClient, LocalReqResService, LocalReqRespService, LocalReqServer,
    LocalRequestServer, ReplyHandle,
};
pub use service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
pub use transport::LocalTransport;

/// Starting value for a handle's request or session counter. Counters
/// that all start at zero collide across clients on one shared ring: a
/// retained response with the same id is taken by the wrong caller.
pub fn seed_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SALT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let salt = SALT.fetch_add(1, Ordering::Relaxed);
    let mut hasher = blake3::Hasher::new();
    hasher.update(&nanos.to_le_bytes());
    hasher.update(&(std::process::id() as u64).to_le_bytes());
    hasher.update(&salt.to_le_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest.as_bytes()[..8]);
    // Keep headroom below u64::MAX so fetch_add never wraps in practice.
    u64::from_le_bytes(out) >> 1
}
