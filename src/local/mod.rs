//! Local transport — pure-Rust shared-memory publish/subscribe.
//!
//! [`service`] exposes the typed user-facing API:
//! [`LocalService`], [`LocalPublisher`], [`LocalSubscriber`]. The
//! RAII handles live in [`handle`]. Request/response support sits
//! in [`reqresp`].
//!
//! The current backend uses `shared_memory` plus a small POD ring in
//! [`shm`]. It intentionally keeps delivery poll-based so the async
//! layer can continue wrapping the sync API without a cross-process
//! wakeup primitive.

pub mod handle;
pub mod reqresp;
pub mod service;
pub(crate) mod shm;
pub mod transport;

pub use handle::{Loan, Sample};
pub use reqresp::{LocalClient, LocalReqRespService, LocalRequestServer, ReplyHandle};
pub use service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
pub use transport::LocalTransport;
