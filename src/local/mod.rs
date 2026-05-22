//! Local transport — iceoryx2-backed publish/subscribe.
//!
//! [`service`] exposes the typed user-facing API:
//! [`LocalService`], [`LocalPublisher`], [`LocalSubscriber`]. The
//! RAII handles live in [`handle`]. Request/response support sits
//! in [`reqresp`].
//!
//! Earlier versions of this module shipped a hand-rolled POSIX
//! SHM allocator (`segment.rs`, `layout.rs`, `shm.rs`, plus the
//! cross-process `crate::registry`). All of that was retired in
//! favour of iceoryx2 — the SHM ABA / refcount / multi-publisher
//! work iceoryx2 has shipped in production is well beyond what
//! we'd have built ourselves.

pub mod handle;
pub mod reqresp;
pub mod service;
pub(crate) mod slot;
pub mod transport;

pub use handle::{Loan, Sample};
pub use reqresp::{LocalClient, LocalReqRespService, LocalRequestServer, ReplyHandle};
pub use service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
pub use transport::LocalTransport;
