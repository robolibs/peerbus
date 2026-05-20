//! Local transport — POSIX SHM slot allocator.
//!
//! The on-disk wire layout is defined in [`layout`]. [`segment`]
//! glues the layout to a live mapping. [`service`] exposes the
//! typed user-facing API: [`LocalService`], [`LocalPublisher`],
//! [`LocalSubscriber`]. The RAII handles live in [`handle`].
//! Request/response support sits in [`reqresp`].

pub mod handle;
pub mod layout;
pub mod reqresp;
pub mod segment;
pub mod service;
pub mod shm;
pub mod transport;

pub use handle::{Loan, Sample};
pub use reqresp::{LocalClient, LocalReqRespService, LocalRequestServer, ReplyHandle};
pub use service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
pub use transport::LocalTransport;
