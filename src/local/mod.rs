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
