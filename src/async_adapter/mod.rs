//! Async adapter — `async fn` wrappers over the sync core.
//!
//! The wrappers call the underlying sync API inside
//! `tokio::task::spawn_blocking` so callers can use `.await` from
//! a tokio runtime without blocking its worker threads. The local
//! SHM path is genuinely blocking-friendly (atomic ops + an
//! occasional yield), so this isn't a contortion — it just lets
//! peerbus fit into an async surrounding.
//!
//! For the remote transport, the sync core *already* drives an
//! internal tokio runtime via `block_on`. The async wrapper here is
//! still useful: it lets the calling runtime keep doing other work
//! while the internal runtime handles QUIC traffic.
//!
//! Layout:
//! * [`pubsub`] — [`AsyncPublisher`] / [`AsyncSubscriber`].
//! * [`local`] — req/res, que/ans, put/ack, and pip wrappers over the
//!   same-host SHM services.
//! * [`remote`] — standalone iroh client wrappers.

use std::time::Duration;

use bytemuck::Pod;

use crate::error::{Error, Result};
use crate::local::pip::{LocalPendingPip, LocalPipClient, LocalPipServer, PipSample};
use crate::local::putack::{AckSample, LocalAckServer, LocalPendingPuts, LocalPutClient, PutSample};
use crate::local::queans::{AnsSample, LocalAnsServer, LocalQueClient, QueSample};
use crate::local::reqresp::{LocalReqClient, LocalReqServer, RequestSample, ResponseSample};
use crate::remote::{RemotePipClient, RemotePutClient, RemoteQueClient, RemoteReqClient};
use crate::transport::{LocalPayload, PublisherOps, SubscriberOps};

mod local;
mod pubsub;
mod remote;

pub use local::{
    AsyncAckServer, AsyncAnsServer, AsyncPipClient, AsyncPipServer, AsyncPutClient, AsyncQueClient,
    AsyncReqClient, AsyncReqServer,
};
pub use pubsub::{AsyncPublisher, AsyncSubscriber};
pub use remote::{
    AsyncRemotePipClient, AsyncRemotePutClient, AsyncRemoteQueClient, AsyncRemoteReqClient,
};

/// Shared helper: pull the inner sync handle out of its `Option`
/// slot before a `spawn_blocking`, mirroring the pub/sub wrappers.
/// A `None` means the wrapper was polled from two places at once
/// (the inner handle is currently owned by an in-flight blocking
/// task), which is a misuse.
fn take_inner<X>(slot: &mut Option<X>, what: &str) -> Result<X> {
    slot.take().ok_or_else(|| {
        Error::Other(format!(
            "{what} was polled concurrently from two places — inner handle is missing"
        ))
    })
}
