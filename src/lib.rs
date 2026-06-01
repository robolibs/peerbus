//! quicbit — typed zero-copy messaging for robotics.
//!
//! One entry point: [`Node`]. It owns an `iroh::Endpoint` for
//! cross-host pub/sub plus a local SHM service factory for
//! same-host pub/sub, and exposes:
//!
//! * `node.publisher::<T>(topic)` — publish a topic. The publisher
//!   writes payloads into a shared-memory sample slot (local
//!   subscribers read with zero copy) and broadcasts to any
//!   currently-attached iroh subscribers (cross-host).
//! * `node.subscriber::<T>(peer, topic)` — subscribe to a peer's
//!   topic. quicbit tries the local SHM service first;
//!   if the service doesn't exist on this host, it dials over iroh.
//!
//! ```no_run
//! use quicbit::Node;
//!
//! # fn main() -> quicbit::Result<()> {
//! #[datapod::datapod]
//! struct Pose { x: f32, y: f32, yaw: f32 }
//!
//! let node = Node::builder().identity("rover-a").no_relay().bind()?;
//!
//! let mut pubr = node.publisher::<Pose>("rover/pose")?;
//! let mut sub  = node.subscriber::<Pose>("rover-a", "rover/pose")?;
//!
//! pubr.send(&Pose { x: 1.0, y: 2.0, yaw: 0.1 })?;
//! if let Some(s) = sub.take()? {
//!     println!("pose: {:?}", s.header());
//! }
//! # Ok(()) }
//! ```
//!
//! Lower-level building blocks ([`LocalService`], [`LocalTransport`],
//! [`RemoteTransport`], the [`Transport`] trait) are also public for
//! callers who want direct control. Most users want [`Node`].

pub mod async_adapter;
pub mod chunk;
pub mod demo;
pub mod did_key;
pub mod error;
pub mod ffi;
pub mod local;
pub mod node;
pub mod pip;
pub mod putack;
#[cfg(feature = "python")]
pub mod python;
pub mod qos;
pub mod queans;
pub mod raw;
pub mod remote;
pub mod reqresp;
mod trace;
pub mod transport;

pub use async_adapter::{AsyncPublisher, AsyncSubscriber};
pub use error::{Error, Result};
pub use local::{
    AnsReply as LocalAnsReply, AnsSample as LocalAnsSample, Loan, LocalAckSample, LocalAckServer,
    LocalAnsServer, LocalAnswers, LocalClient, LocalConfig, LocalPip, LocalPipClient,
    LocalPipSample, LocalPipServer, LocalPipService, LocalPublisher, LocalPutAckService,
    LocalPutClient, LocalPutSample, LocalPutSender, LocalPuts, LocalQueAnsService, LocalQueClient,
    LocalReqClient, LocalReqResService, LocalReqRespService, LocalReqServer, LocalRequestServer,
    LocalService, LocalSubscriber, LocalTransport, PendingQue as LocalPendingQue,
    QueSample as LocalQueSample, ReplyHandle, Sample,
};
pub use node::{
    AckSample, AckServer, AnsReplyToken, AnsSample, AnsServer, Answers, IntoPeer, ItemStats, Node,
    NodeBuilder, NodeSample, NodeStats, PathDiagnostic, Peer, PeerPathDiagnostics,
    PendingPipMessage, PendingPutMessage, PendingQue, PendingQueMessage, PendingReq,
    PendingReqMessage, Pip, PipClient, PipSample, PipServer, PipServerToken, PipSessionToken,
    PipStats, Publisher, PublisherStats, PutAckToken, PutClient, PutSample, PutSender, PutStats,
    PutUploadToken, Puts, QueClient, QueSample, QueStats, ReqClient, ReqReply, ReqReplyToken,
    ReqSample, ReqServer, ReqStats, ResSample, Subscriber, SubscriberStats,
};
pub use qos::{DeliveryPolicy, TopicQos};
pub use raw::RawMsg;
pub use remote::{
    RemotePipClient, RemotePutClient, RemoteQueClient, RemoteReqClient, RemoteTransport,
    RemoteTransportBuilder,
};
pub use reqresp::Envelope;
pub use transport::{LocalPayload, PublisherOps, SubscriberOps, Transport};
