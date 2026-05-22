//! quicbit — typed zero-copy messaging for robotics.
//!
//! One entry point: [`Node`]. It owns an `iroh::Endpoint` for
//! cross-host pub/sub plus an iceoryx2 service factory for
//! same-host pub/sub, and exposes:
//!
//! * `node.publisher::<T>(topic)` — publish a topic. The publisher
//!   writes payloads into an iceoryx2 sample slot (local
//!   subscribers read with zero copy) and broadcasts to any
//!   currently-attached iroh subscribers (cross-host).
//! * `node.subscriber::<T>(peer, topic)` — subscribe to a peer's
//!   topic. quicbit tries the local iceoryx2 service first;
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
pub mod error;
pub mod local;
pub mod node;
pub mod remote;
pub mod reqresp;
mod trace;
pub mod transport;

pub use error::{Error, Result};
pub use local::{
    LocalClient, LocalConfig, LocalPublisher, LocalReqRespService, LocalRequestServer,
    LocalService, LocalSubscriber, LocalTransport, Loan, ReplyHandle, Sample,
};
pub use node::{
    IntoPeer, Node, NodeBuilder, NodeSample, NodeStats, Peer, Publisher, PublisherStats,
    Subscriber, SubscriberStats,
};
pub use remote::{RemoteTransport, RemoteTransportBuilder};
pub use reqresp::Envelope;
pub use async_adapter::{AsyncPublisher, AsyncSubscriber};
pub use transport::{LocalPayload, PublisherOps, SubscriberOps, Transport};
