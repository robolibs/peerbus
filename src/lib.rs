//! quicbit — typed zero-copy messaging for robotics.
//!
//! One entry point: [`Node`]. It owns an `iroh::Endpoint`, registers
//! itself in the host-wide registry, and exposes:
//!
//! * `node.publisher::<T>(topic)` — publish a topic. The publisher
//!   writes payloads into an SHM slot (local subscribers read with
//!   zero copy) and broadcasts to any currently-attached iroh
//!   subscribers (cross-host).
//! * `node.subscriber::<T>(peer, topic)` — subscribe to a peer's
//!   topic. quicbit checks the registry: if the peer is on this
//!   host, the subscriber attaches to the SHM segment; otherwise
//!   it dials over iroh.
//!
//! ```no_run
//! use bytemuck::{Pod, Zeroable};
//! use quicbit::Node;
//!
//! # fn main() -> quicbit::Result<()> {
//! #[repr(C)]
//! #[derive(Clone, Copy, Pod, Zeroable, Debug)]
//! struct Pose { x: f32, y: f32, yaw: f32 }
//!
//! let node = Node::builder().no_relay().bind()?;
//! let peer_id = node.endpoint_id();
//!
//! let mut pubr = node.publisher::<Pose>("rover/pose")?;
//! let mut sub  = node.subscriber::<Pose>(peer_id, "rover/pose")?;
//!
//! pubr.send(Pose { x: 1.0, y: 2.0, yaw: 0.1 })?;
//! if let Some(s) = sub.take()? {
//!     println!("pose: {:?}", *s);
//! }
//! # Ok(()) }
//! ```
//!
//! Lower-level building blocks ([`LocalService`], [`LocalTransport`],
//! [`RemoteTransport`], the [`Transport`] trait) are also public for
//! callers who want direct control. Most users want [`Node`].

#[cfg(feature = "async")]
pub mod async_adapter;
pub mod error;
pub mod local;
#[cfg(feature = "remote")]
pub mod node;
pub mod registry;
pub mod reqresp;
pub mod transport;

#[cfg(feature = "remote")]
pub mod remote;

pub mod ffi;
#[cfg(feature = "python")]
pub mod python;

pub use error::{Error, Result};
pub use local::{
    LocalClient, LocalConfig, LocalPublisher, LocalReqRespService, LocalRequestServer,
    LocalService, LocalSubscriber, LocalTransport, Loan, ReplyHandle, Sample,
};
pub use reqresp::Envelope;
#[cfg(feature = "async")]
pub use async_adapter::{AsyncPublisher, AsyncSubscriber};
pub use transport::{LocalPayload, PublisherOps, SubscriberOps, Transport};

#[cfg(feature = "remote")]
pub use node::{IntoPeer, Node, NodeBuilder, NodeSample, Peer, Publisher, Subscriber};
#[cfg(feature = "remote")]
pub use remote::{RemoteTransport, RemoteTransportBuilder};
