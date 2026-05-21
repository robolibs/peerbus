//! Remote transport — iroh peer-to-peer QUIC.
//!
//! One `RemoteTransport` owns one [`iroh::Endpoint`] and is scoped to
//! one topic name (the `Service<RemoteTransport>` analogue of
//! `LocalService`'s SHM-segment name). The transport supports both
//! publishing and subscribing on that topic; whichever side a peer
//! is on, the wire protocol is the same:
//!
//! 1. The subscribing side opens an `open_bi` stream on the iroh
//!    connection and writes the topic handshake (magic + version +
//!    type hash + topic name), then `finish()`es the send half.
//! 2. The publishing side accepts the bi stream, reads the
//!    handshake, looks up its local broadcast queue, and copies
//!    every broadcast message onto the bi stream's send half.
//! 3. The subscribing side reads length-prefixed frames off the
//!    recv half until the publisher closes.
//!
//! Payloads use `bytemuck` for serialization on the wire: any
//! `LocalPayload` (i.e. `Pod + 'static`) is just its byte
//! representation. Non-Pod payloads come with Phase 4.

mod handshake;
pub(crate) mod reqresp;
pub(crate) mod runtime;
pub(crate) mod transport;

pub use handshake::{HANDSHAKE_MAGIC, HANDSHAKE_VERSION, REQRESP_MAGIC};
pub use reqresp::{parse_request_handshake_tail, RemoteClient};
pub use transport::{
    parse_frame, parse_pubsub_handshake_tail, RemoteLoan, RemotePublisher,
    RemotePublisherStats, RemoteSample, RemoteSubscriber, RemoteSubscriberStats,
    RemoteTransport, RemoteTransportBuilder,
};
