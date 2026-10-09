//! [`RemoteTransport`] — pub/sub on top of an [`iroh::Endpoint`].
//!
//! Architectural shape, mirroring the local transport's
//! "one transport = one topic" rule:
//!
//! * A `RemoteTransport` owns exactly one `iroh::Endpoint` and is
//!   bound to one topic name. Multiple transports can coexist in
//!   one process; each opens its own endpoint.
//! * The transport runs a background **accept loop** that handles
//!   inbound subscriber connections. Each incoming bi stream
//!   starts with the [`handshake`] bytes; if the topic matches
//!   the transport's, we hook the stream's send half up to the
//!   topic's broadcast queue and start forwarding.
//! * Subscribers are *active*: at construction time, the subscriber
//!   task dials a configured peer (`builder.peer(id)`), opens a
//!   bi stream, writes the handshake, then reads length-prefixed
//!   frames off the recv half into a broadcast that each
//!   subscriber's `take()` polls (fan-out: one wire stream serves
//!   N in-process subscribers on the same topic).
//!
//! Payloads are `bytemuck::Pod`, sent as raw bytes. Frames carry a
//! length prefix only because QUIC streams don't preserve write
//! boundaries; the wire format is `[u32_le length][raw bytes]`.
//!
//! Module layout:
//! * [`endpoint`] — the builder, the `RemoteTransport` handle, its
//!   shared inner state, and the [`Transport`] impl.
//! * [`channels`] — the publisher/subscriber/loan/sample handles and
//!   their stats.
//! * [`serve`] — the inbound accept loop and per-connection serving.
//! * [`dial`] — the outbound subscriber dial/reconnect loop.
//! * [`wire`] — handshake and length-prefixed frame encode/decode.

use std::any::type_name;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::endpoint::presets;
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use tokio::task::JoinHandle;

use crate::chunk::{
    CHUNK_FRAME_FLAG, CHUNK_FRAME_LEN_MASK, CHUNK_HEADER_LEN, Reassembler, parse_chunk_payload,
};
use crate::error::{Error, Result};
use crate::qos::{DeliveryPolicy, TopicQos};
use crate::remote::handshake::{
    HANDSHAKE_MAGIC, HANDSHAKE_VERSION, MAX_PAYLOAD_LEN, MAX_TOPIC_LEN, PIP_MAGIC,
    PUBSUB_HANDSHAKE_VERSION_QOS, PUTACK_MAGIC, QUEANS_MAGIC, REQRESP_MAGIC,
};
use crate::remote::runtime;
use crate::transport::wire_type_hash;
use crate::transport::{LocalPayload, PublisherOps, SubscriberOps, Transport};
use crate::{qb_debug, qb_info, qb_warn};

mod channels;
mod dial;
mod endpoint;
mod serve;
mod wire;

// Re-export the full surface so `crate::remote::transport::*` keeps
// resolving exactly as it did when this was a single flat module.
pub use channels::*;
pub use endpoint::*;
pub use wire::*;
// `serve`/`dial` expose only crate-internal helpers (accept + dial
// loops); re-export them at crate visibility so siblings resolve them.
pub(crate) use dial::*;
pub(crate) use serve::*;
