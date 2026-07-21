//! `Node` — the unified, topic-shaped entry point.
//!
//! One `Node` per process. Internally:
//!
//! * Owns a single [`iroh::Endpoint`] (cross-host transport).
//! * Hosts a single accept loop that dispatches incoming iroh bi
//!   streams to registered publishers' broadcast queues, by topic
//!   name.
//! * Caches outbound iroh `Connection`s, one per peer, reused
//!   across topics.
//! * For same-host routing, asks the local SHM backend whether a service
//!   exists for the topic on this host; if so, attaches locally
//!   via SHM. If not, dials the peer via iroh.
//!
//! User-facing API:
//!
//! ```no_run
//! use peerbus::Node;
//!
//! # fn run() -> peerbus::Result<()> {
//! # #[datapod::datapod]
//! # struct Pose { x: f32, y: f32, yaw: f32 }
//! let node = Node::builder().no_relay().bind()?;
//! let mut pubr = node.publisher::<Pose>("rover/pose")?;
//! pubr.send(&Pose { x: 0.0, y: 0.0, yaw: 0.0 })?;
//! # Ok(()) }
//! ```

use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use iroh::endpoint::{Connection, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use tokio::task::JoinHandle;

use crate::chunk::{
    CHUNK_FRAME_FLAG, CHUNK_FRAME_LEN_MASK, CHUNK_HEADER_LEN, Reassembler, make_chunk_payload,
    parse_chunk_payload,
};
use crate::error::{Error, Result};
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::local::{Loan, Sample};
use crate::local::{
    LocalAckServer, LocalAnsServer, LocalPipServer, LocalPipService, LocalPutAckService,
    LocalQueAnsService, LocalReqResService, LocalReqServer,
};
use crate::pip::{PIP_KIND_DONE, PIP_KIND_ITEM};
use crate::putack::{PUT_KIND_DONE, PUT_KIND_ITEM};
use crate::qos::{DeliveryPolicy, TopicQos};
use crate::queans::{ANS_KIND_DONE, ANS_KIND_ITEM};
use crate::remote::runtime;
use crate::remote::{
    HANDSHAKE_MAGIC, HANDSHAKE_VERSION, ITEM_HANDSHAKE_VERSION_CHUNKED, MAX_PAYLOAD_LEN, PIP_MAGIC,
    PUBSUB_HANDSHAKE_VERSION_QOS, PUTACK_MAGIC, QUEANS_MAGIC, REQRESP_MAGIC,
    parse_pubsub_handshake_tail_qos,
};
use crate::transport::{fnv1a64, wire_type_hash};
use crate::{qb_debug, qb_info, qb_warn};

const DEFAULT_ALPN: &[u8] = b"peerbus/1";
const DEFAULT_BROADCAST_CAPACITY: usize = 256;
const IDENTITY_DERIVATION_TAG: &[u8] = b"peerbus/v1/identity";
/// QUIC application close code used when an inbound connection is
/// refused by the peer ACL. Distinct from 0 (graceful close) so the
/// dialing side can tell "rejected" from "went away".
const ACL_REJECT_CODE: u32 = 1;
/// Close reason sent to a peer refused by the ACL.
const ACL_REJECT_REASON: &[u8] = b"peerbus: inbound peer not allowed";
const PUBSUB_DATAGRAM_MAGIC: &[u8; 4] = b"QBD1";
const PUBSUB_DATAGRAM_HEADER_LEN: usize = 4 + 8;
static NEXT_REMOTE_REQ_ID: AtomicU64 = AtomicU64::new(0);

/// Initial backoff between reconnect attempts on the subscriber
/// loop. Doubles up to [`RECONNECT_BACKOFF_MAX`].
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Cap on the reconnect backoff between attempts.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(10);

mod accept;
mod builder;
mod core;
mod datagram;
mod frames;
mod identity;
mod peer;
mod pip;
mod pip_client;
mod pubsub;
mod putack;
mod putack_client;
mod queans;
mod queans_client;
mod reqres;
mod topic;
mod wire;

// Reconstruct the flat `node::*` namespace so `crate::node::<Item>` (and
// lib.rs's re-exports) keep resolving after the split. Modules that expose
// only crate-internal helpers are re-exported at crate visibility.
pub use builder::*;
pub use core::*;
pub use peer::*;
pub use pip::*;
pub use pip_client::*;
pub use pubsub::*;
pub use putack::*;
pub use putack_client::*;
pub use queans::*;
pub use queans_client::*;
pub use reqres::*;
pub use topic::*;

pub(crate) use accept::*;
pub(crate) use datagram::*;
pub(crate) use frames::*;
pub(crate) use identity::*;
pub(crate) use wire::*;
