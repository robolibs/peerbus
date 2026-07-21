use super::*;

// ---- handles ----

pub struct PeerbusNode {
    pub(crate) node: Node,
}

/// Optional node construction settings. NULL string pointers mean "unset";
/// zero numeric limits mean "use the Rust default".
///
/// Inbound connections are denied by default: unless `allowed_peers` is
/// non-empty or `allow_any_peer` is true, every incoming connection is
/// refused right after the QUIC handshake.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusNodeConfig {
    pub identity: *const c_char,
    pub no_relay: bool,
    pub system_did: *const c_char,
    pub allowed_peers: *const *const c_char,
    pub allowed_peers_len: usize,
    /// Accept connections from ANY peer that knows the ALPN. Insecure;
    /// only appropriate on a trusted network. Logs a WARN at bind.
    pub allow_any_peer: bool,
    pub max_payload_bytes: usize,
    pub history_depth: u32,
    pub subscriber_buffer: u32,
    pub max_publishers: u32,
    pub max_subscribers: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusNodeStats {
    pub publisher_topics: usize,
    pub cached_peers: usize,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusPublisherStats {
    pub published: u64,
    pub remote_dropped: u64,
    pub stale_dropped: u64,
    pub bytes_sent: u64,
    pub send_errors: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusSubscriberStats {
    pub received: u64,
    pub disconnects: u64,
    pub stale_dropped: u64,
    pub incomplete_dropped: u64,
    pub bytes_received: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerbusItemStats {
    pub messages_out: u64,
    pub messages_in: u64,
    pub bytes_out: u64,
    pub bytes_in: u64,
    pub errors: u64,
}

pub struct PeerbusPublisher {
    pub(crate) publisher: Publisher<RawMsg>,
}

pub struct PeerbusSubscriber {
    pub(crate) subscriber: Subscriber<RawMsg>,
}

pub struct PeerbusSample {
    pub(crate) sample: NodeSample<RawMsg>,
}

pub struct PeerbusDatapodPublisher {
    pub(crate) publisher: Publisher<DatapodMsg>,
}

pub struct PeerbusDatapodSubscriber {
    pub(crate) subscriber: Subscriber<DatapodMsg>,
}

pub struct PeerbusDatapodSample {
    pub(crate) sample: NodeSample<DatapodMsg>,
}

pub struct PeerbusDatapodMessage {
    pub(crate) type_hash: u64,
    pub(crate) wire: Vec<u8>,
}

pub struct PeerbusDatapodMessages {
    pub(crate) messages: Vec<PeerbusDatapodMessage>,
}

/// Preferred three-letter generic-datapod que/ans answer list handle name.
///
/// `PeerbusDatapodMessages` remains the shared finite-list storage type for
/// compatibility with earlier binding code.
pub type PeerbusDatapodAnswers = PeerbusDatapodMessages;

pub struct PeerbusPeerPathDiagnostics {
    pub(crate) diag: crate::PeerPathDiagnostics,
}

/// An owned, received message (kind tag + payload bytes).
pub struct PeerbusMessage {
    pub(crate) kind: u64,
    pub(crate) data: Vec<u8>,
}

pub struct PeerbusReqClient {
    pub(crate) client: ReqClient<RawMsg, RawMsg>,
}

pub struct PeerbusReqServer {
    pub(crate) server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusDatapodReqClient {
    pub(crate) client: ReqClient<DatapodMsg, DatapodMsg>,
}

pub struct PeerbusDatapodReqServer {
    pub(crate) server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodQueClient {
    pub(crate) client: QueClient<DatapodMsg, DatapodMsg>,
}

pub struct PeerbusDatapodAnsServer {
    pub(crate) server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodPutClient {
    pub(crate) client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodAckServer {
    pub(crate) server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodPipClient {
    pub(crate) client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusDatapodPipServer {
    pub(crate) server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
}

pub struct PeerbusQueClient {
    pub(crate) client: QueClient<RawMsg, RawMsg>,
}

pub struct PeerbusAnsServer {
    pub(crate) server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusPutClient {
    pub(crate) client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
}

pub struct PeerbusAckServer {
    pub(crate) server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusPuts {
    pub(crate) server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PutAckToken>,
    pub(crate) first: Option<PeerbusMessage>,
    pub(crate) done: bool,
}

pub struct PeerbusDatapodPuts {
    pub(crate) server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PutAckToken>,
    pub(crate) first: Option<PeerbusDatapodMessage>,
    pub(crate) done: bool,
}

pub struct PeerbusDatapodPutUpload {
    pub(crate) client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PutUploadToken>,
}

pub struct PeerbusPipClient {
    pub(crate) client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
}

pub struct PeerbusPipServer {
    pub(crate) server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
}

pub struct PeerbusPendingReq {
    pub(crate) server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
    pub(crate) reply: Option<ReqReplyToken>,
    pub(crate) request: PeerbusMessage,
}

pub struct PeerbusPendingDatapodReq {
    pub(crate) server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) reply: Option<ReqReplyToken>,
    pub(crate) request: PeerbusDatapodMessage,
}

pub struct PeerbusPendingDatapodQue {
    pub(crate) server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) answers: Option<AnsReplyToken>,
    pub(crate) request: PeerbusDatapodMessage,
}

pub struct PeerbusPendingQue {
    pub(crate) server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
    pub(crate) answers: Option<AnsReplyToken>,
    pub(crate) request: PeerbusMessage,
}

pub struct PeerbusPutUpload {
    pub(crate) client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PutUploadToken>,
}

/// Preferred three-letter put/ack sender handle name.
///
/// `PeerbusPutUpload` remains as a compatibility alias in the C ABI.
pub type PeerbusPutSender = PeerbusPutUpload;

/// Preferred three-letter generic-datapod put/ack sender handle name.
///
/// `PeerbusDatapodPutUpload` remains as a compatibility alias in the C ABI.
pub type PeerbusDatapodPutSender = PeerbusDatapodPutUpload;

pub struct PeerbusPip {
    pub(crate) client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PipSessionToken>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
}

pub struct PeerbusDatapodPip {
    pub(crate) client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PipSessionToken>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
}

pub struct PeerbusPendingPip {
    pub(crate) server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PipServerToken>,
    pub(crate) first: Option<PeerbusMessage>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
}

pub struct PeerbusPendingDatapodPip {
    pub(crate) server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PipServerToken>,
    pub(crate) first: Option<PeerbusDatapodMessage>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
}

/// Passed to a request handler so it can set the response.
pub struct PeerbusResponder {
    pub(crate) kind: u64,
    pub(crate) data: Vec<u8>,
    pub(crate) set: bool,
}

/// Request handler callback: receives the request `kind` + bytes and the
/// `responder` to fill via [`peerbus_responder_set`].
pub type PeerbusReqHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        kind: u64,
        data: *const u8,
        len: usize,
        responder: *mut PeerbusResponder,
    ),
>;

/// Delivery behavior for C QoS: reliable, in-order delivery.
pub const PEERBUS_DELIVERY_RELIABLE: u32 = 0;
/// Delivery behavior for C QoS: keep only the latest value.
pub const PEERBUS_DELIVERY_LATEST: u32 = 1;
/// Delivery behavior for C QoS: best-effort, may drop.
pub const PEERBUS_DELIVERY_BEST_EFFORT: u32 = 2;

/// C mirror of [`TopicQos`]. Zero fields are allowed; use
/// [`peerbus_topic_qos_reliable`], [`peerbus_topic_qos_latest`], or
/// [`peerbus_topic_qos_best_effort`] for canonical defaults.
///
/// `delivery` is a plain integer (not a Rust enum) so that an out-of-range
/// value from C is never undefined behavior: it is validated at every use
/// (`0..=2`, see [`PEERBUS_DELIVERY_RELIABLE`] and friends) and an invalid
/// value fails the call via [`peerbus_last_error_message`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusTopicQos {
    pub delivery: u32,
    pub max_message_bytes: usize,
    pub max_inflight_bytes: usize,
    pub chunk_bytes: usize,
    pub subscriber_queue: usize,
    pub priority: u8,
}

impl From<TopicQos> for PeerbusTopicQos {
    fn from(value: TopicQos) -> Self {
        let delivery = match value.delivery {
            DeliveryPolicy::Reliable => PEERBUS_DELIVERY_RELIABLE,
            DeliveryPolicy::Latest => PEERBUS_DELIVERY_LATEST,
            DeliveryPolicy::BestEffort => PEERBUS_DELIVERY_BEST_EFFORT,
        };
        Self {
            delivery,
            max_message_bytes: value.max_message_bytes,
            max_inflight_bytes: value.max_inflight_bytes,
            chunk_bytes: value.chunk_bytes,
            subscriber_queue: value.subscriber_queue,
            priority: value.priority,
        }
    }
}

/// Fallible conversion from the C QoS mirror to [`TopicQos`]. Rejects an
/// out-of-range `delivery` discriminant (must be `0..=2`) rather than
/// materializing an invalid enum, which would be undefined behavior.
pub(crate) fn topic_qos_from_c(value: PeerbusTopicQos) -> Result<TopicQos, ()> {
    let mut qos = match value.delivery {
        PEERBUS_DELIVERY_RELIABLE => TopicQos::reliable(),
        PEERBUS_DELIVERY_LATEST => TopicQos::latest(),
        PEERBUS_DELIVERY_BEST_EFFORT => TopicQos::best_effort(),
        _ => {
            set_last_error(
                "invalid QoS delivery policy (expected 0=RELIABLE, 1=LATEST, 2=BEST_EFFORT)",
            );
            return Err(());
        }
    };
    if value.max_message_bytes != 0 {
        qos.max_message_bytes = value.max_message_bytes;
    }
    if value.max_inflight_bytes != 0 {
        qos.max_inflight_bytes = value.max_inflight_bytes;
    }
    if value.chunk_bytes != 0 {
        qos.chunk_bytes = value.chunk_bytes;
    }
    if value.subscriber_queue != 0 {
        qos.subscriber_queue = value.subscriber_queue;
    }
    qos.priority = value.priority;
    Ok(qos)
}

/// Test-only shim exposing the private [`topic_qos_from_c`] conversion so the
/// FFI test suite can assert the zero-fill-to-policy-defaults contract and the
/// out-of-range rejection without standing up a live transport. Returns
/// `None` for an invalid `delivery`. Not part of the stable C ABI (it is a
/// plain Rust `fn`, so cbindgen does not emit it).
#[doc(hidden)]
pub fn __peerbus_topic_qos_from_c(value: PeerbusTopicQos) -> Option<TopicQos> {
    topic_qos_from_c(value).ok()
}

/// Borrowed message input used by finite C convenience APIs.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusRawMessage {
    pub kind: u64,
    pub data: PeerbusBytes,
}

/// Borrowed datapod message input used by generic datapod C convenience APIs.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PeerbusDatapodRawMessage {
    pub type_hash: u64,
    pub wire: PeerbusBytes,
}

/// Owned message list returned by que/ans and pip convenience calls.
pub struct PeerbusMessages {
    pub(crate) messages: Vec<PeerbusMessage>,
}

/// Preferred three-letter que/ans answer list handle name.
///
/// `PeerbusMessages` remains the shared finite-list storage type for
/// compatibility with earlier binding code.
pub type PeerbusAnswers = PeerbusMessages;

/// Passed to a que/ans handler so it can append answer items.
pub struct PeerbusAnsResponder {
    pub(crate) messages: Vec<RawMsg>,
}

/// Passed to put/ack or pip handlers so they can build the final reply list.
pub struct PeerbusMessageResponder {
    pub(crate) messages: Vec<RawMsg>,
}

pub type PeerbusAnsHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        kind: u64,
        data: *const u8,
        len: usize,
        responder: *mut PeerbusAnsResponder,
    ),
>;

pub type PeerbusAckHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        items: *const PeerbusMessages,
        responder: *mut PeerbusResponder,
    ),
>;

pub type PeerbusPipHandler = Option<
    unsafe extern "C" fn(
        ctx: *mut c_void,
        items: *const PeerbusMessages,
        responder: *mut PeerbusMessageResponder,
    ),
>;

