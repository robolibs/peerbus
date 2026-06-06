//! Python bindings for quicbit (pyo3).
//!
//! The binding surface intentionally transports opaque byte messages:
//! a `kind: int` tag plus `bytes`, carried by [`crate::RawMsg`]. Concrete
//! datapod Python classes from datapod 0.3 can sit above this by using
//! `kind = TYPE_HASH` and their own wire bytes.

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pyo3::exceptions::{PyBufferError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyMemoryView, PyModule};

use crate::{
    AckServer, AnsReplyToken, AnsServer, DatapodMsg, DeliveryPolicy, LocalConfig, Node, NodeSample,
    PipClient, PipServer, PipServerToken, PipSessionToken, Publisher, PutClient, PutUploadToken,
    QueClient, RawMsg, ReqClient, ReqReplyToken, ReqServer, Subscriber, TopicQos,
};

fn py_err(err: crate::Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn lock_py<'a, T>(mutex: &'a Mutex<T>, name: &str) -> PyResult<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| PyRuntimeError::new_err(format!("{name} mutex poisoned")))
}

fn timeout(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms)
}

fn raw_from_py(value: &Bound<'_, PyAny>) -> PyResult<RawMsg> {
    if let Ok((kind, data)) = value.extract::<(u64, Vec<u8>)>() {
        Ok(RawMsg::new(kind, &data))
    } else {
        match value.extract::<Vec<u8>>() {
            Ok(data) => Ok(RawMsg::new(0, &data)),
            Err(_) => raw_from_pod(value),
        }
    }
}

fn raw_vec_from_py(value: &Bound<'_, PyAny>) -> PyResult<Vec<RawMsg>> {
    if value.is_none() {
        return Ok(Vec::new());
    }
    if let Ok((kind, data)) = value.extract::<(u64, Vec<u8>)>() {
        return Ok(vec![RawMsg::new(kind, &data)]);
    }
    if let Ok(data) = value.extract::<Vec<u8>>() {
        return Ok(vec![RawMsg::new(0, &data)]);
    }
    if let Ok(items) = value.extract::<Vec<(u64, Vec<u8>)>>() {
        return Ok(items
            .into_iter()
            .map(|(kind, data)| RawMsg::new(kind, &data))
            .collect());
    }
    if let Ok(items) = value.extract::<Vec<Vec<u8>>>() {
        return Ok(items
            .into_iter()
            .map(|data| RawMsg::new(0, &data))
            .collect());
    }
    if let Ok(items) = value.extract::<Vec<Py<PyAny>>>() {
        let py = value.py();
        return items
            .into_iter()
            .map(|item| raw_from_py(item.bind(py)))
            .collect();
    }
    Ok(vec![raw_from_pod(value)?])
}

fn sample_tuple<'py>(py: Python<'py>, kind: u64, payload: &[u8]) -> (u64, Bound<'py, PyBytes>) {
    (kind, PyBytes::new(py, payload))
}

/// Fill a read-only Python buffer view from bytes owned by `owner`.
///
/// The data must remain valid for as long as `owner` is alive. For the
/// zero-copy sample views below, `owner` is the Python sample object holding the
/// Rust `NodeSample`, which pins the SHM slot until the Python object and all
/// memoryviews are released.
unsafe fn fill_readonly_buffer(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    data_ptr: *const u8,
    data_len: usize,
    owner: Bound<'_, PyAny>,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("buffer view is null"));
    }
    if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
        return Err(PyBufferError::new_err("quicbit sample views are read-only"));
    }

    unsafe {
        (*view).obj = owner.into_ptr();
        (*view).buf = data_ptr as *mut c_void;
        (*view).len = data_len as isize;
        (*view).readonly = 1;
        (*view).itemsize = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            CString::new("B")
                .expect("static buffer format contains no NULs")
                .into_raw()
        } else {
            ptr::null_mut()
        };
        (*view).ndim = 1;
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            &mut (*view).len
        } else {
            ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            &mut (*view).itemsize
        } else {
            ptr::null_mut()
        };
        (*view).suboffsets = ptr::null_mut();
        (*view).internal = ptr::null_mut();
    }
    Ok(())
}

unsafe fn release_readonly_buffer(view: *mut ffi::Py_buffer) {
    unsafe {
        if !view.is_null() && !(*view).format.is_null() {
            drop(CString::from_raw((*view).format));
            (*view).format = ptr::null_mut();
        }
    }
}

fn raw_from_pod(value: &Bound<'_, PyAny>) -> PyResult<RawMsg> {
    let wire = value.call_method0("to_wire_message").map_err(|_| {
        PyRuntimeError::new_err(
            "expected bytes, (kind, bytes), or object with to_wire_message() -> (kind, bytes)",
        )
    })?;
    let (kind, data) = wire.extract::<(u64, Vec<u8>)>()?;
    Ok(RawMsg::new(kind, &data))
}

fn datapod_msg_from_py(value: &Bound<'_, PyAny>) -> PyResult<DatapodMsg> {
    let wire = value.call_method0("to_wire_message").map_err(|_| {
        PyRuntimeError::new_err(
            "expected datapod object with to_wire_message() -> (type_hash, bytes)",
        )
    })?;
    let (type_hash, wire) = wire.extract::<(u64, Vec<u8>)>()?;
    Ok(DatapodMsg::new(type_hash, wire))
}

fn decode_datapod(
    py: Python<'_>,
    datapod_type: &Bound<'_, PyAny>,
    kind: u64,
    payload: &[u8],
) -> PyResult<Py<PyAny>> {
    let data = PyBytes::new(py, payload);
    let decoded = if datapod_type.hasattr("from_wire_message")? {
        datapod_type.call_method1("from_wire_message", (kind, data))?
    } else if datapod_type.hasattr("from_wire")? {
        let expected = datapod_type
            .getattr("TYPE_HASH")
            .and_then(|value| value.extract::<u64>())
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "datapod type with from_wire(header, payload) must expose TYPE_HASH",
                )
            })?;
        if kind != expected {
            return Err(PyRuntimeError::new_err(format!(
                "wrong datapod kind: got {kind}, expected {expected}"
            )));
        }
        datapod_type.call_method1("from_wire", (data, PyBytes::new(py, &[])))?
    } else {
        return Err(PyRuntimeError::new_err(
            "datapod type must provide from_wire_message(kind, data) or from_wire(header, payload)",
        ));
    };
    Ok(decoded.unbind())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("endpoint address hex has odd length".to_string());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = nibble(pair[0]).ok_or_else(|| "invalid endpoint address hex".to_string())?;
        let lo = nibble(pair[1]).ok_or_else(|| "invalid endpoint address hex".to_string())?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn encode_endpoint_addr(addr: &iroh::EndpointAddr) -> PyResult<String> {
    let bytes = postcard::to_stdvec(addr)
        .map_err(|e| PyRuntimeError::new_err(format!("encode endpoint addr: {e}")))?;
    Ok(hex_encode(&bytes))
}

fn decode_endpoint_addr(encoded: &str) -> PyResult<iroh::EndpointAddr> {
    let bytes = hex_decode(encoded).map_err(PyRuntimeError::new_err)?;
    postcard::from_bytes(&bytes)
        .map_err(|e| PyRuntimeError::new_err(format!("decode endpoint addr: {e}")))
}

fn item_stats_dict(py: Python<'_>, stats: crate::ItemStats) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("messages_out", stats.messages_out)?;
    dict.set_item("messages_in", stats.messages_in)?;
    dict.set_item("bytes_out", stats.bytes_out)?;
    dict.set_item("bytes_in", stats.bytes_in)?;
    dict.set_item("errors", stats.errors)?;
    Ok(dict.into())
}

#[pyclass(name = "DeliveryPolicy")]
#[derive(Clone, Copy)]
pub struct PyDeliveryPolicy {
    inner: DeliveryPolicy,
}

#[pymethods]
impl PyDeliveryPolicy {
    #[classattr]
    const RELIABLE: Self = Self {
        inner: DeliveryPolicy::Reliable,
    };

    #[classattr]
    const LATEST: Self = Self {
        inner: DeliveryPolicy::Latest,
    };

    #[classattr]
    const BEST_EFFORT: Self = Self {
        inner: DeliveryPolicy::BestEffort,
    };

    fn __repr__(&self) -> &'static str {
        match self.inner {
            DeliveryPolicy::Reliable => "DeliveryPolicy.RELIABLE",
            DeliveryPolicy::Latest => "DeliveryPolicy.LATEST",
            DeliveryPolicy::BestEffort => "DeliveryPolicy.BEST_EFFORT",
        }
    }
}

#[pyclass(name = "TopicQos")]
#[derive(Clone, Copy)]
pub struct PyTopicQos {
    inner: TopicQos,
}

impl PyTopicQos {
    fn from_kind(kind: DeliveryPolicy) -> Self {
        let inner = match kind {
            DeliveryPolicy::Reliable => TopicQos::reliable(),
            DeliveryPolicy::Latest => TopicQos::latest(),
            DeliveryPolicy::BestEffort => TopicQos::best_effort(),
        };
        Self { inner }
    }

    fn with_options(
        mut self,
        max_message_bytes: Option<usize>,
        max_inflight_bytes: Option<usize>,
        chunk_bytes: Option<usize>,
        subscriber_queue: Option<usize>,
        priority: Option<u8>,
    ) -> Self {
        if let Some(value) = max_message_bytes {
            self.inner = self.inner.with_max_message_bytes(value);
        }
        if let Some(value) = max_inflight_bytes {
            self.inner = self.inner.with_max_inflight_bytes(value);
        }
        if let Some(value) = chunk_bytes {
            self.inner = self.inner.with_chunk_bytes(value);
        }
        if let Some(value) = subscriber_queue {
            self.inner = self.inner.with_subscriber_queue(value);
        }
        if let Some(value) = priority {
            self.inner = self.inner.with_priority(value);
        }
        self
    }
}

fn qos_value(qos: Option<PyRef<'_, PyTopicQos>>) -> TopicQos {
    qos.map(|q| q.inner).unwrap_or_default()
}

#[pymethods]
impl PyTopicQos {
    #[new]
    #[pyo3(signature = (
        delivery=None,
        max_message_bytes=None,
        max_inflight_bytes=None,
        chunk_bytes=None,
        subscriber_queue=None,
        priority=None
    ))]
    fn new(
        delivery: Option<PyRef<'_, PyDeliveryPolicy>>,
        max_message_bytes: Option<usize>,
        max_inflight_bytes: Option<usize>,
        chunk_bytes: Option<usize>,
        subscriber_queue: Option<usize>,
        priority: Option<u8>,
    ) -> Self {
        Self::from_kind(
            delivery
                .map(|d| d.inner)
                .unwrap_or(DeliveryPolicy::Reliable),
        )
        .with_options(
            max_message_bytes,
            max_inflight_bytes,
            chunk_bytes,
            subscriber_queue,
            priority,
        )
    }

    #[staticmethod]
    #[pyo3(signature = (
        max_message_bytes=None,
        max_inflight_bytes=None,
        chunk_bytes=None,
        subscriber_queue=None,
        priority=None
    ))]
    fn reliable(
        max_message_bytes: Option<usize>,
        max_inflight_bytes: Option<usize>,
        chunk_bytes: Option<usize>,
        subscriber_queue: Option<usize>,
        priority: Option<u8>,
    ) -> Self {
        Self::from_kind(DeliveryPolicy::Reliable).with_options(
            max_message_bytes,
            max_inflight_bytes,
            chunk_bytes,
            subscriber_queue,
            priority,
        )
    }

    #[staticmethod]
    #[pyo3(signature = (
        max_message_bytes=None,
        max_inflight_bytes=None,
        chunk_bytes=None,
        subscriber_queue=None,
        priority=None
    ))]
    fn latest(
        max_message_bytes: Option<usize>,
        max_inflight_bytes: Option<usize>,
        chunk_bytes: Option<usize>,
        subscriber_queue: Option<usize>,
        priority: Option<u8>,
    ) -> Self {
        Self::from_kind(DeliveryPolicy::Latest).with_options(
            max_message_bytes,
            max_inflight_bytes,
            chunk_bytes,
            subscriber_queue,
            priority,
        )
    }

    #[staticmethod]
    #[pyo3(signature = (
        max_message_bytes=None,
        max_inflight_bytes=None,
        chunk_bytes=None,
        subscriber_queue=None,
        priority=None
    ))]
    fn best_effort(
        max_message_bytes: Option<usize>,
        max_inflight_bytes: Option<usize>,
        chunk_bytes: Option<usize>,
        subscriber_queue: Option<usize>,
        priority: Option<u8>,
    ) -> Self {
        Self::from_kind(DeliveryPolicy::BestEffort).with_options(
            max_message_bytes,
            max_inflight_bytes,
            chunk_bytes,
            subscriber_queue,
            priority,
        )
    }

    #[getter]
    fn delivery(&self) -> PyDeliveryPolicy {
        PyDeliveryPolicy {
            inner: self.inner.delivery,
        }
    }

    #[getter]
    fn max_message_bytes(&self) -> usize {
        self.inner.max_message_bytes
    }

    #[getter]
    fn max_inflight_bytes(&self) -> usize {
        self.inner.max_inflight_bytes
    }

    #[getter]
    fn chunk_bytes(&self) -> usize {
        self.inner.chunk_bytes
    }

    #[getter]
    fn subscriber_queue(&self) -> usize {
        self.inner.subscriber_queue
    }

    #[getter]
    fn priority(&self) -> u8 {
        self.inner.priority
    }
}

#[pyclass(name = "Message")]
#[derive(Clone)]
pub struct PyMessage {
    #[pyo3(get)]
    kind: u64,
    data: Vec<u8>,
}

#[pymethods]
impl PyMessage {
    #[new]
    #[pyo3(signature = (data=None, kind=0))]
    fn new(data: Option<Vec<u8>>, kind: u64) -> Self {
        Self {
            kind,
            data: data.unwrap_or_default(),
        }
    }

    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    fn as_tuple<'py>(&self, py: Python<'py>) -> (u64, Bound<'py, PyBytes>) {
        sample_tuple(py, self.kind, &self.data)
    }

    fn to_wire_message<'py>(&self, py: Python<'py>) -> (u64, Bound<'py, PyBytes>) {
        self.as_tuple(py)
    }

    #[staticmethod]
    fn from_datapod(py: Python<'_>, value: Py<PyAny>) -> PyResult<Self> {
        let raw = raw_from_pod(value.bind(py))?;
        Ok(Self {
            kind: raw.kind,
            data: raw.data,
        })
    }

    fn as_datapod(&self, py: Python<'_>, datapod_type: Py<PyAny>) -> PyResult<Py<PyAny>> {
        decode_datapod(py, datapod_type.bind(py), self.kind, &self.data)
    }

    fn __repr__(&self) -> String {
        format!("Message(kind={}, len={})", self.kind, self.data.len())
    }
}

#[pyclass(name = "Node")]
pub struct PyNode {
    node: Node,
}

#[pymethods]
impl PyNode {
    #[new]
    #[pyo3(signature = (
        identity=None,
        no_relay=false,
        system_did=None,
        max_payload_bytes=None,
        history_depth=None,
        subscriber_buffer=None,
        max_publishers=None,
        max_subscribers=None
    ))]
    fn new(
        identity: Option<String>,
        no_relay: bool,
        system_did: Option<String>,
        max_payload_bytes: Option<usize>,
        history_depth: Option<u32>,
        subscriber_buffer: Option<u32>,
        max_publishers: Option<u32>,
        max_subscribers: Option<u32>,
    ) -> PyResult<Self> {
        let mut builder = Node::builder();
        if let Some(id) = identity {
            builder = builder.identity(id);
        }
        if no_relay {
            builder = builder.no_relay();
        }
        if let Some(did) = system_did {
            builder = builder.system_did(did);
        }
        if max_payload_bytes.is_some()
            || history_depth.is_some()
            || subscriber_buffer.is_some()
            || max_publishers.is_some()
            || max_subscribers.is_some()
        {
            let mut cfg = LocalConfig::default();
            if let Some(value) = max_payload_bytes {
                cfg.max_payload_bytes = value;
            }
            if let Some(value) = history_depth {
                cfg.history_depth = value;
            }
            if let Some(value) = subscriber_buffer {
                cfg.subscriber_buffer = value;
            }
            if let Some(value) = max_publishers {
                cfg.max_publishers = value;
            }
            if let Some(value) = max_subscribers {
                cfg.max_subscribers = value;
            }
            builder = builder.local_config(cfg);
        }
        let node = builder.bind().map_err(py_err)?;
        Ok(Self { node })
    }

    /// This node's identity as a `did:key:z6Mk…` string.
    fn did_key(&self) -> String {
        self.node.endpoint_did_key()
    }

    fn system_did(&self) -> Option<String> {
        self.node.system_did().map(ToOwned::to_owned)
    }

    /// This node's full iroh endpoint address as a hex-encoded
    /// postcard blob. Pass this string to peer-addressed APIs,
    /// add_topic_route(), or add_system_peer() to avoid discovery.
    fn endpoint_addr(&self) -> PyResult<String> {
        encode_endpoint_addr(&self.node.endpoint_addr())
    }

    /// Add `(system_did, topic) -> endpoint_addr` fallback route.
    fn add_topic_route(&self, topic: &str, endpoint_addr: &str) -> PyResult<()> {
        self.node
            .add_topic_route(topic, decode_endpoint_addr(endpoint_addr)?)
            .map_err(py_err)
    }

    /// Add a peer fallback for this node's configured system DID.
    fn add_system_peer(&self, endpoint_addr: &str) -> PyResult<()> {
        self.node
            .add_system_peer(decode_endpoint_addr(endpoint_addr)?)
            .map_err(py_err)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = self.node.stats();
        let dict = PyDict::new(py);
        dict.set_item("publisher_topics", stats.publisher_topics)?;
        dict.set_item("cached_peers", stats.cached_peers)?;
        Ok(dict.into())
    }

    fn peer_path_diagnostics(
        &self,
        py: Python<'_>,
        endpoint_addr: &str,
    ) -> PyResult<Option<Py<PyDict>>> {
        let endpoint_addr = decode_endpoint_addr(endpoint_addr)?;
        let Some(diag) = self
            .node
            .peer_path_diagnostics(endpoint_addr)
            .map_err(py_err)?
        else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("peer", crate::did_key::endpoint_id_to_did_key(&diag.peer))?;
        dict.set_item("max_datagram_size", diag.max_datagram_size)?;
        dict.set_item(
            "datagram_send_buffer_space",
            diag.datagram_send_buffer_space,
        )?;
        let paths = PyList::empty(py);
        for path in diag.paths {
            let item = PyDict::new(py);
            item.set_item("path_id", path.path_id)?;
            item.set_item("remote_addr", path.remote_addr)?;
            item.set_item("selected", path.selected)?;
            item.set_item("is_ip", path.is_ip)?;
            item.set_item("is_relay", path.is_relay)?;
            item.set_item("rtt_ms", path.rtt.as_secs_f64() * 1000.0)?;
            item.set_item("current_mtu", path.current_mtu)?;
            item.set_item("cwnd", path.cwnd)?;
            item.set_item("lost_packets", path.lost_packets)?;
            paths.append(item)?;
        }
        dict.set_item("paths", paths)?;
        Ok(Some(dict.into()))
    }

    #[pyo3(signature = (topic, qos=None))]
    fn publisher(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyPublisher> {
        let publisher = self
            .node
            .publisher_with_qos::<RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyPublisher { publisher })
    }

    /// Generic datapod publisher. Accepts any Python datapod object with
    /// `to_wire_message() -> (TYPE_HASH, bytes)`.
    #[pyo3(signature = (topic, qos=None))]
    fn datapod_publisher(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodPublisher> {
        let publisher = self
            .node
            .publisher_with_qos::<DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodPublisher { publisher })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn subscriber(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PySubscriber> {
        let qos = qos_value(qos);
        let subscriber = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node.subscriber_with_qos::<RawMsg>(addr, topic, qos)
        } else {
            self.node.subscriber_with_qos::<RawMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PySubscriber { subscriber })
    }

    /// Generic datapod subscriber. `take(datapod.Type)` decodes with the
    /// datapod Python binding's `from_wire_message()`.
    #[pyo3(signature = (peer, topic, qos=None))]
    fn datapod_subscriber(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodSubscriber> {
        let qos = qos_value(qos);
        let subscriber = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .subscriber_with_qos::<DatapodMsg>(addr, topic, qos)
        } else {
            self.node
                .subscriber_with_qos::<DatapodMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyDatapodSubscriber { subscriber })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn subscribe(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PySubscriber> {
        let subscriber = self
            .node
            .subscribe_with_qos::<RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PySubscriber { subscriber })
    }

    /// System-DID topic-only generic datapod subscriber.
    #[pyo3(signature = (topic, qos=None))]
    fn datapod_subscribe(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodSubscriber> {
        let subscriber = self
            .node
            .subscribe_with_qos::<DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodSubscriber { subscriber })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn req_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyReqClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .req_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos)
        } else {
            self.node
                .req_client_with_qos::<RawMsg, RawMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyReqClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn req(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyReqClient> {
        let client = self
            .node
            .req_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyReqClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn req_server(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyReqServer> {
        let server = self
            .node
            .req_server_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyReqServer {
            server: Arc::new(Mutex::new(server)),
        })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn que_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyQueClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .que_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos)
        } else {
            self.node
                .que_client_with_qos::<RawMsg, RawMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyQueClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn que(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyQueClient> {
        let client = self
            .node
            .que_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyQueClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn ans_server(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyAnsServer> {
        let server = self
            .node
            .ans_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyAnsServer {
            server: Arc::new(Mutex::new(server)),
        })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn put_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyPutClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .put_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos)
        } else {
            self.node
                .put_client_with_qos::<RawMsg, RawMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyPutClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn put(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyPutClient> {
        let client = self
            .node
            .put_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyPutClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn ack_server(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyAckServer> {
        let server = self
            .node
            .ack_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyAckServer { server })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn pip_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyPipClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .pip_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos)
        } else {
            self.node
                .pip_client_with_qos::<RawMsg, RawMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyPipClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn pip(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyPipClient> {
        let client = self
            .node
            .pip_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyPipClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn pip_server(&self, topic: &str, qos: Option<PyRef<'_, PyTopicQos>>) -> PyResult<PyPipServer> {
        let server = self
            .node
            .pip_server_with_qos::<RawMsg, RawMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyPipServer {
            server: Arc::new(Mutex::new(server)),
        })
    }
}

#[pyclass(name = "Publisher")]
pub struct PyPublisher {
    publisher: Publisher<RawMsg>,
}

#[pymethods]
impl PyPublisher {
    /// Publish `data` with user tag `kind`.
    #[pyo3(signature = (data, kind=0))]
    fn send(&mut self, data: &[u8], kind: u64) -> PyResult<()> {
        self.publisher
            .send(&RawMsg::new(kind, data))
            .map(|_| ())
            .map_err(py_err)
    }

    fn send_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        self.publisher.send(&raw).map(|_| ()).map_err(py_err)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = self.publisher.stats();
        let dict = PyDict::new(py);
        dict.set_item("published", stats.published)?;
        dict.set_item("remote_dropped", stats.remote_dropped)?;
        dict.set_item("stale_dropped", stats.stale_dropped)?;
        dict.set_item("bytes_sent", stats.bytes_sent)?;
        dict.set_item("send_errors", stats.send_errors)?;
        Ok(dict.into())
    }
}

#[pyclass(name = "Subscriber")]
pub struct PySubscriber {
    subscriber: Subscriber<RawMsg>,
}

#[pymethods]
impl PySubscriber {
    /// Poll for the next sample. Returns `(kind, data)` or `None`.
    fn take<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(u64, Bound<'py, PyBytes>)>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(sample_tuple(
                py,
                sample.header().kind,
                sample.payload(),
            ))),
            None => Ok(None),
        }
    }

    /// Poll for the next sample as a Message object.
    fn take_message(&mut self) -> PyResult<Option<PyMessage>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(PyMessage {
                kind: sample.header().kind,
                data: sample.payload().to_vec(),
            })),
            None => Ok(None),
        }
    }

    fn take_pod(&mut self, py: Python<'_>, datapod_type: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(decode_datapod(
                py,
                datapod_type.bind(py),
                sample.header().kind,
                sample.payload(),
            )?)),
            None => Ok(None),
        }
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = self.subscriber.stats();
        let dict = PyDict::new(py);
        dict.set_item("received", stats.received)?;
        dict.set_item("disconnects", stats.disconnects)?;
        dict.set_item("stale_dropped", stats.stale_dropped)?;
        dict.set_item("incomplete_dropped", stats.incomplete_dropped)?;
        dict.set_item("bytes_received", stats.bytes_received)?;
        Ok(dict.into())
    }
}

#[pyclass(name = "DatapodPublisher")]
pub struct PyDatapodPublisher {
    publisher: Publisher<DatapodMsg>,
}

#[pymethods]
impl PyDatapodPublisher {
    /// Publish any datapod Python object.
    fn send(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value)?;
        self.publisher.send(&msg).map(|_| ()).map_err(py_err)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = self.publisher.stats();
        let dict = PyDict::new(py);
        dict.set_item("published", stats.published)?;
        dict.set_item("remote_dropped", stats.remote_dropped)?;
        dict.set_item("stale_dropped", stats.stale_dropped)?;
        dict.set_item("bytes_sent", stats.bytes_sent)?;
        dict.set_item("send_errors", stats.send_errors)?;
        Ok(dict.into())
    }
}

#[pyclass(name = "DatapodSubscriber")]
pub struct PyDatapodSubscriber {
    subscriber: Subscriber<DatapodMsg>,
}

/// Borrowed zero-copy datapod sample.
///
/// For local SHM this object pins the underlying slot until it and all
/// memoryviews derived from it are dropped. `memoryview(sample)` or
/// `sample.wire_view()` exposes the datapod wire bytes without copying.
#[pyclass(name = "DatapodSampleView")]
pub struct PyDatapodSampleView {
    sample: NodeSample<DatapodMsg>,
}

#[pymethods]
impl PyDatapodSampleView {
    #[getter]
    fn type_hash(&self) -> u64 {
        self.sample.header().type_hash
    }

    #[getter]
    fn wire_len(&self) -> usize {
        self.sample.payload().len()
    }

    fn __len__(&self) -> usize {
        self.sample.payload().len()
    }

    /// Return a Python memoryview of the datapod wire bytes.
    ///
    /// This is the zero-copy path. Keep this sample object alive while using
    /// the memoryview; holding it pins the SHM slot.
    fn wire_view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        let any = slf.into_any();
        PyMemoryView::from(&any)
    }

    /// Alias for `wire_view()`.
    fn payload_view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        Self::wire_view(slf)
    }

    /// Explicit copy helper for callers that really want owned bytes.
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.sample.payload())
    }

    /// Decode through the datapod Python class. This is convenient but copies.
    fn decode(&self, py: Python<'_>, datapod_type: Py<PyAny>) -> PyResult<Py<PyAny>> {
        decode_datapod(
            py,
            datapod_type.bind(py),
            self.sample.header().type_hash,
            self.sample.payload(),
        )
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let (data_ptr, data_len) = {
            let borrowed = slf.borrow();
            let data = borrowed.sample.payload();
            (data.as_ptr(), data.len())
        };
        unsafe { fill_readonly_buffer(view, flags, data_ptr, data_len, slf.into_any()) }
    }

    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        unsafe { release_readonly_buffer(view) };
    }
}

#[pymethods]
impl PyDatapodSubscriber {
    /// Poll and decode as `datapod_type`, e.g. `datapod.Grid`.
    fn take(&mut self, py: Python<'_>, datapod_type: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(decode_datapod(
                py,
                datapod_type.bind(py),
                sample.header().type_hash,
                sample.payload(),
            )?)),
            None => Ok(None),
        }
    }

    /// Poll without decoding. Returns `(TYPE_HASH, wire_bytes)` or `None`.
    fn take_wire<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(u64, Bound<'py, PyBytes>)>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(sample_tuple(
                py,
                sample.header().type_hash,
                sample.payload(),
            ))),
            None => Ok(None),
        }
    }

    /// Poll and return a borrowed zero-copy sample view.
    ///
    /// For same-host SHM this pins the slot until the returned object and any
    /// memoryviews made from it are released. Use this for high-throughput
    /// video and other large payloads instead of `take()` / `take_wire()`.
    fn take_view(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyDatapodSampleView>>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Py::new(py, PyDatapodSampleView { sample }).map(Some),
            None => Ok(None),
        }
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let stats = self.subscriber.stats();
        let dict = PyDict::new(py);
        dict.set_item("received", stats.received)?;
        dict.set_item("disconnects", stats.disconnects)?;
        dict.set_item("stale_dropped", stats.stale_dropped)?;
        dict.set_item("incomplete_dropped", stats.incomplete_dropped)?;
        dict.set_item("bytes_received", stats.bytes_received)?;
        Ok(dict.into())
    }
}

#[pyclass(name = "ReqClient")]
pub struct PyReqClient {
    client: ReqClient<RawMsg, RawMsg>,
}

#[pymethods]
impl PyReqClient {
    /// Send a request and block for the response `(kind, data)`.
    #[pyo3(signature = (data, kind=0))]
    fn call<'py>(
        &mut self,
        py: Python<'py>,
        data: &[u8],
        kind: u64,
    ) -> PyResult<(u64, Bound<'py, PyBytes>)> {
        let res = py
            .allow_threads(|| self.client.call(&RawMsg::new(kind, data)))
            .map_err(py_err)?;
        Ok(sample_tuple(py, res.header().kind, res.payload()))
    }

    #[pyo3(signature = (data, kind=0))]
    fn call_message(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<PyMessage> {
        let res = py
            .allow_threads(|| self.client.call(&RawMsg::new(kind, data)))
            .map_err(py_err)?;
        Ok(PyMessage {
            kind: res.header().kind,
            data: res.payload().to_vec(),
        })
    }

    fn call_pod(
        &mut self,
        py: Python<'_>,
        value: Py<PyAny>,
        response_type: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let raw = raw_from_pod(value.bind(py))?;
        let (kind, data) = py.allow_threads(|| {
            let res = self.client.call(&raw).map_err(py_err)?;
            Ok::<_, PyErr>((res.header().kind, res.payload().to_vec()))
        })?;
        decode_datapod(py, response_type.bind(py), kind, &data)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        item_stats_dict(py, self.client.stats())
    }
}

#[pyclass(name = "ReqServer")]
pub struct PyReqServer {
    server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PendingReq")]
pub struct PyPendingReq {
    server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
    reply: Option<ReqReplyToken>,
    request: PyMessage,
}

#[pymethods]
impl PyReqServer {
    /// Poll for one pending request. Returns `PendingReq` or `None`.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingReq>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "ReqServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (req, reply) = pending.into_parts();
                    return Ok(Some(PyPendingReq {
                        server: self.server.clone(),
                        reply: Some(reply),
                        request: PyMessage {
                            kind: req.header().kind,
                            data: req.payload().to_vec(),
                        },
                    }));
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    }

    /// Serve at most one request within `timeout_ms`.
    ///
    /// `handler` receives `(kind, data)` and returns either `bytes` (kind 0)
    /// or `(kind, bytes)`.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&mut self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "ReqServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (req, reply) = pending.into_parts();
                        let kind = req.header().kind;
                        let payload = req.payload().to_vec();
                        let response = Python::with_gil(|py| -> PyResult<RawMsg> {
                            let ret = handler.bind(py).call1((kind, PyBytes::new(py, &payload)))?;
                            raw_from_py(&ret)
                        })?;
                        let mut server = lock_py(&self.server, "ReqServer")?;
                        server.respond_pending(reply, &response).map_err(py_err)?;
                        return Ok(true);
                    }
                    None => {
                        if Instant::now() >= deadline {
                            return Ok(false);
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            }
        })
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let server = lock_py(&self.server, "ReqServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyPendingReq {
    #[getter]
    fn request(&self) -> PyMessage {
        self.request.clone()
    }

    #[getter]
    fn req_id(&self) -> Option<u64> {
        self.reply.map(|reply: ReqReplyToken| reply.req_id())
    }

    #[pyo3(signature = (data, kind=0))]
    fn reply(&mut self, data: &[u8], kind: u64) -> PyResult<()> {
        let reply = self
            .reply
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("request already replied"))?;
        let mut server = lock_py(&self.server, "ReqServer")?;
        server
            .respond_pending(reply, &RawMsg::new(kind, data))
            .map_err(py_err)
    }

    fn reply_message(&mut self, message: &PyMessage) -> PyResult<()> {
        let reply = self
            .reply
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("request already replied"))?;
        let mut server = lock_py(&self.server, "ReqServer")?;
        server
            .respond_pending(reply, &RawMsg::new(message.kind, &message.data))
            .map_err(py_err)
    }

    fn reply_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        let reply = self
            .reply
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("request already replied"))?;
        let mut server = lock_py(&self.server, "ReqServer")?;
        server.respond_pending(reply, &raw).map_err(py_err)
    }
}

#[pyclass(name = "QueClient")]
pub struct PyQueClient {
    client: QueClient<RawMsg, RawMsg>,
}

#[pymethods]
impl PyQueClient {
    /// Send one que and collect all ans items. Returns `[(kind, data), ...]`.
    #[pyo3(signature = (data, kind=0))]
    fn send<'py>(
        &mut self,
        py: Python<'py>,
        data: &[u8],
        kind: u64,
    ) -> PyResult<Vec<(u64, Bound<'py, PyBytes>)>> {
        let data = data.to_vec();
        let out = py.allow_threads(|| {
            let mut answers = self
                .client
                .send(&RawMsg::new(kind, &data))
                .map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(ans) = answers.next().map_err(py_err)? {
                out.push((ans.header().kind, ans.payload().to_vec()));
            }
            Ok::<_, PyErr>(out)
        })?;
        Ok(out
            .into_iter()
            .map(|(kind, data)| sample_tuple(py, kind, &data))
            .collect())
    }

    fn send_pod(
        &mut self,
        py: Python<'_>,
        value: Py<PyAny>,
        answer_type: Py<PyAny>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let raw = raw_from_pod(value.bind(py))?;
        let out = py.allow_threads(|| {
            let mut answers = self.client.send(&raw).map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(ans) = answers.next().map_err(py_err)? {
                out.push((ans.header().kind, ans.payload().to_vec()));
            }
            Ok::<_, PyErr>(out)
        })?;
        out.into_iter()
            .map(|(kind, data)| decode_datapod(py, answer_type.bind(py), kind, &data))
            .collect()
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        item_stats_dict(py, self.client.stats())
    }
}

#[pyclass(name = "AnsServer")]
pub struct PyAnsServer {
    server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PendingQue")]
pub struct PyPendingQue {
    request: PyMessage,
    answers: PyPendingAnswers,
}

struct PendingAnswersState {
    server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
    token: Option<AnsReplyToken>,
}

#[pyclass(name = "PendingAnswers")]
#[derive(Clone)]
pub struct PyPendingAnswers {
    state: Arc<Mutex<PendingAnswersState>>,
}

#[pymethods]
impl PyAnsServer {
    /// Poll for one pending que. Returns `PendingQue` or `None`.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingQue>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "AnsServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (que, token) = pending.into_parts();
                    return Ok(Some(PyPendingQue {
                        request: PyMessage {
                            kind: que.header().kind,
                            data: que.payload().to_vec(),
                        },
                        answers: PyPendingAnswers {
                            state: Arc::new(Mutex::new(PendingAnswersState {
                                server: self.server.clone(),
                                token: Some(token),
                            })),
                        },
                    }));
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    }

    /// Serve one que. `handler(kind, data)` returns None, bytes,
    /// `(kind, bytes)`, `[bytes, ...]`, or `[(kind, bytes), ...]`.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&mut self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "AnsServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (que, token) = pending.into_parts();
                        let kind = que.header().kind;
                        let payload = que.payload().to_vec();
                        let replies = Python::with_gil(|py| -> PyResult<Vec<RawMsg>> {
                            let ret = handler.bind(py).call1((kind, PyBytes::new(py, &payload)))?;
                            raw_vec_from_py(&ret)
                        })?;
                        {
                            let mut server = lock_py(&self.server, "AnsServer")?;
                            for reply in replies {
                                server.send_pending(token, &reply).map_err(py_err)?;
                            }
                            server.finish_pending(token).map_err(py_err)?;
                        }
                        return Ok(true);
                    }
                    None => {
                        if Instant::now() >= deadline {
                            return Ok(false);
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            }
        })
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let server = lock_py(&self.server, "AnsServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyPendingQue {
    #[getter]
    fn request(&self) -> PyMessage {
        self.request.clone()
    }

    #[getter]
    fn answers(&self) -> PyPendingAnswers {
        self.answers.clone()
    }
}

#[pymethods]
impl PyPendingAnswers {
    #[getter]
    fn req_id(&self) -> PyResult<Option<u64>> {
        let state = lock_py(&self.state, "PendingAnswers")?;
        Ok(state.token.map(|token: AnsReplyToken| token.req_id()))
    }

    #[pyo3(signature = (data, kind=0))]
    fn send(&mut self, data: &[u8], kind: u64) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingAnswers")?;
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("answer stream already finished"))?;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "AnsServer")?;
        server
            .send_pending(token, &RawMsg::new(kind, data))
            .map_err(py_err)
    }

    fn send_message(&mut self, message: &PyMessage) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingAnswers")?;
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("answer stream already finished"))?;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "AnsServer")?;
        server
            .send_pending(token, &RawMsg::new(message.kind, &message.data))
            .map_err(py_err)
    }

    fn send_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        let (server, token) = {
            let state = lock_py(&self.state, "PendingAnswers")?;
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("answer stream already finished"))?;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "AnsServer")?;
        server.send_pending(token, &raw).map_err(py_err)
    }

    fn finish(&mut self) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingAnswers")?;
            let token = state
                .token
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("answer stream already finished"))?;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "AnsServer")?;
        server.finish_pending(token).map_err(py_err)
    }
}

#[pyclass(name = "PutClient")]
pub struct PyPutClient {
    client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PutUpload")]
pub struct PyPutUpload {
    client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
    token: Option<PutUploadToken>,
}

#[pymethods]
impl PyPutClient {
    /// Open an interactive put upload handle.
    fn open(&self) -> PyResult<PyPutUpload> {
        let mut client = lock_py(&self.client, "PutClient")?;
        let token = client.open_upload().map_err(py_err)?;
        Ok(PyPutUpload {
            client: self.client.clone(),
            token: Some(token),
        })
    }

    /// Send a finite list of put items and return final `(kind, data)` ack.
    fn upload<'py>(
        &mut self,
        py: Python<'py>,
        items: Vec<(u64, Vec<u8>)>,
    ) -> PyResult<(u64, Bound<'py, PyBytes>)> {
        let (kind, data) = py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PutClient")?;
            let mut sender = client.open().map_err(py_err)?;
            for (kind, data) in items {
                sender.send(&RawMsg::new(kind, &data)).map_err(py_err)?;
            }
            let ack = sender.finish().map_err(py_err)?;
            Ok::<_, PyErr>((ack.header().kind, ack.payload().to_vec()))
        })?;
        Ok(sample_tuple(py, kind, &data))
    }

    fn upload_pod(
        &mut self,
        py: Python<'_>,
        items: Vec<Py<PyAny>>,
        ack_type: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let items: Vec<RawMsg> = items
            .into_iter()
            .map(|item| raw_from_pod(item.bind(py)))
            .collect::<PyResult<_>>()?;
        let (kind, data) = py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PutClient")?;
            let mut sender = client.open().map_err(py_err)?;
            for item in items {
                sender.send(&item).map_err(py_err)?;
            }
            let ack = sender.finish().map_err(py_err)?;
            Ok::<_, PyErr>((ack.header().kind, ack.payload().to_vec()))
        })?;
        decode_datapod(py, ack_type.bind(py), kind, &data)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let client = lock_py(&self.client, "PutClient")?;
        item_stats_dict(py, client.stats())
    }
}

#[pymethods]
impl PyPutUpload {
    #[getter]
    fn req_id(&self) -> PyResult<Option<u64>> {
        Ok(self.token.map(|token: PutUploadToken| token.req_id()))
    }

    #[pyo3(signature = (data, kind=0))]
    fn send(&mut self, data: &[u8], kind: u64) -> PyResult<()> {
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("put upload already finished"))?;
        let mut client = lock_py(&self.client, "PutClient")?;
        client
            .send_pending(token, &RawMsg::new(kind, data))
            .map_err(py_err)
    }

    fn send_message(&mut self, message: &PyMessage) -> PyResult<()> {
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("put upload already finished"))?;
        let mut client = lock_py(&self.client, "PutClient")?;
        client
            .send_pending(token, &RawMsg::new(message.kind, &message.data))
            .map_err(py_err)
    }

    fn send_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("put upload already finished"))?;
        let mut client = lock_py(&self.client, "PutClient")?;
        client.send_pending(token, &raw).map_err(py_err)
    }

    fn finish(&mut self, py: Python<'_>) -> PyResult<PyMessage> {
        let token = self
            .token
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("put upload already finished"))?;
        let (kind, data) = py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PutClient")?;
            let ack = client.finish_pending(token).map_err(py_err)?;
            Ok::<_, PyErr>((ack.header().kind, ack.payload().to_vec()))
        })?;
        Ok(PyMessage { kind, data })
    }

    fn finish_pod(&mut self, py: Python<'_>, ack_type: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let ack = self.finish(py)?;
        decode_datapod(py, ack_type.bind(py), ack.kind, &ack.data)
    }
}

#[pyclass(name = "AckServer")]
pub struct PyAckServer {
    server: AckServer<RawMsg, RawMsg>,
}

#[pymethods]
impl PyAckServer {
    /// Serve one upload. `handler(items)` gets `[(kind, data), ...]` and
    /// returns bytes or `(kind, bytes)` for the ack.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&mut self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                match self.server.take().map_err(py_err)? {
                    Some(mut puts) => {
                        let mut items = Vec::new();
                        while let Some(put) = puts.next().map_err(py_err)? {
                            items.push((put.header().kind, put.payload().to_vec()));
                        }
                        let ack = Python::with_gil(|py| -> PyResult<RawMsg> {
                            let ret = handler.bind(py).call1((items,))?;
                            raw_from_py(&ret)
                        })?;
                        puts.ack(&ack).map_err(py_err)?;
                        return Ok(true);
                    }
                    None => {
                        if Instant::now() >= deadline {
                            return Ok(false);
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            }
        })
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        item_stats_dict(py, self.server.stats())
    }
}

#[pyclass(name = "PipClient")]
pub struct PyPipClient {
    client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PipSession")]
pub struct PyPipSession {
    client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
    token: Option<PipSessionToken>,
    incoming_done: bool,
    outgoing_done: bool,
}

#[pymethods]
impl PyPipClient {
    /// Open an interactive pip session.
    fn open(&self) -> PyResult<PyPipSession> {
        let mut client = lock_py(&self.client, "PipClient")?;
        let token = client.open_session().map_err(py_err)?;
        Ok(PyPipSession {
            client: self.client.clone(),
            token: Some(token),
            incoming_done: false,
            outgoing_done: false,
        })
    }

    /// Open a pip session, send all messages, finish the outgoing side,
    /// then collect all replies. This is a convenience API; Rust keeps the
    /// fully interactive low-level pip session.
    fn exchange<'py>(
        &mut self,
        py: Python<'py>,
        items: Vec<(u64, Vec<u8>)>,
    ) -> PyResult<Vec<(u64, Bound<'py, PyBytes>)>> {
        let out = py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            let mut pip = client.open().map_err(py_err)?;
            for (kind, data) in items {
                pip.send(&RawMsg::new(kind, &data)).map_err(py_err)?;
            }
            pip.finish_send().map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(msg) = pip.next().map_err(py_err)? {
                out.push((msg.header().kind, msg.payload().to_vec()));
            }
            Ok::<_, PyErr>(out)
        })?;
        Ok(out
            .into_iter()
            .map(|(kind, data)| sample_tuple(py, kind, &data))
            .collect())
    }

    fn exchange_pod(
        &mut self,
        py: Python<'_>,
        items: Vec<Py<PyAny>>,
        response_type: Py<PyAny>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let items: Vec<RawMsg> = items
            .into_iter()
            .map(|item| raw_from_pod(item.bind(py)))
            .collect::<PyResult<_>>()?;
        let out = py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            let mut pip = client.open().map_err(py_err)?;
            for item in items {
                pip.send(&item).map_err(py_err)?;
            }
            pip.finish_send().map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(msg) = pip.next().map_err(py_err)? {
                out.push((msg.header().kind, msg.payload().to_vec()));
            }
            Ok::<_, PyErr>(out)
        })?;
        out.into_iter()
            .map(|(kind, data)| decode_datapod(py, response_type.bind(py), kind, &data))
            .collect()
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let client = lock_py(&self.client, "PipClient")?;
        item_stats_dict(py, client.stats())
    }
}

#[pymethods]
impl PyPipSession {
    #[getter]
    fn session_id(&self) -> PyResult<Option<u64>> {
        Ok(self.token.map(|token: PipSessionToken| token.session_id()))
    }

    #[pyo3(signature = (data, kind=0))]
    fn send(&mut self, data: &[u8], kind: u64) -> PyResult<()> {
        if self.outgoing_done {
            return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        let mut client = lock_py(&self.client, "PipClient")?;
        client
            .send_pending(token, &RawMsg::new(kind, data))
            .map_err(py_err)
    }

    fn send_message(&mut self, message: &PyMessage) -> PyResult<()> {
        if self.outgoing_done {
            return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        let mut client = lock_py(&self.client, "PipClient")?;
        client
            .send_pending(token, &RawMsg::new(message.kind, &message.data))
            .map_err(py_err)
    }

    fn send_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        if self.outgoing_done {
            return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        let mut client = lock_py(&self.client, "PipClient")?;
        client.send_pending(token, &raw).map_err(py_err)
    }

    fn finish_send(&mut self) -> PyResult<()> {
        if self.outgoing_done {
            return Ok(());
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        let mut client = lock_py(&self.client, "PipClient")?;
        client.finish_send_pending(token).map_err(py_err)?;
        self.outgoing_done = true;
        Ok(())
    }

    fn next(&mut self, py: Python<'_>) -> PyResult<Option<PyMessage>> {
        if self.incoming_done {
            return Ok(None);
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        let item = py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            client.next_pending(token).map_err(py_err)
        })?;
        match item {
            Some(sample) => Ok(Some(PyMessage {
                kind: sample.header().kind,
                data: sample.payload().to_vec(),
            })),
            None => {
                self.incoming_done = true;
                Ok(None)
            }
        }
    }

    fn next_pod(
        &mut self,
        py: Python<'_>,
        response_type: Py<PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let Some(message) = self.next(py)? else {
            return Ok(None);
        };
        Ok(Some(decode_datapod(
            py,
            response_type.bind(py),
            message.kind,
            &message.data,
        )?))
    }

    fn close(&mut self) -> PyResult<()> {
        let Some(token) = self.token.take() else {
            return Ok(());
        };
        let mut client = lock_py(&self.client, "PipClient")?;
        client.close_session(token);
        self.incoming_done = true;
        self.outgoing_done = true;
        Ok(())
    }
}

#[pyclass(name = "PipServer")]
pub struct PyPipServer {
    server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
}

struct PendingPipState {
    server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
    token: Option<PipServerToken>,
    first: Option<PyMessage>,
    incoming_done: bool,
    outgoing_done: bool,
}

#[pyclass(name = "PendingPip")]
#[derive(Clone)]
pub struct PyPendingPip {
    state: Arc<Mutex<PendingPipState>>,
}

#[pymethods]
impl PyPipServer {
    /// Poll for one pending pip session. Returns `PendingPip` or `None`.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingPip>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "PipServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (_session_id, first, incoming_done, token) = pending.into_parts();
                    return Ok(Some(PyPendingPip {
                        state: Arc::new(Mutex::new(PendingPipState {
                            server: self.server.clone(),
                            token: Some(token),
                            first: first.map(|msg| PyMessage {
                                kind: msg.header().kind,
                                data: msg.payload().to_vec(),
                            }),
                            incoming_done,
                            outgoing_done: false,
                        })),
                    }));
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    }

    /// Serve one pip session. `handler(items)` receives all client
    /// messages and returns None, bytes, `(kind, bytes)`, `[bytes, ...]`,
    /// or `[(kind, bytes), ...]`.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&mut self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "PipServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (_session_id, first, mut incoming_done, token) = pending.into_parts();
                        let mut items = Vec::new();
                        if let Some(msg) = first {
                            items.push((msg.header().kind, msg.payload().to_vec()));
                        }
                        while !incoming_done {
                            let next = {
                                let mut server = lock_py(&self.server, "PipServer")?;
                                server.next_pending(token).map_err(py_err)?
                            };
                            match next {
                                Some(msg) => {
                                    items.push((msg.header().kind, msg.payload().to_vec()));
                                }
                                None => {
                                    incoming_done = true;
                                }
                            }
                        }
                        let replies = Python::with_gil(|py| -> PyResult<Vec<RawMsg>> {
                            let ret = handler.bind(py).call1((items,))?;
                            raw_vec_from_py(&ret)
                        })?;
                        {
                            let mut server = lock_py(&self.server, "PipServer")?;
                            for reply in replies {
                                server.send_pending(token, &reply).map_err(py_err)?;
                            }
                            server.finish_send_pending(token).map_err(py_err)?;
                            server.close_pending(token);
                        }
                        return Ok(true);
                    }
                    None => {
                        if Instant::now() >= deadline {
                            return Ok(false);
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            }
        })
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let server = lock_py(&self.server, "PipServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyPendingPip {
    #[getter]
    fn session_id(&self) -> PyResult<Option<u64>> {
        let state = lock_py(&self.state, "PendingPip")?;
        Ok(state.token.map(|token: PipServerToken| token.session_id()))
    }

    fn next(&mut self, py: Python<'_>) -> PyResult<Option<PyMessage>> {
        {
            let mut state = lock_py(&self.state, "PendingPip")?;
            if let Some(first) = state.first.take() {
                return Ok(Some(first));
            }
            if state.incoming_done {
                return Ok(None);
            }
        }

        let item = py.allow_threads(|| {
            let (server, token) = {
                let state = lock_py(&self.state, "PendingPip")?;
                let token = state
                    .token
                    .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
                (state.server.clone(), token)
            };
            let mut server = lock_py(server.as_ref(), "PipServer")?;
            server.next_pending(token).map_err(py_err)
        })?;

        match item {
            Some(sample) => Ok(Some(PyMessage {
                kind: sample.header().kind,
                data: sample.payload().to_vec(),
            })),
            None => {
                let mut state = lock_py(&self.state, "PendingPip")?;
                state.incoming_done = true;
                Ok(None)
            }
        }
    }

    fn next_pod(&mut self, py: Python<'_>, request_type: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let Some(message) = self.next(py)? else {
            return Ok(None);
        };
        Ok(Some(decode_datapod(
            py,
            request_type.bind(py),
            message.kind,
            &message.data,
        )?))
    }

    #[pyo3(signature = (data, kind=0))]
    fn send(&mut self, data: &[u8], kind: u64) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingPip")?;
            if state.outgoing_done {
                return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
            }
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "PipServer")?;
        server
            .send_pending(token, &RawMsg::new(kind, data))
            .map_err(py_err)
    }

    fn send_message(&mut self, message: &PyMessage) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingPip")?;
            if state.outgoing_done {
                return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
            }
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "PipServer")?;
        server
            .send_pending(token, &RawMsg::new(message.kind, &message.data))
            .map_err(py_err)
    }

    fn send_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        let (server, token) = {
            let state = lock_py(&self.state, "PendingPip")?;
            if state.outgoing_done {
                return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
            }
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "PipServer")?;
        server.send_pending(token, &raw).map_err(py_err)
    }

    fn finish_send(&mut self) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingPip")?;
            if state.outgoing_done {
                return Ok(());
            }
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
            state.outgoing_done = true;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "PipServer")?;
        server.finish_send_pending(token).map_err(py_err)
    }

    fn finish_send_pod(&mut self) -> PyResult<()> {
        self.finish_send()
    }

    fn close(&mut self) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingPip")?;
            let Some(token) = state.token.take() else {
                return Ok(());
            };
            state.first = None;
            state.incoming_done = true;
            state.outgoing_done = true;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "PipServer")?;
        server.close_pending(token);
        Ok(())
    }
}

pub fn register_python_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDeliveryPolicy>()?;
    module.add_class::<PyTopicQos>()?;
    module.add_class::<PyMessage>()?;
    module.add_class::<PyNode>()?;
    module.add_class::<PyPublisher>()?;
    module.add_class::<PySubscriber>()?;
    module.add_class::<PyDatapodPublisher>()?;
    module.add_class::<PyDatapodSubscriber>()?;
    module.add_class::<PyDatapodSampleView>()?;
    module.add_class::<PyReqClient>()?;
    module.add_class::<PyReqServer>()?;
    module.add_class::<PyPendingReq>()?;
    module.add_class::<PyQueClient>()?;
    module.add_class::<PyAnsServer>()?;
    module.add_class::<PyPendingQue>()?;
    module.add_class::<PyPendingAnswers>()?;
    module.add_class::<PyPutClient>()?;
    module.add_class::<PyPutUpload>()?;
    module.add_class::<PyAckServer>()?;
    module.add_class::<PyPipClient>()?;
    module.add_class::<PyPipSession>()?;
    module.add_class::<PyPipServer>()?;
    module.add_class::<PyPendingPip>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[pymodule]
fn quicbit(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_python_module(module)
}
