//! Python bindings for peerbus (pyo3).
//!
//! The binding surface intentionally transports opaque byte messages:
//! a `kind: int` tag plus `bytes`, carried by [`crate::RawMsg`]. Concrete
//! datapod Python classes from datapod 0.4 can sit above this by using
//! `kind = TYPE_HASH` and their own wire bytes.

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyBufferError, PyIndexError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyMemoryView, PyModule, PyTuple};

use crate::{
    AckServer, AnsReplyToken, AnsServer, DatapodMsg, DeliveryPolicy, LocalConfig, Node, NodeSample,
    PipClient, PipServer, PipServerToken, PipSessionToken, Publisher, PutAckToken, PutClient,
    PutUploadToken, QueClient, RawMsg, ReqClient, ReqReplyToken, ReqServer, Subscriber, TopicQos,
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
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let item = item.bind(py);
            if item.is_none() {
                continue;
            }
            out.push(raw_from_py(item)?);
        }
        return Ok(out);
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
        return Err(PyBufferError::new_err("peerbus sample views are read-only"));
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

/// Best-effort count of the positional parameters a handler accepts, via
/// `inspect.signature`. Used to dispatch server handlers to the correct calling
/// convention up front, instead of calling one convention and retrying on
/// `TypeError` (which would re-run a correctly-shaped handler body that happened
/// to raise `TypeError` internally, invoking user side effects twice).
///
/// Returns `None` when the signature cannot be introspected (some builtins) or
/// when the handler accepts `*args`; callers then fall back to the canonical
/// (preferred) convention.
fn handler_positional_arity(
    py: Python<'_>,
    handler: &Bound<'_, PyAny>,
) -> PyResult<Option<usize>> {
    let inspect = py.import("inspect")?;
    let signature = match inspect.call_method1("signature", (handler,)) {
        Ok(sig) => sig,
        Err(_) => return Ok(None),
    };
    let parameters = signature.getattr("parameters")?;
    let values = parameters.call_method0("values")?;
    let mut count = 0usize;
    for param in values.try_iter()? {
        let param = param?;
        let kind = param.getattr("kind")?;
        match kind.str()?.to_string_lossy().as_ref() {
            "POSITIONAL_ONLY" | "POSITIONAL_OR_KEYWORD" => count += 1,
            // `*args` means the handler accepts an arbitrary number of
            // positional args; we cannot reason about arity, so defer to the
            // canonical convention.
            "VAR_POSITIONAL" => return Ok(None),
            _ => {}
        }
    }
    Ok(Some(count))
}

fn datapod_msg_from_py(value: &Bound<'_, PyAny>) -> PyResult<DatapodMsg> {
    if let Ok((type_hash, wire)) = value.extract::<(u64, Vec<u8>)>() {
        return Ok(DatapodMsg::new(type_hash, wire));
    }
    let wire = value.call_method0("to_wire_message").map_err(|_| {
        PyRuntimeError::new_err(
            "expected (type_hash, wire_bytes) or datapod object with to_wire_message() -> (type_hash, bytes)",
        )
    })?;
    let (type_hash, wire) = wire.extract::<(u64, Vec<u8>)>()?;
    Ok(DatapodMsg::new(type_hash, wire))
}

fn datapod_vec_from_py(value: &Bound<'_, PyAny>) -> PyResult<Vec<DatapodMsg>> {
    if value.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(items) = value.extract::<Vec<Py<PyAny>>>() {
        let py = value.py();
        return items
            .into_iter()
            .map(|item| datapod_msg_from_py(item.bind(py)))
            .collect();
    }
    Ok(vec![datapod_msg_from_py(value)?])
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
        let header_size = if datapod_type.hasattr("__datapod_header_size__")? {
            datapod_type
                .getattr("__datapod_header_size__")?
                .extract::<usize>()?
        } else if datapod_type.hasattr("HEADER_SIZE")? {
            datapod_type.getattr("HEADER_SIZE")?.extract::<usize>()?
        } else {
            match PyModule::import(py, "datapod")
                .and_then(|module| module.getattr("header_size"))
                .and_then(|func| func.call1((kind,)))
                .and_then(|value| value.extract::<usize>())
            {
                Ok(size) => size,
                Err(_) => payload.len(),
            }
        };
        if payload.len() < header_size {
            return Err(PyRuntimeError::new_err(format!(
                "datapod wire message too short: got {}, need at least {} header bytes",
                payload.len(),
                header_size
            )));
        }
        let header = PyBytes::new(py, &payload[..header_size]);
        let body = PyBytes::new(py, &payload[header_size..]);
        datapod_type.call_method1("from_wire", (header, body))?
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

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let tuple = PyTuple::new(
            py,
            [
                self.kind.into_py_any(py)?,
                self.data(py).into_any().unbind(),
            ],
        )?;
        Ok(tuple.call_method0("__iter__")?.unbind())
    }

    fn __len__(&self) -> usize {
        2
    }

    fn __getitem__(&self, py: Python<'_>, index: isize) -> PyResult<Py<PyAny>> {
        match index {
            0 | -2 => self.kind.into_py_any(py),
            1 | -1 => Ok(self.data(py).into_any().unbind()),
            _ => Err(PyIndexError::new_err("Message index out of range")),
        }
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

#[pyclass(name = "DatapodMessage")]
#[derive(Clone)]
pub struct PyDatapodMessage {
    #[pyo3(get)]
    type_hash: u64,
    wire: Vec<u8>,
}

#[pymethods]
impl PyDatapodMessage {
    #[new]
    fn new(type_hash: u64, wire: Vec<u8>) -> Self {
        Self { type_hash, wire }
    }

    #[staticmethod]
    fn from_datapod(py: Python<'_>, value: Py<PyAny>) -> PyResult<Self> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        Ok(Self {
            type_hash: msg.type_hash,
            wire: msg.wire,
        })
    }

    #[getter]
    fn wire<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.wire)
    }

    #[getter]
    fn wire_len(&self) -> usize {
        self.wire.len()
    }

    fn __len__(&self) -> usize {
        self.wire.len()
    }

    fn wire_view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        let any = slf.into_any();
        PyMemoryView::from(&any)
    }

    fn to_wire_message<'py>(&self, py: Python<'py>) -> (u64, Bound<'py, PyBytes>) {
        (self.type_hash, PyBytes::new(py, &self.wire))
    }

    fn decode(&self, py: Python<'_>, datapod_type: Py<PyAny>) -> PyResult<Py<PyAny>> {
        decode_datapod(py, datapod_type.bind(py), self.type_hash, &self.wire)
    }

    fn __repr__(&self) -> String {
        format!(
            "DatapodMessage(type_hash={}, len={})",
            self.type_hash,
            self.wire.len()
        )
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let (data_ptr, data_len) = {
            let borrowed = slf.borrow();
            (borrowed.wire.as_ptr(), borrowed.wire.len())
        };
        unsafe { fill_readonly_buffer(view, flags, data_ptr, data_len, slf.into_any()) }
    }

    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        unsafe { release_readonly_buffer(view) };
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
        max_subscribers=None,
        allowed_peers=None,
        allow_any_peer=false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        identity: Option<String>,
        no_relay: bool,
        system_did: Option<String>,
        max_payload_bytes: Option<usize>,
        history_depth: Option<u32>,
        subscriber_buffer: Option<u32>,
        max_publishers: Option<u32>,
        max_subscribers: Option<u32>,
        allowed_peers: Option<Vec<String>>,
        allow_any_peer: bool,
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
        if let Some(peers) = allowed_peers {
            for peer in peers {
                builder = builder.allow_peer(peer);
            }
        }
        if allow_any_peer {
            builder = builder.allow_any_peer();
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
    fn datapod_req_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodReqClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .req_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos)
        } else {
            self.node
                .req_client_with_qos::<DatapodMsg, DatapodMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyDatapodReqClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_req(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodReqClient> {
        let client = self
            .node
            .req_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodReqClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_req_server(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodReqServer> {
        let server = self
            .node
            .req_server_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodReqServer {
            server: Arc::new(Mutex::new(server)),
        })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn datapod_que_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodQueClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .que_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos)
        } else {
            self.node
                .que_client_with_qos::<DatapodMsg, DatapodMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyDatapodQueClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_que(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodQueClient> {
        let client = self
            .node
            .que_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodQueClient { client })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_ans_server(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodAnsServer> {
        let server = self
            .node
            .ans_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodAnsServer {
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
        Ok(PyAckServer {
            server: Arc::new(Mutex::new(server)),
        })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn datapod_put_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodPutClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .put_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos)
        } else {
            self.node
                .put_client_with_qos::<DatapodMsg, DatapodMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyDatapodPutClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_put(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodPutClient> {
        let client = self
            .node
            .put_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodPutClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_ack_server(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodAckServer> {
        let server = self
            .node
            .ack_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodAckServer {
            server: Arc::new(Mutex::new(server)),
        })
    }

    #[pyo3(signature = (peer, topic, qos=None))]
    fn datapod_pip_client(
        &self,
        peer: &str,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodPipClient> {
        let qos = qos_value(qos);
        let client = if let Ok(addr) = decode_endpoint_addr(peer) {
            self.node
                .pip_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos)
        } else {
            self.node
                .pip_client_with_qos::<DatapodMsg, DatapodMsg>(peer, topic, qos)
        }
        .map_err(py_err)?;
        Ok(PyDatapodPipClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_pip(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodPipClient> {
        let client = self
            .node
            .pip_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodPipClient {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[pyo3(signature = (topic, qos=None))]
    fn datapod_pip_server(
        &self,
        topic: &str,
        qos: Option<PyRef<'_, PyTopicQos>>,
    ) -> PyResult<PyDatapodPipServer> {
        let server = self
            .node
            .pip_server_with_qos::<DatapodMsg, DatapodMsg>(topic, qos_value(qos))
            .map_err(py_err)?;
        Ok(PyDatapodPipServer {
            server: Arc::new(Mutex::new(server)),
        })
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

/// Borrowed zero-copy raw sample.
///
/// For local SHM this object pins the underlying slot until it and all
/// memoryviews derived from it are dropped. `memoryview(sample)` or
/// `sample.data_view()` exposes the raw payload without copying.
#[pyclass(name = "SampleView")]
pub struct PySampleView {
    sample: NodeSample<RawMsg>,
}

#[pymethods]
impl PySampleView {
    #[getter]
    fn kind(&self) -> u64 {
        self.sample.header().kind
    }

    #[getter]
    fn data_len(&self) -> usize {
        self.sample.payload().len()
    }

    fn __len__(&self) -> usize {
        self.sample.payload().len()
    }

    /// Return a Python memoryview of the raw payload bytes.
    ///
    /// This is the zero-copy path. Keep this sample object alive while using
    /// the memoryview; holding it pins the SHM slot.
    fn data_view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        let any = slf.into_any();
        PyMemoryView::from(&any)
    }

    /// Alias for `data_view()`.
    fn payload_view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        Self::data_view(slf)
    }

    /// Explicit copy helper for callers that really want owned bytes.
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.sample.payload())
    }

    fn as_datapod(&self, py: Python<'_>, datapod_type: Py<PyAny>) -> PyResult<Py<PyAny>> {
        decode_datapod(
            py,
            datapod_type.bind(py),
            self.sample.header().kind,
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
impl PySubscriber {
    /// Poll for the next sample. Returns `Message(kind, data)` or `None`.
    ///
    /// `Message` keeps tuple-unpacking/index compatibility for existing
    /// callers. Use `take_tuple()` when an explicit `(kind, data)` tuple is
    /// required, or `take_view()` for the zero-copy borrowed payload path.
    fn take(&mut self) -> PyResult<Option<PyMessage>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(PyMessage {
                kind: sample.header().kind,
                data: sample.payload().to_vec(),
            })),
            None => Ok(None),
        }
    }

    /// Alias for `take()`.
    fn take_message(&mut self) -> PyResult<Option<PyMessage>> {
        self.take()
    }

    /// Poll for the next sample as an explicit `(kind, data)` tuple.
    fn take_tuple<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(u64, Bound<'py, PyBytes>)>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(sample_tuple(
                py,
                sample.header().kind,
                sample.payload(),
            ))),
            None => Ok(None),
        }
    }

    /// Poll and return a borrowed zero-copy sample view.
    ///
    /// For same-host SHM this pins the slot until the returned object and any
    /// memoryviews made from it are released. Use this for high-throughput
    /// byte payloads instead of `take()` / `take_message()`.
    fn take_view(&mut self, py: Python<'_>) -> PyResult<Option<Py<PySampleView>>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Py::new(py, PySampleView { sample }).map(Some),
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

    /// Poll without decoding. Returns `DatapodMessage(type_hash, wire)` or `None`.
    fn take_message(&mut self) -> PyResult<Option<PyDatapodMessage>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some(PyDatapodMessage {
                type_hash: sample.header().type_hash,
                wire: sample.payload().to_vec(),
            })),
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
    /// Send a request and block for the response `Message`.
    #[pyo3(signature = (data, kind=0))]
    fn call(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<PyMessage> {
        let res = py
            .allow_threads(|| self.client.call(&RawMsg::new(kind, data)))
            .map_err(py_err)?;
        Ok(PyMessage {
            kind: res.header().kind,
            data: res.payload().to_vec(),
        })
    }

    /// Compatibility tuple-returning request call.
    #[pyo3(signature = (data, kind=0))]
    fn call_tuple<'py>(
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

    /// Explicit spelling for callers that prefer the object-returning name.
    #[pyo3(signature = (data, kind=0))]
    fn call_message(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<PyMessage> {
        self.call(py, data, kind)
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
    /// `handler` receives a `Message` and returns `bytes`, `(kind, bytes)`,
    /// `Message`, or any datapod object with `to_wire_message()`.
    /// Compatibility handlers that accept `(kind, data)` are still supported.
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
                            let handler = handler.bind(py);
                            // Dispatch by arity up front: a 2-parameter handler
                            // uses the compatibility `(kind, data)` convention,
                            // anything else uses the canonical `(message)` one.
                            // We never catch `TypeError` as a signature signal,
                            // so a handler body that raises `TypeError` is not
                            // silently re-invoked with a different shape.
                            let arity = handler_positional_arity(py, handler)?;
                            let ret = if arity == Some(2) {
                                handler.call1((kind, PyBytes::new(py, &payload)))?
                            } else {
                                let message = Py::new(
                                    py,
                                    PyMessage {
                                        kind,
                                        data: payload.clone(),
                                    },
                                )?;
                                handler.call1((message,))?
                            };
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
    fn reply(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<()> {
        let reply = self
            .reply
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("request already replied"))?;
        let msg = RawMsg::new(kind, data);
        py.allow_threads(|| {
            let mut server = lock_py(&self.server, "ReqServer")?;
            server.respond_pending(reply, &msg).map_err(py_err)
        })
    }

    fn reply_message(&mut self, py: Python<'_>, message: &PyMessage) -> PyResult<()> {
        let reply = self
            .reply
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("request already replied"))?;
        let msg = RawMsg::new(message.kind, &message.data);
        py.allow_threads(|| {
            let mut server = lock_py(&self.server, "ReqServer")?;
            server.respond_pending(reply, &msg).map_err(py_err)
        })
    }

    fn reply_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        let reply = self
            .reply
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("request already replied"))?;
        py.allow_threads(|| {
            let mut server = lock_py(&self.server, "ReqServer")?;
            server.respond_pending(reply, &raw).map_err(py_err)
        })
    }
}

#[pyclass(name = "DatapodReqClient")]
pub struct PyDatapodReqClient {
    client: ReqClient<DatapodMsg, DatapodMsg>,
}

#[pymethods]
impl PyDatapodReqClient {
    /// Send any datapod Python object and return a generic datapod message.
    fn call(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<PyDatapodMessage> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        let response = py
            .allow_threads(|| self.client.call(&msg))
            .map_err(py_err)?;
        Ok(PyDatapodMessage {
            type_hash: response.header().type_hash,
            wire: response.payload().to_vec(),
        })
    }

    /// Send raw `type_hash + wire` and return raw `type_hash + wire`.
    fn call_wire(
        &mut self,
        py: Python<'_>,
        type_hash: u64,
        wire: Vec<u8>,
    ) -> PyResult<PyDatapodMessage> {
        let response = py
            .allow_threads(|| self.client.call(&DatapodMsg::new(type_hash, wire)))
            .map_err(py_err)?;
        Ok(PyDatapodMessage {
            type_hash: response.header().type_hash,
            wire: response.payload().to_vec(),
        })
    }

    fn call_decode(
        &mut self,
        py: Python<'_>,
        value: Py<PyAny>,
        response_type: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let message = self.call(py, value)?;
        decode_datapod(py, response_type.bind(py), message.type_hash, &message.wire)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        item_stats_dict(py, self.client.stats())
    }
}

#[pyclass(name = "DatapodReqServer")]
pub struct PyDatapodReqServer {
    server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
}

#[pyclass(name = "PendingDatapodReq")]
pub struct PyPendingDatapodReq {
    server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
    reply: Option<ReqReplyToken>,
    request: PyDatapodMessage,
}

#[pymethods]
impl PyDatapodReqServer {
    /// Poll for one pending datapod request.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingDatapodReq>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodReqServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (req, reply) = pending.into_parts();
                    return Ok(Some(PyPendingDatapodReq {
                        server: self.server.clone(),
                        reply: Some(reply),
                        request: PyDatapodMessage {
                            type_hash: req.header().type_hash,
                            wire: req.payload().to_vec(),
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

    /// Serve at most one datapod request. The handler receives a
    /// `DatapodMessage` and returns any object with `to_wire_message()`.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodReqServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (req, reply) = pending.into_parts();
                        let response = Python::with_gil(|py| -> PyResult<DatapodMsg> {
                            let request = Py::new(
                                py,
                                PyDatapodMessage {
                                    type_hash: req.header().type_hash,
                                    wire: req.payload().to_vec(),
                                },
                            )?;
                            let ret = handler.bind(py).call1((request,))?;
                            datapod_msg_from_py(&ret)
                        })?;
                        let mut server = lock_py(&self.server, "DatapodReqServer")?;
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
        let server = lock_py(&self.server, "DatapodReqServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyPendingDatapodReq {
    #[getter]
    fn request(&self) -> PyDatapodMessage {
        self.request.clone()
    }

    #[getter]
    fn req_id(&self) -> Option<u64> {
        self.reply.map(|reply: ReqReplyToken| reply.req_id())
    }

    fn reply(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        self.reply_datapod_msg(py, msg)
    }

    fn reply_wire(&mut self, py: Python<'_>, type_hash: u64, wire: Vec<u8>) -> PyResult<()> {
        self.reply_datapod_msg(py, DatapodMsg::new(type_hash, wire))
    }

    fn reply_message(&mut self, py: Python<'_>, message: &PyDatapodMessage) -> PyResult<()> {
        self.reply_datapod_msg(py, DatapodMsg::new(message.type_hash, message.wire.clone()))
    }
}

impl PyPendingDatapodReq {
    fn reply_datapod_msg(&mut self, py: Python<'_>, msg: DatapodMsg) -> PyResult<()> {
        let reply = self
            .reply
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("datapod request already replied"))?;
        py.allow_threads(|| {
            let mut server = lock_py(&self.server, "DatapodReqServer")?;
            server.respond_pending(reply, &msg).map_err(py_err)
        })
    }
}

#[pyclass(name = "DatapodQueClient")]
pub struct PyDatapodQueClient {
    client: QueClient<DatapodMsg, DatapodMsg>,
}

#[pymethods]
impl PyDatapodQueClient {
    /// Send any datapod Python object as a que and collect generic datapod ans items.
    fn send(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Vec<PyDatapodMessage>> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        py.allow_threads(|| {
            let mut answers = self.client.send(&msg).map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(ans) = answers.next().map_err(py_err)? {
                out.push(PyDatapodMessage {
                    type_hash: ans.header().type_hash,
                    wire: ans.payload().to_vec(),
                });
            }
            Ok(out)
        })
    }

    /// Send raw `type_hash + wire` and collect generic datapod ans items.
    fn send_wire(
        &mut self,
        py: Python<'_>,
        type_hash: u64,
        wire: Vec<u8>,
    ) -> PyResult<Vec<PyDatapodMessage>> {
        py.allow_threads(|| {
            let mut answers = self
                .client
                .send(&DatapodMsg::new(type_hash, wire))
                .map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(ans) = answers.next().map_err(py_err)? {
                out.push(PyDatapodMessage {
                    type_hash: ans.header().type_hash,
                    wire: ans.payload().to_vec(),
                });
            }
            Ok(out)
        })
    }

    fn send_decode(
        &mut self,
        py: Python<'_>,
        value: Py<PyAny>,
        answer_type: Py<PyAny>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        self.send(py, value)?
            .into_iter()
            .map(|message| {
                decode_datapod(py, answer_type.bind(py), message.type_hash, &message.wire)
            })
            .collect()
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        item_stats_dict(py, self.client.stats())
    }
}

#[pyclass(name = "DatapodAnsServer")]
pub struct PyDatapodAnsServer {
    server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
}

#[pyclass(name = "PendingDatapodQue")]
pub struct PyPendingDatapodQue {
    request: PyDatapodMessage,
    answers: PyPendingDatapodAnswers,
}

struct PendingDatapodAnswersState {
    server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
    token: Option<AnsReplyToken>,
}

#[pyclass(name = "PendingDatapodAnswers")]
#[derive(Clone)]
pub struct PyPendingDatapodAnswers {
    state: Arc<Mutex<PendingDatapodAnswersState>>,
}

#[pymethods]
impl PyDatapodAnsServer {
    /// Poll for one pending datapod que.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingDatapodQue>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodAnsServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (que, token) = pending.into_parts();
                    return Ok(Some(PyPendingDatapodQue {
                        request: PyDatapodMessage {
                            type_hash: que.header().type_hash,
                            wire: que.payload().to_vec(),
                        },
                        answers: PyPendingDatapodAnswers {
                            state: Arc::new(Mutex::new(PendingDatapodAnswersState {
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

    /// Serve at most one datapod que. The handler receives a
    /// `DatapodMessage` and returns None, one datapod object/message, or a
    /// list of datapod objects/messages.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodAnsServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (que, token) = pending.into_parts();
                        let replies = Python::with_gil(|py| -> PyResult<Vec<DatapodMsg>> {
                            let request = Py::new(
                                py,
                                PyDatapodMessage {
                                    type_hash: que.header().type_hash,
                                    wire: que.payload().to_vec(),
                                },
                            )?;
                            let ret = handler.bind(py).call1((request,))?;
                            datapod_vec_from_py(&ret)
                        })?;
                        let mut server = lock_py(&self.server, "DatapodAnsServer")?;
                        for reply in replies {
                            server.send_pending(token, &reply).map_err(py_err)?;
                        }
                        server.finish_pending(token).map_err(py_err)?;
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
        let server = lock_py(&self.server, "DatapodAnsServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyPendingDatapodQue {
    #[getter]
    fn request(&self) -> PyDatapodMessage {
        self.request.clone()
    }

    #[getter]
    fn answers(&self) -> PyPendingDatapodAnswers {
        self.answers.clone()
    }

    #[getter]
    fn ans(&self) -> PyPendingDatapodAnswers {
        self.answers.clone()
    }
}

#[pymethods]
impl PyPendingDatapodAnswers {
    #[getter]
    fn req_id(&self) -> PyResult<Option<u64>> {
        let state = lock_py(&self.state, "PendingDatapodAnswers")?;
        Ok(state.token.map(|token: AnsReplyToken| token.req_id()))
    }

    fn send(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        self.send_datapod_msg(py, msg)
    }

    fn send_wire(&mut self, py: Python<'_>, type_hash: u64, wire: Vec<u8>) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(type_hash, wire))
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyDatapodMessage) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(message.type_hash, message.wire.clone()))
    }

    fn finish(&mut self, py: Python<'_>) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingDatapodAnswers")?;
            let token = state
                .token
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("datapod answer stream already finished"))?;
            (state.server.clone(), token)
        };
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "DatapodAnsServer")?;
            server.finish_pending(token).map_err(py_err)
        })
    }
}

impl PyPendingDatapodAnswers {
    fn send_datapod_msg(&mut self, py: Python<'_>, msg: DatapodMsg) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingDatapodAnswers")?;
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("datapod answer stream already finished"))?;
            (state.server.clone(), token)
        };
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "DatapodAnsServer")?;
            server.send_pending(token, &msg).map_err(py_err)
        })
    }
}

#[pyclass(name = "QueClient")]
pub struct PyQueClient {
    client: QueClient<RawMsg, RawMsg>,
}

#[pymethods]
impl PyQueClient {
    /// Send one que and collect all ans items as `Message` objects.
    #[pyo3(signature = (data, kind=0))]
    fn send(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<Vec<PyMessage>> {
        let data = data.to_vec();
        py.allow_threads(|| {
            let mut answers = self
                .client
                .send(&RawMsg::new(kind, &data))
                .map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(ans) = answers.next().map_err(py_err)? {
                out.push(PyMessage {
                    kind: ans.header().kind,
                    data: ans.payload().to_vec(),
                });
            }
            Ok::<_, PyErr>(out)
        })
    }

    /// Compatibility tuple-returning que/ans call.
    #[pyo3(signature = (data, kind=0))]
    fn send_tuples<'py>(
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

    /// Explicit spelling for the canonical object-returning API.
    #[pyo3(signature = (data, kind=0))]
    fn send_messages(
        &mut self,
        py: Python<'_>,
        data: &[u8],
        kind: u64,
    ) -> PyResult<Vec<PyMessage>> {
        self.send(py, data, kind)
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

    /// Serve one que.
    ///
    /// Preferred shape: `handler(request_message, answers)`, where
    /// `answers.send(...)` / `answers.send_message(...)` streams answers and
    /// `answers.finish()` closes the answer stream. A non-None return value is
    /// also accepted and decoded as one or more answers. Compatibility
    /// handlers that accept `(kind, data)` and return bytes, `Message`, or
    /// lists of those are still supported.
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
                        let answers = PyPendingAnswers {
                            state: Arc::new(Mutex::new(PendingAnswersState {
                                server: self.server.clone(),
                                token: Some(token),
                            })),
                        };
                        let answers_state = answers.state.clone();
                        let fallback = Python::with_gil(|py| -> PyResult<Option<Vec<RawMsg>>> {
                            let handler = handler.bind(py);
                            let message = Py::new(
                                py,
                                PyMessage {
                                    kind,
                                    data: payload.clone(),
                                },
                            )?;
                            // Dispatch by arity up front, never catching
                            // `TypeError` as a signature signal (which would
                            // silently re-invoke a correctly-shaped handler body
                            // that raised `TypeError` internally). The canonical
                            // que/ans handler is `(request_message, answers)`.
                            // A 1-arg handler cannot drive the streaming
                            // `answers` object, so it is treated as a
                            // compatibility handler that receives only the
                            // request and returns the full answer set. Builtins
                            // and `*args` default to the canonical convention.
                            let arity = handler_positional_arity(py, handler)?;
                            let ret = if arity == Some(1) {
                                handler.call1((message,))?
                            } else {
                                handler.call1((message, answers))?
                            };
                            raw_vec_from_py(&ret).map(Some)
                        })?;
                        if let Some(replies) = fallback {
                            let maybe_token = {
                                let state = lock_py(answers_state.as_ref(), "PendingAnswers")?;
                                state.token
                            };
                            if let Some(token) = maybe_token {
                                let mut server = lock_py(&self.server, "AnsServer")?;
                                for reply in replies {
                                    server.send_pending(token, &reply).map_err(py_err)?;
                                }
                            }
                        }
                        let maybe_token = {
                            let mut state = lock_py(answers_state.as_ref(), "PendingAnswers")?;
                            state.token.take()
                        };
                        if let Some(token) = maybe_token {
                            let mut server = lock_py(&self.server, "AnsServer")?;
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

    #[getter]
    fn ans(&self) -> PyPendingAnswers {
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
    fn send(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingAnswers")?;
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("answer stream already finished"))?;
            (state.server.clone(), token)
        };
        let msg = RawMsg::new(kind, data);
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "AnsServer")?;
            server.send_pending(token, &msg).map_err(py_err)
        })
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyMessage) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingAnswers")?;
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("answer stream already finished"))?;
            (state.server.clone(), token)
        };
        let msg = RawMsg::new(message.kind, &message.data);
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "AnsServer")?;
            server.send_pending(token, &msg).map_err(py_err)
        })
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
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "AnsServer")?;
            server.send_pending(token, &raw).map_err(py_err)
        })
    }

    fn finish(&mut self, py: Python<'_>) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingAnswers")?;
            let token = state
                .token
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("answer stream already finished"))?;
            (state.server.clone(), token)
        };
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "AnsServer")?;
            server.finish_pending(token).map_err(py_err)
        })
    }
}

#[pyclass(name = "DatapodPutClient")]
pub struct PyDatapodPutClient {
    client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
}

#[pyclass(name = "DatapodPutUpload")]
pub struct PyDatapodPutUpload {
    client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
    token: Option<PutUploadToken>,
}

#[pymethods]
impl PyDatapodPutClient {
    /// Open an interactive datapod put sender handle.
    fn open(&self) -> PyResult<PyDatapodPutUpload> {
        let mut client = lock_py(&self.client, "DatapodPutClient")?;
        let token = client.open_upload().map_err(py_err)?;
        Ok(PyDatapodPutUpload {
            client: self.client.clone(),
            token: Some(token),
        })
    }

    /// Upload a finite list of datapod objects and return a generic datapod ack.
    fn upload(&mut self, py: Python<'_>, items: Vec<Py<PyAny>>) -> PyResult<PyDatapodMessage> {
        let items: Vec<DatapodMsg> = items
            .into_iter()
            .map(|item| datapod_msg_from_py(item.bind(py)))
            .collect::<PyResult<_>>()?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPutClient")?;
            let mut sender = client.open().map_err(py_err)?;
            for item in items {
                sender.send(&item).map_err(py_err)?;
            }
            let ack = sender.finish().map_err(py_err)?;
            Ok(PyDatapodMessage {
                type_hash: ack.header().type_hash,
                wire: ack.payload().to_vec(),
            })
        })
    }

    /// Preferred three-letter put/ack name for finite datapod sends.
    fn put(&mut self, py: Python<'_>, items: Vec<Py<PyAny>>) -> PyResult<PyDatapodMessage> {
        self.upload(py, items)
    }

    /// Upload raw datapod messages and return a generic datapod ack.
    fn upload_messages(
        &mut self,
        py: Python<'_>,
        items: Vec<PyDatapodMessage>,
    ) -> PyResult<PyDatapodMessage> {
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPutClient")?;
            let mut sender = client.open().map_err(py_err)?;
            for item in items {
                sender
                    .send(&DatapodMsg::new(item.type_hash, item.wire))
                    .map_err(py_err)?;
            }
            let ack = sender.finish().map_err(py_err)?;
            Ok(PyDatapodMessage {
                type_hash: ack.header().type_hash,
                wire: ack.payload().to_vec(),
            })
        })
    }

    /// Preferred three-letter put/ack name for finite raw datapod messages.
    fn put_messages(
        &mut self,
        py: Python<'_>,
        items: Vec<PyDatapodMessage>,
    ) -> PyResult<PyDatapodMessage> {
        self.upload_messages(py, items)
    }

    fn upload_decode(
        &mut self,
        py: Python<'_>,
        items: Vec<Py<PyAny>>,
        ack_type: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let message = self.upload(py, items)?;
        decode_datapod(py, ack_type.bind(py), message.type_hash, &message.wire)
    }

    fn put_decode(
        &mut self,
        py: Python<'_>,
        items: Vec<Py<PyAny>>,
        ack_type: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.upload_decode(py, items, ack_type)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let client = lock_py(&self.client, "DatapodPutClient")?;
        item_stats_dict(py, client.stats())
    }
}

#[pymethods]
impl PyDatapodPutUpload {
    #[getter]
    fn req_id(&self) -> PyResult<Option<u64>> {
        Ok(self.token.map(|token: PutUploadToken| token.req_id()))
    }

    fn send(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        self.send_datapod_msg(py, msg)
    }

    fn send_wire(&mut self, py: Python<'_>, type_hash: u64, wire: Vec<u8>) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(type_hash, wire))
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyDatapodMessage) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(message.type_hash, message.wire.clone()))
    }

    fn finish(&mut self, py: Python<'_>) -> PyResult<PyDatapodMessage> {
        let token = self
            .token
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("datapod put upload already finished"))?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPutClient")?;
            let ack = client.finish_pending(token).map_err(py_err)?;
            Ok(PyDatapodMessage {
                type_hash: ack.header().type_hash,
                wire: ack.payload().to_vec(),
            })
        })
    }

    fn finish_decode(&mut self, py: Python<'_>, ack_type: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let ack = self.finish(py)?;
        decode_datapod(py, ack_type.bind(py), ack.type_hash, &ack.wire)
    }
}

impl PyDatapodPutUpload {
    fn send_datapod_msg(&mut self, py: Python<'_>, msg: DatapodMsg) -> PyResult<()> {
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("datapod put upload already finished"))?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPutClient")?;
            client.send_pending(token, &msg).map_err(py_err)
        })
    }
}

#[pyclass(name = "DatapodAckServer")]
pub struct PyDatapodAckServer {
    server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
}

struct PendingDatapodPutState {
    server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
    token: Option<PutAckToken>,
}

/// Batch of datapod puts handed to a `DatapodAckServer` handler. Mirrors the
/// raw `PutBatch`: it is list-like over the received `DatapodMessage` items and
/// exposes `ack(...)` / `ack_wire(...)` / `ack_message(...)` to choose the ack.
#[pyclass(name = "DatapodPutBatch")]
#[derive(Clone)]
pub struct PyDatapodPutBatch {
    items: Vec<PyDatapodMessage>,
    ack: Arc<Mutex<Option<PyDatapodMessage>>>,
}

/// Poll-driven pending datapod upload returned by `DatapodAckServer.take()`.
/// Mirrors `PendingPut`: the received puts are drained into `items` and a single
/// ack is sent with `ack(...)` / `ack_wire(...)` / `ack_message(...)`.
#[pyclass(name = "PendingDatapodPut")]
#[derive(Clone)]
pub struct PyPendingDatapodPut {
    items: Vec<PyDatapodMessage>,
    req_id: u64,
    state: Arc<Mutex<PendingDatapodPutState>>,
}

#[pymethods]
impl PyDatapodAckServer {
    /// Poll for one pending datapod upload. Returns `PendingDatapodPut` or `None`.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingDatapodPut>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodAckServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (req_id, first, mut incoming_done, token) = pending.into_parts();
                    let mut items = Vec::new();
                    if let Some(sample) = first {
                        items.push(PyDatapodMessage {
                            type_hash: sample.header().type_hash,
                            wire: sample.payload().to_vec(),
                        });
                    }
                    while !incoming_done {
                        let next = {
                            let mut server = lock_py(&self.server, "DatapodAckServer")?;
                            server.next_pending(token).map_err(py_err)?
                        };
                        match next {
                            Some(sample) => items.push(PyDatapodMessage {
                                type_hash: sample.header().type_hash,
                                wire: sample.payload().to_vec(),
                            }),
                            None => incoming_done = true,
                        }
                    }
                    return Ok(Some(PyPendingDatapodPut {
                        items,
                        req_id,
                        state: Arc::new(Mutex::new(PendingDatapodPutState {
                            server: self.server.clone(),
                            token: Some(token),
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

    /// Serve one datapod upload.
    ///
    /// Preferred shape: `handler(batch)`, where `batch.items` is a list of
    /// `DatapodMessage` objects and `batch.ack(...)` / `batch.ack_message(...)`
    /// chooses the ack. Returning any datapod object/message is also accepted.
    /// Compatibility handlers that iterate the batch as a list still work
    /// because `DatapodPutBatch` is list-like.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&mut self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodAckServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (_req_id, first, mut incoming_done, token) = pending.into_parts();
                        let mut items = Vec::new();
                        if let Some(sample) = first {
                            items.push(PyDatapodMessage {
                                type_hash: sample.header().type_hash,
                                wire: sample.payload().to_vec(),
                            });
                        }
                        while !incoming_done {
                            let next = {
                                let mut server = lock_py(&self.server, "DatapodAckServer")?;
                                server.next_pending(token).map_err(py_err)?
                            };
                            match next {
                                Some(sample) => items.push(PyDatapodMessage {
                                    type_hash: sample.header().type_hash,
                                    wire: sample.payload().to_vec(),
                                }),
                                None => incoming_done = true,
                            }
                        }
                        let ack_slot = Arc::new(Mutex::new(None));
                        let batch = PyDatapodPutBatch {
                            items: items.clone(),
                            ack: ack_slot.clone(),
                        };
                        let ack = Python::with_gil(|py| -> PyResult<DatapodMsg> {
                            let ret = handler.bind(py).call1((batch,))?;
                            if ret.is_none() {
                                let ack = lock_py(ack_slot.as_ref(), "DatapodPutBatch")?
                                    .clone()
                                    .ok_or_else(|| {
                                        PyRuntimeError::new_err(
                                            "datapod put/ack handler returned None without calling batch.ack()",
                                        )
                                    })?;
                                Ok(DatapodMsg::new(ack.type_hash, ack.wire))
                            } else {
                                datapod_msg_from_py(&ret)
                            }
                        })?;
                        {
                            let mut server = lock_py(&self.server, "DatapodAckServer")?;
                            server.ack_pending(token, &ack).map_err(py_err)?;
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
        let server = lock_py(&self.server, "DatapodAckServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyDatapodPutBatch {
    #[getter]
    fn items(&self) -> Vec<PyDatapodMessage> {
        self.items.clone()
    }

    #[getter]
    fn messages(&self) -> Vec<PyDatapodMessage> {
        self.items.clone()
    }

    fn __len__(&self) -> usize {
        self.items.len()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, self.items.clone())?;
        Ok(list.call_method0("__iter__")?.unbind())
    }

    fn __getitem__(&self, index: isize) -> PyResult<PyDatapodMessage> {
        let len = self.items.len() as isize;
        let index = if index < 0 { len + index } else { index };
        if index < 0 || index >= len {
            return Err(PyIndexError::new_err("DatapodPutBatch index out of range"));
        }
        Ok(self.items[index as usize].clone())
    }

    fn ack(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        let mut ack = lock_py(&self.ack, "DatapodPutBatch")?;
        *ack = Some(PyDatapodMessage {
            type_hash: msg.type_hash,
            wire: msg.wire,
        });
        Ok(())
    }

    fn ack_wire(&mut self, type_hash: u64, wire: Vec<u8>) -> PyResult<()> {
        let mut ack = lock_py(&self.ack, "DatapodPutBatch")?;
        *ack = Some(PyDatapodMessage { type_hash, wire });
        Ok(())
    }

    fn ack_message(&mut self, message: &PyDatapodMessage) -> PyResult<()> {
        let mut ack = lock_py(&self.ack, "DatapodPutBatch")?;
        *ack = Some(message.clone());
        Ok(())
    }
}

#[pymethods]
impl PyPendingDatapodPut {
    #[getter]
    fn req_id(&self) -> u64 {
        self.req_id
    }

    #[getter]
    fn items(&self) -> Vec<PyDatapodMessage> {
        self.items.clone()
    }

    #[getter]
    fn messages(&self) -> Vec<PyDatapodMessage> {
        self.items.clone()
    }

    fn __len__(&self) -> usize {
        self.items.len()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, self.items.clone())?;
        Ok(list.call_method0("__iter__")?.unbind())
    }

    fn __getitem__(&self, index: isize) -> PyResult<PyDatapodMessage> {
        let len = self.items.len() as isize;
        let index = if index < 0 { len + index } else { index };
        if index < 0 || index >= len {
            return Err(PyIndexError::new_err("PendingDatapodPut index out of range"));
        }
        Ok(self.items[index as usize].clone())
    }

    fn ack(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        self.ack_datapod_msg(py, msg)
    }

    fn ack_wire(&mut self, py: Python<'_>, type_hash: u64, wire: Vec<u8>) -> PyResult<()> {
        self.ack_datapod_msg(py, DatapodMsg::new(type_hash, wire))
    }

    fn ack_message(&mut self, py: Python<'_>, message: &PyDatapodMessage) -> PyResult<()> {
        self.ack_datapod_msg(py, DatapodMsg::new(message.type_hash, message.wire.clone()))
    }
}

impl PyPendingDatapodPut {
    fn ack_datapod_msg(&mut self, py: Python<'_>, msg: DatapodMsg) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingDatapodPut")?;
            let token = state
                .token
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("datapod upload already acked"))?;
            (state.server.clone(), token)
        };
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "DatapodAckServer")?;
            server.ack_pending(token, &msg).map_err(py_err)
        })
    }
}

#[pyclass(name = "DatapodPipClient")]
pub struct PyDatapodPipClient {
    client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
}

#[pyclass(name = "DatapodPipSession")]
pub struct PyDatapodPipSession {
    client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
    token: Option<PipSessionToken>,
    incoming_done: bool,
    outgoing_done: bool,
}

#[pymethods]
impl PyDatapodPipClient {
    /// Open an interactive generic datapod pip session.
    fn open(&self) -> PyResult<PyDatapodPipSession> {
        let mut client = lock_py(&self.client, "DatapodPipClient")?;
        let token = client.open_session().map_err(py_err)?;
        Ok(PyDatapodPipSession {
            client: self.client.clone(),
            token: Some(token),
            incoming_done: false,
            outgoing_done: false,
        })
    }

    /// Send datapod items, finish the client side, and collect datapod replies.
    fn exchange(
        &mut self,
        py: Python<'_>,
        items: Vec<Py<PyAny>>,
    ) -> PyResult<Vec<PyDatapodMessage>> {
        let items: Vec<DatapodMsg> = items
            .into_iter()
            .map(|item| datapod_msg_from_py(item.bind(py)))
            .collect::<PyResult<_>>()?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPipClient")?;
            let mut pip = client.open().map_err(py_err)?;
            for item in items {
                pip.send(&item).map_err(py_err)?;
            }
            pip.finish_send().map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(msg) = pip.next().map_err(py_err)? {
                out.push(PyDatapodMessage {
                    type_hash: msg.header().type_hash,
                    wire: msg.payload().to_vec(),
                });
            }
            Ok(out)
        })
    }

    /// Raw `type_hash + wire` convenience exchange.
    fn exchange_wire(
        &mut self,
        py: Python<'_>,
        items: Vec<(u64, Vec<u8>)>,
    ) -> PyResult<Vec<PyDatapodMessage>> {
        let items: Vec<DatapodMsg> = items
            .into_iter()
            .map(|(type_hash, wire)| DatapodMsg::new(type_hash, wire))
            .collect();
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPipClient")?;
            let mut pip = client.open().map_err(py_err)?;
            for item in items {
                pip.send(&item).map_err(py_err)?;
            }
            pip.finish_send().map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(msg) = pip.next().map_err(py_err)? {
                out.push(PyDatapodMessage {
                    type_hash: msg.header().type_hash,
                    wire: msg.payload().to_vec(),
                });
            }
            Ok(out)
        })
    }

    fn exchange_decode(
        &mut self,
        py: Python<'_>,
        items: Vec<Py<PyAny>>,
        response_type: Py<PyAny>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        self.exchange(py, items)?
            .into_iter()
            .map(|message| {
                decode_datapod(py, response_type.bind(py), message.type_hash, &message.wire)
            })
            .collect()
    }

    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let client = lock_py(&self.client, "DatapodPipClient")?;
        item_stats_dict(py, client.stats())
    }
}

#[pymethods]
impl PyDatapodPipSession {
    #[getter]
    fn session_id(&self) -> PyResult<Option<u64>> {
        Ok(self.token.map(|token: PipSessionToken| token.session_id()))
    }

    fn send(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        self.send_datapod_msg(py, msg)
    }

    fn send_wire(&mut self, py: Python<'_>, type_hash: u64, wire: Vec<u8>) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(type_hash, wire))
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyDatapodMessage) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(message.type_hash, message.wire.clone()))
    }

    fn finish_send(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.outgoing_done {
            return Ok(());
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("datapod pip session is closed"))?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPipClient")?;
            client.finish_send_pending(token).map_err(py_err)
        })?;
        self.outgoing_done = true;
        Ok(())
    }

    fn next(&mut self, py: Python<'_>) -> PyResult<Option<PyDatapodMessage>> {
        if self.incoming_done {
            return Ok(None);
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("datapod pip session is closed"))?;
        let item = py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPipClient")?;
            client.next_pending(token).map_err(py_err)
        })?;
        match item {
            Some(sample) => Ok(Some(PyDatapodMessage {
                type_hash: sample.header().type_hash,
                wire: sample.payload().to_vec(),
            })),
            None => {
                self.incoming_done = true;
                Ok(None)
            }
        }
    }

    fn next_decode(
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
            message.type_hash,
            &message.wire,
        )?))
    }

    fn close(&mut self) -> PyResult<()> {
        let Some(token) = self.token.take() else {
            return Ok(());
        };
        let mut client = lock_py(&self.client, "DatapodPipClient")?;
        client.close_session(token);
        self.incoming_done = true;
        self.outgoing_done = true;
        Ok(())
    }
}

impl PyDatapodPipSession {
    fn send_datapod_msg(&mut self, py: Python<'_>, msg: DatapodMsg) -> PyResult<()> {
        if self.outgoing_done {
            return Err(PyRuntimeError::new_err(
                "datapod pip outgoing direction is done",
            ));
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("datapod pip session is closed"))?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "DatapodPipClient")?;
            client.send_pending(token, &msg).map_err(py_err)
        })
    }
}

#[pyclass(name = "DatapodPipServer")]
pub struct PyDatapodPipServer {
    server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
}

struct PendingDatapodPipState {
    server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
    token: Option<PipServerToken>,
    first: Option<PyDatapodMessage>,
    incoming_done: bool,
    outgoing_done: bool,
}

#[pyclass(name = "PendingDatapodPip")]
#[derive(Clone)]
pub struct PyPendingDatapodPip {
    state: Arc<Mutex<PendingDatapodPipState>>,
}

#[pymethods]
impl PyDatapodPipServer {
    /// Poll for one pending generic datapod pip session.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingDatapodPip>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodPipServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (_session_id, first, incoming_done, token) = pending.into_parts();
                    return Ok(Some(PyPendingDatapodPip {
                        state: Arc::new(Mutex::new(PendingDatapodPipState {
                            server: self.server.clone(),
                            token: Some(token),
                            first: first.map(|msg| PyDatapodMessage {
                                type_hash: msg.header().type_hash,
                                wire: msg.payload().to_vec(),
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

    /// Serve one full-duplex session as a finite exchange convenience.
    ///
    /// The handler receives `[DatapodMessage, ...]` and returns None, one
    /// datapod object/message, or a list of datapod objects/messages.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "DatapodPipServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (_session_id, first, mut incoming_done, token) = pending.into_parts();
                        let mut items = Vec::new();
                        if let Some(msg) = first {
                            items.push(PyDatapodMessage {
                                type_hash: msg.header().type_hash,
                                wire: msg.payload().to_vec(),
                            });
                        }
                        while !incoming_done {
                            let next = {
                                let mut server = lock_py(&self.server, "DatapodPipServer")?;
                                server.next_pending(token).map_err(py_err)?
                            };
                            match next {
                                Some(msg) => items.push(PyDatapodMessage {
                                    type_hash: msg.header().type_hash,
                                    wire: msg.payload().to_vec(),
                                }),
                                None => incoming_done = true,
                            }
                        }
                        let replies = Python::with_gil(|py| -> PyResult<Vec<DatapodMsg>> {
                            let ret = handler.bind(py).call1((items,))?;
                            datapod_vec_from_py(&ret)
                        })?;
                        {
                            let mut server = lock_py(&self.server, "DatapodPipServer")?;
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
        let server = lock_py(&self.server, "DatapodPipServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyPendingDatapodPip {
    #[getter]
    fn session_id(&self) -> PyResult<Option<u64>> {
        let state = lock_py(&self.state, "PendingDatapodPip")?;
        Ok(state.token.map(|token: PipServerToken| token.session_id()))
    }

    fn next(&mut self, py: Python<'_>) -> PyResult<Option<PyDatapodMessage>> {
        {
            let mut state = lock_py(&self.state, "PendingDatapodPip")?;
            if let Some(first) = state.first.take() {
                return Ok(Some(first));
            }
            if state.incoming_done {
                return Ok(None);
            }
        }

        let item = py.allow_threads(|| {
            let (server, token) = {
                let state = lock_py(&self.state, "PendingDatapodPip")?;
                let token = state
                    .token
                    .ok_or_else(|| PyRuntimeError::new_err("datapod pip session is closed"))?;
                (state.server.clone(), token)
            };
            let mut server = lock_py(server.as_ref(), "DatapodPipServer")?;
            server.next_pending(token).map_err(py_err)
        })?;

        match item {
            Some(sample) => Ok(Some(PyDatapodMessage {
                type_hash: sample.header().type_hash,
                wire: sample.payload().to_vec(),
            })),
            None => {
                let mut state = lock_py(&self.state, "PendingDatapodPip")?;
                state.incoming_done = true;
                Ok(None)
            }
        }
    }

    fn send(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let msg = datapod_msg_from_py(value.bind(py))?;
        self.send_datapod_msg(py, msg)
    }

    fn send_wire(&mut self, py: Python<'_>, type_hash: u64, wire: Vec<u8>) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(type_hash, wire))
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyDatapodMessage) -> PyResult<()> {
        self.send_datapod_msg(py, DatapodMsg::new(message.type_hash, message.wire.clone()))
    }

    fn finish_send(&mut self, py: Python<'_>) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingDatapodPip")?;
            if state.outgoing_done {
                return Ok(());
            }
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("datapod pip session is closed"))?;
            state.outgoing_done = true;
            (state.server.clone(), token)
        };
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "DatapodPipServer")?;
            server.finish_send_pending(token).map_err(py_err)
        })
    }

    fn close(&mut self) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingDatapodPip")?;
            let Some(token) = state.token.take() else {
                return Ok(());
            };
            state.first = None;
            state.incoming_done = true;
            state.outgoing_done = true;
            (state.server.clone(), token)
        };
        let mut server = lock_py(server.as_ref(), "DatapodPipServer")?;
        server.close_pending(token);
        Ok(())
    }
}

impl PyPendingDatapodPip {
    fn send_datapod_msg(&mut self, py: Python<'_>, msg: DatapodMsg) -> PyResult<()> {
        let (server, token) = {
            let state = lock_py(&self.state, "PendingDatapodPip")?;
            if state.outgoing_done {
                return Err(PyRuntimeError::new_err(
                    "datapod pip outgoing direction is done",
                ));
            }
            let token = state
                .token
                .ok_or_else(|| PyRuntimeError::new_err("datapod pip session is closed"))?;
            (state.server.clone(), token)
        };
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "DatapodPipServer")?;
            server.send_pending(token, &msg).map_err(py_err)
        })
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
    /// Open an interactive put sender handle.
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

    /// Preferred three-letter put/ack name for finite sends. Returns `Message`.
    fn put(&mut self, py: Python<'_>, items: Vec<(u64, Vec<u8>)>) -> PyResult<PyMessage> {
        self.put_message(py, items)
    }

    /// Send a finite list of put items and return the final ack as `Message`.
    fn put_message(&mut self, py: Python<'_>, items: Vec<(u64, Vec<u8>)>) -> PyResult<PyMessage> {
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PutClient")?;
            let mut sender = client.open().map_err(py_err)?;
            for (kind, data) in items {
                sender.send(&RawMsg::new(kind, &data)).map_err(py_err)?;
            }
            let ack = sender.finish().map_err(py_err)?;
            Ok::<_, PyErr>(PyMessage {
                kind: ack.header().kind,
                data: ack.payload().to_vec(),
            })
        })
    }

    /// Compatibility spelling for callers migrating from `upload(...)`.
    fn upload_message(
        &mut self,
        py: Python<'_>,
        items: Vec<(u64, Vec<u8>)>,
    ) -> PyResult<PyMessage> {
        self.put_message(py, items)
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

    fn put_pod(
        &mut self,
        py: Python<'_>,
        items: Vec<Py<PyAny>>,
        ack_type: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.upload_pod(py, items, ack_type)
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
    fn send(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<()> {
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("put upload already finished"))?;
        let msg = RawMsg::new(kind, data);
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PutClient")?;
            client.send_pending(token, &msg).map_err(py_err)
        })
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyMessage) -> PyResult<()> {
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("put upload already finished"))?;
        let msg = RawMsg::new(message.kind, &message.data);
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PutClient")?;
            client.send_pending(token, &msg).map_err(py_err)
        })
    }

    fn send_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("put upload already finished"))?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PutClient")?;
            client.send_pending(token, &raw).map_err(py_err)
        })
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

#[pyclass(name = "PutBatch")]
#[derive(Clone)]
pub struct PyPutBatch {
    items: Vec<PyMessage>,
    ack: Arc<Mutex<Option<PyMessage>>>,
}

#[pymethods]
impl PyPutBatch {
    #[getter]
    fn items(&self) -> Vec<PyMessage> {
        self.items.clone()
    }

    #[getter]
    fn messages(&self) -> Vec<PyMessage> {
        self.items.clone()
    }

    fn __len__(&self) -> usize {
        self.items.len()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, self.items.clone())?;
        Ok(list.call_method0("__iter__")?.unbind())
    }

    fn __getitem__(&self, index: isize) -> PyResult<PyMessage> {
        let len = self.items.len() as isize;
        let index = if index < 0 { len + index } else { index };
        if index < 0 || index >= len {
            return Err(PyIndexError::new_err("PutBatch index out of range"));
        }
        Ok(self.items[index as usize].clone())
    }

    #[pyo3(signature = (data=None, kind=0))]
    fn ack(&mut self, data: Option<&[u8]>, kind: u64) -> PyResult<()> {
        let mut ack = lock_py(&self.ack, "PutBatch")?;
        *ack = Some(PyMessage {
            kind,
            data: data.unwrap_or_default().to_vec(),
        });
        Ok(())
    }

    fn ack_message(&mut self, message: &PyMessage) -> PyResult<()> {
        let mut ack = lock_py(&self.ack, "PutBatch")?;
        *ack = Some(message.clone());
        Ok(())
    }

    fn ack_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        let mut ack = lock_py(&self.ack, "PutBatch")?;
        *ack = Some(PyMessage {
            kind: raw.kind,
            data: raw.data,
        });
        Ok(())
    }
}

#[pyclass(name = "AckServer")]
pub struct PyAckServer {
    server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
}

struct PendingPutState {
    server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
    token: Option<PutAckToken>,
}

/// Poll-driven pending upload returned by `AckServer.take()`. Mirrors
/// `PendingReq`/`PutBatch`: the received puts are drained into `items` and a
/// single ack is sent with `ack(...)` / `ack_message(...)` / `ack_pod(...)`.
#[pyclass(name = "PendingPut")]
#[derive(Clone)]
pub struct PyPendingPut {
    items: Vec<PyMessage>,
    req_id: u64,
    state: Arc<Mutex<PendingPutState>>,
}

#[pymethods]
impl PyAckServer {
    /// Poll for one pending upload. Returns `PendingPut` or `None`.
    #[pyo3(signature = (timeout_ms=0))]
    fn take(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<PyPendingPut>> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "AckServer")?;
                    server.take_message().map_err(py_err)?
                };
                if let Some(pending) = pending {
                    let (req_id, first, mut incoming_done, token) = pending.into_parts();
                    let mut items = Vec::new();
                    if let Some(sample) = first {
                        items.push(PyMessage {
                            kind: sample.header().kind,
                            data: sample.payload().to_vec(),
                        });
                    }
                    while !incoming_done {
                        let next = {
                            let mut server = lock_py(&self.server, "AckServer")?;
                            server.next_pending(token).map_err(py_err)?
                        };
                        match next {
                            Some(sample) => items.push(PyMessage {
                                kind: sample.header().kind,
                                data: sample.payload().to_vec(),
                            }),
                            None => incoming_done = true,
                        }
                    }
                    return Ok(Some(PyPendingPut {
                        items,
                        req_id,
                        state: Arc::new(Mutex::new(PendingPutState {
                            server: self.server.clone(),
                            token: Some(token),
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

    /// Serve one upload.
    ///
    /// Preferred shape: `handler(upload)`, where `upload.items` is a list of
    /// `Message` objects and `upload.ack(...)` / `upload.ack_message(...)`
    /// chooses the ack. Returning bytes, `(kind, bytes)`, `Message`, or a
    /// datapod object is also accepted. Compatibility handlers that expect a
    /// list of `(kind, data)` tuples still work because `Message` is tuple-like.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&mut self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        py.allow_threads(|| {
            let deadline = Instant::now() + timeout(timeout_ms);
            loop {
                let pending = {
                    let mut server = lock_py(&self.server, "AckServer")?;
                    server.take_message().map_err(py_err)?
                };
                match pending {
                    Some(pending) => {
                        let (_req_id, first, mut incoming_done, token) = pending.into_parts();
                        let mut items = Vec::new();
                        if let Some(sample) = first {
                            items.push(PyMessage {
                                kind: sample.header().kind,
                                data: sample.payload().to_vec(),
                            });
                        }
                        while !incoming_done {
                            let next = {
                                let mut server = lock_py(&self.server, "AckServer")?;
                                server.next_pending(token).map_err(py_err)?
                            };
                            match next {
                                Some(sample) => items.push(PyMessage {
                                    kind: sample.header().kind,
                                    data: sample.payload().to_vec(),
                                }),
                                None => incoming_done = true,
                            }
                        }
                        let ack_slot = Arc::new(Mutex::new(None));
                        let upload = PyPutBatch {
                            items: items.clone(),
                            ack: ack_slot.clone(),
                        };
                        let ack = Python::with_gil(|py| -> PyResult<RawMsg> {
                            let ret = handler.bind(py).call1((upload,))?;
                            if ret.is_none() {
                                let ack = lock_py(ack_slot.as_ref(), "PutBatch")?
                                    .clone()
                                    .ok_or_else(|| {
                                        PyRuntimeError::new_err(
                                            "put/ack handler returned None without calling upload.ack()",
                                        )
                                    })?;
                                Ok(RawMsg::new(ack.kind, &ack.data))
                            } else {
                                raw_from_py(&ret)
                            }
                        })?;
                        {
                            let mut server = lock_py(&self.server, "AckServer")?;
                            server.ack_pending(token, &ack).map_err(py_err)?;
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
        let server = lock_py(&self.server, "AckServer")?;
        item_stats_dict(py, server.stats())
    }
}

#[pymethods]
impl PyPendingPut {
    #[getter]
    fn req_id(&self) -> u64 {
        self.req_id
    }

    #[getter]
    fn items(&self) -> Vec<PyMessage> {
        self.items.clone()
    }

    #[getter]
    fn messages(&self) -> Vec<PyMessage> {
        self.items.clone()
    }

    fn __len__(&self) -> usize {
        self.items.len()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, self.items.clone())?;
        Ok(list.call_method0("__iter__")?.unbind())
    }

    fn __getitem__(&self, index: isize) -> PyResult<PyMessage> {
        let len = self.items.len() as isize;
        let index = if index < 0 { len + index } else { index };
        if index < 0 || index >= len {
            return Err(PyIndexError::new_err("PendingPut index out of range"));
        }
        Ok(self.items[index as usize].clone())
    }

    #[pyo3(signature = (data=None, kind=0))]
    fn ack(&mut self, py: Python<'_>, data: Option<&[u8]>, kind: u64) -> PyResult<()> {
        let msg = RawMsg::new(kind, data.unwrap_or_default());
        self.ack_raw(py, msg)
    }

    fn ack_message(&mut self, py: Python<'_>, message: &PyMessage) -> PyResult<()> {
        self.ack_raw(py, RawMsg::new(message.kind, &message.data))
    }

    fn ack_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        self.ack_raw(py, raw)
    }
}

impl PyPendingPut {
    fn ack_raw(&mut self, py: Python<'_>, ack: RawMsg) -> PyResult<()> {
        let (server, token) = {
            let mut state = lock_py(&self.state, "PendingPut")?;
            let token = state
                .token
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("upload already acked"))?;
            (state.server.clone(), token)
        };
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "AckServer")?;
            server.ack_pending(token, &ack).map_err(py_err)
        })
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
    /// then collect all replies as `Message` objects. This is a convenience
    /// API; Rust keeps the fully interactive low-level pip session.
    fn exchange(&mut self, py: Python<'_>, items: Vec<(u64, Vec<u8>)>) -> PyResult<Vec<PyMessage>> {
        self.exchange_messages(py, items)
    }

    /// Compatibility tuple-returning finite pip exchange.
    fn exchange_tuples<'py>(
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

    /// Finite pip exchange convenience returning `Message` objects.
    fn exchange_messages(
        &mut self,
        py: Python<'_>,
        items: Vec<(u64, Vec<u8>)>,
    ) -> PyResult<Vec<PyMessage>> {
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            let mut pip = client.open().map_err(py_err)?;
            for (kind, data) in items {
                pip.send(&RawMsg::new(kind, &data)).map_err(py_err)?;
            }
            pip.finish_send().map_err(py_err)?;
            let mut out = Vec::new();
            while let Some(msg) = pip.next().map_err(py_err)? {
                out.push(PyMessage {
                    kind: msg.header().kind,
                    data: msg.payload().to_vec(),
                });
            }
            Ok::<_, PyErr>(out)
        })
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
    fn send(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<()> {
        if self.outgoing_done {
            return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        let msg = RawMsg::new(kind, data);
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            client.send_pending(token, &msg).map_err(py_err)
        })
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyMessage) -> PyResult<()> {
        if self.outgoing_done {
            return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        let msg = RawMsg::new(message.kind, &message.data);
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            client.send_pending(token, &msg).map_err(py_err)
        })
    }

    fn send_pod(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let raw = raw_from_pod(value.bind(py))?;
        if self.outgoing_done {
            return Err(PyRuntimeError::new_err("pip outgoing direction is done"));
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            client.send_pending(token, &raw).map_err(py_err)
        })
    }

    fn finish_send(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.outgoing_done {
            return Ok(());
        }
        let token = self
            .token
            .ok_or_else(|| PyRuntimeError::new_err("pip session is closed"))?;
        py.allow_threads(|| {
            let mut client = lock_py(&self.client, "PipClient")?;
            client.finish_send_pending(token).map_err(py_err)
        })?;
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

    /// Serve one pip session. `handler(items)` receives all client messages
    /// as a list of `Message` objects and returns None, bytes, `(kind, bytes)`,
    /// `Message`, or lists of those. Compatibility tuple-style handlers still
    /// work because `Message` is tuple-like.
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
                            items.push(PyMessage {
                                kind: msg.header().kind,
                                data: msg.payload().to_vec(),
                            });
                        }
                        while !incoming_done {
                            let next = {
                                let mut server = lock_py(&self.server, "PipServer")?;
                                server.next_pending(token).map_err(py_err)?
                            };
                            match next {
                                Some(msg) => {
                                    items.push(PyMessage {
                                        kind: msg.header().kind,
                                        data: msg.payload().to_vec(),
                                    });
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
    fn send(&mut self, py: Python<'_>, data: &[u8], kind: u64) -> PyResult<()> {
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
        let msg = RawMsg::new(kind, data);
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "PipServer")?;
            server.send_pending(token, &msg).map_err(py_err)
        })
    }

    fn send_message(&mut self, py: Python<'_>, message: &PyMessage) -> PyResult<()> {
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
        let msg = RawMsg::new(message.kind, &message.data);
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "PipServer")?;
            server.send_pending(token, &msg).map_err(py_err)
        })
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
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "PipServer")?;
            server.send_pending(token, &raw).map_err(py_err)
        })
    }

    fn finish_send(&mut self, py: Python<'_>) -> PyResult<()> {
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
        py.allow_threads(|| {
            let mut server = lock_py(server.as_ref(), "PipServer")?;
            server.finish_send_pending(token).map_err(py_err)
        })
    }

    fn finish_send_pod(&mut self, py: Python<'_>) -> PyResult<()> {
        self.finish_send(py)
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
    module.add_class::<PySampleView>()?;
    module.add_class::<PyDatapodMessage>()?;
    module.add_class::<PyNode>()?;
    module.add_class::<PyPublisher>()?;
    module.add_class::<PySubscriber>()?;
    module.add_class::<PyDatapodPublisher>()?;
    module.add_class::<PyDatapodSubscriber>()?;
    module.add_class::<PyDatapodSampleView>()?;
    module.add_class::<PyReqClient>()?;
    module.add_class::<PyReqServer>()?;
    module.add_class::<PyPendingReq>()?;
    module.add_class::<PyDatapodReqClient>()?;
    module.add_class::<PyDatapodReqServer>()?;
    module.add_class::<PyPendingDatapodReq>()?;
    module.add_class::<PyDatapodQueClient>()?;
    module.add_class::<PyDatapodAnsServer>()?;
    module.add_class::<PyPendingDatapodQue>()?;
    module.add_class::<PyPendingDatapodAnswers>()?;
    module.add(
        "PendingDatapodAns",
        module.getattr("PendingDatapodAnswers")?,
    )?;
    module.add_class::<PyDatapodPutClient>()?;
    module.add_class::<PyDatapodPutUpload>()?;
    module.add("DatapodPutSender", module.getattr("DatapodPutUpload")?)?;
    module.add_class::<PyDatapodAckServer>()?;
    module.add_class::<PyDatapodPutBatch>()?;
    module.add_class::<PyPendingDatapodPut>()?;
    module.add_class::<PyDatapodPipClient>()?;
    module.add_class::<PyDatapodPipSession>()?;
    module.add_class::<PyDatapodPipServer>()?;
    module.add_class::<PyPendingDatapodPip>()?;
    module.add_class::<PyQueClient>()?;
    module.add_class::<PyAnsServer>()?;
    module.add_class::<PyPendingQue>()?;
    module.add_class::<PyPendingAnswers>()?;
    module.add("PendingAns", module.getattr("PendingAnswers")?)?;
    module.add_class::<PyPutClient>()?;
    module.add_class::<PyPutUpload>()?;
    module.add("PutSender", module.getattr("PutUpload")?)?;
    module.add_class::<PyPutBatch>()?;
    module.add_class::<PyAckServer>()?;
    module.add_class::<PyPendingPut>()?;
    module.add_class::<PyPipClient>()?;
    module.add_class::<PyPipSession>()?;
    module.add_class::<PyPipServer>()?;
    module.add_class::<PyPendingPip>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[pymodule]
fn peerbus(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_python_module(module)
}
