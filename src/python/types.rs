use super::*;

#[pyclass(name = "DeliveryPolicy")]
#[derive(Clone, Copy)]
pub struct PyDeliveryPolicy {
    pub(crate) inner: DeliveryPolicy,
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
    pub(crate) inner: TopicQos,
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

pub(crate) fn qos_value(qos: Option<PyRef<'_, PyTopicQos>>) -> TopicQos {
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
    pub(crate) kind: u64,
    pub(crate) data: Vec<u8>,
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
    pub(crate) type_hash: u64,
    pub(crate) wire: Vec<u8>,
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

