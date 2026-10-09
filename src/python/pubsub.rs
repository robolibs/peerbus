use super::*;

#[pyclass(name = "Publisher")]
pub struct PyPublisher {
    pub(crate) publisher: Publisher<RawMsg>,
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
    pub(crate) subscriber: Subscriber<RawMsg>,
}

/// Borrowed zero-copy raw sample.
///
/// For local SHM this object pins the underlying slot until it and all
/// memoryviews derived from it are dropped. `memoryview(sample)` or
/// `sample.data_view()` exposes the raw payload without copying.
#[pyclass(name = "SampleView")]
pub struct PySampleView {
    pub(crate) sample: NodeSample<RawMsg>,
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

#[pyclass(name = "DatapodPublisher", subclass)]
pub struct PyDatapodPublisher {
    pub(crate) publisher: Publisher<DatapodMsg>,
}

impl From<Publisher<DatapodMsg>> for PyDatapodPublisher {
    fn from(publisher: Publisher<DatapodMsg>) -> Self {
        Self { publisher }
    }
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

#[pyclass(name = "DatapodSubscriber", subclass)]
pub struct PyDatapodSubscriber {
    pub(crate) subscriber: Subscriber<DatapodMsg>,
}

impl From<Subscriber<DatapodMsg>> for PyDatapodSubscriber {
    fn from(subscriber: Subscriber<DatapodMsg>) -> Self {
        Self { subscriber }
    }
}

/// Borrowed zero-copy datapod sample.
///
/// For local SHM this object pins the underlying slot until it and all
/// memoryviews derived from it are dropped. `memoryview(sample)` or
/// `sample.wire_view()` exposes the datapod wire bytes without copying.
#[pyclass(name = "DatapodSampleView")]
pub struct PyDatapodSampleView {
    pub(crate) sample: NodeSample<DatapodMsg>,
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
