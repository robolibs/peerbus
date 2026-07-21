use super::*;

#[pyclass(name = "ReqClient")]
pub struct PyReqClient {
    pub(crate) client: ReqClient<RawMsg, RawMsg>,
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
    pub(crate) server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PendingReq")]
pub struct PyPendingReq {
    pub(crate) server: Arc<Mutex<ReqServer<RawMsg, RawMsg>>>,
    pub(crate) reply: Option<ReqReplyToken>,
    pub(crate) request: PyMessage,
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
    pub(crate) client: ReqClient<DatapodMsg, DatapodMsg>,
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
    pub(crate) server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
}

#[pyclass(name = "PendingDatapodReq")]
pub struct PyPendingDatapodReq {
    pub(crate) server: Arc<Mutex<ReqServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) reply: Option<ReqReplyToken>,
    pub(crate) request: PyDatapodMessage,
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

