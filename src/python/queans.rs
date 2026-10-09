use super::*;

#[pyclass(name = "DatapodQueClient", subclass)]
pub struct PyDatapodQueClient {
    pub(crate) client: QueClient<DatapodMsg, DatapodMsg>,
}

impl From<QueClient<DatapodMsg, DatapodMsg>> for PyDatapodQueClient {
    fn from(client: QueClient<DatapodMsg, DatapodMsg>) -> Self {
        Self { client }
    }
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

#[pyclass(name = "DatapodAnsServer", subclass)]
pub struct PyDatapodAnsServer {
    pub(crate) server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
}

impl From<AnsServer<DatapodMsg, DatapodMsg>> for PyDatapodAnsServer {
    fn from(server: AnsServer<DatapodMsg, DatapodMsg>) -> Self {
        Self {
            server: Arc::new(Mutex::new(server)),
        }
    }
}

#[pyclass(name = "PendingDatapodQue")]
pub struct PyPendingDatapodQue {
    pub(crate) request: PyDatapodMessage,
    pub(crate) answers: PyPendingDatapodAnswers,
}

pub(crate) struct PendingDatapodAnswersState {
    pub(crate) server: Arc<Mutex<AnsServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<AnsReplyToken>,
}

#[pyclass(name = "PendingDatapodAnswers")]
#[derive(Clone)]
pub struct PyPendingDatapodAnswers {
    pub(crate) state: Arc<Mutex<PendingDatapodAnswersState>>,
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
    pub(crate) client: QueClient<RawMsg, RawMsg>,
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
    pub(crate) server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PendingQue")]
pub struct PyPendingQue {
    pub(crate) request: PyMessage,
    pub(crate) answers: PyPendingAnswers,
}

pub(crate) struct PendingAnswersState {
    pub(crate) server: Arc<Mutex<AnsServer<RawMsg, RawMsg>>>,
    pub(crate) token: Option<AnsReplyToken>,
}

#[pyclass(name = "PendingAnswers")]
#[derive(Clone)]
pub struct PyPendingAnswers {
    pub(crate) state: Arc<Mutex<PendingAnswersState>>,
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
