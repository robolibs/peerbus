use super::*;

#[pyclass(name = "PipClient")]
pub struct PyPipClient {
    pub(crate) client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PipSession")]
pub struct PyPipSession {
    pub(crate) client: Arc<Mutex<PipClient<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PipSessionToken>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
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
    pub(crate) server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
}

pub(crate) struct PendingPipState {
    pub(crate) server: Arc<Mutex<PipServer<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PipServerToken>,
    pub(crate) first: Option<PyMessage>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
}

#[pyclass(name = "PendingPip")]
#[derive(Clone)]
pub struct PyPendingPip {
    pub(crate) state: Arc<Mutex<PendingPipState>>,
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

