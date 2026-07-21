use super::*;

#[pyclass(name = "DatapodPipClient")]
pub struct PyDatapodPipClient {
    pub(crate) client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
}

#[pyclass(name = "DatapodPipSession")]
pub struct PyDatapodPipSession {
    pub(crate) client: Arc<Mutex<PipClient<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PipSessionToken>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
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
    pub(crate) server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
}

pub(crate) struct PendingDatapodPipState {
    pub(crate) server: Arc<Mutex<PipServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PipServerToken>,
    pub(crate) first: Option<PyDatapodMessage>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
}

#[pyclass(name = "PendingDatapodPip")]
#[derive(Clone)]
pub struct PyPendingDatapodPip {
    pub(crate) state: Arc<Mutex<PendingDatapodPipState>>,
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

