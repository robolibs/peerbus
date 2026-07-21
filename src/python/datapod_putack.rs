use super::*;

#[pyclass(name = "DatapodPutClient")]
pub struct PyDatapodPutClient {
    pub(crate) client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
}

#[pyclass(name = "DatapodPutUpload")]
pub struct PyDatapodPutUpload {
    pub(crate) client: Arc<Mutex<PutClient<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PutUploadToken>,
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
    pub(crate) server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
}

pub(crate) struct PendingDatapodPutState {
    pub(crate) server: Arc<Mutex<AckServer<DatapodMsg, DatapodMsg>>>,
    pub(crate) token: Option<PutAckToken>,
}

/// Batch of datapod puts handed to a `DatapodAckServer` handler. Mirrors the
/// raw `PutBatch`: it is list-like over the received `DatapodMessage` items and
/// exposes `ack(...)` / `ack_wire(...)` / `ack_message(...)` to choose the ack.
#[pyclass(name = "DatapodPutBatch")]
#[derive(Clone)]
pub struct PyDatapodPutBatch {
    pub(crate) items: Vec<PyDatapodMessage>,
    pub(crate) ack: Arc<Mutex<Option<PyDatapodMessage>>>,
}

/// Poll-driven pending datapod upload returned by `DatapodAckServer.take()`.
/// Mirrors `PendingPut`: the received puts are drained into `items` and a single
/// ack is sent with `ack(...)` / `ack_wire(...)` / `ack_message(...)`.
#[pyclass(name = "PendingDatapodPut")]
#[derive(Clone)]
pub struct PyPendingDatapodPut {
    pub(crate) items: Vec<PyDatapodMessage>,
    pub(crate) req_id: u64,
    pub(crate) state: Arc<Mutex<PendingDatapodPutState>>,
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

