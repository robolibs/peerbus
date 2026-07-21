use super::*;

#[pyclass(name = "PutClient")]
pub struct PyPutClient {
    pub(crate) client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
}

#[pyclass(name = "PutUpload")]
pub struct PyPutUpload {
    pub(crate) client: Arc<Mutex<PutClient<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PutUploadToken>,
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
    pub(crate) items: Vec<PyMessage>,
    pub(crate) ack: Arc<Mutex<Option<PyMessage>>>,
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
    pub(crate) server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
}

pub(crate) struct PendingPutState {
    pub(crate) server: Arc<Mutex<AckServer<RawMsg, RawMsg>>>,
    pub(crate) token: Option<PutAckToken>,
}

/// Poll-driven pending upload returned by `AckServer.take()`. Mirrors
/// `PendingReq`/`PutBatch`: the received puts are drained into `items` and a
/// single ack is sent with `ack(...)` / `ack_message(...)` / `ack_pod(...)`.
#[pyclass(name = "PendingPut")]
#[derive(Clone)]
pub struct PyPendingPut {
    pub(crate) items: Vec<PyMessage>,
    pub(crate) req_id: u64,
    pub(crate) state: Arc<Mutex<PendingPutState>>,
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

