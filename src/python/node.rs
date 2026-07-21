use super::*;

#[pyclass(name = "Node")]
pub struct PyNode {
    pub(crate) node: Node,
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

