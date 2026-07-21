use super::*;

impl Node {
    /// Serve put/ack uploads for `topic`.
    ///
    /// A put/ack server receives zero or more put items, then sends
    /// one final ack.
    ///
    /// This is the naming-parity counterpart to [`Node::req_server`] and
    /// [`Node::pip_server`]; it is an exact alias of the older
    /// [`Node::ack`].
    #[allow(deprecated)]
    pub fn put_server<Put, Ack>(&self, topic: &str) -> Result<AckServer<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.ack(topic)
    }

    /// Serve put/ack uploads for `topic`.
    ///
    /// A put/ack server receives zero or more put items, then sends
    /// one final ack.
    #[deprecated(note = "renamed to que_server/put_server for naming parity; will be removed pre-1.0")]
    pub fn ack<Put, Ack>(&self, topic: &str) -> Result<AckServer<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.ack_with_qos(topic, TopicQos::default())
    }

    /// Serve put/ack uploads for `topic` with explicit transport QoS.
    ///
    /// The byte-limit fields of [`TopicQos`] chunk large put items and
    /// acks, lifting the 64 MiB single-frame cap. `delivery` is treated
    /// as `Reliable`; put/ack never drops uploaded items.
    pub fn ack_with_qos<Put, Ack>(&self, topic: &str, qos: TopicQos) -> Result<AckServer<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let put_type_hash = wire_type_hash::<Put>();
        let ack_type_hash = wire_type_hash::<Ack>();
        let put_header_size = std::mem::size_of::<Put::Header>() as u32;
        let ack_header_size = std::mem::size_of::<Ack::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map =
                crate::trace::recover_poison(self.inner.put_topics.lock(), "Node::put_topics");
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a put/ack server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                PutTopicState {
                    tx: remote_tx,
                    put_type_hash,
                    ack_type_hash,
                    put_header_size,
                    ack_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalPutAckService::<Put, Ack>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalAckServerState {
            _service: primary_service,
            server: primary_server,
        });

        Ok(AckServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_puts: HashMap::new(),
            next_pending_token: 0,
            stats,
        })
    }

    /// Build a put/ack client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first; if no local put/ack service is
    /// present, the client opens an iroh stream to the peer.
    pub fn put_client<Put, Ack>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.put_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a put/ack client for `peer` and `topic` with explicit QoS.
    pub fn put_client_with_qos<Put, Ack>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let svc_name = service_name(&peer_bytes, topic);
        if let Ok(svc) = LocalPutAckService::<Put, Ack>::open_existing(&svc_name) {
            return Ok(PutClient {
                source: PutClientSource::Local {
                    client: svc.client()?,
                },
                pending_remote_uploads: HashMap::new(),
                next_pending_upload: 0,
            });
        }

        Ok(PutClient {
            source: PutClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: peer.endpoint_id,
                addr_hint: peer.addr,
                topic: topic.to_string(),
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
            pending_remote_uploads: HashMap::new(),
            next_pending_upload: 0,
        })
    }

}

pub(crate) struct LocalAckServerState<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) _service: LocalPutAckService<Put, Ack>,
    pub(crate) server: LocalAckServer<Put, Ack>,
}

pub struct AckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) inner: Arc<NodeInner>,
    pub(crate) route_topic: String,
    pub(crate) local_servers: Vec<LocalAckServerState<Put, Ack>>,
    pub(crate) remote_rx: tokio::sync::mpsc::Receiver<RemotePendingPuts>,
    pub(crate) pending_remote_puts: HashMap<u64, RemotePuts<Put, Ack>>,
    pub(crate) next_pending_token: u64,
    pub(crate) stats: Arc<ItemStatsInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutAckToken {
    pub(crate) req_id: u64,
    pub(crate) source: PutAckTokenSource,
}

impl PutAckToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PutAckTokenSource {
    Local { server_index: usize },
    Remote { token: u64 },
}

pub struct PendingPutMessage<Put>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) req_id: u64,
    pub(crate) first: Option<PutSample<Put>>,
    pub(crate) done: bool,
    pub(crate) token: PutAckToken,
}

impl<Put> PendingPutMessage<Put>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn first(&self) -> Option<&PutSample<Put>> {
        self.first.as_ref()
    }

    pub fn done(&self) -> bool {
        self.done
    }

    pub fn into_parts(self) -> (u64, Option<PutSample<Put>>, bool, PutAckToken) {
        (self.req_id, self.first, self.done, self.token)
    }
}

impl<Put, Ack> Drop for AckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.put_topics.lock(), "Node::put_topics")
            .remove(&self.route_topic);
    }
}

impl<Put, Ack> AckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this put/ack server.
    pub fn stats(&self) -> PutStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<Puts<'_, Put, Ack>>> {
        for local in &mut self.local_servers {
            if let Some(puts) = local.server.take()? {
                return Ok(Some(Puts::Local(puts)));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => Ok(Some(Puts::Remote(RemotePuts {
                req_id: pending.req_id,
                recv: pending.recv,
                send: Some(pending.send),
                rt: self.inner.rt.clone(),
                done: false,
                qos: pending.qos,
                peer_chunks: pending.peer_chunks,
                stats: pending.stats,
                _phantom: PhantomData,
            }))),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    /// Block until an upload is pending or `timeout` elapses.
    ///
    /// Unlike [`AckServer::take`], which returns `Ok(None)` the instant
    /// there is no pending upload, this polls the non-blocking `take` on
    /// a short interval (50 µs, matching the local req/res call loop)
    /// until an upload arrives or the deadline passes, then returns
    /// `Ok(None)`. `take` semantics are unchanged.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Puts<'_, Put, Ack>>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // SAFETY: reborrow through a raw pointer to return a borrow
            // of `self` from inside a poll loop (NLL problem case #3).
            // Only one borrow is live at a time: each non-returning
            // iteration drops `this` before the next, and the returned
            // value is produced on the final iteration.
            let this: &mut Self = unsafe { &mut *(self as *mut Self) };
            if let Some(puts) = this.take()? {
                return Ok(Some(puts));
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    pub fn take_message(&mut self) -> Result<Option<PendingPutMessage<Put>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((req_id, first, done)) = local.server.take_message()? {
                return Ok(Some(PendingPutMessage {
                    req_id,
                    first: first.map(|sample| PutSample {
                        req_id: sample.req_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }),
                    done,
                    token: PutAckToken {
                        req_id,
                        source: PutAckTokenSource::Local { server_index },
                    },
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_token = self.next_pending_token.wrapping_add(1).max(1);
                let token = self.next_pending_token;
                let req_id = pending.req_id;
                self.pending_remote_puts.insert(
                    token,
                    RemotePuts {
                        req_id,
                        recv: pending.recv,
                        send: Some(pending.send),
                        rt: self.inner.rt.clone(),
                        done: false,
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(Some(PendingPutMessage {
                    req_id,
                    first: None,
                    done: false,
                    token: PutAckToken {
                        req_id,
                        source: PutAckTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn next_pending(&mut self, token: PutAckToken) -> Result<Option<PutSample<Put>>> {
        match token.source {
            PutAckTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local put/ack token"))?;
                Ok(local
                    .server
                    .next_from(token.req_id)?
                    .map(|sample| PutSample {
                        req_id: sample.req_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }))
            }
            PutAckTokenSource::Remote { token } => {
                let puts = self
                    .pending_remote_puts
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote put/ack token"))?;
                puts.next()
            }
        }
    }

    pub fn ack_pending(&mut self, token: PutAckToken, ack: &Ack) -> Result<()> {
        match token.source {
            PutAckTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local put/ack token"))?;
                local.server.ack_to(token.req_id, ack)
            }
            PutAckTokenSource::Remote { token } => {
                let mut puts = self
                    .pending_remote_puts
                    .remove(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote put/ack token"))?;
                puts.ack(ack)
            }
        }
    }

    pub fn close_pending(&mut self, token: PutAckToken) {
        if let PutAckTokenSource::Remote { token } = token.source {
            self.pending_remote_puts.remove(&token);
        }
    }
}
