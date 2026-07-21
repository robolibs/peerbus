use super::*;

impl Node {
    /// Serve pip sessions for `topic`.
    ///
    /// A pip session is bidirectional: clients send `ClientMsg`,
    /// servers send `ServerMsg`, and either direction may finish
    /// independently.
    pub fn pip_server<ClientMsg, ServerMsg>(
        &self,
        topic: &str,
    ) -> Result<PipServer<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.pip_server_with_qos(topic, TopicQos::default())
    }

    /// Serve pip sessions for `topic` with explicit transport QoS.
    ///
    /// The byte-limit fields of [`TopicQos`] chunk large messages in
    /// both directions, lifting the 64 MiB single-frame cap. `delivery`
    /// is treated as `Reliable`; pip never drops session messages.
    pub fn pip_server_with_qos<ClientMsg, ServerMsg>(
        &self,
        topic: &str,
        qos: TopicQos,
    ) -> Result<PipServer<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let client_type_hash = wire_type_hash::<ClientMsg>();
        let server_type_hash = wire_type_hash::<ServerMsg>();
        let client_header_size = std::mem::size_of::<ClientMsg::Header>() as u32;
        let server_header_size = std::mem::size_of::<ServerMsg::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map =
                crate::trace::recover_poison(self.inner.pip_topics.lock(), "Node::pip_topics");
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a pip server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                PipTopicState {
                    tx: remote_tx,
                    client_type_hash,
                    server_type_hash,
                    client_header_size,
                    server_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalPipService::<ClientMsg, ServerMsg>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalPipServerState {
            _service: primary_service,
            server: primary_server,
        });


        Ok(PipServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_sessions: HashMap::new(),
            next_pending_session: 0,
            stats,
        })
    }

    /// Build a pip client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first; if no local pip service is
    /// present, the client opens an iroh stream to the peer.
    pub fn pip_client<ClientMsg, ServerMsg>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.pip_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a pip client for `peer` and `topic` with explicit QoS.
    pub fn pip_client_with_qos<ClientMsg, ServerMsg>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let svc_name = service_name(&peer_bytes, topic);
        if let Ok(svc) = LocalPipService::<ClientMsg, ServerMsg>::open_existing(&svc_name) {
            return Ok(PipClient {
                source: PipClientSource::Local {
                    client: svc.client()?,
                },
                pending_remote_sessions: HashMap::new(),
                next_pending_session: 0,
            });
        }

        Ok(PipClient {
            source: PipClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: peer.endpoint_id,
                addr_hint: peer.addr,
                topic: topic.to_string(),
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
            pending_remote_sessions: HashMap::new(),
            next_pending_session: 0,
        })
    }

}

pub(crate) struct LocalPipServerState<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) _service: LocalPipService<ClientMsg, ServerMsg>,
    pub(crate) server: LocalPipServer<ClientMsg, ServerMsg>,
}

pub struct PipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) inner: Arc<NodeInner>,
    pub(crate) route_topic: String,
    pub(crate) local_servers: Vec<LocalPipServerState<ClientMsg, ServerMsg>>,
    pub(crate) remote_rx: tokio::sync::mpsc::Receiver<RemotePendingPip>,
    pub(crate) pending_remote_sessions: HashMap<u64, RemotePip<ServerMsg, ClientMsg>>,
    pub(crate) next_pending_session: u64,
    pub(crate) stats: Arc<ItemStatsInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipServerToken {
    pub(crate) session_id: u64,
    pub(crate) source: PipServerTokenSource,
}

impl PipServerToken {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipServerTokenSource {
    Local { server_index: usize },
    Remote { token: u64 },
}

pub struct PendingPipMessage<ClientMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) session_id: u64,
    pub(crate) first: Option<PipSample<ClientMsg>>,
    pub(crate) incoming_done: bool,
    pub(crate) token: PipServerToken,
}

impl<ClientMsg> PendingPipMessage<ClientMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn first(&self) -> Option<&PipSample<ClientMsg>> {
        self.first.as_ref()
    }

    pub fn incoming_done(&self) -> bool {
        self.incoming_done
    }

    pub fn into_parts(self) -> (u64, Option<PipSample<ClientMsg>>, bool, PipServerToken) {
        (self.session_id, self.first, self.incoming_done, self.token)
    }
}

impl<ClientMsg, ServerMsg> Drop for PipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.pip_topics.lock(), "Node::pip_topics")
            .remove(&self.route_topic);
    }
}

impl<ClientMsg, ServerMsg> PipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this pip server.
    pub fn stats(&self) -> PipStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<Pip<'_, ServerMsg, ClientMsg>>> {
        for local in &mut self.local_servers {
            if let Some(pip) = local.server.take()? {
                return Ok(Some(Pip::Local(pip)));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => Ok(Some(Pip::Remote(RemotePip {
                session_id: pending.session_id,
                send: Some(pending.send),
                recv: pending.recv,
                rt: self.inner.rt.clone(),
                incoming_done: false,
                outgoing_done: false,
                qos: pending.qos,
                peer_chunks: pending.peer_chunks,
                stats: pending.stats,
                _phantom: PhantomData,
            }))),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    /// Block until a session is pending or `timeout` elapses.
    ///
    /// Unlike [`PipServer::take`], which returns `Ok(None)` the instant
    /// there is no pending session, this polls the non-blocking `take`
    /// on a short interval (50 µs, matching the local req/res call loop)
    /// until a session arrives or the deadline passes, then returns
    /// `Ok(None)`. `take` semantics are unchanged.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Pip<'_, ServerMsg, ClientMsg>>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // SAFETY: reborrow through a raw pointer to return a borrow
            // of `self` from inside a poll loop (NLL problem case #3).
            // Only one borrow is live at a time: each non-returning
            // iteration drops `this` before the next, and the returned
            // value is produced on the final iteration.
            let this: &mut Self = unsafe { &mut *(self as *mut Self) };
            if let Some(pip) = this.take()? {
                return Ok(Some(pip));
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    pub fn take_message(&mut self) -> Result<Option<PendingPipMessage<ClientMsg>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((session_id, first, incoming_done)) = local.server.take_message()? {
                return Ok(Some(PendingPipMessage {
                    session_id,
                    first: first.map(|sample| PipSample {
                        session_id: sample.session_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }),
                    incoming_done,
                    token: PipServerToken {
                        session_id,
                        source: PipServerTokenSource::Local { server_index },
                    },
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_session = self.next_pending_session.wrapping_add(1).max(1);
                let token = self.next_pending_session;
                let session_id = pending.session_id;
                self.pending_remote_sessions.insert(
                    token,
                    RemotePip {
                        session_id,
                        send: Some(pending.send),
                        recv: pending.recv,
                        rt: self.inner.rt.clone(),
                        incoming_done: false,
                        outgoing_done: false,
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(Some(PendingPipMessage {
                    session_id,
                    first: None,
                    incoming_done: false,
                    token: PipServerToken {
                        session_id,
                        source: PipServerTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn send_pending(&mut self, token: PipServerToken, msg: &ServerMsg) -> Result<()> {
        match token.source {
            PipServerTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local pip server token"))?;
                local.server.send_to(token.session_id, msg)
            }
            PipServerTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip server token"))?;
                pip.send(msg)
            }
        }
    }

    pub fn finish_send_pending(&mut self, token: PipServerToken) -> Result<()> {
        match token.source {
            PipServerTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local pip server token"))?;
                local.server.finish_send_to(token.session_id)
            }
            PipServerTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip server token"))?;
                pip.finish_send()
            }
        }
    }

    pub fn next_pending(&mut self, token: PipServerToken) -> Result<Option<PipSample<ClientMsg>>> {
        match token.source {
            PipServerTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local pip server token"))?;
                Ok(local
                    .server
                    .next_from(token.session_id)?
                    .map(|sample| PipSample {
                        session_id: sample.session_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }))
            }
            PipServerTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip server token"))?;
                pip.next()
            }
        }
    }

    pub fn close_pending(&mut self, token: PipServerToken) {
        if let PipServerTokenSource::Remote { token } = token.source {
            self.pending_remote_sessions.remove(&token);
        }
    }
}
