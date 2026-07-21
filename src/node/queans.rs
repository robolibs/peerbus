use super::*;

impl Node {
    /// Serve que/ans queries for `topic`.
    ///
    /// A que/ans server receives one query and may send zero or more
    /// answer items before calling `finish()`.
    ///
    /// This is the naming-parity counterpart to [`Node::req_server`] and
    /// [`Node::pip_server`]; it is an exact alias of the older
    /// [`Node::ans`].
    #[allow(deprecated)]
    pub fn que_server<Que, Ans>(&self, topic: &str) -> Result<AnsServer<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.ans(topic)
    }

    /// Serve que/ans queries for `topic`.
    ///
    /// A que/ans server receives one query and may send zero or more
    /// answer items before calling `finish()`.
    #[deprecated(note = "renamed to que_server/put_server for naming parity; will be removed pre-1.0")]
    pub fn ans<Que, Ans>(&self, topic: &str) -> Result<AnsServer<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.ans_with_qos(topic, TopicQos::default())
    }

    /// Serve que/ans queries for `topic` with explicit transport QoS.
    ///
    /// The byte-limit fields of [`TopicQos`] chunk large queries and
    /// answers, lifting the 64 MiB single-frame cap. `delivery` is
    /// treated as `Reliable`; que/ans never drops answer items.
    pub fn ans_with_qos<Que, Ans>(&self, topic: &str, qos: TopicQos) -> Result<AnsServer<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let que_type_hash = wire_type_hash::<Que>();
        let ans_type_hash = wire_type_hash::<Ans>();
        let que_header_size = std::mem::size_of::<Que::Header>() as u32;
        let ans_header_size = std::mem::size_of::<Ans::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map =
                crate::trace::recover_poison(self.inner.que_topics.lock(), "Node::que_topics");
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a que/ans server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                QueTopicState {
                    tx: remote_tx,
                    que_type_hash,
                    ans_type_hash,
                    que_header_size,
                    ans_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalQueAnsService::<Que, Ans>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalAnsServerState {
            _service: primary_service,
            server: primary_server,
        });

        Ok(AnsServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_replies: HashMap::new(),
            next_pending_token: 0,
            stats,
        })
    }

    /// Build a que/ans client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first; if no local que/ans service is
    /// present, the client opens an iroh stream to the peer.
    pub fn que_client<Que, Ans>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.que_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a que/ans client for `peer` and `topic` with explicit QoS.
    pub fn que_client_with_qos<Que, Ans>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let svc_name = service_name(&peer_bytes, topic);
        if let Ok(svc) = LocalQueAnsService::<Que, Ans>::open_existing(&svc_name) {
            return Ok(QueClient {
                source: QueClientSource::Local {
                    client: svc.client()?,
                },
            });
        }

        Ok(QueClient {
            source: QueClientSource::Remote {
                inner: self.inner.clone(),
                peer_id: peer.endpoint_id,
                addr_hint: peer.addr,
                topic: topic.to_string(),
                next_id: AtomicU64::new(0),
                qos,
                stats: Arc::new(ItemStatsInner::default()),
            },
        })
    }

}

pub(crate) struct LocalAnsServerState<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) _service: LocalQueAnsService<Que, Ans>,
    pub(crate) server: LocalAnsServer<Que, Ans>,
}

pub type PendingQue<'a, Que, Ans> = (QueSample<Que>, AnsReply<'a, Que, Ans>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnsReplyToken {
    pub(crate) req_id: u64,
    pub(crate) source: ReplyTokenSource,
}

impl AnsReplyToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

pub struct PendingQueMessage<Que>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) sample: QueSample<Que>,
    pub(crate) answers: AnsReplyToken,
}

impl<Que> PendingQueMessage<Que>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn sample(&self) -> &QueSample<Que> {
        &self.sample
    }

    pub fn into_parts(self) -> (QueSample<Que>, AnsReplyToken) {
        (self.sample, self.answers)
    }
}

pub struct AnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) inner: Arc<NodeInner>,
    pub(crate) route_topic: String,
    pub(crate) local_servers: Vec<LocalAnsServerState<Que, Ans>>,
    pub(crate) remote_rx: tokio::sync::mpsc::Receiver<RemotePendingQue>,
    pub(crate) pending_remote_replies: HashMap<u64, RemoteAnsReply>,
    pub(crate) next_pending_token: u64,
    pub(crate) stats: Arc<ItemStatsInner>,
}

impl<Que, Ans> Drop for AnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.que_topics.lock(), "Node::que_topics")
            .remove(&self.route_topic);
    }
}

impl<Que, Ans> AnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this que/ans server.
    pub fn stats(&self) -> QueStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<PendingQue<'_, Que, Ans>>> {
        for local in &mut self.local_servers {
            if let Some((que, reply)) = local.server.take()? {
                let sample = QueSample {
                    req_id: que.req_id(),
                    header: *que.header(),
                    payload: que.payload().to_vec(),
                };
                return Ok(Some((sample, AnsReply::Local(reply))));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                let sample = que_sample_from_frame::<Que>(pending.req_id, &pending.frame)?;
                Ok(Some((
                    sample,
                    AnsReply::Remote {
                        req_id: pending.req_id,
                        send: Some(pending.send),
                        rt: self.inner.rt.clone(),
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                        _phantom: PhantomData,
                    },
                )))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    /// Block until a query is pending or `timeout` elapses.
    ///
    /// Unlike [`AnsServer::take`], which returns `Ok(None)` the instant
    /// there is no pending query, this polls the non-blocking `take` on
    /// a short interval (50 µs, matching the local req/res call loop)
    /// until a query arrives or the deadline passes, then returns
    /// `Ok(None)`. `take` semantics are unchanged.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<PendingQue<'_, Que, Ans>>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // SAFETY: reborrow through a raw pointer to return a borrow
            // of `self` from inside a poll loop (NLL problem case #3).
            // Only one borrow is live at a time: each non-returning
            // iteration drops `this` before the next, and the returned
            // value is produced on the final iteration.
            let this: &mut Self = unsafe { &mut *(self as *mut Self) };
            if let Some(pending) = this.take()? {
                return Ok(Some(pending));
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    pub fn take_message(&mut self) -> Result<Option<PendingQueMessage<Que>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((que, _reply)) = local.server.take()? {
                let sample = QueSample {
                    req_id: que.req_id(),
                    header: *que.header(),
                    payload: que.payload().to_vec(),
                };
                return Ok(Some(PendingQueMessage {
                    answers: AnsReplyToken {
                        req_id: sample.req_id,
                        source: ReplyTokenSource::Local { server_index },
                    },
                    sample,
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_token = self.next_pending_token.wrapping_add(1).max(1);
                let token = self.next_pending_token;
                let sample = que_sample_from_frame::<Que>(pending.req_id, &pending.frame)?;
                self.pending_remote_replies.insert(
                    token,
                    RemoteAnsReply {
                        send: Some(pending.send),
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                    },
                );
                Ok(Some(PendingQueMessage {
                    sample,
                    answers: AnsReplyToken {
                        req_id: pending.req_id,
                        source: ReplyTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn send_pending(&mut self, answers: AnsReplyToken, ans: &Ans) -> Result<()> {
        match answers.source {
            ReplyTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local que/ans token"))?;
                local.server.send_to(answers.req_id, ans)
            }
            ReplyTokenSource::Remote { token } => {
                let reply = self.pending_remote_replies.get_mut(&token).ok_or_else(|| {
                    Error::invalid_argument("invalid or finished remote que/ans token")
                })?;
                reply.send(&self.inner.rt, ans)
            }
        }
    }

    pub fn finish_pending(&mut self, answers: AnsReplyToken) -> Result<()> {
        match answers.source {
            ReplyTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local que/ans token"))?;
                local.server.finish_to(answers.req_id)
            }
            ReplyTokenSource::Remote { token } => {
                let mut reply = self.pending_remote_replies.remove(&token).ok_or_else(|| {
                    Error::invalid_argument("invalid or finished remote que/ans token")
                })?;
                reply.finish(&self.inner.rt)
            }
        }
    }
}
