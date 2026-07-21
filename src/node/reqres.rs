use super::*;

impl Node {
    /// Serve req/res calls for `topic`.
    ///
    /// The returned server polls both same-host SHM requests and
    /// remote iroh requests.
    pub fn req_server<Req, Res>(&self, topic: &str) -> Result<ReqServer<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.req_server_with_qos(topic, TopicQos::default())
    }

    /// Serve req/res calls for `topic` with explicit transport QoS.
    ///
    /// Only the byte-limit fields of [`TopicQos`] apply to req/res
    /// (`max_message_bytes`, `max_inflight_bytes`, `chunk_bytes`):
    /// large requests/responses are chunked and reassembled, lifting the
    /// 64 MiB single-frame cap. `delivery` is always treated as
    /// `Reliable` — req/res never drops messages.
    pub fn req_server_with_qos<Req, Res>(
        &self,
        topic: &str,
        qos: TopicQos,
    ) -> Result<ReqServer<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let route_topic = self.route_topic(topic)?;
        let req_type_hash = wire_type_hash::<Req>();
        let res_type_hash = wire_type_hash::<Res>();
        let req_header_size = std::mem::size_of::<Req::Header>() as u32;
        let res_header_size = std::mem::size_of::<Res::Header>() as u32;

        let stats = Arc::new(ItemStatsInner::default());
        let (remote_tx, remote_rx) = tokio::sync::mpsc::channel(DEFAULT_BROADCAST_CAPACITY);
        {
            let mut map = crate::trace::recover_poison(
                self.inner.request_topics.lock(),
                "Node::request_topics",
            );
            if map.contains_key(&route_topic) {
                return Err(Error::invalid_argument(format!(
                    "a req/res server is already registered for topic '{route_topic}'"
                )));
            }
            map.insert(
                route_topic.clone(),
                RequestTopicState {
                    tx: remote_tx,
                    req_type_hash,
                    res_type_hash,
                    req_header_size,
                    res_header_size,
                    qos,
                    stats: stats.clone(),
                },
            );
        }

        let mut local_servers = Vec::new();
        let primary_name = self.primary_service_name(topic)?;
        let primary_service = LocalReqResService::<Req, Res>::open_or_create(
            &primary_name,
            self.inner.local_cfg.clone(),
        )?;
        let primary_server = primary_service.server()?;
        local_servers.push(LocalReqServerState {
            _service: primary_service,
            server: primary_server,
        });

        Ok(ReqServer {
            inner: self.inner.clone(),
            route_topic,
            local_servers,
            remote_rx,
            pending_remote_replies: HashMap::new(),
            next_pending_token: 0,
            stats,
        })
    }

    /// Build a req/res client for `peer` and `topic`.
    ///
    /// Same-host SHM is tried first using the same peer/topic naming
    /// rules as pub/sub; if no local req/res service exists, the call
    /// path dials the peer over iroh.
    pub fn req_client<Req, Res>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
    ) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.req_client_with_qos(peer, topic, TopicQos::default())
    }

    /// Build a req/res client for `peer` and `topic` with explicit QoS.
    ///
    /// QoS governs the remote (iroh) path only: the byte-limit fields
    /// enable chunking of large requests/responses. The local SHM path
    /// is unaffected. `delivery` is treated as `Reliable`.
    pub fn req_client_with_qos<Req, Res>(
        &self,
        peer: impl IntoPeer,
        topic: &str,
        qos: TopicQos,
    ) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        validate_topic(topic)?;
        let peer = peer.into_peer();
        let peer_bytes: [u8; 32] = *peer.endpoint_id.as_bytes();

        let svc_name = service_name(&peer_bytes, topic);
        if let Ok(svc) = LocalReqResService::<Req, Res>::open_existing(&svc_name) {
            return Ok(ReqClient {
                source: ReqClientSource::Local {
                    client: svc.client()?,
                },
            });
        }

        Ok(ReqClient {
            source: ReqClientSource::Remote {
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

pub(crate) struct LocalReqServerState<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) _service: LocalReqResService<Req, Res>,
    pub(crate) server: LocalReqServer<Req, Res>,
}

pub type PendingReq<'a, Req, Res> = (ReqSample<Req>, ReqReply<'a, Req, Res>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReqReplyToken {
    pub(crate) req_id: u64,
    pub(crate) source: ReplyTokenSource,
}

impl ReqReplyToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyTokenSource {
    Local { server_index: usize },
    Remote { token: u64 },
}

pub struct PendingReqMessage<Req>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) sample: ReqSample<Req>,
    pub(crate) reply: ReqReplyToken,
}

impl<Req> PendingReqMessage<Req>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn sample(&self) -> &ReqSample<Req> {
        &self.sample
    }

    pub fn into_parts(self) -> (ReqSample<Req>, ReqReplyToken) {
        (self.sample, self.reply)
    }
}

pub struct ReqServer<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) inner: Arc<NodeInner>,
    pub(crate) route_topic: String,
    pub(crate) local_servers: Vec<LocalReqServerState<Req, Res>>,
    pub(crate) remote_rx: tokio::sync::mpsc::Receiver<RemotePendingReq>,
    pub(crate) pending_remote_replies: HashMap<u64, RemoteReqReply>,
    pub(crate) next_pending_token: u64,
    pub(crate) stats: Arc<ItemStatsInner>,
}

impl<Req, Res> Drop for ReqServer<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    fn drop(&mut self) {
        crate::trace::recover_poison(self.inner.request_topics.lock(), "Node::request_topics")
            .remove(&self.route_topic);
    }
}

impl<Req, Res> ReqServer<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this req/res server.
    pub fn stats(&self) -> ReqStats {
        self.stats.snapshot()
    }

    pub fn take(&mut self) -> Result<Option<PendingReq<'_, Req, Res>>> {
        for local in &mut self.local_servers {
            if let Some((req, reply)) = local.server.take_request()? {
                let sample = ReqSample {
                    req_id: req.req_id(),
                    header: *req.header(),
                    payload: req.payload().to_vec(),
                };
                return Ok(Some((sample, ReqReply::Local(reply))));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                let sample = req_sample_from_frame::<Req>(pending.req_id, &pending.frame)?;
                Ok(Some((
                    sample,
                    ReqReply::Remote {
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

    /// Block until a request is pending or `timeout` elapses.
    ///
    /// Unlike [`ReqServer::take`], which returns `Ok(None)` the instant
    /// there is no pending request, this polls the non-blocking `take`
    /// on a short interval (50 µs, matching the local req/res call loop)
    /// until a request arrives or the deadline passes, then returns
    /// `Ok(None)`. `take` semantics are unchanged.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<PendingReq<'_, Req, Res>>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // SAFETY: reborrow through a raw pointer to return a borrow
            // of `self` from inside a poll loop. This sidesteps the
            // current borrow checker's inability to express that the
            // borrow taken on the returning iteration outlives the
            // borrows from earlier, discarded iterations (NLL problem
            // case #3). Only one borrow is ever live at a time: each
            // non-returning iteration drops `this` before the next, and
            // the returned value is produced on the final iteration.
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

    pub fn take_message(&mut self) -> Result<Option<PendingReqMessage<Req>>> {
        for (server_index, local) in self.local_servers.iter_mut().enumerate() {
            if let Some((req, _reply)) = local.server.take_request()? {
                let req_id = req.req_id();
                let sample = ReqSample {
                    req_id,
                    header: *req.header(),
                    payload: req.payload().to_vec(),
                };
                return Ok(Some(PendingReqMessage {
                    sample,
                    reply: ReqReplyToken {
                        req_id,
                        source: ReplyTokenSource::Local { server_index },
                    },
                }));
            }
        }

        match self.remote_rx.try_recv() {
            Ok(pending) => {
                self.next_pending_token = self.next_pending_token.wrapping_add(1).max(1);
                let token = self.next_pending_token;
                let sample = req_sample_from_frame::<Req>(pending.req_id, &pending.frame)?;
                self.pending_remote_replies.insert(
                    token,
                    RemoteReqReply {
                        send: Some(pending.send),
                        qos: pending.qos,
                        peer_chunks: pending.peer_chunks,
                        stats: pending.stats,
                    },
                );
                Ok(Some(PendingReqMessage {
                    sample,
                    reply: ReqReplyToken {
                        req_id: pending.req_id,
                        source: ReplyTokenSource::Remote { token },
                    },
                }))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn respond_pending(&mut self, reply: ReqReplyToken, res: &Res) -> Result<()> {
        match reply.source {
            ReplyTokenSource::Local { server_index } => {
                let local = self
                    .local_servers
                    .get_mut(server_index)
                    .ok_or_else(|| Error::invalid_argument("invalid local req/res reply token"))?;
                local.server.respond_to(reply.req_id, res)
            }
            ReplyTokenSource::Remote { token } => {
                let mut reply = self.pending_remote_replies.remove(&token).ok_or_else(|| {
                    Error::invalid_argument("invalid or already used remote req/res reply token")
                })?;
                reply.respond(&self.inner.rt, res)
            }
        }
    }
}

pub struct ReqClient<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) source: ReqClientSource<Req, Res>,
}

pub(crate) enum ReqClientSource<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalReqClient<Req, Res>,
    },
    Remote {
        inner: Arc<NodeInner>,
        peer_id: EndpointId,
        addr_hint: Option<EndpointAddr>,
        topic: String,
        next_id: AtomicU64,
        qos: TopicQos,
        stats: Arc<ItemStatsInner>,
    },
}

impl<Req, Res> ReqClient<Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this req/res client.
    pub fn stats(&self) -> ReqStats {
        match &self.source {
            ReqClientSource::Remote { stats, .. } => stats.snapshot(),
            ReqClientSource::Local { .. } => ReqStats::default(),
        }
    }

    pub fn call(&mut self, req: &Req) -> Result<ResSample<Res>> {
        match &mut self.source {
            ReqClientSource::Local { client } => {
                let sample = client.call(req)?;
                Ok(ResSample {
                    req_id: sample.req_id(),
                    header: *sample.header(),
                    payload: sample.payload().to_vec(),
                })
            }
            ReqClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let frame = frame_from_datapod(req);
                let req_len = frame.len();
                let stats = stats.clone();
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let req_type_hash = wire_type_hash::<Req>();
                let res_type_hash = wire_type_hash::<Res>();
                let req_header_size = std::mem::size_of::<Req::Header>() as u32;
                let res_header_size = std::mem::size_of::<Res::Header>() as u32;

                let rt = inner.rt.clone();
                let inner_for_call = inner.clone();
                let response = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner_for_call, peer_id, addr_hint).await?;
                    let (mut send, mut recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        REQRESP_MAGIC,
                        &topic,
                        req_type_hash,
                        res_type_hash,
                        req_header_size,
                        res_header_size,
                        qos,
                    )
                    .await?;
                    write_item_chunked(&mut send, &frame, qos.chunk_bytes, true).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    read_item_chunked(&mut recv, qos.max_message_bytes, qos.max_inflight_bytes)
                        .await?
                        .ok_or_else(|| {
                            Error::Remote("server closed without writing a response".to_string())
                        })
                });
                let response = match response {
                    Ok(r) => r,
                    Err(e) => {
                        stats.record_error();
                        return Err(e);
                    }
                };
                stats.record_out(req_len);
                stats.record_in(response.len());
                res_sample_from_frame::<Res>(req_id, &response)
            }
        }
    }
}

pub struct ReqSample<Req: datapod::DataPod + 'static> {
    pub(crate) req_id: u64,
    pub(crate) header: Req::Header,
    pub(crate) payload: Vec<u8>,
}

impl<Req: datapod::DataPod + 'static> ReqSample<Req> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Req::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub struct ResSample<Res: datapod::DataPod + 'static> {
    pub(crate) req_id: u64,
    pub(crate) header: Res::Header,
    pub(crate) payload: Vec<u8>,
}

impl<Res: datapod::DataPod + 'static> ResSample<Res> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Res::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum ReqReply<'a, Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::ReplyHandle<'a, Req, Res>),
    Remote {
        req_id: u64,
        send: Option<iroh::endpoint::SendStream>,
        rt: Arc<Runtime>,
        qos: TopicQos,
        peer_chunks: bool,
        stats: Arc<ItemStatsInner>,
        _phantom: PhantomData<fn() -> (Req, Res)>,
    },
}

impl<Req, Res> ReqReply<'_, Req, Res>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        match self {
            ReqReply::Local(reply) => reply.req_id(),
            ReqReply::Remote { req_id, .. } => *req_id,
        }
    }

    pub fn respond(self, res: &Res) -> Result<()> {
        match self {
            ReqReply::Local(reply) => reply.respond(res),
            ReqReply::Remote {
                send: mut send_opt,
                rt,
                qos,
                peer_chunks,
                stats,
                ..
            } => {
                let mut send = send_opt
                    .take()
                    .ok_or_else(|| Error::Remote("response already sent".to_string()))?;
                let frame = frame_from_datapod(res);
                let res_len = frame.len();
                let result = rt.block_on(async move {
                    write_item_chunked(&mut send, &frame, qos.chunk_bytes, peer_chunks).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    Ok(())
                });
                match result {
                    Ok(()) => {
                        stats.record_out(res_len);
                        Ok(())
                    }
                    Err(e) => {
                        stats.record_error();
                        Err(e)
                    }
                }
            }
        }
    }
}

pub(crate) struct RemotePendingReq {
    pub(crate) req_id: u64,
    pub(crate) frame: Vec<u8>,
    pub(crate) send: iroh::endpoint::SendStream,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
}

pub(crate) struct RemoteReqReply {
    pub(crate) send: Option<iroh::endpoint::SendStream>,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
}

impl RemoteReqReply {
    pub(crate) fn respond<Res>(&mut self, rt: &Runtime, res: &Res) -> Result<()>
    where
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("response already sent".to_string()))?;
        let frame = frame_from_datapod(res);
        let res_len = frame.len();
        let qos = self.qos;
        let peer_chunks = self.peer_chunks;
        let result = rt.block_on(async move {
            write_item_chunked(&mut send, &frame, qos.chunk_bytes, peer_chunks).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok(())
        });
        match result {
            Ok(()) => {
                self.stats.record_out(res_len);
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }
}
