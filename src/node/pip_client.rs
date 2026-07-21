use super::*;

pub struct PipClient<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) source: PipClientSource<ClientMsg, ServerMsg>,
    pub(crate) pending_remote_sessions: HashMap<u64, RemotePip<ClientMsg, ServerMsg>>,
    pub(crate) next_pending_session: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipSessionToken {
    pub(crate) session_id: u64,
    pub(crate) source: PipSessionTokenSource,
}

impl PipSessionToken {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipSessionTokenSource {
    Local,
    Remote { token: u64 },
}

pub(crate) enum PipClientSource<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalPipClient<ClientMsg, ServerMsg>,
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

impl<ClientMsg, ServerMsg> PipClient<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    ServerMsg: datapod::DataPod + 'static,
    <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this pip client.
    pub fn stats(&self) -> PipStats {
        match &self.source {
            PipClientSource::Remote { stats, .. } => stats.snapshot(),
            PipClientSource::Local { .. } => PipStats::default(),
        }
    }

    pub fn open(&mut self) -> Result<Pip<'_, ClientMsg, ServerMsg>> {
        match &mut self.source {
            PipClientSource::Local { client } => Ok(Pip::Local(client.open()?)),
            PipClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let session_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let client_type_hash = wire_type_hash::<ClientMsg>();
                let server_type_hash = wire_type_hash::<ServerMsg>();
                let client_header_size = std::mem::size_of::<ClientMsg::Header>() as u32;
                let server_header_size = std::mem::size_of::<ServerMsg::Header>() as u32;

                let rt = inner.rt.clone();
                let (send, recv) = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PIP_MAGIC,
                        &topic,
                        client_type_hash,
                        server_type_hash,
                        client_header_size,
                        server_header_size,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                Ok(Pip::Remote(RemotePip {
                    session_id,
                    send: Some(send),
                    recv,
                    rt,
                    incoming_done: false,
                    outgoing_done: false,
                    qos,
                    peer_chunks: true,
                    stats,
                    _phantom: PhantomData,
                }))
            }
        }
    }

    pub fn open_session(&mut self) -> Result<PipSessionToken> {
        match &mut self.source {
            PipClientSource::Local { client } => {
                let session_id = client.start_session();
                Ok(PipSessionToken {
                    session_id,
                    source: PipSessionTokenSource::Local,
                })
            }
            PipClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let session_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let client_type_hash = wire_type_hash::<ClientMsg>();
                let server_type_hash = wire_type_hash::<ServerMsg>();
                let client_header_size = std::mem::size_of::<ClientMsg::Header>() as u32;
                let server_header_size = std::mem::size_of::<ServerMsg::Header>() as u32;

                let rt = inner.rt.clone();
                let (send, recv) = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PIP_MAGIC,
                        &topic,
                        client_type_hash,
                        server_type_hash,
                        client_header_size,
                        server_header_size,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                self.next_pending_session = self.next_pending_session.wrapping_add(1).max(1);
                let token = self.next_pending_session;
                self.pending_remote_sessions.insert(
                    token,
                    RemotePip {
                        session_id,
                        send: Some(send),
                        recv,
                        rt,
                        incoming_done: false,
                        outgoing_done: false,
                        qos,
                        peer_chunks: true,
                        stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(PipSessionToken {
                    session_id,
                    source: PipSessionTokenSource::Remote { token },
                })
            }
        }
    }

    pub fn send_pending(&mut self, token: PipSessionToken, msg: &ClientMsg) -> Result<()> {
        match token.source {
            PipSessionTokenSource::Local => match &mut self.source {
                PipClientSource::Local { client } => client.send_to(token.session_id, msg),
                PipClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local pip token used with remote client",
                )),
            },
            PipSessionTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip token"))?;
                pip.send(msg)
            }
        }
    }

    pub fn finish_send_pending(&mut self, token: PipSessionToken) -> Result<()> {
        match token.source {
            PipSessionTokenSource::Local => match &mut self.source {
                PipClientSource::Local { client } => client.finish_send_to(token.session_id),
                PipClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local pip token used with remote client",
                )),
            },
            PipSessionTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip token"))?;
                pip.finish_send()
            }
        }
    }

    pub fn next_pending(&mut self, token: PipSessionToken) -> Result<Option<PipSample<ServerMsg>>> {
        match token.source {
            PipSessionTokenSource::Local => match &mut self.source {
                PipClientSource::Local { client } => {
                    Ok(client.next_from(token.session_id)?.map(|sample| PipSample {
                        session_id: sample.session_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    }))
                }
                PipClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local pip token used with remote client",
                )),
            },
            PipSessionTokenSource::Remote { token } => {
                let pip = self
                    .pending_remote_sessions
                    .get_mut(&token)
                    .ok_or_else(|| Error::invalid_argument("invalid remote pip token"))?;
                pip.next()
            }
        }
    }

    pub fn close_session(&mut self, token: PipSessionToken) {
        if let PipSessionTokenSource::Remote { token } = token.source {
            self.pending_remote_sessions.remove(&token);
        }
    }
}

pub struct PipSample<T: datapod::DataPod + 'static> {
    pub(crate) session_id: u64,
    pub(crate) header: T::Header,
    pub(crate) payload: Vec<u8>,
}

impl<T: datapod::DataPod + 'static> PipSample<T> {
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn header(&self) -> &T::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum Pip<'a, Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::LocalPip<'a, Tx, Rx>),
    Remote(RemotePip<Tx, Rx>),
}

impl<Tx, Rx> Pip<'_, Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn session_id(&self) -> u64 {
        match self {
            Pip::Local(pip) => pip.session_id(),
            Pip::Remote(pip) => pip.session_id,
        }
    }

    pub fn send(&mut self, msg: &Tx) -> Result<()> {
        match self {
            Pip::Local(pip) => pip.send(msg),
            Pip::Remote(pip) => pip.send(msg),
        }
    }

    pub fn finish_send(&mut self) -> Result<()> {
        match self {
            Pip::Local(pip) => pip.finish_send(),
            Pip::Remote(pip) => pip.finish_send(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<PipSample<Rx>>> {
        match self {
            Pip::Local(pip) => Ok(pip.next()?.map(|sample| PipSample {
                session_id: sample.session_id(),
                header: *sample.header(),
                payload: sample.payload().to_vec(),
            })),
            Pip::Remote(pip) => pip.next(),
        }
    }
}

pub struct RemotePip<Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) session_id: u64,
    pub(crate) send: Option<iroh::endpoint::SendStream>,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) rt: Arc<Runtime>,
    pub(crate) incoming_done: bool,
    pub(crate) outgoing_done: bool,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
    pub(crate) _phantom: PhantomData<fn() -> (Tx, Rx)>,
}

impl<Tx, Rx> RemotePip<Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    <Tx as datapod::DataPod>::Header: datapod::LeWireHeader,
    Rx: datapod::DataPod + 'static,
    <Rx as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) fn send(&mut self, msg: &Tx) -> Result<()> {
        if self.outgoing_done {
            return Err(Error::invalid_argument("pip outgoing direction is done"));
        }
        let send = self
            .send
            .as_mut()
            .ok_or_else(|| Error::Remote("pip send stream is closed".to_string()))?;
        let frame = frame_from_datapod(msg);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let peer_chunks = self.peer_chunks;
        let result = self
            .rt
            .block_on(async move { write_pip_item(send, &frame, chunk_bytes, peer_chunks).await });
        match result {
            Ok(()) => {
                self.stats.record_out(frame_len);
                Ok(())
            }
            Err(e) => {
                self.stats.record_error();
                Err(e)
            }
        }
    }

    pub(crate) fn finish_send(&mut self) -> Result<()> {
        if self.outgoing_done {
            return Ok(());
        }
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("pip send stream is closed".to_string()))?;
        self.rt.block_on(async move {
            write_pip_done(&mut send).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok::<_, Error>(())
        })?;
        self.outgoing_done = true;
        Ok(())
    }

    pub(crate) fn next(&mut self) -> Result<Option<PipSample<Rx>>> {
        if self.incoming_done {
            return Ok(None);
        }
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let item = self.rt.block_on(async {
            read_pip_frame(&mut self.recv, max_message_bytes, max_inflight_bytes).await
        });
        let item = match item {
            Ok(i) => i,
            Err(e) => {
                self.stats.record_error();
                return Err(e);
            }
        };
        match item {
            Some(frame) => {
                self.stats.record_in(frame.len());
                Ok(Some(pip_sample_from_frame::<Rx>(self.session_id, &frame)?))
            }
            None => {
                self.incoming_done = true;
                Ok(None)
            }
        }
    }
}

pub(crate) struct RemotePendingPip {
    pub(crate) session_id: u64,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) send: iroh::endpoint::SendStream,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
}
