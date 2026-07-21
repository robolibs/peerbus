use super::*;

pub struct PutClient<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) source: PutClientSource<Put, Ack>,
    pub(crate) pending_remote_uploads: HashMap<u64, RemotePutSender<Put, Ack>>,
    pub(crate) next_pending_upload: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutUploadToken {
    pub(crate) req_id: u64,
    pub(crate) source: PutUploadTokenSource,
}

/// Preferred three-letter put/ack explicit sender token name.
///
/// `PutUploadToken` remains as a compatibility alias for older binding code.
pub type PutSenderToken = PutUploadToken;

impl PutUploadToken {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PutUploadTokenSource {
    Local,
    Remote { token: u64 },
}

pub(crate) enum PutClientSource<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalPutClient<Put, Ack>,
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

impl<Put, Ack> PutClient<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this put/ack client.
    pub fn stats(&self) -> PutStats {
        match &self.source {
            PutClientSource::Remote { stats, .. } => stats.snapshot(),
            PutClientSource::Local { .. } => PutStats::default(),
        }
    }

    pub fn open(&mut self) -> Result<PutSender<'_, Put, Ack>> {
        match &mut self.source {
            PutClientSource::Local { client } => Ok(PutSender::Local(client.open()?)),
            PutClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let put_type_hash = wire_type_hash::<Put>();
                let ack_type_hash = wire_type_hash::<Ack>();
                let put_header_size = std::mem::size_of::<Put::Header>() as u32;
                let ack_header_size = std::mem::size_of::<Ack::Header>() as u32;

                let rt = inner.rt.clone();
                let (send, recv) = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PUTACK_MAGIC,
                        &topic,
                        put_type_hash,
                        ack_type_hash,
                        put_header_size,
                        ack_header_size,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                Ok(PutSender::Remote(RemotePutSender {
                    req_id,
                    send: Some(send),
                    recv,
                    rt,
                    qos,
                    stats,
                    _phantom: PhantomData,
                }))
            }
        }
    }

    pub fn open_upload(&mut self) -> Result<PutUploadToken> {
        match &mut self.source {
            PutClientSource::Local { client } => {
                let req_id = client.open_req();
                Ok(PutUploadToken {
                    req_id,
                    source: PutUploadTokenSource::Local,
                })
            }
            PutClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let stats = stats.clone();
                let rt = inner.rt.clone();
                let send_rt = rt.clone();
                let (send, recv) = send_rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        PUTACK_MAGIC,
                        &topic,
                        wire_type_hash::<Put>(),
                        wire_type_hash::<Ack>(),
                        std::mem::size_of::<Put::Header>() as u32,
                        std::mem::size_of::<Ack::Header>() as u32,
                        qos,
                    )
                    .await?;
                    Ok::<_, Error>((send, recv))
                })?;

                self.next_pending_upload = self.next_pending_upload.wrapping_add(1).max(1);
                let token = self.next_pending_upload;
                self.pending_remote_uploads.insert(
                    token,
                    RemotePutSender {
                        req_id,
                        send: Some(send),
                        recv,
                        rt,
                        qos,
                        stats,
                        _phantom: PhantomData,
                    },
                );
                Ok(PutUploadToken {
                    req_id,
                    source: PutUploadTokenSource::Remote { token },
                })
            }
        }
    }

    pub fn send_pending(&mut self, token: PutUploadToken, put: &Put) -> Result<()> {
        match token.source {
            PutUploadTokenSource::Local => match &mut self.source {
                PutClientSource::Local { client } => client.send_to(token.req_id, put),
                PutClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local put token used with remote put client",
                )),
            },
            PutUploadTokenSource::Remote { token } => self
                .pending_remote_uploads
                .get_mut(&token)
                .ok_or_else(|| Error::invalid_argument("invalid remote put token"))?
                .send(put),
        }
    }

    pub fn finish_pending(&mut self, token: PutUploadToken) -> Result<AckSample<Ack>> {
        match token.source {
            PutUploadTokenSource::Local => match &mut self.source {
                PutClientSource::Local { client } => {
                    let sample = client.finish_req(token.req_id)?;
                    Ok(AckSample {
                        req_id: sample.req_id(),
                        header: *sample.header(),
                        payload: sample.payload().to_vec(),
                    })
                }
                PutClientSource::Remote { .. } => Err(Error::invalid_argument(
                    "local put token used with remote put client",
                )),
            },
            PutUploadTokenSource::Remote { token } => self
                .pending_remote_uploads
                .remove(&token)
                .ok_or_else(|| Error::invalid_argument("invalid remote put token"))?
                .finish(),
        }
    }
}

pub struct PutSample<Put: datapod::DataPod + 'static> {
    pub(crate) req_id: u64,
    pub(crate) header: Put::Header,
    pub(crate) payload: Vec<u8>,
}

impl<Put: datapod::DataPod + 'static> PutSample<Put> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Put::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub struct AckSample<Ack: datapod::DataPod + 'static> {
    pub(crate) req_id: u64,
    pub(crate) header: Ack::Header,
    pub(crate) payload: Vec<u8>,
}

impl<Ack: datapod::DataPod + 'static> AckSample<Ack> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Ack::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum Puts<'a, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::LocalPuts<'a, Put, Ack>),
    Remote(RemotePuts<Put, Ack>),
}

impl<Put, Ack> Puts<'_, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> Option<u64> {
        match self {
            Puts::Local(puts) => puts.req_id(),
            Puts::Remote(puts) => Some(puts.req_id),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<PutSample<Put>>> {
        match self {
            Puts::Local(puts) => Ok(puts.next()?.map(|sample| PutSample {
                req_id: sample.req_id(),
                header: *sample.header(),
                payload: sample.payload().to_vec(),
            })),
            Puts::Remote(puts) => puts.next(),
        }
    }

    pub fn ack(&mut self, ack: &Ack) -> Result<()> {
        match self {
            Puts::Local(puts) => puts.ack(ack),
            Puts::Remote(puts) => puts.ack(ack),
        }
    }
}

pub enum PutSender<'a, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::LocalPutSender<'a, Put, Ack>),
    Remote(RemotePutSender<Put, Ack>),
}

impl<Put, Ack> PutSender<'_, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        match self {
            PutSender::Local(sender) => sender.req_id(),
            PutSender::Remote(sender) => sender.req_id,
        }
    }

    pub fn send(&mut self, put: &Put) -> Result<()> {
        match self {
            PutSender::Local(sender) => sender.send(put),
            PutSender::Remote(sender) => sender.send(put),
        }
    }

    pub fn finish(self) -> Result<AckSample<Ack>> {
        match self {
            PutSender::Local(sender) => {
                let sample = sender.finish()?;
                Ok(AckSample {
                    req_id: sample.req_id(),
                    header: *sample.header(),
                    payload: sample.payload().to_vec(),
                })
            }
            PutSender::Remote(sender) => sender.finish(),
        }
    }
}

pub struct RemotePuts<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) req_id: u64,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) send: Option<iroh::endpoint::SendStream>,
    pub(crate) rt: Arc<Runtime>,
    pub(crate) done: bool,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
    pub(crate) _phantom: PhantomData<fn() -> (Put, Ack)>,
}

impl<Put, Ack> RemotePuts<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) fn next(&mut self) -> Result<Option<PutSample<Put>>> {
        if self.done {
            return Ok(None);
        }
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let item = self.rt.block_on(async {
            read_put_frame(&mut self.recv, max_message_bytes, max_inflight_bytes).await
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
                Ok(Some(put_sample_from_frame::<Put>(self.req_id, &frame)?))
            }
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }

    pub(crate) fn ack(&mut self, ack: &Ack) -> Result<()> {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("ack already sent".to_string()))?;
        let frame = frame_from_datapod(ack);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let peer_chunks = self.peer_chunks;
        let stats = self.stats.clone();
        self.rt.block_on(async move {
            write_item_chunked(&mut send, &frame, chunk_bytes, peer_chunks).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok::<(), Error>(())
        })?;
        stats.record_out(frame_len);
        Ok(())
    }
}

pub struct RemotePutSender<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) req_id: u64,
    pub(crate) send: Option<iroh::endpoint::SendStream>,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) rt: Arc<Runtime>,
    pub(crate) qos: TopicQos,
    pub(crate) stats: Arc<ItemStatsInner>,
    pub(crate) _phantom: PhantomData<fn() -> (Put, Ack)>,
}

impl<Put, Ack> RemotePutSender<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) fn send(&mut self, put: &Put) -> Result<()> {
        let send = self
            .send
            .as_mut()
            .ok_or_else(|| Error::Remote("put stream already finished".to_string()))?;
        let frame = frame_from_datapod(put);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let result = self
            .rt
            .block_on(async move { write_put_item(send, &frame, chunk_bytes, true).await });
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

    pub(crate) fn finish(mut self) -> Result<AckSample<Ack>> {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("put stream already finished".to_string()))?;
        let req_id = self.req_id;
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let frame = self.rt.block_on(async move {
            write_put_done(&mut send).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            read_item_chunked(&mut self.recv, max_message_bytes, max_inflight_bytes)
                .await?
                .ok_or_else(|| Error::Remote("server closed without writing an ack".to_string()))
        });
        let frame = match frame {
            Ok(f) => f,
            Err(e) => {
                self.stats.record_error();
                return Err(e);
            }
        };
        self.stats.record_in(frame.len());
        ack_sample_from_frame::<Ack>(req_id, &frame)
    }
}

pub(crate) struct RemotePendingPuts {
    pub(crate) req_id: u64,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) send: iroh::endpoint::SendStream,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
}
