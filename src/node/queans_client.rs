use super::*;

pub struct QueClient<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub(crate) source: QueClientSource<Que, Ans>,
}

pub(crate) enum QueClientSource<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local {
        client: crate::local::LocalQueClient<Que, Ans>,
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

impl<Que, Ans> QueClient<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    /// Snapshot remote-path counters for this que/ans client.
    pub fn stats(&self) -> QueStats {
        match &self.source {
            QueClientSource::Remote { stats, .. } => stats.snapshot(),
            QueClientSource::Local { .. } => QueStats::default(),
        }
    }

    pub fn send(&mut self, que: &Que) -> Result<Answers<'_, Ans>> {
        match &mut self.source {
            QueClientSource::Local { client } => Ok(Answers::Local(client.send(que)?)),
            QueClientSource::Remote {
                inner,
                peer_id,
                addr_hint,
                topic,
                next_id,
                qos,
                stats,
            } => {
                let req_id = next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let frame = frame_from_datapod(que);
                let que_len = frame.len();
                let stats = stats.clone();
                let inner = inner.clone();
                let peer_id = *peer_id;
                let addr_hint = addr_hint.clone();
                let topic = topic.clone();
                let qos = *qos;
                let que_type_hash = wire_type_hash::<Que>();
                let ans_type_hash = wire_type_hash::<Ans>();
                let que_header_size = std::mem::size_of::<Que::Header>() as u32;
                let ans_header_size = std::mem::size_of::<Ans::Header>() as u32;

                let rt = inner.rt.clone();
                let recv = rt.block_on(async move {
                    let conn = ensure_peer_connection(&inner, peer_id, addr_hint).await?;
                    let (mut send, recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| Error::Remote(format!("open_bi: {e}")))?;
                    write_item_handshake(
                        &mut send,
                        QUEANS_MAGIC,
                        &topic,
                        que_type_hash,
                        ans_type_hash,
                        que_header_size,
                        ans_header_size,
                        qos,
                    )
                    .await?;
                    write_item_chunked(&mut send, &frame, qos.chunk_bytes, true).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    Ok::<_, Error>(recv)
                });
                let recv = match recv {
                    Ok(r) => r,
                    Err(e) => {
                        stats.record_error();
                        return Err(e);
                    }
                };
                stats.record_out(que_len);

                Ok(Answers::Remote(RemoteAnswers {
                    req_id,
                    recv,
                    rt,
                    done: false,
                    qos,
                    stats,
                    _phantom: PhantomData,
                }))
            }
        }
    }
}

pub struct QueSample<Que: datapod::DataPod + 'static> {
    pub(crate) req_id: u64,
    pub(crate) header: Que::Header,
    pub(crate) payload: Vec<u8>,
}

impl<Que: datapod::DataPod + 'static> QueSample<Que> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Que::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub struct AnsSample<Ans: datapod::DataPod + 'static> {
    pub(crate) req_id: u64,
    pub(crate) header: Ans::Header,
    pub(crate) payload: Vec<u8>,
}

impl<Ans: datapod::DataPod + 'static> AnsSample<Ans> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn header(&self) -> &Ans::Header {
        &self.header
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub enum AnsReply<'a, Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    Local(crate::local::AnsReply<'a, Que, Ans>),
    Remote {
        req_id: u64,
        send: Option<iroh::endpoint::SendStream>,
        rt: Arc<Runtime>,
        qos: TopicQos,
        peer_chunks: bool,
        stats: Arc<ItemStatsInner>,
        _phantom: PhantomData<fn() -> (Que, Ans)>,
    },
}

impl<Que, Ans> AnsReply<'_, Que, Ans>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    pub fn req_id(&self) -> u64 {
        match self {
            AnsReply::Local(reply) => reply.req_id(),
            AnsReply::Remote { req_id, .. } => *req_id,
        }
    }

    pub fn send(&mut self, ans: &Ans) -> Result<()> {
        match self {
            AnsReply::Local(reply) => reply.send(ans),
            AnsReply::Remote {
                send,
                rt,
                qos,
                peer_chunks,
                stats,
                ..
            } => {
                let send = send
                    .as_mut()
                    .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
                let frame = frame_from_datapod(ans);
                let frame_len = frame.len();
                let chunk_bytes = qos.chunk_bytes;
                let peer_chunks = *peer_chunks;
                let result = rt.block_on(async move {
                    write_answer_item(send, &frame, chunk_bytes, peer_chunks).await
                });
                match result {
                    Ok(()) => {
                        stats.record_out(frame_len);
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

    pub fn finish(mut self) -> Result<()> {
        match &mut self {
            AnsReply::Local(reply) => reply.finish(),
            AnsReply::Remote { send, rt, .. } => {
                let mut send = send
                    .take()
                    .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
                rt.block_on(async move {
                    write_answer_done(&mut send).await?;
                    send.finish()
                        .map_err(|e| Error::Remote(format!("finish: {e}")))?;
                    Ok(())
                })
            }
        }
    }
}

pub enum Answers<'a, Ans: datapod::DataPod + 'static> {
    Local(crate::local::LocalAnswers<'a, Ans>),
    Remote(RemoteAnswers<Ans>),
}

/// Preferred three-letter que/ans stream name.
///
/// `Answers` remains as a compatibility alias during the public naming
/// transition.
pub type AnsStream<'a, Ans> = Answers<'a, Ans>;

impl<Ans: datapod::DataPod + 'static> Answers<'_, Ans>
where
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<AnsSample<Ans>>> {
        match self {
            Answers::Local(answers) => Ok(answers.next()?.map(|sample| AnsSample {
                req_id: sample.req_id(),
                header: *sample.header(),
                payload: sample.payload().to_vec(),
            })),
            Answers::Remote(answers) => answers.next(),
        }
    }
}

pub struct RemoteAnswers<Ans: datapod::DataPod + 'static> {
    pub(crate) req_id: u64,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) rt: Arc<Runtime>,
    pub(crate) done: bool,
    pub(crate) qos: TopicQos,
    pub(crate) stats: Arc<ItemStatsInner>,
    pub(crate) _phantom: PhantomData<fn() -> Ans>,
}

impl<Ans: datapod::DataPod + 'static> RemoteAnswers<Ans>
where
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<AnsSample<Ans>>> {
        if self.done {
            return Ok(None);
        }
        let max_message_bytes = self.qos.max_message_bytes;
        let max_inflight_bytes = self.qos.max_inflight_bytes;
        let item = self.rt.block_on(async {
            read_answer_frame(&mut self.recv, max_message_bytes, max_inflight_bytes).await
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
                Ok(Some(ans_sample_from_frame::<Ans>(self.req_id, &frame)?))
            }
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }
}

pub(crate) struct RemotePendingQue {
    pub(crate) req_id: u64,
    pub(crate) frame: Vec<u8>,
    pub(crate) send: iroh::endpoint::SendStream,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
}

pub(crate) struct RemoteAnsReply {
    pub(crate) send: Option<iroh::endpoint::SendStream>,
    pub(crate) qos: TopicQos,
    pub(crate) peer_chunks: bool,
    pub(crate) stats: Arc<ItemStatsInner>,
}

impl RemoteAnsReply {
    pub(crate) fn send<Ans>(&mut self, rt: &Runtime, ans: &Ans) -> Result<()>
    where
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let send = self
            .send
            .as_mut()
            .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
        let frame = frame_from_datapod(ans);
        let frame_len = frame.len();
        let chunk_bytes = self.qos.chunk_bytes;
        let peer_chunks = self.peer_chunks;
        let result =
            rt.block_on(
                async move { write_answer_item(send, &frame, chunk_bytes, peer_chunks).await },
            );
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

    pub(crate) fn finish(&mut self, rt: &Runtime) -> Result<()> {
        let mut send = self
            .send
            .take()
            .ok_or_else(|| Error::Remote("answer stream already finished".to_string()))?;
        rt.block_on(async move {
            write_answer_done(&mut send).await?;
            send.finish()
                .map_err(|e| Error::Remote(format!("finish: {e}")))?;
            Ok(())
        })
    }
}
