//! Local (same-host) que/ans over SHM rings.
//!
//! Two services back the implementation: `<name>__que` for the
//! single query and `<name>__ans` for answer items plus an explicit
//! done marker. Answer items are correlated by `req_id`.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytemuck::Zeroable;

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::local::shm::{Consumer, Producer, Segment};
use crate::queans::{ANS_KIND_DONE, ANS_KIND_ITEM, AnsEnvelope};
use crate::reqres::Envelope;
use crate::transport::wire_type_hash;

const QUE_SUFFIX: &str = "__que";
const ANS_SUFFIX: &str = "__ans";
const DEFAULT_ANS_TIMEOUT: Duration = Duration::from_secs(5);

struct QueService<T: datapod::DataPod + 'static> {
    segment: Arc<Segment<Envelope<T::Header>>>,
}

impl<T: datapod::DataPod + 'static> QueService<T> {
    fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Ok(Self {
            segment: Segment::<Envelope<T::Header>>::open_or_create(
                name,
                wire_type_hash::<Envelope<T::Header>>(),
                cfg,
            )?,
        })
    }

    fn open_existing(name: &str) -> Result<Self> {
        Ok(Self {
            segment: Segment::<Envelope<T::Header>>::open_existing(
                name,
                wire_type_hash::<Envelope<T::Header>>(),
            )?,
        })
    }

    fn publisher(&self) -> Result<Producer<Envelope<T::Header>>> {
        self.segment.producer()
    }

    fn subscriber(&self) -> Result<Consumer<Envelope<T::Header>>> {
        self.segment.consumer()
    }
}

struct AnsService<T: datapod::DataPod + 'static> {
    segment: Arc<Segment<AnsEnvelope<T::Header>>>,
}

impl<T: datapod::DataPod + 'static> AnsService<T> {
    fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Ok(Self {
            segment: Segment::<AnsEnvelope<T::Header>>::open_or_create(
                name,
                wire_type_hash::<AnsEnvelope<T::Header>>(),
                cfg,
            )?,
        })
    }

    fn open_existing(name: &str) -> Result<Self> {
        Ok(Self {
            segment: Segment::<AnsEnvelope<T::Header>>::open_existing(
                name,
                wire_type_hash::<AnsEnvelope<T::Header>>(),
            )?,
        })
    }

    fn publisher(&self) -> Result<Producer<AnsEnvelope<T::Header>>> {
        self.segment.producer()
    }

    fn subscriber(&self) -> Result<Consumer<AnsEnvelope<T::Header>>> {
        self.segment.consumer()
    }
}

pub struct LocalQueAnsService<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    queries: QueService<Que>,
    answers: AnsService<Ans>,
    next_id: Arc<AtomicU64>,
}

impl<Que, Ans> LocalQueAnsService<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Ok(Self {
            queries: QueService::<Que>::open_or_create(
                &with_suffix(name, QUE_SUFFIX),
                cfg.clone(),
            )?,
            answers: AnsService::<Ans>::open_or_create(&with_suffix(name, ANS_SUFFIX), cfg)?,
            next_id: Arc::new(AtomicU64::new(crate::local::seed_id())),
        })
    }

    pub fn open_existing(name: &str) -> Result<Self> {
        Ok(Self {
            queries: QueService::<Que>::open_existing(&with_suffix(name, QUE_SUFFIX))?,
            answers: AnsService::<Ans>::open_existing(&with_suffix(name, ANS_SUFFIX))?,
            next_id: Arc::new(AtomicU64::new(crate::local::seed_id())),
        })
    }

    pub fn server(&self) -> Result<LocalAnsServer<Que, Ans>> {
        Ok(LocalAnsServer {
            queries: self.queries.subscriber()?,
            answers: self.answers.publisher()?,
        })
    }

    pub fn client(&self) -> Result<LocalQueClient<Que, Ans>> {
        Ok(LocalQueClient {
            queries: self.queries.publisher()?,
            answers: self.answers.subscriber()?,
            next_id: Arc::clone(&self.next_id),
        })
    }
}

fn with_suffix(name: &str, suffix: &str) -> String {
    format!("{name}{suffix}")
}

pub struct QueSample<Que: datapod::DataPod + 'static> {
    req_id: u64,
    header: Que::Header,
    payload: Vec<u8>,
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

pub type PendingQue<'a, Que, Ans> = (QueSample<Que>, AnsReply<'a, Que, Ans>);

pub struct LocalAnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    queries: Consumer<Envelope<Que::Header>>,
    answers: Producer<AnsEnvelope<Ans::Header>>,
}

impl<Que, Ans> LocalAnsServer<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    pub fn take(&mut self) -> Result<Option<PendingQue<'_, Que, Ans>>> {
        let Some(sample) = self.queries.take()? else {
            return Ok(None);
        };
        let req_id = sample.header().req_id;
        let que = QueSample {
            req_id,
            header: sample.header().header,
            payload: sample.payload().to_vec(),
        };
        Ok(Some((
            que,
            AnsReply {
                req_id,
                answers: &mut self.answers,
                _phantom: PhantomData,
            },
        )))
    }

    pub fn send_to(&mut self, req_id: u64, ans: &Ans) -> Result<()> {
        publish_answer(&mut self.answers, req_id, ANS_KIND_ITEM, ans)
    }

    pub fn finish_to(&mut self, req_id: u64) -> Result<()> {
        publish_done::<Ans>(&mut self.answers, req_id)
    }
}

pub struct AnsReply<'a, Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    req_id: u64,
    answers: &'a mut Producer<AnsEnvelope<Ans::Header>>,
    _phantom: PhantomData<fn() -> Que>,
}

impl<Que, Ans> AnsReply<'_, Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn send(&mut self, ans: &Ans) -> Result<()> {
        publish_answer(self.answers, self.req_id, ANS_KIND_ITEM, ans)
    }

    pub fn finish(&mut self) -> Result<()> {
        publish_done::<Ans>(self.answers, self.req_id)
    }
}

pub struct LocalQueClient<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    queries: Producer<Envelope<Que::Header>>,
    answers: Consumer<AnsEnvelope<Ans::Header>>,
    next_id: Arc<AtomicU64>,
}

impl<Que, Ans> LocalQueClient<Que, Ans>
where
    Que: datapod::DataPod + 'static,
    Ans: datapod::DataPod + 'static,
{
    pub fn send(&mut self, que: &Que) -> Result<LocalAnswers<'_, Ans>> {
        let req_id = self.next_id.fetch_add(1, Ordering::AcqRel) + 1;
        publish_query(&mut self.queries, req_id, que)?;
        Ok(LocalAnswers {
            req_id,
            answers: &mut self.answers,
            done: false,
            timeout: DEFAULT_ANS_TIMEOUT,
        })
    }
}

pub struct AnsSample<Ans: datapod::DataPod + 'static> {
    req_id: u64,
    header: Ans::Header,
    payload: Vec<u8>,
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

pub struct LocalAnswers<'a, Ans: datapod::DataPod + 'static> {
    req_id: u64,
    answers: &'a mut Consumer<AnsEnvelope<Ans::Header>>,
    done: bool,
    timeout: Duration,
}

impl<Ans: datapod::DataPod + 'static> LocalAnswers<'_, Ans> {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<AnsSample<Ans>>> {
        if self.done {
            return Ok(None);
        }
        let deadline = Instant::now() + self.timeout;
        while Instant::now() < deadline {
            match self.answers.take()? {
                Some(sample) if sample.header().req_id == self.req_id => {
                    let header = sample.header();
                    match header.kind {
                        ANS_KIND_ITEM => {
                            return Ok(Some(AnsSample {
                                req_id: self.req_id,
                                header: header.header,
                                payload: sample.payload().to_vec(),
                            }));
                        }
                        ANS_KIND_DONE => {
                            self.done = true;
                            return Ok(None);
                        }
                        kind => {
                            return Err(Error::Remote(format!(
                                "unknown local ans kind {kind} for req_id={}",
                                self.req_id
                            )));
                        }
                    }
                }
                Some(_) => {
                    // Another client's answer on this shared ring. Keep draining.
                }
                None => std::thread::sleep(Duration::from_micros(50)),
            }
        }
        Err(Error::Timeout(self.timeout))
    }
}

fn publish_query<T>(
    publisher: &mut Producer<Envelope<T::Header>>,
    req_id: u64,
    value: &T,
) -> Result<()>
where
    T: datapod::DataPod + 'static,
{
    let bytes = value.payload_bytes();
    let mut loan = publisher.loan(bytes.len())?;
    *loan.header_mut() = Envelope {
        req_id,
        header: value.header(),
    };
    loan.payload_mut().copy_from_slice(bytes);
    publisher.publish(loan)?;
    Ok(())
}

fn publish_answer<T>(
    publisher: &mut Producer<AnsEnvelope<T::Header>>,
    req_id: u64,
    kind: u8,
    value: &T,
) -> Result<()>
where
    T: datapod::DataPod + 'static,
{
    let bytes = value.payload_bytes();
    let mut loan = publisher.loan(bytes.len())?;
    *loan.header_mut() = AnsEnvelope {
        req_id,
        kind,
        reserved: [0; 7],
        header: value.header(),
    };
    loan.payload_mut().copy_from_slice(bytes);
    publisher.publish(loan)?;
    Ok(())
}

fn publish_done<T>(publisher: &mut Producer<AnsEnvelope<T::Header>>, req_id: u64) -> Result<()>
where
    T: datapod::DataPod + 'static,
{
    let mut loan = publisher.loan(0)?;
    *loan.header_mut() = AnsEnvelope {
        req_id,
        kind: ANS_KIND_DONE,
        reserved: [0; 7],
        header: T::Header::zeroed(),
    };
    publisher.publish(loan)?;
    Ok(())
}
