//! Local (same-host) put/ack over SHM rings.
//!
//! Two services back the implementation: `<name>__put` for upload
//! items plus a done marker, and `<name>__ack` for the final
//! acknowledgement correlated by `req_id`.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytemuck::Zeroable;

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::local::shm::{Consumer, Producer, Segment};
use crate::putack::{PUT_KIND_DONE, PUT_KIND_ITEM, PutEnvelope};
use crate::reqresp::Envelope;
use crate::transport::wire_type_hash;

const PUT_SUFFIX: &str = "__put";
const ACK_SUFFIX: &str = "__ack";
const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(5);

struct PutService<T: datapod::DataPod + 'static> {
    segment: Arc<Segment<PutEnvelope<T::Header>>>,
}

impl<T: datapod::DataPod + 'static> PutService<T> {
    fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Ok(Self {
            segment: Segment::<PutEnvelope<T::Header>>::open_or_create(
                name,
                wire_type_hash::<PutEnvelope<T::Header>>(),
                cfg,
            )?,
        })
    }

    fn open_existing(name: &str) -> Result<Self> {
        Ok(Self {
            segment: Segment::<PutEnvelope<T::Header>>::open_existing(
                name,
                wire_type_hash::<PutEnvelope<T::Header>>(),
            )?,
        })
    }

    fn publisher(&self) -> Result<Producer<PutEnvelope<T::Header>>> {
        self.segment.producer()
    }

    fn subscriber(&self) -> Result<Consumer<PutEnvelope<T::Header>>> {
        self.segment.consumer()
    }
}

struct AckService<T: datapod::DataPod + 'static> {
    segment: Arc<Segment<Envelope<T::Header>>>,
}

impl<T: datapod::DataPod + 'static> AckService<T> {
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

pub struct LocalPutAckService<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    puts: PutService<Put>,
    acks: AckService<Ack>,
    next_id: Arc<AtomicU64>,
}

impl<Put, Ack> LocalPutAckService<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Ok(Self {
            puts: PutService::<Put>::open_or_create(&with_suffix(name, PUT_SUFFIX), cfg.clone())?,
            acks: AckService::<Ack>::open_or_create(&with_suffix(name, ACK_SUFFIX), cfg)?,
            next_id: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn open_existing(name: &str) -> Result<Self> {
        Ok(Self {
            puts: PutService::<Put>::open_existing(&with_suffix(name, PUT_SUFFIX))?,
            acks: AckService::<Ack>::open_existing(&with_suffix(name, ACK_SUFFIX))?,
            next_id: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn server(&self) -> Result<LocalAckServer<Put, Ack>> {
        Ok(LocalAckServer {
            puts: self.puts.subscriber()?,
            acks: self.acks.publisher()?,
        })
    }

    pub fn client(&self) -> Result<LocalPutClient<Put, Ack>> {
        Ok(LocalPutClient {
            puts: self.puts.publisher()?,
            acks: self.acks.subscriber()?,
            next_id: Arc::clone(&self.next_id),
        })
    }
}

fn with_suffix(name: &str, suffix: &str) -> String {
    format!("{name}{suffix}")
}

pub struct PutSample<Put: datapod::DataPod + 'static> {
    req_id: u64,
    header: Put::Header,
    payload: Vec<u8>,
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
    req_id: u64,
    header: Ack::Header,
    payload: Vec<u8>,
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

pub struct LocalAckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    puts: Consumer<PutEnvelope<Put::Header>>,
    acks: Producer<Envelope<Ack::Header>>,
}

impl<Put, Ack> LocalAckServer<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    pub fn take(&mut self) -> Result<Option<LocalPuts<'_, Put, Ack>>> {
        let Some(sample) = self.puts.take()? else {
            return Ok(None);
        };
        let header = sample.header();
        let req_id = header.req_id;
        let (pending, done) = match header.kind {
            PUT_KIND_ITEM => (
                Some(PutSample {
                    req_id,
                    header: header.header,
                    payload: sample.payload().to_vec(),
                }),
                false,
            ),
            PUT_KIND_DONE => (None, true),
            kind => {
                return Err(Error::Remote(format!(
                    "unknown local put kind {kind} for req_id={req_id}"
                )));
            }
        };
        Ok(Some(LocalPuts {
            req_id,
            pending,
            done,
            puts: &mut self.puts,
            acks: &mut self.acks,
            _phantom: PhantomData,
        }))
    }
}

pub struct LocalPuts<'a, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    req_id: u64,
    pending: Option<PutSample<Put>>,
    done: bool,
    puts: &'a mut Consumer<PutEnvelope<Put::Header>>,
    acks: &'a mut Producer<Envelope<Ack::Header>>,
    _phantom: PhantomData<fn() -> Ack>,
}

impl<Put, Ack> LocalPuts<'_, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    pub fn req_id(&self) -> Option<u64> {
        Some(self.req_id)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<PutSample<Put>>> {
        if let Some(sample) = self.pending.take() {
            return Ok(Some(sample));
        }
        if self.done {
            return Ok(None);
        }
        loop {
            let Some(sample) = self.puts.take()? else {
                return Ok(None);
            };
            let header = sample.header();
            if header.req_id != self.req_id {
                // Belongs to a different concurrent put session on the
                // same ring. Keep draining.
                continue;
            }
            match header.kind {
                PUT_KIND_ITEM => {
                    return Ok(Some(PutSample {
                        req_id: header.req_id,
                        header: header.header,
                        payload: sample.payload().to_vec(),
                    }));
                }
                PUT_KIND_DONE => {
                    self.done = true;
                    return Ok(None);
                }
                kind => {
                    return Err(Error::Remote(format!(
                        "unknown local put kind {kind} for req_id={}",
                        header.req_id
                    )));
                }
            }
        }
    }

    pub fn ack(&mut self, ack: &Ack) -> Result<()> {
        publish_ack(self.acks, self.req_id, ack)
    }
}

pub struct LocalPutClient<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    puts: Producer<PutEnvelope<Put::Header>>,
    acks: Consumer<Envelope<Ack::Header>>,
    next_id: Arc<AtomicU64>,
}

impl<Put, Ack> LocalPutClient<Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    pub fn open(&mut self) -> Result<LocalPutSender<'_, Put, Ack>> {
        let req_id = self.next_id.fetch_add(1, Ordering::AcqRel) + 1;
        Ok(LocalPutSender {
            req_id,
            puts: &mut self.puts,
            acks: &mut self.acks,
            finished: false,
            timeout: DEFAULT_ACK_TIMEOUT,
            _phantom: PhantomData,
        })
    }
}

pub struct LocalPutSender<'a, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    req_id: u64,
    puts: &'a mut Producer<PutEnvelope<Put::Header>>,
    acks: &'a mut Consumer<Envelope<Ack::Header>>,
    finished: bool,
    timeout: Duration,
    _phantom: PhantomData<fn() -> Ack>,
}

impl<Put, Ack> LocalPutSender<'_, Put, Ack>
where
    Put: datapod::DataPod + 'static,
    Ack: datapod::DataPod + 'static,
{
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn send(&mut self, put: &Put) -> Result<()> {
        if self.finished {
            return Err(Error::invalid_argument("put stream already finished"));
        }
        publish_put(self.puts, self.req_id, PUT_KIND_ITEM, put)
    }

    pub fn finish(mut self) -> Result<AckSample<Ack>> {
        if !self.finished {
            publish_done::<Put>(self.puts, self.req_id)?;
            self.finished = true;
        }

        let deadline = Instant::now() + self.timeout;
        while Instant::now() < deadline {
            match self.acks.take()? {
                Some(sample) if sample.header().req_id == self.req_id => {
                    return Ok(AckSample {
                        req_id: self.req_id,
                        header: sample.header().header,
                        payload: sample.payload().to_vec(),
                    });
                }
                Some(_) => {
                    // Another client's ack on this shared ring.
                }
                None => std::thread::sleep(Duration::from_micros(50)),
            }
        }
        Err(Error::Timeout(self.timeout))
    }
}

fn publish_put<T>(
    publisher: &mut Producer<PutEnvelope<T::Header>>,
    req_id: u64,
    kind: u8,
    value: &T,
) -> Result<()>
where
    T: datapod::DataPod + 'static,
{
    let bytes = value.payload_bytes();
    let mut loan = publisher.loan(bytes.len())?;
    *loan.header_mut() = PutEnvelope {
        req_id,
        kind,
        reserved: [0; 7],
        header: value.header(),
    };
    loan.payload_mut().copy_from_slice(bytes);
    publisher.publish(loan)?;
    Ok(())
}

fn publish_done<T>(publisher: &mut Producer<PutEnvelope<T::Header>>, req_id: u64) -> Result<()>
where
    T: datapod::DataPod + 'static,
{
    let mut loan = publisher.loan(0)?;
    *loan.header_mut() = PutEnvelope {
        req_id,
        kind: PUT_KIND_DONE,
        reserved: [0; 7],
        header: T::Header::zeroed(),
    };
    publisher.publish(loan)?;
    Ok(())
}

fn publish_ack<T>(
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
