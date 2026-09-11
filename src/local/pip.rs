//! Local (same-host) pip over two SHM rings.
//!
//! `<name>__c2s` carries client-to-server messages and `<name>__s2c`
//! carries server-to-client messages. Each message is correlated by a
//! `session_id`; done markers close one direction independently.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytemuck::Zeroable;

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::local::shm::{Consumer, Producer, Segment};
use crate::pip::{PIP_KIND_DONE, PIP_KIND_ITEM, PipEnvelope};
use crate::transport::wire_type_hash;

const C2S_SUFFIX: &str = "__c2s";
const S2C_SUFFIX: &str = "__s2c";
const DEFAULT_PIP_TIMEOUT: Duration = Duration::from_secs(5);

struct MsgService<T: datapod::DataPod + 'static> {
    segment: Arc<Segment<PipEnvelope<T::Header>>>,
}

impl<T: datapod::DataPod + 'static> MsgService<T> {
    fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Ok(Self {
            segment: Segment::<PipEnvelope<T::Header>>::open_or_create(
                name,
                wire_type_hash::<PipEnvelope<T::Header>>(),
                cfg,
            )?,
        })
    }

    fn open_existing(name: &str) -> Result<Self> {
        Ok(Self {
            segment: Segment::<PipEnvelope<T::Header>>::open_existing(
                name,
                wire_type_hash::<PipEnvelope<T::Header>>(),
            )?,
        })
    }

    fn publisher(&self) -> Result<Producer<PipEnvelope<T::Header>>> {
        self.segment.producer()
    }

    fn subscriber(&self) -> Result<Consumer<PipEnvelope<T::Header>>> {
        self.segment.consumer()
    }
}

pub struct LocalPipService<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    ServerMsg: datapod::DataPod + 'static,
{
    c2s: MsgService<ClientMsg>,
    s2c: MsgService<ServerMsg>,
    next_id: Arc<AtomicU64>,
}

impl<ClientMsg, ServerMsg> LocalPipService<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    ServerMsg: datapod::DataPod + 'static,
{
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Ok(Self {
            c2s: MsgService::<ClientMsg>::open_or_create(
                &with_suffix(name, C2S_SUFFIX),
                cfg.clone(),
            )?,
            s2c: MsgService::<ServerMsg>::open_or_create(&with_suffix(name, S2C_SUFFIX), cfg)?,
            next_id: Arc::new(AtomicU64::new(crate::local::seed_id())),
        })
    }

    pub fn open_existing(name: &str) -> Result<Self> {
        Ok(Self {
            c2s: MsgService::<ClientMsg>::open_existing(&with_suffix(name, C2S_SUFFIX))?,
            s2c: MsgService::<ServerMsg>::open_existing(&with_suffix(name, S2C_SUFFIX))?,
            next_id: Arc::new(AtomicU64::new(crate::local::seed_id())),
        })
    }

    pub fn server(&self) -> Result<LocalPipServer<ClientMsg, ServerMsg>> {
        Ok(LocalPipServer {
            incoming: self.c2s.subscriber()?,
            outgoing: self.s2c.publisher()?,
        })
    }

    pub fn client(&self) -> Result<LocalPipClient<ClientMsg, ServerMsg>> {
        Ok(LocalPipClient {
            outgoing: self.c2s.publisher()?,
            incoming: self.s2c.subscriber()?,
            next_id: Arc::clone(&self.next_id),
        })
    }
}

fn with_suffix(name: &str, suffix: &str) -> String {
    format!("{name}{suffix}")
}

pub struct PipSample<T: datapod::DataPod + 'static> {
    session_id: u64,
    header: T::Header,
    payload: Vec<u8>,
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

pub type LocalPendingPip<T> = (u64, Option<PipSample<T>>, bool);

pub struct LocalPipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    ServerMsg: datapod::DataPod + 'static,
{
    incoming: Consumer<PipEnvelope<ClientMsg::Header>>,
    outgoing: Producer<PipEnvelope<ServerMsg::Header>>,
}

impl<ClientMsg, ServerMsg> LocalPipServer<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    ServerMsg: datapod::DataPod + 'static,
{
    pub fn take(&mut self) -> Result<Option<LocalPip<'_, ServerMsg, ClientMsg>>> {
        let Some(sample) = self.incoming.take()? else {
            return Ok(None);
        };
        let header = sample.header();
        let session_id = header.session_id;
        let (pending, incoming_done) = match header.kind {
            PIP_KIND_ITEM => (
                Some(PipSample {
                    session_id,
                    header: header.header,
                    payload: sample.payload().to_vec(),
                }),
                false,
            ),
            PIP_KIND_DONE => (None, true),
            kind => {
                return Err(Error::Remote(format!(
                    "unknown local pip kind {kind} for session_id={session_id}"
                )));
            }
        };
        Ok(Some(LocalPip {
            session_id,
            outgoing: &mut self.outgoing,
            incoming: &mut self.incoming,
            pending,
            incoming_done,
            outgoing_done: false,
        }))
    }

    pub fn take_message(&mut self) -> Result<Option<LocalPendingPip<ClientMsg>>> {
        let Some(sample) = self.incoming.take()? else {
            return Ok(None);
        };
        let header = sample.header();
        let session_id = header.session_id;
        match header.kind {
            PIP_KIND_ITEM => Ok(Some((
                session_id,
                Some(PipSample {
                    session_id,
                    header: header.header,
                    payload: sample.payload().to_vec(),
                }),
                false,
            ))),
            PIP_KIND_DONE => Ok(Some((session_id, None, true))),
            kind => Err(Error::Remote(format!(
                "unknown local pip kind {kind} for session_id={session_id}"
            ))),
        }
    }

    pub fn send_to(&mut self, session_id: u64, msg: &ServerMsg) -> Result<()> {
        publish_pip(&mut self.outgoing, session_id, PIP_KIND_ITEM, msg)
    }

    pub fn finish_send_to(&mut self, session_id: u64) -> Result<()> {
        publish_done::<ServerMsg>(&mut self.outgoing, session_id)
    }

    pub fn next_from(&mut self, session_id: u64) -> Result<Option<PipSample<ClientMsg>>> {
        let deadline = Instant::now() + DEFAULT_PIP_TIMEOUT;
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Timeout(DEFAULT_PIP_TIMEOUT));
            }
            let Some(sample) = self.incoming.take()? else {
                std::thread::sleep(Duration::from_micros(50));
                continue;
            };
            let header = sample.header();
            if header.session_id != session_id {
                continue;
            }
            return match header.kind {
                PIP_KIND_ITEM => Ok(Some(PipSample {
                    session_id,
                    header: header.header,
                    payload: sample.payload().to_vec(),
                })),
                PIP_KIND_DONE => Ok(None),
                kind => Err(Error::Remote(format!(
                    "unknown local pip kind {kind} for session_id={session_id}"
                ))),
            };
        }
    }
}

pub struct LocalPipClient<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    ServerMsg: datapod::DataPod + 'static,
{
    outgoing: Producer<PipEnvelope<ClientMsg::Header>>,
    incoming: Consumer<PipEnvelope<ServerMsg::Header>>,
    next_id: Arc<AtomicU64>,
}

impl<ClientMsg, ServerMsg> LocalPipClient<ClientMsg, ServerMsg>
where
    ClientMsg: datapod::DataPod + 'static,
    ServerMsg: datapod::DataPod + 'static,
{
    pub fn open(&mut self) -> Result<LocalPip<'_, ClientMsg, ServerMsg>> {
        let session_id = self.next_id.fetch_add(1, Ordering::AcqRel) + 1;
        Ok(LocalPip {
            session_id,
            outgoing: &mut self.outgoing,
            incoming: &mut self.incoming,
            pending: None,
            incoming_done: false,
            outgoing_done: false,
        })
    }

    pub fn start_session(&mut self) -> u64 {
        self.next_id.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn send_to(&mut self, session_id: u64, msg: &ClientMsg) -> Result<()> {
        publish_pip(&mut self.outgoing, session_id, PIP_KIND_ITEM, msg)
    }

    pub fn finish_send_to(&mut self, session_id: u64) -> Result<()> {
        publish_done::<ClientMsg>(&mut self.outgoing, session_id)
    }

    pub fn next_from(&mut self, session_id: u64) -> Result<Option<PipSample<ServerMsg>>> {
        let deadline = Instant::now() + DEFAULT_PIP_TIMEOUT;
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Timeout(DEFAULT_PIP_TIMEOUT));
            }
            let Some(sample) = self.incoming.take()? else {
                std::thread::sleep(Duration::from_micros(50));
                continue;
            };
            let header = sample.header();
            if header.session_id != session_id {
                continue;
            }
            return match header.kind {
                PIP_KIND_ITEM => Ok(Some(PipSample {
                    session_id,
                    header: header.header,
                    payload: sample.payload().to_vec(),
                })),
                PIP_KIND_DONE => Ok(None),
                kind => Err(Error::Remote(format!(
                    "unknown local pip kind {kind} for session_id={session_id}"
                ))),
            };
        }
    }
}

pub struct LocalPip<'a, Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    Rx: datapod::DataPod + 'static,
{
    session_id: u64,
    outgoing: &'a mut Producer<PipEnvelope<Tx::Header>>,
    incoming: &'a mut Consumer<PipEnvelope<Rx::Header>>,
    pending: Option<PipSample<Rx>>,
    incoming_done: bool,
    outgoing_done: bool,
}

impl<Tx, Rx> LocalPip<'_, Tx, Rx>
where
    Tx: datapod::DataPod + 'static,
    Rx: datapod::DataPod + 'static,
{
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn send(&mut self, msg: &Tx) -> Result<()> {
        if self.outgoing_done {
            return Err(Error::invalid_argument("pip outgoing direction is done"));
        }
        publish_pip(self.outgoing, self.session_id, PIP_KIND_ITEM, msg)
    }

    pub fn finish_send(&mut self) -> Result<()> {
        if !self.outgoing_done {
            publish_done::<Tx>(self.outgoing, self.session_id)?;
            self.outgoing_done = true;
        }
        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<PipSample<Rx>>> {
        if let Some(sample) = self.pending.take() {
            return Ok(Some(sample));
        }
        if self.incoming_done {
            return Ok(None);
        }
        let deadline = Instant::now() + DEFAULT_PIP_TIMEOUT;
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Timeout(DEFAULT_PIP_TIMEOUT));
            }
            let Some(sample) = self.incoming.take()? else {
                std::thread::sleep(Duration::from_micros(50));
                continue;
            };
            let header = sample.header();
            if header.session_id != self.session_id {
                continue;
            }
            match header.kind {
                PIP_KIND_ITEM => {
                    return Ok(Some(PipSample {
                        session_id: self.session_id,
                        header: header.header,
                        payload: sample.payload().to_vec(),
                    }));
                }
                PIP_KIND_DONE => {
                    self.incoming_done = true;
                    return Ok(None);
                }
                kind => {
                    return Err(Error::Remote(format!(
                        "unknown local pip kind {kind} for session_id={}",
                        self.session_id
                    )));
                }
            }
        }
    }
}

fn publish_pip<T>(
    publisher: &mut Producer<PipEnvelope<T::Header>>,
    session_id: u64,
    kind: u8,
    value: &T,
) -> Result<()>
where
    T: datapod::DataPod + 'static,
{
    let bytes = value.payload_bytes();
    let mut loan = publisher.loan(bytes.len())?;
    *loan.header_mut() = PipEnvelope {
        session_id,
        kind,
        reserved: [0; 7],
        header: value.header(),
    };
    loan.payload_mut().copy_from_slice(bytes);
    publisher.publish(loan)?;
    Ok(())
}

fn publish_done<T>(publisher: &mut Producer<PipEnvelope<T::Header>>, session_id: u64) -> Result<()>
where
    T: datapod::DataPod + 'static,
{
    let mut loan = publisher.loan(0)?;
    *loan.header_mut() = PipEnvelope {
        session_id,
        kind: PIP_KIND_DONE,
        reserved: [0; 7],
        header: T::Header::zeroed(),
    };
    publisher.publish(loan)?;
    Ok(())
}
