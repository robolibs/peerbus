//! Local (same-host) req/res over the SHM ring.
//!
//! Two pub/sub services back the implementation: `<name>__req` for
//! requests and `<name>__resp` for responses. Each carries an
//! `Envelope<T::Header>` in the fixed header plus a variable byte
//! payload for heap-bearing datapods.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::local::shm::{Consumer, Producer, Segment};
use crate::reqresp::{DEFAULT_CALL_TIMEOUT, Envelope};
use crate::transport::wire_type_hash;

const REQ_SUFFIX: &str = "__req";
const RESP_SUFFIX: &str = "__resp";

/// Internal helper service where the fixed SHM header is
/// `Envelope<T::Header>`.
struct EnvelopedService<T: datapod::DataPod + 'static> {
    segment: Arc<Segment<Envelope<T::Header>>>,
    cfg: LocalConfig,
}

impl<T: datapod::DataPod + 'static> Clone for EnvelopedService<T> {
    fn clone(&self) -> Self {
        Self {
            segment: self.segment.clone(),
            cfg: self.cfg.clone(),
        }
    }
}

impl<T: datapod::DataPod + 'static> EnvelopedService<T> {
    fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let segment = Segment::<Envelope<T::Header>>::open_or_create(
            name,
            wire_type_hash::<Envelope<T::Header>>(),
            cfg.clone(),
        )?;
        Ok(Self { segment, cfg })
    }

    fn open_existing(name: &str) -> Result<Self> {
        let segment = Segment::<Envelope<T::Header>>::open_existing(
            name,
            wire_type_hash::<Envelope<T::Header>>(),
        )?;
        Ok(Self {
            segment,
            cfg: LocalConfig::default(),
        })
    }

    fn publisher(&self) -> Result<Producer<Envelope<T::Header>>> {
        self.segment.producer()
    }

    fn subscriber(&self) -> Result<Consumer<Envelope<T::Header>>> {
        self.segment.consumer()
    }
}

#[derive(Clone)]
pub struct LocalReqRespService<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    requests: EnvelopedService<Req>,
    responses: EnvelopedService<Resp>,
    next_id: Arc<AtomicU64>,
}

impl<Req, Resp> LocalReqRespService<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    pub fn create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let requests =
            EnvelopedService::<Req>::open_or_create(&with_suffix(name, REQ_SUFFIX), cfg.clone())?;
        let responses =
            EnvelopedService::<Resp>::open_or_create(&with_suffix(name, RESP_SUFFIX), cfg)?;
        Ok(Self {
            requests,
            responses,
            next_id: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn attach(name: &str) -> Result<Self> {
        Self::create(name, LocalConfig::default())
    }

    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Self::create(name, cfg)
    }

    pub fn open_existing(name: &str) -> Result<Self> {
        let requests = EnvelopedService::<Req>::open_existing(&with_suffix(name, REQ_SUFFIX))?;
        let responses = EnvelopedService::<Resp>::open_existing(&with_suffix(name, RESP_SUFFIX))?;
        Ok(Self {
            requests,
            responses,
            next_id: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn server(&self) -> Result<LocalRequestServer<Req, Resp>> {
        Ok(LocalRequestServer {
            requests: self.requests.subscriber()?,
            responses: self.responses.publisher()?,
        })
    }

    pub fn client(&self) -> Result<LocalClient<Req, Resp>> {
        Ok(LocalClient {
            requests: self.requests.publisher()?,
            responses: self.responses.subscriber()?,
            next_id: Arc::clone(&self.next_id),
        })
    }
}

fn with_suffix(name: &str, suffix: &str) -> String {
    format!("{name}{suffix}")
}

/// Received request: gives access to the header (with `req_id`) and
/// the byte payload. For fixed-Pod `Req`, the header IS the request.
pub struct RequestSample<Req: datapod::DataPod + 'static> {
    inner: crate::local::shm::Sample<Envelope<Req::Header>>,
}

impl<Req: datapod::DataPod + 'static> RequestSample<Req> {
    pub fn req_id(&self) -> u64 {
        self.inner.header().req_id
    }
    pub fn header(&self) -> &Req::Header {
        &self.inner.header().header
    }
    pub fn payload(&self) -> &[u8] {
        self.inner.payload()
    }
}

pub type PendingRequest<'a, Req, Resp> = (RequestSample<Req>, ReplyHandle<'a, Req, Resp>);
pub type PendingReq<'a, Req, Res> = (RequestSample<Req>, ReplyHandle<'a, Req, Res>);

pub struct LocalRequestServer<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    requests: Consumer<Envelope<Req::Header>>,
    responses: Producer<Envelope<Resp::Header>>,
}

impl<Req, Resp> LocalRequestServer<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    pub fn take_request(&mut self) -> Result<Option<PendingRequest<'_, Req, Resp>>> {
        let Some(sample) = self.requests.take()? else {
            return Ok(None);
        };
        let req_id = sample.header().req_id;
        Ok(Some((
            RequestSample { inner: sample },
            ReplyHandle {
                req_id,
                responses: &mut self.responses,
                _phantom: PhantomData,
            },
        )))
    }
}

pub struct ReplyHandle<'a, Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    req_id: u64,
    responses: &'a mut Producer<Envelope<Resp::Header>>,
    _phantom: PhantomData<fn() -> Req>,
}

impl<Req, Resp> ReplyHandle<'_, Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn respond(self, resp: &Resp) -> Result<()> {
        publish_enveloped(self.responses, self.req_id, resp)
    }
}

pub struct LocalClient<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    requests: Producer<Envelope<Req::Header>>,
    responses: Consumer<Envelope<Resp::Header>>,
    next_id: Arc<AtomicU64>,
}

/// Response: header + bytes from the wire. Callers reconstruct
/// their `Resp` from these as appropriate (fixed-Pod = copy the
/// header value; heap = combine header + bytemuck::cast_slice on
/// the payload).
pub struct ResponseSample<Resp: datapod::DataPod + 'static> {
    inner: crate::local::shm::Sample<Envelope<Resp::Header>>,
}

impl<Resp: datapod::DataPod + 'static> ResponseSample<Resp> {
    pub fn req_id(&self) -> u64 {
        self.inner.header().req_id
    }
    pub fn header(&self) -> &Resp::Header {
        &self.inner.header().header
    }
    pub fn payload(&self) -> &[u8] {
        self.inner.payload()
    }
}

impl<Req, Resp> LocalClient<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    pub fn call(&mut self, req: &Req) -> Result<ResponseSample<Resp>> {
        self.call_with_timeout(req, DEFAULT_CALL_TIMEOUT)
    }

    pub fn call_with_timeout(
        &mut self,
        req: &Req,
        timeout: Duration,
    ) -> Result<ResponseSample<Resp>> {
        let req_id = self.next_id.fetch_add(1, Ordering::AcqRel) + 1;
        publish_enveloped(&mut self.requests, req_id, req)?;

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.responses.take()? {
                Some(sample) if sample.header().req_id == req_id => {
                    return Ok(ResponseSample { inner: sample });
                }
                Some(_) => {
                    // wrong reply; keep draining
                }
                None => std::thread::sleep(Duration::from_micros(50)),
            }
        }
        Err(Error::Other(format!(
            "call timed out after {timeout:?} (req_id={req_id})"
        )))
    }
}

pub type LocalReqResService<Req, Res> = LocalReqRespService<Req, Res>;
pub type LocalReqServer<Req, Res> = LocalRequestServer<Req, Res>;
pub type LocalReqClient<Req, Res> = LocalClient<Req, Res>;
pub type ResSample<Res> = ResponseSample<Res>;

fn publish_enveloped<T>(
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
