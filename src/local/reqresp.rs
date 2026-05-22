//! Local (same-host) request/response — iceoryx2-backed.
//!
//! Two pub/sub services back the implementation: `<name>__req` for
//! requests and `<name>__resp` for responses. Each carries an
//! `Envelope<T::Header>` as the user_header (correlation id +
//! metadata) plus a `[u8]` slice payload (the cast bytes of `T`'s
//! internal `Vec<...>` for heap-bearing types; empty for fixed-Pod).
//!
//! The client tags each call with a fresh `req_id`, publishes, then
//! drains the response service until it sees the matching id.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use iceoryx2::node::Node as IoxNode;
use iceoryx2::node::NodeBuilder;
use iceoryx2::port::publisher::Publisher as IoxPublisher;
use iceoryx2::port::subscriber::Subscriber as IoxSubscriber;
use iceoryx2::prelude::*;
use iceoryx2::sample::Sample as IoxSample;
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::reqresp::{DEFAULT_CALL_TIMEOUT, Envelope};

const REQ_SUFFIX: &str = "__req";
const RESP_SUFFIX: &str = "__resp";

/// Internal helper service: iceoryx2 publish-subscribe where the
/// user_header is `Envelope<T::Header>`.
struct EnvelopedService<T: datapod::DataPod + 'static> {
    _iox_node: Arc<IoxNode<ipc_threadsafe::Service>>,
    factory: Arc<PortFactory<ipc_threadsafe::Service, [u8], Envelope<T::Header>>>,
    cfg: LocalConfig,
}

impl<T: datapod::DataPod + 'static> Clone for EnvelopedService<T> {
    fn clone(&self) -> Self {
        Self {
            _iox_node: self._iox_node.clone(),
            factory: self.factory.clone(),
            cfg: self.cfg.clone(),
        }
    }
}

impl<T: datapod::DataPod + 'static> EnvelopedService<T> {
    fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let iox_node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(|e| Error::Other(format!("iox NodeBuilder: {e}")))?;

        let service_name: ServiceName = name
            .try_into()
            .map_err(|e| Error::invalid_argument(format!("bad service name '{name}': {e}")))?;

        let factory = iox_node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<Envelope<T::Header>>()
            .max_publishers(cfg.max_publishers as usize)
            .max_subscribers(cfg.max_subscribers as usize)
            .subscriber_max_buffer_size(cfg.subscriber_buffer as usize)
            .history_size(cfg.history_depth as usize)
            .open_or_create()
            .map_err(|e| Error::Other(format!("iox service open_or_create: {e}")))?;

        Ok(Self {
            _iox_node: Arc::new(iox_node),
            factory: Arc::new(factory),
            cfg,
        })
    }

    fn publisher(&self) -> Result<IoxPublisher<ipc_threadsafe::Service, [u8], Envelope<T::Header>>> {
        self.factory
            .publisher_builder()
            .initial_max_slice_len(self.cfg.max_payload_bytes)
            .create()
            .map_err(|e| Error::Other(format!("iox publisher_builder: {e}")))
    }

    fn subscriber(&self) -> Result<IoxSubscriber<ipc_threadsafe::Service, [u8], Envelope<T::Header>>> {
        self.factory
            .subscriber_builder()
            .create()
            .map_err(|e| Error::Other(format!("iox subscriber_builder: {e}")))
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
    inner: IoxSample<ipc_threadsafe::Service, [u8], Envelope<Req::Header>>,
}

impl<Req: datapod::DataPod + 'static> RequestSample<Req> {
    pub fn req_id(&self) -> u64 {
        self.inner.user_header().req_id
    }
    pub fn header(&self) -> &Req::Header {
        &self.inner.user_header().header
    }
    pub fn payload(&self) -> &[u8] {
        self.inner.payload()
    }
}

pub type PendingRequest<'a, Req, Resp> = (RequestSample<Req>, ReplyHandle<'a, Req, Resp>);

pub struct LocalRequestServer<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    requests: IoxSubscriber<ipc_threadsafe::Service, [u8], Envelope<Req::Header>>,
    responses: IoxPublisher<ipc_threadsafe::Service, [u8], Envelope<Resp::Header>>,
}

impl<Req, Resp> LocalRequestServer<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    pub fn take_request(&mut self) -> Result<Option<PendingRequest<'_, Req, Resp>>> {
        let Some(sample) = self
            .requests
            .receive()
            .map_err(|e| Error::Other(format!("iox receive: {e}")))?
        else {
            return Ok(None);
        };
        let req_id = sample.user_header().req_id;
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
    responses: &'a mut IoxPublisher<ipc_threadsafe::Service, [u8], Envelope<Resp::Header>>,
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
        let bytes = resp.payload_bytes();
        let uninit = self
            .responses
            .loan_slice_uninit(bytes.len())
            .map_err(|e| Error::Other(format!("iox loan_slice_uninit: {e}")))?;
        let mut loan = uninit.write_from_fn(|i| bytes[i]);
        *loan.user_header_mut() = Envelope {
            req_id: self.req_id,
            header: resp.header(),
        };
        loan.send()
            .map_err(|e| Error::Other(format!("iox send: {e}")))?;
        Ok(())
    }
}

pub struct LocalClient<Req, Resp>
where
    Req: datapod::DataPod + 'static,
    Resp: datapod::DataPod + 'static,
{
    requests: IoxPublisher<ipc_threadsafe::Service, [u8], Envelope<Req::Header>>,
    responses: IoxSubscriber<ipc_threadsafe::Service, [u8], Envelope<Resp::Header>>,
    next_id: Arc<AtomicU64>,
}

/// Response: header + bytes from the wire. Callers reconstruct
/// their `Resp` from these as appropriate (fixed-Pod = copy the
/// header value; heap = combine header + bytemuck::cast_slice on
/// the payload).
pub struct ResponseSample<Resp: datapod::DataPod + 'static> {
    inner: IoxSample<ipc_threadsafe::Service, [u8], Envelope<Resp::Header>>,
}

impl<Resp: datapod::DataPod + 'static> ResponseSample<Resp> {
    pub fn req_id(&self) -> u64 {
        self.inner.user_header().req_id
    }
    pub fn header(&self) -> &Resp::Header {
        &self.inner.user_header().header
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

        let bytes = req.payload_bytes();
        let uninit = self
            .requests
            .loan_slice_uninit(bytes.len())
            .map_err(|e| Error::Other(format!("iox loan_slice_uninit: {e}")))?;
        let mut loan = uninit.write_from_fn(|i| bytes[i]);
        *loan.user_header_mut() = Envelope {
            req_id,
            header: req.header(),
        };
        loan.send()
            .map_err(|e| Error::Other(format!("iox send: {e}")))?;

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.responses.receive() {
                Ok(Some(sample)) => {
                    if sample.user_header().req_id == req_id {
                        return Ok(ResponseSample { inner: sample });
                    }
                    // wrong reply; keep draining
                }
                Ok(None) => std::thread::sleep(Duration::from_micros(50)),
                Err(e) => return Err(Error::Other(format!("iox receive: {e}"))),
            }
        }
        Err(Error::Other(format!(
            "call timed out after {timeout:?} (req_id={req_id})"
        )))
    }
}
