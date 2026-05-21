//! Local (same-host) request/response — iceoryx2-backed.
//!
//! Two pub/sub services back the implementation: `<name>.req` for
//! requests and `<name>.resp` for responses. Each carries
//! `Envelope<T>` = `(u64 req_id, T)`. The client tags each call
//! with a fresh `req_id`, publishes, then drains the response
//! service until it sees the matching id.

use core::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytemuck::Pod;
use iceoryx2::prelude::ZeroCopySend;

use crate::error::{Error, Result};
use crate::local::handle::Sample;
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::reqresp::{DEFAULT_CALL_TIMEOUT, Envelope};

const REQ_SUFFIX: &str = "__req";
const RESP_SUFFIX: &str = "__resp";

#[derive(Clone)]
pub struct LocalReqRespService<Req, Resp>
where
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    requests: LocalService<Envelope<Req>>,
    responses: LocalService<Envelope<Resp>>,
    next_id: Arc<AtomicU64>,
}

impl<Req, Resp> LocalReqRespService<Req, Resp>
where
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    pub fn create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let requests =
            LocalService::open_or_create(&with_suffix(name, REQ_SUFFIX), cfg.clone())?;
        let responses = LocalService::open_or_create(&with_suffix(name, RESP_SUFFIX), cfg)?;
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

pub type PendingRequest<'a, Req, Resp> = (Sample<Envelope<Req>>, ReplyHandle<'a, Req, Resp>);

pub struct LocalRequestServer<Req, Resp>
where
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    requests: LocalSubscriber<Envelope<Req>>,
    responses: LocalPublisher<Envelope<Resp>>,
}

impl<Req, Resp> LocalRequestServer<Req, Resp>
where
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    pub fn take_request(&mut self) -> Result<Option<PendingRequest<'_, Req, Resp>>> {
        let Some(sample) = self.requests.take()? else {
            return Ok(None);
        };
        let req_id = sample.req_id;
        Ok(Some((
            sample,
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
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    req_id: u64,
    responses: &'a mut LocalPublisher<Envelope<Resp>>,
    _phantom: PhantomData<fn() -> Req>,
}

impl<Req, Resp> ReplyHandle<'_, Req, Resp>
where
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn respond(self, resp: Resp) -> Result<()> {
        let mut loan = self.responses.loan()?;
        *loan = Envelope {
            req_id: self.req_id,
            payload: resp,
        };
        self.responses.publish(loan)?;
        Ok(())
    }
}

pub struct LocalClient<Req, Resp>
where
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    requests: LocalPublisher<Envelope<Req>>,
    responses: LocalSubscriber<Envelope<Resp>>,
    next_id: Arc<AtomicU64>,
}

impl<Req, Resp> LocalClient<Req, Resp>
where
    Req: Pod + ZeroCopySend + Debug + 'static,
    Resp: Pod + ZeroCopySend + Debug + 'static,
{
    pub fn call(&mut self, req: Req) -> Result<Resp> {
        self.call_with_timeout(req, DEFAULT_CALL_TIMEOUT)
    }

    pub fn call_with_timeout(&mut self, req: Req, timeout: Duration) -> Result<Resp> {
        let req_id = self.next_id.fetch_add(1, Ordering::AcqRel) + 1;
        let mut loan = self.requests.loan()?;
        *loan = Envelope { req_id, payload: req };
        self.requests.publish(loan)?;

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.responses.take() {
                Ok(Some(sample)) => {
                    if sample.req_id == req_id {
                        return Ok(sample.payload);
                    }
                    // wrong reply; keep draining
                }
                Ok(None) => std::thread::sleep(Duration::from_micros(50)),
                Err(e) => return Err(e),
            }
        }
        Err(Error::Other(format!(
            "call timed out after {timeout:?} (req_id={req_id})"
        )))
    }
}
