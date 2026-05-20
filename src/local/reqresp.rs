//! Local (same-host) request/response over POSIX SHM.
//!
//! A `LocalReqRespService<Req, Resp>` owns two SHM segments:
//! `<name>.req` for requests and `<name>.resp` for responses. Each
//! segment holds [`Envelope<T>`](crate::reqresp::Envelope) values:
//! `(u64 req_id, T)`. The client assigns a fresh `req_id` per
//! call, publishes to the request segment, and waits for an
//! envelope on the response segment whose `req_id` matches.
//!
//! Multiple clients can share the same service. The `req_id`
//! counter is process-local; if you need cross-process clients
//! against the same response channel, generate ids out of band.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytemuck::Pod;

use crate::error::{Error, Result};
use crate::local::handle::Sample;
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::reqresp::{Envelope, DEFAULT_CALL_TIMEOUT};

const REQ_SUFFIX: &str = ".req";
const RESP_SUFFIX: &str = ".resp";

/// Local request/response service. Cheap to clone — clones share
/// the same underlying SHM segments and id counter.
#[derive(Clone)]
pub struct LocalReqRespService<Req: Pod, Resp: Pod> {
    requests: LocalService<Envelope<Req>>,
    responses: LocalService<Envelope<Resp>>,
    next_id: Arc<AtomicU64>,
}

impl<Req: Pod, Resp: Pod> LocalReqRespService<Req, Resp> {
    /// Create both segments. `cfg.slot_size` must fit
    /// `Envelope<Req>` (request side) and `Envelope<Resp>`
    /// (response side); the function checks the larger.
    pub fn create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let cfg = ensure_slot_size::<Req, Resp>(cfg)?;
        let requests = LocalService::create(&with_suffix(name, REQ_SUFFIX), cfg.clone())?;
        let responses = match LocalService::create(&with_suffix(name, RESP_SUFFIX), cfg) {
            Ok(s) => s,
            Err(e) => {
                drop(requests);
                return Err(e);
            }
        };
        Ok(Self {
            requests,
            responses,
            next_id: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Attach to both segments.
    pub fn attach(name: &str) -> Result<Self> {
        let requests = LocalService::attach(&with_suffix(name, REQ_SUFFIX))?;
        let responses = LocalService::attach(&with_suffix(name, RESP_SUFFIX))?;
        Ok(Self {
            requests,
            responses,
            next_id: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Try `create`; fall back to `attach` on collision.
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        match Self::create(name, cfg) {
            Ok(s) => Ok(s),
            Err(Error::ServiceAlreadyExists(_)) => Self::attach(name),
            Err(e) => Err(e),
        }
    }

    /// Build a server handle. The server reads incoming requests and
    /// publishes responses back through the response segment.
    pub fn server(&self) -> LocalRequestServer<Req, Resp> {
        LocalRequestServer {
            requests: self.requests.subscriber(),
            responses: self.responses.publisher(),
        }
    }

    /// Build a client handle.
    pub fn client(&self) -> LocalClient<Req, Resp> {
        LocalClient {
            requests: self.requests.publisher(),
            responses: self.responses.subscriber(),
            next_id: Arc::clone(&self.next_id),
        }
    }
}

fn ensure_slot_size<Req: Pod, Resp: Pod>(mut cfg: LocalConfig) -> Result<LocalConfig> {
    let needed = std::mem::size_of::<Envelope<Req>>()
        .max(std::mem::size_of::<Envelope<Resp>>());
    let needed = ((needed + 7) & !7) as u32;
    if cfg.slot_size < needed {
        cfg.slot_size = needed;
    }
    Ok(cfg)
}

fn with_suffix(name: &str, suffix: &str) -> String {
    format!("{name}{suffix}")
}

/// Result returned by [`LocalRequestServer::take_request`]: a
/// request sample plus a handle for sending the matching response.
pub type PendingRequest<'a, Req, Resp> =
    (Sample<Envelope<Req>>, ReplyHandle<'a, Req, Resp>);

/// Server handle. Pulls requests off the request segment and writes
/// responses back through the response segment.
pub struct LocalRequestServer<Req: Pod, Resp: Pod> {
    requests: LocalSubscriber<Envelope<Req>>,
    responses: LocalPublisher<Envelope<Resp>>,
}

impl<Req: Pod, Resp: Pod> LocalRequestServer<Req, Resp> {
    /// Non-blocking. Returns the next pending request plus a reply
    /// handle that carries the matching `req_id`; call
    /// [`ReplyHandle::respond`] to send a response.
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

/// One-shot reply handle. Consumed by [`ReplyHandle::respond`].
pub struct ReplyHandle<'a, Req: Pod, Resp: Pod> {
    req_id: u64,
    responses: &'a mut LocalPublisher<Envelope<Resp>>,
    _phantom: PhantomData<fn() -> Req>,
}

impl<Req: Pod, Resp: Pod> ReplyHandle<'_, Req, Resp> {
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    /// Publish the response. Consumes `self`.
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

/// Client handle. `call` sends a request and waits for the matching
/// response.
pub struct LocalClient<Req: Pod, Resp: Pod> {
    requests: LocalPublisher<Envelope<Req>>,
    responses: LocalSubscriber<Envelope<Resp>>,
    next_id: Arc<AtomicU64>,
}

impl<Req: Pod, Resp: Pod> LocalClient<Req, Resp> {
    /// Blocking call with the default timeout
    /// ([`DEFAULT_CALL_TIMEOUT`]).
    pub fn call(&mut self, req: Req) -> Result<Resp> {
        self.call_with_timeout(req, DEFAULT_CALL_TIMEOUT)
    }

    /// Blocking call with an explicit timeout.
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
                    // Not our reply — keep draining.
                }
                Ok(None) => std::thread::sleep(Duration::from_micros(50)),
                Err(Error::Lagged { .. }) => {
                    // Our response may have scrolled out of the
                    // ring; surface as Other so the caller knows.
                    return Err(Error::Other(format!(
                        "response for req_id={req_id} was overwritten before pickup; \
                         increase history_depth or slot_count"
                    )));
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::Other(format!(
            "call timed out after {timeout:?} (req_id={req_id})"
        )))
    }
}
