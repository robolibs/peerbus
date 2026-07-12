//! Async adapter — `async fn` wrappers over the sync core.
//!
//! The wrappers call the underlying sync API inside
//! `tokio::task::spawn_blocking` so callers can use `.await` from
//! a tokio runtime without blocking its worker threads. The local
//! SHM path is genuinely blocking-friendly (atomic ops + an
//! occasional yield), so this isn't a contortion — it just lets
//! peerbus fit into an async surrounding.
//!
//! For the remote transport, the sync core *already* drives an
//! internal tokio runtime via `block_on`. The async wrapper here is
//! still useful: it lets the calling runtime keep doing other work
//! while the internal runtime handles QUIC traffic.

use std::time::Duration;

use bytemuck::Pod;

use crate::error::{Error, Result};
use crate::local::pip::{LocalPendingPip, LocalPipClient, LocalPipServer, PipSample};
use crate::local::putack::{AckSample, LocalAckServer, LocalPendingPuts, LocalPutClient, PutSample};
use crate::local::queans::{AnsSample, LocalAnsServer, LocalQueClient, QueSample};
use crate::local::reqresp::{LocalReqClient, LocalReqServer, RequestSample, ResponseSample};
use crate::remote::{RemotePipClient, RemotePutClient, RemoteQueClient, RemoteReqClient};
use crate::transport::{LocalPayload, PublisherOps, SubscriberOps};

/// Shared helper: pull the inner sync handle out of its `Option`
/// slot before a `spawn_blocking`, mirroring the pub/sub wrappers.
/// A `None` means the wrapper was polled from two places at once
/// (the inner handle is currently owned by an in-flight blocking
/// task), which is a misuse.
fn take_inner<X>(slot: &mut Option<X>, what: &str) -> Result<X> {
    slot.take().ok_or_else(|| {
        Error::Other(format!(
            "{what} was polled concurrently from two places — inner handle is missing"
        ))
    })
}

/// Async wrapper around any [`PublisherOps`] impl.
pub struct AsyncPublisher<T, P> {
    inner: Option<P>,
    _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T, P> AsyncPublisher<T, P>
where
    T: LocalPayload + Send,
    P: PublisherOps<T> + Send + 'static,
{
    pub fn new(inner: P) -> Self {
        Self {
            inner: Some(inner),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Reserve a slot with `byte_count` payload bytes.
    pub async fn loan(&mut self, byte_count: usize) -> Result<P::Loan> {
        let mut inner = self.take_inner()?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.loan(byte_count);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Publish a previously loaned slot.
    pub async fn publish(&mut self, loan: P::Loan) -> Result<u64> {
        let mut inner = self.take_inner()?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.publish(loan);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    fn take_inner(&mut self) -> Result<P> {
        self.inner.take().ok_or_else(|| {
            Error::Other(
                "AsyncPublisher was polled concurrently from two places — \
                 inner publisher is missing"
                    .to_string(),
            )
        })
    }
}

/// Async wrapper around any [`SubscriberOps`] impl.
pub struct AsyncSubscriber<T, S> {
    inner: Option<S>,
    _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T, S> AsyncSubscriber<T, S>
where
    T: LocalPayload + Send,
    S: SubscriberOps<T> + Send + 'static,
{
    pub fn new(inner: S) -> Self {
        Self {
            inner: Some(inner),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Non-blocking take wrapped in `spawn_blocking`.
    pub async fn take(&mut self) -> Result<Option<S::Sample>> {
        let mut inner = self.take_inner()?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.take();
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    fn take_inner(&mut self) -> Result<S> {
        self.inner.take().ok_or_else(|| {
            Error::Other(
                "AsyncSubscriber was polled concurrently from two places — \
                 inner subscriber is missing"
                    .to_string(),
            )
        })
    }
}

// ---------------------------------------------------------------------------
// req/res
// ---------------------------------------------------------------------------

/// Async wrapper around the sync local req/res client
/// ([`crate::local::reqresp::LocalReqClient`]).
///
/// `call`/`call_with_timeout` internally poll the response ring until
/// a correlated reply arrives (or the timeout elapses), so wrapping
/// them in a single `spawn_blocking` yields a genuinely-awaiting call.
pub struct AsyncReqClient<Req, Res>
where
    Req: LocalPayload + Send,
    Res: LocalPayload + Send,
{
    inner: Option<LocalReqClient<Req, Res>>,
}

impl<Req, Res> AsyncReqClient<Req, Res>
where
    Req: LocalPayload + Send,
    Res: LocalPayload + Send,
{
    pub fn new(inner: LocalReqClient<Req, Res>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Send `req` and await the correlated response (default timeout).
    pub async fn call(&mut self, req: Req) -> Result<ResponseSample<Res>> {
        let mut inner = take_inner(&mut self.inner, "AsyncReqClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.call(&req);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Send `req` and await the correlated response with an explicit
    /// timeout.
    pub async fn call_with_timeout(
        &mut self,
        req: Req,
        timeout: Duration,
    ) -> Result<ResponseSample<Res>> {
        let mut inner = take_inner(&mut self.inner, "AsyncReqClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.call_with_timeout(&req, timeout);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

/// Async wrapper around the sync local req/res server
/// ([`crate::local::reqresp::LocalReqServer`]).
pub struct AsyncReqServer<Req, Res>
where
    Req: LocalPayload + Send,
    Res: LocalPayload + Send,
{
    inner: Option<LocalReqServer<Req, Res>>,
}

impl<Req, Res> AsyncReqServer<Req, Res>
where
    Req: LocalPayload + Send,
    Res: LocalPayload + Send,
{
    pub fn new(inner: LocalReqServer<Req, Res>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Non-blocking take of the next pending request, wrapped in
    /// `spawn_blocking`. Returns `Ok(None)` when nothing is queued.
    /// The returned `req_id` is used with [`respond_to`](Self::respond_to)
    /// to reply.
    pub async fn take_request(&mut self) -> Result<Option<(u64, RequestSample<Req>)>> {
        let mut inner = take_inner(&mut self.inner, "AsyncReqServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = match inner.take_request() {
                Ok(Some((sample, _reply))) => Ok(Some((sample.req_id(), sample))),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            };
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Respond to a previously taken request.
    pub async fn respond_to(&mut self, req_id: u64, resp: Res) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncReqServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.respond_to(req_id, &resp);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

// ---------------------------------------------------------------------------
// que/ans
// ---------------------------------------------------------------------------

/// Async wrapper around the sync local que/ans client
/// ([`crate::local::queans::LocalQueClient`]).
///
/// The sync client hands back a borrowed streaming handle
/// (`LocalAnswers`) whose lifetime is tied to the client, so it cannot
/// cross an `.await`. Instead [`query`](Self::query) runs the whole
/// send-then-drain sequence inside one `spawn_blocking` and awaits the
/// full set of answers.
pub struct AsyncQueClient<Que, Ans>
where
    Que: LocalPayload + Send,
    Ans: LocalPayload + Send,
{
    inner: Option<LocalQueClient<Que, Ans>>,
}

impl<Que, Ans> AsyncQueClient<Que, Ans>
where
    Que: LocalPayload + Send,
    Ans: LocalPayload + Send,
{
    pub fn new(inner: LocalQueClient<Que, Ans>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Send `que` and await every answer, up to the terminating done
    /// marker. Returns the collected answers in arrival order.
    pub async fn query(&mut self, que: Que) -> Result<Vec<AnsSample<Ans>>> {
        let mut inner = take_inner(&mut self.inner, "AsyncQueClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = (|| {
                let mut answers = inner.send(&que)?;
                let mut out = Vec::new();
                while let Some(sample) = answers.next()? {
                    out.push(sample);
                }
                Ok(out)
            })();
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

/// Async wrapper around the sync local que/ans server
/// ([`crate::local::queans::LocalAnsServer`]).
pub struct AsyncAnsServer<Que, Ans>
where
    Que: LocalPayload + Send,
    Ans: LocalPayload + Send,
{
    inner: Option<LocalAnsServer<Que, Ans>>,
}

impl<Que, Ans> AsyncAnsServer<Que, Ans>
where
    Que: LocalPayload + Send,
    Ans: LocalPayload + Send,
{
    pub fn new(inner: LocalAnsServer<Que, Ans>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Non-blocking take of the next pending query. Returns
    /// `Ok(None)` when nothing is queued. Reply to the returned
    /// `req_id` with [`send_to`](Self::send_to) / [`finish_to`](Self::finish_to).
    pub async fn take_query(&mut self) -> Result<Option<(u64, QueSample<Que>)>> {
        let mut inner = take_inner(&mut self.inner, "AsyncAnsServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = match inner.take() {
                Ok(Some((sample, _reply))) => Ok(Some((sample.req_id(), sample))),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            };
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Send one answer item for `req_id`.
    pub async fn send_to(&mut self, req_id: u64, ans: Ans) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncAnsServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.send_to(req_id, &ans);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Publish the terminating done marker for `req_id`.
    pub async fn finish_to(&mut self, req_id: u64) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncAnsServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.finish_to(req_id);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

// ---------------------------------------------------------------------------
// put/ack
// ---------------------------------------------------------------------------

/// Async wrapper around the sync local put/ack client
/// ([`crate::local::putack::LocalPutClient`]).
///
/// Uses the id-based (`open_req` → `send_to`* → `finish_req`) API so
/// each step can round-trip through `spawn_blocking` without holding
/// the borrowed `LocalPutSender` handle across an `.await`.
pub struct AsyncPutClient<Put, Ack>
where
    Put: LocalPayload + Send,
    Ack: LocalPayload + Send,
{
    inner: Option<LocalPutClient<Put, Ack>>,
}

impl<Put, Ack> AsyncPutClient<Put, Ack>
where
    Put: LocalPayload + Send,
    Ack: LocalPayload + Send,
{
    pub fn new(inner: LocalPutClient<Put, Ack>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Allocate a new upload session id.
    pub async fn open_req(&mut self) -> Result<u64> {
        let mut inner = take_inner(&mut self.inner, "AsyncPutClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.open_req();
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        Ok(result)
    }

    /// Send one upload item for `req_id`.
    pub async fn send_to(&mut self, req_id: u64, put: Put) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncPutClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.send_to(req_id, &put);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Close the upload for `req_id` and await the final ack.
    pub async fn finish_req(&mut self, req_id: u64) -> Result<AckSample<Ack>> {
        let mut inner = take_inner(&mut self.inner, "AsyncPutClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.finish_req(req_id);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

/// Async wrapper around the sync local put/ack server
/// ([`crate::local::putack::LocalAckServer`]).
pub struct AsyncAckServer<Put, Ack>
where
    Put: LocalPayload + Send,
    Ack: LocalPayload + Send,
{
    inner: Option<LocalAckServer<Put, Ack>>,
}

impl<Put, Ack> AsyncAckServer<Put, Ack>
where
    Put: LocalPayload + Send,
    Ack: LocalPayload + Send,
{
    pub fn new(inner: LocalAckServer<Put, Ack>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Non-blocking take of the next put frame: `(req_id, item, done)`.
    /// `item` is `Some` for an upload item and `None` for the done
    /// marker. Returns `Ok(None)` when nothing is queued.
    pub async fn take_message(&mut self) -> Result<Option<LocalPendingPuts<Put>>> {
        let mut inner = take_inner(&mut self.inner, "AsyncAckServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.take_message();
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Await the next upload item for `req_id`. `Ok(None)` marks the
    /// end of the stream (done marker seen).
    pub async fn next_from(&mut self, req_id: u64) -> Result<Option<PutSample<Put>>> {
        let mut inner = take_inner(&mut self.inner, "AsyncAckServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.next_from(req_id);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Send the final ack for `req_id`.
    pub async fn ack_to(&mut self, req_id: u64, ack: Ack) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncAckServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.ack_to(req_id, &ack);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

// ---------------------------------------------------------------------------
// pip (bidirectional)
// ---------------------------------------------------------------------------

/// Async wrapper around the sync local pip client
/// ([`crate::local::pip::LocalPipClient`]).
///
/// Uses the id-based (`start_session` → `send_to` / `next_from` /
/// `finish_send_to`) API so each step round-trips through
/// `spawn_blocking` without holding the borrowed `LocalPip` handle
/// across an `.await`.
pub struct AsyncPipClient<ClientMsg, ServerMsg>
where
    ClientMsg: LocalPayload + Send,
    ServerMsg: LocalPayload + Send,
{
    inner: Option<LocalPipClient<ClientMsg, ServerMsg>>,
}

impl<ClientMsg, ServerMsg> AsyncPipClient<ClientMsg, ServerMsg>
where
    ClientMsg: LocalPayload + Send,
    ServerMsg: LocalPayload + Send,
{
    pub fn new(inner: LocalPipClient<ClientMsg, ServerMsg>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Allocate a new pip session id.
    pub async fn start_session(&mut self) -> Result<u64> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.start_session();
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        Ok(result)
    }

    /// Send one client-to-server message on `session_id`.
    pub async fn send_to(&mut self, session_id: u64, msg: ClientMsg) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.send_to(session_id, &msg);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Close the client-to-server direction for `session_id`.
    pub async fn finish_send_to(&mut self, session_id: u64) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.finish_send_to(session_id);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Await the next server-to-client message on `session_id`.
    /// `Ok(None)` marks the end of that direction (done marker seen).
    pub async fn next_from(&mut self, session_id: u64) -> Result<Option<PipSample<ServerMsg>>> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.next_from(session_id);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

/// Async wrapper around the sync local pip server
/// ([`crate::local::pip::LocalPipServer`]).
pub struct AsyncPipServer<ClientMsg, ServerMsg>
where
    ClientMsg: LocalPayload + Send,
    ServerMsg: LocalPayload + Send,
{
    inner: Option<LocalPipServer<ClientMsg, ServerMsg>>,
}

impl<ClientMsg, ServerMsg> AsyncPipServer<ClientMsg, ServerMsg>
where
    ClientMsg: LocalPayload + Send,
    ServerMsg: LocalPayload + Send,
{
    pub fn new(inner: LocalPipServer<ClientMsg, ServerMsg>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Non-blocking take of the next client-to-server frame:
    /// `(session_id, item, done)`. `item` is `Some` for a message and
    /// `None` for the done marker. Returns `Ok(None)` when nothing is
    /// queued.
    pub async fn take_message(&mut self) -> Result<Option<LocalPendingPip<ClientMsg>>> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.take_message();
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Await the next client-to-server message on `session_id`.
    /// `Ok(None)` marks the end of that direction (done marker seen).
    pub async fn next_from(&mut self, session_id: u64) -> Result<Option<PipSample<ClientMsg>>> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.next_from(session_id);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Send one server-to-client message on `session_id`.
    pub async fn send_to(&mut self, session_id: u64, msg: ServerMsg) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.send_to(session_id, &msg);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }

    /// Close the server-to-client direction for `session_id`.
    pub async fn finish_send_to(&mut self, session_id: u64) -> Result<()> {
        let mut inner = take_inner(&mut self.inner, "AsyncPipServer")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.finish_send_to(session_id);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

// ===========================================================================
// remote (iroh) — standalone client wrappers
// ===========================================================================
//
// The standalone remote clients (`RemoteReqClient`, `RemoteQueClient`,
// `RemotePutClient`, `RemotePipClient`, built via
// `RemoteTransport::{req,que,put,pip}_client`) are *sync* handles that
// drive the transport's own internal tokio runtime via
// `runtime.block_on(...)` on each call. That `block_on` must run from a
// blocking (non-async) thread, so — exactly like the local wrappers
// above — every call round-trips through a single `tokio::task::spawn_blocking`.
// Calling their methods directly from an async context (without
// `spawn_blocking`) would nest one runtime inside another and panic;
// `spawn_blocking` moves the work onto a dedicated blocking thread where
// `block_on` is legitimate.
//
// Only the *client* side is wrapped. The remote server side is
// registration/callback based (`RemoteTransport::serve_requests`,
// `serve_ques`, `serve_puts`, `serve_pips`) — it installs a handler that
// the transport's internal runtime invokes; there is no standalone,
// pollable server handle analogous to `LocalReqServer`/`LocalAnsServer`/
// `LocalAckServer`/`LocalPipServer`, so there is nothing here to wrap.
//
// Payload bounds are `Pod + Send + 'static`, matching what the concrete
// remote client types require (see `src/remote/reqresp.rs`,
// `queans.rs`, `putack.rs`, `pip.rs`).

// ---------------------------------------------------------------------------
// remote req/res
// ---------------------------------------------------------------------------

/// Async wrapper around the sync standalone remote req/res client
/// ([`crate::remote::RemoteReqClient`], i.e. `RemoteClient`).
///
/// [`call`](Self::call) opens a fresh QUIC bi stream, writes the
/// request, and awaits the correlated response — all inside one
/// `spawn_blocking`, so the calling runtime stays free.
pub struct AsyncRemoteReqClient<Req, Res>
where
    Req: Pod + Send + 'static,
    Res: Pod + Send + 'static,
{
    inner: Option<RemoteReqClient<Req, Res>>,
}

impl<Req, Res> AsyncRemoteReqClient<Req, Res>
where
    Req: Pod + Send + 'static,
    Res: Pod + Send + 'static,
{
    pub fn new(inner: RemoteReqClient<Req, Res>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Send `req` and await the response.
    pub async fn call(&mut self, req: Req) -> Result<Res> {
        let mut inner = take_inner(&mut self.inner, "AsyncRemoteReqClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.call(req);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

// ---------------------------------------------------------------------------
// remote que/ans
// ---------------------------------------------------------------------------

/// Async wrapper around the sync standalone remote que/ans client
/// ([`crate::remote::RemoteQueClient`]).
///
/// The remote que/ans path is collect-then-respond: one query yields
/// the full set of answers, drained on the wire before the sync `send`
/// returns. So — exactly like the local [`AsyncQueClient`] — there is no
/// per-item async `next`: [`query`](Self::query) awaits the whole set in
/// one `spawn_blocking` and returns it as a `Vec`.
pub struct AsyncRemoteQueClient<Que, Ans>
where
    Que: Pod + Send + 'static,
    Ans: Pod + Send + 'static,
{
    inner: Option<RemoteQueClient<Que, Ans>>,
}

impl<Que, Ans> AsyncRemoteQueClient<Que, Ans>
where
    Que: Pod + Send + 'static,
    Ans: Pod + Send + 'static,
{
    pub fn new(inner: RemoteQueClient<Que, Ans>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Send `que` and await every answer. Returns the collected answers
    /// in arrival order.
    pub async fn query(&mut self, que: Que) -> Result<Vec<Ans>> {
        let mut inner = take_inner(&mut self.inner, "AsyncRemoteQueClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.send(que);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

// ---------------------------------------------------------------------------
// remote put/ack
// ---------------------------------------------------------------------------

/// Async wrapper around the sync standalone remote put/ack client
/// ([`crate::remote::RemotePutClient`]).
///
/// The remote put/ack path is collect-then-respond: all upload items go
/// out on one QUIC stream, followed by a done marker, and the server
/// replies with one ack. [`upload`](Self::upload) awaits that whole
/// exchange inside one `spawn_blocking`.
pub struct AsyncRemotePutClient<Put, Ack>
where
    Put: Pod + Send + 'static,
    Ack: Pod + Send + 'static,
{
    inner: Option<RemotePutClient<Put, Ack>>,
}

impl<Put, Ack> AsyncRemotePutClient<Put, Ack>
where
    Put: Pod + Send + 'static,
    Ack: Pod + Send + 'static,
{
    pub fn new(inner: RemotePutClient<Put, Ack>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Upload all `puts` and await the final ack.
    pub async fn upload(&mut self, puts: Vec<Put>) -> Result<Ack> {
        let mut inner = take_inner(&mut self.inner, "AsyncRemotePutClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.upload(&puts);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}

// ---------------------------------------------------------------------------
// remote pip (bidirectional, collect-then-respond)
// ---------------------------------------------------------------------------

/// Async wrapper around the sync standalone remote pip client
/// ([`crate::remote::RemotePipClient`]).
///
/// The standalone remote pip path is collect-then-respond (not a live
/// interactive session): all client messages go out, then the server's
/// replies come back as a set. [`exchange`](Self::exchange) awaits that
/// whole round-trip inside one `spawn_blocking`.
pub struct AsyncRemotePipClient<ClientMsg, ServerMsg>
where
    ClientMsg: Pod + Send + 'static,
    ServerMsg: Pod + Send + 'static,
{
    inner: Option<RemotePipClient<ClientMsg, ServerMsg>>,
}

impl<ClientMsg, ServerMsg> AsyncRemotePipClient<ClientMsg, ServerMsg>
where
    ClientMsg: Pod + Send + 'static,
    ServerMsg: Pod + Send + 'static,
{
    pub fn new(inner: RemotePipClient<ClientMsg, ServerMsg>) -> Self {
        Self { inner: Some(inner) }
    }

    /// Send all `msgs`, then await the server's replies.
    pub async fn exchange(&mut self, msgs: Vec<ClientMsg>) -> Result<Vec<ServerMsg>> {
        let mut inner = take_inner(&mut self.inner, "AsyncRemotePipClient")?;
        let (inner, result) = tokio::task::spawn_blocking(move || {
            let r = inner.exchange(&msgs);
            (inner, r)
        })
        .await
        .map_err(|e| Error::Other(format!("spawn_blocking: {e}")))?;
        self.inner = Some(inner);
        result
    }
}
