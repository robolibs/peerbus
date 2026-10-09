use super::*;

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
