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

use crate::error::{Error, Result};
use crate::transport::{LocalPayload, PublisherOps, SubscriberOps};

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
