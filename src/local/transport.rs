//! [`Transport`] impl backed by iceoryx2.
//!
//! Each `LocalTransport` is bound to one service name. Publisher
//! and subscriber construction lazily open the iceoryx2 service.

use std::marker::PhantomData;

use crate::error::Result;
use crate::local::handle::{Loan, Sample};
use crate::local::service::{LocalConfig, LocalPublisher, LocalService, LocalSubscriber};
use crate::transport::{LocalPayload, PublisherOps, SubscriberOps, Transport};

/// Same-host iceoryx2 transport. Cheap to clone (`Arc`-backed).
#[derive(Clone)]
pub struct LocalTransport {
    name: String,
    cfg: LocalConfig,
}

impl LocalTransport {
    pub fn new(name: impl Into<String>, cfg: LocalConfig) -> Self {
        Self {
            name: name.into(),
            cfg,
        }
    }

    fn open<T: datapod::DataPod + 'static>(&self) -> Result<LocalService<T>> {
        LocalService::<T>::open_or_create(&self.name, self.cfg.clone())
    }
}

impl Transport for LocalTransport {
    type Publisher<T: LocalPayload> = LocalPublisherTyped<T>;
    type Subscriber<T: LocalPayload> = LocalSubscriberTyped<T>;

    fn publisher<T: LocalPayload>(&self) -> Result<Self::Publisher<T>> {
        let svc = self.open::<T>()?;
        Ok(LocalPublisherTyped {
            inner: svc.publisher()?,
            _service: svc,
            _phantom: PhantomData,
        })
    }

    fn subscriber<T: LocalPayload>(&self) -> Result<Self::Subscriber<T>> {
        let svc = self.open::<T>()?;
        Ok(LocalSubscriberTyped {
            inner: svc.subscriber()?,
            _service: svc,
            _phantom: PhantomData,
        })
    }
}

/// Holds a `LocalService` to keep the iceoryx2 service alive for
/// the publisher's lifetime.
pub struct LocalPublisherTyped<T: datapod::DataPod + 'static> {
    inner: LocalPublisher<T>,
    _service: LocalService<T>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: LocalPayload> PublisherOps<T> for LocalPublisherTyped<T> {
    type Loan = Loan<T>;

    fn loan(&mut self, byte_count: usize) -> Result<Self::Loan> {
        self.inner.loan(byte_count)
    }

    fn publish(&mut self, loan: Self::Loan) -> Result<u64> {
        self.inner.publish(loan)
    }
}

pub struct LocalSubscriberTyped<T: datapod::DataPod + 'static> {
    inner: LocalSubscriber<T>,
    _service: LocalService<T>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: LocalPayload> SubscriberOps<T> for LocalSubscriberTyped<T> {
    type Sample = Sample<T>;

    fn take(&mut self) -> Result<Option<Self::Sample>> {
        self.inner.take()
    }
}

// Direct trait impls on `LocalPublisher` / `LocalSubscriber` so
// callers that built the handle straight from `LocalService` can
// also drop it into `AsyncPublisher` / `AsyncSubscriber`.
impl<T: LocalPayload> PublisherOps<T> for LocalPublisher<T> {
    type Loan = Loan<T>;

    fn loan(&mut self, byte_count: usize) -> Result<Self::Loan> {
        LocalPublisher::loan(self, byte_count)
    }

    fn publish(&mut self, loan: Self::Loan) -> Result<u64> {
        LocalPublisher::publish(self, loan)
    }
}

impl<T: LocalPayload> SubscriberOps<T> for LocalSubscriber<T> {
    type Sample = Sample<T>;

    fn take(&mut self) -> Result<Option<Self::Sample>> {
        LocalSubscriber::take(self)
    }
}
