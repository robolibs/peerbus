//! Typed entrypoint to the pure-Rust shared-memory local transport.
//!
//! Each `LocalService<T>` maps to one named shared-memory ring. The
//! ring stores a fixed `T::Header` plus variable-length payload bytes
//! in every slot, preserving the existing loan → fill → publish → take
//! API while removing the direct iceoryx2 dependency from peerbus.

use std::sync::Arc;

use crate::error::Result;
use crate::local::handle::{Loan, LoanInner, Sample};
use crate::local::shm::{Consumer, Producer, Segment};
use crate::transport::wire_type_hash;

/// Configurable parameters for the local SHM ring.
#[derive(Debug, Clone)]
pub struct LocalConfig {
    pub max_publishers: u32,
    pub max_subscribers: u32,
    pub subscriber_buffer: u32,
    pub history_depth: u32,
    /// Default max byte-count provisioned per loan. Increase for
    /// services that ship large heap payloads (e.g. images).
    pub max_payload_bytes: usize,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            max_publishers: 2,
            max_subscribers: 8,
            subscriber_buffer: 16,
            history_depth: 1,
            max_payload_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Typed handle to a local publish-subscribe service. Cheap to clone —
/// internally an `Arc<Segment<_>>`.
pub struct LocalService<T: datapod::DataPod + 'static> {
    segment: Arc<Segment<T::Header>>,
    cfg: LocalConfig,
}

impl<T: datapod::DataPod + 'static> Clone for LocalService<T> {
    fn clone(&self) -> Self {
        Self {
            segment: self.segment.clone(),
            cfg: self.cfg.clone(),
        }
    }
}

impl<T: datapod::DataPod + 'static> LocalService<T> {
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let segment =
            Segment::<T::Header>::open_or_create(name, wire_type_hash::<T>(), cfg.clone())?;
        Ok(Self { segment, cfg })
    }

    pub fn create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let segment = Segment::<T::Header>::create(name, wire_type_hash::<T>(), cfg.clone())?;
        Ok(Self { segment, cfg })
    }

    pub fn attach(name: &str) -> Result<Self> {
        Self::open_or_create(name, LocalConfig::default())
    }

    pub fn open_existing(name: &str) -> Result<Self> {
        let segment = Segment::<T::Header>::open_existing(name, wire_type_hash::<T>())?;
        Ok(Self {
            segment,
            cfg: LocalConfig::default(),
        })
    }

    pub fn publisher_count(&self) -> usize {
        self.segment.publisher_count()
    }

    pub fn publisher(&self) -> Result<LocalPublisher<T>> {
        Ok(LocalPublisher {
            inner: self.segment.producer()?,
        })
    }

    pub fn subscriber(&self) -> Result<LocalSubscriber<T>> {
        Ok(LocalSubscriber {
            inner: self.segment.consumer()?,
        })
    }
}

/// Publisher port. Loan a slot (specifying byte count for the
/// payload), fill the header + bytes, publish.
pub struct LocalPublisher<T: datapod::DataPod + 'static> {
    pub(crate) inner: Producer<T::Header>,
}

impl<T: datapod::DataPod + 'static> LocalPublisher<T> {
    /// Loan an in-flight slot with `byte_count` payload bytes. The
    /// header and payload bytes are zero-initialised.
    pub fn loan(&mut self, byte_count: usize) -> Result<Loan<T>> {
        Ok(Loan {
            inner: LoanInner::Shm(self.inner.loan(byte_count)?),
        })
    }

    /// Hand the loan back to the publisher.
    pub fn publish(&mut self, loan: Loan<T>) -> Result<u64> {
        match loan.inner {
            LoanInner::Shm(inner) => self.inner.publish(inner),
            LoanInner::Owned { .. } => Err(crate::Error::invalid_argument(
                "an owned iroh loan cannot be published by LocalPublisher",
            )),
        }
    }

    /// Convenience: build header + bytes from a `&T` and send.
    pub fn send(&mut self, value: &T) -> Result<u64> {
        let bytes = value.payload_bytes();
        let mut loan = self.loan(bytes.len())?;
        *loan.header_mut() = value.header();
        loan.payload_mut().copy_from_slice(bytes);
        self.publish(loan)
    }
}

/// Subscriber port. Non-blocking `take`.
pub struct LocalSubscriber<T: datapod::DataPod + 'static> {
    pub(crate) inner: Consumer<T::Header>,
}

impl<T: datapod::DataPod + 'static> LocalSubscriber<T> {
    pub fn take(&mut self) -> Result<Option<Sample<T>>> {
        Ok(self.inner.take()?.map(|inner| Sample { inner }))
    }
}
