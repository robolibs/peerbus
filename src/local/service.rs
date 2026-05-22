//! Typed entrypoint to the iceoryx2-backed local transport.
//!
//! Each `LocalService<T>` maps to one iceoryx2 publish/subscribe
//! service. The mapping uses iceoryx2's `[u8]` slice payload for
//! variable-length data plus its `UserHeader` slot for the small
//! Pod header `T::Header`. This unifies both shapes of `DataPod`:
//!
//! - **Fixed-Pod `T`** (`type Header = T; type Payload = ()`): the
//!   entire value rides in `user_header`; the slice payload has
//!   length 0.
//! - **Heap-bearing `T`** (`type Header = TH; type Payload = [u8]`):
//!   the metadata rides in `user_header`; the cast bytes of `T`'s
//!   internal `Vec<...>` ride in the slice payload.

use std::sync::Arc;

use iceoryx2::node::Node as IoxNode;
use iceoryx2::node::NodeBuilder;
use iceoryx2::port::publisher::Publisher as IoxPublisher;
use iceoryx2::port::subscriber::Subscriber as IoxSubscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;

use crate::error::{Error, Result};
use crate::local::handle::{Loan, Sample};
use crate::local::slot::Slot;

/// Configurable parameters. iceoryx2 wires its own defaults so
/// most fields are advisory; we keep the struct for API parity
/// with the old allocator and to leave room for future tuning.
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

/// Typed handle to an iceoryx2 publish-subscribe service. Cheap
/// to clone — internally an `Arc<IoxNode>` and an `Arc<PortFactory>`.
pub struct LocalService<T: datapod::DataPod + 'static> {
    iox_node: Arc<IoxNode<ipc_threadsafe::Service>>,
    factory: Arc<PortFactory<ipc_threadsafe::Service, [u8], Slot<T::Header>>>,
    cfg: LocalConfig,
}

impl<T: datapod::DataPod + 'static> Clone for LocalService<T> {
    fn clone(&self) -> Self {
        Self {
            iox_node: self.iox_node.clone(),
            factory: self.factory.clone(),
            cfg: self.cfg.clone(),
        }
    }
}

impl<T: datapod::DataPod + 'static> LocalService<T> {
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let iox_node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(|e| Error::Other(format!("iox NodeBuilder: {e}")))?;

        let service_name: ServiceName = name
            .try_into()
            .map_err(|e| Error::invalid_argument(format!("bad service name '{name}': {e}")))?;

        let factory = iox_node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<Slot<T::Header>>()
            .max_publishers(cfg.max_publishers as usize)
            .max_subscribers(cfg.max_subscribers as usize)
            .subscriber_max_buffer_size(cfg.subscriber_buffer as usize)
            .history_size(cfg.history_depth as usize)
            .open_or_create()
            .map_err(|e| Error::Other(format!("iox service open_or_create: {e}")))?;

        Ok(Self {
            iox_node: Arc::new(iox_node),
            factory: Arc::new(factory),
            cfg,
        })
    }

    pub fn create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Self::open_or_create(name, cfg)
    }

    pub fn attach(name: &str) -> Result<Self> {
        Self::open_or_create(name, LocalConfig::default())
    }

    pub fn open_existing(name: &str) -> Result<Self> {
        let iox_node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(|e| Error::Other(format!("iox NodeBuilder: {e}")))?;

        let service_name: ServiceName = name
            .try_into()
            .map_err(|e| Error::invalid_argument(format!("bad service name '{name}': {e}")))?;

        let factory = iox_node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<Slot<T::Header>>()
            .open()
            .map_err(|e| Error::Other(format!("iox service open: {e}")))?;

        Ok(Self {
            iox_node: Arc::new(iox_node),
            factory: Arc::new(factory),
            cfg: LocalConfig::default(),
        })
    }

    pub fn publisher_count(&self) -> usize {
        use iceoryx2::service::port_factory::PortFactory as _;
        self.factory.dynamic_config().number_of_publishers()
    }

    pub fn publisher(&self) -> Result<LocalPublisher<T>> {
        let publisher = self
            .factory
            .publisher_builder()
            .initial_max_slice_len(self.cfg.max_payload_bytes)
            .create()
            .map_err(|e| Error::Other(format!("iox publisher_builder: {e}")))?;
        Ok(LocalPublisher {
            inner: publisher,
            _service: self.clone(),
        })
    }

    pub fn subscriber(&self) -> Result<LocalSubscriber<T>> {
        let subscriber = self
            .factory
            .subscriber_builder()
            .create()
            .map_err(|e| Error::Other(format!("iox subscriber_builder: {e}")))?;
        Ok(LocalSubscriber {
            inner: subscriber,
            _service: self.clone(),
        })
    }
}

/// Publisher port. Loan a slot (specifying byte count for the
/// payload), fill the header + bytes, publish.
pub struct LocalPublisher<T: datapod::DataPod + 'static> {
    pub(crate) inner: IoxPublisher<ipc_threadsafe::Service, [u8], Slot<T::Header>>,
    _service: LocalService<T>,
}

impl<T: datapod::DataPod + 'static> LocalPublisher<T> {
    /// Loan an in-flight slot with `byte_count` payload bytes. The
    /// header is zero-initialised; the payload bytes are
    /// uninitialised (write before publish).
    pub fn loan(&mut self, byte_count: usize) -> Result<Loan<T>> {
        let uninit = self
            .inner
            .loan_slice_uninit(byte_count)
            .map_err(|e| Error::Other(format!("iox loan_slice_uninit: {e}")))?;
        // Zero-init the payload so callers can safely overwrite it.
        let initialised = uninit.write_from_fn(|_| 0u8);
        Ok(Loan { inner: initialised })
    }

    /// Hand the loan back to the publisher.
    pub fn publish(&mut self, loan: Loan<T>) -> Result<u64> {
        loan.inner
            .send()
            .map_err(|e| Error::Other(format!("iox send: {e}")))?;
        Ok(0)
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
    pub(crate) inner: IoxSubscriber<ipc_threadsafe::Service, [u8], Slot<T::Header>>,
    _service: LocalService<T>,
}

impl<T: datapod::DataPod + 'static> LocalSubscriber<T> {
    pub fn take(&mut self) -> Result<Option<Sample<T>>> {
        match self
            .inner
            .receive()
            .map_err(|e| Error::Other(format!("iox receive: {e}")))?
        {
            Some(sample) => Ok(Some(Sample { inner: sample })),
            None => Ok(None),
        }
    }
}
