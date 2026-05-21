//! Typed entrypoint to the iceoryx2-backed local transport.
//!
//! Replaces the previous home-grown SHM allocator. A
//! `LocalService<T>` is one iceoryx2 publish/subscribe service
//! scoped to one payload type `T`. Multiple publishers and
//! subscribers can attach to the same service name on the same
//! host; iceoryx2 handles SHM allocation, slot recycling, and
//! cross-process discovery.

use core::fmt::Debug;
use std::sync::Arc;

use bytemuck::Pod;
use iceoryx2::node::Node as IoxNode;
use iceoryx2::node::NodeBuilder;
use iceoryx2::port::publisher::Publisher as IoxPublisher;
use iceoryx2::port::subscriber::Subscriber as IoxSubscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;

use crate::error::{Error, Result};
use crate::local::handle::{Loan, Sample};

/// Configurable parameters. iceoryx2 wires its own defaults so
/// most fields are advisory; we keep the struct for API parity
/// with the old allocator and to leave room for future tuning.
#[derive(Debug, Clone)]
pub struct LocalConfig {
    /// Maximum concurrent publishers that may attach to the
    /// service. iceoryx2 default is 2; we expose the knob.
    pub max_publishers: u32,
    /// Maximum concurrent subscribers.
    pub max_subscribers: u32,
    /// Per-subscriber sample buffer size. Subscribers that fall
    /// behind by more than this number of unread samples observe
    /// loss.
    pub subscriber_buffer: u32,
    /// History length kept on the publisher side for late-joining
    /// subscribers.
    pub history_depth: u32,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            max_publishers: 2,
            max_subscribers: 8,
            subscriber_buffer: 16,
            history_depth: 1,
        }
    }
}

/// Typed handle to an iceoryx2 publish-subscribe service.
///
/// Cheap to clone — internally an `Arc<IoxNode>` and an
/// `Arc<PortFactory>`. The first process to call `create_or_open`
/// for a given name owns the service definition; subsequent
/// callers attach.
pub struct LocalService<T: Pod + ZeroCopySend + Debug + 'static> {
    iox_node: Arc<IoxNode<ipc_threadsafe::Service>>,
    factory: Arc<PortFactory<ipc_threadsafe::Service, T, ()>>,
}

impl<T: Pod + ZeroCopySend + Debug + 'static> Clone for LocalService<T> {
    fn clone(&self) -> Self {
        Self {
            iox_node: self.iox_node.clone(),
            factory: self.factory.clone(),
        }
    }
}

impl<T: Pod + ZeroCopySend + Debug + 'static> LocalService<T> {
    /// Open the service named `name`, creating it if necessary.
    /// The first caller pins the QoS settings from `cfg`;
    /// subsequent attaches reuse the existing definition (their
    /// `cfg` is ignored beyond compatibility checks iceoryx2
    /// performs internally).
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let iox_node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(|e| Error::Other(format!("iox NodeBuilder: {e}")))?;

        let service_name: ServiceName = name
            .try_into()
            .map_err(|e| Error::invalid_argument(format!("bad service name '{name}': {e}")))?;

        let factory = iox_node
            .service_builder(&service_name)
            .publish_subscribe::<T>()
            .max_publishers(cfg.max_publishers as usize)
            .max_subscribers(cfg.max_subscribers as usize)
            .subscriber_max_buffer_size(cfg.subscriber_buffer as usize)
            .history_size(cfg.history_depth as usize)
            .open_or_create()
            .map_err(|e| Error::Other(format!("iox service open_or_create: {e}")))?;

        Ok(Self {
            iox_node: Arc::new(iox_node),
            factory: Arc::new(factory),
        })
    }

    /// Alias preserved for API parity with the old allocator.
    pub fn create(name: &str, cfg: LocalConfig) -> Result<Self> {
        Self::open_or_create(name, cfg)
    }

    /// Alias preserved for API parity. Same as `open_or_create`
    /// with default QoS.
    pub fn attach(name: &str) -> Result<Self> {
        Self::open_or_create(name, LocalConfig::default())
    }

    /// Open an existing service — does NOT create one if missing.
    /// Returns `Err` if no other process has created the service.
    /// quicbit uses this for same-host detection: if a publisher
    /// is registered for the topic name we route locally;
    /// otherwise we fall through to iroh.
    pub fn open_existing(name: &str) -> Result<Self> {
        let iox_node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .map_err(|e| Error::Other(format!("iox NodeBuilder: {e}")))?;

        let service_name: ServiceName = name
            .try_into()
            .map_err(|e| Error::invalid_argument(format!("bad service name '{name}': {e}")))?;

        let factory = iox_node
            .service_builder(&service_name)
            .publish_subscribe::<T>()
            .open()
            .map_err(|e| Error::Other(format!("iox service open: {e}")))?;

        Ok(Self {
            iox_node: Arc::new(iox_node),
            factory: Arc::new(factory),
        })
    }

    /// Number of publishers currently attached to this service —
    /// useful as a "is anyone broadcasting?" signal.
    pub fn publisher_count(&self) -> usize {
        use iceoryx2::service::port_factory::publish_subscribe::PortFactory as PF;
        // PortFactory's dynamic_config() lives on a trait, drag it in.
        use iceoryx2::service::port_factory::PortFactory as _;
        let _: &PF<ipc_threadsafe::Service, T, ()> = &self.factory;
        self.factory.dynamic_config().number_of_publishers()
    }

    /// Build a publisher.
    pub fn publisher(&self) -> Result<LocalPublisher<T>> {
        let publisher = self
            .factory
            .publisher_builder()
            .create()
            .map_err(|e| Error::Other(format!("iox publisher_builder: {e}")))?;
        Ok(LocalPublisher {
            inner: publisher,
            _service: self.clone(),
        })
    }

    /// Build a subscriber whose cursor starts at the next publish.
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

/// Publisher port. Loan a slot, write the payload in place,
/// publish.
pub struct LocalPublisher<T: Pod + ZeroCopySend + Debug + 'static> {
    inner: IoxPublisher<ipc_threadsafe::Service, T, ()>,
    _service: LocalService<T>,
}

impl<T: Pod + ZeroCopySend + Debug + 'static> LocalPublisher<T> {
    /// Loan an in-flight slot. Returned [`Loan<T>`] derefs mutably
    /// to a zero-initialised `T` living in shared memory. Mutate
    /// in place, then call [`publish`](Self::publish).
    pub fn loan(&mut self) -> Result<Loan<T>> {
        // `loan_uninit` gives a `SampleMut<MaybeUninit<T>>`; we
        // initialise it to all-zeros (which is a valid Pod value)
        // so the caller can DerefMut into a `&mut T` without
        // first having to assemble the full struct.
        let uninit = self
            .inner
            .loan_uninit()
            .map_err(|e| Error::Other(format!("iox loan_uninit: {e}")))?;
        let initialised = uninit.write_payload(T::zeroed());
        Ok(Loan { inner: initialised })
    }

    /// Hand the loan back to the publisher; the iceoryx2 backend
    /// dispatches the sample to all attached subscribers without a
    /// copy.
    pub fn publish(&mut self, loan: Loan<T>) -> Result<u64> {
        loan.inner
            .send()
            .map_err(|e| Error::Other(format!("iox send: {e}")))?;
        Ok(0) // iceoryx2 doesn't surface a per-publish seq #
    }

    /// Convenience: loan + write + publish.
    pub fn send(&mut self, value: T) -> Result<u64> {
        let mut loan = self.loan()?;
        *loan = value;
        self.publish(loan)
    }
}

/// Subscriber port. Non-blocking `take`.
pub struct LocalSubscriber<T: Pod + ZeroCopySend + Debug + 'static> {
    inner: IoxSubscriber<ipc_threadsafe::Service, T, ()>,
    _service: LocalService<T>,
}

impl<T: Pod + ZeroCopySend + Debug + 'static> LocalSubscriber<T> {
    /// Take the next sample if available. Returns
    /// `Ok(Some(Sample))` when a sample is ready, `Ok(None)` when
    /// the queue is empty, `Err` on backend failure.
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
