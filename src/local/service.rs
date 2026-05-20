//! Typed entrypoint: [`LocalService`] / [`LocalPublisher`] /
//! [`LocalSubscriber`].
//!
//! A `LocalService<T>` is one SHM segment scoped to a single payload
//! type `T: Pod`. Creating a service `shm_open`s the segment with
//! `O_CREAT | O_EXCL`; attaching does `O_RDWR` and validates the
//! magic / version / type hash. Same process can hold any number of
//! publishers and subscribers — the per-segment refcount handles
//! cross-process teardown automatically.

use std::any::type_name;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytemuck::Pod;

use crate::error::{Error, Result};
use crate::local::handle::{Loan, Sample};
use crate::local::layout::fnv1a64;
use crate::local::segment::{Segment, SegmentParams};

/// Configurable parameters for a local SHM service.
#[derive(Debug, Clone)]
pub struct LocalConfig {
    /// Number of slots in the pool.
    pub slot_count: u32,
    /// Per-slot payload capacity in bytes. Must be `>= size_of::<T>()`
    /// at the time a publisher/subscriber is constructed.
    pub slot_size: u32,
    /// Number of past samples a late-joining subscriber can replay.
    /// Defaults to 1 (latest only).
    pub history_depth: u32,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            slot_count: 16,
            slot_size: 256,
            history_depth: 1,
        }
    }
}

/// A typed local SHM service. Cheap to clone — clones share the
/// underlying mapping.
#[derive(Clone)]
pub struct LocalService<T: Pod> {
    segment: Segment,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Pod> LocalService<T> {
    /// Create a brand-new service named `name`. Returns
    /// [`Error::ServiceAlreadyExists`] if a segment with this name
    /// is already present in `/dev/shm`.
    pub fn create(name: &str, cfg: LocalConfig) -> Result<Self> {
        let required = std::mem::size_of::<T>();
        if (cfg.slot_size as usize) < required {
            return Err(Error::PayloadTooLarge {
                actual: required,
                capacity: cfg.slot_size as usize,
            });
        }
        let segment = Segment::create(
            name,
            SegmentParams {
                slot_count: cfg.slot_count,
                slot_size: cfg.slot_size,
                history_depth: cfg.history_depth,
                type_name: type_name::<T>(),
            },
        )?;
        Ok(Self {
            segment,
            _phantom: PhantomData,
        })
    }

    /// Attach to an existing service.
    pub fn attach(name: &str) -> Result<Self> {
        let expected = fnv1a64(type_name::<T>());
        let segment = Segment::attach(name, expected)?;
        if (segment.slot_size() as usize) < std::mem::size_of::<T>() {
            return Err(Error::PayloadTooLarge {
                actual: std::mem::size_of::<T>(),
                capacity: segment.slot_size() as usize,
            });
        }
        Ok(Self {
            segment,
            _phantom: PhantomData,
        })
    }

    /// Create-or-attach: try `create`, fall back to `attach` on
    /// collision. Convenient when multiple processes race to create
    /// the same service.
    pub fn open_or_create(name: &str, cfg: LocalConfig) -> Result<Self> {
        match Self::create(name, cfg.clone()) {
            Ok(svc) => Ok(svc),
            Err(Error::ServiceAlreadyExists(_)) => Self::attach(name),
            Err(e) => Err(e),
        }
    }

    /// Build a publisher handle. Cheap — does not allocate slots.
    pub fn publisher(&self) -> LocalPublisher<T> {
        LocalPublisher {
            segment: self.segment.clone(),
            _phantom: PhantomData,
        }
    }

    /// Build a subscriber handle. The subscriber's first `take` will
    /// return whatever is currently in the history window (newest
    /// first).
    pub fn subscriber(&self) -> LocalSubscriber<T> {
        // Start at "next message after current latest" so subscribers
        // never replay history that pre-dates their attach.
        let start_next = self.segment.latest_seq() + 1;
        LocalSubscriber {
            segment: self.segment.clone(),
            next_seq: Arc::new(AtomicU64::new(start_next)),
            _phantom: PhantomData,
        }
    }

    /// Subscriber that starts at the oldest still-resident sample in
    /// the history ring (rather than future-only). Useful for tests
    /// where you want the full visible backlog without a transient
    /// `Lagged` on the first take.
    pub fn subscriber_from_start(&self) -> LocalSubscriber<T> {
        let latest = self.segment.latest_seq();
        let depth = self.segment.history_depth() as u64;
        let start_next = if latest == 0 {
            1
        } else if latest > depth {
            latest - depth + 1
        } else {
            1
        };
        LocalSubscriber {
            segment: self.segment.clone(),
            next_seq: Arc::new(AtomicU64::new(start_next)),
            _phantom: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.segment.name()
    }

    /// Internal accessor, exposed at `pub(crate)` for the FFI /
    /// Python module shims so they can read segment metadata without
    /// reaching into private fields.
    #[allow(dead_code)]
    pub(crate) fn segment(&self) -> &Segment {
        &self.segment
    }
}

/// Handle that loans slots and publishes them.
pub struct LocalPublisher<T: Pod> {
    segment: Segment,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Pod> LocalPublisher<T> {
    /// Reserve one slot for in-place writes. The returned [`Loan`]
    /// derefs to `T` initialized to all-zeros (POD safe).
    pub fn loan(&mut self) -> Result<Loan<T>> {
        let slot_idx = self.segment.pop_free().ok_or_else(|| Error::NoFreeSlot {
            service: self.segment.name().to_string(),
        })?;
        // Zero-init the payload — Pod permits this and it gives the
        // user a predictable starting state.
        unsafe {
            std::ptr::write_bytes(
                self.segment.slot_payload(slot_idx),
                0u8,
                std::mem::size_of::<T>(),
            );
        }
        Ok(Loan::new(self.segment.clone(), slot_idx))
    }

    /// Hand the loan over to subscribers. Returns the assigned
    /// publish sequence.
    pub fn publish(&mut self, loan: Loan<T>) -> Result<u64> {
        let slot_idx = loan.mark_published();
        let seq = self.segment.publish_slot(slot_idx);
        Ok(seq)
    }

    /// Shortcut for "loan + write + publish" when you have a `T`
    /// already.
    pub fn send(&mut self, value: T) -> Result<u64> {
        let mut loan = self.loan()?;
        *loan = value;
        self.publish(loan)
    }
}

/// Handle that pulls samples off the publish ring.
pub struct LocalSubscriber<T: Pod> {
    segment: Segment,
    next_seq: Arc<AtomicU64>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Pod> LocalSubscriber<T> {
    /// Non-blocking take. Returns:
    /// * `Ok(Some(sample))` — a fresh sample is available.
    /// * `Ok(None)` — no new sample since the last take.
    /// * `Err(Lagged { dropped })` — subscriber fell behind; the
    ///   internal cursor has been fast-forwarded so the next take
    ///   sees the freshest available sample.
    pub fn take(&mut self) -> Result<Option<Sample<T>>> {
        let wanted = self.next_seq.load(Ordering::Acquire);
        match self.segment.try_acquire(wanted) {
            Ok(Some((idx, seq))) => {
                self.next_seq.store(seq + 1, Ordering::Release);
                Ok(Some(Sample::new(self.segment.clone(), idx, seq)))
            }
            Ok(None) => Ok(None),
            Err(Error::Lagged { dropped }) => {
                // Fast-forward past the lost window so the next call
                // sees the freshest available sample. Surface the
                // lag once.
                let latest = self.segment.latest_seq();
                let depth = self.segment.history_depth() as u64;
                let new_next = if latest > depth { latest - depth + 1 } else { 1 };
                self.next_seq.store(new_next, Ordering::Release);
                Err(Error::Lagged { dropped })
            }
            Err(other) => Err(other),
        }
    }
}

impl<T: Pod> Clone for LocalSubscriber<T> {
    /// Each clone tracks the SAME read cursor — useful when sharing
    /// a subscriber across threads. For an independent cursor, build
    /// a fresh subscriber via `LocalService::subscriber()`.
    fn clone(&self) -> Self {
        Self {
            segment: self.segment.clone(),
            next_seq: Arc::clone(&self.next_seq),
            _phantom: PhantomData,
        }
    }
}
