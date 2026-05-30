//! Pure-Rust shared-memory ring used by the local transport.
//!
//! This module is intentionally small and POD-only: one named
//! `shared_memory` mapping per service, a fixed control block, and a
//! fixed-size ring of slots. The public local API still exposes
//! loan/fill/publish and non-blocking take; the implementation details
//! stay private to `src/local`.

use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytemuck::{Pod, Zeroable};
use shared_memory::{Shmem, ShmemConf};

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::transport::fnv1a64;

const MAGIC: u64 = 0x5155_4943_4249_5431; // "QUICBIT1"
const VERSION: u32 = 3;
const MAX_SERVICE_NAME_BYTES: usize = 200;
const MAX_TRACKED_PUBLISHERS: usize = 64;
const MAX_TRACKED_SUBSCRIBERS: usize = 31;
const WRITER_STATE: u32 = u32::MAX;

#[repr(C, align(64))]
struct ControlBlock {
    magic: AtomicU64,
    version: AtomicU32,
    type_hash_hi: AtomicU32,
    type_hash_lo: AtomicU32,
    header_size: AtomicU32,
    payload_cap: AtomicU32,
    slot_count: AtomicU32,
    slot_stride: AtomicU32,
    write_seq: AtomicU64,
    publishers: AtomicU32,
    subscribers: AtomicU32,
    max_publishers: AtomicU32,
    max_subscribers: AtomicU32,
    subscriber_buffer: AtomicU32,
    history_depth: AtomicU32,
    publisher_pids: [AtomicU32; MAX_TRACKED_PUBLISHERS],
    publisher_tokens: [AtomicU64; MAX_TRACKED_PUBLISHERS],
    subscriber_pids: [AtomicU32; MAX_TRACKED_SUBSCRIBERS],
    subscriber_tokens: [AtomicU64; MAX_TRACKED_SUBSCRIBERS],
}

#[repr(C, align(8))]
struct SlotHeader {
    seq: AtomicU64,
    /// Reader bitmask, or [`WRITER_STATE`] while a producer owns the slot.
    ///
    /// Each subscriber gets one bit for the life of the subscriber plus
    /// any samples derived from it. That makes dead-reader cleanup
    /// possible: if a process dies while holding a sample, the next
    /// publisher can clear only that process' bit instead of pinning the
    /// slot forever.
    refcount: AtomicU32,
    len: AtomicU32,
    writer_pid: AtomicU32,
    writer_token: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct Layout {
    total_size: usize,
    slots_offset: usize,
    slot_stride: usize,
    header_offset: usize,
    payload_offset: usize,
    payload_cap: usize,
    slot_count: usize,
}

impl Layout {
    fn new<H>(cfg: &LocalConfig) -> Result<Self> {
        validate_config(cfg)?;
        let payload_cap = cfg.max_payload_bytes;
        if payload_cap > u32::MAX as usize {
            return Err(Error::invalid_argument(format!(
                "max_payload_bytes too large: {payload_cap}"
            )));
        }

        let history = cfg.history_depth.max(1) as usize;
        let subscribers = cfg.max_subscribers.max(1) as usize;
        let slot_count = history
            .saturating_mul(subscribers)
            .max(cfg.subscriber_buffer.max(1) as usize)
            .max(1);
        if slot_count > u32::MAX as usize {
            return Err(Error::invalid_argument(format!(
                "slot count too large: {slot_count}"
            )));
        }

        let header_align = std::mem::align_of::<H>().max(1);
        let header_offset = align_up(std::mem::size_of::<SlotHeader>(), header_align);
        let payload_offset = header_offset
            .checked_add(std::mem::size_of::<H>())
            .ok_or_else(|| Error::invalid_argument("slot layout overflow"))?;
        let slot_stride = align_up(
            payload_offset
                .checked_add(payload_cap)
                .ok_or_else(|| Error::invalid_argument("slot layout overflow"))?,
            8,
        );
        let slots_offset = align_up(std::mem::size_of::<ControlBlock>(), 64);
        let total_size = slots_offset
            .checked_add(
                slot_stride
                    .checked_mul(slot_count)
                    .ok_or_else(|| Error::invalid_argument("segment layout overflow"))?,
            )
            .ok_or_else(|| Error::invalid_argument("segment layout overflow"))?;

        Ok(Self {
            total_size,
            slots_offset,
            slot_stride,
            header_offset,
            payload_offset,
            payload_cap,
            slot_count,
        })
    }
}

fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (value + align - 1) & !(align - 1)
}

/// A named SHM segment containing one typed ring.
pub(crate) struct Segment<H: Pod + Zeroable + Copy + 'static> {
    _shmem: Shmem,
    ptr: NonNull<u8>,
    layout: Layout,
    key: String,
    type_hash: u64,
    _header: PhantomData<H>,
}

// SAFETY: `Segment` points at a shared mapping whose interior mutation is
// coordinated by atomics in the mapping. `H` is POD and copied as bytes.
unsafe impl<H: Pod + Zeroable + Copy + 'static> Send for Segment<H> {}
// SAFETY: Shared access to the mapping is safe under the same atomic
// protocol; mutable user access is only handed out through a claimed `Loan`.
unsafe impl<H: Pod + Zeroable + Copy + 'static> Sync for Segment<H> {}

impl<H: Pod + Zeroable + Copy + 'static> Segment<H> {
    pub(crate) fn open_or_create(
        name: &str,
        type_hash: u64,
        cfg: LocalConfig,
    ) -> Result<Arc<Self>> {
        match Self::create(name, type_hash, cfg.clone()) {
            Ok(segment) => Ok(segment),
            Err(err @ Error::InvalidArgument(_)) | Err(err @ Error::TopicNameTooLong { .. }) => {
                Err(err)
            }
            Err(_) => Self::open_existing(name, type_hash),
        }
    }

    pub(crate) fn create(name: &str, type_hash: u64, cfg: LocalConfig) -> Result<Arc<Self>> {
        validate_name(name)?;
        let key = os_key(name);
        let layout = Layout::new::<H>(&cfg)?;
        let shmem = ShmemConf::new()
            .os_id(&key)
            .size(layout.total_size)
            .create()
            .map_err(|e| Error::Other(format!("shared_memory create {key}: {e}")))?;

        let ptr = NonNull::new(shmem.as_ptr()).ok_or_else(|| {
            Error::Other(format!("shared_memory create {key}: returned null mapping"))
        })?;

        // SAFETY: `ptr` is a valid writable mapping of `layout.total_size`
        // bytes just created by this process. Zeroing POD control/slot
        // storage before publishing `MAGIC` gives openers a clean segment.
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr(), 0, layout.total_size);
        }

        let segment = Arc::new(Self {
            _shmem: shmem,
            ptr,
            layout,
            key,
            type_hash,
            _header: PhantomData,
        });
        segment.init_control(&cfg)?;
        Ok(segment)
    }

    pub(crate) fn open_existing(name: &str, type_hash: u64) -> Result<Arc<Self>> {
        validate_name(name)?;
        let key = os_key(name);
        let shmem = ShmemConf::new()
            .os_id(&key)
            .open()
            .map_err(|e| Error::ServiceNotFound(format!("{name} ({key}): {e}")))?;
        let ptr = NonNull::new(shmem.as_ptr())
            .ok_or_else(|| Error::Other(format!("shared_memory open {key}: null mapping")))?;

        let control = unsafe_control(ptr);
        wait_until_initialised(control, name)?;
        validate_control::<H>(control, type_hash)?;

        let cfg = LocalConfig {
            max_publishers: control.max_publishers.load(Ordering::Acquire),
            max_subscribers: control.max_subscribers.load(Ordering::Acquire),
            subscriber_buffer: control.subscriber_buffer.load(Ordering::Acquire),
            history_depth: control.history_depth.load(Ordering::Acquire),
            max_payload_bytes: control.payload_cap.load(Ordering::Acquire) as usize,
        };
        let layout = Layout::new::<H>(&cfg)?;
        validate_layout(control, &layout)?;

        Ok(Arc::new(Self {
            _shmem: shmem,
            ptr,
            layout,
            key,
            type_hash,
            _header: PhantomData,
        }))
    }

    fn init_control(&self, cfg: &LocalConfig) -> Result<()> {
        let control = self.control();
        control.version.store(VERSION, Ordering::Relaxed);
        control
            .type_hash_hi
            .store((self.type_hash >> 32) as u32, Ordering::Relaxed);
        control
            .type_hash_lo
            .store(self.type_hash as u32, Ordering::Relaxed);
        control
            .header_size
            .store(std::mem::size_of::<H>() as u32, Ordering::Relaxed);
        control
            .payload_cap
            .store(self.layout.payload_cap as u32, Ordering::Relaxed);
        control
            .slot_count
            .store(self.layout.slot_count as u32, Ordering::Relaxed);
        control
            .slot_stride
            .store(self.layout.slot_stride as u32, Ordering::Relaxed);
        control.write_seq.store(0, Ordering::Relaxed);
        control.publishers.store(0, Ordering::Relaxed);
        control.subscribers.store(0, Ordering::Relaxed);
        control
            .max_publishers
            .store(cfg.max_publishers, Ordering::Relaxed);
        control
            .max_subscribers
            .store(cfg.max_subscribers, Ordering::Relaxed);
        control
            .subscriber_buffer
            .store(cfg.subscriber_buffer, Ordering::Relaxed);
        control
            .history_depth
            .store(cfg.history_depth, Ordering::Relaxed);
        control.magic.store(MAGIC, Ordering::Release);
        Ok(())
    }

    pub(crate) fn publisher_count(&self) -> usize {
        self.reap_dead_publishers();
        self.control().publishers.load(Ordering::Acquire) as usize
    }

    pub(crate) fn producer(self: &Arc<Self>) -> Result<Producer<H>> {
        let lease = Arc::new(ProducerLease {
            segment: Arc::clone(self),
            slot: self.register_publisher()?,
            pid: current_pid(),
            token: current_process_token(),
        });
        Ok(Producer { lease })
    }

    pub(crate) fn consumer(self: &Arc<Self>) -> Result<Consumer<H>> {
        let consumer_id = self.register_subscriber()?;

        let control = self.control();
        let latest = control.write_seq.load(Ordering::Acquire);
        let history = control.history_depth.load(Ordering::Acquire).max(1) as u64;
        let read_cursor = latest.saturating_sub(history);

        Ok(Consumer {
            lease: Arc::new(ConsumerLease {
                segment: Arc::clone(self),
                slot: consumer_id,
                pid: current_pid(),
                token: current_process_token(),
            }),
            read_cursor,
        })
    }

    fn register_publisher(&self) -> Result<usize> {
        self.reap_dead_publishers();
        let control = self.control();
        let max = control.max_publishers.load(Ordering::Acquire) as usize;
        let pid = current_pid();
        let token = current_process_token();
        for index in 0..max {
            if control.publisher_pids[index]
                .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                control.publisher_tokens[index].store(token, Ordering::Release);
                control.publishers.fetch_add(1, Ordering::AcqRel);
                return Ok(index);
            }
        }
        self.reap_dead_publishers();
        Err(Error::Other(format!(
            "too many publishers on {}: max {}",
            self.key, max
        )))
    }

    fn deregister_publisher(&self, index: usize, pid: u32, token: u64) {
        let control = self.control();
        if control.publisher_tokens[index].load(Ordering::Acquire) != token {
            return;
        }
        if control.publisher_pids[index]
            .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            control.publisher_tokens[index].store(0, Ordering::Release);
            decrement_counter(&control.publishers);
        }
    }

    fn reap_dead_publishers(&self) {
        let control = self.control();
        let max = control.max_publishers.load(Ordering::Acquire) as usize;
        for index in 0..max.min(MAX_TRACKED_PUBLISHERS) {
            let pid = control.publisher_pids[index].load(Ordering::Acquire);
            let token = control.publisher_tokens[index].load(Ordering::Acquire);
            if pid != 0
                && !process_alive(pid, token)
                && control.publisher_pids[index]
                    .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                control.publisher_tokens[index].store(0, Ordering::Release);
                decrement_counter(&control.publishers);
            }
        }
    }

    fn register_subscriber(&self) -> Result<usize> {
        self.reap_dead_subscribers();
        let control = self.control();
        let max = control.max_subscribers.load(Ordering::Acquire) as usize;
        let pid = current_pid();
        let token = current_process_token();
        for index in 0..max {
            if control.subscriber_pids[index]
                .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                control.subscriber_tokens[index].store(token, Ordering::Release);
                control.subscribers.fetch_add(1, Ordering::AcqRel);
                return Ok(index);
            }
        }
        self.reap_dead_subscribers();
        Err(Error::Other(format!(
            "too many subscribers on {}: max {}",
            self.key, max
        )))
    }

    fn deregister_subscriber(&self, index: usize, pid: u32, token: u64) {
        let control = self.control();
        if control.subscriber_tokens[index].load(Ordering::Acquire) != token {
            return;
        }
        if control.subscriber_pids[index]
            .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            control.subscriber_tokens[index].store(0, Ordering::Release);
            decrement_counter(&control.subscribers);
        }
    }

    fn reap_dead_subscribers(&self) {
        let control = self.control();
        let max = control.max_subscribers.load(Ordering::Acquire) as usize;
        for index in 0..max.min(MAX_TRACKED_SUBSCRIBERS) {
            let pid = control.subscriber_pids[index].load(Ordering::Acquire);
            let token = control.subscriber_tokens[index].load(Ordering::Acquire);
            if pid != 0
                && !process_alive(pid, token)
                && control.subscriber_pids[index]
                    .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                control.subscriber_tokens[index].store(0, Ordering::Release);
                decrement_counter(&control.subscribers);
            }
        }
    }

    fn reap_dead_slot_holders(&self, slot: &SlotHeader) {
        let state = slot.refcount.load(Ordering::Acquire);
        if state == 0 {
            return;
        }
        if state == WRITER_STATE {
            let pid = slot.writer_pid.load(Ordering::Acquire);
            let token = slot.writer_token.load(Ordering::Acquire);
            if pid != 0
                && !process_alive(pid, token)
                && slot
                    .refcount
                    .compare_exchange(WRITER_STATE, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                slot.writer_pid.store(0, Ordering::Release);
                slot.writer_token.store(0, Ordering::Release);
                slot.seq.store(0, Ordering::Release);
            }
            return;
        }

        let control = self.control();
        let max = control.max_subscribers.load(Ordering::Acquire) as usize;
        for index in 0..max.min(MAX_TRACKED_SUBSCRIBERS) {
            let bit = 1u32 << index;
            if state & bit == 0 {
                continue;
            }
            let pid = control.subscriber_pids[index].load(Ordering::Acquire);
            let token = control.subscriber_tokens[index].load(Ordering::Acquire);
            if pid == 0 || !process_alive(pid, token) {
                slot.refcount.fetch_and(!bit, Ordering::AcqRel);
                if pid != 0
                    && control.subscriber_pids[index]
                        .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    control.subscriber_tokens[index].store(0, Ordering::Release);
                    decrement_counter(&control.subscribers);
                }
            }
        }
    }

    fn control(&self) -> &ControlBlock {
        unsafe_control(self.ptr)
    }

    fn slot_header(&self, index: usize) -> &SlotHeader {
        debug_assert!(index < self.layout.slot_count);
        let offset = self.layout.slots_offset + index * self.layout.slot_stride;
        // SAFETY: layout construction keeps every slot within the mapping
        // and aligned to at least 8 bytes for `SlotHeader`.
        unsafe { &*(self.ptr.as_ptr().add(offset).cast::<SlotHeader>()) }
    }

    fn header_ptr(&self, index: usize) -> *mut H {
        let offset =
            self.layout.slots_offset + index * self.layout.slot_stride + self.layout.header_offset;
        // SAFETY: caller uses the pointer according to the slot protocol.
        unsafe { self.ptr.as_ptr().add(offset).cast::<H>() }
    }

    fn payload_ptr(&self, index: usize) -> *mut u8 {
        let offset =
            self.layout.slots_offset + index * self.layout.slot_stride + self.layout.payload_offset;
        // SAFETY: caller bounds slices by `payload_cap`/recorded `len`.
        unsafe { self.ptr.as_ptr().add(offset) }
    }

    fn slot_index(&self, seq: u64) -> usize {
        ((seq - 1) as usize) % self.layout.slot_count
    }
}

pub(crate) struct Producer<H: Pod + Zeroable + Copy + 'static> {
    lease: Arc<ProducerLease<H>>,
}

struct ProducerLease<H: Pod + Zeroable + Copy + 'static> {
    segment: Arc<Segment<H>>,
    slot: usize,
    pid: u32,
    token: u64,
}

impl<H: Pod + Zeroable + Copy + 'static> Producer<H> {
    pub(crate) fn loan(&mut self, byte_count: usize) -> Result<Loan<H>> {
        if byte_count > self.lease.segment.layout.payload_cap {
            return Err(Error::PayloadTooLarge {
                actual: byte_count,
                capacity: self.lease.segment.layout.payload_cap,
            });
        }

        let mut spin_attempts = 0u32;
        let (seq, index, slot) = loop {
            let current = self
                .lease
                .segment
                .control()
                .write_seq
                .load(Ordering::Acquire);
            let seq = current + 1;
            let index = self.lease.segment.slot_index(seq);
            let slot = self.lease.segment.slot_header(index);

            let mut state = slot.refcount.load(Ordering::Acquire);
            if state != 0 {
                self.lease.segment.reap_dead_slot_holders(slot);
                state = slot.refcount.load(Ordering::Acquire);
            }
            if state != 0 {
                if state == WRITER_STATE && spin_attempts < 1024 {
                    spin_attempts += 1;
                    std::thread::yield_now();
                    continue;
                }
                return Err(Error::NoFreeSlot {
                    service: self.lease.segment.key.clone(),
                });
            }
            if let Err(actual) =
                slot.refcount
                    .compare_exchange(0, WRITER_STATE, Ordering::AcqRel, Ordering::Acquire)
            {
                if actual == WRITER_STATE && spin_attempts < 1024 {
                    spin_attempts += 1;
                    std::thread::yield_now();
                    continue;
                }
                return Err(Error::NoFreeSlot {
                    service: self.lease.segment.key.clone(),
                });
            }
            slot.writer_token
                .store(current_process_token(), Ordering::Release);
            slot.writer_pid.store(current_pid(), Ordering::Release);

            match self.lease.segment.control().write_seq.compare_exchange(
                current,
                seq,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break (seq, index, slot),
                Err(_) => {
                    slot.writer_pid.store(0, Ordering::Release);
                    slot.writer_token.store(0, Ordering::Release);
                    slot.refcount.store(0, Ordering::Release);
                    std::thread::yield_now();
                }
            }
        };

        slot.seq.store(0, Ordering::Release);
        slot.len.store(byte_count as u32, Ordering::Release);

        // SAFETY: writer ownership is marked by `WRITER_STATE`, so no
        // reader can acquire this slot while we zero/copy into it.
        unsafe {
            self.lease.segment.header_ptr(index).write(H::zeroed());
            std::ptr::write_bytes(
                self.lease.segment.payload_ptr(index),
                0,
                self.lease.segment.layout.payload_cap,
            );
        }

        Ok(Loan {
            segment: Arc::clone(&self.lease.segment),
            index,
            seq,
            len: byte_count,
            published: false,
            _header: PhantomData,
        })
    }

    pub(crate) fn publish(&mut self, mut loan: Loan<H>) -> Result<u64> {
        loan.commit()
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for ProducerLease<H> {
    fn drop(&mut self) {
        self.segment
            .deregister_publisher(self.slot, self.pid, self.token);
    }
}

pub(crate) struct Loan<H: Pod + Zeroable + Copy + 'static> {
    segment: Arc<Segment<H>>,
    index: usize,
    seq: u64,
    len: usize,
    published: bool,
    _header: PhantomData<H>,
}

impl<H: Pod + Zeroable + Copy + 'static> Loan<H> {
    pub(crate) fn header(&self) -> &H {
        // SAFETY: a live loan owns the slot for writing; shared access from
        // `&self` is read-only and bounded by the loan lifetime.
        unsafe { &*self.segment.header_ptr(self.index).cast_const() }
    }

    pub(crate) fn header_mut(&mut self) -> &mut H {
        // SAFETY: the loan owns this slot (`WRITER_STATE`) and `&mut self`
        // gives unique access to the header value.
        unsafe { &mut *self.segment.header_ptr(self.index) }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        // SAFETY: `len` was validated at loan time.
        unsafe { std::slice::from_raw_parts(self.segment.payload_ptr(self.index), self.len) }
    }

    pub(crate) fn payload_mut(&mut self) -> &mut [u8] {
        // SAFETY: the loan owns this slot (`WRITER_STATE`) and `len` was
        // validated at loan time.
        unsafe { std::slice::from_raw_parts_mut(self.segment.payload_ptr(self.index), self.len) }
    }

    fn commit(&mut self) -> Result<u64> {
        let slot = self.segment.slot_header(self.index);
        slot.len.store(self.len as u32, Ordering::Release);
        slot.seq.store(self.seq, Ordering::Release);
        slot.writer_pid.store(0, Ordering::Release);
        slot.writer_token.store(0, Ordering::Release);
        slot.refcount.store(0, Ordering::Release);
        self.published = true;
        Ok(self.seq)
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for Loan<H> {
    fn drop(&mut self) {
        if !self.published {
            let slot = self.segment.slot_header(self.index);
            slot.seq.store(0, Ordering::Release);
            slot.writer_pid.store(0, Ordering::Release);
            slot.writer_token.store(0, Ordering::Release);
            slot.refcount.store(0, Ordering::Release);
        }
    }
}

pub(crate) struct Consumer<H: Pod + Zeroable + Copy + 'static> {
    lease: Arc<ConsumerLease<H>>,
    read_cursor: u64,
}

struct ConsumerLease<H: Pod + Zeroable + Copy + 'static> {
    segment: Arc<Segment<H>>,
    slot: usize,
    pid: u32,
    token: u64,
}

impl<H: Pod + Zeroable + Copy + 'static> Consumer<H> {
    pub(crate) fn take(&mut self) -> Result<Option<Sample<H>>> {
        let latest = self
            .lease
            .segment
            .control()
            .write_seq
            .load(Ordering::Acquire);
        if self.read_cursor >= latest {
            return Ok(None);
        }

        let next = self.read_cursor + 1;
        let index = self.lease.segment.slot_index(next);
        let slot = self.lease.segment.slot_header(index);
        let observed = slot.seq.load(Ordering::Acquire);

        if observed == 0 {
            return Ok(None);
        }

        if observed != next {
            if observed > next {
                let dropped = observed - next;
                self.read_cursor = observed - 1;
                return Err(Error::Lagged { dropped });
            }
            // `write_seq` is claimed before the slot is committed. Seeing an
            // older sequence here means the next sample is still in progress;
            // do not advance the cursor or we would silently skip it.
            return Ok(None);
        }

        let bit = 1u32 << self.lease.slot;
        loop {
            let refs = slot.refcount.load(Ordering::Acquire);
            if refs == WRITER_STATE {
                return Ok(None);
            }
            let next_refs = refs | bit;
            match slot.refcount.compare_exchange_weak(
                refs,
                next_refs,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }

        let reread = slot.seq.load(Ordering::Acquire);
        if reread != next {
            slot.refcount.fetch_and(!bit, Ordering::AcqRel);
            if reread > next {
                let dropped = reread - next;
                self.read_cursor = reread - 1;
                return Err(Error::Lagged { dropped });
            }
            // Writer claimed `next` but has not committed it yet.
            return Ok(None);
        }

        let len = slot.len.load(Ordering::Acquire) as usize;
        if len > self.lease.segment.layout.payload_cap {
            slot.refcount.fetch_and(!bit, Ordering::AcqRel);
            return Err(Error::incompatible_shm(format!(
                "slot payload len {len} exceeds cap {}",
                self.lease.segment.layout.payload_cap
            )));
        }

        self.read_cursor = next;
        Ok(Some(Sample {
            lease: Arc::clone(&self.lease),
            index,
            seq: next,
            len,
        }))
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for ConsumerLease<H> {
    fn drop(&mut self) {
        self.segment
            .deregister_subscriber(self.slot, self.pid, self.token);
    }
}

pub(crate) struct Sample<H: Pod + Zeroable + Copy + 'static> {
    lease: Arc<ConsumerLease<H>>,
    index: usize,
    seq: u64,
    len: usize,
}

impl<H: Pod + Zeroable + Copy + 'static> Sample<H> {
    pub(crate) fn header(&self) -> &H {
        // SAFETY: the consumer acquired a refcount and verified `seq`.
        unsafe { &*self.lease.segment.header_ptr(self.index).cast_const() }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        // SAFETY: `len` was read from the slot after acquiring a refcount.
        unsafe { std::slice::from_raw_parts(self.lease.segment.payload_ptr(self.index), self.len) }
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.seq
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Drop for Sample<H> {
    fn drop(&mut self) {
        let bit = 1u32 << self.lease.slot;
        self.lease
            .segment
            .slot_header(self.index)
            .refcount
            .fetch_and(!bit, Ordering::AcqRel);
    }
}

fn validate_config(cfg: &LocalConfig) -> Result<()> {
    if cfg.max_publishers as usize > MAX_TRACKED_PUBLISHERS {
        return Err(Error::invalid_argument(format!(
            "max_publishers {} exceeds tracked-process cap {}",
            cfg.max_publishers, MAX_TRACKED_PUBLISHERS
        )));
    }
    if cfg.max_subscribers as usize > MAX_TRACKED_SUBSCRIBERS {
        return Err(Error::invalid_argument(format!(
            "max_subscribers {} exceeds tracked-process cap {}",
            cfg.max_subscribers, MAX_TRACKED_SUBSCRIBERS
        )));
    }
    Ok(())
}

fn decrement_counter(counter: &AtomicU32) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_sub(1))
    });
}

fn current_pid() -> u32 {
    std::process::id()
}

fn current_process_token() -> u64 {
    process_start_token(current_pid()).unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn process_start_token(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rsplit_once(") ")?.1;
    // Field 22 (`starttime`) is index 19 after stripping fields 1 and 2
    // (`pid` and `comm`). See `proc_pid_stat(5)`.
    after_comm.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_start_token(_pid: u32) -> Option<u64> {
    None
}

#[cfg(not(unix))]
fn process_start_token(_pid: u32) -> Option<u64> {
    None
}

#[cfg(unix)]
fn process_alive(pid: u32, token: u64) -> bool {
    if token != 0 {
        return process_start_token(pid) == Some(token);
    }
    // SAFETY: `kill(pid, 0)` does not deliver a signal; it only asks the
    // kernel whether the process exists and is visible to this process.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    )
}

#[cfg(not(unix))]
fn process_alive(pid: u32, token: u64) -> bool {
    // Conservative fallback for platforms where this module has not grown a
    // native liveness probe yet: never reap another process' slots.
    pid == current_pid() && (token == 0 || token == current_process_token())
}

fn validate_name(name: &str) -> Result<()> {
    let len = name.len();
    if len == 0 {
        return Err(Error::invalid_argument("service name must not be empty"));
    }
    if len > MAX_SERVICE_NAME_BYTES {
        return Err(Error::TopicNameTooLong {
            len,
            limit: MAX_SERVICE_NAME_BYTES,
        });
    }
    Ok(())
}

fn os_key(name: &str) -> String {
    format!("qb_{:016x}", fnv1a64(name))
}

fn unsafe_control(ptr: NonNull<u8>) -> &'static ControlBlock {
    // SAFETY: every segment mapping starts with a `ControlBlock`.
    unsafe { &*(ptr.as_ptr().cast::<ControlBlock>()) }
}

fn wait_until_initialised(control: &ControlBlock, name: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    while control.magic.load(Ordering::Acquire) != MAGIC {
        if Instant::now() >= deadline {
            return Err(Error::incompatible_shm(format!(
                "service {name} did not finish initialising"
            )));
        }
        std::thread::yield_now();
    }
    Ok(())
}

fn validate_control<H: Pod + Zeroable + Copy + 'static>(
    control: &ControlBlock,
    expected_type_hash: u64,
) -> Result<()> {
    let version = control.version.load(Ordering::Acquire);
    if version != VERSION {
        return Err(Error::incompatible_shm(format!(
            "version mismatch: expected {VERSION}, got {version}"
        )));
    }

    let actual_type_hash = ((control.type_hash_hi.load(Ordering::Acquire) as u64) << 32)
        | control.type_hash_lo.load(Ordering::Acquire) as u64;
    if actual_type_hash != expected_type_hash {
        return Err(Error::incompatible_shm(format!(
            "type hash mismatch: expected {expected_type_hash:#x}, got {actual_type_hash:#x}"
        )));
    }

    let actual_header_size = control.header_size.load(Ordering::Acquire) as usize;
    let expected_header_size = std::mem::size_of::<H>();
    if actual_header_size != expected_header_size {
        return Err(Error::incompatible_shm(format!(
            "header size mismatch: expected {expected_header_size}, got {actual_header_size}"
        )));
    }

    Ok(())
}

fn validate_layout(control: &ControlBlock, expected: &Layout) -> Result<()> {
    let slot_count = control.slot_count.load(Ordering::Acquire) as usize;
    let slot_stride = control.slot_stride.load(Ordering::Acquire) as usize;
    let payload_cap = control.payload_cap.load(Ordering::Acquire) as usize;
    if slot_count != expected.slot_count
        || slot_stride != expected.slot_stride
        || payload_cap != expected.payload_cap
    {
        return Err(Error::incompatible_shm(format!(
            "layout mismatch: slots {slot_count}/{}, stride {slot_stride}/{}, payload {payload_cap}/{}",
            expected.slot_count, expected.slot_stride, expected.payload_cap
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
    struct Header {
        value: u32,
    }

    fn unique_name(stem: &str) -> String {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("quicbit_shm_{stem}_{pid}_{nanos}")
    }

    #[test]
    fn create_open_round_trip() {
        let name = unique_name("round_trip");
        let segment = Segment::<Header>::create(&name, 0xfeed, LocalConfig::default()).unwrap();
        let opened = Segment::<Header>::open_existing(&name, 0xfeed).unwrap();
        assert_eq!(segment.layout.slot_count, opened.layout.slot_count);
        assert_eq!(opened.publisher_count(), 0);
    }

    #[test]
    fn open_absent_errors() {
        let err = match Segment::<Header>::open_existing(&unique_name("absent"), 0xfeed) {
            Ok(_) => panic!("absent segment should not open"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::ServiceNotFound(_)));
    }

    #[test]
    fn type_hash_mismatch_is_rejected() {
        let name = unique_name("type_mismatch");
        let _segment = Segment::<Header>::create(&name, 0xaaaa, LocalConfig::default()).unwrap();
        let err = match Segment::<Header>::open_existing(&name, 0xbbbb) {
            Ok(_) => panic!("wrong type hash should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::IncompatibleShm(_)));
    }

    #[test]
    fn publisher_registration_count_tracks_drops() {
        let name = unique_name("counts");
        let segment = Segment::<Header>::create(&name, 0xfeed, LocalConfig::default()).unwrap();
        assert_eq!(segment.publisher_count(), 0);

        let producer = segment.producer().unwrap();
        assert_eq!(segment.publisher_count(), 1);

        drop(producer);
        assert_eq!(segment.publisher_count(), 0);
    }

    #[test]
    fn payload_cap_is_enforced_before_sequence_claim() {
        let name = unique_name("payload_cap");
        let cfg = LocalConfig {
            max_payload_bytes: 4,
            ..LocalConfig::default()
        };
        let segment = Segment::<Header>::create(&name, 0xfeed, cfg).unwrap();
        let mut producer = segment.producer().unwrap();

        let err = match producer.loan(5) {
            Ok(_) => panic!("oversized loan should fail"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            Error::PayloadTooLarge {
                actual: 5,
                capacity: 4
            }
        ));
        assert_eq!(segment.control().write_seq.load(Ordering::Acquire), 0);
    }

    #[test]
    fn dropped_loan_does_not_publish_sample() {
        let name = unique_name("drop_loan");
        let segment = Segment::<Header>::create(&name, 0xfeed, LocalConfig::default()).unwrap();
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();

        {
            let mut loan = producer.loan(0).unwrap();
            loan.header_mut().value = 42;
        }

        assert!(consumer.take().unwrap().is_none());
    }

    #[test]
    fn pinned_slot_returns_no_free_slot_without_sequence_hole() {
        let name = unique_name("pinned");
        let cfg = LocalConfig {
            max_publishers: 1,
            max_subscribers: 1,
            subscriber_buffer: 1,
            history_depth: 1,
            max_payload_bytes: 0,
        };
        let segment = Segment::<Header>::create(&name, 0xfeed, cfg).unwrap();
        let mut producer = segment.producer().unwrap();
        let mut consumer = segment.consumer().unwrap();

        let mut loan = producer.loan(0).unwrap();
        loan.header_mut().value = 1;
        assert_eq!(producer.publish(loan).unwrap(), 1);

        let held = consumer.take().unwrap().expect("sample should be present");
        assert_eq!(held.header().value, 1);

        let err = match producer.loan(0) {
            Ok(_) => panic!("one-slot ring should be pinned by held sample"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::NoFreeSlot { .. }));
        assert_eq!(segment.control().write_seq.load(Ordering::Acquire), 1);

        drop(held);
        let mut loan = producer.loan(0).unwrap();
        loan.header_mut().value = 2;
        assert_eq!(producer.publish(loan).unwrap(), 2);
    }

    #[test]
    fn process_liveness_token_matches_current_process() {
        let pid = current_pid();
        let token = current_process_token();
        assert!(process_alive(pid, token));
        #[cfg(target_os = "linux")]
        if token != u64::MAX {
            assert!(
                !process_alive(pid, token + 1),
                "a mismatched start token must not be treated as the same process"
            );
        }
    }
}
