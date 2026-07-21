use super::*;


pub(crate) struct Producer<H: Pod + Zeroable + Copy + 'static> {
    pub(crate) lease: Arc<ProducerLease<H>>,
}

pub(crate) struct ProducerLease<H: Pod + Zeroable + Copy + 'static> {
    pub(crate) segment: Arc<Segment<H>>,
    pub(crate) slot: usize,
    pub(crate) pid: u32,
    pub(crate) token: u64,
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
    pub(crate) segment: Arc<Segment<H>>,
    pub(crate) index: usize,
    pub(crate) seq: u64,
    pub(crate) len: usize,
    pub(crate) published: bool,
    pub(crate) _header: PhantomData<H>,
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

    pub(crate) fn commit(&mut self) -> Result<u64> {
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
