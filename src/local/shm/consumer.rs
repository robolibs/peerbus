use super::*;


pub(crate) struct Consumer<H: Pod + Zeroable + Copy + 'static> {
    pub(crate) lease: Arc<ConsumerLease<H>>,
    pub(crate) read_cursor: u64,
}

pub(crate) struct ConsumerLease<H: Pod + Zeroable + Copy + 'static> {
    pub(crate) segment: Arc<Segment<H>>,
    pub(crate) slot: usize,
    pub(crate) pid: u32,
    pub(crate) token: u64,
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
            // `next`'s slot is empty. Two cases:
            //   (a) a writer is still legitimately filling `next` (leave it —
            //       report `None` and let the caller retry), or
            //   (b) a permanent hole: the loan for `next` was dropped before
            //       commit, or the producer died between claiming `write_seq`
            //       and committing. `write_seq` is advanced at claim time and
            //       never rolled back, so an abandoned slot keeps seq==0 while
            //       `write_seq` moves on — in-order `take()` would otherwise
            //       stall here forever.
            // Only skip the hole when a strictly newer sequence is already
            // visible (`latest > next`, i.e. the producers moved past it) AND
            // no *live* writer owns the slot. A slot is owned by a live writer
            // when its refcount is `WRITER_STATE` and the recorded writer
            // process is still alive; in that case we must wait, never skip.
            // These loads are sequenced after the `latest`/`write_seq` load, so
            // if `latest` already reflects the claim of `next`, refcount is
            // visible as `WRITER_STATE` (or as committed-0, handled below).
            let writer_live = slot.refcount.load(Ordering::Acquire) == WRITER_STATE
                && process_alive(
                    slot.writer_pid.load(Ordering::Acquire),
                    slot.writer_token.load(Ordering::Acquire),
                );
            // Re-read `seq` after inspecting the writer: `commit()` stores
            // `seq` (Release) before clearing `refcount` (Release), so a slot
            // that just committed to `next` is guaranteed visible here and is
            // not treated as a hole (avoids skipping a fresh sample).
            if latest > next && !writer_live && slot.seq.load(Ordering::Acquire) == 0 {
                self.read_cursor = next;
                return Err(Error::Lagged { dropped: 1 });
            }
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
            // Advance past the poisoned slot before surfacing the error;
            // otherwise every subsequent poll re-hits this same corrupt slot
            // and the consumer can never make progress.
            self.read_cursor = next;
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
    pub(crate) lease: Arc<ConsumerLease<H>>,
    pub(crate) index: usize,
    pub(crate) seq: u64,
    pub(crate) len: usize,
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
