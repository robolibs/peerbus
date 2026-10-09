use super::*;

/// A named SHM segment containing one typed ring.
pub(crate) struct Segment<H: Pod + Zeroable + Copy + 'static> {
    pub(crate) _shmem: Shmem,
    pub(crate) ptr: NonNull<u8>,
    pub(crate) layout: Layout,
    pub(crate) key: String,
    pub(crate) type_hash: u64,
    /// Identity (`dev`, `ino`) of the inode this segment actually maps, read
    /// from the mapping itself (`capture_mapped_identity`), not by re-resolving
    /// the name. Used to make the drop-time `shm_unlink` *inode-verified*: we
    /// remove the name only while it still resolves to the very inode we map,
    /// never a repurposed one. Because it is the mapped inode (not a by-name
    /// stat), a repurposed name can only fail the match, never match a
    /// different healthy inode.
    pub(crate) dev: u64,
    pub(crate) ino: u64,
    /// Whether this handle is responsible for unlinking the OS name when the
    /// last local reference drops. Only the creating handle sets this; every
    /// opener leaves the name alone (an opener that later becomes a reclaimer
    /// unlinks through the coordinated reclaim path, not on drop).
    pub(crate) owns_name: bool,
    pub(crate) _header: PhantomData<H>,
}

// SAFETY: `Segment` points at a shared mapping whose interior mutation is
// coordinated by atomics in the mapping. `H` is POD and copied as bytes.
unsafe impl<H: Pod + Zeroable + Copy + 'static> Send for Segment<H> {}
// SAFETY: Shared access to the mapping is safe under the same atomic
// protocol; mutable user access is only handed out through a claimed `Loan`.
unsafe impl<H: Pod + Zeroable + Copy + 'static> Sync for Segment<H> {}

impl<H: Pod + Zeroable + Copy + 'static> Drop for Segment<H> {
    fn drop(&mut self) {
        // Normal teardown. Because we disabled the `shared_memory` crate's
        // blind owner-unlink (`set_owner(false)`), peerbus must remove the OS
        // name itself when the creating handle's last local reference drops —
        // otherwise `/dev/shm` leaks. Only the creating handle owns the name;
        // openers leave it alone (the inode stays live for them regardless).
        //
        // Inode-verified: `unlink_if_matches` removes the name only while it
        // still resolves to the inode this handle created, so a name a peer has
        // meanwhile repurposed to a healthy segment is never destroyed. The
        // `_shmem` field's `Drop` (which unmaps + closes, but no longer
        // unlinks) runs after this, so the mapping is still valid here.
        if self.owns_name {
            unlink_if_matches(&self.key, self.dev, self.ino);
        }
    }
}

impl<H: Pod + Zeroable + Copy + 'static> Segment<H> {
    pub(crate) fn open_or_create(
        name: &str,
        type_hash: u64,
        cfg: LocalConfig,
    ) -> Result<Arc<Self>> {
        // Two callers racing on the same name must converge: exactly one wins
        // the exclusive `create` (`shm_open(O_CREAT|O_EXCL)`); the others
        // attach. This loop rescues two distinct failures.
        //
        // 1. The narrow *unsized* window: the winner has created the OS name
        //    (so a racer's `create` returns "already exists") but has not yet
        //    `ftruncate`d it, so the racer's attach fails with
        //    `ServiceNotFound` (a zero-length object cannot be `mmap`ed).
        //    Retry until the winner finishes sizing or the deadline elapses.
        //
        // 2. A *poisoned* name: the party responsible for a sized segment died
        //    before storing `MAGIC`. The object exists and is sized, so
        //    `O_EXCL` create always fails and the attach can never see `MAGIC`
        //    — the service used to be dead forever, needing a manual
        //    `rm /dev/shm/qb_*`. Now the attach proves the responsible party
        //    is dead and *takes over* the inode (installs our PID via CAS);
        //    exactly one live racer wins and becomes the sole reclaimer,
        //    unlinking the name here so the next `create` makes a fresh
        //    segment. If that reclaimer dies before unlinking, the next opener
        //    finds *its* PID dead and takes over in turn — self-healing.
        //
        // Any other error (a genuine `IncompatibleShm` mismatch, `ShmExhausted`,
        // bad config) is permanent and returned at once.
        let deadline = Instant::now() + OPEN_OR_CREATE_DEADLINE;
        let mut backoff = Duration::from_micros(50);
        let mut reclaims = 0u32;
        loop {
            match Self::create(name, type_hash, cfg.clone()) {
                Ok(segment) => return Ok(segment),
                // Permanent errors: bad config, oversized name, or the
                // host cannot back the segment. Never retry these.
                Err(err @ Error::InvalidArgument(_))
                | Err(err @ Error::TopicNameTooLong { .. })
                | Err(err @ Error::ShmExhausted(_)) => return Err(err),
                // Otherwise the segment already exists (or a peer is mid
                // creation): attach to it.
                Err(_) => match Self::attach(name, type_hash, true) {
                    Ok(Attach::Ready(segment)) => return Ok(segment),
                    // We won the take-over of a dead party's inode, so we are
                    // the only live party that may unlink it. `unlink_if_matches`
                    // removes the name only while it still resolves to
                    // `(dev, ino)` — the very inode we took over — so we can
                    // never destroy a healthy segment a peer rebuilt under the
                    // same name. Then loop: we, or a peer, will win a fresh
                    // `create`.
                    Ok(Attach::Reclaim { dev, ino }) => {
                        reclaims += 1;
                        if reclaims > MAX_RECLAIM_ATTEMPTS {
                            return Err(Error::incompatible_shm(format!(
                                "service {name}: gave up after reclaiming {} poisoned segments",
                                reclaims - 1
                            )));
                        }
                        unlink_if_matches(&os_key(name), dev, ino);
                        backoff = Duration::from_micros(50);
                    }
                    // Someone else is reclaiming this segment (or already
                    // condemned it). Our mapping is of a doomed inode that
                    // will never be initialised — abandon it and retry by
                    // name, which will resolve to the fresh incarnation.
                    Ok(Attach::Retry) if Instant::now() < deadline => {
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(Duration::from_millis(2));
                    }
                    Ok(Attach::Retry) => {
                        return Err(Error::incompatible_shm(format!(
                            "service {name}: timed out waiting for a peer to reclaim it"
                        )));
                    }
                    Err(Error::ServiceNotFound(_)) if Instant::now() < deadline => {
                        // Sleep with a small capped backoff rather than a
                        // hot `yield_now()` spin: a stuck peer would
                        // otherwise churn shm_open/mmap/munmap for the full
                        // 2 s. Capped so convergence stays sub-millisecond
                        // once the winner finishes sizing.
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(Duration::from_millis(2));
                    }
                    Err(err) => return Err(err),
                },
            }
        }
    }

    pub(crate) fn create(name: &str, type_hash: u64, cfg: LocalConfig) -> Result<Arc<Self>> {
        validate_name(name)?;
        let key = os_key(name);
        let layout = Layout::new::<H>(&cfg)?;
        let mut shmem = ShmemConf::new()
            .os_id(&key)
            .size(layout.total_size)
            .create()
            .map_err(|e| Error::Other(format!("shared_memory create {key}: {e}")))?;

        // Take over the OS name's lifetime from the `shared_memory` crate.
        //
        // The crate's owner-drop calls `shm_unlink(name)` *unconditionally* on
        // the NAME, ignoring which inode that name currently resolves to. If a
        // live-but-slow creator were ever condemned (see below) and its name
        // rebuilt onto a fresh inode by a peer, that blind unlink would destroy
        // the peer's healthy segment — split brain. So peerbus becomes the sole
        // unlink authority and only ever unlinks *inode-verified* (see
        // `unlink_if_matches` / `Drop for Segment`).
        shmem.set_owner(false);

        let ptr = NonNull::new(shmem.as_ptr()).ok_or_else(|| {
            Error::Other(format!("shared_memory create {key}: returned null mapping"))
        })?;

        // Record the identity of the inode we just mapped, read from the
        // mapping itself (not a by-name lookup), so the identity we later
        // unlink against is unconditionally the mapped-and-arbitrated inode. If
        // capture fails (only plausibly under fd exhaustion / no `/proc`), we
        // must not risk a blind unlink of a future occupant of this name, so we
        // mark the handle non-owning: the segment then relies on the reclaim
        // path for eventual cleanup.
        let (dev, ino, owns_name) = match capture_mapped_identity(ptr, &key) {
            Some((dev, ino)) => (dev, ino, true),
            None => {
                crate::qb_warn!(
                    target: "peerbus::local::shm",
                    key = %key,
                    "could not capture freshly created shm inode identity; \
                     leaving it unowned so we never blind-unlink a repurposed name"
                );
                (0, 0, false)
            }
        };

        // Reserve backing store *before* touching any page. `shared_memory`'s
        // create does `shm_open` + `ftruncate` + `mmap`; on tmpfs (`/dev/shm`)
        // `ftruncate` only sets the file *size* and leaves it sparse, so pages
        // are allocated lazily on first write. If the tmpfs (or the process'
        // tmpfs quota) is exhausted, that first write faults as SIGBUS —
        // process-fatal and uncatchable. `fallocate` commits the blocks up
        // front and reports exhaustion as an ordinary `ENOSPC`/`EDQUOT` error
        // instead. On failure `shmem` drops here (owner=false, so only unmap).
        //
        // Two steps, so the creator stamp below lands on backed memory as early
        // as possible: first the control page (cheap), then the whole segment.
        reserve_shm_backing(&key, std::mem::size_of::<ControlBlock>())?;

        // SAFETY: the control page is now backed, and the object is a brand
        // new inode (`O_CREAT|O_EXCL`) so it reads as zero — but zero the
        // control block explicitly before stamping so the identity we publish
        // sits on known-clean storage.
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr(), 0, std::mem::size_of::<ControlBlock>());
        }

        // Stamp our identity NOW — the very first thing after sizing — so an
        // opener racing this create can tell "creator still working" from
        // "creator died mid-init". Without it, a creator that dies after
        // `ftruncate` but before `MAGIC` poisons the name forever.
        //
        // Ordering: token first (Relaxed), pid last (Release). A reader that
        // Acquire-loads a real pid therefore always observes the matching
        // token, so its liveness check can never be fooled by a stale one.
        //
        // Sticky CAS `0 -> pid`: it must only stamp an *unstamped* control
        // block. If a peer already took responsibility for this inode (found us
        // unstamped past the grace and installed its own reclaimer PID — only
        // reachable if we somehow stalled in the microsecond pre-stamp window),
        // `creator_pid` is non-zero and the CAS fails. We then ABANDON: return
        // without storing `MAGIC` and without unlinking (the reclaimer owns
        // cleanup of this inode). This is what makes a wrongly-reclaimed live
        // creator safe — it never fights the reclaimer over the name.
        //
        // Test-only hook: hold the segment in the unstamped window (pid still
        // 0) so a peer can attempt to (wrongly) reclaim a *live* creator. Zero
        // cost and entirely absent in non-test builds.
        #[cfg(test)]
        tests::stall_before_stamp_hook();

        let control = unsafe_control(ptr);
        control
            .creator_token
            .store(current_process_token(), Ordering::Relaxed);
        if control
            .creator_pid
            .compare_exchange(0, current_pid(), Ordering::Release, Ordering::Acquire)
            .is_err()
        {
            // `shmem` drops here with owner=false → unmap only, no unlink.
            return Err(Error::Other(format!(
                "shm segment {key}: superseded by a reclaimer during initialisation; retrying"
            )));
        }

        // Now the long part: back and zero the rest of the segment. An opener
        // that shows up during this sees `magic == 0` but a *live* creator
        // stamp, and correctly waits instead of reclaiming.
        reserve_shm_backing(&key, layout.total_size)?;

        // SAFETY: `ptr` is a valid writable mapping of `layout.total_size`
        // bytes, now fully backed. Zero only the slot region: `slots_offset`
        // is always >= `size_of::<ControlBlock>()`, so this cannot clobber the
        // creator stamp we just published. The control block was zeroed above
        // and every scalar in it is written by `init_control`; the pid/token
        // arrays stay zero, as required.
        unsafe {
            std::ptr::write_bytes(
                ptr.as_ptr().add(layout.slots_offset),
                0,
                layout.total_size - layout.slots_offset,
            );
        }

        let segment = Arc::new(Self {
            _shmem: shmem,
            ptr,
            layout,
            key,
            type_hash,
            dev,
            ino,
            owns_name,
            _header: PhantomData,
        });
        segment.init_control(&cfg)?;
        Ok(segment)
    }

    pub(crate) fn open_existing(name: &str, type_hash: u64) -> Result<Arc<Self>> {
        // A bare attach has no `LocalConfig` and therefore cannot re-create a
        // reclaimed segment, so it must not condemn one either: pass
        // `reclaim = false` and report a poisoned segment as the same
        // `IncompatibleShm` error callers already handle. Only
        // `open_or_create`, which *can* rebuild the segment, drives reclaim.
        match Self::attach(name, type_hash, false)? {
            Attach::Ready(segment) => Ok(segment),
            // Unreachable with `reclaim = false` (`attach` returns
            // `IncompatibleShm` instead), but stay total rather than panic.
            Attach::Reclaim { .. } | Attach::Retry => Err(Error::incompatible_shm(format!(
                "service {name}: creator died before finishing initialisation"
            ))),
        }
    }

    /// Map an existing segment and decide what state it is in.
    ///
    /// `reclaim` enables condemnation of a segment whose creator died
    /// mid-initialisation; see [`Attach`] and `wait_until_initialised`.
    pub(crate) fn attach(name: &str, type_hash: u64, reclaim: bool) -> Result<Attach<H>> {
        validate_name(name)?;
        let key = os_key(name);
        let mut shmem = ShmemConf::new()
            .os_id(&key)
            .open()
            .map_err(|e| Error::ServiceNotFound(format!("{name} ({key}): {e}")))?;
        // Openers are never owners in the crate's model, but be explicit: no
        // handle other than the coordinated create/reclaim path unlinks.
        shmem.set_owner(false);
        let ptr = NonNull::new(shmem.as_ptr())
            .ok_or_else(|| Error::Other(format!("shared_memory open {key}: null mapping")))?;

        // Identity of the inode we ACTUALLY mapped, read from the mapping
        // itself (not a by-name lookup). This is exactly the inode arbitration
        // runs on below, so the identity we later unlink against is
        // unconditionally the mapped-and-arbitrated inode: a name repurposed in
        // the gap between `ShmemConf::open` and here can only make
        // `unlink_if_matches` FAIL (safe no-op), never match a *different*
        // healthy inode. See `capture_mapped_identity` / `unlink_if_matches`.
        //
        // If capture failed (e.g. fd exhaustion → `(0, 0)`), we have no
        // identity to verify an unlink against, so we must NOT take over /
        // reclaim: doing so would install a responsibility we cannot discharge
        // (condemn-without-unlink → permanent poison). Pass `have_identity =
        // false` so the segment is left intact for a later, better-resourced
        // attempt to reclaim it.
        let (dev, ino) = capture_mapped_identity(ptr, &key).unwrap_or((0, 0));
        let have_identity = dev != 0 || ino != 0;

        let control = unsafe_control(ptr);
        match wait_until_initialised(ptr, name, reclaim, have_identity)? {
            InitState::Ready => {}
            // We took over responsibility for this dead inode; hand the
            // decision (with the identity to verify) up. `shmem` (owner=false)
            // drops on return, unmapping only — the caller performs the
            // inode-verified unlink.
            InitState::Reclaim => return Ok(Attach::Reclaim { dev, ino }),
            InitState::Retry => return Ok(Attach::Retry),
        }
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

        Ok(Attach::Ready(Arc::new(Self {
            _shmem: shmem,
            ptr,
            layout,
            key,
            type_hash,
            dev,
            ino,
            // Openers never unlink the name on drop; only the creating handle
            // does. A reclaimer unlinks through `open_or_create`, not here.
            owns_name: false,
            _header: PhantomData,
        })))
    }

    pub(crate) fn init_control(&self, cfg: &LocalConfig) -> Result<()> {
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

    pub(crate) fn register_publisher(&self) -> Result<usize> {
        self.reap_dead_publishers();
        let control = self.control();
        let max = control.max_publishers.load(Ordering::Acquire) as usize;
        let pid = current_pid();
        let token = current_process_token();
        // Clamp to the fixed array bound: a peer that corrupts the shared
        // `max_publishers` above the tracked cap must not drive an OOB index
        // (or an oversized `1u32 << index` elsewhere). The reap paths clamp
        // identically. The returned index is therefore always < the cap.
        for index in 0..max.min(MAX_TRACKED_PUBLISHERS) {
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

    pub(crate) fn deregister_publisher(&self, index: usize, pid: u32, token: u64) {
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

    pub(crate) fn reap_dead_publishers(&self) {
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

    pub(crate) fn register_subscriber(&self) -> Result<usize> {
        self.reap_dead_subscribers();
        let control = self.control();
        let max = control.max_subscribers.load(Ordering::Acquire) as usize;
        let pid = current_pid();
        let token = current_process_token();
        // Clamp to the fixed array bound so a corrupted `max_subscribers`
        // cannot force an OOB index or an oversized `1u32 << index` shift in
        // the reader-bit paths. The returned index is therefore always < the
        // cap, keeping `1u32 << slot` well-defined.
        for index in 0..max.min(MAX_TRACKED_SUBSCRIBERS) {
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

    pub(crate) fn deregister_subscriber(&self, index: usize, pid: u32, token: u64) {
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

    pub(crate) fn reap_dead_subscribers(&self) {
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

    pub(crate) fn reap_dead_slot_holders(&self, slot: &SlotHeader) {
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

    pub(crate) fn control(&self) -> &ControlBlock {
        unsafe_control(self.ptr)
    }

    pub(crate) fn slot_header(&self, index: usize) -> &SlotHeader {
        debug_assert!(index < self.layout.slot_count);
        let offset = self.layout.slots_offset + index * self.layout.slot_stride;
        // SAFETY: layout construction keeps every slot within the mapping
        // and aligned to at least 8 bytes for `SlotHeader`.
        unsafe { &*(self.ptr.as_ptr().add(offset).cast::<SlotHeader>()) }
    }

    pub(crate) fn header_ptr(&self, index: usize) -> *mut H {
        let offset =
            self.layout.slots_offset + index * self.layout.slot_stride + self.layout.header_offset;
        // SAFETY: caller uses the pointer according to the slot protocol.
        unsafe { self.ptr.as_ptr().add(offset).cast::<H>() }
    }

    pub(crate) fn payload_ptr(&self, index: usize) -> *mut u8 {
        let offset =
            self.layout.slots_offset + index * self.layout.slot_stride + self.layout.payload_offset;
        // SAFETY: caller bounds slices by `payload_cap`/recorded `len`.
        unsafe { self.ptr.as_ptr().add(offset) }
    }

    pub(crate) fn slot_index(&self, seq: u64) -> usize {
        ((seq - 1) as usize) % self.layout.slot_count
    }
}
