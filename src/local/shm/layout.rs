use super::*;

#[repr(C, align(64))]
pub(crate) struct ControlBlock {
    pub(crate) magic: AtomicU64,
    pub(crate) version: AtomicU32,
    /// PID of the process currently *responsible* for driving this segment to
    /// `MAGIC` (or for tearing it down if it turns out abandoned).
    ///
    ///   * `0` — not stamped yet (the creator is in the microsecond window
    ///     between sizing the object and stamping).
    ///   * original creator's PID — set once by `create` (see `creator_token`
    ///     for how its liveness is checked).
    ///   * a reclaimer's PID — a peer found the previous responsible party
    ///     dead and *took over* via a `compare_exchange` on this field, making
    ///     itself the one party that will unlink the name. If that reclaimer
    ///     then dies before unlinking, the next opener finds *its* PID dead and
    ///     takes over in turn — so cleanup is self-healing, and there is never
    ///     more than one *live* responsible party (hence never two concurrent
    ///     unlinkers of the same inode).
    ///
    /// There is deliberately no separate "condemned" sentinel: a dead
    /// responsible party IS the condemnation, and it can always be superseded.
    ///
    /// Ordering: the original creator stores `creator_token` *first* and
    /// `creator_pid` *last* with `Release`, so a reader that `Acquire`-loads a
    /// real PID always observes that process' matching token — the liveness
    /// check can never be fooled by a token from a previous incarnation.
    pub(crate) creator_pid: AtomicU32,
    /// Start-time token paired with `creator_pid` for reuse-proof liveness.
    ///
    /// `0` is a distinguished value meaning "check liveness by `kill(pid, 0)`
    /// only" (no `/proc` token). The *original creator* stores its real token
    /// (so a recycled PID can never masquerade as it over a long-lived
    /// poison). A *reclaimer* that takes over instead stores `0`: it is
    /// short-lived (it unlinks within microseconds), so PID reuse cannot
    /// realistically bite, and — crucially — every reclaimer writing the *same*
    /// value (`0`) makes concurrent take-over attempts free of any
    /// token/PID pairing race.
    pub(crate) creator_token: AtomicU64,
    pub(crate) type_hash_hi: AtomicU32,
    pub(crate) type_hash_lo: AtomicU32,
    pub(crate) header_size: AtomicU32,
    pub(crate) payload_cap: AtomicU32,
    pub(crate) slot_count: AtomicU32,
    pub(crate) slot_stride: AtomicU32,
    pub(crate) write_seq: AtomicU64,
    pub(crate) publishers: AtomicU32,
    pub(crate) subscribers: AtomicU32,
    pub(crate) max_publishers: AtomicU32,
    pub(crate) max_subscribers: AtomicU32,
    pub(crate) subscriber_buffer: AtomicU32,
    pub(crate) history_depth: AtomicU32,
    pub(crate) publisher_pids: [AtomicU32; MAX_TRACKED_PUBLISHERS],
    pub(crate) publisher_tokens: [AtomicU64; MAX_TRACKED_PUBLISHERS],
    pub(crate) subscriber_pids: [AtomicU32; MAX_TRACKED_SUBSCRIBERS],
    pub(crate) subscriber_tokens: [AtomicU64; MAX_TRACKED_SUBSCRIBERS],
}

#[repr(C, align(8))]
pub(crate) struct SlotHeader {
    pub(crate) seq: AtomicU64,
    /// Reader bitmask, or [`WRITER_STATE`] while a producer owns the slot.
    ///
    /// Each subscriber gets one bit for the life of the subscriber plus
    /// any samples derived from it. That makes dead-reader cleanup
    /// possible: if a process dies while holding a sample, the next
    /// publisher can clear only that process' bit instead of pinning the
    /// slot forever.
    pub(crate) refcount: AtomicU32,
    pub(crate) len: AtomicU32,
    pub(crate) writer_pid: AtomicU32,
    pub(crate) writer_token: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Layout {
    pub(crate) total_size: usize,
    pub(crate) slots_offset: usize,
    pub(crate) slot_stride: usize,
    pub(crate) header_offset: usize,
    pub(crate) payload_offset: usize,
    pub(crate) payload_cap: usize,
    pub(crate) slot_count: usize,
}

impl Layout {
    pub(crate) fn new<H>(cfg: &LocalConfig) -> Result<Self> {
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
        // Every slot base (`slots_offset + index * slot_stride`) must satisfy
        // both `SlotHeader`'s alignment and `H`'s alignment: the header lives
        // at `header_offset` (a multiple of `header_align`) from the base, so
        // the base must be aligned to `align_of::<H>()` for `header_ptr(index)`
        // to be well-aligned at *every* index, and to `align_of::<SlotHeader>()`
        // for the leading header. Align the stride and the first slot's offset
        // to this shared alignment (never below 8).
        let slot_align = std::mem::align_of::<SlotHeader>()
            .max(std::mem::align_of::<H>())
            .max(8);
        let slot_stride = align_up(
            payload_offset
                .checked_add(payload_cap)
                .ok_or_else(|| Error::invalid_argument("slot layout overflow"))?,
            slot_align,
        );
        let slots_offset = align_up(std::mem::size_of::<ControlBlock>(), slot_align.max(64));
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

pub(crate) fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (value + align - 1) & !(align - 1)
}

