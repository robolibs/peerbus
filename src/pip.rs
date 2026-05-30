//! Cross-cutting types for pip.
//!
//! `pip` is the bidirectional finite/session primitive:
//! both sides may send zero or more typed messages and may
//! independently mark their outgoing direction done.

use bytemuck::{Pod, Zeroable};
use datapod::ZeroCopySend;

pub(crate) const PIP_KIND_ITEM: u8 = 1;
pub(crate) const PIP_KIND_DONE: u8 = 2;

/// Envelope used by local SHM pip rings.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PipEnvelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    pub session_id: u64,
    pub kind: u8,
    pub reserved: [u8; 7],
    pub header: H,
}

// SAFETY: `PipEnvelope<H>` is `#[repr(C)]` and contains only fixed POD
// fields plus the POD header `H`.
unsafe impl<H> Pod for PipEnvelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> Zeroable for PipEnvelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> ZeroCopySend for PipEnvelope<H> where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static
{
}

impl<H> Default for PipEnvelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    fn default() -> Self {
        Self {
            session_id: 0,
            kind: PIP_KIND_DONE,
            reserved: [0; 7],
            header: H::zeroed(),
        }
    }
}
