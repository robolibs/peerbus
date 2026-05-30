//! Cross-cutting types for que/ans.
//!
//! `que/ans` is the finite-stream query primitive:
//! one `que` from the client, zero or more `ans` items from the
//! server, then an explicit done marker.

use bytemuck::{Pod, Zeroable};
use datapod::ZeroCopySend;

pub(crate) const ANS_KIND_ITEM: u8 = 1;
pub(crate) const ANS_KIND_DONE: u8 = 2;

/// Envelope used by the local SHM ans ring.
///
/// The `H` parameter is `T::Header` for the answer datatype.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct AnsEnvelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    pub req_id: u64,
    pub kind: u8,
    pub reserved: [u8; 7],
    pub header: H,
}

// SAFETY: `AnsEnvelope<H>` is `#[repr(C)]` and contains only fixed POD
// fields plus the POD header `H`. The explicit `reserved` bytes remove
// internal padding between `kind` and `header` for the common header
// alignments used by datapod.
unsafe impl<H> Pod for AnsEnvelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> Zeroable for AnsEnvelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> ZeroCopySend for AnsEnvelope<H> where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static
{
}

impl<H> Default for AnsEnvelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    fn default() -> Self {
        Self {
            req_id: 0,
            kind: ANS_KIND_DONE,
            reserved: [0; 7],
            header: H::zeroed(),
        }
    }
}
