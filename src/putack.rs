//! Cross-cutting types for put/ack.
//!
//! `put/ack` is the finite upload/sink primitive:
//! zero or more `put` items from the client, then one final `ack`
//! from the server.

use bytemuck::{Pod, Zeroable};
use datapod::ZeroCopySend;

pub(crate) const PUT_KIND_ITEM: u8 = 1;
pub(crate) const PUT_KIND_DONE: u8 = 2;

/// Envelope used by local SHM put rings.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PutEnvelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    pub req_id: u64,
    pub kind: u8,
    pub reserved: [u8; 7],
    pub header: H,
}

// SAFETY: `PutEnvelope<H>` is `#[repr(C)]` and contains only fixed POD
// fields plus the POD header `H`.
unsafe impl<H> Pod for PutEnvelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> Zeroable for PutEnvelope<H> where H: Pod + Zeroable + ZeroCopySend + Copy + 'static {}
unsafe impl<H> ZeroCopySend for PutEnvelope<H> where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static
{
}

impl<H> Default for PutEnvelope<H>
where
    H: Pod + Zeroable + ZeroCopySend + Copy + 'static,
{
    fn default() -> Self {
        Self {
            req_id: 0,
            kind: PUT_KIND_DONE,
            reserved: [0; 7],
            header: H::zeroed(),
        }
    }
}
