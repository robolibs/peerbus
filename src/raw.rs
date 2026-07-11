//! Byte-oriented payload for the foreign-language bindings.
//!
//! peerbus's native API is generic over `datapod::DataPod`, which can't
//! cross a C ABI or the Python boundary. The C and Python bindings
//! therefore speak in opaque byte buffers carried by [`RawMsg`]: a
//! `u64 kind` tag rides the fixed header, and the payload bytes ride the
//! variable-length `#[dp(bytes)]` field — exactly the
//! `header + bytes` split the transports already use.
//!
//! Both bindings use `RawMsg` for every mode, so a C publisher and a
//! Python subscriber (or two C nodes) share one wire type and
//! interoperate over SHM or iroh. Talking to a *native* Rust `T` would
//! require matching `T`'s type hash and is out of scope for the
//! bindings.

/// Opaque byte message used by the C ABI and Python bindings.
#[datapod::datapod]
pub struct RawMsg {
    /// User-defined type tag, echoed through unchanged.
    pub kind: u64,
    /// Opaque payload bytes.
    #[dp(bytes)]
    pub data: Vec<u8>,
}

impl RawMsg {
    /// Build a `RawMsg` from a kind tag and a byte slice.
    pub fn new(kind: u64, data: &[u8]) -> Self {
        Self {
            kind,
            data: data.to_vec(),
        }
    }
}
