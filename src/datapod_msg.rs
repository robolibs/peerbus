//! Generic datapod wire message.
//!
//! This is the language-independent bridge: Python/C/Rust datapod bindings
//! produce `(TYPE_HASH, wire_bytes)`, and quicbit transports that as one typed
//! message without needing a separate quicbit API for every datapod type.

/// A type-erased datapod value.
///
/// `type_hash` is the datapod binding type hash for the original type
/// (`datapod::bind::type_hash::<T>()` / `T.TYPE_HASH` in Python).
/// `wire` is the datapod wire bytes: fixed header followed by heap payload.
#[datapod::datapod]
pub struct DatapodMsg {
    pub type_hash: u64,
    #[dp(bytes)]
    pub wire: Vec<u8>,
}

impl DatapodMsg {
    pub fn new(type_hash: u64, wire: impl Into<Vec<u8>>) -> Self {
        Self {
            type_hash,
            wire: wire.into(),
        }
    }

    pub fn from_datapod<T>(value: &T) -> Self
    where
        T: datapod::DataPod,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let message = datapod::to_wire_message(value);
        Self::new(message.type_hash, message.bytes)
    }

    pub fn to_datapod<T>(&self) -> Result<T, datapod::WireError>
    where
        T: datapod::DataPodDecode + datapod::DataPodValidate,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        datapod::from_wire_message(&datapod::WireMessage {
            type_hash: self.type_hash,
            bytes: self.wire.clone(),
        })
    }
}
