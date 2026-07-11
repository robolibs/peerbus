//! Generic datapod wire message.
//!
//! This is the language-independent bridge: Python/C/Rust datapod bindings
//! produce `(TYPE_HASH, wire_bytes)`, and peerbus transports that as one typed
//! message without needing a separate peerbus API for every datapod type.

use crate::node::{
    AckSample, AnsSample, NodeSample, PipSample, Publisher, PutSample, QueSample, ReqSample,
    ResSample, Subscriber,
};

/// Borrowed datapod sample delivered by peerbus.
pub type DatapodSample = NodeSample<DatapodMsg>;

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

    pub fn type_hash(&self) -> u64 {
        self.type_hash
    }

    pub fn wire(&self) -> &[u8] {
        &self.wire
    }

    pub fn dynamic(&self) -> Result<datapod::dynamic::DynamicView<'_>, datapod::WireError> {
        datapod::dynamic::view_message(self.type_hash, &self.wire)
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

impl NodeSample<DatapodMsg> {
    /// Datapod type hash for the original value carried inside this message.
    ///
    /// This is not peerbus's transport type (`DatapodMsg`); it is the inner
    /// datapod type such as `datapod.Grid`.
    pub fn type_hash(&self) -> u64 {
        self.header().type_hash
    }

    /// Borrow the inner datapod wire bytes (`header || payload`) without
    /// copying. For same-host SHM, the returned slice stays valid while this
    /// sample pins the SHM slot.
    pub fn wire(&self) -> &[u8] {
        self.payload()
    }

    /// Build a zero-copy dynamic datapod view over the borrowed wire bytes.
    pub fn dynamic(&self) -> Result<datapod::dynamic::DynamicView<'_>, datapod::WireError> {
        datapod::dynamic::view_message(self.type_hash(), self.wire())
    }
}

impl Publisher<DatapodMsg> {
    /// Publish a language-neutral datapod wire message.
    pub fn send_datapod_wire(
        &mut self,
        type_hash: u64,
        wire: impl Into<Vec<u8>>,
    ) -> crate::Result<u64> {
        self.send(&DatapodMsg::new(type_hash, wire))
    }
}

impl Subscriber<DatapodMsg> {
    /// Poll for a generic datapod sample.
    ///
    /// This is a named alias for `take()` used by generic language-neutral
    /// datapod flows. The returned sample exposes `type_hash()`, `wire()`, and
    /// `dynamic()`.
    pub fn take_datapod_view(&mut self) -> crate::Result<Option<DatapodSample>> {
        self.take()
    }
}

macro_rules! impl_datapod_item_sample {
    ($sample:ident) => {
        impl $sample<DatapodMsg> {
            /// Datapod type hash for the original value carried inside this
            /// item, not peerbus's transport-envelope type.
            pub fn type_hash(&self) -> u64 {
                self.header().type_hash
            }

            /// Borrow the inner datapod wire bytes (`header || payload`).
            pub fn wire(&self) -> &[u8] {
                self.payload()
            }

            /// Build a zero-copy dynamic datapod view over the borrowed wire.
            pub fn dynamic(&self) -> Result<datapod::dynamic::DynamicView<'_>, datapod::WireError> {
                datapod::dynamic::view_message(self.type_hash(), self.wire())
            }
        }
    };
}

impl_datapod_item_sample!(ReqSample);
impl_datapod_item_sample!(ResSample);
impl_datapod_item_sample!(QueSample);
impl_datapod_item_sample!(AnsSample);
impl_datapod_item_sample!(PutSample);
impl_datapod_item_sample!(AckSample);
impl_datapod_item_sample!(PipSample);
