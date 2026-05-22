//! RAII handles for borrowed iceoryx2 samples.
//!
//! Both [`Loan<T>`] and [`Sample<T>`] expose the header+payload split:
//! `header()` returns the small Pod metadata (a `T::Header`), and
//! `payload()` returns the variable-length byte slice. For fixed-Pod
//! types, the payload slice has length 0 and all data is in the
//! header. For heap-bearing types, the payload bytes are the
//! `bytemuck::cast_slice` view of the type's internal `Vec<...>`.

use iceoryx2::prelude::*;
use iceoryx2::sample::Sample as IoxSample;
use iceoryx2::sample_mut::SampleMut as IoxSampleMut;

use crate::local::slot::Slot;

/// Writable handle to an in-flight iceoryx2 sample.
pub struct Loan<T: datapod::DataPod + 'static> {
    pub(crate) inner: IoxSampleMut<ipc_threadsafe::Service, [u8], Slot<T::Header>>,
}

impl<T: datapod::DataPod + 'static> Loan<T> {
    /// Read the header.
    pub fn header(&self) -> &T::Header {
        &self.inner.user_header().0
    }

    /// Mutate the header. For fixed-Pod `T`, set this to the full value.
    pub fn header_mut(&mut self) -> &mut T::Header {
        &mut self.inner.user_header_mut().0
    }

    /// Read the variable-length payload bytes.
    pub fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    /// Mutate the variable-length payload bytes. For heap-bearing
    /// `T`, copy the cast bytes of the inner `Vec<...>` here.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.inner.payload_mut()
    }
}

/// Read-only handle to a received iceoryx2 sample.
pub struct Sample<T: datapod::DataPod + 'static> {
    pub(crate) inner: IoxSample<ipc_threadsafe::Service, [u8], Slot<T::Header>>,
}

impl<T: datapod::DataPod + 'static> Sample<T> {
    pub fn header(&self) -> &T::Header {
        &self.inner.user_header().0
    }

    pub fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    /// Placeholder for per-publish sequence number — iceoryx2 v0.7
    /// doesn't surface this on the sample's header.
    pub fn sequence(&self) -> u64 {
        0
    }
}
