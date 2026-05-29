//! RAII handles for borrowed local shared-memory samples.
//!
//! Both [`Loan<T>`] and [`Sample<T>`] expose the header+payload split:
//! `header()` returns the small Pod metadata (`T::Header`), and
//! `payload()` returns the variable-length byte slice.

use crate::local::shm;

/// Writable handle to an in-flight SHM sample.
pub struct Loan<T: datapod::DataPod + 'static> {
    pub(crate) inner: shm::Loan<T::Header>,
}

impl<T: datapod::DataPod + 'static> Loan<T> {
    /// Read the header.
    pub fn header(&self) -> &T::Header {
        self.inner.header()
    }

    /// Mutate the header. For fixed-Pod `T`, set this to the full value.
    pub fn header_mut(&mut self) -> &mut T::Header {
        self.inner.header_mut()
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

/// Read-only handle to a received SHM sample.
pub struct Sample<T: datapod::DataPod + 'static> {
    pub(crate) inner: shm::Sample<T::Header>,
}

impl<T: datapod::DataPod + 'static> Sample<T> {
    pub fn header(&self) -> &T::Header {
        self.inner.header()
    }

    pub fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    pub fn sequence(&self) -> u64 {
        self.inner.sequence()
    }
}
