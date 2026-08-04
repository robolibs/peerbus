//! RAII handles for borrowed local shared-memory samples.
//!
//! Both [`Loan<T>`] and [`Sample<T>`] expose the header+payload split:
//! `header()` returns the small Pod metadata (`T::Header`), and
//! `payload()` returns the variable-length byte slice.

use crate::local::shm;
use bytemuck::Zeroable;

/// Writable handle to an in-flight sample.
///
/// Most loans point directly into a shared-memory slot. A [`Node`](crate::Node)
/// built with [`NodeBuilder::skip_shm`](crate::NodeBuilder::skip_shm) instead
/// uses an owned buffer so the same loan/publish API remains available while
/// all traffic is forced through iroh.
pub struct Loan<T: datapod::DataPod + 'static> {
    pub(crate) inner: LoanInner<T::Header>,
}

pub(crate) enum LoanInner<H: bytemuck::Pod + bytemuck::Zeroable + Copy + 'static> {
    Shm(shm::Loan<H>),
    Owned { header: H, payload: Vec<u8> },
}

impl<T: datapod::DataPod + 'static> Loan<T> {
    pub(crate) fn owned(byte_count: usize) -> Self {
        Self {
            inner: LoanInner::Owned {
                header: T::Header::zeroed(),
                payload: vec![0; byte_count],
            },
        }
    }

    /// Read the header.
    pub fn header(&self) -> &T::Header {
        match &self.inner {
            LoanInner::Shm(inner) => inner.header(),
            LoanInner::Owned { header, .. } => header,
        }
    }

    /// Mutate the header. For fixed-Pod `T`, set this to the full value.
    pub fn header_mut(&mut self) -> &mut T::Header {
        match &mut self.inner {
            LoanInner::Shm(inner) => inner.header_mut(),
            LoanInner::Owned { header, .. } => header,
        }
    }

    /// Read the variable-length payload bytes.
    pub fn payload(&self) -> &[u8] {
        match &self.inner {
            LoanInner::Shm(inner) => inner.payload(),
            LoanInner::Owned { payload, .. } => payload,
        }
    }

    /// Mutate the variable-length payload bytes. For heap-bearing
    /// `T`, copy the cast bytes of the inner `Vec<...>` here.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        match &mut self.inner {
            LoanInner::Shm(inner) => inner.payload_mut(),
            LoanInner::Owned { payload, .. } => payload,
        }
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
