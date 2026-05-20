//! Python bindings via pyo3 (abi3-py311).
//!
//! Single `#[pymodule]` exposing `Service`, `Publisher`,
//! `Subscriber`, and `Sample`, plus error-to-exception mapping.
//! Like the C FFI, the surface is byte-oriented: payloads are
//! `slot_size` bytes; the caller is responsible for
//! `struct.pack` / `struct.unpack` framing on top.
//!
//! `Subscriber.take()` returns a [`Sample`] that implements
//! Python's buffer protocol — `memoryview(sample)` is a zero-copy
//! view directly into the SHM slot. The slot is held until the
//! `Sample` is dropped (refcount-based, exactly like the Rust API).

use std::os::raw::c_int;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;

use crate::error::Error;
use crate::local::layout::fnv1a64;
use crate::local::segment::{Segment, SegmentParams};

fn py_err(e: Error) -> PyErr {
    match e {
        Error::InvalidArgument(_) | Error::PayloadTooLarge { .. } => {
            PyValueError::new_err(e.to_string())
        }
        _ => PyRuntimeError::new_err(e.to_string()),
    }
}

const PY_TYPE_NAME: &str = "<quicbit python>";

/// Python-side `Service` handle. Wraps a `Segment`.
#[pyclass]
struct Service {
    segment: Segment,
}

#[pymethods]
impl Service {
    /// `Service.create(name, slot_count, slot_size, history_depth=1)`
    /// — create a brand-new local SHM service.
    #[staticmethod]
    #[pyo3(signature = (name, slot_count, slot_size, history_depth=1))]
    fn create(
        name: &str,
        slot_count: u32,
        slot_size: u32,
        history_depth: u32,
    ) -> PyResult<Self> {
        let params = SegmentParams {
            slot_count,
            slot_size,
            history_depth,
            type_name: PY_TYPE_NAME,
        };
        Segment::create(name, params)
            .map(|segment| Self { segment })
            .map_err(py_err)
    }

    /// `Service.attach(name)` — attach to an existing service by name.
    #[staticmethod]
    fn attach(name: &str) -> PyResult<Self> {
        Segment::attach(name, fnv1a64(PY_TYPE_NAME))
            .map(|segment| Self { segment })
            .map_err(py_err)
    }

    /// `Service.open_or_create(name, slot_count, slot_size, history_depth=1)`
    /// — try create, fall back to attach.
    #[staticmethod]
    #[pyo3(signature = (name, slot_count, slot_size, history_depth=1))]
    fn open_or_create(
        name: &str,
        slot_count: u32,
        slot_size: u32,
        history_depth: u32,
    ) -> PyResult<Self> {
        match Self::create(name, slot_count, slot_size, history_depth) {
            Ok(s) => Ok(s),
            Err(_) => Self::attach(name),
        }
    }

    fn publisher(&self) -> Publisher {
        Publisher {
            segment: self.segment.clone(),
        }
    }

    fn subscriber(&self) -> Subscriber {
        Subscriber {
            segment: self.segment.clone(),
            next_seq: self.segment.latest_seq() + 1,
        }
    }

    #[getter]
    fn name(&self) -> String {
        self.segment.name().to_string()
    }

    #[getter]
    fn slot_size(&self) -> u32 {
        self.segment.slot_size()
    }

    #[getter]
    fn slot_count(&self) -> u32 {
        self.segment.slot_count()
    }
}

/// Python-side `Publisher`.
#[pyclass]
struct Publisher {
    segment: Segment,
}

#[pymethods]
impl Publisher {
    /// Publish `data: bytes`. Length must equal the service's
    /// `slot_size`; raises `ValueError` otherwise. Returns the
    /// assigned publish sequence.
    fn publish(&mut self, data: &[u8]) -> PyResult<u64> {
        let expected = self.segment.slot_size() as usize;
        if data.len() != expected {
            return Err(PyValueError::new_err(format!(
                "payload length {} != slot_size {}",
                data.len(),
                expected
            )));
        }
        let slot_idx = self.segment.pop_free().ok_or_else(|| {
            py_err(Error::NoFreeSlot {
                service: self.segment.name().to_string(),
            })
        })?;
        // SAFETY: slot_payload(idx) yields a writable region of
        // exactly `slot_size` bytes; we just verified `data.len()
        // == slot_size`.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.segment.slot_payload(slot_idx),
                expected,
            );
        }
        let seq = self.segment.publish_slot(slot_idx);
        Ok(seq)
    }
}

/// Python-side `Subscriber`.
#[pyclass]
struct Subscriber {
    segment: Segment,
    next_seq: u64,
}

#[pymethods]
impl Subscriber {
    /// Non-blocking take. Returns a [`Sample`] that exposes the SHM
    /// bytes via Python's buffer protocol — `memoryview(sample)` is
    /// a zero-copy view backed by the slot. Returns `None` when no
    /// new sample is available; raises on lag.
    fn take(&mut self) -> PyResult<Option<Sample>> {
        let wanted = self.next_seq;
        match self.segment.try_acquire(wanted) {
            Ok(Some((slot_idx, seq))) => {
                self.next_seq = seq + 1;
                Ok(Some(Sample {
                    segment: self.segment.clone(),
                    slot_idx,
                    seq,
                    len: self.segment.slot_size() as usize,
                }))
            }
            Ok(None) => Ok(None),
            Err(Error::Lagged { dropped }) => {
                let latest = self.segment.latest_seq();
                let depth = self.segment.history_depth() as u64;
                self.next_seq = if latest > depth { latest - depth + 1 } else { 1 };
                Err(py_err(Error::Lagged { dropped }))
            }
            Err(e) => Err(py_err(e)),
        }
    }
}

/// A zero-copy view of a published SHM slot.
///
/// The slot stays loaned to this `Sample` (refcount += 1) until the
/// object is dropped. `memoryview(sample)`, `bytes(sample)`, and
/// any buffer-protocol consumer (`numpy.frombuffer`, `struct.unpack_from`,
/// …) all read the SHM bytes directly without copying.
#[pyclass]
struct Sample {
    segment: Segment,
    slot_idx: u32,
    seq: u64,
    len: usize,
}

#[pymethods]
impl Sample {
    /// Publish sequence this sample was taken from.
    #[getter]
    fn sequence(&self) -> u64 {
        self.seq
    }

    /// Length of the underlying slot in bytes.
    #[getter]
    fn nbytes(&self) -> usize {
        self.len
    }

    /// Eagerly copy out to a Python `bytes`. Equivalent to
    /// `bytes(memoryview(sample))` but in one call. Useful when the
    /// caller wants to keep the data past the `Sample`'s lifetime
    /// without holding the slot.
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        // SAFETY: refcount >= 1 (we hold one) so the slot is alive.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self.segment.slot_payload(self.slot_idx) as *const u8,
                self.len,
            )
        };
        pyo3::types::PyBytes::new(py, bytes)
    }

    // --- buffer protocol ---
    //
    // These two magic methods are how pyo3 exposes a Python-side
    // memoryview backed by Rust-owned memory. Python code stays
    // identical: `memoryview(sample)` Just Works.

    /// Fill in the `Py_buffer` struct so Python can read the SHM
    /// bytes directly.
    ///
    /// # Safety
    ///
    /// Standard pyo3 buffer-protocol contract: `view` is a writable
    /// `Py_buffer`; we populate it via `PyBuffer_FillInfo`. The slot
    /// is kept alive by the `Sample`'s own refcount on the segment.
    unsafe fn __getbuffer__(
        slf: PyRefMut<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyValueError::new_err("buffer view is null"));
        }
        let buf_ptr = slf.segment.slot_payload(slf.slot_idx) as *mut std::os::raw::c_void;
        let buf_len = slf.len as ffi::Py_ssize_t;
        // 1 = read-only. PyBuffer_FillInfo increments the refcount
        // on the owner (this Sample), so Python keeps it alive as
        // long as any consumer holds the memoryview.
        let owner = slf.as_ptr();
        let read_only: c_int = 1;
        let rc = unsafe { ffi::PyBuffer_FillInfo(view, owner, buf_ptr, buf_len, read_only, flags) };
        if rc != 0 {
            Err(PyErr::fetch(slf.py()))
        } else {
            Ok(())
        }
    }

    /// Buffer protocol release hook. The Python side already
    /// decremented the owner refcount; no per-buffer state to free
    /// because `PyBuffer_FillInfo` doesn't allocate.
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        // Nothing to do — `PyBuffer_FillInfo` doesn't allocate
        // `view->internal`, and the slot's refcount is held by
        // `self`, not the buffer view.
    }

    fn __len__(&self) -> usize {
        self.len
    }

    fn __repr__(&self) -> String {
        format!("Sample(seq={}, nbytes={})", self.seq, self.len)
    }
}

impl Drop for Sample {
    fn drop(&mut self) {
        self.segment.release(self.slot_idx);
    }
}

/// `quicbit` python module entry point. Mounted by maturin when
/// building with `--features python-extension`.
#[pymodule]
fn quicbit(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Service>()?;
    m.add_class::<Publisher>()?;
    m.add_class::<Subscriber>()?;
    m.add_class::<Sample>()?;
    Ok(())
}
