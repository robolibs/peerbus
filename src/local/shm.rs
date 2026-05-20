//! Thin libc wrapper for shm_open + ftruncate + mmap.
//!
//! POSIX SHM (`/dev/shm` on Linux) is fast and survives until
//! `shm_unlink` or reboot. We keep this layer dumb: it owns an fd and
//! a mapping pointer, exposes raw bytes, and handles teardown. All
//! typed layout sits one module up in `layout.rs`.
//!
//! For tests and miri runs there is also a `heap` backing — a
//! plain `Box<[u8]>` that the rest of the segment code can treat
//! identically. miri can't call `shm_open`, so the heap backing is
//! the only way to exercise the slot-pool / publish-ring atomics
//! under miri.

use std::alloc::{self, Layout};
use std::ffi::CString;
use std::io;
use std::os::raw::c_int;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::Error;

/// Alignment for the test-only heap backing. Real POSIX SHM is
/// page-aligned (4096 bytes); we match that so heap-backed tests
/// see the same alignment guarantees as the production path.
const TEST_HEAP_ALIGN: usize = 4096;

/// Owned shared-memory mapping. Either a POSIX `shm_open` mapping
/// or, for tests, a heap-allocated buffer.
pub struct ShmMapping {
    name: CString,
    ptr: *mut u8,
    len: usize,
    backing: Backing,
    /// Only used for the POSIX backing; controls whether `Drop`
    /// calls `shm_unlink`.
    owns_name: AtomicBool,
}

enum Backing {
    Posix { fd: c_int },
    /// Heap-allocated buffer; the `*mut u8` in [`ShmMapping::ptr`]
    /// is the head of an `into_raw`'d `Box<[u8]>` of `len` bytes.
    /// Reconstructed via `Box::from_raw` on drop to free the
    /// allocation. We can't keep the `Box` alongside the pointer
    /// because moving the `Box` after taking its pointer invalidates
    /// the pointer's provenance under Stacked Borrows.
    Heap,
}

unsafe impl Send for ShmMapping {}
unsafe impl Sync for ShmMapping {}

impl ShmMapping {
    /// Create a new SHM segment of `size` bytes, opening with
    /// `O_CREAT | O_EXCL`. Returns [`Error::ServiceAlreadyExists`] if
    /// another process already owns this name.
    pub fn create(name: &str, size: usize) -> Result<Self, Error> {
        let cname = posix_name(name)?;
        let fd = unsafe {
            libc::shm_open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        };
        if fd < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EEXIST) {
                return Err(Error::ServiceAlreadyExists(name.to_string()));
            }
            return Err(Error::Io(err));
        }
        if unsafe { libc::ftruncate(fd, size as libc::off_t) } != 0 {
            let err = io::Error::last_os_error();
            unsafe {
                libc::close(fd);
                libc::shm_unlink(cname.as_ptr());
            }
            return Err(Error::Io(err));
        }
        let ptr = map(fd, size)?;
        Ok(Self {
            name: cname,
            ptr,
            len: size,
            backing: Backing::Posix { fd },
            owns_name: AtomicBool::new(true),
        })
    }

    /// Open an existing SHM segment by name. Caller is responsible
    /// for validating the magic/version inside the segment.
    pub fn open(name: &str, size: usize) -> Result<Self, Error> {
        let cname = posix_name(name)?;
        let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600) };
        if fd < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Err(Error::ServiceNotFound(name.to_string()));
            }
            return Err(Error::Io(err));
        }
        let ptr = map(fd, size)?;
        Ok(Self {
            name: cname,
            ptr,
            len: size,
            backing: Backing::Posix { fd },
            owns_name: AtomicBool::new(false),
        })
    }

    /// Heap-backed mapping for tests and miri. Returns a buffer
    /// aligned to [`TEST_HEAP_ALIGN`] (4 KiB, matching POSIX page
    /// alignment) so the segment's `ControlPage` deref is sound.
    /// The buffer is owned by the resulting `ShmMapping` and freed
    /// on drop. `name` is cosmetic — no `shm_open` is called.
    #[doc(hidden)]
    pub fn test_heap(name: &str, size: usize) -> Self {
        let layout = Layout::from_size_align(size, TEST_HEAP_ALIGN)
            .expect("test_heap size must be > 0");
        // SAFETY: layout has size > 0 (checked above).
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        let cname = CString::new(format!("test:{name}")).unwrap_or_else(|_| CString::default());
        Self {
            name: cname,
            ptr,
            len: size,
            backing: Backing::Heap,
            owns_name: AtomicBool::new(false),
        }
    }

    /// Drop ownership of the named segment without unlinking. Used
    /// when transferring ownership to the SHM refcount mechanism.
    pub fn release_ownership(&self) {
        self.owns_name.store(false, Ordering::Release);
    }

    /// Claim ownership (so this handle will unlink on drop). Used
    /// when the per-segment refcount reaches zero.
    pub fn claim_ownership(&self) {
        self.owns_name.store(true, Ordering::Release);
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for ShmMapping {
    fn drop(&mut self) {
        match self.backing {
            Backing::Posix { fd } => unsafe {
                libc::munmap(self.ptr as *mut libc::c_void, self.len);
                libc::close(fd);
                if self.owns_name.load(Ordering::Acquire) {
                    libc::shm_unlink(self.name.as_ptr());
                }
            },
            Backing::Heap => {
                // Pair with `alloc::alloc_zeroed` in
                // [`Self::test_heap`]: same layout, same pointer.
                let layout = Layout::from_size_align(self.len, TEST_HEAP_ALIGN)
                    .expect("layout must be valid (created in test_heap)");
                // SAFETY: `self.ptr` came from `alloc_zeroed` with
                // this exact layout; no other references remain
                // because we're inside `Drop`.
                unsafe { alloc::dealloc(self.ptr, layout) };
            }
        }
    }
}

fn posix_name(name: &str) -> Result<CString, Error> {
    if name.is_empty() {
        return Err(Error::invalid_argument("service name must not be empty"));
    }
    // POSIX shm_open expects a name starting with '/' and no further
    // slashes. We escape `/` and `.` from the user-facing name.
    let sanitized = name.replace(['/', '.'], "_");
    let leading = format!("/quicbit.{}", sanitized);
    CString::new(leading).map_err(|_| Error::invalid_argument("name contains nul byte"))
}

fn map(fd: c_int, size: usize) -> Result<*mut u8, Error> {
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        let err = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(Error::Io(err));
    }
    Ok(ptr as *mut u8)
}

/// Manually unlink a segment by name. Idempotent — missing names
/// are treated as success.
pub fn unlink(name: &str) -> Result<(), Error> {
    let cname = posix_name(name)?;
    let rc = unsafe { libc::shm_unlink(cname.as_ptr()) };
    if rc != 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOENT) {
            return Ok(());
        }
        return Err(Error::Io(err));
    }
    Ok(())
}
