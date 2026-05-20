//! Cross-process tests for the local SHM transport.
//!
//! Forks a child process that attaches to the same SHM segment and
//! exchanges messages with the parent. Tests both directions
//! (parent→child and child→parent) and the per-segment refcount
//! that drives `shm_unlink` on last detach.
//!
//! Only runs on Linux + macOS (POSIX SHM). Should the host lack
//! `/dev/shm` (e.g., a sandbox), the test self-skips.

use std::io::{Read, Write};
use std::os::unix::io::FromRawFd;
use std::os::unix::io::IntoRawFd;
use std::time::{Duration, Instant};

use bytemuck::{Pod, Zeroable};
use quicbit::{Error, LocalConfig, LocalService};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq)]
struct Ping {
    seq: u32,
    payload: u32,
}

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("xproc-{stem}-{pid}-{nanos}")
}

fn poll_until<R>(deadline: Instant, mut f: impl FnMut() -> Option<R>) -> Option<R> {
    while Instant::now() < deadline {
        if let Some(v) = f() {
            return Some(v);
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    None
}

/// Pipe pair used to synchronize parent and child.
struct Pipe {
    read: std::fs::File,
    write: std::fs::File,
}

fn make_pipe() -> Pipe {
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "pipe() failed");
    Pipe {
        read: unsafe { std::fs::File::from_raw_fd(fds[0]) },
        write: unsafe { std::fs::File::from_raw_fd(fds[1]) },
    }
}

#[test]
fn parent_publishes_child_subscribes() {
    let name = unique_name("p2c");

    // Pipe carries a single byte from child → parent meaning "I've
    // attached, you can publish."
    let mut sync = make_pipe();

    let svc = LocalService::<Ping>::create(&name, LocalConfig::default()).unwrap();

    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");

    if pid == 0 {
        // --- child ---
        // The child uses the fork-inherited mapping; calling
        // `attach()` here is also valid (it would bump the
        // attached counter), but inheriting is the simpler path.
        drop(sync.read);

        let mut sub = svc.subscriber();

        // Signal "ready" to parent.
        sync.write.write_all(b"R").unwrap();
        drop(sync.write);

        let deadline = Instant::now() + Duration::from_secs(5);
        let sample = poll_until(deadline, || sub.take().ok().flatten())
            .expect("child should receive a sample");
        assert_eq!(*sample, Ping { seq: 1, payload: 42 });

        // Exit success so the parent's waitpid sees code 0.
        // `process::exit` bypasses Drop, which is what we want for
        // the inherited mapping anyway.
        std::process::exit(0);
    }

    // --- parent ---
    drop(sync.write);

    // Wait for child's "ready" signal.
    let mut buf = [0u8; 1];
    sync.read.read_exact(&mut buf).expect("ready byte");
    assert_eq!(&buf, b"R");

    let mut pubr = svc.publisher();
    pubr.send(Ping { seq: 1, payload: 42 }).unwrap();

    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "child exited badly: status={status:#x}"
    );

    // Drop both handles, then re-create. The per-segment refcount
    // should have hit zero and `shm_unlink` should have run, so
    // `O_CREAT|O_EXCL` succeeds with the same name.
    drop(pubr);
    drop(svc);
    let _again = LocalService::<Ping>::create(&name, LocalConfig::default())
        .expect("re-create after both detached should succeed");
}

#[test]
fn child_publishes_parent_subscribes() {
    let name = unique_name("c2p");

    let mut sync = make_pipe();
    let svc = LocalService::<Ping>::create(&name, LocalConfig::default()).unwrap();
    let mut sub = svc.subscriber();

    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");

    if pid == 0 {
        // --- child publisher --- uses inherited mapping.
        drop(sync.read);

        let mut pubr = svc.publisher();

        // Brief wait so parent's subscriber cursor is past the
        // "before-attach" cutoff for our first publish.
        std::thread::sleep(Duration::from_millis(10));
        pubr.send(Ping { seq: 7, payload: 1337 }).unwrap();

        sync.write.write_all(b"D").unwrap();
        drop(sync.write);
        std::process::exit(0);
    }

    drop(sync.write);
    // Wait for the child's "done" signal.
    let mut buf = [0u8; 1];
    sync.read.read_exact(&mut buf).expect("done byte");

    let deadline = Instant::now() + Duration::from_secs(5);
    let sample = poll_until(deadline, || sub.take().ok().flatten())
        .expect("parent should receive child's publish");
    assert_eq!(*sample, Ping { seq: 7, payload: 1337 });

    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
    assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
}

#[test]
fn create_collision_returns_already_exists() {
    let name = unique_name("collision");
    let _svc = LocalService::<Ping>::create(&name, LocalConfig::default()).unwrap();
    match LocalService::<Ping>::create(&name, LocalConfig::default()) {
        Err(Error::ServiceAlreadyExists(_)) => {}
        Err(e) => panic!("expected ServiceAlreadyExists, got {e:?}"),
        Ok(_) => panic!("expected ServiceAlreadyExists, got Ok"),
    }
}

// Force File's RawFd to be consumed (avoid -D unused_must_use noise
// if `IntoRawFd` happens to be unused).
fn _force_into_raw_fd<T: IntoRawFd>(_: T) {}
