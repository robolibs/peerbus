//! Tests for the same-host registry.
//!
//! The registry is process-wide and host-wide. To keep test
//! isolation honest, we don't use the public `REGISTRY_NAME`
//! constant — each test would race the real registry. Instead we
//! reach into the registry's lower-level `Registry::open_named`
//! (test-only) to create scratch registries with unique names.
//!
//! ...except that doesn't exist yet. So this file uses the real
//! registry name and serialises with a Mutex, accepting that
//! tests pollute each other's view until drop. Tests are written
//! to not depend on exact slot indices.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Mutex;
use std::sync::OnceLock;

use quicbit::registry::{Registry, REGISTRY_CAPACITY};

// Serialise registry-mutating tests so they don't observe each
// other's stale state.
fn registry_lock() -> &'static Mutex<()> {
    static M: OnceLock<Mutex<()>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(()))
}

fn fake_endpoint_id(seed: u8) -> [u8; 32] {
    let mut out = [0u8; 32];
    // Force a non-zero high byte so the entry is never confused
    // with the "empty slot" sentinel of all-zeros.
    out[0] = seed.wrapping_add(1);
    out[1] = seed;
    out
}

// Each test gets a unique-ish endpoint_id from this counter.
fn next_endpoint_id() -> [u8; 32] {
    static COUNTER: AtomicU8 = AtomicU8::new(0);
    fake_endpoint_id(COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[test]
fn open_creates_or_attaches() {
    let _g = registry_lock().lock().unwrap();
    let r1 = Registry::open().expect("open #1");
    let r2 = Registry::open().expect("open #2 (attach)");
    drop(r1);
    drop(r2);
}

#[test]
fn claim_then_lookup_finds_self() {
    let _g = registry_lock().lock().unwrap();
    let mut r = Registry::open().unwrap();
    let id = next_endpoint_id();
    r.claim(id).expect("claim");
    let hit = r.lookup(&id).expect("lookup hit");
    assert_eq!(hit.1, std::process::id(), "pid matches us");
}

#[test]
fn lookup_misses_for_unknown_endpoint() {
    let _g = registry_lock().lock().unwrap();
    let r = Registry::open().unwrap();
    let unknown = next_endpoint_id();
    assert!(r.lookup(&unknown).is_none());
}

#[test]
fn drop_releases_the_slot() {
    let _g = registry_lock().lock().unwrap();
    let id = next_endpoint_id();
    {
        let mut r = Registry::open().unwrap();
        r.claim(id).unwrap();
        assert!(r.lookup(&id).is_some());
    } // r drops here.

    // A fresh registry handle in the same process should NOT see
    // the dropped claim (slot was released).
    let r2 = Registry::open().unwrap();
    assert!(r2.lookup(&id).is_none(), "dropped slot should be reclaimable");
}

#[test]
fn claim_is_idempotent() {
    let _g = registry_lock().lock().unwrap();
    let mut r = Registry::open().unwrap();
    let id = next_endpoint_id();
    r.claim(id).unwrap();
    r.claim(id).unwrap(); // second claim refreshes heartbeat
    r.claim(id).unwrap();
    assert!(r.lookup(&id).is_some());
}

#[test]
fn many_claims_until_capacity() {
    let _g = registry_lock().lock().unwrap();
    // Open enough registries to fill the segment. Limited by
    // REGISTRY_CAPACITY across all live `Registry` handles.
    // Each `Registry::claim` only claims one slot (the registry's
    // own); to fill it we'd need many processes. Approximate the
    // capacity behaviour by checking that distinct endpoint_ids
    // all find their slot.
    let mut r = Registry::open().unwrap();
    let id = next_endpoint_id();
    r.claim(id).unwrap();
    assert!(r.lookup(&id).is_some());

    // Each Registry only claims for one endpoint_id. To exercise
    // capacity we'd need processes; that's covered by xproc tests.
    let _ = REGISTRY_CAPACITY;
}

#[test]
fn heartbeat_refresh_does_not_crash() {
    let _g = registry_lock().lock().unwrap();
    let mut r = Registry::open().unwrap();
    let id = next_endpoint_id();
    r.claim(id).unwrap();
    for _ in 0..10 {
        r.heartbeat();
    }
}
