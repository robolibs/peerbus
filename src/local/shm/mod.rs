//! Pure-Rust shared-memory ring used by the local transport.
//!
//! This module is intentionally small and POD-only: one named
//! `shared_memory` mapping per service, a fixed control block, and a
//! fixed-size ring of slots. The public local API still exposes
//! loan/fill/publish and non-blocking take; the implementation details
//! stay private to `src/local`.
//!
//! Module layout (all submodules share this module's imports and
//! constants via `use super::*`):
//! * [`layout`] — the on-wire `ControlBlock` / `SlotHeader` / `Layout`
//!   structs and their size math.
//! * [`segment`] — a named SHM mapping's create/attach/reclaim lifecycle.
//! * [`producer`] — the writer side: `Producer`, its lease, and `Loan`.
//! * [`consumer`] — the reader side: `Consumer`, its lease, and `Sample`.
//! * [`process`] — pid / process-start-token liveness probes.
//! * [`os`] — OS-specific naming, backing-store reservation, and
//!   segment validation.

use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytemuck::{Pod, Zeroable};
use shared_memory::{Shmem, ShmemConf};

use crate::error::{Error, Result};
use crate::local::service::LocalConfig;
use crate::transport::fnv1a64;

const MAGIC: u64 = 0x5155_4943_4249_5431; // "PEERBUS1"
// Bumped from 3: `ControlBlock` gained the creator-identity stamp
// (`creator_pid` / `creator_token`), so the layout differs from segments
// written by older peers.
const VERSION: u32 = 4;
const MAX_SERVICE_NAME_BYTES: usize = 200;
const MAX_TRACKED_PUBLISHERS: usize = 64;
const MAX_TRACKED_SUBSCRIBERS: usize = 31;
const WRITER_STATE: u32 = u32::MAX;

/// How long an opener tolerates an *unstamped* control block (`creator_pid`
/// still 0) before declaring the segment poisoned. `create` stamps its pid
/// within microseconds of sizing the object (the stamp is the very first
/// thing it does after `ftruncate`), so a live creator is essentially never
/// seen unstamped past this window. It is deliberately far longer than any
/// plausible scheduling stall (preemption, cgroup CPU throttle, brief
/// SIGSTOP), so eagerly condemning a *live* creator is rare. Crucially,
/// *correctness does not depend on this value*: the common "creator died
/// after stamping" case is caught immediately and precisely by the pid+token
/// liveness check, which never uses this timer, and even if a live creator
/// were wrongly condemned via this backstop, its sticky-stamp CAS in `create`
/// then fails and it abandons the segment WITHOUT unlinking — so no split
/// brain results. The grace only trades a longer wait against needless
/// reclaim work.
const CREATOR_STAMP_GRACE: Duration = Duration::from_secs(1);

/// Overall deadline a single attach waits for a segment to become usable
/// (creator finishes, or an abandoned one is condemned). Must exceed
/// [`CREATOR_STAMP_GRACE`] so the unstamped backstop can actually fire.
const ATTACH_DEADLINE: Duration = Duration::from_secs(2);

/// Overall deadline for `open_or_create`, spanning any create/attach retries
/// and reclaim rounds. Must comfortably exceed [`ATTACH_DEADLINE`].
const OPEN_OR_CREATE_DEADLINE: Duration = Duration::from_secs(12);

/// Bound on how many poisoned segments a single `open_or_create` will
/// reclaim before giving up, so a pathological churn of dying creators can
/// never spin here forever.
const MAX_RECLAIM_ATTEMPTS: u32 = 8;

mod consumer;
mod layout;
mod os;
mod process;
mod producer;
mod segment;

// Reconstruct the flat `shm::*` namespace at crate visibility so both the
// sibling submodules (via `use super::*`) and the rest of `crate::local`
// keep resolving `shm::Segment`, `shm::Producer`, etc. unchanged.
pub(crate) use consumer::*;
pub(crate) use layout::*;
pub(crate) use os::*;
pub(crate) use process::*;
pub(crate) use producer::*;
pub(crate) use segment::*;

#[cfg(test)]
mod tests;
