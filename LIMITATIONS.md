# LIMITATIONS

What's actually tested, what's audited, and what's known sharp.
Pair this with [`PLAN.md`](PLAN.md) for the roadmap.

## Versioning

`0.0.x` — pre-1.0. Wire formats are documented but **not stable**
between minor releases. The PLAN targets `0.1.0` as the first
stable release after Phase 6 hardening completes.

## What is tested

Test counts as of the most recent commit (Phase 4 + 6 land):

| Suite              | Count | Notes                                       |
|--------------------|-------|---------------------------------------------|
| `local_inproc`     | 8     | loan/publish/take, fan-out, lag, type hash  |
| `local_xproc`      | 3     | fork-based parent ⇄ child                   |
| `local_reqresp`     | 3     | single-server, timeouts, multi-client      |
| `remote_loopback`   | 2     | iroh pub/sub, single + multi-subscriber    |
| `remote_reqresp`    | 1     | iroh req/resp, two endpoints               |
| `any_transport`     | 1     | `AnyTransport::Local` dispatch             |
| `auto_routing`      | 8     | `Service::auto` URL parsing (shm + iroh)   |
| `async_adapter`     | 1     | `AsyncPublisher` / `AsyncSubscriber`       |
| `ffi_smoke`         | 3     | C ABI in-process                           |
| `ffi_adversarial`   | 16    | NULL handles, bad UTF-8, missing out-ptrs  |
| `auto_traits`       | 3     | compile-time `Send`/`Sync` assertions      |
| `drop_ordering`     | 6     | sample-outlives-sub, reverse drop, clones  |
| `segment_concurrent`| 3     | concurrent free-list, SPMC, single-thread  |
| `segment_proptest`  | 1     | proptest: 64 random op-sequences           |
| `segment::tests`    | 8     | miri-runnable allocator unit tests         |
| Doc tests           | 1     | top-of-crate example                       |

`cargo clippy --all-features --all-targets -- -D warnings` is clean,
and `cargo miri test --lib local::segment::tests` runs the 8 unit
tests on miri's interpreter (heap-backed segment; see below).

## What is *not* yet tested

- **Cross-host iroh tests** — loopback works (the `remote_*` tests
  prove the protocol round-trips between two endpoints), but
  cross-machine connectivity is documented and demonstrable
  manually rather than baked into CI.
- **Lossy-link tests via wirebit** — iroh wraps its own UDP socket
  (`noq-udp`) and does not currently expose a custom-socket hook,
  so the original "swap UDP for `wirebit::TunLink`" plan is on hold
  (see PLAN §3.8). Adopt the hook when/if iroh provides it.
- **Sustained-load endurance** — the bench in
  `examples/bench_local.rs` covers short bursts; we have not yet
  measured behavior under hours of traffic or with kernel pressure.

## CI

`.github/workflows/ci.yml` runs the full matrix on every push /
PR:

* `rustfmt --check`
* `clippy --all-features --all-targets -- -D warnings`
* `cargo test` on six feature combinations (default, `remote`,
  `async`, `remote + async`, `python`, all-features)
* `cargo miri test --lib local::segment::tests` (nightly)
* `cargo doc --all-features --no-deps` with `-D warnings`
* `cargo deny check` against `deny.toml` (license + advisory +
  bans + sources policy)

Locally: `cargo fmt`, `cargo clippy --all-features --all-targets
-- -D warnings`, and (in the nightly devshell) `cargo miri test
--lib`.

## miri coverage

`cargo miri test --lib local::segment::tests` (in `nix develop
.#nightly`) runs the 8 allocator unit tests on miri's
Stacked-Borrows + Aliasing interpreter. Coverage:

* `pop_free` / `push_free` (lock-free CAS, slot index validity)
* `publish_slot` / `try_acquire` / `release` (refcount lifecycle,
  generation bumps, ring eviction)
* monotonic sequence numbers

The tests run on a heap-backed `Segment` (via the test-only
`Segment::test_from_heap`) so miri doesn't need `shm_open`. Two
real bugs were found and fixed during the first miri run:

1. Stacked-Borrows violation: the heap-backed `ShmMapping` used to
   take a raw pointer from a `Box<[u8]>` then move the box into the
   struct; that invalidated the pointer's provenance. Fixed by
   replacing the box with a manual `alloc::alloc_zeroed` /
   `dealloc` pair.
2. Misaligned `ControlPage` deref: the heap backing was
   1-byte-aligned. `ControlPage` requires 8-byte alignment because
   it holds `AtomicU64` fields. Fixed by allocating with
   page-aligned (4 KiB) `Layout`, matching real POSIX SHM.

Production paths (`shm_open` → `mmap`) were not affected; both
bugs only surfaced with the heap backing miri uses.

## Known sharp edges

### SHM allocator

- **Single publisher per topic is the supported configuration.**
  Multi-publisher use *appears* to work for short bursts but the
  multi-publisher stress test (`segment_concurrent::
  multi_publisher_stress_documented_failure_mode`) reproducibly
  leaks slots under load. The root cause is that concurrent
  publishers fetching sequence numbers from `publish_seq` can land
  on the same ring position out of order — `publish_slot`'s
  bump → swap → evict trio is non-atomic across the three
  operations, and an out-of-order publisher can effectively
  "delete" another publisher's ring entry while still bumping
  refcount for it. The free list is multi-producer safe; the *ring
  + refcount* dance is not. The single-publisher / multi-subscriber
  shape (the realistic robotics topology) is exercised by
  `segment_concurrent::single_publisher_many_subscribers_balanced`
  with 50 000 publishes against 6 contended subscribers — zero
  leaks. Designing a truly multi-publisher-safe ring is a separate
  project.
- **Slot size is rounded up to 8 bytes** at create time to keep
  the `AtomicU64` fields in each slot's header naturally aligned.
  Tiny payloads (< 8 bytes) therefore use more memory than asked.
- **History default is 1.** A subscriber that polls slower than
  the publisher publishes will see `Lagged { dropped: N }` errors
  with the count of skipped samples; on the next `take()` it sees
  the freshest available. Choose `history_depth` according to your
  worst-case subscriber latency.
- **Cross-process drop coordination.** If a publisher process
  crashes holding a loan, that slot stays out of the free list
  until the segment is fully unlinked. There is no per-slot
  watchdog yet (PLAN §6 risks). Linear in slot count; in practice
  the next `LocalService::create` with the same name unlinks the
  old segment and starts fresh.
- **Fork inheritance is opt-in.** A `Segment` carries the
  creator's PID and skips the per-segment refcount on drop if it
  observes a different live PID (i.e. it was inherited via fork
  and never explicitly `attach()`ed). Children of a fork that want
  to participate in the refcount must call `LocalService::attach`
  explicitly.

### Remote (iroh) transport

- **One topic per `RemoteTransport`.** The transport's `name()` is
  the topic; multiple topics need multiple endpoints. This matches
  the local transport's "one segment per service" semantics but
  isn't free of cost — each transport spins up one iroh
  `Endpoint`.
- **Pod-only payloads.** Phase 3/4 wires raw bytemuck bytes; the
  serde extension (non-POD types over the wire) lands later as
  part of the Phase 4 follow-on plan.
- **Best-effort publish.** `RemotePublisher::publish` returns the
  sequence number; messages are pushed onto a `broadcast::Sender`
  with depth 256. If no peer has subscribed yet, messages are
  dropped silently. The Phase 4 follow-on adds explicit
  back-pressure modes.
- **Auto-discovery via `Service::auto`** understands `shm://name`,
  `iroh://endpoint_id`, and `iroh://...?relay=URL&direct=IP:PORT`
  combinations. Full iroh-dns/pkarr resolution is supported
  inside iroh itself but isn't yet exposed as its own URL scheme
  here; pass a pre-resolved `EndpointAddr` via the builder API if
  you need it.

### FFI / Python

- **Byte-oriented.** The C ABI moves raw `slot_size` byte buffers
  in and out; payload types are the caller's concern.
- **Single-threaded handles.** `quicbit_publisher_t` /
  `quicbit_subscriber_t` are not safe to share across threads
  without external synchronization. Build a separate
  publisher/subscriber per thread.
- **Python `Subscriber.take()` returns a `Sample`** that exposes
  the SHM slot via the buffer protocol. `memoryview(sample)` is a
  zero-copy view into Rust-owned shared memory; the slot is held
  until the `Sample` is garbage-collected. Use `sample.to_bytes()`
  if you want an owned copy.
- **Python floor is 3.11** (`abi3-py311`). The buffer protocol via
  `__getbuffer__` is gated on `Py_3_11+` in the stable ABI; older
  Python versions are not supported. Python 3.9 + 3.10 reach EOL
  before this crate's 0.1.0 ships, so the bump is a non-event.

## Panic / unwrap audit

`cargo clippy --all-features` reports zero `unwrap()` lints under
the workspace's default lint level. Remaining `unwrap()` calls in
non-test code fall into two categories:

1. **`<[u8]>::try_into::<[u8; N]>().unwrap()`** on fixed-size
   handshake header slices. The slice is sized at the call site
   (`let mut header = [0u8; N]; recv.read_exact(&mut header)?`);
   the `try_into` cannot fail.
2. **`Mutex::lock().unwrap_or_else(|p| p.into_inner())`** on the
   transport's internal hash maps. Lock poisoning only occurs if a
   thread panics while holding the lock; we recover the inner
   value rather than propagate.

There are **no** `expect()` or `panic!()` calls on a runtime path
in non-test code.

## Benchmark snapshot

From `cargo run --release --example bench_local` (single thread,
single publisher, single subscriber, 100,000 64-byte messages):

| Metric                | Value (this machine)   |
|-----------------------|------------------------|
| Throughput            | ~3.6 M msg/s           |
| Mean publish latency  | ~250 ns                |
| Mean end-to-end       | ~300 ns                |

Numbers will vary by hardware and contention. The benchmark is
hand-rolled — no `criterion` dependency — so re-running it on your
target environment is the right way to compare.

## Platforms

Linux x86_64 + aarch64. macOS likely works (POSIX SHM + the same
iroh stack) but is not in the test matrix. Windows is not
supported on the local transport (no `shm_open`).
