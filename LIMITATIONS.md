# LIMITATIONS

Known sharp edges. Pair with [`PLAN.md`](PLAN.md) for the roadmap.

`0.0.x` — pre-1.0. Wire formats are documented but **not stable**
between minor releases.

## Local transport (shared-memory ring)

- **Topic names** must match `[A-Za-z0-9._/-]+` and be at most
  `node::MAX_TOPIC_BYTES` (200 bytes). `Node::publisher` /
  `Node::subscriber` reject anything else with
  `Error::InvalidArgument`. Direct `LocalService::open_or_create`
  calls hash the name to a short OS shared-memory id.
- **History default is 1.** A subscriber that polls slower than the
  publisher will miss samples; the publish side drops the oldest
  in-flight sample. Tune `LocalConfig::history_depth` to your
  worst-case subscriber latency.
- **`max_publishers` defaults to 2 and `max_subscribers` to 8.**
  These are service-creation parameters and are *pinned* by the
  first creator; subsequent attaches must be compatible.
- **Multi-publisher contention has bounded coverage.** The suite covers
  a two-publisher / one-subscriber happy path plus a four-thread burst
  where history covers every sample, plus a small proptest model for
  random publish/take/hold/drop sequences. Queue-saturation and
  publisher-eviction edge cases are still not endurance-tested.
- **Dead process cleanup is Linux-strong, Unix-first.** Publisher/subscriber port
  slots carry process ids, and sample holds are tracked by
  per-subscriber bits so the next publisher can reap a dead reader or
  writer before returning `NoFreeSlot`. This is covered by spawned
  child-process exit tests. On Linux, the PID is paired with the
  `/proc/<pid>/stat` process start token to avoid fast PID-reuse
  confusion. Remaining caveat: non-Unix targets use a conservative
  fallback unless/until a native process-start token is added.

## Remote (iroh) transport

- **Reconnect is best-effort.** `ensure_peer_connection` checks the
  cached `Connection`'s `close_reason()` on every use and re-dials
  if it's dead. The `Node` subscriber loop wraps that in bounded
  exponential backoff (100 ms → 10 s cap). Recovery across machine
  reboots / network partition is exercised manually only.
- **Best-effort publish.** `RemotePublisher::publish` pushes onto a
  256-deep `broadcast::Sender`. With no subscriber attached, the
  send is silently dropped (counted under `remote_dropped`). The
  `Lagged(n)` path is observed on the subscriber but the publisher
  gets no per-subscriber feedback.
- **`datapod::DataPod` payloads only.** Both transports ride the
  `header + bytes` split that `DataPod` exposes. `serde`-shaped
  payloads are not wired in; heap-bearing types participate via
  `#[datapod::datapod]` + `#[dp(bytes)]`.
- **Type identity uses size + alignment** hashed with FNV-1a
  (`transport::wire_type_hash::<T>()`). Stable across rustc
  versions; two unrelated types with identical size and alignment
  collide. Size mismatches are still caught at frame-decode time
  via the explicit `payload_size` field in the handshake.
- **Peer ACL is opt-in.** Without `.allow_peer(...)` on the
  builder, every peer that knows the ALPN can dial and subscribe.
- **Cross-host connectivity is not in CI.** Loopback is covered by
  `remote_*` tests; cross-machine paths are demonstrated manually.
- **Wire parsers are bounded but unfuzzed in CI.** A `fuzz/`
  directory holds `cargo-fuzz` targets; hook them into a nightly
  runner.
- **No sustained-load endurance.** `examples/bench_local.rs` is a
  short microbench — no hours-long run, no latency histogram, no
  leak audit under kernel pressure.
- **One iroh `Endpoint` per `RemoteTransport`.** `Node` collapses
  this back down to one endpoint for the whole process; most
  users should be on `Node`.

## Identity

- **`identity("name")` is impersonable.** The literal string hashes
  to a deterministic `SecretKey`; anyone who knows the string can
  dial as that identity. Fine on a trusted LAN, *not* fine on the
  open internet. Use `identity_file(path)` on untrusted networks.
- **Named publishers carry a hex-alias mirror.** When a publisher
  uses `.identity("name")`, the local service is opened under
  *two* names — the canonical `<name>__<topic>` and an alias
  `<hex(EndpointId)>__<topic>` — so DID:KEY / bare-EndpointId
  subscribers can still route locally. Each publish does an extra
  loan + byte-copy on the alias service; the cost is per-message
  and scales with payload size. `identity_file` / ephemeral
  publishers compose by hex directly and pay no alias cost.
- **`identity_file` stores 32 raw bytes** and sets `0600` on Unix.
  Mode is enforced on every read; chmod failures (read-only mount,
  foreign FS) downgrade to a warning so the bind doesn't refuse on
  pre-existing keys.

## Platforms

Linux x86_64 + aarch64. macOS likely works (POSIX SHM + the same
iroh stack) but is not in the test matrix. The local backend itself is
plausibly portable through `shared_memory` / `raw_sync`, but Windows is
not yet exercised here.
