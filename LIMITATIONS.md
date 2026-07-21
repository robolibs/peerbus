# LIMITATIONS

Known sharp edges. Pair with [`PLAN.md`](PLAN.md) for the roadmap.

`0.3.x` — pre-1.0. Wire formats are documented but **not stable**
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

- **Backing-store exhaustion is handled on tmpfs, best-effort
  elsewhere.** A segment's pages are backed lazily on first touch, so
  a full `/dev/shm` would otherwise deliver an uncatchable `SIGBUS`
  when the creator zeroes the segment. On Linux tmpfs the creator
  pre-reserves the whole segment with `fallocate` and turns an
  out-of-space condition into a clean `Error::ShmExhausted`; an opener
  probes pages with `MADV_POPULATE_READ` before reading. On paths
  where pre-reservation is unavailable — a non-tmpfs `/dev/shm`,
  kernels older than 5.14 (no `fallocate`/`MADV_POPULATE_READ`), or
  file-descriptor exhaustion during the reservation reopen — the
  creator falls back to the lazy path and logs a warning; a genuine
  allocation failure there can still abort the *creating* process via
  `SIGBUS`. This never corrupts consumers: `magic` is published only
  after the segment is fully backed, so an opener never observes a
  half-backed segment.

- **A creator that dies mid-initialization self-heals.** If a process
  dies after creating a segment but before it is fully initialized, the
  next opener detects the dead creator (by pid + `/proc` start token),
  reclaims the name via an inode-verified unlink, and rebuilds — no
  manual `rm /dev/shm/qb_*`. A wrongly-suspected *live* creator can
  never lose its segment: it abandons rather than fighting for the
  name, and only the mapped-and-arbitrated inode is ever unlinked, so a
  healthy segment recreated under the same name is never destroyed.
  One narrow, self-healing corner remains: if a *reclaiming* process
  dies in the sub-millisecond window between claiming responsibility
  and unlinking, and its pid is immediately reused by an unrelated live
  process, recovery stalls until that unrelated process exits (bounded,
  transient, no data loss). On non-Linux targets, or if
  `/proc/self/maps` is unreadable, the reclaimer's inode identity falls
  back to a by-name stat, which reopens a vanishingly small
  repurpose-during-crash window.

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
- **Type identity uses size + alignment + type name** hashed with
  FNV-1a (`transport::wire_type_hash::<T>()`). Distinct types produce
  distinct hashes, so a mismatched publisher/subscriber pair fails
  loudly with `TypeMismatch` rather than silently interchanging
  same-shaped messages. The trade-off: the hash is **not** invariant
  across rustc versions that reformat type names, nor across renaming
  or moving a type — **peers must be built from the same type
  definitions**, and ideally the same toolchain. A refused connection
  is diagnosable in seconds; corrupted data in a control loop is not.
  (Earlier releases hashed size+align only, which was toolchain-stable
  but collision-prone.) Note this hash identifies the Rust-side
  transport slot only; the cross-language `datapod` schema hash that
  the C/Python bindings read rides *inside* the message and is
  unaffected.
- **Peer ACL is deny-by-default.** A node accepts an inbound
  connection only from peers named by `.allow_peer(...)` /
  `.allow_peers(...)`. With no allowlist configured it rejects
  everyone, unless the deny-by-default policy is explicitly waived
  with `.allow_any_peer()` — which accepts any peer that knows the
  ALPN and WARNs at bind. The check runs once per connection, right
  after the QUIC handshake, so it covers pub/sub and all five item
  modes. Only inbound accepts are filtered; outbound dials are not.
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

## Item modes (req/res, que/ans, put/ack, pip)

- **Large messages are chunked.** Each remote item rides the same
  opaque-byte chunk/reassembly path as pub/sub, so a request / answer /
  put / pip message larger than the 64 MiB single-frame cap is split and
  reassembled. Total size is bounded by `TopicQos::max_inflight_bytes`.
  Opt into bigger limits with the `*_with_qos` constructors
  (`req_server_with_qos`, `que_client_with_qos`, …); the byte-limit
  fields apply, and `delivery` is treated as **Reliable**.
- **`DeliveryPolicy::Latest` / `BestEffort` and QUIC datagrams are
  pub/sub-only by design.** These modes never drop in-flight items —
  dropping a response / a queued answer / an uploaded chunk / a session
  message would break their contract — so freshness-wins delivery and
  the datagram path do not apply.
- **Per-mode counters** are available via `.stats()` on every server and
  client (`ReqStats`/`QueStats`/`PutStats`/`PipStats`). They count the
  **remote (iroh)** path; locally-routed (SHM) endpoints report zeros,
  mirroring `PublisherStats`.
- **Standalone `RemoteTransport` servers/clients exist for all five
  modes** (`serve_queries`/`que_client`, `serve_uploads`/`put_client`,
  `serve_sessions`/`pip_client`, alongside pub/sub and req/res), and
  interoperate with the `Node` path over iroh in both directions. They
  take `bytemuck::Pod` payloads and are an advanced escape hatch — most
  users want `Node`. The standalone **pip** server is
  *collect-then-respond* (the handler receives all client messages, then
  returns all replies); for a truly interactive session use the `Node`
  pip API. Standalone clients send single-frame items (like standalone
  req/res), so the >64 MiB chunked-*send* path is `Node`-only; standalone
  servers do reassemble chunked items from a `Node` client.

## Identity

- **peerbus only consumes a key.** `Node::builder()` takes either
  `.secret_key(sk)` (an explicit ed25519 key) or `.ephemeral()` (a fresh
  random key). The key's public half is the node's `EndpointId` — the
  single id used for both iroh dialing and the shared-memory rendezvous
  name (`<hex(EndpointId)>__<topic>`). peerbus does **not** derive keys
  from names, load them from files, or persist them; producing, naming,
  and securing keys is the higher-level crate's responsibility (see
  `PLAN_HIGHER_CRATE.md`). A caller that derives a key from a guessable
  string is responsible for the impersonation risk that implies.
- **Peers are addressed only by id.** `subscriber` / `*_client` take an
  `EndpointId` or `EndpointAddr`; there is no friendly-name or `did:key`
  form inside peerbus. The higher-level crate resolves names → ids and
  then calls in here.

## Platforms

Linux x86_64 + aarch64. macOS likely works (POSIX SHM + the same
iroh stack) but is not in the test matrix. The local backend itself is
plausibly portable through `shared_memory` / `raw_sync`, but Windows is
not yet exercised here.
