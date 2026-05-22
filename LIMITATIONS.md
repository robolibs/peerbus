# LIMITATIONS

What's actually tested, what's audited, and what's known sharp.
Pair this with [`PLAN.md`](PLAN.md) for the production-readiness
roadmap.

## Versioning

`0.0.x` — pre-1.0. Wire formats are documented but **not stable**
between minor releases. The `PLAN.md` phases target `0.1.0` as the
first stable release after operability hardening completes.

## What is tested

Integration tests in `tests/`:

| File                  | Tests | Notes                                            |
|-----------------------|------:|--------------------------------------------------|
| `local_inproc`        |     4 | iceoryx2 loan/publish/take, in-process           |
| `local_reqresp`       |     2 | iceoryx2-backed req/resp                         |
| `node`                |     7 | unified `Node` API, local + remote routing + ACL |
| `remote_loopback`     |     3 | iroh pub/sub round-trip + publisher-drop survive |
| `remote_reqresp`      |     1 | iroh req/resp round-trip                         |
| `async_adapter`       |     1 | `AsyncPublisher` / `AsyncSubscriber` smoke       |
| `auto_traits`         |     2 | compile-time `Send`/`Sync` assertions            |
| `datapod_payload`     |     5 | `datapod` types crossing the wire                |
| `wire_parsers`        |    11 | edge cases on the pure-byte wire parsers         |
| Doc tests             |     2 | top-of-crate + `Node` examples                   |

A `fuzz/` directory holds `cargo-fuzz` targets for the wire
parsers (`pubsub_handshake`, `request_handshake`, `frame`). They
share the same `pub fn parse_*` entrypoints the integration tests
hit, so seed corpora can be developed locally without disturbing
the main test suite. Hook them into CI as a nightly job when the
`cargo-fuzz` toolchain is available on the runner.

`cargo clippy --all-features --all-targets -- -D warnings` is clean.

The legacy custom-SHM allocator (`src/local/segment.rs`,
`src/local/layout.rs`, the cross-process registry, the C ABI, and
the Python bindings) was retired in the iceoryx2 migration. Its
miri / proptest / fuzz coverage went with it; iceoryx2 carries its
own test surface upstream.

## What is *not* yet tested

See `PLAN.md` §F for the planned coverage. The big absences today:

- **Cross-host iroh tests** — loopback works (the `remote_*` tests
  prove the protocol round-trips between two endpoints in one
  process), but cross-machine connectivity is demonstrated manually
  rather than baked into CI.
- **Connection-drop / reconnect across reboots** — the
  `node_subscriber_survives_publisher_drop` test covers in-process
  publisher loss with the reconnect loop in place. Recovery across
  machine reboots / network partition is exercised manually only.
- **Malicious / adversarial peer suite** — the wire parsers
  (`read_*_handshake_tail`, `read_frame`) are bounded but unfuzzed.
- **Sustained-load endurance** — `examples/bench_local.rs` is a
  short microbench (100 000 × 64-byte messages); there is no
  hours-long run, no latency histogram beyond mean, no leak audit
  under kernel pressure.
- **Multi-publisher on iceoryx2** — the legacy custom allocator's
  failure mode is gone, but iceoryx2's behaviour under
  multi-publisher contention is not explicitly exercised in this
  crate's tests.

## CI

`.github/workflows/ci.yml` runs on every push / PR:

* `rustfmt --check`
* `clippy --all-features --all-targets -- -D warnings`
* `cargo test` on the real feature matrix: default, `tracing`,
  `config`, `tracing config`
* `cargo doc --all-features --no-deps` with `-D warnings`
* `cargo deny check` against `deny.toml`

Local development uses the Nix devshell (`nix develop`) which
exports `LIBCLANG_PATH` and `LD_LIBRARY_PATH` so iceoryx2's
`bindgen` step finds libclang. CI installs `libclang-dev` for the
same reason.

## Known sharp edges

### Local transport (iceoryx2)

- **Service name composition** is only lightly sanitised — spaces
  are mapped to `_`, everything else passes through to iceoryx2.
  Topic strings should stick to `[A-Za-z0-9._/-]`; PLAN §D.4
  tightens this at the API boundary.
- **History default is 1.** A subscriber that polls slower than the
  publisher publishes will miss samples; iceoryx2 surfaces this as
  the publish-side dropping the oldest in-flight sample. Choose
  `LocalConfig::history_depth` according to your worst-case
  subscriber latency.
- **`max_publishers` defaults to 2 and `max_subscribers` to 8.**
  These are iceoryx2 service-creation parameters and are *pinned*
  by the first creator; subsequent attaches must be compatible.

### Remote (iroh) transport

- **Reconnect is best-effort.** `ensure_peer_connection` checks the
  cached `Connection`'s `close_reason()` on every use and re-dials
  if it's dead. The `Node` subscriber loop wraps that in bounded
  exponential backoff (100 ms → 10 s cap), and `Subscriber::take()`
  surfaces `Err(Error::Disconnected)` when the foreground channel
  drops. Recovery across machine reboots / network partition is
  not tested; the in-process `node_subscriber_survives_publisher_drop`
  test covers the publisher-vanishes case.
- **Best-effort publish.** `RemotePublisher::publish` pushes onto a
  256-deep `broadcast::Sender`. If no subscriber is attached, the
  send is silently dropped. The `Lagged(n)` path is observed on the
  subscriber but the publisher gets no feedback. PLAN §B.2 surfaces
  per-publisher / per-subscriber counters.
- **`datapod::DataPod` payloads only on the wire.** Both transports
  ride the `header + bytes` split that `DataPod` exposes — the Pod
  header rides as iceoryx2's `user_header` / iroh's frame prefix,
  the optional byte payload as iceoryx2's slice payload / iroh's
  frame body. `serde`-shaped payloads are not yet wired in; types
  carrying owned heap data participate via `#[datapod::datapod]` +
  `#[dp(bytes)]`.
- **Type identity uses size + alignment** hashed with FNV-1a
  (`transport::wire_type_hash::<T>()`). The hash is stable across
  rustc versions, but two unrelated types with identical size and
  alignment hash the same; size mismatches are still caught at
  frame-decode time via the explicit `payload_size` field in the
  handshake.
- **Peer ACL is opt-in.** Without any `.allow_peer(...)` on the
  builder, every peer that knows the ALPN can dial the endpoint and
  subscribe to any topic. Pin the inbound set with one or more
  `.allow_peer` calls (see the `rejects_unallowlisted_peer` test).
  The `identity("name")` deterministic-key path is documented as
  impersonable, so still prefer `identity_file(path)` on untrusted
  networks.
- **One iroh `Endpoint` per `RemoteTransport`** (the low-level
  type). `Node` collapses this back down to one endpoint for the
  whole process; most users should be on `Node`.

### Identity

- **`identity("name")` is impersonable.** The literal string hashes
  to a deterministic `SecretKey`; anyone who knows the string can
  dial as that identity. Fine on a trusted LAN, *not* fine on the
  open internet. Use `identity_file(path)` for the
  cryptographically meaningful path.
- **`identity_file` stores 32 raw bytes** and sets `0600` on Unix
  on first generation. The file is *not* re-permissioned on
  subsequent reads — if an operator copies it without preserving
  mode, the key may end up world-readable.

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
   value rather than propagate. Recovery is silent; PLAN §B.1 adds
   a `tracing::warn!` so it does not disappear in prod.

There are **no** `expect()` or `panic!()` calls on a runtime path
in non-test code.

## Benchmark snapshot

From `cargo run --release --example bench_local` (single thread,
single publisher, single subscriber, 100 000 64-byte messages on
the iceoryx2 transport):

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
