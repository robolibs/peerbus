# quicbit

`quicbit` is a Rust **typed zero-copy messaging** library for
robotics. One service-oriented API, two transports under the hood
— both always on:

- **Local (same host)** — loan-publish-consume pub/sub on top of
  [iceoryx2]'s production-grade shared-memory IPC. Publisher
  writes the payload *in place* into a slot, hands over a pointer;
  subscribers read the same bytes. No serialization, no copy
  across the process boundary.
- **Remote (across hosts)** — pub/sub over [iroh] peer-to-peer
  QUIC. Peers are identified by ed25519 `EndpointId`s rather than
  IP\:port. NAT hole punching + relay fallback are handled by iroh,
  TLS 1.3 is mandatory (and PKI-free — the `SecretKey` *is* the
  identity). Topics map to QUIC streams; back-pressure is the
  stream's own flow control.

[iceoryx2]: https://github.com/eclipse-iceoryx/iceoryx2
[iroh]: https://github.com/n0-computer/iroh

A `quicbit::Node` is the same to callers regardless of where its
subscribers live — local-only, remote-only, or a mixed fan-out.
The routing decision (SHM vs iroh) happens automatically at
`subscriber()` time.

## What quicbit is NOT

- **Not a simulator.** [`wirebit`](https://codeberg.org/robolibs/wirebit)
  is the bus simulator + HIL bridge; quicbit *uses* it as a test
  substrate, not as its runtime transport.
- **Not ROS.** No CDR, no DDS, no ROS graph. Interop with ROS 2 is
  a future sidecar (quicbit ⇄ Zenoh ⇄ DDS), not in-process.
- **Not RPC.** Req/resp is a native pattern, but this is message
  middleware, not a full RPC framework (no service registry, no
  codegen).

## Architecture

```text
                 ┌────────────────────────────────┐
                 │       Application              │
                 └───────────┬────────────────────┘
                             │  loan / publish / subscribe / call
                 ┌───────────▼────────────────────┐
                 │           Node                 │   <- this crate
                 │  (pub/sub + req/resp, typed)   │
                 └───────────┬────────────────────┘
                             │
              ┌──────────────┴──────────────────┐
              │                                 │
     ┌────────▼─────────┐              ┌────────▼─────────┐
     │   LocalTransport │              │  RemoteTransport │
     │  (iceoryx2 SHM)  │              │  (iroh / QUIC)   │
     └──────────────────┘              └──────────────────┘
       same-host,                         across hosts,
       true zero-copy,                    TLS 1.3, hole punching,
       lock-free                          per-topic streams
```

## Quick start

One entry point: `Node`. Two strings: who **I** am, who I'm
listening to.

```rust,ignore
use bytemuck::{Pod, Zeroable};
use iceoryx2::prelude::ZeroCopySend;
use quicbit::Node;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, ZeroCopySend)]
struct Pose { x: f32, y: f32, yaw: f32 }

let node = Node::builder().identity("rover-a").no_relay().bind()?;

let mut pubr = node.publisher::<Pose>("rover/pose")?;
let mut sub  = node.subscriber::<Pose>("rover-a", "rover/pose")?;

pubr.send(Pose { x: 1.0, y: 2.0, yaw: 0.1 })?;
if let Some(s) = sub.take()? {
    println!("pose: {:?}", *s);
}
# Ok::<_, quicbit::Error>(())
```

If the publisher's iceoryx2 service exists on this host (any
process using the same `identity` + `topic`), the subscriber
attaches to its SHM slot and reads with zero copy. Otherwise it
dials over iroh. **Same call either way.**

### Cross-host

For a real cross-host dial you need the publisher's full transport
address, not just its identity string. `IntoPeer` accepts an
`EndpointAddr`:

```rust,ignore
// publisher side
let pub_node = Node::builder().identity_file("/etc/rover.key").bind()?;
pub_node.wait_for_direct_addresses(std::time::Duration::from_secs(5))?;
let pub_addr = pub_node.endpoint_addr(); // share this with peers

// subscriber side
let mut sub = sub_node.subscriber::<Pose>(pub_addr, "rover/pose")?;
```

If the iroh `Connection` drops mid-stream, the subscriber's
background loop redials with bounded exponential backoff (100 ms
→ 10 s cap) and re-issues the handshake automatically — the
caller stays oblivious unless they explicitly look at
`Subscriber::stats()`. `take()` returns `Err(Error::Disconnected)`
only after the foreground channel itself goes away.

### Observability

Each publisher and subscriber tracks lifetime counters readable
via `.stats()`:

```rust,ignore
let s = sub.stats();      // received, disconnects
let p = pubr.stats();     // published, remote_dropped
let n = node.stats();     // publisher_topics, cached_peers
```

For structured logs, enable the `tracing` feature. quicbit then
emits events at accept / connect / disconnect / handshake-mismatch
/ broadcast-lag boundaries; the loan-publish-consume hot path
stays uninstrumented to keep it free of overhead.

### Limiting who can dial in

By default a `Node` accepts any peer that knows the ALPN. To pin
the inbound set, hand the builder one or more allowlisted peer
ids:

```rust,ignore
let node = Node::builder()
    .identity_file("/etc/rover.key")
    .allow_peer(planner_endpoint_id)
    .allow_peer(logger_endpoint_id)
    .bind()?;
```

Non-allowlisted peers are closed immediately after the QUIC
handshake completes; no streams open. Outbound dials are not
affected.

### Payload type requirements

Every `T` you publish/subscribe must satisfy three traits:

- `bytemuck::Pod + bytemuck::Zeroable` — fixed memory layout for
  the iroh wire path.
- `iceoryx2::ZeroCopySend` — marker that the type may ride in
  shared memory between processes.
- `Debug` — required by iceoryx2's `Sample` / `SampleMut`.

In practice that's one struct annotation:

```rust,ignore
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, ZeroCopySend)]
struct MyMessage { /* ... */ }
```

### Identity

Three ways to pin a `Node`'s iroh identity:

```rust,ignore
.identity("rover-a")              // string in code — dev / trusted networks
.identity_env("ROVER_ID")         // string from an env var
.identity_file("/etc/rover.key")  // random key, persisted — production
```

String identities hash to a deterministic `SecretKey` (and so to a
stable `EndpointId`). Anyone with the string can impersonate; only
use in trusted contexts. The `_file` variant is the
cryptographically meaningful path — the file holds 32 raw bytes
and is generated on first run.

## Request / response

```rust,ignore
use quicbit::{LocalConfig, LocalReqRespService};

let svc = LocalReqRespService::<Ping, Pong>::create("calc", LocalConfig::default())?;
let mut server = svc.server()?;
while let Some((req, reply)) = server.take_request()? {
    reply.respond(handle(&*req))?;
}

let mut client = svc.client()?;
let pong: Pong = client.call(Ping { /* ... */ })?;
# Ok::<_, quicbit::Error>(())
```

## Lower-level building blocks

`Node` is the recommended entry point. The pieces it composes are
also public if you want direct control:

- `LocalTransport` / `LocalService<T>` — iceoryx2-backed local pub/sub.
- `RemoteTransport` — iroh-backed remote pub/sub.
- `AsyncPublisher` / `AsyncSubscriber` — `async fn` shims over the
  sync core, for callers running in a tokio runtime.

See `examples/local_pose.rs` and `examples/remote_loopback.rs` for
direct usage.

## Development shells (Nix)

```text
nix develop              # stable toolchain (default)
nix develop .#nightly    # nightly toolchain for forward-compat checks
```

The shells export `LIBCLANG_PATH` and `LD_LIBRARY_PATH` so
iceoryx2's `bindgen` step finds libclang + the C++ runtime. CI
installs `libclang-dev` for the same reason.

## Cargo features

`quicbit` ships with iceoryx2 and iroh always on — there are no
feature flags for the transports. The only optional knobs:

| Feature   | Adds                                                              |
|-----------|-------------------------------------------------------------------|
| `tracing` | structured events at accept / connect / disconnect / lag / errors |
| `config`  | service-discovery config files (TOML / JSON)                      |

So `cargo build` / `cargo test` / `cargo run --example <name>`
just work — no `--features ...` needed.

## C / Python bindings

The previous custom-SHM C ABI and Python bindings were retired in
the iceoryx2 migration. If you need them back, the cleanest path
is a thin shim around iceoryx2's own C bindings; happy to revisit
on request.

## Status

Pre-1.0 (`0.0.x`). The wire format and public API are documented
but **not stable** between minor releases. See
[`PLAN.md`](PLAN.md) for the production-readiness roadmap and
[`LIMITATIONS.md`](LIMITATIONS.md) for the current known sharp
edges.

## See also

- [`iceoryx2`](https://github.com/eclipse-iceoryx/iceoryx2) —
  zero-copy SHM IPC; the substrate of `LocalTransport`.
- [`iroh`](https://github.com/n0-computer/iroh) — peer-to-peer
  QUIC with built-in NAT traversal; the wire for the remote
  transport.
- [`wirebit`](https://codeberg.org/robolibs/wirebit) — the bus
  simulator / HIL bridge used as quicbit's test substrate.
