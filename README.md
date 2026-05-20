# quicbit

`quicbit` is a Rust **typed zero-copy messaging** library for robotics,
with two transports behind one service-oriented API:

- **Local (same host)** — loan-publish-consume pub/sub over POSIX
  shared memory. Publisher writes the payload *in place* into an SHM
  slot, hands over a pointer; subscribers read the same bytes. No
  serialization, no copy across the process boundary.
- **Remote (across hosts)** — pub/sub over [iroh] peer-to-peer QUIC.
  Peers are identified by ed25519 `EndpointId`s rather than
  IP\:port, NAT hole punching + relay fallback are handled by iroh,
  TLS 1.3 is mandatory (and PKI-free — the `SecretKey` *is* the
  identity). Topics map to QUIC streams; back-pressure is the
  stream's own flow control.

A `quicbit::Service` is the same thing to callers regardless of where
subscribers live — local-only, remote-only, or a mixed fan-out.

[iroh]: https://github.com/n0-computer/iroh

## What quicbit is NOT

- **Not a simulator.** [`wirebit`](https://codeberg.org/robolibs/wirebit)
  is the bus simulator + HIL bridge; quicbit *uses* it as a test
  substrate, not as its runtime transport.
- **Not ROS.** No CDR, no DDS, no ROS graph. Interop with ROS 2 is a
  future sidecar (quicbit ⇄ Zenoh ⇄ DDS), not in-process.
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
                 │          Service               │   <- this crate
                 │  (pub/sub + req/resp, typed)   │
                 └───────────┬────────────────────┘
                             │
              ┌──────────────┴──────────────────┐
              │                                 │
     ┌────────▼─────────┐              ┌────────▼─────────┐
     │   LocalTransport │              │  RemoteTransport │
     │   (SHM slots)    │              │  (iroh / QUIC)   │
     └──────────────────┘              └──────────────────┘
       same-host,                         across hosts,
       true zero-copy,                    TLS 1.3, hole punching,
       lock-free                          per-topic streams
```

## Quick start

One entry point: `Node`. It owns the iroh endpoint, registers itself
in the host registry, and routes by topic.

```rust,ignore
use bytemuck::{Pod, Zeroable};
use quicbit::Node;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug)]
struct Pose { x: f32, y: f32, yaw: f32 }

let node = Node::builder().no_relay().bind()?;
let peer = node.endpoint_id();

let mut pubr = node.publisher::<Pose>("rover/pose")?;
let mut sub  = node.subscriber::<Pose>(peer, "rover/pose")?;

pubr.send(Pose { x: 1.0, y: 2.0, yaw: 0.1 })?;
if let Some(s) = sub.take()? {
    println!("pose: {:?}", *s);
}
# Ok::<_, quicbit::Error>(())
```

If the peer's `EndpointId` is registered on this host (i.e. it's a
sibling process on the same machine), the subscriber attaches to its
SHM segment and reads with zero copy. Otherwise it dials over iroh.
**Same call either way.**

## Development shells (Nix)

```text
nix develop              # stable toolchain (default)
nix develop .#nightly    # stable + miri
nix develop .#python     # stable + python312 + maturin
```

## Features

| Feature      | Adds                                                     |
|--------------|----------------------------------------------------------|
| *(default)*  | `local` — SHM loan/publish/consume pub/sub               |
| `remote`     | iroh peer-to-peer QUIC transport                         |
| `async`      | `AsyncPublisher` / `AsyncSubscriber` shims over the sync core |
| `tracing`    | tracing spans around loan / publish / consume            |
| `config`     | service-discovery config files (TOML / JSON)             |
| `python`     | pyo3 bindings                                            |

## Request / response

```rust,ignore
use quicbit::{LocalConfig, LocalReqRespService};

let svc = LocalReqRespService::<Ping, Pong>::create("calc", LocalConfig::default())?;
// Server thread:
let mut server = svc.server();
while let Some((req, reply)) = server.take_request()? {
    reply.respond(handle(&*req))?;
}
// Client thread:
let mut client = svc.client();
let pong: Pong = client.call(Ping { /* ... */ })?;
# Ok::<_, quicbit::Error>(())
```

## Lower-level building blocks

`Node` is built on top of `LocalTransport` (SHM) and
`RemoteTransport` (iroh). Both are public if you want direct
control; otherwise prefer `Node`. See `examples/local_pose.rs`
and `examples/remote_loopback.rs` for direct usage.

## C / Python

The C ABI is byte-oriented (publisher hands you a `*mut u8` of
`slot_size` bytes). See [`include/quicbit.h`](include/quicbit.h)
and `tests/ffi_smoke.rs` for usage.

Python bindings (via pyo3, abi3-py39) expose `Service`, `Publisher`,
`Subscriber` with the same byte-oriented model — see
`examples/python_smoke.py`. Build a wheel with:

```text
maturin build --release --features python-extension
```

## See also

- [`PLAN.md`](PLAN.md) — phased roadmap, ADRs, risks.
- [`iroh`](https://github.com/n0-computer/iroh) — peer-to-peer QUIC
  with built-in NAT traversal; the wire for the remote transport.
- [`wirebit`](https://codeberg.org/robolibs/wirebit) — the bus
  simulator / HIL bridge used as quicbit's test substrate.
