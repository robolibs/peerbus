# quicbit

Typed zero-copy messaging for robotics. One API, two transports:

- **local SHM** when both ends are on the same host — shared-memory, no copy, no serialization.
- **iroh** when they aren't — peer-to-peer QUIC with NAT traversal and TLS 1.3.

The routing decision happens once at `subscriber()` and is invisible afterwards.

## Install

```toml
quicbit = { git = "https://codeberg.org/robolibs/quicbit" }
```

On Nix: `nix develop`. If `NVIDIA_VERSION` is detected, the shell's
`nixGL` / `nixVulkan` aliases target `nixGLNvidia`; otherwise they
fall back to `nixGLIntel` / `nixVulkanIntel`. The local backend is
pure Rust (`shared_memory` + `raw_sync`); with `datapod` 0.2.0 there is
no iceoryx2/libclang dependency in quicbit's Cargo graph.

## Publish and subscribe

```rust
use quicbit::Node;

#[datapod::datapod]
struct Pose { x: f32, y: f32, yaw: f32 }

let node = Node::builder().identity("rover-a").no_relay().bind()?;

let mut pubr = node.publisher::<Pose>("rover/pose")?;
let mut sub  = node.subscriber::<Pose>("rover-a", "rover/pose")?;

pubr.send(&Pose { x: 1.0, y: 2.0, yaw: 0.1 })?;
if let Some(s) = sub.take()? {
    println!("{:?}", s.header());
}
# Ok::<_, quicbit::Error>(())
```

Payload types implement `datapod::DataPod` — typically a one-line `#[datapod::datapod]` annotation. Fixed-size types ride entirely in the local SHM header / iroh frame prefix; heap-bearing types (one `#[dp(bytes)]` field) ride the variable-length payload too.

## System DID mode

For multi-process systems that together form one machine, join a
logical `did:key` system namespace and route by topic key:

```rust
let node = Node::builder()
    .system_did("did:key:z6MkSystem...")
    .bind()?;

let mut pubr = node.publisher::<Pose>("/state/pose")?;
let mut sub  = node.subscribe::<Pose>("/state/pose")?;
```

In this mode local SHM names derive from `system_did + topic`, not
from the process identity. Multiple processes can therefore use
different transport identities while joining the same system DID/topic
namespace. If the topic is not local, add an explicit remote route:

```rust
node.add_topic_route("/state/pose", publisher_endpoint_addr)?;
let mut sub = node.subscribe::<Pose>("/state/pose")?;
```

Or add a topic-agnostic system peer and let `subscribe(topic)` use the
same `(system_did, topic)` route key over iroh when SHM is absent:

```rust
node.add_system_peer(remote_system_endpoint_addr)?;
let mut sub = node.subscribe::<Pose>("/state/pose")?;
```

## Addressing a peer

A subscriber names *who* it's listening to. `IntoPeer` accepts:

| Form                              | Routes locally? | Use when                                 |
|-----------------------------------|-----------------|------------------------------------------|
| `"rover-a"` (string)              | yes             | Trusted LAN — string hashes to a key.    |
| `did:key:z6Mk…` (W3C DID:KEY)     | yes             | Sharing a public key out-of-band.        |
| `EndpointId` / `EndpointAddr`     | yes             | Already holding the iroh object.         |

All three resolve to the same 32-byte Ed25519 key. `Node::endpoint_did_key()` prints your own identity in DID:KEY form.

For cross-host you need the publisher's full `EndpointAddr`, not just an identifier:

```rust
let pub_addr = pub_node.endpoint_addr();          // share this with peers
let mut sub  = sub_node.subscriber::<Pose>(pub_addr, "rover/pose")?;
```

## Your own identity

```rust
.identity("rover-a")              // string → impersonable, dev only
.identity_env("ROVER_ID")         // string from env var
.identity_file("/etc/rover.key")  // 32 raw bytes on disk, mode 0600
```

`identity_file` is the only path that can't be impersonated. Mode is re-enforced (`0600`) on every read.

## Limiting who can dial in

```rust
let node = Node::builder()
    .identity_file("/etc/rover.key")
    .allow_peer(planner_id)
    .allow_peer(logger_id)
    .bind()?;
```

Without `allow_peer`, any peer that knows the ALPN can subscribe. Once one is set, every other connection is closed immediately after the QUIC handshake.

## Request / response

```rust
use quicbit::{LocalConfig, LocalReqRespService};

let svc = LocalReqRespService::<Ping, Pong>::create("calc", LocalConfig::default())?;

// server
let mut server = svc.server()?;
while let Some((req, reply)) = server.take_request()? {
    reply.respond(&handle(req.header()))?;
}

// client
let mut client = svc.client()?;
let resp = client.call(&Ping { /* … */ })?;
let pong: Pong = *resp.header();
# Ok::<_, quicbit::Error>(())
```

Correlated by `req_id` on the shared `Envelope<H>` wire format.

## Observability

```rust
node.stats();    // publisher_topics, cached_peers
pubr.stats();    // published, remote_dropped
sub.stats();     // received, disconnects
```

Enable the `tracing` feature for structured events on accept / connect / disconnect / handshake mismatch / broadcast lag / poisoned-mutex recovery. The loan-publish-take hot path stays uninstrumented.

## When things go wrong

- **Connection drops.** The subscriber loop redials with bounded backoff (100 ms → 10 s). `take()` only returns `Err(Disconnected)` after the foreground channel itself goes away.
- **Slow subscriber.** The local ring's default `history_depth = 1` means a subscriber that polls slower than the publisher misses samples. Bump `LocalConfig::history_depth`.
- **No subscriber attached.** Remote publishes silently drop; counted under `remote_dropped` on `pubr.stats()`.

## Cargo features

| Feature   | Adds                                                              |
|-----------|-------------------------------------------------------------------|
| `tracing` | structured events at accept / connect / disconnect / lag / errors |
| `config`  | service-discovery config files (TOML / JSON)                      |

The local SHM backend and iroh are always on; there is no feature gate for either transport.

## Lower-level building blocks

`Node` is the recommended entry point. The pieces it composes are also public:

- `LocalTransport` / `LocalService<T>` — local SHM pub/sub directly.
- `RemoteTransport` — iroh pub/sub directly.
- `AsyncPublisher` / `AsyncSubscriber` — `async fn` shims over the sync core.
- `did_key::endpoint_id_to_did_key` / `did_key_to_endpoint_id` — DID:KEY adapter (delegates to [`authbox`](https://codeberg.org/robolibs/authbox)).

See `examples/` for direct usage. The GUI video subscriber
(`video_sub`) is intentionally built with minifb's **Wayland-only**
backend; run it from a Wayland session (`WAYLAND_DISPLAY` must be set).
It will not fall back to Xorg/XWayland.

## Status

`0.0.x` — pre-1.0. Wire format documented in [`PLAN.md`](PLAN.md), not stable between minor releases. Sharp edges in [`LIMITATIONS.md`](LIMITATIONS.md).
