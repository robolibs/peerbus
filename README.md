# peerbus

Typed zero-copy messaging for robotics. One API, two transports:

- **local SHM** when both ends are on the same host — shared-memory, no copy, no serialization.
- **iroh** when they aren't — peer-to-peer QUIC with NAT traversal and TLS 1.3.

The routing decision happens once at `subscriber()` and is invisible afterwards.

## Modes at a glance

Five messaging modes, one `Node` API. They differ by how many messages
each side sends and how the peers relate:

```text
                     server sends ONE        server sends MANY
                   ┌──────────────────────┬──────────────────────┐
 client sends ONE  │       req/res        │       que/ans        │
                   │   1 req → 1 res      │   1 que → 0..N ans   │
                   ├──────────────────────┼──────────────────────┤
 client sends MANY │       put/ack        │         pip          │
                   │  0..N put → 1 ack    │  0..N ↔ 0..N (bidi)  │
                   └──────────────────────┴──────────────────────┘

 pub/sub  —  a producer's stream fans out to every subscriber (M : N)
```

| Mode      | client → server | server → client | flow       |
|-----------|:---------------:|:---------------:|------------|
| `pub/sub` | — (stream)      | broadcast       | 1-to-many  |
| `req/res` | 1               | 1               | 1-to-1     |
| `que/ans` | 1               | 0..N            | 1-to-many  |
| `put/ack` | 0..N            | 1               | many-to-1  |
| `pip`     | 0..N            | 0..N            | many↔many  |

**Peer model — the same for every mode.** A consumer addresses one
producer by `(identity, topic)`; any number of producers and consumers
can coexist on a topic across hosts, and each consumer attaches to the
one it names. So the modes are at parity on *how peers connect* — they
differ only in the **message flow** above and in **delivery**: pub/sub
delivers each producer message to **all** attached subscribers
(fan-out), while req/res, que/ans, put/ack, and pip give each client
its **own** independent exchange.

**Transport is orthogonal too.** Same host → shared memory (zero-copy);
remote → iroh QUIC. `Node` picks per peer at construction time; every
shape above is identical on both.

## Install

```toml
peerbus = { git = "https://codeberg.org/robolibs/peerbus" }
```

On Nix: `nix develop`. The local backend is pure Rust
(`shared_memory` + `raw_sync`) — no iceoryx2/libclang in the
dependency graph.

## Publish and subscribe

```rust
use peerbus::Node;

#[datapod::datapod]
struct Pose { x: f32, y: f32, yaw: f32 }

let node = Node::builder().identity("rover-a").no_relay().bind()?;

let mut pubr = node.publisher::<Pose>("rover/pose")?;
let mut sub  = node.subscriber::<Pose>("rover-a", "rover/pose")?;

pubr.send(&Pose { x: 1.0, y: 2.0, yaw: 0.1 })?;
if let Some(s) = sub.take()? {
    println!("{:?}", s.header());
}
# Ok::<_, peerbus::Error>(())
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

## Req/res

```rust
use peerbus::Node;

let server_node = Node::builder().identity("calc").bind()?;
let client_node = Node::builder().bind()?;

// server
let mut server = server_node.req_server::<Ping, Pong>("calc/ping")?;
while let Some((req, res)) = server.take()? {
    res.respond(&handle(req.header()))?;
}

// client
let mut client = client_node.req_client::<Ping, Pong>("calc", "calc/ping")?;
let res = client.call(&Ping { /* … */ })?;
let pong: Pong = *res.header();
# Ok::<_, peerbus::Error>(())
```

The high-level `Node` API chooses local SHM first and falls back to
iroh, matching pub/sub routing. Lower-level `LocalReqResService` and
`RemoteTransport` req/res APIs remain available for advanced use.

## Que/ans

For one query that returns zero or more finite answers, use `que/ans`:

```rust
let mut server = server_node.ans::<RangeQue, Hit>("search/range")?;
let mut client = client_node.que_client::<RangeQue, Hit>("search", "search/range")?;

// server
while let Some((que, mut ans)) = server.take()? {
    for value in lookup(que.header()) {
        ans.send(&Hit { value })?;
    }
    ans.finish()?;
}

// client
let mut answers = client.send(&RangeQue { start: 10, count: 3 })?;
while let Some(hit) = answers.next()? {
    println!("hit: {:?}", hit.header());
}
# Ok::<_, peerbus::Error>(())
```

Like pub/sub and req/res, `que_client(peer, topic)` tries local SHM first
and falls back to iroh. In system-DID mode, use `node.que::<Que, Ans>(topic)`
to route by `(system_did, topic)`.

## Put/ack

For a finite client-to-server item stream with one final acknowledgement,
use `put/ack`:

```rust
let mut server = server_node.ack::<LogChunk, UploadAck>("logs/upload")?;
let mut client = client_node.put_client::<LogChunk, UploadAck>("logger", "logs/upload")?;

// server
while let Some(mut puts) = server.take()? {
    let mut count = 0;
    while let Some(chunk) = puts.next()? {
        save(chunk.payload());
        count += 1;
    }
    puts.ack(&UploadAck { count })?;
}

// client
let mut put = client.open()?;
put.send(&chunk_a)?;
put.send(&chunk_b)?;
let ack = put.finish()?;
# Ok::<_, peerbus::Error>(())
```

`put_client(peer, topic)` chooses local SHM first and then iroh. In
system-DID mode, use `node.put::<Put, Ack>(topic)`.

## Pip

For a bidirectional session where both sides can exchange many messages,
use `pip`:

```rust
let mut server = server_node.pip_server::<ClientMsg, ServerMsg>("session")?;
let mut client = client_node.pip_client::<ClientMsg, ServerMsg>("robot", "session")?;

// server
while let Some(mut pip) = server.take()? {
    if let Some(msg) = pip.next()? {
        pip.send(&handle(msg.header()))?;
        pip.finish_send()?;
    }
}

// client
let mut pip = client.open()?;
pip.send(&ClientMsg { /* … */ })?;
while let Some(msg) = pip.next()? {
    handle_server_msg(msg.header());
}
# Ok::<_, peerbus::Error>(())
```

`pip_client(peer, topic)` chooses local SHM first and then iroh. In
system-DID mode, use `node.pip::<ClientMsg, ServerMsg>(topic)`.

## Observability

```rust
node.stats();    // publisher_topics, cached_peers
pubr.stats();    // published, remote_dropped, stale_dropped, bytes_sent, send_errors
sub.stats();     // received, disconnects, stale_dropped, incomplete_dropped, bytes_received
node.peer_path_diagnostics(peer); // direct/relay, RTT, MTU, datagram capacity
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

## Foreign-language bindings

peerbus follows the same robolibs binding layout as the sibling crates:

```text
src/ffi.rs                  # C ABI implementation
include/peerbus.h           # generated C header
src/python/mod.rs           # Python pyo3 module
examples/c_abi/             # C ABI demos + Makefile
examples/python_binding/    # Python demo + Makefile
```

Generate/build bindings with the root Makefile:

```sh
make bind
```

Run the C ABI demos:

```sh
make -C examples/c_abi run
```

Run the Python binding demo:

```sh
python examples/python_binding/demo.py
```

For language-independent datapod traffic, use the generic datapod API:
Python passes any datapod object with `to_wire_message()` to
`DatapodPublisher.send()`, and subscribers decode with the datapod type,
for example `sub.take(datapod.Grid)`. This works for datapod containers
such as `Grid` and `Matrix` without adding peerbus APIs per type.
For high-throughput subscribers, `Subscriber.take_view()` and
`DatapodSubscriber.take_view()` return borrowed sample objects that expose
read-only `memoryview` payload/wire bytes without copying.
Python also exposes `node.peer_path_diagnostics(endpoint_addr)` for remote
path checks; the C ABI mirrors it through `peerbus_node_peer_path_diagnostics`
and the `peerbus_peer_path_diagnostics_*` accessors.
Inbound peer allowlisting is also binding-visible: Python accepts
`Node(allowed_peers=[did_key_or_name, ...])`, and C uses
`PeerbusNodeConfig.allowed_peers` / `allowed_peers_len`.

The video examples use `datapod.Grid` (`Encoding.Rgba8`) over that generic
datapod path:

```sh
# Python publisher -> Rust Wayland subscriber
peerbus-video-pub
cargo run --release --example video_sub -- <did printed by Python>

# Rust publisher -> Python headless subscriber
cargo run --release --example video_pub
peerbus-video-sub <did printed by Rust>
```

## Topic QoS

High-rate topics can choose transport behavior without putting datatype logic
inside peerbus:

```rust
use peerbus::{TopicQos, DeliveryPolicy};

let qos = TopicQos::latest()
    .with_subscriber_queue(8)
    .with_max_message_bytes(64 * 1024 * 1024);

let mut pubr = node.publisher_with_qos::<DatapodMsg>("demo/video", qos)?;
```

`DeliveryPolicy::Reliable` is the default. Its publisher-side remote fanout
queue is bounded by `subscriber_queue`; if a reliable remote subscriber falls
behind that queue, peerbus reports lag instead of silently dropping old data.
`Latest` lets slow remote subscribers skip stale queued samples and receive
the newest sample instead.
For local SHM subscribers, `Latest`/`BestEffort` drain immediately-available
ring samples and return only the newest one, with skipped samples counted in
`sub.stats().stale_dropped`.
`BestEffort` uses iroh QUIC datagrams on the v3 Node pub/sub path when the
peer/path supports them; otherwise it falls back to latest-over-stream.
Node subscribers advertise these QoS fields in the pub/sub v3 handshake;
lower-level v2 peers remain accepted during the transition.
When both sides use the v3 Node pub/sub path, frames larger than
`chunk_bytes` are split and reassembled as opaque byte chunks.

### QoS for req/res, que/ans, put/ack, pip

The item modes accept the **byte-limit** subset of `TopicQos` via
`*_with_qos` constructors (`req_server_with_qos`, `que_client_with_qos`,
`ack_with_qos`, `pip_server_with_qos`, …). Large requests, answers,
puts, and pip messages are chunked and reassembled exactly like pub/sub,
lifting the 64 MiB single-frame cap (bounded by `max_inflight_bytes`).
These modes are always **Reliable** — `Latest`/`BestEffort` and the
datagram path are pub/sub-only, since dropping an in-flight item would
break their contract. Each server and client exposes remote-path
counters via `.stats()` (`ReqStats`/`QueStats`/`PutStats`/`PipStats`).

## Lower-level building blocks

`Node` is the recommended entry point. The pieces it composes are also public:

- `LocalTransport` / `LocalService<T>` — local SHM pub/sub directly.
- `RemoteTransport` — iroh directly, for all five modes without `Node`:
  pub/sub, req/res (`serve_requests`/`client`), que/ans
  (`serve_ques`/`que_client`), put/ack (`serve_puts`/`put_client`),
  and pip (`serve_pips`/`pip_client`). `Pod` payloads; interoperates
  with the `Node` path over iroh.
- `AsyncPublisher` / `AsyncSubscriber` — `async fn` shims over the sync core.
- `did_key::endpoint_id_to_did_key` / `did_key_to_endpoint_id` — DID:KEY adapter (delegates to [`authbox`](https://codeberg.org/robolibs/authbox)).

See `examples/` for runnable demos of every mode over **both**
transports: `req_res`, `que_ans`, `put_ack`, and `pip` each show a
shared-memory and an iroh section, with pub/sub in `node_demo` (SHM)
and `remote_loopback` (iroh). The `video_sub` GUI demo is Wayland-only
(minifb).

Datapod-focused examples:

- `datapod_pose` — minimal custom fixed-size datapod.
- `datapod_fixed_gallery` — built-in robotics/geometry datapods nested in one
  fixed-size user type over pub/sub.
- `datapod_heap_payloads` — heap-bearing `Bytes` and `Linestring` payloads
  with QoS/chunk settings.
- `datapod_primitives_gallery` — one system-DID namespace using different
  datapods across pub/sub, req/res, que/ans, put/ack, and pip.

## Status

`0.0.x` — pre-1.0. Wire format documented in [`PLAN.md`](PLAN.md), not stable between minor releases. Sharp edges in [`LIMITATIONS.md`](LIMITATIONS.md).
