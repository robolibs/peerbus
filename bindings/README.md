# quicbit bindings

C ABI and Python bindings for quicbit, mirroring the layout of the
sibling `maptrax` crate.

Both bindings speak **opaque byte messages**: every payload is a
`kind` tag (`u64`) plus a byte buffer, carried by the internal
`RawMsg` wire type. They expose **pub/sub** (publisher + subscriber) and
**req/res** (client + server) over both transports — shared memory on
the same host, iroh QUIC across hosts, chosen automatically per peer.
Because every node uses the one `RawMsg` wire type, C↔C, Python↔Python,
and C↔Python all interoperate.

> Talking to a *native* Rust `T` (a `#[datapod::datapod]` type) from the
> bindings is out of scope: that would require matching `T`'s type hash.
> The bindings are their own typed channel.

## C ABI

Header: [`include/quicbit.h`](../include/quicbit.h). The crate builds a
`cdylib` (`crate-type = ["rlib", "cdylib"]`).

```sh
cargo build --lib                         # produces target/debug/libquicbit.so
cc bindings/c/pubsub.c -Iinclude -Ltarget/debug -lquicbit -o /tmp/qb_pubsub
LD_LIBRARY_PATH=target/debug /tmp/qb_pubsub
```

Conventions: opaque `Box`-backed handles freed with the matching
`*_free`; fallible calls return `bool`/`int` with the reason in the
thread-local `quicbit_last_error_message()`; `QuicbitBytes` views borrow
memory owned by the message they came from. See
[`c/pubsub.c`](c/pubsub.c).

## Python

Built with [maturin](https://www.maturin.rs/) and pyo3 (abi3, py3.9+).
maturin is provided by the Nix dev shell (`flake.nix`); the
`[tool.maturin]` table in `pyproject.toml` enables the `python` feature.

```sh
nix develop                          # maturin + python on PATH
python -m venv .venv && source .venv/bin/activate
maturin develop                      # builds + installs the extension editable
python bindings/python/demo.py
```

`maturin build` instead produces a wheel under `target/wheels/`.

```python
import quicbit
node = quicbit.Node(identity="rover-a", no_relay=True)
pub  = node.publisher("rover/pose")
sub  = node.subscriber("rover-a", "rover/pose")
pub.send(b"\x01\x02", kind=7)
kind, data = sub.take()              # -> (kind, bytes) or None

# req/res
server = node.req_server("calc/double")
# in a thread: server.serve_one(lambda k, d: (k, bytes(b*2 for b in d)))
client = node.req_client("rover-a", "calc/double")
kind, data = client.call(b"\x01\x02\x03", kind=7)   # -> (7, b"\x02\x04\x06")
```

See [`python/demo.py`](python/demo.py).

## Scope

Implemented for both bindings: node lifecycle + `did:key`, pub/sub,
req/res. The streaming modes (que/ans, put/ack, pip) are reachable from
Rust and are straightforward to add here on the same `RawMsg` pattern.
