# Changelog

## [0.4.0] - 2026-07-12

### <!-- 0 -->⛰️  Features

- Async, deny-by-default ACL, recv, close

### <!-- 1 -->🐛 Bug Fixes

- Release GIL, put/ack poll surface
- Panic guards, handle safety, parity
- Chunk DoS, net timeouts, type-hash
- SIGBUS guard and self-healing reclaim

### <!-- 2 -->🚜 Refactor

- Promote reqres over reqresp

### <!-- 7 -->⚙️ Miscellaneous Tasks

- Drop dead config feature, CI, docs, versions

## [0.3.2] - 2026-07-11

### <!-- 0 -->⛰️  Features

- Rename the crate, C header, and Python module from quicbit to peerbus
- Expand the C ABI and pyo3 bindings across all five modes
- Add zero-copy sample views (`take_view` / `DatapodSubscriber.take_view`) exposing read-only `memoryview` payload and wire bytes
- Expose `peer_path_diagnostics` and inbound peer allowlisting through the Python and C bindings
- Add a dedicated `reqres` module and export `DatapodSample`, `AnsStream`, and `PutSenderToken`

### <!-- 7 -->⚙️ Miscellaneous Tasks

- Move the `datapod` dependency from a local path to git tag 0.4.1

## [0.3.1] - 2026-06-07

### <!-- 0 -->⛰️  Features

- Add `DatapodMsg` and a generic datapod binding path for cross-language pub/sub and item modes
- Add Python video publisher and subscriber examples over the datapod `Grid` path
- Rework the Rust video publisher/subscriber examples onto the datapod path

## [0.3.0] - 2026-06-01

### <!-- 0 -->⛰️  Features

- Expand C and Python FFI for all modes and add examples
- Add C and Python API bindings with opaque messages

## [0.2.0] - 2026-05-30

### <!-- 0 -->⛰️  Features

- Add req/res, que/ans, put/ack, pip item modes
- Rename and simplify local SHM example
- Migrate local transport to pure-Rust shared-memory ring

### <!-- 1 -->🐛 Bug Fixes

- Improve video publisher and subscriber resilience
- Correctly handle large payloads and subscriber stream errors

## [0.1.0] - 2026-05-29

### <!-- 0 -->⛰️  Features

- Add tracing and improved example setup
- Add video publishing and subscribing examples
- Add DID:KEY support for EndpointId routing
- Add changelog generation and topic validation
- Adopt datapod::DataPod for unified payloads
- Introduce Node API for simplified usage
- Move local SHM off iceoryx2 onto peerbus's pure-Rust shared-memory ring
- Initial release of peerbus messaging library
