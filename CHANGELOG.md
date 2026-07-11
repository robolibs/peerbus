# Changelog

## [0.3.2] - 2026-07-11

### <!-- 0 -->⛰️  Features

- More on phase 2

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
