//! Per-stream handshake exchanged at stream open.
//!
//! Two stream shapes:
//!
//! 1. **Pub/sub** (`HANDSHAKE_MAGIC` = "QBR1"):
//!
//!    ```text
//!    [u32 magic = HANDSHAKE_MAGIC]
//!    [u32 version]
//!    [u64 type_hash]
//!    [u32 payload_size]
//!    [u16 topic_len][topic_bytes]
//!    [u32 length][payload bytes]   x N
//!    ```
//!
//! 2. **Request/response** (`REQRESP_MAGIC` = "QBR2"):
//!
//!    ```text
//!    [u32 magic = REQRESP_MAGIC]
//!    [u32 version]
//!    [u64 req_type_hash]
//!    [u64 resp_type_hash]
//!    [u32 req_size]
//!    [u32 resp_size]
//!    [u16 topic_len][topic_bytes]
//!    [u32 length][request bytes]
//!    -- server writes response on its send half --
//!    [u32 length][response bytes]
//!    ```
//!
//! The accept side discriminates on the leading `magic` so a single
//! iroh endpoint can multiplex pub/sub and req/resp streams.

/// ASCII "QBR1" little-endian — pub/sub stream identifier.
pub const HANDSHAKE_MAGIC: u32 = 0x3152_4251;

/// ASCII "QBR2" little-endian — req/resp stream identifier.
pub const REQRESP_MAGIC: u32 = 0x3252_4251;

/// Stream protocol version. Bump on any breaking wire change.
///
/// History:
/// * `1` — type identity hashed from `std::any::type_name::<T>()`.
///   Unstable across rustc versions; retired.
/// * `2` — type identity hashed from `(size_of::<T>(), align_of::<T>())`
///   via [`crate::transport::wire_type_hash`]. Stable across
///   toolchains; coarser (size+align collisions possible).
pub const HANDSHAKE_VERSION: u32 = 2;

/// Maximum topic name length on the wire.
pub const MAX_TOPIC_LEN: u16 = 1024;

/// Maximum single-message payload (64 MiB). Cap exists to defend
/// against a malicious peer asking us to allocate huge buffers while
/// still allowing the 4K RGBA video demo (~32 MiB/frame) over iroh.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;
