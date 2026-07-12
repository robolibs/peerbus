//! Per-stream handshake exchanged at stream open.
//!
//! Two stream shapes:
//!
//! 1. **Pub/sub** (`HANDSHAKE_MAGIC` = "QBR1"):
//!
//!    ```text
//!    [u32 magic = HANDSHAKE_MAGIC]
//!    [u32 version = 2]
//!    [u64 type_hash]
//!    [u32 payload_size]
//!    [u16 topic_len][topic_bytes]
//!    [u32 length][payload bytes]   x N
//!    ```
//!
//!    Version 3 adds data-agnostic QoS before `topic_len`:
//!
//!    ```text
//!    [u64 max_message_bytes]
//!    [u64 max_inflight_bytes]
//!    [u32 chunk_bytes]
//!    [u8  delivery_policy]
//!    [u8  priority]
//!    ```
//!
//! 2. **Req/res** (`REQRESP_MAGIC` = "QBR2"):
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
//! 3. **Que/ans** (`QUEANS_MAGIC` = "QBA1"), **put/ack**
//!    (`PUTACK_MAGIC` = "QBP1"), and **pip** (`PIP_MAGIC` = "QBI1")
//!    use the same typed topic handshake shape as req/res, then
//!    exchange finite item streams with explicit done markers.
//!
//! The accept side discriminates on the leading `magic` so a single
//! iroh endpoint can multiplex pub/sub, req/res, que/ans, and put/ack
//! streams.

/// ASCII "QBR1" little-endian — pub/sub stream identifier.
pub const HANDSHAKE_MAGIC: u32 = 0x3152_4251;

/// ASCII "QBR2" little-endian — req/res stream identifier.
pub const REQRESP_MAGIC: u32 = 0x3252_4251;

/// ASCII "QBA1" little-endian — que/ans stream identifier.
pub const QUEANS_MAGIC: u32 = 0x3141_4251;

/// ASCII "QBP1" little-endian — put/ack stream identifier.
pub const PUTACK_MAGIC: u32 = 0x3150_4251;

/// ASCII "QBI1" little-endian — pip stream identifier.
pub const PIP_MAGIC: u32 = 0x3149_4251;

/// Stream protocol version. Bump on any breaking wire change.
///
/// History:
/// * `1` — type identity hashed from `std::any::type_name::<T>()`.
///   Unstable across rustc versions; retired.
/// * `2` — current. The wire *shape* below is unchanged, but the type
///   identity it carries is now hashed from
///   `(size_of::<T>(), align_of::<T>(), type_name::<T>())` via
///   [`crate::transport::wire_type_hash`], not size+align alone.
///   Size+align alone was toolchain-stable but *collision-prone*: two
///   unrelated types with the same size and alignment hashed
///   identically and could be interchanged silently. Distinct types
///   now produce distinct hashes, so a mismatched pair fails loudly
///   with `TypeMismatch` instead of exchanging garbage — at the cost
///   of the hash no longer being invariant across rustc versions that
///   reformat type names, or across renaming/moving a type. Peers must
///   be built from the same type definitions. A loud refusal is
///   strictly preferable to silent corruption.
///
/// The version number is NOT bumped for that change: the frame layout
/// is byte-identical, and a peer built against the old hashing already
/// fails loudly (`TypeMismatch`) rather than misparsing. Note also that
/// this constant doubles as the *legacy baseline* marker that the item
/// and pub/sub parsers compare against, so it must stay distinct from
/// [`ITEM_HANDSHAKE_VERSION_CHUNKED`] / [`PUBSUB_HANDSHAKE_VERSION_QOS`].
pub const HANDSHAKE_VERSION: u32 = 2;

/// Pub/sub handshake version carrying data-agnostic topic QoS.
///
/// This is only for `QBR1` pub/sub streams. Other stream families
/// continue using [`HANDSHAKE_VERSION`] until their own wire shape
/// changes.
pub const PUBSUB_HANDSHAKE_VERSION_QOS: u32 = 3;

/// Item-stream handshake version carrying data-agnostic byte limits
/// (`max_message_bytes`, `max_inflight_bytes`, `chunk_bytes`) for the
/// req/res, que/ans, put/ack, and pip families. Appended to the v2 tail
/// before `topic_len`. Version 2 (legacy, single-frame, 64 MiB cap) is
/// still accepted on the read path. Enables chunked large messages.
pub const ITEM_HANDSHAKE_VERSION_CHUNKED: u32 = 3;

/// Maximum topic name length on the wire.
pub const MAX_TOPIC_LEN: u16 = 1024;

/// Maximum single-message payload (64 MiB). Cap exists to defend
/// against a malicious peer asking us to allocate huge buffers while
/// still allowing the 4K RGBA video demo (~32 MiB/frame) over iroh.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;
