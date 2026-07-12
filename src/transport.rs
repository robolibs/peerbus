//! Transport abstractions.
//!
//! peerbus ships two transports today: [`crate::local::LocalTransport`]
//! (local SHM) and [`crate::remote::RemoteTransport`] (iroh QUIC).
//! Both implement the [`Transport`] trait so higher layers can be
//! generic over them.
//!
//! Every payload `T` shipped over a peerbus transport implements
//! [`datapod::DataPod`]. That trait provides:
//!
//! * A Pod **header** `T::Header` — fixed size, lives in the wire's
//!   metadata slot.
//! * An optional byte **payload** `T::Payload` (= `()` or `[u8]`) —
//!   variable-length data carrying the heap bytes for types like
//!   `Polygon`, `Grid`, `Linestring`, etc.
//!
//! For fixed-Pod types (e.g. `Point`, `Pose`, `Joint`) `T::Header = T`,
//! so the entire value rides in the header and the payload is empty.

use crate::error::Result;

/// FNV-1a (64-bit). Used internally to hash type-layout descriptors
/// for the wire handshake.
pub fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Hash a type's wire identity from its **name plus its memory layout**
/// (`type_name` + `size_of` + `align_of`), FNV-1a folded.
///
/// This hash is the transport-level type guard: a publisher and a
/// subscriber that disagree on it are rejected with
/// [`Error::TypeMismatch`](crate::error::Error::TypeMismatch) instead of
/// silently exchanging bytes.
///
/// # Why `type_name` is mixed in
///
/// This function used to hash *only* size + align, on the theory that
/// layout is stable across rustc versions while `type_name` is not. That
/// made the guard useless: any two unrelated types with the same size and
/// alignment produced the *same* hash. `struct Pose { x: f32, y: f32, yaw:
/// f32 }` and `struct Rgb { r: f32, g: f32, b: f32 }` are both 12 bytes,
/// align 4 — a `Pose` publisher and an `Rgb` subscriber would connect
/// cleanly and reinterpret each other's bytes. That is silent data
/// corruption, which is strictly worse than a loud handshake failure.
/// Layout alone simply is not a type identity.
///
/// # The trade-off, stated honestly
///
/// Mixing in `core::any::type_name::<T>()` costs the stability the old
/// comment was chasing. The hash is **no longer invariant** across:
///
/// 1. **rustc versions that change type-name formatting.** The exact
///    string `type_name` returns is explicitly not a stable guarantee, so
///    two peers built with different toolchains *could* disagree on the
///    hash for the same type and refuse to talk.
/// 2. **Renaming or moving a type.** `app::Pose` and `app::geom::Pose`
///    hash differently even with identical fields.
///
/// Both are acceptable, and (2) is arguably correct:
///
/// * A renamed or relocated type **is** a different contract. Peers built
///   from different versions of the type definitions should not silently
///   interoperate — that is the exact failure this hash exists to catch.
/// * The normal deployment is a set of peers built from the same source
///   revision with the same toolchain, where both invariants hold
///   trivially. Such peers are entirely unaffected.
/// * A refused connection is diagnosable in seconds. Corrupted data
///   flowing through a robot's control loop is not.
///
/// **Requirement:** peers must be built from the same type definitions
/// (and, to be safe, the same rustc version). peerbus is pre-1.0 and its
/// wire formats are explicitly unstable; this is not a compatibility
/// regression it promises against.
///
/// # Cross-language identity is a separate thing
///
/// This hash does **not** participate in C/Python interop. The
/// language-neutral schema identity is datapod's canonical type hash
/// (derived from `DataPod::CANONICAL_NAME` / datapod's global registry)
/// and it rides *inside* the message as
/// [`DatapodMsg::n`](crate::datapod_msg::DatapodMsg). `wire_type_hash`
/// only identifies the Rust-side transport slot. Changing it therefore
/// cannot break Rust/C/Python agreement.
///
/// A `T: DataPod` bound would let us use `CANONICAL_NAME` here and get a
/// truly language-neutral, rename-proof identity — but it is not
/// applicable at every call site: the local ring services hash wrapper
/// types (`Envelope<T::Header>`, `AnsEnvelope<T::Header>`, …) and the
/// standalone remote transport hashes plain `T: Pod`. Neither is a
/// `DataPod`. See the crate docs for the follow-up.
pub fn wire_type_hash<T>() -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    // Name first: this is what actually distinguishes types.
    for b in std::any::type_name::<T>().as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    // Layout still folded in: catches a same-named type whose fields
    // changed shape between two builds (e.g. a field added behind a
    // feature flag), which the name alone would miss.
    for byte in (std::mem::size_of::<T>() as u64).to_le_bytes() {
        h ^= byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    for byte in (std::mem::align_of::<T>() as u64).to_le_bytes() {
        h ^= byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Marker trait for the local-transport payload bound. Adds a
/// `Send + Sync` bound to `T::Header` so loans/samples carrying a
/// `T::Header` value across threads (e.g. via `tokio::spawn_blocking`)
/// type-check. `Pod` types are always thread-safe in practice;
/// stating it here lets us be generic over `T: LocalPayload` without
/// repeating the bound at every impl site.
pub trait LocalPayload: datapod::DataPod<Header: Send + Sync> + 'static {}
impl<T> LocalPayload for T
where
    T: datapod::DataPod + 'static,
    T::Header: Send + Sync,
{
}

/// Marker trait for the remote-transport (iroh) payload bound.
pub trait RemotePayload:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

impl<T> RemotePayload for T where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

/// Operations a publisher handle must support. Loans are
/// fixed-shape "header + variable-byte payload" containers; the
/// publisher writes both halves in place and hands the loan back
/// via [`publish`](PublisherOps::publish).
pub trait PublisherOps<T: LocalPayload>: Send {
    type Loan: Send;

    /// Reserve a slot with `byte_count` payload bytes. Pass 0 for
    /// fixed-Pod types (`T::Payload = ()`); pass the cast-byte
    /// length for heap-bearing types (`T::Payload = [u8]`).
    fn loan(&mut self, byte_count: usize) -> Result<Self::Loan>;

    /// Hand the loan over to subscribers. Returns a transport-defined
    /// sequence number (monotonically increasing).
    fn publish(&mut self, loan: Self::Loan) -> Result<u64>;
}

/// Operations a subscriber handle must support.
pub trait SubscriberOps<T: LocalPayload>: Send {
    type Sample: Send;

    /// Non-blocking take. `Ok(None)` means "no new sample".
    fn take(&mut self) -> Result<Option<Self::Sample>>;
}

/// A transport — the thing that owns slots/streams and hands out
/// typed publishers and subscribers.
pub trait Transport: Send + Sync {
    type Publisher<T: LocalPayload>: PublisherOps<T>;
    type Subscriber<T: LocalPayload>: SubscriberOps<T>;

    fn publisher<T: LocalPayload>(&self) -> Result<Self::Publisher<T>>;
    fn subscriber<T: LocalPayload>(&self) -> Result<Self::Subscriber<T>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two unrelated types with IDENTICAL size (12) and alignment (4).
    // Under the old layout-only hash these collided exactly, so a `Pose`
    // publisher and an `Rgb` subscriber passed the type guard and
    // reinterpreted each other's bytes.
    #[repr(C)]
    struct Pose {
        x: f32,
        y: f32,
        yaw: f32,
    }

    #[repr(C)]
    struct Rgb {
        r: f32,
        g: f32,
        b: f32,
    }

    // Same size/align again, but different field *types* — layout is
    // still indistinguishable.
    #[repr(C)]
    struct Counters {
        a: u32,
        b: u32,
        c: u32,
    }

    #[test]
    fn identical_layout_distinct_types_do_not_collide() {
        // Precondition: the three types really are layout-identical, so
        // this test is exercising the collision the fix targets and not
        // accidentally passing because the layouts differ.
        assert_eq!(std::mem::size_of::<Pose>(), 12);
        assert_eq!(std::mem::size_of::<Rgb>(), 12);
        assert_eq!(std::mem::size_of::<Counters>(), 12);
        assert_eq!(std::mem::align_of::<Pose>(), 4);
        assert_eq!(std::mem::align_of::<Rgb>(), 4);
        assert_eq!(std::mem::align_of::<Counters>(), 4);

        let pose = wire_type_hash::<Pose>();
        let rgb = wire_type_hash::<Rgb>();
        let counters = wire_type_hash::<Counters>();

        assert_ne!(pose, rgb, "Pose and Rgb must not share a wire type hash");
        assert_ne!(pose, counters);
        assert_ne!(rgb, counters);
    }

    #[test]
    fn same_type_hashes_consistently() {
        assert_eq!(wire_type_hash::<Pose>(), wire_type_hash::<Pose>());
        assert_eq!(wire_type_hash::<u64>(), wire_type_hash::<u64>());
    }

    #[test]
    fn distinct_generic_instantiations_do_not_collide() {
        // The local ring services hash wrapper types like
        // `Envelope<T::Header>`. Two wrappers over layout-identical but
        // distinct headers must stay distinguishable.
        struct Wrapper<T>(#[allow(dead_code)] T);

        assert_ne!(
            wire_type_hash::<Wrapper<Pose>>(),
            wire_type_hash::<Wrapper<Rgb>>(),
        );
        // ...and the wrapper is not confusable with its own payload.
        assert_ne!(wire_type_hash::<Wrapper<Pose>>(), wire_type_hash::<Pose>());
    }

    #[test]
    fn primitives_of_equal_layout_do_not_collide() {
        // u64 / i64 / f64: all 8 bytes, align 8. Previously identical.
        assert_ne!(wire_type_hash::<u64>(), wire_type_hash::<i64>());
        assert_ne!(wire_type_hash::<u64>(), wire_type_hash::<f64>());
        assert_ne!(wire_type_hash::<i64>(), wire_type_hash::<f64>());
    }

    #[test]
    fn fnv1a64_is_unchanged() {
        // The string helper is used for topic/name hashing elsewhere and
        // is deliberately untouched by this fix.
        assert_eq!(fnv1a64(""), 0xcbf29ce484222325);
        assert_ne!(fnv1a64("a"), fnv1a64("b"));
    }
}
