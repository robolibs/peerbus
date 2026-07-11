//! Compile-time auto-trait assertions.
//!
//! Local handles carry raw pointers into owned shared-memory mappings;
//! they're `Send` from the user's perspective (you only get one
//! mutable publisher/subscriber handle at a time).
//!
//! We keep the assertions that still hold (Node, Error,
//! LocalConfig) and let go of the ones the new backend doesn't
//! support.

use peerbus::{Error, LocalConfig};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn auto_trait_assertions() {
    // Error rides on `Result` and gets sent across threads.
    assert_send_sync::<Error>();
}

#[test]
fn auto_trait_assertions_remote() {
    use peerbus::{Node, RemoteTransport, RemoteTransportBuilder};
    assert_send_sync::<RemoteTransport>();
    fn assert_send<T: Send>() {}
    assert_send::<RemoteTransportBuilder>();
    // Node is the user-facing entry point — must be Send + Sync.
    assert_send_sync::<Node>();
}

const _: () = {
    fn check<T: Send + Sync + Clone>() {}
    let _ = check::<LocalConfig>;
};
