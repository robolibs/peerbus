//! Compile-time assertions that every public handle has the
//! `Send` / `Sync` shape we promise.
//!
//! If a future refactor adds a `Rc` / non-`Send` field, this file
//! fails to compile — the assertion lives at the type system, not
//! at runtime.

use bytemuck::{Pod, Zeroable};
use quicbit::{
    Error, LocalConfig, LocalPublisher, LocalReqRespService, LocalRequestServer, LocalService,
    LocalSubscriber, LocalTransport, Loan, Sample,
};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Demo {
    a: u64,
    b: u64,
}

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}
fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn auto_trait_assertions() {
    // Low-level transport.
    assert_send_sync::<LocalTransport>();

    // Per-type local handles.
    assert_send_sync::<LocalService<Demo>>();
    assert_send_sync::<LocalPublisher<Demo>>();
    assert_send_sync::<LocalSubscriber<Demo>>();

    // RAII handles — must be Send so subscribers can drain on a
    // worker thread.
    assert_send::<Loan<Demo>>();
    assert_send::<Sample<Demo>>();
    assert_sync::<Loan<Demo>>();
    assert_sync::<Sample<Demo>>();

    // Req/resp wrappers.
    assert_send_sync::<LocalReqRespService<Demo, Demo>>();
    assert_send::<LocalRequestServer<Demo, Demo>>();

    // Error crosses thread boundaries via Result — Send + Sync.
    assert_send_sync::<Error>();
}

#[cfg(feature = "remote")]
#[test]
fn auto_trait_assertions_remote() {
    use quicbit::{Node, RemoteTransport, RemoteTransportBuilder};
    assert_send_sync::<RemoteTransport>();
    assert_send::<RemoteTransportBuilder>();
    // Node is the main user-facing entry point.
    assert_send_sync::<Node>();
}

#[cfg(feature = "async")]
#[test]
fn auto_trait_assertions_async() {
    use quicbit::{AsyncPublisher, AsyncSubscriber};
    // Must be Send so they can be moved into a tokio task. Sync
    // isn't required (the user holds &mut self for `await`).
    assert_send::<AsyncPublisher<Demo, LocalPublisher<Demo>>>();
    assert_send::<AsyncSubscriber<Demo, LocalSubscriber<Demo>>>();
}

const _: () = {
    fn check<T: Send + Sync + Clone>() {}
    let _ = check::<LocalConfig>;
};
