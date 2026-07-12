//! Loopback test for the iroh-backed req/res transport.

use std::time::Duration;

use bytemuck::{Pod, Zeroable};
use datapod::ZeroCopySend;
use peerbus::RemoteTransport;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq, ZeroCopySend)]
struct Add {
    a: i32,
    b: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq, ZeroCopySend)]
struct Sum {
    value: i32,
}

#[test]
fn remote_reqresp_call_roundtrip() {
    // Server side: bind, register a handler, no peer needed.
    let server_side = RemoteTransport::builder("test/calc")
        .no_relay()
        .build_blocking()
        .expect("server endpoint");
    server_side
        .wait_for_direct_addresses(Duration::from_secs(5))
        .expect("addresses");

    server_side
        .serve_requests::<Add, Sum, _>(|req| Sum {
            value: req.a + req.b,
        })
        .expect("register handler");

    let server_addr = server_side.endpoint_addr();

    // Client side: bind, dial the server.
    let client_side = RemoteTransport::builder("test/calc")
        .no_relay()
        .peer(server_addr)
        .build_blocking()
        .expect("client endpoint");

    let mut client = client_side.req_client::<Add, Sum>().expect("client");

    // Issue a handful of calls; verify each response.
    for i in 0..5 {
        let resp = client.call(Add { a: i, b: i * 2 }).unwrap();
        assert_eq!(
            resp,
            Sum { value: i + i * 2 },
            "wrong response for call {i}"
        );
    }
}
