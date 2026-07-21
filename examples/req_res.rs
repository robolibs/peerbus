//! Unified req/res — one request, one response — over **both**
//! transports: same-host shared memory and iroh QUIC.
//!
//! ```text
//! cargo run --example req_res
//! ```
//!
//! `local_shm` routes two `Node`s on the same host through shared
//! memory. `over_iroh` dials a standalone `RemoteTransport` server
//! (which has no SHM service) so the call crosses the network stack —
//! the same path used between separate hosts.

use std::time::{Duration, Instant};

use peerbus::{Node, RemoteTransport};

#[datapod::datapod]
struct Add {
    a: i32,
    b: i32,
}

#[datapod::datapod]
struct Sum {
    value: i32,
}

fn main() -> peerbus::Result<()> {
    local_shm()?;
    over_iroh()?;
    Ok(())
}

/// Same-host: server + client are two `Node`s; routing is SHM.
fn local_shm() -> peerbus::Result<()> {
    println!("== req/res over shared memory ==");
    let server_node = Node::builder().no_relay().ephemeral().label("calc").bind()?;
    let client_node = Node::builder().no_relay().ephemeral().label("caller").bind()?;

    let mut server = server_node.req_server::<Add, Sum>("calc/add")?;
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut answered = 0;
        while Instant::now() < deadline && answered < 3 {
            match server.take() {
                Ok(Some((req, reply))) => {
                    let add = req.header();
                    reply
                        .respond(&Sum {
                            value: add.a + add.b,
                        })
                        .unwrap();
                    answered += 1;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => {}
            }
        }
    });

    let mut client = client_node.req_client::<Add, Sum>(server_node.endpoint_id(), "calc/add")?;
    for i in 1..=3 {
        let res = client.call(&Add { a: i, b: 100 })?;
        println!("  {i} + 100 = {}", res.header().value);
    }
    handle.join().unwrap();
    Ok(())
}

/// Cross-stack: a standalone `RemoteTransport` server (no SHM) answered
/// by a `Node` client, so the exchange goes over iroh QUIC.
fn over_iroh() -> peerbus::Result<()> {
    println!("== req/res over iroh ==");
    let server = RemoteTransport::builder("calc/add")
        .no_relay()
        .build_blocking()?;
    server.wait_for_direct_addresses(Duration::from_secs(5))?;
    server.serve_requests::<Add, Sum, _>(|req| Sum {
        value: req.a + req.b,
    })?;

    let client_node = Node::builder().ephemeral().no_relay().bind()?;
    let mut client = client_node.req_client::<Add, Sum>(server.endpoint_addr(), "calc/add")?;
    for i in 1..=3 {
        let res = client.call(&Add { a: i, b: 200 })?;
        println!("  {i} + 200 = {}", res.header().value);
    }
    Ok(())
}
