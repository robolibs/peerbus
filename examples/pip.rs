//! pip — a bidirectional session (echo loop here) — over **both**
//! shared memory and iroh QUIC.
//!
//! ```text
//! cargo run --example pip
//! ```
//!
//! Note: the `Node` pip API is fully interactive. The standalone
//! `RemoteTransport` pip server used in `over_iroh` is
//! collect-then-respond (it receives all client messages, then returns
//! all replies) — fine for an echo, but use the `Node` API for a truly
//! interactive session.

use std::time::{Duration, Instant};

use peerbus::{Node, RemoteTransport};

#[datapod::datapod]
struct ClientMsg {
    value: u32,
}

#[datapod::datapod]
struct ServerMsg {
    value: u32,
}

fn main() -> peerbus::Result<()> {
    local_shm()?;
    over_iroh()?;
    Ok(())
}

/// Same-host: routing is SHM between two `Node`s.
fn local_shm() -> peerbus::Result<()> {
    println!("== pip over shared memory ==");
    let server_node = Node::builder()
        .no_relay()
        .ephemeral()
        .label("session-srv")
        .bind()?;
    let client_node = Node::builder()
        .no_relay()
        .ephemeral()
        .label("session-cli")
        .bind()?;

    let mut server = server_node.pip_server::<ClientMsg, ServerMsg>("session/echo")?;
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match server.take() {
                Ok(Some(mut pip)) => {
                    while let Some(msg) = pip.next().unwrap() {
                        pip.send(&ServerMsg {
                            value: msg.header().value * 2,
                        })
                        .unwrap();
                    }
                    pip.finish_send().unwrap();
                    break;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => {}
            }
        }
    });

    let mut client =
        client_node.pip_client::<ClientMsg, ServerMsg>(server_node.endpoint_id(), "session/echo")?;
    let mut pip = client.open()?;
    for value in [1, 2, 3] {
        pip.send(&ClientMsg { value })?;
    }
    pip.finish_send()?;
    while let Some(reply) = pip.next()? {
        println!("  server echoed: {}", reply.header().value);
    }
    handle.join().unwrap();
    Ok(())
}

/// Cross-stack: standalone `RemoteTransport` pip server over iroh
/// (collect-then-respond).
fn over_iroh() -> peerbus::Result<()> {
    println!("== pip over iroh ==");
    let server = RemoteTransport::builder("session/echo")
        .no_relay()
        .build_blocking()?;
    server.wait_for_direct_addresses(Duration::from_secs(5))?;
    server.serve_pips::<ClientMsg, ServerMsg, _>(|msgs| {
        msgs.iter()
            .map(|m| ServerMsg { value: m.value * 2 })
            .collect()
    })?;

    let client_node = Node::builder().ephemeral().no_relay().bind()?;
    let mut client =
        client_node.pip_client::<ClientMsg, ServerMsg>(server.endpoint_addr(), "session/echo")?;
    let mut pip = client.open()?;
    for value in [10, 20, 30] {
        pip.send(&ClientMsg { value })?;
    }
    pip.finish_send()?;
    while let Some(reply) = pip.next()? {
        println!("  server echoed: {}", reply.header().value);
    }
    Ok(())
}
