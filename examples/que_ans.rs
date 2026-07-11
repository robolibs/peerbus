//! que/ans — one query, zero-or-more answers, then done — over **both**
//! shared memory and iroh QUIC.
//!
//! ```text
//! cargo run --example que_ans
//! ```

use std::time::{Duration, Instant};

use peerbus::{Node, RemoteTransport};

#[datapod::datapod]
struct RangeQue {
    start: u32,
    count: u32,
}

#[datapod::datapod]
struct Hit {
    value: u32,
}

fn main() -> peerbus::Result<()> {
    local_shm()?;
    over_iroh()?;
    Ok(())
}

/// Same-host: routing is SHM between two `Node`s.
fn local_shm() -> peerbus::Result<()> {
    println!("== que/ans over shared memory ==");
    let server_node = Node::builder().no_relay().identity("search").bind()?;
    let client_node = Node::builder().no_relay().identity("seeker").bind()?;

    let mut server = server_node.ans::<RangeQue, Hit>("search/range")?;
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match server.take() {
                Ok(Some((que, mut ans))) => {
                    let q = *que.header();
                    for offset in 0..q.count {
                        ans.send(&Hit {
                            value: q.start + offset,
                        })
                        .unwrap();
                    }
                    ans.finish().unwrap();
                    break;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => {}
            }
        }
    });

    let mut client = client_node.que_client::<RangeQue, Hit>("search", "search/range")?;
    let mut answers = client.send(&RangeQue {
        start: 10,
        count: 4,
    })?;
    while let Some(hit) = answers.next()? {
        println!("  hit: {}", hit.header().value);
    }
    handle.join().unwrap();
    Ok(())
}

/// Cross-stack: standalone `RemoteTransport` que/ans server over iroh.
fn over_iroh() -> peerbus::Result<()> {
    println!("== que/ans over iroh ==");
    let server = RemoteTransport::builder("search/range")
        .no_relay()
        .build_blocking()?;
    server.wait_for_direct_addresses(Duration::from_secs(5))?;
    server.serve_ques::<RangeQue, Hit, _>(|q| {
        (0..q.count).map(|o| Hit { value: q.start + o }).collect()
    })?;

    let client_node = Node::builder().no_relay().bind()?;
    let mut client =
        client_node.que_client::<RangeQue, Hit>(server.endpoint_addr(), "search/range")?;
    let mut answers = client.send(&RangeQue {
        start: 50,
        count: 3,
    })?;
    while let Some(hit) = answers.next()? {
        println!("  hit: {}", hit.header().value);
    }
    Ok(())
}
