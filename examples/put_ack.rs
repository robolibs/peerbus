//! put/ack — zero-or-more uploads, then one acknowledgement — over
//! **both** shared memory and iroh QUIC.
//!
//! ```text
//! cargo run --example put_ack
//! ```

use std::time::{Duration, Instant};

use quicbit::{Node, RemoteTransport};

#[datapod::datapod]
struct LogChunk {
    value: u32,
}

#[datapod::datapod]
struct UploadAck {
    count: u32,
    sum: u32,
}

fn main() -> quicbit::Result<()> {
    local_shm()?;
    over_iroh()?;
    Ok(())
}

/// Same-host: routing is SHM between two `Node`s.
fn local_shm() -> quicbit::Result<()> {
    println!("== put/ack over shared memory ==");
    let server_node = Node::builder().no_relay().identity("sink").bind()?;
    let client_node = Node::builder().no_relay().identity("uploader").bind()?;

    let mut server = server_node.ack::<LogChunk, UploadAck>("logs/upload")?;
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match server.take() {
                Ok(Some(mut puts)) => {
                    let mut count = 0;
                    let mut sum = 0;
                    while let Some(chunk) = puts.next().unwrap() {
                        count += 1;
                        sum += chunk.header().value;
                    }
                    puts.ack(&UploadAck { count, sum }).unwrap();
                    break;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => {}
            }
        }
    });

    let mut client = client_node.put_client::<LogChunk, UploadAck>("sink", "logs/upload")?;
    let mut upload = client.open()?;
    for value in [3, 14, 15, 92] {
        upload.send(&LogChunk { value })?;
    }
    let ack = upload.finish()?;
    println!(
        "  server acked {} chunks summing to {}",
        ack.header().count,
        ack.header().sum
    );
    handle.join().unwrap();
    Ok(())
}

/// Cross-stack: standalone `RemoteTransport` put/ack server over iroh.
fn over_iroh() -> quicbit::Result<()> {
    println!("== put/ack over iroh ==");
    let server = RemoteTransport::builder("logs/upload")
        .no_relay()
        .build_blocking()?;
    server.wait_for_direct_addresses(Duration::from_secs(5))?;
    server.serve_uploads::<LogChunk, UploadAck, _>(|puts| UploadAck {
        count: puts.len() as u32,
        sum: puts.iter().map(|p| p.value).sum(),
    })?;

    let client_node = Node::builder().no_relay().bind()?;
    let mut client =
        client_node.put_client::<LogChunk, UploadAck>(server.endpoint_addr(), "logs/upload")?;
    let mut upload = client.open()?;
    for value in [1, 2, 3, 4, 5] {
        upload.send(&LogChunk { value })?;
    }
    let ack = upload.finish()?;
    println!(
        "  server acked {} chunks summing to {}",
        ack.header().count,
        ack.header().sum
    );
    Ok(())
}
