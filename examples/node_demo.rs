//! Minimum-ceremony pub/sub with identity-by-name.
//!
//! ```text
//! cargo run --example node_demo
//! ```
//!
//! No key files. No URLs. No EndpointIds in user code. Two
//! strings: who I am, who I'm subscribed to.

use std::time::{Duration, Instant};

use quicbit::Node;

#[datapod::datapod]
struct Pose {
    x: f32,
    y: f32,
    yaw: f32,
}

fn main() -> quicbit::Result<()> {
    // One process running both sides for the demo. In the real
    // world these are separate binaries — same code, just separate
    // identities.
    let pub_node = Node::builder().no_relay().identity("rover-a").bind()?;
    let sub_node = Node::builder().no_relay().identity("planner").bind()?;

    let mut pubr = pub_node.publisher::<Pose>("rover/pose")?;
    let mut sub = sub_node.subscriber::<Pose>("rover-a", "rover/pose")?;

    let publisher = std::thread::spawn(move || {
        for i in 0..5 {
            pubr.send(&Pose {
                x: i as f32,
                y: 2.0 * i as f32,
                yaw: 0.1 * i as f32,
            })
            .unwrap();
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let subscriber = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut seen = 0;
        while Instant::now() < deadline && seen < 5 {
            match sub.take() {
                Ok(Some(s)) => {
                    println!("received: {:?}", s.header());
                    seen += 1;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => {}
            }
        }
    });

    publisher.join().unwrap();
    subscriber.join().unwrap();
    Ok(())
}
