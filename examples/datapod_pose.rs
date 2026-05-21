//! Publish + subscribe a `datapod::Pose` end-to-end.
//!
//! ```text
//! cargo run --example datapod_pose
//! ```
//!
//! Two `Node`s in one process, both `no_relay`. The publisher
//! simulates a slowly-moving robot reporting its pose every 100 ms
//! over the topic `"rover/pose"`. The subscriber attaches by the
//! publisher's identity name and prints what it sees.
//!
//! Because `datapod::Pose` derives `Pod + ZeroCopySend`, it goes
//! straight into / out of the SHM slot — no serialization step,
//! no schema metadata on the wire. Both sides just `use
//! datapod::Pose;` and the Rust type system enforces agreement.

use std::thread;
use std::time::{Duration, Instant};

use datapod::{Point, Pose, Quaternion};
use quicbit::Node;

fn main() -> quicbit::Result<()> {
    let pub_node = Node::builder().no_relay().identity("rover-a").bind()?;
    let sub_node = Node::builder().no_relay().identity("planner").bind()?;

    let mut pubr = pub_node.publisher::<Pose>("rover/pose")?;
    let mut sub = sub_node.subscriber::<Pose>("rover-a", "rover/pose")?;

    println!(
        "publisher \"rover-a\"   EndpointId={}",
        pub_node.endpoint_id()
    );
    println!(
        "subscriber \"planner\"  EndpointId={}",
        sub_node.endpoint_id()
    );

    let publisher = thread::spawn(move || {
        for i in 0..5 {
            let pose = Pose {
                point: Point {
                    x: i as f64,
                    y: 0.5 * i as f64,
                    z: 0.0,
                },
                // Spin slowly around z; identity at i=0.
                rotation: Quaternion::new(
                    (0.05 * i as f64).cos(),
                    0.0,
                    0.0,
                    (0.05 * i as f64).sin(),
                ),
            };
            pubr.send(pose).expect("publish");
            thread::sleep(Duration::from_millis(100));
        }
    });

    let subscriber = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut seen = 0u32;
        while Instant::now() < deadline && seen < 5 {
            match sub.take() {
                Ok(Some(s)) => {
                    println!(
                        "got pose:  ({:.2}, {:.2}, {:.2})   q=({:.3}, {:.3}, {:.3}, {:.3})",
                        s.point.x,
                        s.point.y,
                        s.point.z,
                        s.rotation.w,
                        s.rotation.x,
                        s.rotation.y,
                        s.rotation.z,
                    );
                    seen += 1;
                }
                Ok(None) => thread::sleep(Duration::from_millis(10)),
                Err(_) => {}
            }
        }
    });

    publisher.join().unwrap();
    subscriber.join().unwrap();
    Ok(())
}
