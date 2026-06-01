//! Datapod gallery across the high-level three-letter primitives.
//!
//! This keeps everything in one local system-DID namespace, so the client
//! side never names a transport peer. It just asks for data on a topic:
//! `pub/sub`, `req/res`, `que/ans`, `put/ack`, and `pip` all route by
//! `(system_did, topic)`.
//!
//! ```text
//! cargo run --example datapod_primitives_gallery
//! ```

use std::time::{Duration, Instant};

use datapod::{Aabb, Bytes, Linestring, Odom, Point, Pose, Quaternion, Twist, Velocity, Wrench};
use quicbit::{LocalConfig, Node, TopicQos, did_key::endpoint_id_to_did_key};

#[datapod::datapod]
struct UploadAck {
    chunks: u32,
    bytes: u64,
    checksum: u64,
}

fn main() -> quicbit::Result<()> {
    let system_did = endpoint_id_to_did_key(&iroh::SecretKey::generate().public());
    let cfg = LocalConfig {
        history_depth: 8,
        subscriber_buffer: 8,
        max_payload_bytes: 2 * 1024 * 1024,
        ..LocalConfig::default()
    };

    let server_node = Node::builder()
        .no_relay()
        .system_did(&system_did)
        .local_config(cfg)
        .bind()?;
    let client_node = Node::builder().no_relay().system_did(&system_did).bind()?;

    println!("system did: {system_did}");
    pub_sub_odom(&server_node, &client_node)?;
    req_res_pose_to_wrench(&server_node, &client_node)?;
    que_ans_bbox_to_path(&server_node, &client_node)?;
    put_ack_byte_chunks(&server_node, &client_node)?;
    pip_twist_to_pose(&server_node, &client_node)?;
    Ok(())
}

fn pub_sub_odom(server_node: &Node, client_node: &Node) -> quicbit::Result<()> {
    println!("== pub/sub: Odom state ==");
    let qos = TopicQos::latest().with_subscriber_queue(4);
    let mut pubr = server_node.publisher_with_qos::<Odom>("state/odom", qos)?;
    let mut sub = client_node.subscribe_with_qos::<Odom>("state/odom", qos)?;

    pubr.send(&Odom {
        pose: pose(1.0, 2.0, 0.0),
        twist: twist(0.8, 0.0, 0.05),
    })?;

    let odom = poll_for(Duration::from_secs(2), || sub.take().ok().flatten())
        .ok_or_else(|| quicbit::Error::Timeout(Duration::from_secs(2)))?;
    let h = odom.header();
    println!(
        "  odom pose=({:.1}, {:.1}, {:.1}) vx={:.1}",
        h.pose.point.x, h.pose.point.y, h.pose.point.z, h.twist.linear.vx
    );
    Ok(())
}

fn req_res_pose_to_wrench(server_node: &Node, client_node: &Node) -> quicbit::Result<()> {
    println!("== req/res: Pose -> Wrench ==");
    let mut server = server_node.req_server::<Pose, Wrench>("control/wrench")?;
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let (req, reply) = server.take().unwrap()?;
            let p = req.header().point;
            reply
                .respond(&Wrench::from_components(
                    p.x * 10.0,
                    p.y * 10.0,
                    p.z * 10.0,
                    0.0,
                    0.0,
                    0.1,
                ))
                .unwrap();
            Some(())
        })
        .expect("req/res server should receive pose");
    });

    let mut client = client_node.req::<Pose, Wrench>("control/wrench")?;
    let wrench = client.call(&pose(0.2, -0.1, 0.4))?;
    println!(
        "  wrench force=({:.1}, {:.1}, {:.1})",
        wrench.header().force.x,
        wrench.header().force.y,
        wrench.header().force.z
    );
    handle.join().unwrap();
    Ok(())
}

fn que_ans_bbox_to_path(server_node: &Node, client_node: &Node) -> quicbit::Result<()> {
    println!("== que/ans: Aabb -> Linestring answers ==");
    let qos = TopicQos::reliable()
        .with_chunk_bytes(4096)
        .with_max_message_bytes(1024 * 1024);
    let mut server = server_node.ans_with_qos::<Aabb, Linestring>("planner/path", qos)?;
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let (bbox, mut ans) = server.take().unwrap()?;
            let min = bbox.header().min_point;
            let max = bbox.header().max_point;
            ans.send(&Linestring::new(vec![
                Point::new(min.x, min.y, 0.0),
                Point::new((min.x + max.x) * 0.5, (min.y + max.y) * 0.5, 0.0),
                Point::new(max.x, max.y, 0.0),
            ]))
            .unwrap();
            ans.finish().unwrap();
            Some(())
        })
        .expect("que/ans server should receive bbox");
    });

    let mut client = client_node.que_with_qos::<Aabb, Linestring>("planner/path", qos)?;
    let mut answers = client.send(&Aabb::new(
        Point::new(-2.0, -1.0, 0.0),
        Point::new(4.0, 3.0, 0.0),
    ))?;
    while let Some(path) = answers.next()? {
        let points: &[Point] = bytemuck::cast_slice(path.payload());
        println!("  path answer: {} points", points.len());
        assert_eq!(points.len(), 3);
    }
    handle.join().unwrap();
    Ok(())
}

fn put_ack_byte_chunks(server_node: &Node, client_node: &Node) -> quicbit::Result<()> {
    println!("== put/ack: Bytes chunks -> UploadAck ==");
    let qos = TopicQos::reliable()
        .with_chunk_bytes(4096)
        .with_max_message_bytes(1024 * 1024);
    let mut server = server_node.ack_with_qos::<Bytes, UploadAck>("bag/upload", qos)?;
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let mut puts = server.take().unwrap()?;
            let mut chunks = 0;
            let mut bytes = 0;
            let mut checksum = 0;
            while let Some(chunk) = puts.next().unwrap() {
                chunks += 1;
                bytes += chunk.payload().len() as u64;
                checksum += chunk.payload().iter().map(|b| *b as u64).sum::<u64>();
            }
            puts.ack(&UploadAck {
                chunks,
                bytes,
                checksum,
            })
            .unwrap();
            Some(())
        })
        .expect("put/ack server should receive upload");
    });

    let chunk_a: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let chunk_b: Vec<u8> = (0..48 * 1024).map(|i| (255 - (i % 251)) as u8).collect();
    let mut client = client_node.put_with_qos::<Bytes, UploadAck>("bag/upload", qos)?;
    let mut put = client.open()?;
    put.send(&Bytes::from_slice(&chunk_a))?;
    put.send(&Bytes::from_slice(&chunk_b))?;
    let ack = put.finish()?;
    println!(
        "  ack chunks={} bytes={} checksum={}",
        ack.header().chunks,
        ack.header().bytes,
        ack.header().checksum
    );
    assert_eq!(ack.header().chunks, 2);
    assert_eq!(ack.header().bytes, (chunk_a.len() + chunk_b.len()) as u64);
    handle.join().unwrap();
    Ok(())
}

fn pip_twist_to_pose(server_node: &Node, client_node: &Node) -> quicbit::Result<()> {
    println!("== pip: Twist commands <-> Pose updates ==");
    let mut server = server_node.pip_server::<Twist, Pose>("session/integrate")?;
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let mut pip = server.take().unwrap()?;
            let mut x = 0.0;
            while let Some(cmd) = pip.next().unwrap() {
                x += cmd.header().linear.vx;
                pip.send(&pose(x, 0.0, 0.0)).unwrap();
            }
            pip.finish_send().unwrap();
            Some(())
        })
        .expect("pip server should receive session");
    });

    let mut client = client_node.pip::<Twist, Pose>("session/integrate")?;
    let mut pip = client.open()?;
    for vx in [0.25, 0.5, 1.0] {
        pip.send(&twist(vx, 0.0, 0.0))?;
    }
    pip.finish_send()?;
    while let Some(update) = pip.next()? {
        println!("  pose update x={:.2}", update.header().point.x);
    }
    handle.join().unwrap();
    Ok(())
}

fn pose(x: f64, y: f64, z: f64) -> Pose {
    Pose {
        point: Point::new(x, y, z),
        rotation: Quaternion::identity(),
    }
}

fn twist(vx: f64, vy: f64, wz: f64) -> Twist {
    Twist {
        linear: Velocity { vx, vy, vz: 0.0 },
        angular: Velocity {
            vx: 0.0,
            vy: 0.0,
            vz: wz,
        },
    }
}

fn poll_for<R>(timeout: Duration, mut f: impl FnMut() -> Option<R>) -> Option<R> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(r) = f() {
            return Some(r);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}
