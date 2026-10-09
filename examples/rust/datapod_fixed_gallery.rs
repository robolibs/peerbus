//! Fixed-size datapod gallery over the high-level `Node` pub/sub API.
//!
//! This example uses several built-in datapod robotics/geometry types inside
//! one user-defined datapod. There is no serialization step: the composite
//! header is POD and rides directly through peerbus.
//!
//! ```text
//! cargo run --example datapod_fixed_gallery
//! ```

use std::time::{Duration, Instant};

use datapod::{
    Aabb, BoundingSphere, JointLimits, Point, Pose, Quaternion, Twist, Velocity, Wrench,
};
use peerbus::{Node, TopicQos};

#[datapod::datapod]
struct RobotSnapshot {
    pose: Pose,
    twist: Twist,
    wrench: Wrench,
    workspace: Aabb,
    keepout: BoundingSphere,
    lift_limits: JointLimits,
}

fn main() -> peerbus::Result<()> {
    let pub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .label(unique("gallery-pub"))
        .bind()?;
    let sub_node = Node::builder()
        .no_relay()
        .ephemeral()
        .label(unique("gallery-sub"))
        .bind()?;

    let mut pubr =
        pub_node.publisher_with_qos::<RobotSnapshot>("robot/snapshot", TopicQos::reliable())?;
    let mut sub = sub_node.subscriber_with_qos::<RobotSnapshot>(
        pub_node.endpoint_id(),
        "robot/snapshot",
        TopicQos::reliable(),
    )?;

    pubr.send(&RobotSnapshot {
        pose: Pose {
            point: Point::new(1.0, 2.0, 0.25),
            rotation: Quaternion::identity(),
        },
        twist: Twist {
            linear: Velocity {
                vx: 0.7,
                vy: 0.0,
                vz: 0.0,
            },
            angular: Velocity {
                vx: 0.0,
                vy: 0.0,
                vz: 0.15,
            },
        },
        wrench: Wrench::from_components(10.0, 0.0, -3.0, 0.0, 0.2, 0.0),
        workspace: Aabb::new(Point::new(-5.0, -5.0, 0.0), Point::new(5.0, 5.0, 3.0)),
        keepout: BoundingSphere {
            center: Point::new(2.0, 0.0, 0.0),
            radius: 0.75,
        },
        lift_limits: JointLimits {
            lower: -0.2,
            upper: 1.8,
            effort: 200.0,
            velocity: 0.6,
        },
    })?;

    let sample = poll_for(Duration::from_secs(2), || sub.take().ok().flatten())
        .ok_or_else(|| peerbus::Error::Timeout(Duration::from_secs(2)))?;
    let h = sample.header();
    println!(
        "pose=({:.2}, {:.2}, {:.2}) linear_vx={:.2} force_z={:.2}",
        h.pose.point.x, h.pose.point.y, h.pose.point.z, h.twist.linear.vx, h.wrench.force.z
    );
    println!(
        "workspace volume={:.2} keepout radius={:.2} lift=[{:.2}, {:.2}]",
        h.workspace.volume(),
        h.keepout.radius,
        h.lift_limits.lower,
        h.lift_limits.upper
    );

    Ok(())
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

fn unique(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{stem}-{pid}-{nanos}")
}
