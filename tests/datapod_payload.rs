//! Round-trip datapod types through quicbit's pub/sub.
//!
//! Verifies that the `bytemuck::Pod` derives added to datapod's
//! spatial / temporal / sugar types let them ride on quicbit's
//! local SHM transport without a serializer in the middle.


use std::time::Duration;

use datapod::spatial::primitives::Triangle;
use datapod::spatial::robot::{Inertial, JointLimits, Odom, Twist, Wrench};
use datapod::spatial::{Acceleration, Aabb, BoundingSphere, Euler, GaussianPoint, Size};
use datapod::temporal::Stamp;
use datapod::{Point, Pose, Quaternion, Velocity};
use quicbit::Node;

fn poll_for<R>(timeout: Duration, mut f: impl FnMut() -> Option<R>) -> Option<R> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Some(r) = f() {
            return Some(r);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

#[test]
fn datapod_point_round_trip() {
    let p_node = Node::builder().no_relay().identity("sensors").bind().unwrap();
    let s_node = Node::builder().no_relay().identity("planner").bind().unwrap();

    let mut pubr = p_node.publisher::<Point>("imu/position").unwrap();
    let mut sub = s_node
        .subscriber::<Point>("sensors", "imu/position")
        .unwrap();

    pubr.send(Point { x: 1.0, y: 2.0, z: 3.0 }).unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("sample");
    assert_eq!(got.x, 1.0);
    assert_eq!(got.y, 2.0);
    assert_eq!(got.z, 3.0);
}

#[test]
fn datapod_pose_round_trip() {
    let p_node = Node::builder().no_relay().identity("loc").bind().unwrap();
    let s_node = Node::builder().no_relay().identity("nav").bind().unwrap();

    let mut pubr = p_node.publisher::<Pose>("rover/pose").unwrap();
    let mut sub = s_node.subscriber::<Pose>("loc", "rover/pose").unwrap();

    let p = Pose {
        point: Point { x: 10.0, y: 20.0, z: 0.0 },
        rotation: Quaternion::identity(),
    };
    pubr.send(p).unwrap();

    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("sample");
    assert_eq!(got.point.x, 10.0);
    assert_eq!(got.point.y, 20.0);
    assert_eq!(got.rotation.w, 1.0);
}

#[test]
fn datapod_compound_payload_works() {
    // A user-defined POD that nests datapod types — the whole
    // composite is Pod by virtue of every leaf field being Pod.
    use bytemuck::{Pod, Zeroable};
use iceoryx2::prelude::ZeroCopySend;

    #[repr(C)]
    #[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq, ZeroCopySend)]
    struct Telemetry {
        stamp: Stamp,
        pose: Pose,
        velocity: Velocity,
    }

    let p_node = Node::builder().no_relay().identity("rover").bind().unwrap();
    let s_node = Node::builder().no_relay().identity("logger").bind().unwrap();

    let mut pubr = p_node.publisher::<Telemetry>("rover/telemetry").unwrap();
    let mut sub = s_node
        .subscriber::<Telemetry>("rover", "rover/telemetry")
        .unwrap();

    let t = Telemetry {
        stamp: Stamp { nanos: 1_700_000_000_000_000_000 },
        pose: Pose {
            point: Point { x: 5.0, y: -2.0, z: 0.5 },
            rotation: Quaternion::identity(),
        },
        velocity: Velocity { vx: 0.5, vy: 0.0, vz: 0.0 },
    };
    pubr.send(t).unwrap();

    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("sample");
    assert_eq!(got.stamp.nanos, 1_700_000_000_000_000_000);
    assert_eq!(got.pose.point.x, 5.0);
    assert_eq!(got.velocity.vx, 0.5);
}

/// Spot-check that the Pod derives on the spatial subfolders
/// (gaussian, primitives, robot) actually compile and round-trip.
#[test]
fn datapod_subfolder_types_round_trip() {
    let p_node = Node::builder().no_relay().identity("pubr").bind().unwrap();
    let s_node = Node::builder().no_relay().identity("subr").bind().unwrap();

    // Twist (robot)
    let mut pubr = p_node.publisher::<Twist>("rover/twist").unwrap();
    let mut sub = s_node.subscriber::<Twist>("pubr", "rover/twist").unwrap();
    pubr.send(Twist {
        linear: Velocity { vx: 1.0, vy: 0.0, vz: 0.0 },
        angular: Velocity { vx: 0.0, vy: 0.0, vz: 0.5 },
    })
    .unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("twist");
    assert_eq!(got.linear.vx, 1.0);
    assert_eq!(got.angular.vz, 0.5);
}

/// Compile-only sanity: every newly-Pod type should be usable as
/// a quicbit payload. We don't actually publish here — the test
/// passes if it builds.
#[test]
fn newly_pod_types_are_local_payload() {
    fn assert_payload<T: quicbit::LocalPayload>() {}
    assert_payload::<Point>();
    assert_payload::<Pose>();
    assert_payload::<Quaternion>();
    assert_payload::<Euler>();
    assert_payload::<Velocity>();
    assert_payload::<Acceleration>();
    assert_payload::<Aabb>();
    assert_payload::<BoundingSphere>();
    assert_payload::<Size>();
    assert_payload::<Stamp>();
    assert_payload::<GaussianPoint>();
    assert_payload::<Triangle>();
    assert_payload::<Twist>();
    assert_payload::<Wrench>();
    assert_payload::<Odom>();
    assert_payload::<Inertial>();
    assert_payload::<JointLimits>();
}
