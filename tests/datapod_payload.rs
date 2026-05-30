//! Round-trip datapod types through quicbit's pub/sub.
//!
//! Verifies that datapod's fixed-Pod types ride on quicbit's local
//! SHM transport without a serializer in the middle.

use std::time::Duration;

use datapod::{
    Aabb, Acceleration, BoundingSphere, Euler, GaussianPoint, Inertial, JointLimits, Odom, Point,
    Pose, Quaternion, Size, Triangle, Twist, Velocity, Wrench,
};
use quicbit::{LocalConfig, LocalService};

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

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{stem}_{pid}_{nanos}")
}

#[test]
fn datapod_point_round_trip() {
    let svc = LocalService::<Point>::create(&unique_name("imu_position"), LocalConfig::default())
        .unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    pubr.send(&Point {
        x: 1.0,
        y: 2.0,
        z: 3.0,
    })
    .unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("sample");
    let h = got.header();
    assert_eq!(h.x, 1.0);
    assert_eq!(h.y, 2.0);
    assert_eq!(h.z, 3.0);
}

#[test]
fn datapod_pose_round_trip() {
    let svc =
        LocalService::<Pose>::create(&unique_name("rover_pose"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    let p = Pose {
        point: Point {
            x: 10.0,
            y: 20.0,
            z: 0.0,
        },
        rotation: Quaternion::identity(),
    };
    pubr.send(&p).unwrap();

    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("sample");
    let h = got.header();
    assert_eq!(h.point.x, 10.0);
    assert_eq!(h.point.y, 20.0);
    assert_eq!(h.rotation.w, 1.0);
}

#[test]
fn datapod_compound_payload_works() {
    // A user-defined fixed-Pod that nests datapod types — annotate
    // with `#[datapod::datapod]` so the whole composite participates
    // in the wire contract.
    #[datapod::datapod]
    struct Telemetry {
        pose: Pose,
        velocity: Velocity,
    }

    let svc =
        LocalService::<Telemetry>::create(&unique_name("rover_telemetry"), LocalConfig::default())
            .unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();

    let t = Telemetry {
        pose: Pose {
            point: Point {
                x: 5.0,
                y: -2.0,
                z: 0.5,
            },
            rotation: Quaternion::identity(),
        },
        velocity: Velocity {
            vx: 0.5,
            vy: 0.0,
            vz: 0.0,
        },
    };
    pubr.send(&t).unwrap();

    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("sample");
    let h = got.header();
    assert_eq!(h.pose.point.x, 5.0);
    assert_eq!(h.velocity.vx, 0.5);
}

/// Spot-check that a robot-domain type (Twist) round-trips through
/// quicbit.
#[test]
fn datapod_subfolder_types_round_trip() {
    let svc =
        LocalService::<Twist>::create(&unique_name("rover_twist"), LocalConfig::default()).unwrap();
    let mut pubr = svc.publisher().unwrap();
    let mut sub = svc.subscriber().unwrap();
    pubr.send(&Twist {
        linear: Velocity {
            vx: 1.0,
            vy: 0.0,
            vz: 0.0,
        },
        angular: Velocity {
            vx: 0.0,
            vy: 0.0,
            vz: 0.5,
        },
    })
    .unwrap();
    let got = poll_for(Duration::from_secs(2), || sub.take().unwrap()).expect("twist");
    let h = got.header();
    assert_eq!(h.linear.vx, 1.0);
    assert_eq!(h.angular.vz, 0.5);
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
    assert_payload::<GaussianPoint>();
    assert_payload::<Triangle>();
    assert_payload::<Twist>();
    assert_payload::<Wrench>();
    assert_payload::<Odom>();
    assert_payload::<Inertial>();
    assert_payload::<JointLimits>();
}
