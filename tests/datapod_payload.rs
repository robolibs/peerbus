//! Round-trip datapod types through peerbus's pub/sub.
//!
//! Verifies that datapod's fixed-Pod types ride on peerbus's local
//! SHM transport without a serializer in the middle.

use std::time::Duration;

use datapod::{
    Aabb, Acceleration, BoundingSphere, DataPod, Encoding, Euler, GaussianPoint, Grid, Inertial,
    JointLimits, Odom, Point, Pose, Quaternion, Size, Triangle, Twist, Velocity, Wrench,
};
use peerbus::{DatapodMsg, LocalConfig, LocalService, Node};

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
/// peerbus.
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

#[test]
fn datapod_msg_uses_datapod_canonical_wire_message() {
    let grid = Grid::new(
        2,
        2,
        Encoding::Rgba8,
        0.5,
        false,
        Pose::default(),
        (0_u8..16).collect(),
    );
    let msg = DatapodMsg::from_datapod(&grid);
    let canonical = datapod::to_wire_message(&grid);
    assert_eq!(msg.type_hash, canonical.type_hash);
    assert_eq!(msg.wire, canonical.bytes);

    let decoded: Grid = msg.to_datapod().unwrap();
    assert_eq!(decoded.rows, 2);
    assert_eq!(decoded.cols, 2);
    assert_eq!(decoded.encoding, Encoding::Rgba8);
    assert_eq!(decoded.payload_bytes(), &(0_u8..16).collect::<Vec<_>>());
}

#[test]
fn node_datapod_msg_dynamic_view_reads_builtin_grid_without_callsite_type() {
    let pub_identity = unique_name("datapod_msg_pub");
    let topic = unique_name("datapod/msg/grid");
    let pub_node = Node::builder()
        .no_relay()
        .identity(&pub_identity)
        .bind()
        .expect("publisher node");
    let sub_node = Node::builder()
        .no_relay()
        .identity(unique_name("datapod_msg_sub"))
        .bind()
        .expect("subscriber node");

    let mut pubr = pub_node.publisher::<DatapodMsg>(&topic).unwrap();
    let mut sub = sub_node
        .subscriber::<DatapodMsg>(pub_identity.as_str(), &topic)
        .unwrap();

    let grid = Grid::new(
        2,
        3,
        Encoding::Rgba8,
        0.25,
        false,
        Pose::default(),
        (0_u8..24).collect(),
    );
    let canonical = datapod::to_wire_message(&grid);
    pubr.send_datapod_wire(canonical.type_hash, canonical.bytes.clone())
        .unwrap();

    let sample = poll_for(Duration::from_secs(2), || sub.take_datapod_view().unwrap())
        .expect("generic datapod sample");
    assert_eq!(sample.type_hash(), canonical.type_hash);
    assert_eq!(sample.wire(), canonical.bytes.as_slice());

    let view = sample.dynamic().unwrap();
    assert_eq!(view.schema().canonical_name, "datapod.grid.v1");
    assert_eq!(view.get_u32("rows").unwrap(), 2);
    assert_eq!(view.get_u32("cols").unwrap(), 3);
    assert_eq!(view.get_f64("resolution").unwrap(), 0.25);
    assert_eq!(view.payload(), &(0_u8..24).collect::<Vec<_>>());
}

#[test]
fn node_req_res_can_exchange_generic_datapod_msg_and_dynamic_views() {
    let server_identity = unique_name("datapod_req_server");
    let client_identity = unique_name("datapod_req_client");
    let topic = unique_name("datapod/req");
    let server_node = Node::builder()
        .no_relay()
        .identity(&server_identity)
        .bind()
        .expect("server node");
    let client_node = Node::builder()
        .no_relay()
        .identity(client_identity)
        .bind()
        .expect("client node");

    let response_grid = Grid::new(
        1,
        2,
        Encoding::Rgba8,
        0.5,
        false,
        Pose::default(),
        (100_u8..108).collect(),
    );
    let response_wire = datapod::to_wire_message(&response_grid);
    let mut server = server_node
        .req_server::<DatapodMsg, DatapodMsg>(&topic)
        .unwrap();
    let handle = std::thread::spawn(move || {
        poll_for(Duration::from_secs(2), || {
            let (req, reply) = server.take().unwrap()?;
            let request_view = req.dynamic().unwrap();
            assert_eq!(request_view.schema().canonical_name, "datapod.grid.v1");
            assert_eq!(request_view.get_u32("rows").unwrap(), 2);
            assert_eq!(request_view.get_u32("cols").unwrap(), 3);
            reply
                .respond(&DatapodMsg::new(
                    response_wire.type_hash,
                    response_wire.bytes.clone(),
                ))
                .unwrap();
            Some(())
        })
        .expect("server should receive generic datapod req");
    });

    let request_grid = Grid::new(
        2,
        3,
        Encoding::Rgba8,
        0.25,
        false,
        Pose::default(),
        (0_u8..24).collect(),
    );
    let request_wire = datapod::to_wire_message(&request_grid);
    let mut client = client_node
        .req_client::<DatapodMsg, DatapodMsg>(server_identity.as_str(), &topic)
        .unwrap();
    let res = client
        .call(&DatapodMsg::new(
            request_wire.type_hash,
            request_wire.bytes.clone(),
        ))
        .unwrap();
    let response_view = res.dynamic().unwrap();
    assert_eq!(response_view.schema().canonical_name, "datapod.grid.v1");
    assert_eq!(response_view.get_u32("rows").unwrap(), 1);
    assert_eq!(response_view.get_u32("cols").unwrap(), 2);
    assert_eq!(response_view.payload(), &(100_u8..108).collect::<Vec<_>>());
    handle.join().unwrap();
}

/// Compile-only sanity: every newly-Pod type should be usable as
/// a peerbus payload. We don't actually publish here — the test
/// passes if it builds.
#[test]
fn newly_pod_types_are_local_payload() {
    fn assert_payload<T: peerbus::LocalPayload>() {}
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
