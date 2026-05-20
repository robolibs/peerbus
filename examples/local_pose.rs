//! Same-host loan/publish/consume demo.
//!
//! Run with `cargo run --example local_pose`. Spawns a thread that
//! subscribes and prints incoming `Pose` samples while the main
//! thread publishes a handful.

use std::thread;
use std::time::Duration;

use bytemuck::{Pod, Zeroable};
use quicbit::{Error, LocalConfig, LocalService};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug)]
struct Pose {
    x: f32,
    y: f32,
    yaw: f32,
}

fn main() -> Result<(), Error> {
    let name = format!("example-pose-{}", std::process::id());
    let svc = LocalService::<Pose>::create(&name, LocalConfig::default())?;

    let mut pubr = svc.publisher();
    let sub_svc = svc.clone();
    let consumer = thread::spawn(move || {
        let mut sub = sub_svc.subscriber();
        for _ in 0..50 {
            if let Some(sample) = sub.take().unwrap() {
                println!("pose: {:?} (seq={})", *sample, sample.sequence());
            }
            thread::sleep(Duration::from_millis(20));
        }
    });

    for i in 0..5 {
        pubr.send(Pose {
            x: i as f32,
            y: 2.0 * i as f32,
            yaw: 0.1 * i as f32,
        })?;
        thread::sleep(Duration::from_millis(50));
    }

    consumer.join().unwrap();
    Ok(())
}
