//! Two-endpoint iroh loopback demo.
//!
//! Run with `cargo run --features remote --example remote_loopback`.
//! Spins up two `RemoteTransport`s, one publishing and one
//! subscribing, both on the loopback interface with relays disabled.
//! Demonstrates the Phase 3 wire protocol end-to-end.

#[cfg(feature = "remote")]
use std::thread;
#[cfg(feature = "remote")]
use std::time::Duration;

#[cfg(feature = "remote")]
use bytemuck::{Pod, Zeroable};
#[cfg(feature = "remote")]
use quicbit::transport::{PublisherOps, SubscriberOps};
#[cfg(feature = "remote")]
use quicbit::{RemoteTransport, Transport};

#[cfg(feature = "remote")]
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug)]
struct Tick {
    seq: u32,
    payload: u32,
}

#[cfg(feature = "remote")]
fn main() {
    let publisher_side = RemoteTransport::builder("demo/tick")
        .no_relay()
        .build_blocking()
        .expect("publisher endpoint");
    publisher_side
        .wait_for_direct_addresses()
        .expect("addresses");
    let publisher_addr = publisher_side.endpoint_addr();
    println!("publisher endpoint id: {}", publisher_side.endpoint_id());

    let subscriber_side = RemoteTransport::builder("demo/tick")
        .no_relay()
        .peer(publisher_addr)
        .build_blocking()
        .expect("subscriber endpoint");

    let mut pubr = publisher_side.publisher::<Tick>().expect("publisher");
    let mut sub = subscriber_side.subscriber::<Tick>().expect("subscriber");

    // Give the bi stream time to come up; then publish ten ticks.
    let consumer = thread::spawn(move || {
        for _ in 0..200 {
            if let Some(sample) = sub.take().unwrap() {
                println!("got: {:?}", *sample);
            }
            thread::sleep(Duration::from_millis(20));
        }
    });

    for i in 1..=10 {
        let mut loan = pubr.loan().expect("loan");
        *loan = Tick {
            seq: i,
            payload: i * 100,
        };
        pubr.publish(loan).expect("publish");
        thread::sleep(Duration::from_millis(100));
    }

    consumer.join().unwrap();
}

#[cfg(not(feature = "remote"))]
fn main() {
    eprintln!("This example requires --features remote");
}
