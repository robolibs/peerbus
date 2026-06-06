//! Single-pub / single-sub same-host throughput + latency bench.
//!
//! Hand-rolled — no `criterion` dependency. Reports:
//!
//! * Throughput (messages / second).
//! * Mean per-publish latency (publisher's `loan + write + publish`).
//! * Approximate end-to-end latency (publisher timestamp → subscriber
//!   take), measured via a `u64` field in the payload itself.
//!
//! Run with:
//!
//! ```text
//! cargo run --release --example bench_local
//! ```
//!
//! Defaults: 100,000 messages, 64-byte slots, history_depth=64.

use std::thread;
use std::time::{Duration, Instant};

use quicbit::{LocalConfig, LocalService};

#[datapod::datapod]
struct Sample {
    sent_nanos: u64,
    seq: u64,
    // 48 bytes of padding. Nested as [[u8; 24]; 2] because arrays longer
    // than 32 don't implement `Default`, which datapod 0.4.0's macro requires.
    _pad: [[u8; 24]; 2],
}

fn now_ns() -> u64 {
    // A monotonic-ish clock. We're measuring deltas; we don't care
    // about absolute time.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn main() {
    let total: u64 = std::env::var("QUICBIT_BENCH_MSGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    let name = format!("quicbit_bench_{}_{}", std::process::id(), now_ns());
    let svc = LocalService::<Sample>::create(
        &name,
        LocalConfig {
            max_publishers: 2,
            max_subscribers: 2,
            subscriber_buffer: 64,
            history_depth: 64,
            ..LocalConfig::default()
        },
    )
    .expect("create");

    let mut sub = svc.subscriber().expect("subscriber");
    let svc_for_pub = svc.clone();

    // Subscriber thread: drains samples until it sees a sentinel
    // (seq == total) or 5 seconds pass with no progress.
    let consumer = thread::spawn(move || {
        let mut received: u64 = 0;
        let mut dropped: u64 = 0;
        let mut total_latency_ns: u128 = 0;
        let mut empties: u64 = 0;
        let mut last_progress = Instant::now();
        let mut last_seen_seq: u64 = 0;
        let start = Instant::now();
        loop {
            match sub.take() {
                Ok(Some(s)) => {
                    let h = s.header();
                    total_latency_ns += (now_ns() - h.sent_nanos) as u128;
                    received += 1;
                    last_seen_seq = h.seq;
                    last_progress = Instant::now();
                    if h.seq == total {
                        break;
                    }
                }
                Ok(None) => {
                    empties += 1;
                    if last_progress.elapsed() > Duration::from_secs(5) {
                        break; // give up — publisher hung or finished
                    }
                    std::hint::spin_loop();
                }
                Err(quicbit::Error::Lagged { dropped: n }) => {
                    dropped += n;
                    last_progress = Instant::now();
                }
                Err(_) => break,
            }
        }
        let elapsed = start.elapsed();
        (
            received,
            dropped,
            total_latency_ns,
            empties,
            elapsed,
            last_seen_seq,
        )
    });

    // Publisher: publish `total` samples as fast as it can.
    let mut pubr = svc_for_pub.publisher().expect("publisher");
    let start = Instant::now();
    let mut publish_latency_ns: u128 = 0;
    let mut emitted: u64 = 0;
    let mut backoffs: u64 = 0;
    while emitted < total {
        let loan_start = Instant::now();
        let mut loan = match pubr.loan(0) {
            Ok(l) => l,
            Err(_) => {
                // Slot pool full — back off briefly.
                backoffs += 1;
                thread::sleep(Duration::from_micros(10));
                continue;
            }
        };
        let h = loan.header_mut();
        h.sent_nanos = now_ns();
        h.seq = emitted + 1;
        pubr.publish(loan).expect("publish");
        publish_latency_ns += loan_start.elapsed().as_nanos();
        emitted += 1;
    }
    let publisher_elapsed = start.elapsed();

    let (received, dropped, total_latency_ns, empties, consumer_elapsed, last_seen_seq) =
        consumer.join().expect("consumer thread");

    let pps = if consumer_elapsed.as_nanos() > 0 {
        (received as f64 / consumer_elapsed.as_secs_f64()).round() as u64
    } else {
        0
    };
    let pub_mean_ns = if emitted > 0 {
        publish_latency_ns / emitted as u128
    } else {
        0
    };
    let end_to_end_mean_ns = if received > 0 {
        total_latency_ns / received as u128
    } else {
        0
    };

    println!("quicbit local bench (target {total} messages)");
    println!("  publisher elapsed      : {:?}", publisher_elapsed);
    println!("  publisher emitted      : {emitted}");
    println!("  consumer elapsed       : {:?}", consumer_elapsed);
    println!("  consumer received      : {received}");
    println!("  consumer dropped (lag) : {dropped}");
    println!("  last seq seen          : {last_seen_seq}");
    println!("  throughput             : {pps} msg/s");
    println!("  mean publish latency   : {pub_mean_ns} ns");
    println!("  mean end-to-end        : {end_to_end_mean_ns} ns");
    println!("  publisher backoffs     : {backoffs}");
    println!("  subscriber empty polls : {empties}");
}
