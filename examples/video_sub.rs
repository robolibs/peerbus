//! Subscriber: opens a window and draws frames from `video_pub`.
//!
//! Run:
//!
//! ```bash
//! cargo run --release --example video_sub -- <did:key:z…>
//! ```
//!
//! Same command works whether the publisher is on the same host
//! (iceoryx2 SHM) or another machine (iroh). quicbit picks.

use std::time::{Duration, Instant};

use minifb::{Key, Window, WindowOptions};
use quicbit::demo::VideoFrame;
use quicbit::{LocalConfig, Node};

const TOPIC: &str = "demo/video";

fn main() -> quicbit::Result<()> {
    let did = std::env::args()
        .nth(1)
        .expect("usage: video_sub <did:key:z…>");

    // Big enough for 4K RGBA frames.
    let local_cfg = LocalConfig {
        max_payload_bytes: 64 * 1024 * 1024,
        subscriber_buffer: 4,
        ..LocalConfig::default()
    };
    let node = Node::builder().local_config(local_cfg).bind()?;

    println!("subscribing to {did} on '{TOPIC}'  (Esc to quit)");
    let mut sub = node.subscriber::<VideoFrame>(did.as_str(), TOPIC)?;

    println!("waiting for first frame …");
    let (width, height) = loop {
        if let Some(s) = sub.take()? {
            let h = s.header();
            break (h.width as usize, h.height as usize);
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    println!("first frame {width}x{height}");

    let mut window = Window::new(
        "quicbit video sub  (Esc to quit)",
        width,
        height,
        WindowOptions {
            resize: true,
            ..WindowOptions::default()
        },
    )
    .expect("open window");
    window.set_target_fps(60);

    let mut display_buf = vec![0u32; width * height];
    let mut bench_start = Instant::now();
    let mut bench_frames: u64 = 0;
    let mut total_latency_us: u128 = 0;

    while window.is_open() && !window.is_key_down(Key::Escape) {
        let mut got = false;
        while let Some(s) = sub.take()? {
            let h = s.header();
            if h.width as usize == width && h.height as usize == height {
                copy_payload_into(s.payload(), &mut display_buf);
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                total_latency_us += ((now.saturating_sub(h.stamp_ns)) / 1_000) as u128;
                bench_frames += 1;
                got = true;
            }
        }
        if !got {
            std::thread::sleep(Duration::from_millis(2));
        }

        window
            .update_with_buffer(&display_buf, width, height)
            .expect("update");

        if bench_start.elapsed() >= Duration::from_secs(2) {
            let elapsed = bench_start.elapsed().as_secs_f64();
            let fps = bench_frames as f64 / elapsed;
            let mean_us = if bench_frames > 0 {
                total_latency_us / bench_frames as u128
            } else {
                0
            };
            println!("[sub] {fps:>6.1} fps  |  pub→display ~{mean_us} µs");
            bench_start = Instant::now();
            bench_frames = 0;
            total_latency_us = 0;
        }
    }
    Ok(())
}

fn copy_payload_into(payload: &[u8], dst: &mut [u32]) {
    if let Ok(typed) = bytemuck::try_cast_slice::<u8, u32>(payload)
        && typed.len() == dst.len()
    {
        dst.copy_from_slice(typed);
        return;
    }
    for (slot, chunk) in dst.iter_mut().zip(payload.chunks_exact(4)) {
        *slot = u32::from_le_bytes(chunk.try_into().unwrap());
    }
}
