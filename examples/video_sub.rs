//! Subscriber: opens a Wayland window and draws frames from `video_pub`.
//!
//! Run:
//!
//! ```bash
//! cargo run --release --example video_sub -- <did:key:z…>
//! ```
//!
//! Same command works whether the publisher is on the same host
//! (local SHM) or another machine (iroh). quicbit picks.

use std::time::{Duration, Instant};

use minifb::{Key, Window, WindowOptions};
use quicbit::demo::VideoFrame;
use quicbit::{LocalConfig, Node};

const TOPIC: &str = "demo/video";

fn main() -> quicbit::Result<()> {
    init_tracing();
    ensure_wayland_session();

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

fn ensure_wayland_session() {
    if std::env::var_os("WAYLAND_DISPLAY").is_none() {
        eprintln!(
            "error: video_sub is built Wayland-only; WAYLAND_DISPLAY is not set \
             (start it inside a Wayland session, not Xorg/XWayland)"
        );
        std::process::exit(2);
    }
}

fn init_tracing() {
    // RUST_LOG=iroh=info,quicbit=debug … cargo run --example …
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = fmt().with_env_filter(filter).try_init();
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
