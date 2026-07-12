//! Spinning-cube wireframe, published through peerbus.
//! Pair with `examples/video_sub.rs`.
//!
//! Run:
//!
//! ```bash
//! cargo run --release --example video_pub
//! ```
//!
//! Defaults to a remote-friendly uncompressed 1280x720 @ 15 fps.
//! Override for local SHM / very fast links:
//!
//! ```bash
//! PEERBUS_VIDEO_WIDTH=3840 PEERBUS_VIDEO_HEIGHT=2160 PEERBUS_VIDEO_FPS=30 \
//!   cargo run --release --example video_pub
//! ```
//!
//! Same command works whether the subscriber is on the same host
//! (local SHM) or another machine (iroh). peerbus picks.

use std::thread;
use std::time::{Duration, Instant};

use datapod::{Encoding, Grid, Pose};
use peerbus::remote::MAX_PAYLOAD_LEN;
use peerbus::{DatapodMsg, LocalConfig, Node, TopicQos};

const DEFAULT_WIDTH: u32 = 1280;
const DEFAULT_HEIGHT: u32 = 720;
const DEFAULT_FPS: u32 = 15;
const DEFAULT_SHM_SLOTS: u32 = 32;
const TOPIC: &str = "demo/video";
const KEY_PATH: &str = "/tmp/peerbus_video_pub.key";

#[derive(Debug, Clone, Copy)]
struct VideoSettings {
    width: u32,
    height: u32,
    fps: u32,
    shm_slots: u32,
}

fn init_tracing() {
    // RUST_LOG=iroh=info,peerbus=debug … cargo run --example …
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn main() -> peerbus::Result<()> {
    init_tracing();
    let settings = video_settings()?;
    let width = settings.width;
    let height = settings.height;
    let fps_target = settings.fps;
    let shm_slots = settings.shm_slots;

    let pixel_count = (width as usize)
        .checked_mul(height as usize)
        .ok_or_else(|| peerbus::Error::invalid_argument("video dimensions overflow"))?;
    let bytes_per_frame = pixel_count
        .checked_mul(4)
        .ok_or_else(|| peerbus::Error::invalid_argument("video frame size overflow"))?;
    let grid_header_bytes = datapod::bind::header_size::<Grid>();
    let bytes_per_message = grid_header_bytes
        .checked_add(bytes_per_frame)
        .ok_or_else(|| peerbus::Error::invalid_argument("video message size overflow"))?;
    if bytes_per_message > MAX_PAYLOAD_LEN as usize {
        return Err(peerbus::Error::PayloadTooLarge {
            actual: bytes_per_message,
            capacity: MAX_PAYLOAD_LEN as usize,
        });
    }

    let local_cfg = LocalConfig {
        max_payload_bytes: bytes_per_message + 4096,
        subscriber_buffer: shm_slots,
        max_publishers: 2,
        max_subscribers: 8,
        history_depth: 1,
    };

    // `allow_any_peer`: inbound peers are denied by default, and this demo
    // streams to whatever subscriber shows up (the Rust/Python `video_sub`
    // processes use ephemeral keys, so there is no id to allowlist ahead of
    // time). Trusted-network only — a real deployment would list the
    // subscribers' ids with `.allow_peer(<did:key or EndpointId>)`.
    let node = Node::builder()
        .identity_file(KEY_PATH)
        .allow_any_peer()
        .local_config(local_cfg)
        .bind()?;

    let did = node.endpoint_did_key();
    let raw_mbps = bytes_per_frame as f64 * fps_target as f64 * 8.0 / 1_000_000.0;
    println!("publisher ready: {width}x{height} @ {fps_target} fps");
    println!(
        "raw uncompressed stream: {:.1} MiB/frame, {:.0} Mbit/s",
        bytes_per_frame as f64 / 1_048_576.0,
        raw_mbps
    );
    println!("local SHM slots: {shm_slots}");
    println!("identity: {did}");

    // Wait for iroh to publish at least one transport address.
    // Without this, a remote subscriber dialing immediately would
    // not be able to look us up.
    eprintln!("waiting for iroh to publish addresses …");
    if let Err(e) = node.wait_for_direct_addresses(Duration::from_secs(15)) {
        eprintln!("warning: addresses not ready after 15s: {e}");
        eprintln!("(same-host subscribers still work via SHM)");
    } else {
        eprintln!("addresses ready, remote subscribers can dial");
    }

    println!();
    println!("run subscriber:");
    println!("    cargo run --release --example video_sub -- {did}");
    println!();

    let qos = TopicQos::latest().with_max_message_bytes(MAX_PAYLOAD_LEN as usize);
    let mut pubr = node.publisher_with_qos::<DatapodMsg>(TOPIC, qos)?;

    let frame_period = Duration::from_secs_f64(1.0 / fps_target as f64);
    let t_start = Instant::now();
    let mut bench_start = Instant::now();
    let mut bench_frames: u64 = 0;
    let mut bench_dropped: u64 = 0;
    let mut pixels = vec![0u32; pixel_count];

    loop {
        let render_start = Instant::now();
        let t = t_start.elapsed().as_secs_f32();

        render_cube(&mut pixels, width as usize, height as usize, t);
        let grid = Grid::new(
            height,
            width,
            Encoding::Rgba8,
            1.0,
            false,
            Pose::default(),
            bytemuck::cast_slice(&pixels).to_vec(),
        );
        match pubr.send(&DatapodMsg::from_datapod(&grid)) {
            Ok(_) => bench_frames += 1,
            Err(peerbus::Error::NoFreeSlot { service }) => {
                bench_dropped += 1;
                if bench_dropped == 1 {
                    eprintln!(
                        "warning: local SHM service '{service}' is full; dropping latest video frames instead of exiting"
                    );
                    eprintln!(
                        "         if this persists after changing SHM slot settings, stop old subscribers and remove stale /dev/shm/qb_* segments"
                    );
                }
            }
            Err(e) => return Err(e),
        }

        if bench_start.elapsed() >= Duration::from_secs(2) {
            let elapsed = bench_start.elapsed().as_secs_f64();
            let fps = bench_frames as f64 / elapsed;
            let mb_per_s = fps * bytes_per_frame as f64 / 1_048_576.0;
            println!("[pub] {fps:>6.1} fps  |  {mb_per_s:>7.1} MB/s  |  dropped {bench_dropped}");
            bench_start = Instant::now();
            bench_frames = 0;
            bench_dropped = 0;
        }

        if let Some(rem) = frame_period.checked_sub(render_start.elapsed()) {
            thread::sleep(rem);
        }
    }
}

fn video_settings() -> peerbus::Result<VideoSettings> {
    let width = env_u32("PEERBUS_VIDEO_WIDTH", DEFAULT_WIDTH)?;
    let height = env_u32("PEERBUS_VIDEO_HEIGHT", DEFAULT_HEIGHT)?;
    let fps = env_u32("PEERBUS_VIDEO_FPS", DEFAULT_FPS)?;
    let shm_slots = env_u32("PEERBUS_VIDEO_SHM_SLOTS", DEFAULT_SHM_SLOTS)?;
    if width == 0 || height == 0 || fps == 0 || shm_slots == 0 {
        return Err(peerbus::Error::invalid_argument(
            "PEERBUS_VIDEO_WIDTH, PEERBUS_VIDEO_HEIGHT, PEERBUS_VIDEO_FPS and PEERBUS_VIDEO_SHM_SLOTS must be non-zero",
        ));
    }
    Ok(VideoSettings {
        width,
        height,
        fps,
        shm_slots,
    })
}

fn env_u32(name: &str, default: u32) -> peerbus::Result<u32> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u32>()
            .map_err(|_| peerbus::Error::invalid_argument(format!("{name} must be a u32"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(peerbus::Error::invalid_argument(format!("{name}: {e}"))),
    }
}

// ---------------- software wireframe renderer ----------------

#[derive(Clone, Copy)]
struct V3 {
    x: f32,
    y: f32,
    z: f32,
}

const CUBE_VERTS: [V3; 8] = [
    V3 {
        x: -1.0,
        y: -1.0,
        z: -1.0,
    },
    V3 {
        x: 1.0,
        y: -1.0,
        z: -1.0,
    },
    V3 {
        x: 1.0,
        y: 1.0,
        z: -1.0,
    },
    V3 {
        x: -1.0,
        y: 1.0,
        z: -1.0,
    },
    V3 {
        x: -1.0,
        y: -1.0,
        z: 1.0,
    },
    V3 {
        x: 1.0,
        y: -1.0,
        z: 1.0,
    },
    V3 {
        x: 1.0,
        y: 1.0,
        z: 1.0,
    },
    V3 {
        x: -1.0,
        y: 1.0,
        z: 1.0,
    },
];

const CUBE_EDGES: [(usize, usize); 12] = [
    (0, 1),
    (1, 2),
    (2, 3),
    (3, 0),
    (4, 5),
    (5, 6),
    (6, 7),
    (7, 4),
    (0, 4),
    (1, 5),
    (2, 6),
    (3, 7),
];

fn rotate(v: V3, ax: f32, ay: f32, az: f32) -> V3 {
    let (cx, sx) = (ax.cos(), ax.sin());
    let (cy, sy) = (ay.cos(), ay.sin());
    let (cz, sz) = (az.cos(), az.sin());
    let (y, z) = (v.y * cx - v.z * sx, v.y * sx + v.z * cx);
    let v = V3 { x: v.x, y, z };
    let (x, z) = (v.x * cy + v.z * sy, -v.x * sy + v.z * cy);
    let v = V3 { x, y: v.y, z };
    let (x, y) = (v.x * cz - v.y * sz, v.x * sz + v.y * cz);
    V3 { x, y, z: v.z }
}

fn project(v: V3, w: usize, h: usize) -> (i32, i32) {
    let scale = (w.min(h) as f32) * 0.5;
    let z = v.z + 4.0;
    let px = w as f32 * 0.5 + scale * v.x / z;
    let py = h as f32 * 0.5 - scale * v.y / z;
    (px as i32, py as i32)
}

fn render_cube(pixels: &mut [u32], w: usize, h: usize, t: f32) {
    for y in 0..h {
        let v = (y as f32 / h as f32 * 255.0) as u32;
        let row = (v << 16) | ((v / 2) << 8) | (255u32.saturating_sub(v));
        let start = y * w;
        for x in 0..w {
            pixels[start + x] = row;
        }
    }

    let mut screen = [(0i32, 0i32); 8];
    for (i, v) in CUBE_VERTS.iter().enumerate() {
        let rv = rotate(*v, t * 0.7, t * 0.9, t * 0.3);
        screen[i] = project(rv, w, h);
    }

    let brush = ((h as i32) / 360).max(2);
    for &(a, b) in &CUBE_EDGES {
        let (x0, y0) = screen[a];
        let (x1, y1) = screen[b];
        draw_line(pixels, w, h, x0, y0, x1, y1, 0x00FF_FFFF, brush);
    }
}

#[allow(clippy::too_many_arguments)]
fn draw_line(
    pixels: &mut [u32],
    w: usize,
    h: usize,
    x0: i32,
    y0: i32,
    x1: i32,
    y1: i32,
    color: u32,
    brush: i32,
) {
    let dx = (x1 - x0).abs();
    let dy = -(y1 - y0).abs();
    let sx = if x0 < x1 { 1 } else { -1 };
    let sy = if y0 < y1 { 1 } else { -1 };
    let mut err = dx + dy;
    let mut x = x0;
    let mut y = y0;
    loop {
        for ddy in -brush..=brush {
            for ddx in -brush..=brush {
                let px = x + ddx;
                let py = y + ddy;
                if px >= 0 && py >= 0 && (px as usize) < w && (py as usize) < h {
                    pixels[(py as usize) * w + (px as usize)] = color;
                }
            }
        }
        if x == x1 && y == y1 {
            break;
        }
        let e2 = 2 * err;
        if e2 >= dy {
            err += dy;
            x += sx;
        }
        if e2 <= dx {
            err += dx;
            y += sy;
        }
    }
}
