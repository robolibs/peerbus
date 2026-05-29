//! Spinning-cube wireframe at 4K, published through quicbit.
//! Pair with `examples/video_sub.rs`.
//!
//! Run:
//!
//! ```bash
//! cargo run --release --example video_pub
//! ```
//!
//! Same command works whether the subscriber is on the same host
//! (local SHM) or another machine (iroh). quicbit picks.

use std::thread;
use std::time::{Duration, Instant};

use quicbit::demo::VideoFrame;
use quicbit::{LocalConfig, Node};

const WIDTH: u32 = 3840;
const HEIGHT: u32 = 2160;
const FPS: u32 = 30;
const TOPIC: &str = "demo/video";
const KEY_PATH: &str = "/tmp/quicbit_video_pub.key";

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn init_tracing() {
    // RUST_LOG=iroh=info,quicbit=debug … cargo run --example …
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn main() -> quicbit::Result<()> {
    init_tracing();

    let pixel_count = WIDTH as usize * HEIGHT as usize;
    let bytes_per_frame = pixel_count * 4;

    let local_cfg = LocalConfig {
        max_payload_bytes: bytes_per_frame + 4096,
        subscriber_buffer: 4,
        max_publishers: 2,
        max_subscribers: 4,
        history_depth: 1,
    };

    let node = Node::builder()
        .identity_file(KEY_PATH)
        .local_config(local_cfg)
        .bind()?;

    let did = node.endpoint_did_key();
    println!("publisher ready: {WIDTH}x{HEIGHT} @ {FPS} fps");
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

    let mut pubr = node.publisher::<VideoFrame>(TOPIC)?;

    let frame_period = Duration::from_secs_f64(1.0 / FPS as f64);
    let t_start = Instant::now();
    let mut frame_no: u64 = 0;
    let mut bench_start = Instant::now();
    let mut bench_frames: u64 = 0;

    loop {
        let render_start = Instant::now();
        let t = t_start.elapsed().as_secs_f32();

        let mut loan = pubr.loan(bytes_per_frame)?;
        {
            let header = loan.header_mut();
            header.width = WIDTH;
            header.height = HEIGHT;
            header.frame_no = frame_no;
            header.stamp_ns = now_ns();
        }
        {
            let pixels: &mut [u32] = bytemuck::try_cast_slice_mut(loan.payload_mut())
                .expect("local SHM slot 4-byte aligned");
            render_cube(pixels, WIDTH as usize, HEIGHT as usize, t);
        }
        pubr.publish(loan)?;

        frame_no += 1;
        bench_frames += 1;
        if bench_start.elapsed() >= Duration::from_secs(2) {
            let elapsed = bench_start.elapsed().as_secs_f64();
            let fps = bench_frames as f64 / elapsed;
            let mb_per_s = fps * bytes_per_frame as f64 / 1_048_576.0;
            println!("[pub] {fps:>6.1} fps  |  {mb_per_s:>7.1} MB/s");
            bench_start = Instant::now();
            bench_frames = 0;
        }

        if let Some(rem) = frame_period.checked_sub(render_start.elapsed()) {
            thread::sleep(rem);
        }
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
