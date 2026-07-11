//! Subscriber: opens a Wayland window and draws frames from `video_pub`.
//!
//! Run:
//!
//! ```bash
//! cargo run --release --example video_sub -- <did:key:z…>
//! ```
//!
//! Same command works whether the publisher is on the same host
//! (local SHM) or another machine (iroh). peerbus picks.

use std::time::{Duration, Instant};

use datapod::{Encoding, Grid};
use minifb::{Key, Window, WindowOptions};
use peerbus::{DatapodMsg, LocalConfig, Node};

const TOPIC: &str = "demo/video";

fn main() -> peerbus::Result<()> {
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
    let mut sub = node.subscriber::<DatapodMsg>(did.as_str(), TOPIC)?;

    println!("waiting for first frame …");
    let (width, height, first_frame) = loop {
        if let Some(s) = sub.take()? {
            if let Some(grid) = decode_grid_message(s.header(), s.payload())? {
                break (grid.cols as usize, grid.rows as usize, grid.data);
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    println!("first frame {width}x{height}");

    let mut window = Window::new(
        "peerbus video sub  (Esc to quit)",
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
    copy_payload_into(&first_frame, &mut display_buf);
    let mut bench_start = Instant::now();
    let mut bench_frames: u64 = 0;

    while window.is_open() && !window.is_key_down(Key::Escape) {
        let mut got = false;
        while let Some(s) = sub.take()? {
            if let Some(grid) = decode_grid_message(s.header(), s.payload())?
                && grid.cols as usize == width
                && grid.rows as usize == height
            {
                copy_payload_into(&grid.data, &mut display_buf);
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
            println!("[sub] {fps:>6.1} fps  |  datapod.Grid RGBA8");
            bench_start = Instant::now();
            bench_frames = 0;
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
    // RUST_LOG=iroh=info,peerbus=debug … cargo run --example …
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn decode_grid_message(
    msg: &peerbus::datapod_msg::DatapodMsgHeader,
    wire: &[u8],
) -> peerbus::Result<Option<Grid>> {
    let msg = DatapodMsg::new(msg.type_hash, wire.to_vec());
    let grid = match msg.to_datapod::<Grid>() {
        Ok(grid) => grid,
        Err(datapod::WireError::WrongTypeHash { .. }) => return Ok(None),
        Err(e) => {
            return Err(peerbus::Error::invalid_argument(format!(
                "invalid datapod.Grid wire message: {e}"
            )));
        }
    };
    if grid.encoding != Encoding::Rgba8 {
        return Ok(None);
    }
    let expected = (grid.rows as usize)
        .checked_mul(grid.cols as usize)
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(|| peerbus::Error::invalid_argument("video dimensions overflow"))?;
    if grid.data.len() != expected {
        return Ok(None);
    }
    Ok(Some(grid))
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
