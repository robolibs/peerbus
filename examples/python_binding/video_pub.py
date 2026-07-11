#!/usr/bin/env python3
"""Python publisher compatible with Rust `examples/video_sub.rs`.

Run from the repo root inside `nix develop`:

    peerbus-python-develop
    peerbus-video-pub

Then point the Rust subscriber at the printed DID:

    cargo run --release --example video_sub -- <did:key:z...>
"""

from __future__ import annotations

import argparse
import math
import os
import time

import datapod
import peerbus


TOPIC = "demo/video"
DEFAULT_WIDTH = 640
DEFAULT_HEIGHT = 360
DEFAULT_FPS = 15
DEFAULT_SHM_SLOTS = 32

CUBE_VERTS = [
    (-1.0, -1.0, -1.0),
    (1.0, -1.0, -1.0),
    (1.0, 1.0, -1.0),
    (-1.0, 1.0, -1.0),
    (-1.0, -1.0, 1.0),
    (1.0, -1.0, 1.0),
    (1.0, 1.0, 1.0),
    (-1.0, 1.0, 1.0),
]
CUBE_EDGES = [
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
]


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    return int(value)


def rotate(v: tuple[float, float, float], ax: float, ay: float, az: float) -> tuple[float, float, float]:
    x, y, z = v
    cx, sx = math.cos(ax), math.sin(ax)
    cy, sy = math.cos(ay), math.sin(ay)
    cz, sz = math.cos(az), math.sin(az)
    y, z = y * cx - z * sx, y * sx + z * cx
    x, z = x * cy + z * sy, -x * sy + z * cy
    x, y = x * cz - y * sz, x * sz + y * cz
    return x, y, z


def project(v: tuple[float, float, float], width: int, height: int) -> tuple[int, int]:
    x, y, z = v
    scale = min(width, height) * 0.5
    z += 4.0
    return int(width * 0.5 + scale * x / z), int(height * 0.5 - scale * y / z)


def put_pixel(buf: bytearray, width: int, height: int, x: int, y: int, color: bytes) -> None:
    if 0 <= x < width and 0 <= y < height:
        off = (y * width + x) * 4
        buf[off : off + 4] = color


def draw_line(
    buf: bytearray,
    width: int,
    height: int,
    x0: int,
    y0: int,
    x1: int,
    y1: int,
    color: bytes,
    brush: int,
) -> None:
    dx = abs(x1 - x0)
    dy = -abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    x, y = x0, y0
    while True:
        for yy in range(y - brush, y + brush + 1):
            for xx in range(x - brush, x + brush + 1):
                put_pixel(buf, width, height, xx, yy, color)
        if x == x1 and y == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x += sx
        if e2 <= dx:
            err += dx
            y += sy


def render_cube(buf: bytearray, width: int, height: int, t: float) -> None:
    for y in range(height):
        v = int(y / height * 255.0)
        color = (v << 16) | ((v // 2) << 8) | max(0, 255 - v)
        row = color.to_bytes(4, "little") * width
        start = y * width * 4
        buf[start : start + len(row)] = row

    screen = [
        project(rotate(v, t * 0.7, t * 0.9, t * 0.3), width, height)
        for v in CUBE_VERTS
    ]
    brush = max(height // 360, 2)
    white = (0x00FFFFFF).to_bytes(4, "little")
    for a, b in CUBE_EDGES:
        x0, y0 = screen[a]
        x1, y1 = screen[b]
        draw_line(buf, width, height, x0, y0, x1, y1, white, brush)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity", default=os.environ.get("PEERBUS_VIDEO_IDENTITY", "py-video-pub"))
    parser.add_argument("--topic", default=os.environ.get("PEERBUS_VIDEO_TOPIC", TOPIC))
    parser.add_argument("--width", type=int, default=env_int("PEERBUS_VIDEO_WIDTH", DEFAULT_WIDTH))
    parser.add_argument("--height", type=int, default=env_int("PEERBUS_VIDEO_HEIGHT", DEFAULT_HEIGHT))
    parser.add_argument("--fps", type=int, default=env_int("PEERBUS_VIDEO_FPS", DEFAULT_FPS))
    parser.add_argument(
        "--shm-slots",
        type=int,
        default=env_int("PEERBUS_VIDEO_SHM_SLOTS", DEFAULT_SHM_SLOTS),
    )
    args = parser.parse_args()

    if args.width <= 0 or args.height <= 0 or args.fps <= 0 or args.shm_slots <= 0:
        raise SystemExit("width, height, fps, and shm-slots must be positive")

    frame_bytes = args.width * args.height * 4
    node = peerbus.Node(
        identity=args.identity,
        max_payload_bytes=frame_bytes + 4096,
        subscriber_buffer=args.shm_slots,
        max_subscribers=8,
    )
    qos = peerbus.TopicQos.latest(max_message_bytes=max(frame_bytes + 4096, 64 * 1024 * 1024))
    pub = node.datapod_publisher(args.topic, qos)

    print(f"python video publisher ready: {args.width}x{args.height} @ {args.fps} fps")
    print(f"local SHM slots: {args.shm_slots}")
    print(f"identity: {node.did_key()}")
    print()
    print("run Rust subscriber:")
    print(f"    cargo run --release --example video_sub -- {node.did_key()}")
    print()

    pixels = bytearray(frame_bytes)
    t0 = time.monotonic()
    bench_t0 = time.monotonic()
    bench_frames = 0
    bench_dropped = 0
    warned_full_shm = False
    period = 1.0 / args.fps

    while True:
        started = time.monotonic()
        render_cube(pixels, args.width, args.height, started - t0)
        grid = datapod.Grid(
            args.height,
            args.width,
            11,  # datapod.Encoding.Rgba8
            False,
            1.0,
            [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
            bytes(pixels),
        )
        try:
            pub.send(grid)
            bench_frames += 1
        except RuntimeError as exc:
            if "NoFreeSlot" not in str(exc):
                raise
            bench_dropped += 1
            if not warned_full_shm:
                print(
                    "warning: local SHM is full; dropping latest video frames instead of exiting"
                )
                print(
                    "         if this persists, stop old subscribers and remove stale /dev/shm/qb_* segments"
                )
                warned_full_shm = True

        elapsed = time.monotonic() - bench_t0
        if elapsed >= 2.0:
            fps = bench_frames / elapsed
            mib_s = fps * frame_bytes / 1_048_576.0
            print(
                f"[py pub] {fps:>6.1f} fps  |  {mib_s:>7.1f} MB/s  |  dropped {bench_dropped}"
            )
            bench_t0 = time.monotonic()
            bench_frames = 0
            bench_dropped = 0

        remaining = period - (time.monotonic() - started)
        if remaining > 0:
            time.sleep(remaining)


if __name__ == "__main__":
    main()
