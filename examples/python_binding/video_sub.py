#!/usr/bin/env python3
"""Python subscriber compatible with Rust `examples/video_pub.rs`.

This is intentionally headless/no-GUI so it works without pulling in
Xorg/Qt/pygame dependencies. Use the Rust `video_sub` example when you
want a Wayland window.

Run from the repo root inside `nix develop`:

    peerbus-python-develop
    peerbus-video-sub <did:key:z...>
"""

from __future__ import annotations

import argparse
import os
import struct
import time

import datapod
import peerbus


TOPIC = "demo/video"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("peer", nargs="?", default=os.environ.get("PEER"))
    parser.add_argument("--topic", default=os.environ.get("PEERBUS_VIDEO_TOPIC", TOPIC))
    parser.add_argument("--max-payload-bytes", type=int, default=64 * 1024 * 1024)
    args = parser.parse_args()

    if not args.peer:
        raise SystemExit("usage: video_sub.py <publisher did:key:z...>")

    node = peerbus.Node(max_payload_bytes=args.max_payload_bytes, subscriber_buffer=4)
    qos = peerbus.TopicQos.latest(max_message_bytes=args.max_payload_bytes)
    sub = node.datapod_subscriber(args.peer, args.topic, qos)
    grid_type_hash = datapod.Grid.TYPE_HASH
    grid_header_size = datapod.header_size(grid_type_hash)

    print(f"python subscriber listening to {args.peer} on '{args.topic}'")
    print("waiting for first frame ...")

    width = height = None
    bench_t0 = time.monotonic()
    bench_frames = 0
    bytes_sum = 0

    while True:
        sample = sub.take_view()
        if sample is None:
            time.sleep(0.002)
            continue

        if sample.type_hash != grid_type_hash:
            continue

        wire = sample.wire_view()
        if len(wire) < grid_header_size:
            continue

        # GridHeader starts with:
        # rows: u32, cols: u32, encoding: u32, centered: u32, resolution: f64.
        # `wire` is a memoryview backed directly by the SHM slot on local
        # transport; struct.unpack_from reads the small header without copying
        # the frame payload.
        h, w, encoding_id, _centered, _resolution = struct.unpack_from("<IIIId", wire, 0)
        pixels = wire[grid_header_size:]
        if encoding_id != 11 or len(pixels) != w * h * 4:
            continue
        if (w, h) != (width, height):
            width, height = w, h
            print(f"frame size {width}x{height}")

        bench_frames += 1
        bytes_sum += len(pixels)

        elapsed = time.monotonic() - bench_t0
        if elapsed >= 2.0:
            fps = bench_frames / elapsed
            mib_s = bytes_sum / elapsed / 1_048_576.0
            print(f"[py sub] {fps:>6.1f} fps  |  {mib_s:>7.1f} MB/s  |  datapod.Grid RGBA8")
            bench_t0 = time.monotonic()
            bench_frames = 0
            bytes_sum = 0


if __name__ == "__main__":
    main()
