"""
Python smoke test for quicbit's pyo3 bindings.

Build the wheel first:

    maturin build --release --features python-extension

Then install + run:

    pip install target/wheels/quicbit-*.whl
    python examples/python_smoke.py
"""

import os
import struct
import time

import quicbit


def main():
    name = f"py-smoke-{os.getpid()}"
    svc = quicbit.Service.create(name, slot_count=8, slot_size=16, history_depth=1)
    pubr = svc.publisher()
    sub = svc.subscriber()

    # 16-byte payload: (u32 seq, u32 payload, u64 reserved).
    fmt = "<IIQ"
    for i in range(1, 4):
        pubr.publish(struct.pack(fmt, i, i * 100, 0))
        time.sleep(0.01)
        sample = sub.take()
        assert sample is not None, f"missing sample {i}"

        # Zero-copy read: memoryview points straight at the SHM slot
        # bytes. No allocation. The slot is held until `sample` is
        # garbage-collected.
        view = memoryview(sample)
        (seq, payload, _) = struct.unpack_from(fmt, view, 0)
        print(f"received: seq={seq} payload={payload} (nbytes={sample.nbytes})")
        assert seq == i and payload == i * 100

        # Optional eager copy, for callers that want to keep the
        # bytes past the sample's lifetime.
        copied = sample.to_bytes()
        assert len(copied) == sample.nbytes

        del sample  # releases the slot

    print("OK")


if __name__ == "__main__":
    main()
