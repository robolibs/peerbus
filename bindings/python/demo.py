#!/usr/bin/env python3
"""quicbit Python bindings demo: pub/sub and req/res over shared memory.

Build & run (from the repo root):
    maturin develop --features python   # or: maturin build --features python
    python bindings/python/demo.py

Same-host nodes route through shared memory; point a subscriber/client at
a remote peer's did:key / EndpointAddr string to cross hosts over iroh.
"""

import threading
import time

import quicbit


def pubsub_demo() -> None:
    print("== pub/sub ==")
    node = quicbit.Node(identity="py-demo", no_relay=True)
    pub = node.publisher("py/topic")
    sub = node.subscriber("py-demo", "py/topic")

    pub.send(b"\xde\xad\xbe\xef", kind=42)

    for _ in range(200):
        msg = sub.take()
        if msg is not None:
            kind, data = msg
            print(f"received kind={kind} data={data.hex()}")
            break
        time.sleep(0.005)
    else:
        print("no message received")


def reqres_demo() -> None:
    print("== req/res ==")
    server_node = quicbit.Node(identity="py-calc", no_relay=True)
    client_node = quicbit.Node(no_relay=True)

    server = server_node.req_server("calc/double")

    def serve():
        # Handler gets (kind, data) and returns (kind, bytes) or bytes.
        server.serve_one(
            lambda kind, data: (kind, bytes(b * 2 for b in data)),
            timeout_ms=3000,
        )

    t = threading.Thread(target=serve)
    t.start()

    client = client_node.req_client("py-calc", "calc/double")
    kind, data = client.call(b"\x01\x02\x03", kind=7)
    print(f"response kind={kind} data={data.hex()}")  # -> 020406
    t.join()


if __name__ == "__main__":
    print("quicbit", quicbit.__version__)
    pubsub_demo()
    reqres_demo()
