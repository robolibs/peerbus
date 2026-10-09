#!/usr/bin/env python3
"""peerbus Python bindings demo: every RawMsg mode over shared memory.

Build & run (from the repo root):
    maturin develop --features python   # or: maturin build --features python
    python examples/python_binding/demo.py

Or:
    make -C examples/python_binding demo

Same-host nodes route through shared memory; point a subscriber/client at
a remote peer's did:key / EndpointAddr string to cross hosts over iroh.
"""

import threading
import time

import peerbus


class DemoPod:
    """Tiny stand-in for a datapod Python class.

    Real datapod classes can use the same generic protocol:
    to_wire_message() -> (TYPE_HASH, bytes)
    from_wire_message(kind, data) -> object
    """

    TYPE_HASH = 0xD00D

    def __init__(self, value: int):
        self.value = value

    def to_wire_message(self):
        return (self.TYPE_HASH, self.value.to_bytes(4, "little"))

    @classmethod
    def from_wire_message(cls, kind, data):
        if kind != cls.TYPE_HASH:
            raise ValueError(f"unexpected kind {kind}")
        return cls(int.from_bytes(data, "little"))

    def __repr__(self):
        return f"DemoPod({self.value})"


def pubsub_demo() -> None:
    print("== pub/sub ==")
    node = peerbus.Node(no_relay=True)
    pub = node.publisher("py/topic")
    sub = node.subscriber(node.endpoint_addr(), "py/topic")

    pub.send(b"\xde\xad\xbe\xef", kind=42)

    for _ in range(200):
        msg = sub.take()
        if msg is not None:
            print(f"received kind={msg.kind} data={msg.data.hex()}")
            break
        time.sleep(0.005)
    else:
        print("no message received")


def reqres_demo() -> None:
    print("== req/res ==")
    server_node = peerbus.Node(no_relay=True)
    client_node = peerbus.Node(no_relay=True)

    server = server_node.req_server("calc/double")
    server_addr = server_node.endpoint_addr()

    def serve():
        # Handler gets (kind, data) and returns (kind, bytes) or bytes.
        server.serve_one(
            lambda kind, data: (kind, bytes(b * 2 for b in data)),
            timeout_ms=3000,
        )

    t = threading.Thread(target=serve)
    t.start()

    # Peer-addressed APIs accept either a stable local name/did:key or the
    # endpoint_addr() hex string for explicit iroh routing.
    client = client_node.req_client(server_addr, "calc/double")
    msg = client.call(b"\x01\x02\x03", kind=7)
    print(f"response kind={msg.kind} data={msg.data.hex()}")  # -> 020406
    print("client stats", client.stats())
    t.join()


def queans_demo() -> None:
    print("== que/ans ==")
    server_node = peerbus.Node(no_relay=True)
    client_node = peerbus.Node(no_relay=True)
    server = server_node.ans_server("search/range")

    def serve():
        server.serve_one(
            lambda kind, data: [(kind, bytes([data[0] + i])) for i in range(data[1])],
            timeout_ms=3000,
        )

    t = threading.Thread(target=serve)
    t.start()
    client = client_node.que_client(server_node.endpoint_addr(), "search/range")
    answers = client.send(bytes([10, 4]), kind=11)
    print("answers", [(msg.kind, msg.data.hex()) for msg in answers])
    t.join()


def putack_demo() -> None:
    print("== put/ack ==")
    server_node = peerbus.Node(no_relay=True)
    client_node = peerbus.Node(no_relay=True)
    server = server_node.ack_server("logs/upload")

    def serve():
        def handle(items):
            total = sum(sum(data) for _kind, data in items)
            return (99, total.to_bytes(4, "little"))

        server.serve_one(handle, timeout_ms=3000)

    t = threading.Thread(target=serve)
    t.start()
    client = client_node.put_client(server_node.endpoint_addr(), "logs/upload")
    ack = client.put_message([(1, b"\x01\x02"), (1, b"\x03\x04")])
    print(f"ack kind={ack.kind} total={int.from_bytes(ack.data, 'little')}")
    t.join()


def pip_demo() -> None:
    print("== pip ==")
    server_node = peerbus.Node(no_relay=True)
    client_node = peerbus.Node(no_relay=True)
    server = server_node.pip_server("session/echo")

    def serve():
        server.serve_one(
            lambda items: [(kind, bytes(b * 2 for b in data)) for kind, data in items],
            timeout_ms=3000,
        )

    t = threading.Thread(target=serve)
    t.start()
    client = client_node.pip_client(server_node.endpoint_addr(), "session/echo")
    replies = client.exchange([(7, b"\x01\x02"), (8, b"\x03")])
    print("replies", [(msg.kind, msg.data.hex()) for msg in replies])
    t.join()

