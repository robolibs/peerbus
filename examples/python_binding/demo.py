#!/usr/bin/env python3
"""quicbit Python bindings demo: every RawMsg mode over shared memory.

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

import quicbit


class DemoPod:
    """Tiny stand-in for a datapod 0.3 Python class.

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
    kind, data = client.call(b"\x01\x02\x03", kind=7)
    print(f"response kind={kind} data={data.hex()}")  # -> 020406
    print("client stats", client.stats())
    t.join()


def queans_demo() -> None:
    print("== que/ans ==")
    server_node = quicbit.Node(identity="py-search", no_relay=True)
    client_node = quicbit.Node(no_relay=True)
    server = server_node.ans_server("search/range")

    def serve():
        server.serve_one(
            lambda kind, data: [(kind, bytes([data[0] + i])) for i in range(data[1])],
            timeout_ms=3000,
        )

    t = threading.Thread(target=serve)
    t.start()
    client = client_node.que_client("py-search", "search/range")
    answers = client.send(bytes([10, 4]), kind=11)
    print("answers", [(kind, data.hex()) for kind, data in answers])
    t.join()


def putack_demo() -> None:
    print("== put/ack ==")
    server_node = quicbit.Node(identity="py-sink", no_relay=True)
    client_node = quicbit.Node(no_relay=True)
    server = server_node.ack_server("logs/upload")

    def serve():
        def handle(items):
            total = sum(sum(data) for _kind, data in items)
            return (99, total.to_bytes(4, "little"))

        server.serve_one(handle, timeout_ms=3000)

    t = threading.Thread(target=serve)
    t.start()
    client = client_node.put_client("py-sink", "logs/upload")
    kind, data = client.upload([(1, b"\x01\x02"), (1, b"\x03\x04")])
    print(f"ack kind={kind} total={int.from_bytes(data, 'little')}")
    t.join()


def pip_demo() -> None:
    print("== pip ==")
    server_node = quicbit.Node(identity="py-session", no_relay=True)
    client_node = quicbit.Node(no_relay=True)
    server = server_node.pip_server("session/echo")

    def serve():
        server.serve_one(
            lambda items: [(kind, bytes(b * 2 for b in data)) for kind, data in items],
            timeout_ms=3000,
        )

    t = threading.Thread(target=serve)
    t.start()
    client = client_node.pip_client("py-session", "session/echo")
    replies = client.exchange([(7, b"\x01\x02"), (8, b"\x03")])
    print("replies", [(kind, data.hex()) for kind, data in replies])
    t.join()


def system_did_demo() -> None:
    print("== system DID topic-only pub/sub ==")
    seed = quicbit.Node(no_relay=True)
    system_did = seed.did_key()
    pub_node = quicbit.Node(no_relay=True, system_did=system_did)
    sub_node = quicbit.Node(no_relay=True, system_did=system_did)
    pub = pub_node.publisher("system/topic", qos=quicbit.TopicQos.latest())
    sub = sub_node.subscribe("system/topic", qos=quicbit.TopicQos.latest())
    sub_node.add_topic_route("system/topic", pub_node.endpoint_addr())
    pub.send(b"hello-system", kind=123)
    for _ in range(200):
        msg = sub.take()
        if msg is not None:
            kind, data = msg
            print(f"system received kind={kind} data={data!r}")
            print("pub/sub stats", pub.stats(), sub.stats())
            break
        time.sleep(0.005)


def datapod_bridge_demo() -> None:
    print("== datapod bridge helpers ==")
    node = quicbit.Node(identity="py-pod", no_relay=True)

    pub = node.publisher("pod/topic")
    sub = node.subscriber("py-pod", "pod/topic")
    pub.send_pod(DemoPod(1234))
    for _ in range(200):
        pod = sub.take_pod(DemoPod)
        if pod is not None:
            print("pod pub/sub", pod)
            break
        time.sleep(0.005)

    server = node.req_server("pod/double")

    def serve():
        def handle(kind, data):
            pod = DemoPod.from_wire_message(kind, data)
            return DemoPod(pod.value * 2)

        server.serve_one(handle, timeout_ms=3000)

    t = threading.Thread(target=serve)
    t.start()
    client = node.req_client("py-pod", "pod/double")
    doubled = client.call_pod(DemoPod(21), DemoPod)
    print("pod req/res", doubled)
    t.join()


if __name__ == "__main__":
    print("quicbit", quicbit.__version__)
    pubsub_demo()
    reqres_demo()
    queans_demo()
    putack_demo()
    pip_demo()
    system_did_demo()
    datapod_bridge_demo()
