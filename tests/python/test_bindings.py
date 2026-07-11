import threading
import time
import uuid

import datapod
import peerbus


def unique(stem: str) -> str:
    return f"py-{stem}-{uuid.uuid4().hex}"


def wait_for(fn, timeout: float = 2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if value is not None:
            return value
        time.sleep(0.005)
    raise AssertionError("timed out waiting for value")


def checked_thread(fn):
    errors = []

    def run():
        try:
            fn()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, errors


def join_checked(thread, errors):
    thread.join()
    if errors:
        raise errors[0]


def grid(rows: int, cols: int, value: int) -> datapod.Grid:
    return datapod.Grid(
        rows,
        cols,
        11,  # RGBA8
        False,
        1.0,
        [0, 0, 0, 1, 0, 0, 0],
        bytes([value] * (rows * cols * 4)),
    )


class SplitWirePod:
    TYPE_HASH = 0x51504C4954504F44
    HEADER_SIZE = 4

    def __init__(self, size: int, payload: bytes):
        self.size = size
        self.payload = payload

    def to_wire_message(self):
        return (self.TYPE_HASH, self.size.to_bytes(4, "little") + self.payload)

    @classmethod
    def from_wire(cls, header, payload):
        return cls(int.from_bytes(bytes(header), "little"), bytes(payload))


def view(message):
    return datapod.dynamic_view(message.type_hash, message.wire_view())


def test_python_raw_pubsub_message_and_stats():
    identity = unique("pubsub")
    node = peerbus.Node(identity=identity, no_relay=True)
    pub = node.publisher("py/test/pubsub", qos=peerbus.TopicQos.latest())
    sub = node.subscriber(identity, "py/test/pubsub", qos=peerbus.TopicQos.latest())

    pub.send(b"hello", kind=42)
    msg = wait_for(sub.take)

    assert isinstance(msg, peerbus.Message)
    assert msg.kind == 42
    assert bytes(msg.data) == b"hello"
    assert tuple(msg) == (42, b"hello")
    assert pub.stats()["published"] >= 1
    assert sub.stats()["received"] >= 1

    pub.send(b"tuple", kind=44)
    kind, data = wait_for(sub.take_tuple)
    assert kind == 44
    assert bytes(data) == b"tuple"

    pub.send(b"borrowed", kind=43)
    sample = wait_for(sub.take_view)
    data = sample.data_view()
    assert sample.kind == 43
    assert len(sample) == sample.data_len == len(data)
    assert isinstance(data, memoryview)
    assert data.readonly
    assert bytes(data) == b"borrowed"
    assert bytes(memoryview(sample)) == b"borrowed"


def test_python_pubsub_qos_latest_stats_and_large_endpoint_payload():
    latest_identity = unique("latest")
    latest_node = peerbus.Node(identity=latest_identity, no_relay=True)
    latest_qos = peerbus.TopicQos.latest(subscriber_queue=8)
    latest_pub = latest_node.publisher("py/test/latest", qos=latest_qos)
    latest_sub = latest_node.subscriber(latest_identity, "py/test/latest", qos=latest_qos)

    for value in range(10):
        latest_pub.send(bytes([value]), kind=value)

    msg = wait_for(latest_sub.take)
    assert msg.kind == 9
    assert bytes(msg.data) == b"\x09"
    assert latest_sub.stats()["received"] == 1
    assert latest_sub.stats()["stale_dropped"] >= 9

    pub_node = peerbus.Node(identity=unique("remote-pubsub"), no_relay=True)
    sub_node = peerbus.Node(identity=unique("remote-sub"), no_relay=True)
    peer = pub_node.endpoint_addr()
    remote_qos = peerbus.TopicQos.reliable(
        max_message_bytes=512 * 1024,
        max_inflight_bytes=2 * 1024 * 1024,
        chunk_bytes=4096,
    )
    sub = sub_node.subscriber(peer, "py/test/remote-large-pubsub", qos=remote_qos)
    pub = pub_node.publisher("py/test/remote-large-pubsub", qos=remote_qos)
    large = bytes(i % 251 for i in range(96 * 1024))

    deadline = time.monotonic() + 5.0
    got = None
    while time.monotonic() < deadline and got is None:
        pub.send(large, kind=1234)
        time.sleep(0.02)
        got = sub.take()

    assert got is not None
    assert got.kind == 1234
    assert bytes(got.data) == large
    assert pub.stats()["published"] >= 1
    assert sub.stats()["received"] >= 1

    diag = wait_for(lambda: sub_node.peer_path_diagnostics(peer), timeout=5.0)
    assert diag["peer"].startswith("did:key:")
    assert isinstance(diag["datagram_send_buffer_space"], int)
    assert len(diag["paths"]) >= 1
    path = diag["paths"][0]
    assert "selected" in path
    assert "is_ip" in path
    assert "is_relay" in path
    assert "rtt_ms" in path


def test_python_raw_req_que_put_pip_polling_surfaces():
    req_identity = unique("req")
    req_node = peerbus.Node(identity=req_identity, no_relay=True)
    req_server = req_node.req_server("py/test/req")
    req_client = req_node.req_client(req_identity, "py/test/req")

    def serve_req():
        pending = wait_for(lambda: req_server.take(10))
        assert pending.request.kind == 7
        pending.reply(bytes(b * 2 for b in pending.request.data), kind=8)

    th, errors = checked_thread(serve_req)
    req_res = req_client.call_message(b"\x01\x02", kind=7)
    join_checked(th, errors)
    assert req_res.kind == 8
    assert bytes(req_res.data) == b"\x02\x04"

    que_identity = unique("que")
    que_node = peerbus.Node(identity=que_identity, no_relay=True)
    ans_server = que_node.ans_server("py/test/que")
    que_client = que_node.que_client(que_identity, "py/test/que")

    def serve_ans():
        pending = wait_for(lambda: ans_server.take(10))
        assert pending.request.kind == 3
        pending.ans.send(b"a", kind=4)
        pending.ans.send(b"b", kind=5)
        pending.ans.finish()

    th, errors = checked_thread(serve_ans)
    answers = que_client.send(b"q", kind=3)
    join_checked(th, errors)
    assert [(kind, bytes(data)) for kind, data in answers] == [(4, b"a"), (5, b"b")]
    assert peerbus.PendingAns is peerbus.PendingAnswers

    put_identity = unique("put")
    put_node = peerbus.Node(identity=put_identity, no_relay=True)
    ack_server = put_node.ack_server("py/test/put")
    put_client = put_node.put_client(put_identity, "py/test/put")

    def serve_ack():
        def handler(items):
            total = sum(sum(data) for _kind, data in items)
            return (9, total.to_bytes(2, "little"))

        assert ack_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put([(1, b"\x01\x02"), (1, b"\x03")])
    join_checked(th, errors)
    assert (ack[0], bytes(ack[1])) == (9, b"\x06\x00")
    assert peerbus.PutSender is peerbus.PutUpload

    pip_identity = unique("pip")
    pip_node = peerbus.Node(identity=pip_identity, no_relay=True)
    pip_server = pip_node.pip_server("py/test/pip")
    pip_client = pip_node.pip_client(pip_identity, "py/test/pip")

    def serve_pip():
        pending = wait_for(lambda: pip_server.take(10))
        got = []
        while True:
            msg = pending.next()
            if msg is None:
                break
            got.append((msg.kind, bytes(msg.data)))
        assert got == [(1, b"x"), (2, b"y")]
        pending.send(b"z", kind=3)
        pending.finish_send()

    th, errors = checked_thread(serve_pip)
    session = pip_client.open()
    session.send(b"x", kind=1)
    session.send(b"y", kind=2)
    session.finish_send()
    reply = wait_for(session.next)
    assert reply.kind == 3
    assert bytes(reply.data) == b"z"
    assert session.next() is None
    join_checked(th, errors)


def test_python_raw_message_object_convenience_surfaces():
    identity = unique("message-api")
    node = peerbus.Node(identity=identity, no_relay=True)

    req_server = node.req_server("py/test/message/req")
    req_client = node.req_client(identity, "py/test/message/req")

    def serve_req():
        pending = wait_for(lambda: req_server.take(10))
        pending.reply(b"res", kind=pending.request.kind + 1)

    th, errors = checked_thread(serve_req)
    res = req_client.call(b"req", kind=10)
    join_checked(th, errors)
    assert isinstance(res, peerbus.Message)
    assert res.kind == 11
    assert bytes(res.data) == b"res"
    assert tuple(res) == (11, b"res")

    ans_server = node.ans_server("py/test/message/que")
    que_client = node.que_client(identity, "py/test/message/que")

    def serve_ans():
        pending = wait_for(lambda: ans_server.take(10))
        pending.ans.send(b"one", kind=20)
        pending.ans.send(b"two", kind=21)
        pending.ans.finish()

    th, errors = checked_thread(serve_ans)
    answers = que_client.send(b"que", kind=19)
    join_checked(th, errors)
    assert [msg.kind for msg in answers] == [20, 21]
    assert [bytes(msg.data) for msg in answers] == [b"one", b"two"]

    ack_server = node.ack_server("py/test/message/put")
    put_client = node.put_client(identity, "py/test/message/put")

    def serve_ack():
        def handler(items):
            assert [(kind, bytes(data)) for kind, data in items] == [
                (30, b"a"),
                (31, b"b"),
            ]
            return (32, b"ack")

        assert ack_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put([(30, b"a"), (31, b"b")])
    join_checked(th, errors)
    assert ack.kind == 32
    assert bytes(ack.data) == b"ack"

    pip_server = node.pip_server("py/test/message/pip")
    pip_client = node.pip_client(identity, "py/test/message/pip")

    def serve_pip():
        pending = wait_for(lambda: pip_server.take(10))
        got = []
        while True:
            msg = pending.next()
            if msg is None:
                break
            got.append((msg.kind, bytes(msg.data)))
        assert got == [(40, b"x"), (41, b"y")]
        pending.send(b"z", kind=42)
        pending.finish_send()

    th, errors = checked_thread(serve_pip)
    replies = pip_client.exchange([(40, b"x"), (41, b"y")])
    join_checked(th, errors)
    assert [msg.kind for msg in replies] == [42]
    assert [bytes(msg.data) for msg in replies] == [b"z"]


def test_python_allowed_peers_endpoint_reqres():
    client_identity = unique("allowed-client")
    server_identity = unique("allowed-server")
    client_node = peerbus.Node(identity=client_identity, no_relay=True)
    server_node = peerbus.Node(
        identity=server_identity,
        no_relay=True,
        allowed_peers=[client_node.did_key()],
    )

    server = server_node.req_server("py/test/allowlist/req")
    client = client_node.req_client(server_node.endpoint_addr(), "py/test/allowlist/req")

    def serve_req():
        pending = wait_for(lambda: server.take(10), timeout=5.0)
        assert pending.request.kind == 90
        pending.reply(b"allowed", kind=91)

    th, errors = checked_thread(serve_req)
    res = client.call_message(b"hello", kind=90)
    join_checked(th, errors)
    assert res.kind == 91
    assert bytes(res.data) == b"allowed"


def test_python_raw_message_object_server_callbacks():
    identity = unique("message-callback")
    node = peerbus.Node(identity=identity, no_relay=True)

    req_server = node.req_server("py/test/message-callback/req")
    req_client = node.req_client(identity, "py/test/message-callback/req")

    def serve_req():
        def handler(msg):
            assert isinstance(msg, peerbus.Message)
            assert msg.kind == 1
            assert bytes(msg.data) == b"req"
            return peerbus.Message(b"res", kind=2)

        assert req_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_req)
    res = req_client.call(b"req", kind=1)
    join_checked(th, errors)
    assert res.kind == 2
    assert bytes(res.data) == b"res"

    ans_server = node.ans_server("py/test/message-callback/que")
    que_client = node.que_client(identity, "py/test/message-callback/que")

    def serve_ans():
        def handler(msg, ans):
            assert isinstance(msg, peerbus.Message)
            assert msg.kind == 3
            assert bytes(msg.data) == b"que"
            ans.send_message(peerbus.Message(b"one", kind=4))
            ans.send(b"two", kind=5)
            ans.finish()

        assert ans_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ans)
    answers = que_client.send(b"que", kind=3)
    join_checked(th, errors)
    assert [(msg.kind, bytes(msg.data)) for msg in answers] == [
        (4, b"one"),
        (5, b"two"),
    ]

    ack_server = node.ack_server("py/test/message-callback/put")
    put_client = node.put_client(identity, "py/test/message-callback/put")

    def serve_ack():
        def handler(upload):
            assert isinstance(upload, peerbus.PutBatch)
            assert [(msg.kind, bytes(msg.data)) for msg in upload.items] == [
                (6, b"a"),
                (7, b"b"),
            ]
            assert tuple(upload[0]) == (6, b"a")
            upload.ack_message(peerbus.Message(b"ack", kind=8))

        assert ack_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put([(6, b"a"), (7, b"b")])
    join_checked(th, errors)
    assert ack.kind == 8
    assert bytes(ack.data) == b"ack"

    pip_server = node.pip_server("py/test/message-callback/pip")
    pip_client = node.pip_client(identity, "py/test/message-callback/pip")

    def serve_pip():
        def handler(items):
            assert all(isinstance(msg, peerbus.Message) for msg in items)
            assert [(msg.kind, bytes(msg.data)) for msg in items] == [
                (9, b"x"),
                (10, b"y"),
            ]
            return [peerbus.Message(b"z", kind=11)]

        assert pip_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_pip)
    replies = pip_client.exchange([(9, b"x"), (10, b"y")])
    join_checked(th, errors)
    assert [(msg.kind, bytes(msg.data)) for msg in replies] == [(11, b"z")]


def test_python_system_did_topic_only_modes():
    system_did = peerbus.Node(no_relay=True).did_key()
    identity = unique("system")
    node = peerbus.Node(identity=identity, no_relay=True, system_did=system_did)

    pub = node.publisher("py/test/system/pubsub")
    sub = node.subscribe("py/test/system/pubsub")
    pub.send(b"system", kind=1)
    msg = wait_for(sub.take_message)
    assert msg.kind == 1
    assert bytes(msg.data) == b"system"

    server = node.req_server("py/test/system/req")
    client = node.req("py/test/system/req")

    def serve_req():
        pending = wait_for(lambda: server.take(10))
        pending.reply(b"ok", kind=pending.request.kind + 1)

    th, errors = checked_thread(serve_req)
    res = client.call_message(b"go", kind=10)
    join_checked(th, errors)
    assert res.kind == 11
    assert bytes(res.data) == b"ok"

    ans_server = node.ans_server("py/test/system/que")
    que_client = node.que("py/test/system/que")

    def serve_ans():
        pending = wait_for(lambda: ans_server.take(10))
        assert pending.request.kind == 20
        pending.answers.send(b"ans-a", kind=21)
        pending.answers.send(b"ans-b", kind=22)
        pending.answers.finish()

    th, errors = checked_thread(serve_ans)
    answers = que_client.send(b"que", kind=20)
    join_checked(th, errors)
    assert [(kind, bytes(data)) for kind, data in answers] == [
        (21, b"ans-a"),
        (22, b"ans-b"),
    ]

    ack_server = node.ack_server("py/test/system/put")
    put_client = node.put("py/test/system/put")

    def serve_ack():
        def handler(items):
            assert [(kind, bytes(data)) for kind, data in items] == [
                (30, b"put-a"),
                (31, b"put-b"),
            ]
            return (32, b"ack")

        assert ack_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put([(30, b"put-a"), (31, b"put-b")])
    join_checked(th, errors)
    assert (ack[0], bytes(ack[1])) == (32, b"ack")

    pip_server = node.pip_server("py/test/system/pip")
    pip_client = node.pip("py/test/system/pip")

    def serve_pip():
        pending = wait_for(lambda: pip_server.take(10))
        got = []
        while True:
            msg = pending.next()
            if msg is None:
                break
            got.append((msg.kind, bytes(msg.data)))
        assert got == [(40, b"pip-a"), (41, b"pip-b")]
        pending.send(b"pip-r", kind=42)
        pending.finish_send()

    th, errors = checked_thread(serve_pip)
    session = pip_client.open()
    session.send(b"pip-a", kind=40)
    session.send(b"pip-b", kind=41)
    session.finish_send()
    reply = wait_for(session.next)
    assert reply.kind == 42
    assert bytes(reply.data) == b"pip-r"
    assert session.next() is None
    join_checked(th, errors)


def test_python_concrete_datapod_helpers():
    identity = unique("podhelpers")
    node = peerbus.Node(identity=identity, no_relay=True)

    pub = node.publisher("py/test/podhelpers/pubsub")
    sub = node.subscriber(identity, "py/test/podhelpers/pubsub")

    point = datapod.Point(1.0, 2.0, 3.0)
    pub.send_pod(point)
    decoded = wait_for(lambda: sub.take_pod(datapod.Point))
    assert (decoded.x, decoded.y, decoded.z) == (1.0, 2.0, 3.0)

    pose = datapod.Pose(1.0, 2.0, 3.0, 1.0, 0.0, 0.0, 0.0)
    pub.send_pod(pose)
    msg = wait_for(sub.take_message)
    decoded_pose = msg.as_datapod(datapod.Pose)
    assert (decoded_pose.x, decoded_pose.y, decoded_pose.z) == (1.0, 2.0, 3.0)
    assert (decoded_pose.qw, decoded_pose.qx, decoded_pose.qy, decoded_pose.qz) == (
        1.0,
        0.0,
        0.0,
        0.0,
    )

    blob = datapod.Bytes(b"heap-bytes")
    pub.send_pod(blob)
    decoded_blob = wait_for(lambda: sub.take_pod(datapod.Bytes))
    assert decoded_blob.payload_bytes() == b"heap-bytes"

    line = datapod.Linestring([(1.0, 2.0, 3.0), (4.0, 5.0, 6.0)])
    pub.send_pod(line)
    decoded_line = wait_for(lambda: sub.take_pod(datapod.Linestring))
    assert decoded_line.len() == 2
    assert decoded_line.payload_bytes() == line.payload_bytes()

    split_pub = node.publisher("py/test/podhelpers/split")
    split_sub = node.subscriber(identity, "py/test/podhelpers/split")
    split_pub.send_pod(SplitWirePod(5, b"abcde"))
    split = wait_for(lambda: split_sub.take_pod(SplitWirePod))
    assert split.size == 5
    assert split.payload == b"abcde"

    req_server = node.req_server("py/test/podhelpers/req")
    req_client = node.req_client(identity, "py/test/podhelpers/req")

    def serve_req():
        pending = wait_for(lambda: req_server.take(10))
        request = pending.request.as_datapod(datapod.Point)
        pending.reply_pod(datapod.Point(request.x + 1.0, request.y + 1.0, request.z + 1.0))

    th, errors = checked_thread(serve_req)
    res = req_client.call_pod(datapod.Point(4.0, 5.0, 6.0), datapod.Point)
    join_checked(th, errors)
    assert (res.x, res.y, res.z) == (5.0, 6.0, 7.0)

    ans_server = node.ans_server("py/test/podhelpers/que")
    que_client = node.que_client(identity, "py/test/podhelpers/que")

    def serve_ans():
        pending = wait_for(lambda: ans_server.take(10))
        request = pending.request.as_datapod(datapod.Point)
        pending.answers.send_pod(datapod.Point(request.x, request.y, request.z))
        pending.answers.send_pod(datapod.Point(request.x + 10.0, request.y, request.z))
        pending.answers.finish()

    th, errors = checked_thread(serve_ans)
    answers = que_client.send_pod(datapod.Point(8.0, 9.0, 10.0), datapod.Point)
    join_checked(th, errors)
    assert [(ans.x, ans.y, ans.z) for ans in answers] == [
        (8.0, 9.0, 10.0),
        (18.0, 9.0, 10.0),
    ]

    ack_server = node.ack_server("py/test/podhelpers/put")
    put_client = node.put_client(identity, "py/test/podhelpers/put")

    def serve_ack():
        def handler(items):
            points = [
                datapod.Point.from_wire_message(kind, bytes(data))
                for kind, data in items
            ]
            return datapod.Point(
                sum(point.x for point in points),
                sum(point.y for point in points),
                sum(point.z for point in points),
            )

        assert ack_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put_pod(
        [datapod.Point(1.0, 2.0, 3.0), datapod.Point(4.0, 5.0, 6.0)],
        datapod.Point,
    )
    join_checked(th, errors)
    assert (ack.x, ack.y, ack.z) == (5.0, 7.0, 9.0)

    pip_server = node.pip_server("py/test/podhelpers/pip")
    pip_client = node.pip_client(identity, "py/test/podhelpers/pip")

    def serve_pip():
        pending = wait_for(lambda: pip_server.take(10))
        first = pending.next_pod(datapod.Point)
        second = pending.next_pod(datapod.Point)
        assert pending.next_pod(datapod.Point) is None
        pending.send_pod(datapod.Point(first.x + second.x, first.y + second.y, first.z + second.z))
        pending.finish_send()

    th, errors = checked_thread(serve_pip)
    replies = pip_client.exchange_pod(
        [datapod.Point(1.0, 1.0, 1.0), datapod.Point(2.0, 3.0, 4.0)],
        datapod.Point,
    )
    join_checked(th, errors)
    assert [(reply.x, reply.y, reply.z) for reply in replies] == [(3.0, 4.0, 5.0)]


def test_python_generic_datapod_all_primitives():
    identity = unique("datapod")
    node = peerbus.Node(identity=identity, no_relay=True)

    pub = node.datapod_publisher("py/test/datapod/pubsub")
    sub = node.datapod_subscriber(identity, "py/test/datapod/pubsub")
    pub.send(grid(1, 1, 2))
    msg = wait_for(sub.take_message)
    assert isinstance(msg, peerbus.DatapodMessage)
    assert view(msg)["cols"] == 1
    assert bytes(view(msg).payload) == bytes([2] * 4)

    pub.send(grid(1, 2, 3))
    sample = wait_for(sub.take_view)
    wire = sample.wire_view()
    assert isinstance(wire, memoryview)
    assert wire.readonly
    assert len(wire) == sample.wire_len == len(sample)
    assert bytes(memoryview(sample)) == bytes(wire)
    sample_view = datapod.dynamic_view(sample.type_hash, wire)
    assert sample_view["rows"] == 1
    assert sample_view["cols"] == 2
    assert bytes(sample_view.payload) == bytes([3] * 8)

    wire_tuple = grid(1, 1, 4).to_wire_message()
    pub.send(wire_tuple)
    tuple_sample = wait_for(sub.take_view)
    tuple_view = datapod.dynamic_view(tuple_sample.type_hash, tuple_sample.wire_view())
    assert tuple_view["cols"] == 1
    assert bytes(tuple_view.payload) == bytes([4] * 4)

    req_server = node.datapod_req_server("py/test/datapod/req")
    req_client = node.datapod_req_client(identity, "py/test/datapod/req")

    def serve_req():
        pending = wait_for(lambda: req_server.take(10))
        assert view(pending.request)["cols"] == 2
        pending.reply(grid(1, 1, 4))

    th, errors = checked_thread(serve_req)
    res = req_client.call(grid(1, 2, 3))
    join_checked(th, errors)
    assert view(res)["cols"] == 1
    assert bytes(view(res).payload) == bytes([4] * 4)

    ans_server = node.datapod_ans_server("py/test/datapod/que")
    que_client = node.datapod_que_client(identity, "py/test/datapod/que")

    def serve_ans():
        pending = wait_for(lambda: ans_server.take(10))
        assert view(pending.request)["rows"] == 1
        pending.ans.send(grid(1, 1, 5))
        pending.ans.send(grid(1, 2, 6))
        pending.ans.finish()

    th, errors = checked_thread(serve_ans)
    answers = que_client.send(grid(1, 2, 3))
    join_checked(th, errors)
    assert [view(answer)["cols"] for answer in answers] == [1, 2]
    assert peerbus.PendingDatapodAns is peerbus.PendingDatapodAnswers

    ack_server = node.datapod_ack_server("py/test/datapod/put")
    put_client = node.datapod_put_client(identity, "py/test/datapod/put")

    def serve_ack():
        def handler(items):
            assert [view(item)["cols"] for item in items] == [1, 2]
            return grid(1, 1, 7)

        assert ack_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put([grid(1, 1, 1), grid(1, 2, 2)])
    join_checked(th, errors)
    assert view(ack)["cols"] == 1
    assert bytes(view(ack).payload) == bytes([7] * 4)
    assert peerbus.DatapodPutSender is peerbus.DatapodPutUpload

    pip_server = node.datapod_pip_server("py/test/datapod/pip")
    pip_client = node.datapod_pip_client(identity, "py/test/datapod/pip")

    def serve_pip():
        pending = wait_for(lambda: pip_server.take(10))
        got = []
        while True:
            msg = pending.next()
            if msg is None:
                break
            got.append(view(msg)["cols"])
        assert got == [1, 2]
        pending.send(grid(1, 1, 8))
        pending.send(grid(1, 2, 9))
        pending.finish_send()

    th, errors = checked_thread(serve_pip)
    session = pip_client.open()
    session.send(grid(1, 1, 1))
    session.send(grid(1, 2, 2))
    session.finish_send()
    first = wait_for(session.next)
    second = wait_for(session.next)
    assert session.next() is None
    join_checked(th, errors)
    assert [view(first)["cols"], view(second)["cols"]] == [1, 2]
    assert bytes(view(second).payload) == bytes([9] * 8)


def test_python_generic_datapod_system_did_topic_only_all_primitives():
    system_did = peerbus.Node(no_relay=True).did_key()
    node = peerbus.Node(identity=unique("datapod-system"), no_relay=True, system_did=system_did)

    pub = node.datapod_publisher("py/test/datapod-system/pubsub")
    sub = node.datapod_subscribe("py/test/datapod-system/pubsub")
    pub.send(grid(1, 1, 10))
    sample = wait_for(sub.take_view)
    assert memoryview(sample).readonly
    assert view(sample)["cols"] == 1
    assert bytes(datapod.dynamic_view(sample.type_hash, sample.wire_view()).payload) == bytes(
        [10] * 4
    )

    req_server = node.datapod_req_server("py/test/datapod-system/req")
    req_client = node.datapod_req("py/test/datapod-system/req")

    def serve_req():
        pending = wait_for(lambda: req_server.take(10))
        assert view(pending.request)["cols"] == 2
        pending.reply(grid(1, 1, 11))

    th, errors = checked_thread(serve_req)
    res = req_client.call(grid(1, 2, 3))
    join_checked(th, errors)
    assert view(res)["cols"] == 1
    assert bytes(view(res).payload) == bytes([11] * 4)

    ans_server = node.datapod_ans_server("py/test/datapod-system/que")
    que_client = node.datapod_que("py/test/datapod-system/que")

    def serve_ans():
        pending = wait_for(lambda: ans_server.take(10))
        assert view(pending.request)["rows"] == 1
        pending.answers.send(grid(1, 1, 12))
        pending.answers.send(grid(1, 2, 13))
        pending.answers.finish()

    th, errors = checked_thread(serve_ans)
    answers = que_client.send(grid(1, 2, 3))
    join_checked(th, errors)
    assert [view(answer)["cols"] for answer in answers] == [1, 2]
    assert bytes(view(answers[1]).payload) == bytes([13] * 8)

    ack_server = node.datapod_ack_server("py/test/datapod-system/put")
    put_client = node.datapod_put("py/test/datapod-system/put")

    def serve_ack():
        def handler(items):
            assert [view(item)["cols"] for item in items] == [1, 2]
            return grid(1, 1, 14)

        assert ack_server.serve_one(handler, 3000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put([grid(1, 1, 1), grid(1, 2, 2)])
    join_checked(th, errors)
    assert view(ack)["cols"] == 1
    assert bytes(view(ack).payload) == bytes([14] * 4)

    pip_server = node.datapod_pip_server("py/test/datapod-system/pip")
    pip_client = node.datapod_pip("py/test/datapod-system/pip")

    def serve_pip():
        pending = wait_for(lambda: pip_server.take(10))
        got = []
        while True:
            msg = pending.next()
            if msg is None:
                break
            got.append(view(msg)["cols"])
        assert got == [1, 2]
        pending.send(grid(1, 1, 15))
        pending.finish_send()

    th, errors = checked_thread(serve_pip)
    session = pip_client.open()
    session.send(grid(1, 1, 1))
    session.send(grid(1, 2, 2))
    session.finish_send()
    reply = wait_for(session.next)
    assert session.next() is None
    join_checked(th, errors)
    assert view(reply)["cols"] == 1
    assert bytes(view(reply).payload) == bytes([15] * 4)


def test_python_remote_large_raw_chunking_all_item_primitives():
    server_id = unique("large-raw-server")
    server_node = peerbus.Node(identity=server_id, no_relay=True)
    client_node = peerbus.Node(no_relay=True)
    peer = server_node.endpoint_addr()
    qos = peerbus.TopicQos.reliable(
        max_message_bytes=512 * 1024,
        max_inflight_bytes=2 * 1024 * 1024,
        chunk_bytes=4096,
    )
    large_a = bytes((i % 251 for i in range(96 * 1024)))
    large_b = bytes(((i + 7) % 251 for i in range(80 * 1024)))

    req_server = server_node.req_server("py/test/large/req", qos=qos)
    req_client = client_node.req_client(peer, "py/test/large/req", qos=qos)

    def serve_req():
        pending = wait_for(lambda: req_server.take(10), timeout=5.0)
        assert pending.request.kind == 101
        assert bytes(pending.request.data) == large_a
        pending.reply(large_b, kind=102)

    th, errors = checked_thread(serve_req)
    req_res = req_client.call_message(large_a, kind=101)
    join_checked(th, errors)
    assert req_res.kind == 102
    assert bytes(req_res.data) == large_b

    ans_server = server_node.ans_server("py/test/large/que", qos=qos)
    que_client = client_node.que_client(peer, "py/test/large/que", qos=qos)

    def serve_ans():
        pending = wait_for(lambda: ans_server.take(10), timeout=5.0)
        assert pending.request.kind == 201
        assert bytes(pending.request.data) == large_a
        pending.answers.send(large_b, kind=202)
        pending.answers.send(large_a, kind=203)
        pending.answers.finish()

    th, errors = checked_thread(serve_ans)
    answers = que_client.send(large_a, kind=201)
    join_checked(th, errors)
    assert [(kind, len(data)) for kind, data in answers] == [
        (202, len(large_b)),
        (203, len(large_a)),
    ]
    assert bytes(answers[0][1]) == large_b
    assert bytes(answers[1][1]) == large_a

    ack_server = server_node.ack_server("py/test/large/put", qos=qos)
    put_client = client_node.put_client(peer, "py/test/large/put", qos=qos)

    def serve_ack():
        def handler(items):
            assert [(kind, bytes(data)) for kind, data in items] == [
                (301, large_a),
                (302, large_b),
            ]
            return (303, large_b)

        assert ack_server.serve_one(handler, 5000)

    th, errors = checked_thread(serve_ack)
    ack = put_client.put([(301, large_a), (302, large_b)])
    join_checked(th, errors)
    assert (ack[0], bytes(ack[1])) == (303, large_b)

    pip_server = server_node.pip_server("py/test/large/pip", qos=qos)
    pip_client = client_node.pip_client(peer, "py/test/large/pip", qos=qos)

    def serve_pip():
        pending = wait_for(lambda: pip_server.take(10), timeout=5.0)
        first = pending.next()
        second = pending.next()
        assert first.kind == 401 and bytes(first.data) == large_a
        assert second.kind == 402 and bytes(second.data) == large_b
        assert pending.next() is None
        pending.send(large_b, kind=403)
        pending.send(large_a, kind=404)
        pending.finish_send()

    th, errors = checked_thread(serve_pip)
    session = pip_client.open()
    session.send(large_a, kind=401)
    session.send(large_b, kind=402)
    session.finish_send()
    first = wait_for(session.next, timeout=5.0)
    second = wait_for(session.next, timeout=5.0)
    assert session.next() is None
    join_checked(th, errors)
    assert first.kind == 403 and bytes(first.data) == large_b
    assert second.kind == 404 and bytes(second.data) == large_a
