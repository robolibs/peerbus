//! Smoke tests for the wire-format parsers exposed to fuzz.
//!
//! These cover the easy edge cases (truncated input, oversize
//! topic / frame, version mismatch) so a regression in any of them
//! shows up in CI even without running `cargo fuzz`.

use peerbus::Error;
use peerbus::chunk::{Reassembler, make_chunk_payload, parse_chunk_payload};
use peerbus::remote::{
    HANDSHAKE_VERSION, ITEM_HANDSHAKE_VERSION_CHUNKED, MAX_PAYLOAD_LEN,
    PUBSUB_HANDSHAKE_VERSION_QOS, parse_frame, parse_pubsub_handshake_tail,
    parse_pubsub_handshake_tail_qos, parse_request_handshake_tail,
};
use peerbus::{DeliveryPolicy, TopicQos};

#[test]
fn pubsub_handshake_rejects_empty() {
    assert!(matches!(
        parse_pubsub_handshake_tail(&[]),
        Err(Error::HandshakeMalformed(_))
    ));
}

#[test]
fn pubsub_handshake_rejects_version_mismatch() {
    let mut buf = vec![];
    // Use a version that's definitely not the current one.
    let bogus_version: u32 = 999;
    buf.extend_from_slice(&bogus_version.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // type_hash
    buf.extend_from_slice(&0u32.to_le_bytes()); // payload_size
    buf.extend_from_slice(&0u16.to_le_bytes()); // topic_len
    assert!(matches!(
        parse_pubsub_handshake_tail(&buf),
        Err(Error::HandshakeVersionMismatch { .. })
    ));
}

#[test]
fn pubsub_handshake_rejects_oversize_topic_len() {
    let mut buf = vec![];
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // type_hash
    buf.extend_from_slice(&0u32.to_le_bytes()); // payload_size
    buf.extend_from_slice(&u16::MAX.to_le_bytes()); // far over MAX_TOPIC_LEN
    assert!(matches!(
        parse_pubsub_handshake_tail(&buf),
        Err(Error::TopicNameTooLong { .. })
    ));
}

#[test]
fn pubsub_handshake_rejects_truncated_topic_body() {
    let mut buf = vec![];
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&16u16.to_le_bytes()); // claim 16-byte topic
    // ... but provide no topic bytes.
    assert!(matches!(
        parse_pubsub_handshake_tail(&buf),
        Err(Error::HandshakeMalformed(_))
    ));
}

#[test]
fn pubsub_handshake_round_trip() {
    let topic = "rover/pose";
    let mut buf = vec![];
    buf.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
    buf.extend_from_slice(&0xdead_beefu64.to_le_bytes());
    buf.extend_from_slice(&12u32.to_le_bytes());
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());
    let (t, h, s) = parse_pubsub_handshake_tail(&buf).expect("parses");
    assert_eq!(t, topic);
    assert_eq!(h, 0xdead_beef);
    assert_eq!(s, 12);
}

#[test]
fn pubsub_v3_handshake_round_trips_qos() {
    let topic = "demo/video";
    let qos = TopicQos::latest()
        .with_max_message_bytes(64 * 1024 * 1024)
        .with_max_inflight_bytes(8 * 1024 * 1024)
        .with_chunk_bytes(128 * 1024)
        .with_priority(7);

    let mut buf = vec![];
    buf.extend_from_slice(&PUBSUB_HANDSHAKE_VERSION_QOS.to_le_bytes());
    buf.extend_from_slice(&0xfeed_beefu64.to_le_bytes());
    buf.extend_from_slice(&32u32.to_le_bytes());
    buf.extend_from_slice(&(qos.max_message_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&(qos.max_inflight_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&(qos.chunk_bytes as u32).to_le_bytes());
    buf.push(1); // Latest
    buf.push(qos.priority);
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());

    let h = parse_pubsub_handshake_tail_qos(&buf).expect("parses");
    assert_eq!(h.topic, topic);
    assert_eq!(h.type_hash, 0xfeed_beef);
    assert_eq!(h.payload_size, 32);
    assert_eq!(h.version, PUBSUB_HANDSHAKE_VERSION_QOS);
    assert_eq!(h.qos.delivery, DeliveryPolicy::Latest);
    assert_eq!(h.qos.max_message_bytes, 64 * 1024 * 1024);
    assert_eq!(h.qos.max_inflight_bytes, 8 * 1024 * 1024);
    assert_eq!(h.qos.chunk_bytes, 128 * 1024);
    assert_eq!(h.qos.priority, 7);
}

#[test]
fn pubsub_v3_rejects_invalid_delivery_policy() {
    let topic = "demo/video";
    let mut buf = vec![];
    buf.extend_from_slice(&PUBSUB_HANDSHAKE_VERSION_QOS.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&1024u64.to_le_bytes());
    buf.extend_from_slice(&2048u64.to_le_bytes());
    buf.extend_from_slice(&256u32.to_le_bytes());
    buf.push(99);
    buf.push(0);
    buf.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    buf.extend_from_slice(topic.as_bytes());

    assert!(matches!(
        parse_pubsub_handshake_tail_qos(&buf),
        Err(Error::HandshakeMalformed(_))
    ));
}

#[test]
fn request_handshake_rejects_empty() {
    assert!(matches!(
        parse_request_handshake_tail(&[]),
        Err(Error::HandshakeMalformed(_))
    ));
}

#[test]
fn request_handshake_rejects_truncated() {
    // Just under the fixed header size of 30 bytes.
    let buf = vec![0u8; 29];
    assert!(matches!(
        parse_request_handshake_tail(&buf),
        Err(Error::HandshakeMalformed(_))
    ));
}

/// Build a post-magic item handshake tail (shared by req/res, que/ans,
/// put/ack, pip). `qos = None` → v2; `Some((mmb, mib, cb))` → v3.
fn item_tail(qos: Option<(u64, u64, u32)>, topic: &str) -> Vec<u8> {
    let mut b = Vec::new();
    let version = if qos.is_some() {
        ITEM_HANDSHAKE_VERSION_CHUNKED
    } else {
        HANDSHAKE_VERSION
    };
    b.extend_from_slice(&version.to_le_bytes());
    b.extend_from_slice(&0xAABB_CCDD_1122_3344u64.to_le_bytes()); // hash_a
    b.extend_from_slice(&0x5566_7788_99AA_BBCCu64.to_le_bytes()); // hash_b
    b.extend_from_slice(&24u32.to_le_bytes()); // size_a
    b.extend_from_slice(&48u32.to_le_bytes()); // size_b
    if let Some((mmb, mib, cb)) = qos {
        b.extend_from_slice(&mmb.to_le_bytes());
        b.extend_from_slice(&mib.to_le_bytes());
        b.extend_from_slice(&cb.to_le_bytes());
    }
    b.extend_from_slice(&(topic.len() as u16).to_le_bytes());
    b.extend_from_slice(topic.as_bytes());
    b
}

#[test]
fn item_handshake_v2_round_trips() {
    let (topic, a, b, sa, sb) = parse_request_handshake_tail(&item_tail(None, "calc/add")).unwrap();
    assert_eq!(topic, "calc/add");
    assert_eq!(a, 0xAABB_CCDD_1122_3344);
    assert_eq!(b, 0x5566_7788_99AA_BBCC);
    assert_eq!(sa, 24);
    assert_eq!(sb, 48);
}

#[test]
fn item_handshake_v3_round_trips() {
    let tail = item_tail(Some((1 << 20, 8 << 20, 64 << 10)), "map/tiles");
    let (topic, a, _, _, _) = parse_request_handshake_tail(&tail).unwrap();
    assert_eq!(topic, "map/tiles");
    assert_eq!(a, 0xAABB_CCDD_1122_3344);
}

#[test]
fn item_handshake_rejects_version_mismatch() {
    let mut tail = item_tail(None, "x");
    tail[0..4].copy_from_slice(&99u32.to_le_bytes());
    assert!(matches!(
        parse_request_handshake_tail(&tail),
        Err(Error::HandshakeVersionMismatch { .. })
    ));
}

#[test]
fn item_handshake_rejects_oversize_topic() {
    // Declare a topic far larger than the wire limit (1024).
    let mut tail = item_tail(None, "");
    let len = tail.len();
    tail[len - 2..].copy_from_slice(&5000u16.to_le_bytes());
    assert!(matches!(
        parse_request_handshake_tail(&tail),
        Err(Error::TopicNameTooLong { .. })
    ));
}

#[test]
fn item_handshake_v3_rejects_truncated_qos() {
    // v3 version byte but the byte-limit region is cut short.
    let full = item_tail(Some((1 << 20, 8 << 20, 64 << 10)), "topic");
    assert!(matches!(
        parse_request_handshake_tail(&full[..34]),
        Err(Error::HandshakeMalformed(_))
    ));
}

#[test]
fn chunk_parser_rejects_malformed() {
    assert!(matches!(
        parse_chunk_payload(&[0; 8]),
        Err(Error::HandshakeMalformed(_))
    ));

    let bad = make_chunk_payload(1, 2, 2, 4, b"xx").expect_err("index out of bounds");
    assert!(matches!(bad, Error::HandshakeMalformed(_)));
}

#[test]
fn reliable_reassembly_preserves_order() {
    let payload = b"abcdefghij";
    let c0 = make_chunk_payload(7, 0, 3, payload.len(), &payload[0..4]).unwrap();
    let c1 = make_chunk_payload(7, 1, 3, payload.len(), &payload[4..8]).unwrap();
    let c2 = make_chunk_payload(7, 2, 3, payload.len(), &payload[8..]).unwrap();

    let mut r = Reassembler::new(DeliveryPolicy::Reliable, 1024);
    assert!(r.push(parse_chunk_payload(&c0).unwrap()).unwrap().is_none());
    assert!(r.push(parse_chunk_payload(&c1).unwrap()).unwrap().is_none());
    let out = r
        .push(parse_chunk_payload(&c2).unwrap())
        .unwrap()
        .expect("complete");
    assert_eq!(out, payload);
}

#[test]
fn latest_reassembly_abandons_older_incomplete_message() {
    let old = make_chunk_payload(1, 0, 2, 8, b"abcd").unwrap();
    let new = make_chunk_payload(2, 0, 1, 3, b"xyz").unwrap();
    let old_tail = make_chunk_payload(1, 1, 2, 8, b"efgh").unwrap();

    let mut r = Reassembler::new(DeliveryPolicy::Latest, 1024);
    assert!(
        r.push(parse_chunk_payload(&old).unwrap())
            .unwrap()
            .is_none()
    );
    let out = r
        .push(parse_chunk_payload(&new).unwrap())
        .unwrap()
        .expect("newer message completes");
    assert_eq!(out, b"xyz");
    assert!(
        r.push(parse_chunk_payload(&old_tail).unwrap())
            .unwrap()
            .is_none()
    );
}

#[test]
fn frame_rejects_empty() {
    assert!(matches!(
        parse_frame(&[]),
        Err(Error::HandshakeMalformed(_))
    ));
}

#[test]
fn frame_rejects_oversize() {
    let mut buf = vec![];
    buf.extend_from_slice(&(MAX_PAYLOAD_LEN + 1).to_le_bytes());
    assert!(matches!(
        parse_frame(&buf),
        Err(Error::FrameTooLarge { .. })
    ));
}

#[test]
fn frame_rejects_truncated_body() {
    let mut buf = vec![];
    buf.extend_from_slice(&8u32.to_le_bytes()); // claim 8-byte body
    buf.extend_from_slice(&[1, 2, 3]); // only 3 bytes
    assert!(matches!(
        parse_frame(&buf),
        Err(Error::HandshakeMalformed(_))
    ));
}

#[test]
fn frame_round_trip() {
    let body = [1, 2, 3, 4u8];
    let mut buf = vec![];
    buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body);
    let parsed = parse_frame(&buf).expect("parses");
    assert_eq!(parsed, &body);
}
