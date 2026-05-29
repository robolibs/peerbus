//! Smoke tests for the wire-format parsers exposed to fuzz.
//!
//! These cover the easy edge cases (truncated input, oversize
//! topic / frame, version mismatch) so a regression in any of them
//! shows up in CI even without running `cargo fuzz`.

use quicbit::Error;
use quicbit::remote::{
    HANDSHAKE_VERSION, parse_frame, parse_pubsub_handshake_tail, parse_request_handshake_tail,
};

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
    let bogus_version: u32 = HANDSHAKE_VERSION.wrapping_add(1);
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
    // 16 MiB + 1 — over MAX_PAYLOAD_LEN.
    buf.extend_from_slice(&(16u32 * 1024 * 1024 + 1).to_le_bytes());
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
