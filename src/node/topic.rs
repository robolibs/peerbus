use super::*;

/// Local service name = `<identity_name|hex_endpoint_id>__<sanitised_topic>`.
/// The SHM backend hashes this to an OS-safe id; we still sanitise spaces and a
/// few oddities so the logical name remains friendly in logs/tests.
pub fn service_name(identity_name: Option<&str>, endpoint_id: &[u8; 32], topic: &str) -> String {
    let topic = sanitise(topic);
    match identity_name {
        Some(name) => format!("{}__{topic}", sanitise(name)),
        None => {
            let mut hex = String::with_capacity(64);
            for b in endpoint_id.iter() {
                let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{b:02x}"));
            }
            format!("{hex}__{topic}")
        }
    }
}

pub(crate) fn sanitise(s: &str) -> String {
    s.replace([' '], "_")
}

pub(crate) fn validate_system_did(did: &str) -> Result<()> {
    if !crate::did_key::looks_like_did_key(did) {
        return Err(Error::invalid_argument(format!(
            "system_did must be did:key:z..., got '{did}'"
        )));
    }
    crate::did_key::did_key_to_endpoint_id(did)?;
    Ok(())
}

pub(crate) fn system_route_topic(system_did: &str, topic: &str) -> String {
    format!("{system_did}::{topic}")
}

pub(crate) fn system_service_name(system_did: &str, topic: &str) -> String {
    format!(
        "sys_{:016x}",
        fnv1a64(&system_route_topic(system_did, topic))
    )
}

/// Maximum byte length for a topic name. The local SHM backend uses the same
/// cap for direct services and for composed `<identity>__<topic>` names.
pub const MAX_TOPIC_BYTES: usize = 200;

/// Validate a user-supplied topic string. Allowed characters:
/// `A-Z`, `a-z`, `0-9`, and `._/-`. Empty strings and overlong strings
/// are also rejected.
pub(crate) fn validate_topic(topic: &str) -> Result<()> {
    if topic.is_empty() {
        return Err(Error::invalid_argument("topic name must not be empty"));
    }
    if topic.len() > MAX_TOPIC_BYTES {
        return Err(Error::invalid_argument(format!(
            "topic name '{topic}' is {} bytes; cap is {MAX_TOPIC_BYTES}",
            topic.len()
        )));
    }
    for c in topic.chars() {
        let ok = c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-');
        if !ok {
            return Err(Error::invalid_argument(format!(
                "topic name '{topic}' contains invalid character {c:?}; \
                 allowed: A-Z a-z 0-9 . _ / -"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubsub_datagram_frame_round_trips() {
        let chunk = make_chunk_payload(7, 0, 1, 3, b"abc").unwrap();
        let datagram = make_pubsub_datagram(42, &chunk);
        let (session_id, frame) = parse_pubsub_datagram(&datagram).unwrap();
        assert_eq!(session_id, 42);
        assert_eq!(frame.message_id, 7);
        assert_eq!(frame.chunk_index, 0);
        assert_eq!(frame.chunk_count, 1);
        assert_eq!(frame.message_len, 3);
        assert_eq!(frame.chunk, b"abc");
    }

    #[test]
    fn pubsub_datagram_session_id_is_endpoint_ordered() {
        let publisher = SecretKey::generate().public();
        let subscriber = SecretKey::generate().public();
        let a = pubsub_datagram_session_id(publisher, subscriber, "demo/video", 0x12, 8);
        let b = pubsub_datagram_session_id(publisher, subscriber, "demo/video", 0x12, 8);
        let reversed = pubsub_datagram_session_id(subscriber, publisher, "demo/video", 0x12, 8);
        assert_eq!(a, b);
        assert_ne!(a, reversed);
    }

    fn item_tail_v2(topic: &str) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
        b.extend_from_slice(&0x1111_2222_3333_4444u64.to_le_bytes()); // hash_a
        b.extend_from_slice(&0x5555_6666_7777_8888u64.to_le_bytes()); // hash_b
        b.extend_from_slice(&12u32.to_le_bytes()); // size_a
        b.extend_from_slice(&34u32.to_le_bytes()); // size_b
        b.extend_from_slice(&(topic.len() as u16).to_le_bytes());
        b.extend_from_slice(topic.as_bytes());
        b
    }

    fn item_tail_v3(topic: &str) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&ITEM_HANDSHAKE_VERSION_CHUNKED.to_le_bytes());
        b.extend_from_slice(&0x1111_2222_3333_4444u64.to_le_bytes()); // hash_a
        b.extend_from_slice(&0x5555_6666_7777_8888u64.to_le_bytes()); // hash_b
        b.extend_from_slice(&12u32.to_le_bytes()); // size_a
        b.extend_from_slice(&34u32.to_le_bytes()); // size_b
        b.extend_from_slice(&(1024u64 * 1024).to_le_bytes()); // max_message_bytes
        b.extend_from_slice(&(8u64 * 1024 * 1024).to_le_bytes()); // max_inflight_bytes
        b.extend_from_slice(&(64u32 * 1024).to_le_bytes()); // chunk_bytes
        b.extend_from_slice(&(topic.len() as u16).to_le_bytes());
        b.extend_from_slice(topic.as_bytes());
        b
    }

    #[test]
    fn item_handshake_v2_tail_parses_without_chunking() {
        let (topic, a, b, sa, sb, qos) =
            parse_item_handshake_tail(&item_tail_v2("calc/add")).unwrap();
        assert_eq!(topic, "calc/add");
        assert_eq!(a, 0x1111_2222_3333_4444);
        assert_eq!(b, 0x5555_6666_7777_8888);
        assert_eq!(sa, 12);
        assert_eq!(sb, 34);
        assert!(!qos.peer_chunks, "v2 peers must not be told to chunk");
    }

    #[test]
    fn item_handshake_v3_tail_round_trips_byte_limits() {
        let (topic, _, _, _, _, qos) =
            parse_item_handshake_tail(&item_tail_v3("map/tiles")).unwrap();
        assert_eq!(topic, "map/tiles");
        assert!(qos.peer_chunks);
        assert_eq!(qos.max_message_bytes, 1024 * 1024);
        assert_eq!(qos.max_inflight_bytes, 8 * 1024 * 1024);
        assert_eq!(qos.chunk_bytes, 64 * 1024);
    }

    #[test]
    fn item_handshake_rejects_unknown_version() {
        let mut tail = item_tail_v2("x");
        tail[0..4].copy_from_slice(&99u32.to_le_bytes());
        assert!(matches!(
            parse_item_handshake_tail(&tail),
            Err(Error::HandshakeVersionMismatch { .. })
        ));
    }

    #[test]
    fn item_handshake_rejects_truncated() {
        assert!(parse_item_handshake_tail(&[]).is_err());
        let tail = item_tail_v3("topic");
        // Truncate inside the v3 byte-limit region.
        assert!(parse_item_handshake_tail(&tail[..32]).is_err());
    }
}
