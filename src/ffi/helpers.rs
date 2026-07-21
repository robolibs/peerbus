use super::*;

// ---- helpers ----

pub(crate) unsafe fn cstr<'a>(ptr: *const c_char) -> Result<&'a str, ()> {
    if ptr.is_null() {
        set_last_error("null string argument");
        return Err(());
    }
    // SAFETY: caller promises a valid NUL-terminated C string.
    match unsafe { CStr::from_ptr(ptr) }.to_str() {
        Ok(s) => Ok(s),
        Err(_) => {
            set_last_error("string argument is not valid UTF-8");
            Err(())
        }
    }
}

pub(crate) unsafe fn bytes_in<'a>(data: *const u8, len: usize) -> Result<&'a [u8], ()> {
    if len == 0 {
        Ok(&[])
    } else if data.is_null() {
        set_last_error("null byte buffer with non-zero length");
        Err(())
    } else {
        // SAFETY: caller promises `len` valid bytes at `data`.
        Ok(unsafe { std::slice::from_raw_parts(data, len) })
    }
}

pub(crate) unsafe fn raw_messages_in<'a>(
    items: *const PeerbusRawMessage,
    len: usize,
) -> Result<&'a [PeerbusRawMessage], ()> {
    if items.is_null() {
        if len == 0 {
            Ok(&[])
        } else {
            set_last_error("null message array with non-zero length");
            Err(())
        }
    } else {
        // SAFETY: caller promises `len` valid PeerbusRawMessage values.
        Ok(unsafe { std::slice::from_raw_parts(items, len) })
    }
}

pub(crate) unsafe fn datapod_raw_messages_in<'a>(
    items: *const PeerbusDatapodRawMessage,
    len: usize,
) -> Result<&'a [PeerbusDatapodRawMessage], ()> {
    if items.is_null() {
        if len == 0 {
            Ok(&[])
        } else {
            set_last_error("null datapod message array with non-zero length");
            Err(())
        }
    } else {
        Ok(unsafe { std::slice::from_raw_parts(items, len) })
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        set_last_error("endpoint address hex has odd length");
        return Err(());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = match nibble(pair[0]) {
            Some(value) => value,
            None => {
                set_last_error("invalid endpoint address hex");
                return Err(());
            }
        };
        let lo = match nibble(pair[1]) {
            Some(value) => value,
            None => {
                set_last_error("invalid endpoint address hex");
                return Err(());
            }
        };
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

pub(crate) fn encode_endpoint_addr(addr: &EndpointAddr) -> Result<CString, ()> {
    let bytes = postcard::to_stdvec(addr).map_err(|e| {
        set_last_error(format!("encode endpoint addr: {e}"));
    })?;
    CString::new(hex_encode(&bytes)).map_err(|_| {
        set_last_error("encoded endpoint addr contained NUL");
    })
}

pub(crate) unsafe fn endpoint_addr_in(ptr: *const c_char) -> Result<EndpointAddr, ()> {
    let encoded = unsafe { cstr(ptr) }?;
    let bytes = hex_decode(encoded)?;
    postcard::from_bytes(&bytes).map_err(|e| {
        set_last_error(format!("decode endpoint addr: {e}"));
    })
}

/// Parse a peer argument. peerbus addresses peers only by id, so the C
/// string must be a hex postcard-encoded `EndpointAddr` (as produced by
/// `peerbus_node_endpoint_addr`). Friendly names / `did:key` resolution
/// live in the higher-level crate.
pub(crate) unsafe fn peer_arg(peer: *const c_char) -> Result<EndpointAddr, ()> {
    unsafe { endpoint_addr_in(peer) }
}

pub(crate) fn item_stats_out(stats: crate::ItemStats) -> PeerbusItemStats {
    PeerbusItemStats {
        messages_out: stats.messages_out,
        messages_in: stats.messages_in,
        bytes_out: stats.bytes_out,
        bytes_in: stats.bytes_in,
        errors: stats.errors,
    }
}

pub(crate) fn message_from_raw(raw: PeerbusRawMessage) -> RawMsg {
    // Element pointers come from an already-validated array; treat a null or
    // empty element as an empty payload (the array-level check rejects a null
    // array with non-zero length).
    let bytes: &[u8] = if raw.data.ptr.is_null() || raw.data.len == 0 {
        &[]
    } else {
        // SAFETY: caller promises `len` valid bytes at `ptr`.
        unsafe { std::slice::from_raw_parts(raw.data.ptr, raw.data.len) }
    };
    RawMsg::new(raw.kind, bytes)
}

pub(crate) fn message_from_datapod_raw(raw: PeerbusDatapodRawMessage) -> DatapodMsg {
    let wire: &[u8] = if raw.wire.ptr.is_null() || raw.wire.len == 0 {
        &[]
    } else {
        // SAFETY: caller promises `len` valid bytes at `ptr`.
        unsafe { std::slice::from_raw_parts(raw.wire.ptr, raw.wire.len) }
    };
    DatapodMsg::new(raw.type_hash, wire)
}

pub(crate) fn owned_message(kind: u64, payload: &[u8]) -> PeerbusMessage {
    PeerbusMessage {
        kind,
        data: payload.to_vec(),
    }
}

pub(crate) fn owned_datapod_message(type_hash: u64, wire: &[u8]) -> PeerbusDatapodMessage {
    PeerbusDatapodMessage {
        type_hash,
        wire: wire.to_vec(),
    }
}

pub(crate) fn messages_from_raw(values: Vec<RawMsg>) -> PeerbusMessages {
    PeerbusMessages {
        messages: values
            .into_iter()
            .map(|msg| owned_message(msg.kind, &msg.data))
            .collect(),
    }
}

pub(crate) fn build_node_from_config(cfg: PeerbusNodeConfig) -> Result<Node, ()> {
    let mut builder = Node::builder();
    // 32-byte ed25519 secret key, or ephemeral when null.
    if cfg.secret_key.is_null() {
        builder = builder.ephemeral();
    } else {
        // SAFETY: caller promises 32 readable bytes when non-null.
        let bytes = unsafe { std::slice::from_raw_parts(cfg.secret_key, 32) };
        let mut key = [0u8; 32];
        key.copy_from_slice(bytes);
        builder = builder.secret_key(iroh::SecretKey::from_bytes(&key));
    }
    if cfg.no_relay {
        builder = builder.no_relay();
    }
    if cfg.allowed_peers_len != 0 {
        if cfg.allowed_peers.is_null() {
            set_last_error("allowed_peers is null but allowed_peers_len is non-zero");
            return Err(());
        }
        // SAFETY: caller promises `allowed_peers_len` valid C string pointers.
        let peers = unsafe { std::slice::from_raw_parts(cfg.allowed_peers, cfg.allowed_peers_len) };
        for peer in peers {
            let peer = unsafe { endpoint_addr_in(*peer) }?;
            builder = builder.allow_peer(peer);
        }
    }
    if cfg.allow_any_peer {
        builder = builder.allow_any_peer();
    }
    if cfg.max_payload_bytes != 0
        || cfg.history_depth != 0
        || cfg.subscriber_buffer != 0
        || cfg.max_publishers != 0
        || cfg.max_subscribers != 0
    {
        let mut local_cfg = LocalConfig::default();
        if cfg.max_payload_bytes != 0 {
            local_cfg.max_payload_bytes = cfg.max_payload_bytes;
        }
        if cfg.history_depth != 0 {
            local_cfg.history_depth = cfg.history_depth;
        }
        if cfg.subscriber_buffer != 0 {
            local_cfg.subscriber_buffer = cfg.subscriber_buffer;
        }
        if cfg.max_publishers != 0 {
            local_cfg.max_publishers = cfg.max_publishers;
        }
        if cfg.max_subscribers != 0 {
            local_cfg.max_subscribers = cfg.max_subscribers;
        }
        builder = builder.local_config(local_cfg);
    }
    builder.bind().map_err(|e| {
        set_last_error(e.to_string());
    })
}

