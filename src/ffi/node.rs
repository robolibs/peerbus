use super::*;

// ---- node ----

/// Create a node. `identity` may be NULL for an ephemeral key. Returns
/// NULL on failure (see [`peerbus_last_error_message`]).
///
/// The node this creates denies every inbound connection (no allowlist, no
/// `allow_any_peer`). To serve remote peers, use
/// [`peerbus_node_new_with_config`] and set `allowed_peers` (preferred) or
/// `allow_any_peer`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_new(identity: *const c_char, no_relay: bool) -> *mut PeerbusNode {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let cfg = PeerbusNodeConfig {
        identity,
        no_relay,
        system_did: ptr::null(),
        allowed_peers: ptr::null(),
        allowed_peers_len: 0,
        allow_any_peer: false,
        max_payload_bytes: 0,
        history_depth: 0,
        subscriber_buffer: 0,
        max_publishers: 0,
        max_subscribers: 0,
    };
    match build_node_from_config(cfg) {
        Ok(node) => Box::into_raw(Box::new(PeerbusNode { node })),
        Err(()) => ptr::null_mut(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_config_default() -> PeerbusNodeConfig {
    PeerbusNodeConfig {
        identity: ptr::null(),
        no_relay: false,
        system_did: ptr::null(),
        allowed_peers: ptr::null(),
        allowed_peers_len: 0,
        allow_any_peer: false,
        max_payload_bytes: 0,
        history_depth: 0,
        subscriber_buffer: 0,
        max_publishers: 0,
        max_subscribers: 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_new_with_config(cfg: PeerbusNodeConfig) -> *mut PeerbusNode {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    match build_node_from_config(cfg) {
        Ok(node) => Box::into_raw(Box::new(PeerbusNode { node })),
        Err(()) => ptr::null_mut(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_free(node: *mut PeerbusNode) {
    ffi_guard((), move || {
    if node.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw in peerbus_node_new.
    unsafe { drop(Box::from_raw(node)) };
})
}

/// This node's identity as a `did:key:z6Mk…` string. Caller owns the
/// returned C string and must free it with [`peerbus_string_free`].
/// Returns NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_did_key(node: *const PeerbusNode) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    match CString::new(node.node.endpoint_did_key()) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("did:key contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_endpoint_addr(node: *const PeerbusNode) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    match encode_endpoint_addr(&node.node.endpoint_addr()) {
        Ok(addr) => addr.into_raw(),
        Err(()) => ptr::null_mut(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_add_topic_route(
    node: *const PeerbusNode,
    topic: *const c_char,
    endpoint_addr: *const c_char,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return false;
    }
    let topic = match unsafe { cstr(topic) } {
        Ok(topic) => topic,
        Err(()) => return false,
    };
    let endpoint_addr = match unsafe { endpoint_addr_in(endpoint_addr) } {
        Ok(addr) => addr,
        Err(()) => return false,
    };
    let node = unsafe { &*node };
    match node.node.add_topic_route(topic, endpoint_addr) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_add_system_peer(
    node: *const PeerbusNode,
    endpoint_addr: *const c_char,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return false;
    }
    let endpoint_addr = match unsafe { endpoint_addr_in(endpoint_addr) } {
        Ok(addr) => addr,
        Err(()) => return false,
    };
    let node = unsafe { &*node };
    match node.node.add_system_peer(endpoint_addr) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_stats(node: *const PeerbusNode) -> PeerbusNodeStats {
    ffi_guard(PeerbusNodeStats::default(), move || {
    if node.is_null() {
        return PeerbusNodeStats::default();
    }
    let stats = unsafe { &*node }.node.stats();
    PeerbusNodeStats {
        publisher_topics: stats.publisher_topics,
        cached_peers: stats.cached_peers,
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_node_peer_path_diagnostics(
    node: *const PeerbusNode,
    endpoint_addr: *const c_char,
) -> *mut PeerbusPeerPathDiagnostics {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let endpoint_addr = match unsafe { endpoint_addr_in(endpoint_addr) } {
        Ok(addr) => addr,
        Err(()) => return ptr::null_mut(),
    };
    let node = unsafe { &*node };
    match node.node.peer_path_diagnostics(endpoint_addr) {
        Ok(Some(diag)) => Box::into_raw(Box::new(PeerbusPeerPathDiagnostics { diag })),
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_free(diag: *mut PeerbusPeerPathDiagnostics) {
    ffi_guard((), move || {
    if diag.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(diag)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_peer(
    diag: *const PeerbusPeerPathDiagnostics,
) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if diag.is_null() {
        set_last_error("null peer path diagnostics handle");
        return ptr::null_mut();
    }
    let did = crate::did_key::endpoint_id_to_did_key(&unsafe { &*diag }.diag.peer);
    match CString::new(did) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("peer did:key contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_count(
    diag: *const PeerbusPeerPathDiagnostics,
) -> usize {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }.diag.paths.len()
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_max_datagram_size(
    diag: *const PeerbusPeerPathDiagnostics,
    out: *mut usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() || out.is_null() {
        return false;
    }
    let Some(value) = unsafe { &*diag }.diag.max_datagram_size else {
        return false;
    };
    unsafe { *out = value };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_datagram_send_buffer_space(
    diag: *const PeerbusPeerPathDiagnostics,
) -> usize {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }.diag.datagram_send_buffer_space
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_id(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if diag.is_null() {
        set_last_error("null peer path diagnostics handle");
        return ptr::null_mut();
    }
    let Some(path) = unsafe { &*diag }.diag.paths.get(index) else {
        set_last_error("path index out of range");
        return ptr::null_mut();
    };
    match CString::new(path.path_id.as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("path id contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_remote_addr(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> *mut c_char {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    if diag.is_null() {
        set_last_error("null peer path diagnostics handle");
        return ptr::null_mut();
    }
    let Some(path) = unsafe { &*diag }.diag.paths.get(index) else {
        set_last_error("path index out of range");
        return ptr::null_mut();
    };
    match CString::new(path.remote_addr.as_str()) {
        Ok(s) => s.into_raw(),
        Err(_) => {
            set_last_error("path remote addr contained a NUL byte");
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_selected(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() {
        return false;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .is_some_and(|path| path.selected)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_is_ip(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() {
        return false;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .is_some_and(|path| path.is_ip)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_is_relay(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> bool {
    ffi_guard(false, move || {
    if diag.is_null() {
        return false;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .is_some_and(|path| path.is_relay)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_rtt_ms(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> f64 {
    ffi_guard(0.0, move || {
    if diag.is_null() {
        return 0.0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0.0, |path| path.rtt.as_secs_f64() * 1000.0)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_current_mtu(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> u16 {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0, |path| path.current_mtu)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_cwnd(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0, |path| path.cwnd)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_peer_path_diagnostics_path_lost_packets(
    diag: *const PeerbusPeerPathDiagnostics,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    if diag.is_null() {
        return 0;
    }
    unsafe { &*diag }
        .diag
        .paths
        .get(index)
        .map_or(0, |path| path.lost_packets)
})
}

/// Free a string returned by peerbus (e.g. [`peerbus_node_did_key`]).
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_string_free(s: *mut c_char) {
    ffi_guard((), move || {
    if s.is_null() {
        return;
    }
    // SAFETY: originated from CString::into_raw.
    unsafe { drop(CString::from_raw(s)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_topic_qos_reliable() -> PeerbusTopicQos {
    TopicQos::reliable().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_topic_qos_latest() -> PeerbusTopicQos {
    TopicQos::latest().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_topic_qos_best_effort() -> PeerbusTopicQos {
    TopicQos::best_effort().into()
}

