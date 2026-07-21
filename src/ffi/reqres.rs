use super::*;

// ---- req/res client ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusReqClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_req_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusReqClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = match peer {
        Ok(addr) => node
            .node
            .req_client_with_qos::<RawMsg, RawMsg>(addr, topic, qos),
        Err(name) => node
            .node
            .req_client_with_qos::<RawMsg, RawMsg>(name.as_str(), topic, qos),
    };
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusReqClient {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node.node.req_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(client) => Box::into_raw(Box::new(PeerbusReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_free(client: *mut PeerbusReqClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_stats(client: *const PeerbusReqClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

/// Send a request and block for the response. Returns false on failure;
/// on success writes an owned response message to `*out_message` (free
/// with [`peerbus_message_free`]).
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_client_call(
    client: *mut PeerbusReqClient,
    kind: u64,
    data: *const u8,
    len: usize,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null client or out pointer");
        return false;
    }
    // SAFETY: validated non-null.
    let client = unsafe { &mut *client };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match client.client.call(&RawMsg::new(kind, bytes)) {
        Ok(res) => {
            let msg = PeerbusMessage {
                kind: res.header().kind,
                data: res.payload().to_vec(),
            };
            // SAFETY: out_message validated non-null.
            unsafe { *out_message = Box::into_raw(Box::new(msg)) };
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

// ---- req/res server ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusReqServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_req_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusReqServer {
    ffi_guard(ptr::null_mut(), move || {
    clear_last_error();
    let qos = match topic_qos_from_c(qos) {
        Ok(qos) => qos,
        Err(()) => return ptr::null_mut(),
    };
    if node.is_null() {
        set_last_error("null node handle");
        return ptr::null_mut();
    }
    // SAFETY: validated non-null.
    let node = unsafe { &*node };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    match node
        .node
        .req_server_with_qos::<RawMsg, RawMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusReqServer {
                server: Arc::new(Mutex::new(server)),
            }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_free(server: *mut PeerbusReqServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_stats(server: *const PeerbusReqServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

/// Set the response on a responder passed to a request handler. Copies
/// `data` immediately; safe to call once per handler invocation.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_responder_set(
    responder: *mut PeerbusResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    ffi_guard((), move || {
    if responder.is_null() {
        return;
    }
    // SAFETY: validated non-null; lives on serve_one's stack.
    let responder = unsafe { &mut *responder };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return,
    };
    responder.kind = kind;
    responder.data = bytes.to_vec();
    responder.set = true;
})
}

/// Serve at most one request, waiting up to `timeout_ms`. Invokes
/// `handler` with the request and a responder; whatever the handler sets
/// (via [`peerbus_responder_set`]) is sent back. Returns `1` if a request
/// was served, `0` on timeout, `-1` on error.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_serve_one(
    server: *mut PeerbusReqServer,
    timeout_ms: u64,
    handler: PeerbusReqHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null request handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some((req, reply))) => {
                let kind = req.header().kind;
                let payload = req.payload();
                let mut responder = PeerbusResponder {
                    kind: 0,
                    data: Vec::new(),
                    set: false,
                };
                // SAFETY: handler is a valid fn pointer; responder lives
                // for the duration of this call.
                unsafe {
                    handler(
                        ctx,
                        kind,
                        payload.as_ptr(),
                        payload.len(),
                        &mut responder as *mut PeerbusResponder,
                    )
                };
                let response = RawMsg::new(responder.kind, &responder.data);
                return match reply.respond(&response) {
                    Ok(()) => 1,
                    Err(e) => {
                        set_last_error(e.to_string());
                        -1
                    }
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_req_server_take(
    server: *mut PeerbusReqServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingReq,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null req server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (req, reply) = pending.into_parts();
                let handle = PeerbusPendingReq {
                    server: arc.clone(),
                    reply: Some(reply),
                    request: owned_message(req.header().kind, req.payload()),
                };
                unsafe { *out_pending = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_pending = ptr::null_mut() };
                    return 0;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_req_request(
    pending: *const PeerbusPendingReq,
) -> *const PeerbusMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_req_reply(
    pending: *mut PeerbusPendingReq,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending req handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(reply) = pending.reply.take() else {
        set_last_error("pending req already replied");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).respond_pending(reply, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_req_free(pending: *mut PeerbusPendingReq) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

// ---- message-list accessors ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_len(messages: *const PeerbusMessages) -> usize {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }.messages.len()
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_kind_at(messages: *const PeerbusMessages, index: usize) -> u64 {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map(|msg| msg.kind)
        .unwrap_or(0)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_data_at(
    messages: *const PeerbusMessages,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if messages.is_null() {
        return PeerbusBytes::empty();
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map(|msg| PeerbusBytes {
            ptr: msg.data.as_ptr(),
            len: msg.data.len(),
        })
        .unwrap_or_else(PeerbusBytes::empty)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_messages_free(messages: *mut PeerbusMessages) {
    ffi_guard((), move || {
    if messages.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(messages)) };
})
}

