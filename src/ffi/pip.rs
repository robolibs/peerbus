use super::*;

// ---- pip ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPipClient {
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
    let peer = match unsafe { peer_arg(peer) } {
        Ok(p) => p,
        Err(()) => return ptr::null_mut(),
    };
    let topic = match unsafe { cstr(topic) } {
        Ok(t) => t,
        Err(()) => return ptr::null_mut(),
    };
    let result = node
            .node
            .pip_client_with_qos::<RawMsg, RawMsg>(peer, topic, qos);
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusPipClient {
                client: Arc::new(Mutex::new(client)),
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
pub extern "C" fn peerbus_pip_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusPipClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_pip_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}


#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_free(client: *mut PeerbusPipClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_stats(client: *const PeerbusPipClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*client }.client).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_exchange(
    client: *mut PeerbusPipClient,
    items: *const PeerbusRawMessage,
    len: usize,
    out_messages: *mut *mut PeerbusMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null pip client or out pointer");
        return false;
    }
    let raw_items = match unsafe { raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &*client };
    let mut guard = lock_inner(&client.client);
    let mut pip = match guard.open() {
        Ok(pip) => pip,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    for raw in raw_items {
        if let Err(e) = pip.send(&message_from_raw(*raw)) {
            set_last_error(e.to_string());
            return false;
        }
    }
    if let Err(e) = pip.finish_send() {
        set_last_error(e.to_string());
        return false;
    }
    let mut messages = Vec::new();
    loop {
        match pip.next() {
            Ok(Some(msg)) => messages.push(owned_message(msg.header().kind, msg.payload())),
            Ok(None) => break,
            Err(e) => {
                set_last_error(e.to_string());
                return false;
            }
        }
    }
    unsafe { *out_messages = Box::into_raw(Box::new(PeerbusMessages { messages })) };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_client_open(
    client: *mut PeerbusPipClient,
    out_pip: *mut *mut PeerbusPip,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_pip.is_null() {
        set_last_error("null pip client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_session() {
        Ok(token) => {
            unsafe {
                *out_pip = Box::into_raw(Box::new(PeerbusPip {
                    client: client_ref.client.clone(),
                    token: Some(token),
                    incoming_done: false,
                    outgoing_done: false,
                }));
            }
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_send(
    pip: *mut PeerbusPip,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("pip session is closed");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.client).send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_finish_send(pip: *mut PeerbusPip) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("pip session is closed");
        return false;
    };
    let result = lock_inner(&pip.client).finish_send_pending(token);
    match result {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_next(
    pip: *mut PeerbusPip,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null pip handle or out pointer");
        return -1;
    }
    let pip = unsafe { &mut *pip };
    if pip.incoming_done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = pip.token else {
        set_last_error("pip session is closed");
        return -1;
    };
    let result = lock_inner(&pip.client).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(msg.header().kind, msg.payload())));
            }
            1
        }
        Ok(None) => {
            pip.incoming_done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_close(pip: *mut PeerbusPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take() {
        lock_inner(&pip_ref.client).close_session(token);
    }
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_free(pip: *mut PeerbusPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPipServer {
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
    match node
        .node
        .pip_server_with_qos::<RawMsg, RawMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusPipServer {
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
pub extern "C" fn peerbus_pip_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusPipServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_pip_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_free(server: *mut PeerbusPipServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_stats(server: *const PeerbusPipServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_responder_send(
    responder: *mut PeerbusMessageResponder,
    kind: u64,
    data: *const u8,
    len: usize,
) {
    ffi_guard((), move || {
    if responder.is_null() {
        return;
    }
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return,
    };
    unsafe { &mut *responder }
        .messages
        .push(RawMsg::new(kind, bytes));
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pip_server_serve_one(
    server: *mut PeerbusPipServer,
    timeout_ms: u64,
    handler: PeerbusPipHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null pip server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null pip handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some(mut pip)) => {
                let mut values = Vec::new();
                loop {
                    match pip.next() {
                        Ok(Some(msg)) => values.push(RawMsg::new(msg.header().kind, msg.payload())),
                        Ok(None) => break,
                        Err(e) => {
                            set_last_error(e.to_string());
                            return -1;
                        }
                    }
                }
                let messages = messages_from_raw(values);
                let mut responder = PeerbusMessageResponder {
                    messages: Vec::new(),
                };
                unsafe { handler(ctx, &messages, &mut responder) };
                for msg in responder.messages {
                    if let Err(e) = pip.send(&msg) {
                        set_last_error(e.to_string());
                        return -1;
                    }
                }
                return match pip.finish_send() {
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
pub extern "C" fn peerbus_pip_server_take(
    server: *mut PeerbusPipServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingPip,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null pip server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_session_id, first, incoming_done, token) = pending.into_parts();
                let handle = PeerbusPendingPip {
                    server: arc.clone(),
                    token: Some(token),
                    first: first.map(|msg| owned_message(msg.header().kind, msg.payload())),
                    incoming_done,
                    outgoing_done: false,
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
pub extern "C" fn peerbus_pending_pip_next(
    pip: *mut PeerbusPendingPip,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null pending pip or out pointer");
        return -1;
    }
    let pip = unsafe { &mut *pip };
    if let Some(first) = pip.first.take() {
        unsafe { *out_message = Box::into_raw(Box::new(first)) };
        return 1;
    }
    if pip.incoming_done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = pip.token else {
        set_last_error("pending pip is closed");
        return -1;
    };
    let result = lock_inner(&pip.server).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(msg.header().kind, msg.payload())));
            }
            1
        }
        Ok(None) => {
            pip.incoming_done = true;
            unsafe { *out_message = ptr::null_mut() };
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_send(
    pip: *mut PeerbusPendingPip,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("pending pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("pending pip is closed");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.server).send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_finish_send(pip: *mut PeerbusPendingPip) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("pending pip is closed");
        return false;
    };
    let result = lock_inner(&pip.server).finish_send_pending(token);
    match result {
        Ok(()) => {
            pip.outgoing_done = true;
            true
        }
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_close(pip: *mut PeerbusPendingPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    let pip_ref = unsafe { &mut *pip };
    if let Some(token) = pip_ref.token.take() {
        lock_inner(&pip_ref.server).close_pending(token);
    }
    pip_ref.first = None;
    pip_ref.incoming_done = true;
    pip_ref.outgoing_done = true;
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_pip_free(pip: *mut PeerbusPendingPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_pending_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

