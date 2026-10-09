use super::*;

// ---- put/ack ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPutClient {
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
            .put_client_with_qos::<RawMsg, RawMsg>(peer, topic, qos);
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusPutClient {
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
pub extern "C" fn peerbus_put_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusPutClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_put_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}


#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_free(client: *mut PeerbusPutClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_stats(client: *const PeerbusPutClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*client }.client).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_upload(
    client: *mut PeerbusPutClient,
    items: *const PeerbusRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null put client or out pointer");
        return false;
    }
    let raw_items = match unsafe { raw_messages_in(items, len) } {
        Ok(items) => items,
        Err(()) => return false,
    };
    let client = unsafe { &*client };
    let mut guard = lock_inner(&client.client);
    let mut sender = match guard.open() {
        Ok(sender) => sender,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    for raw in raw_items {
        if let Err(e) = sender.send(&message_from_raw(*raw)) {
            set_last_error(e.to_string());
            return false;
        }
    }
    match sender.finish() {
        Ok(ack) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(ack.header().kind, ack.payload())));
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
pub extern "C" fn peerbus_put_client_put(
    client: *mut PeerbusPutClient,
    items: *const PeerbusRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_client_upload(client, items, len, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_client_open(
    client: *mut PeerbusPutClient,
    out_upload: *mut *mut PeerbusPutUpload,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_upload.is_null() {
        set_last_error("null put client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_upload() {
        Ok(token) => {
            unsafe {
                *out_upload = Box::into_raw(Box::new(PeerbusPutUpload {
                    client: client_ref.client.clone(),
                    token: Some(token),
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
pub extern "C" fn peerbus_put_client_open_sender(
    client: *mut PeerbusPutClient,
    out_sender: *mut *mut PeerbusPutSender,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_client_open(client, out_sender)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_upload_send(
    upload: *mut PeerbusPutUpload,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() {
        set_last_error("null put upload handle");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token else {
        set_last_error("put upload already finished");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&upload.client).send_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_sender_send(
    sender: *mut PeerbusPutSender,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_upload_send(sender, kind, data, len)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_upload_finish(
    upload: *mut PeerbusPutUpload,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() || out_message.is_null() {
        set_last_error("null put upload or out pointer");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token.take() else {
        set_last_error("put upload already finished");
        return false;
    };
    match lock_inner(&upload.client).finish_pending(token) {
        Ok(ack) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(ack.header().kind, ack.payload())));
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
pub extern "C" fn peerbus_put_sender_finish(
    sender: *mut PeerbusPutSender,
    out_message: *mut *mut PeerbusMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_put_upload_finish(sender, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_upload_free(upload: *mut PeerbusPutUpload) {
    ffi_guard((), move || {
    if upload.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(upload)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_put_sender_free(sender: *mut PeerbusPutSender) {
    ffi_guard((), move || {
    peerbus_put_upload_free(sender);
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusAckServer {
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
    match node.node.ack_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusAckServer {
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
pub extern "C" fn peerbus_ack_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusAckServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_ack_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_free(server: *mut PeerbusAckServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_stats(server: *const PeerbusAckServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ack_server_serve_one(
    server: *mut PeerbusAckServer,
    timeout_ms: u64,
    handler: PeerbusAckHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null ack server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ack handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some(mut puts)) => {
                let mut values = Vec::new();
                loop {
                    match puts.next() {
                        Ok(Some(put)) => values.push(RawMsg::new(put.header().kind, put.payload())),
                        Ok(None) => break,
                        Err(e) => {
                            set_last_error(e.to_string());
                            return -1;
                        }
                    }
                }
                let messages = messages_from_raw(values);
                let mut responder = PeerbusResponder {
                    kind: 0,
                    data: Vec::new(),
                    set: false,
                };
                unsafe { handler(ctx, &messages, &mut responder) };
                let ack = RawMsg::new(responder.kind, &responder.data);
                return match puts.ack(&ack) {
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
pub extern "C" fn peerbus_ack_server_take(
    server: *mut PeerbusAckServer,
    timeout_ms: u64,
    out_puts: *mut *mut PeerbusPuts,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_puts.is_null() {
        set_last_error("null ack server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_req_id, first, done, token) = pending.into_parts();
                let handle = PeerbusPuts {
                    server: arc.clone(),
                    token: Some(token),
                    first: first.map(|msg| owned_message(msg.header().kind, msg.payload())),
                    done,
                };
                unsafe { *out_puts = Box::into_raw(Box::new(handle)) };
                return 1;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    unsafe { *out_puts = ptr::null_mut() };
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
pub extern "C" fn peerbus_puts_next(
    puts: *mut PeerbusPuts,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if puts.is_null() || out_message.is_null() {
        set_last_error("null puts or out pointer");
        return -1;
    }
    let puts = unsafe { &mut *puts };
    if let Some(first) = puts.first.take() {
        unsafe { *out_message = Box::into_raw(Box::new(first)) };
        return 1;
    }
    if puts.done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = puts.token else {
        set_last_error("puts handle already acked or closed");
        return -1;
    };
    let result = lock_inner(&puts.server).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message =
                    Box::into_raw(Box::new(owned_message(msg.header().kind, msg.payload())));
            }
            1
        }
        Ok(None) => {
            puts.done = true;
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
pub extern "C" fn peerbus_puts_ack(
    puts: *mut PeerbusPuts,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if puts.is_null() {
        set_last_error("null puts handle");
        return false;
    }
    let puts = unsafe { &mut *puts };
    let Some(token) = puts.token.take() else {
        set_last_error("puts handle already acked or closed");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&puts.server).ack_pending(token, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_puts_free(puts: *mut PeerbusPuts) {
    ffi_guard((), move || {
    if puts.is_null() {
        return;
    }
    let puts_ref = unsafe { &mut *puts };
    if let Some(token) = puts_ref.token.take() {
        lock_inner(&puts_ref.server).close_pending(token);
    }
    unsafe { drop(Box::from_raw(puts)) };
})
}

