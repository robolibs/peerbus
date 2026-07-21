use super::*;

// ---- generic datapod put/ack ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPutClient {
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
    let result = match peer {
        Ok(addr) => node
            .node
            .put_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos),
        Err(name) => {
            node.node
                .put_client_with_qos::<DatapodMsg, DatapodMsg>(name.as_str(), topic, qos)
        }
    };
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPutClient {
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
pub extern "C" fn peerbus_datapod_put_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPutClient {
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
        .put_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPutClient {
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
pub extern "C" fn peerbus_datapod_put_client_free(client: *mut PeerbusDatapodPutClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_stats(
    client: *const PeerbusDatapodPutClient,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*client }.client).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_upload(
    client: *mut PeerbusDatapodPutClient,
    items: *const PeerbusDatapodRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null datapod put client or out pointer");
        return false;
    }
    let raw_items = match unsafe { datapod_raw_messages_in(items, len) } {
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
        if let Err(e) = sender.send(&message_from_datapod_raw(*raw)) {
            set_last_error(e.to_string());
            return false;
        }
    }
    match sender.finish() {
        Ok(ack) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    ack.header().type_hash,
                    ack.payload(),
                )));
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
pub extern "C" fn peerbus_datapod_put_client_put(
    client: *mut PeerbusDatapodPutClient,
    items: *const PeerbusDatapodRawMessage,
    len: usize,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_client_upload(client, items, len, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_client_open(
    client: *mut PeerbusDatapodPutClient,
    out_upload: *mut *mut PeerbusDatapodPutUpload,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_upload.is_null() {
        set_last_error("null datapod put client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_upload() {
        Ok(token) => {
            unsafe {
                *out_upload = Box::into_raw(Box::new(PeerbusDatapodPutUpload {
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
pub extern "C" fn peerbus_datapod_put_client_open_sender(
    client: *mut PeerbusDatapodPutClient,
    out_sender: *mut *mut PeerbusDatapodPutSender,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_client_open(client, out_sender)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_upload_send(
    upload: *mut PeerbusDatapodPutUpload,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() {
        set_last_error("null datapod put upload handle");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token else {
        set_last_error("datapod put upload already finished");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&upload.client).send_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_sender_send(
    sender: *mut PeerbusDatapodPutSender,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_upload_send(sender, type_hash, wire, len)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_upload_finish(
    upload: *mut PeerbusDatapodPutUpload,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if upload.is_null() || out_message.is_null() {
        set_last_error("null datapod put upload or out pointer");
        return false;
    }
    let upload = unsafe { &mut *upload };
    let Some(token) = upload.token.take() else {
        set_last_error("datapod put upload already finished");
        return false;
    };
    match lock_inner(&upload.client).finish_pending(token) {
        Ok(ack) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    ack.header().type_hash,
                    ack.payload(),
                )));
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
pub extern "C" fn peerbus_datapod_put_sender_finish(
    sender: *mut PeerbusDatapodPutSender,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    peerbus_datapod_put_upload_finish(sender, out_message)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_upload_free(upload: *mut PeerbusDatapodPutUpload) {
    ffi_guard((), move || {
    if upload.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(upload)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_put_sender_free(sender: *mut PeerbusDatapodPutSender) {
    ffi_guard((), move || {
    peerbus_datapod_put_upload_free(sender);
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodAckServer {
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
        .ack_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodAckServer {
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
pub extern "C" fn peerbus_datapod_ack_server_free(server: *mut PeerbusDatapodAckServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_stats(
    server: *const PeerbusDatapodAckServer,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_take(
    server: *mut PeerbusDatapodAckServer,
    timeout_ms: u64,
    out_puts: *mut *mut PeerbusDatapodPuts,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_puts.is_null() {
        set_last_error("null datapod ack server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_req_id, first, done, token) = pending.into_parts();
                let handle = PeerbusDatapodPuts {
                    server: arc.clone(),
                    token: Some(token),
                    first: first
                        .map(|msg| owned_datapod_message(msg.header().type_hash, msg.payload())),
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
pub extern "C" fn peerbus_datapod_puts_next(
    puts: *mut PeerbusDatapodPuts,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if puts.is_null() || out_message.is_null() {
        set_last_error("null datapod puts or out pointer");
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
        set_last_error("datapod puts handle already acked or closed");
        return -1;
    };
    let result = lock_inner(&puts.server).next_pending(token);
    match result {
        Ok(Some(msg)) => {
            unsafe {
                *out_message = Box::into_raw(Box::new(owned_datapod_message(
                    msg.header().type_hash,
                    msg.payload(),
                )));
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
pub extern "C" fn peerbus_datapod_puts_ack(
    puts: *mut PeerbusDatapodPuts,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if puts.is_null() {
        set_last_error("null datapod puts handle");
        return false;
    }
    let puts = unsafe { &mut *puts };
    let Some(token) = puts.token.take() else {
        set_last_error("datapod puts handle already acked or closed");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&puts.server).ack_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_puts_free(puts: *mut PeerbusDatapodPuts) {
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

