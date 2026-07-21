use super::*;

// ---- generic datapod pip ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPipClient {
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
            .pip_client_with_qos::<DatapodMsg, DatapodMsg>(addr, topic, qos),
        Err(name) => {
            node.node
                .pip_client_with_qos::<DatapodMsg, DatapodMsg>(name.as_str(), topic, qos)
        }
    };
    match result {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPipClient {
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
pub extern "C" fn peerbus_datapod_pip_system_client_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPipClient {
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
        .pip_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(client) => {
            Box::into_raw(Box::new(PeerbusDatapodPipClient {
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
pub extern "C" fn peerbus_datapod_pip_client_free(client: *mut PeerbusDatapodPipClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_stats(
    client: *const PeerbusDatapodPipClient,
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
pub extern "C" fn peerbus_datapod_pip_client_exchange(
    client: *mut PeerbusDatapodPipClient,
    items: *const PeerbusDatapodRawMessage,
    len: usize,
    out_messages: *mut *mut PeerbusDatapodMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null datapod pip client or out pointer");
        return false;
    }
    let raw_items = match unsafe { datapod_raw_messages_in(items, len) } {
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
        if let Err(e) = pip.send(&message_from_datapod_raw(*raw)) {
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
            Ok(Some(msg)) => {
                messages.push(owned_datapod_message(msg.header().type_hash, msg.payload()))
            }
            Ok(None) => break,
            Err(e) => {
                set_last_error(e.to_string());
                return false;
            }
        }
    }
    unsafe { *out_messages = Box::into_raw(Box::new(PeerbusDatapodMessages { messages })) };
    true
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_client_open(
    client: *mut PeerbusDatapodPipClient,
    out_pip: *mut *mut PeerbusDatapodPip,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_pip.is_null() {
        set_last_error("null datapod pip client or out pointer");
        return false;
    }
    let client_ref = unsafe { &*client };
    match lock_inner(&client_ref.client).open_session() {
        Ok(token) => {
            unsafe {
                *out_pip = Box::into_raw(Box::new(PeerbusDatapodPip {
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
pub extern "C" fn peerbus_datapod_pip_send(
    pip: *mut PeerbusDatapodPip,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("datapod pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("datapod pip session is closed");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.client).send_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_finish_send(pip: *mut PeerbusDatapodPip) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("datapod pip session is closed");
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
pub extern "C" fn peerbus_datapod_pip_next(
    pip: *mut PeerbusDatapodPip,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null datapod pip handle or out pointer");
        return -1;
    }
    let pip = unsafe { &mut *pip };
    if pip.incoming_done {
        unsafe { *out_message = ptr::null_mut() };
        return 0;
    }
    let Some(token) = pip.token else {
        set_last_error("datapod pip session is closed");
        return -1;
    };
    let result = lock_inner(&pip.client).next_pending(token);
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
pub extern "C" fn peerbus_datapod_pip_close(pip: *mut PeerbusDatapodPip) {
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
pub extern "C" fn peerbus_datapod_pip_free(pip: *mut PeerbusDatapodPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_datapod_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPipServer {
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
        .pip_server_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodPipServer {
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
pub extern "C" fn peerbus_datapod_pip_server_free(server: *mut PeerbusDatapodPipServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_stats(
    server: *const PeerbusDatapodPipServer,
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
pub extern "C" fn peerbus_datapod_pip_server_take(
    server: *mut PeerbusDatapodPipServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingDatapodPip,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null datapod pip server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (_session_id, first, incoming_done, token) = pending.into_parts();
                let handle = PeerbusPendingDatapodPip {
                    server: arc.clone(),
                    token: Some(token),
                    first: first
                        .map(|msg| owned_datapod_message(msg.header().type_hash, msg.payload())),
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
pub extern "C" fn peerbus_pending_datapod_pip_next(
    pip: *mut PeerbusPendingDatapodPip,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if pip.is_null() || out_message.is_null() {
        set_last_error("null pending datapod pip or out pointer");
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
        set_last_error("pending datapod pip is closed");
        return -1;
    };
    let result = lock_inner(&pip.server).next_pending(token);
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
pub extern "C" fn peerbus_pending_datapod_pip_send(
    pip: *mut PeerbusPendingDatapodPip,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        set_last_error("pending datapod pip outgoing direction is done");
        return false;
    }
    let Some(token) = pip.token else {
        set_last_error("pending datapod pip is closed");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pip.server).send_pending(token, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_pip_finish_send(
    pip: *mut PeerbusPendingDatapodPip,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pip.is_null() {
        set_last_error("null pending datapod pip handle");
        return false;
    }
    let pip = unsafe { &mut *pip };
    if pip.outgoing_done {
        return true;
    }
    let Some(token) = pip.token else {
        set_last_error("pending datapod pip is closed");
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
pub extern "C" fn peerbus_pending_datapod_pip_close(pip: *mut PeerbusPendingDatapodPip) {
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
pub extern "C" fn peerbus_pending_datapod_pip_free(pip: *mut PeerbusPendingDatapodPip) {
    ffi_guard((), move || {
    if pip.is_null() {
        return;
    }
    peerbus_pending_datapod_pip_close(pip);
    unsafe { drop(Box::from_raw(pip)) };
})
}

