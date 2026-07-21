use super::*;

// ---- generic datapod que/ans ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodQueClient {
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
            .que_client_with_qos::<DatapodMsg, DatapodMsg>(peer, topic, qos);
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusDatapodQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}


#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_free(client: *mut PeerbusDatapodQueClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_stats(
    client: *const PeerbusDatapodQueClient,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_que_client_send(
    client: *mut PeerbusDatapodQueClient,
    type_hash: u64,
    wire: *const u8,
    len: usize,
    out_messages: *mut *mut PeerbusDatapodMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null datapod que client or out pointer");
        return false;
    }
    let client = unsafe { &mut *client };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    let mut answers = match client.client.send(&DatapodMsg::new(type_hash, wire)) {
        Ok(answers) => answers,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    let mut messages = Vec::new();
    loop {
        match answers.next() {
            Ok(Some(ans)) => {
                messages.push(owned_datapod_message(ans.header().type_hash, ans.payload()))
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
pub extern "C" fn peerbus_datapod_ans_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodAnsServer {
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
        .ans_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodAnsServer {
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
pub extern "C" fn peerbus_datapod_ans_server_free(server: *mut PeerbusDatapodAnsServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ans_server_stats(
    server: *const PeerbusDatapodAnsServer,
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
pub extern "C" fn peerbus_datapod_ans_server_take(
    server: *mut PeerbusDatapodAnsServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingDatapodQue,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null datapod ans server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (que, answers) = pending.into_parts();
                let handle = PeerbusPendingDatapodQue {
                    server: arc.clone(),
                    answers: Some(answers),
                    request: owned_datapod_message(que.header().type_hash, que.payload()),
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
pub extern "C" fn peerbus_pending_datapod_que_request(
    pending: *const PeerbusPendingDatapodQue,
) -> *const PeerbusDatapodMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusDatapodMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_que_send(
    pending: *mut PeerbusPendingDatapodQue,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending datapod que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers else {
        set_last_error("pending datapod que already finished");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).send_pending(answers, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_que_finish(
    pending: *mut PeerbusPendingDatapodQue,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending datapod que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers.take() else {
        return true;
    };
    match lock_inner(&pending.server).finish_pending(answers) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_que_free(pending: *mut PeerbusPendingDatapodQue) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

