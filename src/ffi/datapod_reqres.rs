use super::*;

// ---- generic datapod req/res client ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodReqClient {
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
            .req_client_with_qos::<DatapodMsg, DatapodMsg>(peer, topic, qos);
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusDatapodReqClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}


#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_free(client: *mut PeerbusDatapodReqClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_stats(
    client: *const PeerbusDatapodReqClient,
) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_client_call(
    client: *mut PeerbusDatapodReqClient,
    type_hash: u64,
    wire: *const u8,
    len: usize,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_message.is_null() {
        set_last_error("null datapod req client or out pointer");
        return false;
    }
    let client = unsafe { &mut *client };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match client.client.call(&DatapodMsg::new(type_hash, wire)) {
        Ok(res) => {
            let msg = owned_datapod_message(res.header().type_hash, res.payload());
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

// ---- generic datapod req/res server ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodReqServer {
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
        .req_server_with_qos::<DatapodMsg, DatapodMsg>(topic, qos)
    {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusDatapodReqServer {
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
pub extern "C" fn peerbus_datapod_req_server_free(server: *mut PeerbusDatapodReqServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_stats(
    server: *const PeerbusDatapodReqServer,
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
pub extern "C" fn peerbus_datapod_req_server_take(
    server: *mut PeerbusDatapodReqServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingDatapodReq,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null datapod req server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (req, reply) = pending.into_parts();
                let request = owned_datapod_message(req.header().type_hash, req.payload());
                drop(guard);
                let handle = PeerbusPendingDatapodReq {
                    server: arc.clone(),
                    reply: Some(reply),
                    request,
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
pub extern "C" fn peerbus_pending_datapod_req_request(
    pending: *const PeerbusPendingDatapodReq,
) -> *const PeerbusDatapodMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusDatapodMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_req_reply(
    pending: *mut PeerbusPendingDatapodReq,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending datapod req handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(reply) = pending.reply.take() else {
        set_last_error("pending datapod req already replied");
        return false;
    };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).respond_pending(reply, &DatapodMsg::new(type_hash, wire)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_datapod_req_free(pending: *mut PeerbusPendingDatapodReq) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

