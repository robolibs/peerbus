use super::*;

// Preferred que/ans answer-list aliases. These wrap `PeerbusMessages` so old
// `messages` accessors and new `answers` accessors remain ownership-compatible.

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_len(answers: *const PeerbusAnswers) -> usize {
    ffi_guard(0, move || {
    peerbus_messages_len(answers)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_kind_at(answers: *const PeerbusAnswers, index: usize) -> u64 {
    ffi_guard(0, move || {
    peerbus_messages_kind_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_data_at(
    answers: *const PeerbusAnswers,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    peerbus_messages_data_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_answers_free(answers: *mut PeerbusAnswers) {
    ffi_guard((), move || {
    peerbus_messages_free(answers);
})
}

// ---- que/ans ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusQueClient {
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
            .que_client_with_qos::<RawMsg, RawMsg>(peer, topic, qos);
    match result {
        Ok(client) => Box::into_raw(Box::new(PeerbusQueClient { client })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusQueClient {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_que_client_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}


#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_free(client: *mut PeerbusQueClient) {
    ffi_guard((), move || {
    if client.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(client)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_stats(client: *const PeerbusQueClient) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if client.is_null() {
        return PeerbusItemStats::default();
    }
    item_stats_out(unsafe { &*client }.client.stats())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_que_client_send(
    client: *mut PeerbusQueClient,
    kind: u64,
    data: *const u8,
    len: usize,
    out_messages: *mut *mut PeerbusMessages,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if client.is_null() || out_messages.is_null() {
        set_last_error("null que client or out pointer");
        return false;
    }
    let client = unsafe { &mut *client };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    let mut answers = match client.client.send(&RawMsg::new(kind, bytes)) {
        Ok(answers) => answers,
        Err(e) => {
            set_last_error(e.to_string());
            return false;
        }
    };
    let mut messages = Vec::new();
    loop {
        match answers.next() {
            Ok(Some(ans)) => messages.push(owned_message(ans.header().kind, ans.payload())),
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
pub extern "C" fn peerbus_ans_server_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusAnsServer {
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
    match node.node.ans_with_qos::<RawMsg, RawMsg>(topic, qos) {
        Ok(server) => {
            Box::into_raw(Box::new(PeerbusAnsServer {
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
pub extern "C" fn peerbus_ans_server_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusAnsServer {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_ans_server_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_free(server: *mut PeerbusAnsServer) {
    ffi_guard((), move || {
    if server.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(server)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_server_stats(server: *const PeerbusAnsServer) -> PeerbusItemStats {
    ffi_guard(PeerbusItemStats::default(), move || {
    if server.is_null() {
        return PeerbusItemStats::default();
    }
    let stats = lock_inner(&unsafe { &*server }.server).stats();
    item_stats_out(stats)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_ans_responder_send(
    responder: *mut PeerbusAnsResponder,
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
pub extern "C" fn peerbus_ans_server_serve_one(
    server: *mut PeerbusAnsServer,
    timeout_ms: u64,
    handler: PeerbusAnsHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null ans server handle");
        return -1;
    }
    let Some(handler) = handler else {
        set_last_error("null ans handler");
        return -1;
    };
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take() {
            Ok(Some((que, mut ans))) => {
                let mut responder = PeerbusAnsResponder {
                    messages: Vec::new(),
                };
                unsafe {
                    handler(
                        ctx,
                        que.header().kind,
                        que.payload().as_ptr(),
                        que.payload().len(),
                        &mut responder,
                    )
                };
                for msg in responder.messages {
                    if let Err(e) = ans.send(&msg) {
                        set_last_error(e.to_string());
                        return -1;
                    }
                }
                return match ans.finish() {
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
pub extern "C" fn peerbus_ans_server_take(
    server: *mut PeerbusAnsServer,
    timeout_ms: u64,
    out_pending: *mut *mut PeerbusPendingQue,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() || out_pending.is_null() {
        set_last_error("null ans server or out pointer");
        return -1;
    }
    let arc = &unsafe { &*server }.server;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut guard = lock_inner(arc);
        match guard.take_message() {
            Ok(Some(pending)) => {
                let (que, answers) = pending.into_parts();
                let handle = PeerbusPendingQue {
                    server: arc.clone(),
                    answers: Some(answers),
                    request: owned_message(que.header().kind, que.payload()),
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
pub extern "C" fn peerbus_pending_que_request(
    pending: *const PeerbusPendingQue,
) -> *const PeerbusMessage {
    ffi_guard(ptr::null(), move || {
    if pending.is_null() {
        return ptr::null();
    }
    &unsafe { &*pending }.request as *const PeerbusMessage
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_que_send(
    pending: *mut PeerbusPendingQue,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending que handle");
        return false;
    }
    let pending = unsafe { &mut *pending };
    let Some(answers) = pending.answers else {
        set_last_error("pending que already finished");
        return false;
    };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match lock_inner(&pending.server).send_pending(answers, &RawMsg::new(kind, bytes)) {
        Ok(()) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_pending_que_finish(pending: *mut PeerbusPendingQue) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if pending.is_null() {
        set_last_error("null pending que handle");
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
pub extern "C" fn peerbus_pending_que_free(pending: *mut PeerbusPendingQue) {
    ffi_guard((), move || {
    if pending.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(pending)) };
})
}

