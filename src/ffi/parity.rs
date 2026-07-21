use super::*;

// ---- datapod pub/sub parity with the raw family ----

/// No-QoS datapod publisher, mirroring [`peerbus_publisher_new`].
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusDatapodPublisher {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_datapod_publisher_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_stats(
    publisher: *const PeerbusDatapodPublisher,
) -> PeerbusPublisherStats {
    ffi_guard(PeerbusPublisherStats::default(), move || {
    if publisher.is_null() {
        return PeerbusPublisherStats::default();
    }
    let stats = unsafe { &*publisher }.publisher.stats();
    PeerbusPublisherStats {
        published: stats.published,
        remote_dropped: stats.remote_dropped,
        stale_dropped: stats.stale_dropped,
        bytes_sent: stats.bytes_sent,
        send_errors: stats.send_errors,
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_stats(
    subscriber: *const PeerbusDatapodSubscriber,
) -> PeerbusSubscriberStats {
    ffi_guard(PeerbusSubscriberStats::default(), move || {
    if subscriber.is_null() {
        return PeerbusSubscriberStats::default();
    }
    let stats = unsafe { &*subscriber }.subscriber.stats();
    PeerbusSubscriberStats {
        received: stats.received,
        disconnects: stats.disconnects,
        stale_dropped: stats.stale_dropped,
        incomplete_dropped: stats.incomplete_dropped,
        bytes_received: stats.bytes_received,
    }
})
}

/// Poll for the next datapod sample as an owned message, mirroring the raw
/// [`peerbus_subscriber_take`]. Returns `1` and writes an owned message to
/// `*out_message` when one is available, `0` when none is ready, and `-1`
/// on error. Free the message with [`peerbus_datapod_message_free`].
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_take(
    subscriber: *mut PeerbusDatapodSubscriber,
    out_message: *mut *mut PeerbusDatapodMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_message.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            let msg = owned_datapod_message(sample.header().type_hash, sample.payload());
            unsafe { *out_message = Box::into_raw(Box::new(msg)) };
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
})
}

// ---- datapod server serve_one parity with the raw family ----

/// Datapod req/res `serve_one`, mirroring [`peerbus_req_server_serve_one`].
/// The handler's `kind` argument carries the request `type_hash`, and the
/// responder's `kind` becomes the response `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_req_server_serve_one(
    server: *mut PeerbusDatapodReqServer,
    timeout_ms: u64,
    handler: PeerbusReqHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod req server handle");
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
                let type_hash = req.header().type_hash;
                let payload = req.payload();
                let mut responder = PeerbusResponder {
                    kind: 0,
                    data: Vec::new(),
                    set: false,
                };
                unsafe {
                    handler(
                        ctx,
                        type_hash,
                        payload.as_ptr(),
                        payload.len(),
                        &mut responder as *mut PeerbusResponder,
                    )
                };
                let response = DatapodMsg::new(responder.kind, &*responder.data);
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

/// Datapod que/ans `serve_one`, mirroring [`peerbus_ans_server_serve_one`].
/// Answer items pushed via [`peerbus_ans_responder_send`] use their `kind`
/// argument as the answer `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ans_server_serve_one(
    server: *mut PeerbusDatapodAnsServer,
    timeout_ms: u64,
    handler: PeerbusAnsHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod ans server handle");
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
                        que.header().type_hash,
                        que.payload().as_ptr(),
                        que.payload().len(),
                        &mut responder,
                    )
                };
                for msg in responder.messages {
                    if let Err(e) = ans.send(&DatapodMsg::new(msg.kind, &*msg.data)) {
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

/// Datapod put/ack `serve_one`, mirroring [`peerbus_ack_server_serve_one`].
/// Uploaded items are exposed via `items` with each element's `kind`
/// carrying the `type_hash`; the responder's `kind` becomes the ack
/// `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_ack_server_serve_one(
    server: *mut PeerbusDatapodAckServer,
    timeout_ms: u64,
    handler: PeerbusAckHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod ack server handle");
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
                        Ok(Some(put)) => {
                            values.push(RawMsg::new(put.header().type_hash, put.payload()))
                        }
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
                let ack = DatapodMsg::new(responder.kind, &*responder.data);
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

/// Datapod pip `serve_one`, mirroring [`peerbus_pip_server_serve_one`].
/// Incoming items are exposed via `items` with each element's `kind`
/// carrying the `type_hash`; reply items pushed via
/// [`peerbus_message_responder_send`] use their `kind` as the `type_hash`.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_pip_server_serve_one(
    server: *mut PeerbusDatapodPipServer,
    timeout_ms: u64,
    handler: PeerbusPipHandler,
    ctx: *mut c_void,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if server.is_null() {
        set_last_error("null datapod pip server handle");
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
                        Ok(Some(msg)) => {
                            values.push(RawMsg::new(msg.header().type_hash, msg.payload()))
                        }
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
                    if let Err(e) = pip.send(&DatapodMsg::new(msg.kind, &*msg.data)) {
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
