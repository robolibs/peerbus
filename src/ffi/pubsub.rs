use super::*;

// ---- pub/sub ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_new(
    node: *const PeerbusNode,
    topic: *const c_char,
) -> *mut PeerbusPublisher {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_publisher_new_with_qos(node, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusPublisher {
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
    match node.node.publisher_with_qos::<RawMsg>(topic, qos) {
        Ok(publisher) => Box::into_raw(Box::new(PeerbusPublisher { publisher })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_free(publisher: *mut PeerbusPublisher) {
    ffi_guard((), move || {
    if publisher.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(publisher)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_stats(
    publisher: *const PeerbusPublisher,
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

/// Publish `data` (`len` bytes) with user tag `kind`. Returns false on
/// failure.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_publisher_send(
    publisher: *mut PeerbusPublisher,
    kind: u64,
    data: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if publisher.is_null() {
        set_last_error("null publisher handle");
        return false;
    }
    // SAFETY: validated non-null.
    let publisher = unsafe { &mut *publisher };
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match publisher.publisher.send(&RawMsg::new(kind, bytes)) {
        Ok(_) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_new(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
) -> *mut PeerbusSubscriber {
    ffi_guard(ptr::null_mut(), move || {
    peerbus_subscriber_new_with_qos(node, peer, topic, peerbus_topic_qos_reliable())
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusSubscriber {
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
    let result = node.node.subscriber_with_qos::<RawMsg>(peer, topic, qos);
    match result {
        Ok(subscriber) => Box::into_raw(Box::new(PeerbusSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}


#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_free(subscriber: *mut PeerbusSubscriber) {
    ffi_guard((), move || {
    if subscriber.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(subscriber)) };
})
}

// ---- generic datapod pub/sub ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_new_with_qos(
    node: *const PeerbusNode,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodPublisher {
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
        .publisher_with_qos::<DatapodMsg>(topic, qos)
    {
        Ok(publisher) => Box::into_raw(Box::new(PeerbusDatapodPublisher { publisher })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}


#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_free(publisher: *mut PeerbusDatapodPublisher) {
    ffi_guard((), move || {
    if publisher.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(publisher)) };
})
}

/// Publish a datapod wire message: `type_hash` plus `header || payload` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_publisher_send(
    publisher: *mut PeerbusDatapodPublisher,
    type_hash: u64,
    wire: *const u8,
    len: usize,
) -> bool {
    ffi_guard(false, move || {
    clear_last_error();
    if publisher.is_null() {
        set_last_error("null publisher handle");
        return false;
    }
    let publisher = unsafe { &mut *publisher };
    let wire = match unsafe { bytes_in(wire, len) } {
        Ok(v) => v,
        Err(()) => return false,
    };
    match publisher.publisher.send(&DatapodMsg::new(type_hash, wire)) {
        Ok(_) => true,
        Err(e) => {
            set_last_error(e.to_string());
            false
        }
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_new_with_qos(
    node: *const PeerbusNode,
    peer: *const c_char,
    topic: *const c_char,
    qos: PeerbusTopicQos,
) -> *mut PeerbusDatapodSubscriber {
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
            .subscriber_with_qos::<DatapodMsg>(peer, topic, qos);
    match result {
        Ok(subscriber) => Box::into_raw(Box::new(PeerbusDatapodSubscriber { subscriber })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
})
}



#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_free(subscriber: *mut PeerbusDatapodSubscriber) {
    ffi_guard((), move || {
    if subscriber.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(subscriber)) };
})
}

/// Poll for a datapod sample without copying the wire bytes.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_subscriber_take_sample(
    subscriber: *mut PeerbusDatapodSubscriber,
    out_sample: *mut *mut PeerbusDatapodSample,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_sample.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            unsafe { *out_sample = Box::into_raw(Box::new(PeerbusDatapodSample { sample })) };
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

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_sample_type_hash(sample: *const PeerbusDatapodSample) -> u64 {
    ffi_guard(0, move || {
    if sample.is_null() {
        return 0;
    }
    unsafe { &*sample }.sample.header().type_hash
})
}

/// Borrowed zero-copy view of datapod `header || payload` wire bytes.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_sample_wire(sample: *const PeerbusDatapodSample) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if sample.is_null() {
        return PeerbusBytes::empty();
    }
    let sample = unsafe { &*sample };
    PeerbusBytes {
        ptr: sample.sample.payload().as_ptr(),
        len: sample.sample.payload().len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_sample_free(sample: *mut PeerbusDatapodSample) {
    ffi_guard((), move || {
    if sample.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(sample)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_message_type_hash(message: *const PeerbusDatapodMessage) -> u64 {
    ffi_guard(0, move || {
    if message.is_null() {
        return 0;
    }
    unsafe { &*message }.type_hash
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_message_wire(
    message: *const PeerbusDatapodMessage,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if message.is_null() {
        return PeerbusBytes::empty();
    }
    let message = unsafe { &*message };
    PeerbusBytes {
        ptr: message.wire.as_ptr(),
        len: message.wire.len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_message_free(message: *mut PeerbusDatapodMessage) {
    ffi_guard((), move || {
    if message.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(message)) };
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_len(messages: *const PeerbusDatapodMessages) -> usize {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }.messages.len()
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_type_hash_at(
    messages: *const PeerbusDatapodMessages,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    if messages.is_null() {
        return 0;
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map_or(0, |message| message.type_hash)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_wire_at(
    messages: *const PeerbusDatapodMessages,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if messages.is_null() {
        return PeerbusBytes::empty();
    }
    unsafe { &*messages }
        .messages
        .get(index)
        .map_or_else(PeerbusBytes::empty, |message| PeerbusBytes {
            ptr: message.wire.as_ptr(),
            len: message.wire.len(),
        })
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_messages_free(messages: *mut PeerbusDatapodMessages) {
    ffi_guard((), move || {
    if messages.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(messages)) };
})
}

// Preferred que/ans answer-list aliases. These wrap the shared finite-list
// storage used by the older `messages` names so callers can use the public
// three-letter `ans` terminology without a second ownership model.

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_len(answers: *const PeerbusDatapodAnswers) -> usize {
    ffi_guard(0, move || {
    peerbus_datapod_messages_len(answers)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_type_hash_at(
    answers: *const PeerbusDatapodAnswers,
    index: usize,
) -> u64 {
    ffi_guard(0, move || {
    peerbus_datapod_messages_type_hash_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_wire_at(
    answers: *const PeerbusDatapodAnswers,
    index: usize,
) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    peerbus_datapod_messages_wire_at(answers, index)
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_datapod_answers_free(answers: *mut PeerbusDatapodAnswers) {
    ffi_guard((), move || {
    peerbus_datapod_messages_free(answers);
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_stats(
    subscriber: *const PeerbusSubscriber,
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

/// Poll for the next sample. Returns `1` and writes an owned message to
/// `*out_message` when one is available, `0` when none is ready, and
/// `-1` on error. A returned message must be freed with
/// [`peerbus_message_free`].
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_take(
    subscriber: *mut PeerbusSubscriber,
    out_message: *mut *mut PeerbusMessage,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_message.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    // SAFETY: validated non-null.
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            let msg = PeerbusMessage {
                kind: sample.header().kind,
                data: sample.payload().to_vec(),
            };
            // SAFETY: out_message validated non-null.
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

/// Poll for the next sample without copying payload bytes.
///
/// Returns `1` and writes a borrowed sample handle to `*out_sample` when one is
/// available, `0` when none is ready, and `-1` on error. A returned sample must
/// be freed with [`peerbus_sample_free`]. The byte view returned from
/// [`peerbus_sample_data`] is valid until that free call.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_subscriber_take_sample(
    subscriber: *mut PeerbusSubscriber,
    out_sample: *mut *mut PeerbusSample,
) -> i32 {
    ffi_guard(-1, move || {
    clear_last_error();
    if subscriber.is_null() || out_sample.is_null() {
        set_last_error("null subscriber or out pointer");
        return -1;
    }
    let subscriber = unsafe { &mut *subscriber };
    match subscriber.subscriber.take() {
        Ok(Some(sample)) => {
            unsafe { *out_sample = Box::into_raw(Box::new(PeerbusSample { sample })) };
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

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_sample_kind(sample: *const PeerbusSample) -> u64 {
    ffi_guard(0, move || {
    if sample.is_null() {
        return 0;
    }
    unsafe { &*sample }.sample.header().kind
})
}

/// Borrowed zero-copy view of a sample payload.
///
/// For local SHM this points directly into the shared-memory slot and pins that
/// slot until [`peerbus_sample_free`] is called. Copy it if you need to keep the
/// data longer.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_sample_data(sample: *const PeerbusSample) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if sample.is_null() {
        return PeerbusBytes::empty();
    }
    let sample = unsafe { &*sample };
    PeerbusBytes {
        ptr: sample.sample.payload().as_ptr(),
        len: sample.sample.payload().len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_sample_free(sample: *mut PeerbusSample) {
    ffi_guard((), move || {
    if sample.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(sample)) };
})
}

// ---- message accessors ----

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_new(
    kind: u64,
    data: *const u8,
    len: usize,
) -> *mut PeerbusMessage {
    ffi_guard(ptr::null_mut(), move || {
    let bytes = match unsafe { bytes_in(data, len) } {
        Ok(v) => v,
        Err(()) => return ptr::null_mut(),
    };
    Box::into_raw(Box::new(owned_message(kind, bytes)))
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_kind(message: *const PeerbusMessage) -> u64 {
    ffi_guard(0, move || {
    if message.is_null() {
        return 0;
    }
    // SAFETY: validated non-null.
    unsafe { (*message).kind }
})
}

/// Borrowed view of the message payload, valid until the message is
/// freed.
#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_data(message: *const PeerbusMessage) -> PeerbusBytes {
    ffi_guard(PeerbusBytes::empty(), move || {
    if message.is_null() {
        return PeerbusBytes::empty();
    }
    // SAFETY: validated non-null.
    let message = unsafe { &*message };
    PeerbusBytes {
        ptr: message.data.as_ptr(),
        len: message.data.len(),
    }
})
}

#[unsafe(no_mangle)]
pub extern "C" fn peerbus_message_free(message: *mut PeerbusMessage) {
    ffi_guard((), move || {
    if message.is_null() {
        return;
    }
    // SAFETY: originated from Box::into_raw.
    unsafe { drop(Box::from_raw(message)) };
})
}

