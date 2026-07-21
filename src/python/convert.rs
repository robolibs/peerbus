use super::*;

pub(crate) fn py_err(err: crate::Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

pub(crate) fn lock_py<'a, T>(mutex: &'a Mutex<T>, name: &str) -> PyResult<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| PyRuntimeError::new_err(format!("{name} mutex poisoned")))
}

pub(crate) fn timeout(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms)
}

pub(crate) fn raw_from_py(value: &Bound<'_, PyAny>) -> PyResult<RawMsg> {
    if let Ok((kind, data)) = value.extract::<(u64, Vec<u8>)>() {
        Ok(RawMsg::new(kind, &data))
    } else {
        match value.extract::<Vec<u8>>() {
            Ok(data) => Ok(RawMsg::new(0, &data)),
            Err(_) => raw_from_pod(value),
        }
    }
}

pub(crate) fn raw_vec_from_py(value: &Bound<'_, PyAny>) -> PyResult<Vec<RawMsg>> {
    if value.is_none() {
        return Ok(Vec::new());
    }
    if let Ok((kind, data)) = value.extract::<(u64, Vec<u8>)>() {
        return Ok(vec![RawMsg::new(kind, &data)]);
    }
    if let Ok(data) = value.extract::<Vec<u8>>() {
        return Ok(vec![RawMsg::new(0, &data)]);
    }
    if let Ok(items) = value.extract::<Vec<(u64, Vec<u8>)>>() {
        return Ok(items
            .into_iter()
            .map(|(kind, data)| RawMsg::new(kind, &data))
            .collect());
    }
    if let Ok(items) = value.extract::<Vec<Vec<u8>>>() {
        return Ok(items
            .into_iter()
            .map(|data| RawMsg::new(0, &data))
            .collect());
    }
    if let Ok(items) = value.extract::<Vec<Py<PyAny>>>() {
        let py = value.py();
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let item = item.bind(py);
            if item.is_none() {
                continue;
            }
            out.push(raw_from_py(item)?);
        }
        return Ok(out);
    }
    Ok(vec![raw_from_pod(value)?])
}

pub(crate) fn sample_tuple<'py>(py: Python<'py>, kind: u64, payload: &[u8]) -> (u64, Bound<'py, PyBytes>) {
    (kind, PyBytes::new(py, payload))
}

/// Fill a read-only Python buffer view from bytes owned by `owner`.
///
/// The data must remain valid for as long as `owner` is alive. For the
/// zero-copy sample views below, `owner` is the Python sample object holding the
/// Rust `NodeSample`, which pins the SHM slot until the Python object and all
/// memoryviews are released.
pub(crate) unsafe fn fill_readonly_buffer(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    data_ptr: *const u8,
    data_len: usize,
    owner: Bound<'_, PyAny>,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("buffer view is null"));
    }
    if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
        return Err(PyBufferError::new_err("peerbus sample views are read-only"));
    }

    unsafe {
        (*view).obj = owner.into_ptr();
        (*view).buf = data_ptr as *mut c_void;
        (*view).len = data_len as isize;
        (*view).readonly = 1;
        (*view).itemsize = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            CString::new("B")
                .expect("static buffer format contains no NULs")
                .into_raw()
        } else {
            ptr::null_mut()
        };
        (*view).ndim = 1;
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            &mut (*view).len
        } else {
            ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            &mut (*view).itemsize
        } else {
            ptr::null_mut()
        };
        (*view).suboffsets = ptr::null_mut();
        (*view).internal = ptr::null_mut();
    }
    Ok(())
}

pub(crate) unsafe fn release_readonly_buffer(view: *mut ffi::Py_buffer) {
    unsafe {
        if !view.is_null() && !(*view).format.is_null() {
            drop(CString::from_raw((*view).format));
            (*view).format = ptr::null_mut();
        }
    }
}

pub(crate) fn raw_from_pod(value: &Bound<'_, PyAny>) -> PyResult<RawMsg> {
    let wire = value.call_method0("to_wire_message").map_err(|_| {
        PyRuntimeError::new_err(
            "expected bytes, (kind, bytes), or object with to_wire_message() -> (kind, bytes)",
        )
    })?;
    let (kind, data) = wire.extract::<(u64, Vec<u8>)>()?;
    Ok(RawMsg::new(kind, &data))
}

/// Best-effort count of the positional parameters a handler accepts, via
/// `inspect.signature`. Used to dispatch server handlers to the correct calling
/// convention up front, instead of calling one convention and retrying on
/// `TypeError` (which would re-run a correctly-shaped handler body that happened
/// to raise `TypeError` internally, invoking user side effects twice).
///
/// Returns `None` when the signature cannot be introspected (some builtins) or
/// when the handler accepts `*args`; callers then fall back to the canonical
/// (preferred) convention.
pub(crate) fn handler_positional_arity(
    py: Python<'_>,
    handler: &Bound<'_, PyAny>,
) -> PyResult<Option<usize>> {
    let inspect = py.import("inspect")?;
    let signature = match inspect.call_method1("signature", (handler,)) {
        Ok(sig) => sig,
        Err(_) => return Ok(None),
    };
    let parameters = signature.getattr("parameters")?;
    let values = parameters.call_method0("values")?;
    let mut count = 0usize;
    for param in values.try_iter()? {
        let param = param?;
        let kind = param.getattr("kind")?;
        match kind.str()?.to_string_lossy().as_ref() {
            "POSITIONAL_ONLY" | "POSITIONAL_OR_KEYWORD" => count += 1,
            // `*args` means the handler accepts an arbitrary number of
            // positional args; we cannot reason about arity, so defer to the
            // canonical convention.
            "VAR_POSITIONAL" => return Ok(None),
            _ => {}
        }
    }
    Ok(Some(count))
}

pub(crate) fn datapod_msg_from_py(value: &Bound<'_, PyAny>) -> PyResult<DatapodMsg> {
    if let Ok((type_hash, wire)) = value.extract::<(u64, Vec<u8>)>() {
        return Ok(DatapodMsg::new(type_hash, wire));
    }
    let wire = value.call_method0("to_wire_message").map_err(|_| {
        PyRuntimeError::new_err(
            "expected (type_hash, wire_bytes) or datapod object with to_wire_message() -> (type_hash, bytes)",
        )
    })?;
    let (type_hash, wire) = wire.extract::<(u64, Vec<u8>)>()?;
    Ok(DatapodMsg::new(type_hash, wire))
}

pub(crate) fn datapod_vec_from_py(value: &Bound<'_, PyAny>) -> PyResult<Vec<DatapodMsg>> {
    if value.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(items) = value.extract::<Vec<Py<PyAny>>>() {
        let py = value.py();
        return items
            .into_iter()
            .map(|item| datapod_msg_from_py(item.bind(py)))
            .collect();
    }
    Ok(vec![datapod_msg_from_py(value)?])
}

pub(crate) fn decode_datapod(
    py: Python<'_>,
    datapod_type: &Bound<'_, PyAny>,
    kind: u64,
    payload: &[u8],
) -> PyResult<Py<PyAny>> {
    let data = PyBytes::new(py, payload);
    let decoded = if datapod_type.hasattr("from_wire_message")? {
        datapod_type.call_method1("from_wire_message", (kind, data))?
    } else if datapod_type.hasattr("from_wire")? {
        let expected = datapod_type
            .getattr("TYPE_HASH")
            .and_then(|value| value.extract::<u64>())
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "datapod type with from_wire(header, payload) must expose TYPE_HASH",
                )
            })?;
        if kind != expected {
            return Err(PyRuntimeError::new_err(format!(
                "wrong datapod kind: got {kind}, expected {expected}"
            )));
        }
        let header_size = if datapod_type.hasattr("__datapod_header_size__")? {
            datapod_type
                .getattr("__datapod_header_size__")?
                .extract::<usize>()?
        } else if datapod_type.hasattr("HEADER_SIZE")? {
            datapod_type.getattr("HEADER_SIZE")?.extract::<usize>()?
        } else {
            match PyModule::import(py, "datapod")
                .and_then(|module| module.getattr("header_size"))
                .and_then(|func| func.call1((kind,)))
                .and_then(|value| value.extract::<usize>())
            {
                Ok(size) => size,
                Err(_) => payload.len(),
            }
        };
        if payload.len() < header_size {
            return Err(PyRuntimeError::new_err(format!(
                "datapod wire message too short: got {}, need at least {} header bytes",
                payload.len(),
                header_size
            )));
        }
        let header = PyBytes::new(py, &payload[..header_size]);
        let body = PyBytes::new(py, &payload[header_size..]);
        datapod_type.call_method1("from_wire", (header, body))?
    } else {
        return Err(PyRuntimeError::new_err(
            "datapod type must provide from_wire_message(kind, data) or from_wire(header, payload)",
        ));
    };
    Ok(decoded.unbind())
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("endpoint address hex has odd length".to_string());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = nibble(pair[0]).ok_or_else(|| "invalid endpoint address hex".to_string())?;
        let lo = nibble(pair[1]).ok_or_else(|| "invalid endpoint address hex".to_string())?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

pub(crate) fn encode_endpoint_addr(addr: &iroh::EndpointAddr) -> PyResult<String> {
    let bytes = postcard::to_stdvec(addr)
        .map_err(|e| PyRuntimeError::new_err(format!("encode endpoint addr: {e}")))?;
    Ok(hex_encode(&bytes))
}

pub(crate) fn decode_endpoint_addr(encoded: &str) -> PyResult<iroh::EndpointAddr> {
    let bytes = hex_decode(encoded).map_err(PyRuntimeError::new_err)?;
    postcard::from_bytes(&bytes)
        .map_err(|e| PyRuntimeError::new_err(format!("decode endpoint addr: {e}")))
}

pub(crate) fn item_stats_dict(py: Python<'_>, stats: crate::ItemStats) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("messages_out", stats.messages_out)?;
    dict.set_item("messages_in", stats.messages_in)?;
    dict.set_item("bytes_out", stats.bytes_out)?;
    dict.set_item("bytes_in", stats.bytes_in)?;
    dict.set_item("errors", stats.errors)?;
    Ok(dict.into())
}

