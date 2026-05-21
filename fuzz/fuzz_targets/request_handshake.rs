#![no_main]

use libfuzzer_sys::fuzz_target;
use quicbit::remote::parse_request_handshake_tail;

fuzz_target!(|data: &[u8]| {
    let _ = parse_request_handshake_tail(data);
});
