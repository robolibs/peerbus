#![no_main]

use libfuzzer_sys::fuzz_target;
use quicbit::remote::parse_pubsub_handshake_tail;

// Property: the parser must NEVER panic on arbitrary input. Any
// failure mode is fine as long as it's a graceful `Err`.
fuzz_target!(|data: &[u8]| {
    let _ = parse_pubsub_handshake_tail(data);
});
