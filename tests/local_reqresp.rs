//! In-process request/response tests for the local transport.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use bytemuck::{Pod, Zeroable};
use quicbit::{LocalConfig, LocalReqRespService};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq)]
struct Add {
    a: i32,
    b: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq)]
struct Sum {
    value: i32,
}

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("rr-{stem}-{pid}-{nanos}")
}

#[test]
fn single_client_single_server_roundtrip() {
    let svc = LocalReqRespService::<Add, Sum>::create(
        &unique_name("rt"),
        LocalConfig {
            slot_count: 8,
            slot_size: 32,
            history_depth: 4,
        },
    )
    .unwrap();

    let mut server = svc.server();
    let mut client = svc.client();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let server_thread = thread::spawn(move || {
        while !stop_for_thread.load(Ordering::Acquire) {
            if let Some((req, reply)) = server.take_request().unwrap() {
                let sum = Sum { value: req.payload.a + req.payload.b };
                reply.respond(sum).unwrap();
            } else {
                thread::sleep(Duration::from_micros(100));
            }
        }
    });

    // Issue several calls; verify each is answered correctly.
    for i in 0..10 {
        let resp = client.call(Add { a: i, b: i * 2 }).unwrap();
        assert_eq!(resp, Sum { value: i + i * 2 });
    }

    stop.store(true, Ordering::Release);
    server_thread.join().unwrap();
}

#[test]
fn timeout_when_no_server() {
    let svc = LocalReqRespService::<Add, Sum>::create(
        &unique_name("timeout"),
        LocalConfig {
            slot_count: 4,
            slot_size: 32,
            history_depth: 1,
        },
    )
    .unwrap();
    let mut client = svc.client();
    let r = client.call_with_timeout(Add { a: 1, b: 2 }, Duration::from_millis(100));
    assert!(r.is_err(), "expected timeout error");
}

#[test]
fn two_clients_get_their_own_replies() {
    let svc = LocalReqRespService::<Add, Sum>::create(
        &unique_name("multi"),
        LocalConfig {
            slot_count: 16,
            slot_size: 32,
            history_depth: 8,
        },
    )
    .unwrap();

    let svc_for_server = svc.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let server_thread = thread::spawn(move || {
        let mut server = svc_for_server.server();
        while !stop_for_thread.load(Ordering::Acquire) {
            if let Some((req, reply)) = server.take_request().unwrap() {
                // Server multiplies a*b to make the result obviously
                // dependent on the request, so we can be sure the
                // correlation isn't accidental.
                reply
                    .respond(Sum {
                        value: req.payload.a * req.payload.b,
                    })
                    .unwrap();
            } else {
                thread::sleep(Duration::from_micros(50));
            }
        }
    });

    let svc_a = svc.clone();
    let svc_b = svc.clone();

    let handle_a = thread::spawn(move || {
        let mut c = svc_a.client();
        let mut results = Vec::new();
        for i in 1..=20 {
            results.push(c.call(Add { a: i, b: 100 }).unwrap());
        }
        results
    });

    let handle_b = thread::spawn(move || {
        let mut c = svc_b.client();
        let mut results = Vec::new();
        for i in 1..=20 {
            results.push(c.call(Add { a: i, b: 1000 }).unwrap());
        }
        results
    });

    let a_results = handle_a.join().unwrap();
    let b_results = handle_b.join().unwrap();
    stop.store(true, Ordering::Release);
    server_thread.join().unwrap();

    for (i, r) in a_results.iter().enumerate() {
        let expected = ((i + 1) as i32) * 100;
        assert_eq!(r.value, expected, "client A wrong response at {i}");
    }
    for (i, r) in b_results.iter().enumerate() {
        let expected = ((i + 1) as i32) * 1000;
        assert_eq!(r.value, expected, "client B wrong response at {i}");
    }
}
