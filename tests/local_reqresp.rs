//! In-process req/res tests for the local transport.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use peerbus::{LocalConfig, LocalReqResService, LocalReqRespService};

#[datapod::datapod]
struct Add {
    a: i32,
    b: i32,
}

#[datapod::datapod]
struct Sum {
    value: i32,
}

fn name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("peerbus_rr_{stem}_{pid}_{nanos}")
}

#[test]
fn single_client_single_server_roundtrip() {
    let svc = LocalReqResService::<Add, Sum>::create(&name("rt"), LocalConfig::default()).unwrap();

    let mut server = svc.server().unwrap();
    let mut client = svc.client().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let server_thread = thread::spawn(move || {
        while !stop_for_thread.load(Ordering::Acquire) {
            if let Some((req, reply)) = server.take_request().unwrap() {
                let h = *req.header();
                let sum = Sum { value: h.a + h.b };
                reply.respond(&sum).unwrap();
            } else {
                thread::sleep(Duration::from_micros(100));
            }
        }
    });

    for i in 0..10 {
        let resp = client.call(&Add { a: i, b: i * 2 }).unwrap();
        assert_eq!(*resp.header(), Sum { value: i + i * 2 });
    }

    stop.store(true, Ordering::Release);
    server_thread.join().unwrap();
}

#[test]
fn timeout_when_no_server() {
    let svc =
        LocalReqResService::<Add, Sum>::create(&name("timeout"), LocalConfig::default()).unwrap();
    let mut client = svc.client().unwrap();
    let r = client.call_with_timeout(&Add { a: 1, b: 2 }, Duration::from_millis(100));
    assert!(r.is_err(), "expected timeout error");
}

#[test]
fn old_reqresp_alias_still_compiles() {
    let _svc =
        LocalReqRespService::<Add, Sum>::create(&name("alias"), LocalConfig::default()).unwrap();
}

#[test]
fn reqres_module_alias_is_public() {
    let envelope = peerbus::reqres::Envelope::<Add> {
        req_id: 7,
        header: Add { a: 1, b: 2 },
    };
    assert_eq!(envelope.req_id, 7);
    assert_eq!(envelope.header.a + envelope.header.b, 3);
}
