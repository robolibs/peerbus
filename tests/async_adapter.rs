//! Async adapter smoke test.

#![cfg(feature = "async")]

use bytemuck::{Pod, Zeroable};
use quicbit::{AsyncPublisher, AsyncSubscriber, LocalConfig, LocalService};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq)]
struct Tick {
    seq: u32,
    payload: u32,
}

fn unique_name(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("aa-{stem}-{pid}-{nanos}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_local_round_trip() {
    let svc = LocalService::<Tick>::create(&unique_name("rt"), LocalConfig::default()).unwrap();
    let mut pubr = AsyncPublisher::new(svc.publisher());
    let mut sub = AsyncSubscriber::new(svc.subscriber());

    let mut loan = pubr.loan().await.unwrap();
    *loan = Tick { seq: 1, payload: 42 };
    pubr.publish(loan).await.unwrap();

    let sample = sub.take().await.unwrap().expect("sample should be available");
    assert_eq!(*sample, Tick { seq: 1, payload: 42 });
}
