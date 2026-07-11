//! Async adapter smoke test.

use peerbus::{AsyncPublisher, AsyncSubscriber, LocalConfig, LocalService};

#[datapod::datapod]
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
    format!("peerbus_aa_{stem}_{pid}_{nanos}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_local_round_trip() {
    let svc = LocalService::<Tick>::create(&unique_name("rt"), LocalConfig::default()).unwrap();
    let mut pubr = AsyncPublisher::new(svc.publisher().unwrap());
    let mut sub = AsyncSubscriber::new(svc.subscriber().unwrap());

    let mut loan = pubr.loan(0).await.unwrap();
    *loan.header_mut() = Tick {
        seq: 1,
        payload: 42,
    };
    pubr.publish(loan).await.unwrap();

    let sample = sub
        .take()
        .await
        .unwrap()
        .expect("sample should be available");
    assert_eq!(
        *sample.header(),
        Tick {
            seq: 1,
            payload: 42
        }
    );
}
