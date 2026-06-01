//! Heap-bearing datapods (`#[dp(bytes)]`) with QoS and payload inspection.
//!
//! `datapod::Bytes` stores raw bytes in the payload. `datapod::Linestring`
//! stores `Vec<Point>` in the payload. quicbit keeps the payload opaque; the
//! example shows how callers pair the typed header with `sample.payload()`.
//!
//! ```text
//! cargo run --example datapod_heap_payloads
//! ```

use std::time::{Duration, Instant};

use datapod::{Bytes, Linestring, Point};
use quicbit::{LocalConfig, Node, TopicQos};

fn main() -> quicbit::Result<()> {
    let pub_name = unique("heap-pub");
    let local_cfg = LocalConfig {
        max_payload_bytes: 1024 * 1024,
        history_depth: 4,
        subscriber_buffer: 4,
        ..LocalConfig::default()
    };
    let pub_node = Node::builder()
        .no_relay()
        .identity(&pub_name)
        .local_config(local_cfg)
        .bind()?;
    let sub_node = Node::builder()
        .no_relay()
        .identity(unique("heap-sub"))
        .bind()?;

    let qos = TopicQos::latest()
        .with_chunk_bytes(16 * 1024)
        .with_max_message_bytes(1024 * 1024)
        .with_max_inflight_bytes(2 * 1024 * 1024)
        .with_subscriber_queue(4);

    bytes_case(&pub_node, &sub_node, &pub_name, qos)?;
    linestring_case(&pub_node, &sub_node, &pub_name, qos)?;
    Ok(())
}

fn bytes_case(
    pub_node: &Node,
    sub_node: &Node,
    pub_name: &str,
    qos: TopicQos,
) -> quicbit::Result<()> {
    let mut pubr = pub_node.publisher_with_qos::<Bytes>("blob/raw", qos)?;
    let mut sub = sub_node.subscriber_with_qos::<Bytes>(pub_name, "blob/raw", qos)?;

    let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();
    pubr.send(&Bytes::from_slice(&payload))?;

    let sample = poll_for(Duration::from_secs(2), || sub.take().ok().flatten())
        .ok_or_else(|| quicbit::Error::Timeout(Duration::from_secs(2)))?;
    println!(
        "Bytes payload: {} bytes, first={}, last={}",
        sample.payload().len(),
        sample.payload().first().copied().unwrap_or_default(),
        sample.payload().last().copied().unwrap_or_default()
    );
    assert_eq!(sample.payload(), payload.as_slice());
    Ok(())
}

fn linestring_case(
    pub_node: &Node,
    sub_node: &Node,
    pub_name: &str,
    qos: TopicQos,
) -> quicbit::Result<()> {
    let mut pubr = pub_node.publisher_with_qos::<Linestring>("path/local_plan", qos)?;
    let mut sub = sub_node.subscriber_with_qos::<Linestring>(pub_name, "path/local_plan", qos)?;

    let points = vec![
        Point::new(0.0, 0.0, 0.0),
        Point::new(1.0, 0.5, 0.0),
        Point::new(2.0, 1.0, 0.0),
        Point::new(3.0, 1.0, 0.0),
    ];
    pubr.send(&Linestring::new(points.clone()))?;

    let sample = poll_for(Duration::from_secs(2), || sub.take().ok().flatten())
        .ok_or_else(|| quicbit::Error::Timeout(Duration::from_secs(2)))?;
    let received_points: &[Point] = bytemuck::cast_slice(sample.payload());
    println!(
        "Linestring payload: {} points, byte_len={}",
        received_points.len(),
        sample.payload().len()
    );
    assert_eq!(received_points.len(), points.len());
    assert_eq!(received_points[2].x, 2.0);
    Ok(())
}

fn poll_for<R>(timeout: Duration, mut f: impl FnMut() -> Option<R>) -> Option<R> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(r) = f() {
            return Some(r);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

fn unique(stem: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{stem}-{pid}-{nanos}")
}
