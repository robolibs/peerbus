//! Minimal local SHM smoke test.

use core::time::Duration;

use peerbus::{LocalConfig, LocalService};

#[datapod::datapod]
struct Tick {
    seq: u64,
}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    let name = format!("peerbus_smoke_{}", std::process::id());
    let service = LocalService::<Tick>::create(&name, LocalConfig::default())?;
    let mut publisher = service.publisher()?;
    let mut subscriber = service.subscriber()?;

    for i in 1..=5 {
        publisher.send(&Tick { seq: i })?;
        std::thread::sleep(Duration::from_millis(10));

        while let Some(received) = subscriber.take()? {
            println!("received {:?}", received.header());
        }
    }
    Ok(())
}
