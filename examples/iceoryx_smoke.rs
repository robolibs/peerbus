//! Minimal iceoryx2 smoke test — confirms the library actually
//! pubs/subs in our nix shell before we start wiring it into
//! quicbit's `LocalTransport`. Doesn't use anything from quicbit.

use core::time::Duration;
use iceoryx2::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, ZeroCopySend)]
#[type_name("quicbit_smoke::Tick")]
struct Tick {
    seq: u64,
}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let service = node
        .service_builder(&"quicbit/smoke".try_into()?)
        .publish_subscribe::<Tick>()
        .open_or_create()?;

    let publisher = service.publisher_builder().create()?;
    let subscriber = service.subscriber_builder().create()?;

    for i in 1..=5 {
        let sample = publisher.loan_uninit()?;
        let sample = sample.write_payload(Tick { seq: i });
        sample.send()?;
        std::thread::sleep(Duration::from_millis(10));

        while let Some(received) = subscriber.receive()? {
            println!("received {:?}", *received);
        }
    }
    Ok(())
}
