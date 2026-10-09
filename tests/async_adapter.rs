//! Async adapter smoke test.

use std::time::Duration;

use bytemuck::{Pod, Zeroable};
use datapod::ZeroCopySend;
use peerbus::async_adapter::{
    AsyncAckServer, AsyncAnsServer, AsyncPipClient, AsyncPipServer, AsyncPutClient, AsyncQueClient,
    AsyncReqClient, AsyncRemotePipClient, AsyncRemotePutClient, AsyncRemoteQueClient,
    AsyncRemoteReqClient, AsyncReqServer,
};
use peerbus::{
    AsyncPublisher, AsyncSubscriber, LocalConfig, LocalPipService, LocalPutAckService,
    LocalQueAnsService, LocalReqResService, LocalService, RemoteTransport,
};

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

// ---------------------------------------------------------------------------
// req/res
// ---------------------------------------------------------------------------

#[datapod::datapod]
struct Add {
    a: i32,
    b: i32,
}

#[datapod::datapod]
struct Sum {
    value: i32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_reqres_round_trip() {
    let svc =
        LocalReqResService::<Add, Sum>::create(&unique_name("rr"), LocalConfig::default()).unwrap();
    let mut client = AsyncReqClient::new(svc.client().unwrap());
    let mut server = AsyncReqServer::new(svc.server().unwrap());

    let server_task = tokio::spawn(async move {
        loop {
            if let Some((req_id, req)) = server.take_request().await.unwrap() {
                let h = *req.header();
                server
                    .respond_to(req_id, Sum { value: h.a + h.b })
                    .await
                    .unwrap();
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    let resp = client.call(Add { a: 2, b: 5 }).await.unwrap();
    assert_eq!(*resp.header(), Sum { value: 7 });

    server_task.await.unwrap();
}

// ---------------------------------------------------------------------------
// que/ans
// ---------------------------------------------------------------------------

#[datapod::datapod]
struct Query {
    n: u32,
}

#[datapod::datapod]
struct Answer {
    idx: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_queans_round_trip() {
    let svc =
        LocalQueAnsService::<Query, Answer>::open_or_create(&unique_name("qa"), LocalConfig::default())
            .unwrap();
    let mut client = AsyncQueClient::new(svc.client().unwrap());
    let mut server = AsyncAnsServer::new(svc.server().unwrap());

    let server_task = tokio::spawn(async move {
        loop {
            if let Some((req_id, que)) = server.take_query().await.unwrap() {
                let count = que.header().n;
                for idx in 0..count {
                    server.send_to(req_id, Answer { idx }).await.unwrap();
                }
                server.finish_to(req_id).await.unwrap();
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    let answers = client.query(Query { n: 3 }).await.unwrap();
    let got: Vec<u32> = answers.iter().map(|a| a.header().idx).collect();
    assert_eq!(got, vec![0, 1, 2]);

    server_task.await.unwrap();
}

// ---------------------------------------------------------------------------
// put/ack
// ---------------------------------------------------------------------------

#[datapod::datapod]
struct Chunk {
    val: u32,
}

#[datapod::datapod]
struct Receipt {
    total: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_putack_round_trip() {
    let svc =
        LocalPutAckService::<Chunk, Receipt>::open_or_create(&unique_name("pa"), LocalConfig::default())
            .unwrap();
    let mut client = AsyncPutClient::new(svc.client().unwrap());
    let mut server = AsyncAckServer::new(svc.server().unwrap());

    let server_task = tokio::spawn(async move {
        let mut total = 0u32;
        let req_id = loop {
            match server.take_message().await.unwrap() {
                Some((rid, item, done)) => {
                    if let Some(sample) = item {
                        total += sample.header().val;
                    }
                    if done {
                        break rid;
                    }
                }
                None => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        };
        server
            .ack_to(req_id, Receipt { total })
            .await
            .unwrap();
    });

    let req_id = client.open_req().await.unwrap();
    client.send_to(req_id, Chunk { val: 10 }).await.unwrap();
    client.send_to(req_id, Chunk { val: 20 }).await.unwrap();
    let ack = client.finish_req(req_id).await.unwrap();
    assert_eq!(*ack.header(), Receipt { total: 30 });

    server_task.await.unwrap();
}

// ---------------------------------------------------------------------------
// pip (bidirectional)
// ---------------------------------------------------------------------------

#[datapod::datapod]
struct ClientMsg {
    ping: u32,
}

#[datapod::datapod]
struct ServerMsg {
    pong: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_pip_round_trip() {
    let svc = LocalPipService::<ClientMsg, ServerMsg>::open_or_create(
        &unique_name("pip"),
        LocalConfig::default(),
    )
    .unwrap();
    let mut client = AsyncPipClient::new(svc.client().unwrap());
    let mut server = AsyncPipServer::new(svc.server().unwrap());

    let server_task = tokio::spawn(async move {
        // Read the first client message, echo it back doubled, then
        // close the server->client direction.
        let mut ping = 0u32;
        let sid = loop {
            match server.take_message().await.unwrap() {
                Some((sid, item, done)) => {
                    if let Some(sample) = item {
                        ping = sample.header().ping;
                        break sid;
                    }
                    if done {
                        break sid;
                    }
                }
                None => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        };
        server
            .send_to(sid, ServerMsg { pong: ping * 2 })
            .await
            .unwrap();
        server.finish_send_to(sid).await.unwrap();
    });

    let session = client.start_session().await.unwrap();
    client.send_to(session, ClientMsg { ping: 21 }).await.unwrap();
    client.finish_send_to(session).await.unwrap();

    let resp = client.next_from(session).await.unwrap();
    let resp = resp.expect("server should reply");
    assert_eq!(resp.header().pong, 42);

    let end = client.next_from(session).await.unwrap();
    assert!(end.is_none(), "server direction should be done");

    server_task.await.unwrap();
}

// ===========================================================================
// remote (iroh) — async client wrappers over two in-process endpoints
// ===========================================================================
//
// The remote clients are `Pod`-based (not `#[datapod]`), matching
// `RemoteTransport::{req,que,put,pip}_client`'s bounds. The remote server
// side is registration/callback based (`serve_requests`/`serve_ques`/
// `serve_puts`/`serve_pips`), so there is no async server wrapper: we
// register a handler on the server transport and drive the async client
// against it. Transport construction (`build_blocking`,
// `wait_for_direct_addresses`) drives the transport's own runtime via
// `block_on`, so it must run off the async worker thread — we do it inside
// `spawn_blocking`. Each client call is wrapped in a bounded `timeout` so
// a wiring failure surfaces as a test failure rather than a hang.

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq, ZeroCopySend)]
struct RAdd {
    a: i32,
    b: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable, Debug, PartialEq, ZeroCopySend)]
struct RSum {
    value: i32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_remote_reqres_round_trip() {
    let t = unique_name("remote_rr");
    let (_server_side, client_side) = tokio::task::spawn_blocking(move || {
        let server_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .build_blocking()
            .expect("server endpoint");
        server_side
            .wait_for_direct_addresses(Duration::from_secs(5))
            .expect("addresses");
        server_side
            .serve_requests::<RAdd, RSum, _>(|req| RSum {
                value: req.a + req.b,
            })
            .expect("register handler");
        let server_addr = server_side.endpoint_addr();
        let client_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .peer(server_addr)
            .build_blocking()
            .expect("client endpoint");
        (server_side, client_side)
    })
    .await
    .unwrap();

    let mut client = AsyncRemoteReqClient::new(client_side.req_client::<RAdd, RSum>().unwrap());
    let resp = tokio::time::timeout(Duration::from_secs(10), client.call(RAdd { a: 2, b: 5 }))
        .await
        .expect("call should not hang")
        .unwrap();
    assert_eq!(resp, RSum { value: 7 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_remote_queans_round_trip() {
    let t = unique_name("remote_qa");
    let (_server_side, client_side) = tokio::task::spawn_blocking(move || {
        let server_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .build_blocking()
            .expect("server endpoint");
        server_side
            .wait_for_direct_addresses(Duration::from_secs(5))
            .expect("addresses");
        server_side
            .serve_ques::<RAdd, RSum, _>(|que| {
                (0..que.a).map(|idx| RSum { value: idx }).collect()
            })
            .expect("register handler");
        let server_addr = server_side.endpoint_addr();
        let client_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .peer(server_addr)
            .build_blocking()
            .expect("client endpoint");
        (server_side, client_side)
    })
    .await
    .unwrap();

    let mut client = AsyncRemoteQueClient::new(client_side.que_client::<RAdd, RSum>().unwrap());
    let answers = tokio::time::timeout(
        Duration::from_secs(10),
        client.query(RAdd { a: 3, b: 0 }),
    )
    .await
    .expect("query should not hang")
    .unwrap();
    let got: Vec<i32> = answers.iter().map(|a| a.value).collect();
    assert_eq!(got, vec![0, 1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_remote_putack_round_trip() {
    let t = unique_name("remote_pa");
    let (_server_side, client_side) = tokio::task::spawn_blocking(move || {
        let server_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .build_blocking()
            .expect("server endpoint");
        server_side
            .wait_for_direct_addresses(Duration::from_secs(5))
            .expect("addresses");
        server_side
            .serve_puts::<RAdd, RSum, _>(|puts| RSum {
                value: puts.iter().map(|p| p.a).sum(),
            })
            .expect("register handler");
        let server_addr = server_side.endpoint_addr();
        let client_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .peer(server_addr)
            .build_blocking()
            .expect("client endpoint");
        (server_side, client_side)
    })
    .await
    .unwrap();

    let mut client = AsyncRemotePutClient::new(client_side.put_client::<RAdd, RSum>().unwrap());
    let ack = tokio::time::timeout(
        Duration::from_secs(10),
        client.upload(vec![RAdd { a: 10, b: 0 }, RAdd { a: 20, b: 0 }]),
    )
    .await
    .expect("upload should not hang")
    .unwrap();
    assert_eq!(ack, RSum { value: 30 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_remote_pip_round_trip() {
    let t = unique_name("remote_pip");
    let (_server_side, client_side) = tokio::task::spawn_blocking(move || {
        let server_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .build_blocking()
            .expect("server endpoint");
        server_side
            .wait_for_direct_addresses(Duration::from_secs(5))
            .expect("addresses");
        // collect-then-respond: echo each client message doubled.
        server_side
            .serve_pips::<RAdd, RSum, _>(|msgs| {
                msgs.iter().map(|m| RSum { value: m.a * 2 }).collect()
            })
            .expect("register handler");
        let server_addr = server_side.endpoint_addr();
        let client_side = RemoteTransport::builder(t.as_str())
            .no_relay()
            .peer(server_addr)
            .build_blocking()
            .expect("client endpoint");
        (server_side, client_side)
    })
    .await
    .unwrap();

    let mut client = AsyncRemotePipClient::new(client_side.pip_client::<RAdd, RSum>().unwrap());
    let replies = tokio::time::timeout(
        Duration::from_secs(10),
        client.exchange(vec![RAdd { a: 21, b: 0 }, RAdd { a: 5, b: 0 }]),
    )
    .await
    .expect("exchange should not hang")
    .unwrap();
    let got: Vec<i32> = replies.iter().map(|r| r.value).collect();
    assert_eq!(got, vec![42, 10]);
}
