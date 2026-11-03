// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Helpers shared by the `client` and `writable` test suites.
use base_http_client::{Client, Endpoint, Options};
use base_scheduler::Cancellation;
use base_scheduler_native::NativeScheduler;
use futures_lite::future::{block_on, zip};
use std::{
    io::{BufRead, Read},
    net::{SocketAddr, TcpListener},
    thread,
    time::Duration,
};

pub fn peer(
    work: impl FnOnce(TcpListener) + Send + 'static,
) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    (address, thread::spawn(move || work(listener)))
}

pub fn head(stream: &mut impl BufRead) -> String {
    let mut fields = String::new();
    loop {
        let mut line = String::new();
        assert_ne!(
            stream.read_line(&mut line).unwrap(),
            0,
            "EOF before request fields"
        );
        fields.push_str(&line);
        if line == "\r\n" {
            return fields;
        }
    }
}

/// Read until the client closes its side of the connection.
pub fn remainder(stream: &mut impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    bytes
}

pub use base_http_fixtures::{body::payload, error_chain as chain};

/// Run a client with a single pooled connection beside its driver, then require that every connection was retired.
pub fn run_endpoint(
    endpoint: Endpoint,
    options: Options,
    application: impl for<'scope> AsyncFnOnce(Client<'scope>),
) {
    let scheduler = NativeScheduler::new().unwrap();
    block_on(async {
        let lifetime = Cancellation::new();
        let watchdog = lifetime.clone();
        let (done, wait) = std::sync::mpsc::channel::<()>();
        let timer = thread::spawn(move || {
            if wait.recv_timeout(Duration::from_secs(15)).is_err() {
                watchdog.cancel();
            }
        });
        let options = Options {
            pool: base_pool::Options {
                max_resources: 1,
                ..Default::default()
            },
            ..options
        };
        let (client, driver) = Client::new(&scheduler, endpoint, options).unwrap();
        let stats = client.clone();
        let (driven, ()) = zip(driver.run(&lifetime), application(client)).await;
        let _ = done.send(());
        timer.join().unwrap();
        driven.unwrap();
        assert_eq!(stats.pool_stats().total(), 0);
        scheduler.shutdown().await;
    });
}

/// Run a client that never dials. Its driver is dropped without being polled.
pub fn unconnected(application: impl for<'scope> AsyncFnOnce(Client<'scope>)) {
    let scheduler = NativeScheduler::new().unwrap();
    block_on(async {
        let endpoint = Endpoint::new("127.0.0.1:1".parse().unwrap());
        let (client, driver) = Client::new(&scheduler, endpoint, Options::default()).unwrap();
        application(client).await;
        drop(driver);
        scheduler.shutdown().await;
    });
}
