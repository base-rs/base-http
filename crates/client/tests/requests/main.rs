// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod call;
mod open;
mod options;

use base_http::body::{BodyFrame, ReadableBody};
use base_http_client::{Client, Endpoint, Options};
use base_scheduler::Cancellation;
use base_scheduler_direct::DirectScheduler;
use futures_lite::future::{block_on, zip};
use std::{
    io::{BufRead, BufReader, Read},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
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

pub fn accept(listener: &TcpListener) -> BufReader<TcpStream> {
    let (stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    BufReader::new(stream)
}

pub fn head(stream: &mut impl BufRead) -> String {
    let mut fields = String::new();
    loop {
        let mut line = String::new();
        assert_ne!(stream.read_line(&mut line).unwrap(), 0);
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

pub async fn payload(body: &mut impl ReadableBody) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let frame = body.read(&Cancellation::never()).await.unwrap();
        let end = frame.is_end();
        if let BodyFrame::Data { data, .. } = frame {
            bytes.extend_from_slice(data.as_ref());
        }
        if end {
            return bytes;
        }
    }
}

pub fn chain(error: &dyn std::error::Error) -> Vec<String> {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(error) = source {
        messages.push(error.to_string());
        source = error.source();
    }
    messages
}

/// Run a client with a single pooled connection beside its driver, then require that every connection was retired.
pub fn run(
    address: SocketAddr,
    options: Options,
    application: impl for<'scope> AsyncFnOnce(Client<'scope>),
) {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
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
        let (client, driver) = Client::new(&scheduler, Endpoint::new(address), options).unwrap();
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
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        let endpoint = Endpoint::new("127.0.0.1:1".parse().unwrap());
        let (client, driver) = Client::new(&scheduler, endpoint, Options::default()).unwrap();
        application(client).await;
        drop(driver);
        scheduler.shutdown().await;
    });
}

/// A request body that counts how often it is closed.
pub struct Tracked(pub Arc<AtomicUsize>);
impl ReadableBody for Tracked {
    type Data = Vec<u8>;
    type Error = std::convert::Infallible;
    fn close(&mut self, _: Option<Self::Error>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    async fn read(&mut self, _: &Cancellation) -> Result<BodyFrame<Vec<u8>>, Self::Error> {
        Ok(BodyFrame::End)
    }
}
