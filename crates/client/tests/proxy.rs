// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Direct client handles forward a transient borrowed server body. The proxy
//! always polls both directions; it never selects the sequential convenience.
use base_endpoint::{AddressEndpoint, BindEndpoint, BindOptions};
use base_http::body::ReadableBody;
use base_http_client::{Client, Endpoint, Error, Options};
use base_http1::{RequestReader, ResponseWriter};
use base_scheduler::{
    Cancellation, Clock, SocketAccept, SocketClose, SocketLocalAddress, SocketStreamReading,
    SocketStreamWriting,
};
use base_scheduler_direct::DirectScheduler;
use futures_lite::future::{block_on, zip};
use std::{
    future::Future,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

fn head(stream: &mut impl BufRead) -> String {
    let mut result = String::new();
    loop {
        let mut line = String::new();
        assert_ne!(stream.read_line(&mut line).unwrap(), 0);
        result.push_str(&line);
        if line == "\r\n" {
            return result;
        }
    }
}
fn peer_stream(stream: TcpStream) -> BufReader<TcpStream> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    BufReader::new(stream)
}
async fn supervised<T>(
    work: impl Future<Output = Result<T, Error>>,
    cancellation: &Cancellation,
) -> Result<T, Error> {
    let result = work.await;
    if result.is_err() {
        cancellation.cancel();
    }
    result
}
fn forwarding(response_bytes: usize) {
    const UPLOAD: usize = 8 * 1024 * 1024;
    let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let (uploaded, complete) = std::sync::mpsc::channel();
    let upstream = thread::spawn(move || {
        let mut stream = peer_stream(upstream.accept().unwrap().0);
        let metadata = head(&mut stream);
        assert!(metadata.starts_with("POST /upload HTTP/1.1\r\n"));
        assert!(metadata.contains(&format!("Host: {upstream_address}\r\n")));
        let mut output = stream.get_ref().try_clone().unwrap();
        let sending = thread::spawn(move || {
            if response_bytes == 0 {
                output
                    .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                    .unwrap();
            } else {
                output
                    .write_all(
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {response_bytes}\r\n\r\n")
                            .as_bytes(),
                    )
                    .unwrap();
                output.write_all(&vec![b'r'; response_bytes]).unwrap();
            }
        });
        let mut body = vec![0; UPLOAD];
        stream.read_exact(&mut body).unwrap();
        assert!(body.iter().all(|byte| *byte == b'u'));
        uploaded.send(()).unwrap();
        sending.join().unwrap();
        assert!(head(&mut stream).starts_with("GET /next HTTP/1.1\r\n"));
        let mut terminator = [0; 5];
        stream.read_exact(&mut terminator).unwrap();
        assert_eq!(&terminator, b"0\r\n\r\n");
        stream
            .get_mut()
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .unwrap();
    });
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        scheduler.timeout(Duration::from_secs(15), async {
            let lifetime = Cancellation::new();
            let listener = AddressEndpoint::new("127.0.0.1:0".parse().unwrap())
                .bind(&scheduler, BindOptions::default(), &lifetime).await.unwrap().remove(0);
            let address = scheduler.socket_local_address(&listener).unwrap();
            let downstream = thread::spawn(move || {
                let mut stream = peer_stream(TcpStream::connect(address).unwrap());
                stream.get_mut().write_all(format!("POST /upload HTTP/1.1\r\nHost: proxy\r\nContent-Length: {UPLOAD}\r\n\r\n").as_bytes()).unwrap();
                if response_bytes == 0 {
                    // Upload does not even begin until the whole response is
                    // consumed: successful download must not stop upload work.
                    assert!(head(&mut stream).starts_with("HTTP/1.1 204"));
                }
                stream.get_mut().write_all(&vec![b'u'; UPLOAD]).unwrap();
                // In the large-response case download is backpressured until
                // upstream has consumed every upload byte. It must still finish.
                complete.recv_timeout(Duration::from_secs(10)).unwrap();
                if response_bytes != 0 {
                    assert!(head(&mut stream).starts_with("HTTP/1.1 200"));
                    let mut body = vec![0; response_bytes];
                    stream.read_exact(&mut body).unwrap();
                    assert!(body.iter().all(|byte| *byte == b'r'));
                }
                stream.get_mut().write_all(b"GET /next HTTP/1.1\r\nHost: proxy\r\nConnection: close\r\n\r\n").unwrap();
                assert!(head(&mut stream).starts_with("HTTP/1.1 204"));
                let mut byte = [0]; assert_eq!(stream.read(&mut byte).unwrap(), 0);
            });
            let (client, driver) = Client::new(&scheduler, Endpoint::new(upstream_address), Options {
                pool: base_pool::Options { max_resources: 1, ..Default::default() }, ..Default::default()
            }).unwrap();
            let proxy = async {
                let (socket, _) = scheduler.socket_accept(&listener, &lifetime).await.unwrap();
                scheduler.socket_close(listener).await.unwrap();
                let mut input = RequestReader::new(scheduler.socket_stream_reader(&socket).unwrap(), Default::default()).unwrap();
                let mut output = ResponseWriter::new(scheduler.socket_stream_writer(&socket).unwrap());
                let result = async {
                    for index in 0..2 {
                        let (mut request, exchange) = input.read_request(&lifetime).await?.unwrap();
                        let mut metadata = request.metadata.clone();
                        metadata.authority = None;
                        metadata.fields.remove("host");
                        let (mut upstream_output, response) = client.open_request(metadata, &lifetime).await?.split();
                        let send = supervised(async {
                            loop {
                                let frame = request.body.read(&lifetime).await?;
                                let end = frame.is_end();
                                upstream_output.write_frame(frame, &lifetime).await?;
                                if end { break; }
                            }
                            upstream_output.finish(&lifetime).await
                        }, &lifetime);
                        let receive = supervised(async {
                            let mut response = response.await?;
                            response.version = None;
                            Ok(output.write_response(&exchange, &mut response, &lifetime).await?)
                        }, &lifetime);
                        let (sent, received) = zip(send, receive).await;
                        sent?;
                        assert_eq!(received?, index == 0);
                        drop(request);
                        assert!(input.is_reusable());
                    }
                    Ok::<_, Error>(())
                }.await;
                let (a, b) = zip(input.close(), output.close()).await;
                drop((input, output));
                scheduler.socket_close(socket).await.unwrap();
                a.unwrap(); b.unwrap();
                client.begin_close();
                result
            };
            let (driven, proxied) = zip(driver.run(&lifetime), supervised(proxy, &lifetime)).await;
            proxied.unwrap(); driven.unwrap();
            assert_eq!(client.pool_stats().total(), 0);
            downstream.join().unwrap();
        }).await.unwrap();
        scheduler.shutdown().await;
    });
    upstream.join().unwrap();
}

#[test]
fn proxy_forwards_upload_when_downstream_response_reads_are_paused() {
    forwarding(8 * 1024 * 1024);
}
#[test]
fn proxy_continues_upload_after_downstream_response_completion() {
    forwarding(0);
}
