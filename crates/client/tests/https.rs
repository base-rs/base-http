// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(feature = "rustls")]

use base_endpoint::{AddressEndpoint, BindEndpoint, BindOptions};
use base_http::{
    InterimHandler, InterimResponses, Method, Request, Response, Status,
    body::{BodyFrame, BufferedBody, EmptyBody, ReadableBody},
};
use base_http_client::{Client, Endpoint, Error, Options};
use base_http_server::Handler;
use base_scheduler::{Cancellation, Clock, SocketLocalAddress};
use base_scheduler_direct::DirectScheduler;
use futures_lite::future::{block_on, zip};
use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
use std::{
    convert::Infallible,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    time::Duration,
};

fn configs(tls12: bool) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert = identity.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let versions = if tls12 {
        &[&rustls::version::TLS12][..]
    } else {
        &[&rustls::version::TLS13][..]
    };
    let client = ClientConfig::builder_with_protocol_versions(versions)
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = ServerConfig::builder_with_protocol_versions(versions)
        .with_no_client_auth()
        .with_single_cert(vec![cert], key.into())
        .unwrap();
    (Arc::new(client), Arc::new(server))
}

async fn payload(body: &mut impl ReadableBody) -> Vec<u8> {
    let mut output = Vec::new();
    loop {
        let frame = body.read(&Cancellation::never()).await.unwrap();
        let end = frame.is_end();
        if let BodyFrame::Data { data, .. } = frame {
            output.extend_from_slice(data.as_ref());
        }
        if end {
            return output;
        }
    }
}
struct Echo;
impl Handler for Echo {
    type Error = Infallible;
    type Body<B: ReadableBody> = B;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<B>, Infallible> {
        assert_eq!(request.scheme.as_deref(), Some("https"));
        Ok(Response::new(Status::OK, request.body))
    }
}

#[test]
fn https_echo_reuses_connections_and_accepts_during_another_handshake() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        for tls12 in [false, true] {
            scheduler
                .timeout(Duration::from_secs(15), async {
                    let (client_config, server_config) = configs(tls12);
                    let endpoint = AddressEndpoint::new("127.0.0.1:0".parse().unwrap());
                    let listener = endpoint
                        .bind(&scheduler, BindOptions::default(), &Cancellation::never())
                        .await
                        .unwrap()
                        .remove(0);
                    let address = scheduler.socket_local_address(&listener).unwrap();
                    // This peer never begins TLS. A valid peer must still progress.
                    let stalled = TcpStream::connect(address).unwrap();
                    let stop = Cancellation::new();
                    let lifetime = Cancellation::new();
                    let endpoint = Endpoint::new(address).with_tls(client_config, "localhost");
                    let options = Options {
                        protocols: base_http_protocol::HttpProtocols { require_alpn: true },
                        ..Options::default()
                    };
                    let (client, driver) = Client::new(&scheduler, endpoint, options).unwrap();
                    let server_options = Default::default();
                    let server = base_http_server::serve_tls(
                        &scheduler,
                        listener,
                        server_config,
                        &Echo,
                        &server_options,
                        &stop,
                    );
                    let application = async {
                        for index in 0..2 {
                            let bytes = vec![b'x'; 1024 * 1024];
                            let mut request = Request::new(
                                Method::POST,
                                "/upload",
                                BufferedBody::from(bytes.clone()),
                            );
                            request.persistent = Some(index == 0);
                            let mut response = client.call(request, &lifetime).await.unwrap();
                            assert_eq!(response.status, Status::OK);
                            assert_eq!(payload(&mut response.body).await, bytes);
                        }
                        client.begin_close();
                    };
                    let client_work = async {
                        let (driven, ()) = zip(driver.run(&lifetime), application).await;
                        stop.cancel();
                        driven.unwrap();
                        assert_eq!(client.pool_stats().total(), 0);
                    };
                    let (served, ()) = zip(server, client_work).await;
                    let statistics = served.unwrap();
                    assert_eq!(statistics.connections, 2);
                    assert_eq!(statistics.requests, 2);
                    drop(stalled);
                })
                .await
                .unwrap();
        }
        scheduler.shutdown().await;
    });
}

#[test]
fn https_client_applies_missing_alpn_policy_before_pool_publication() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        for required in [false, true] {
            let (client_config, server_config) = configs(false);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let peer = std::thread::spawn(move || {
                let (socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut stream = rustls::StreamOwned::new(
                    rustls::ServerConnection::new(server_config).unwrap(),
                    socket,
                );
                let mut request = Vec::new();
                let mut byte = [0];
                loop {
                    match stream.read(&mut byte) {
                        Ok(0) => {
                            assert!(required);
                            break;
                        }
                        Ok(_) => {
                            request.push(byte[0]);
                            if request.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) if required => break,
                        Err(error) => panic!("request: {error}"),
                    }
                }
                if !required {
                    assert!(request.starts_with(b"GET / HTTP/1.1\r\n"));
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .unwrap();
                    stream.conn.send_close_notify();
                    stream.flush().unwrap();
                    let _ = stream.read(&mut byte);
                } else {
                    assert!(request.is_empty());
                }
            });
            scheduler
                .timeout(Duration::from_secs(15), async {
                    let endpoint = Endpoint::new(address).with_tls(client_config, "localhost");
                    let options = Options {
                        protocols: base_http_protocol::HttpProtocols {
                            require_alpn: required,
                        },
                        ..Options::default()
                    };
                    let (client, driver) = Client::new(&scheduler, endpoint, options).unwrap();
                    let application = async {
                        let result = client
                            .call(
                                Request::new(Method::GET, "/", EmptyBody),
                                &Cancellation::never(),
                            )
                            .await;
                        if required {
                            assert!(matches!(
                                result,
                                Err(Error::Negotiation(
                                    base_http_protocol::NegotiationError::Missing
                                ))
                            ));
                        } else {
                            assert_eq!(payload(&mut result.unwrap().body).await, b"ok");
                        }
                        client.begin_close();
                    };
                    let (driven, ()) = zip(driver.run(&Cancellation::never()), application).await;
                    driven.unwrap();
                    assert_eq!(client.pool_stats().total(), 0);
                })
                .await
                .unwrap();
            peer.join().unwrap();
        }
        scheduler.shutdown().await;
    });
}

#[test]
fn https_upload_progresses_while_response_consumption_is_paused() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let (client_config, mut server_config) = configs(false);
    Arc::make_mut(&mut server_config).alpn_protocols = vec![b"http/1.1".to_vec()];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (uploaded, complete) = futures_channel::oneshot::channel();
    let peer = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut stream = rustls::StreamOwned::new(
            rustls::ServerConnection::new(server_config).unwrap(),
            socket,
        );
        let mut request = Vec::new();
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        assert!(
            String::from_utf8(request)
                .unwrap()
                .to_ascii_lowercase()
                .contains("content-length: 1048576\r\n")
        );
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 65536\r\nConnection: close\r\n\r\n")
            .unwrap();
        stream.write_all(&vec![b'r'; 65536]).unwrap();
        stream.flush().unwrap();
        let mut upload = vec![0; 1024 * 1024];
        stream.read_exact(&mut upload).unwrap();
        assert!(upload.iter().all(|byte| *byte == b'u'));
        uploaded.send(()).unwrap();
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
        stream.conn.send_close_notify();
        stream.flush().unwrap();
    });
    block_on(async {
        scheduler
            .timeout(Duration::from_secs(15), async {
                let endpoint = Endpoint::new(address).with_tls(client_config, "localhost");
                let options = Options {
                    response_buffer_bytes: 32768,
                    response_buffer_frames: 1,
                    ..Options::default()
                };
                let (client, driver) = Client::new(&scheduler, endpoint, options).unwrap();
                let application = async {
                    let request = Request::new(
                        Method::POST,
                        "/upload",
                        BufferedBody::from(vec![b'u'; 1024 * 1024]),
                    );
                    let mut response = client.call(request, &Cancellation::never()).await.unwrap();
                    complete.await.unwrap();
                    assert_eq!(payload(&mut response.body).await, vec![b'r'; 65536]);
                    client.begin_close();
                };
                let (result, ()) = zip(driver.run(&Cancellation::never()), application).await;
                result.unwrap();
                assert_eq!(client.pool_stats().total(), 0);
            })
            .await
            .unwrap();
        scheduler.shutdown().await;
    });
    peer.join().unwrap();
}

#[test]
fn https_server_interoperates_with_an_independent_http_client() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    block_on(async {
        let (mut client_config, server_config) = configs(false);
        Arc::make_mut(&mut client_config).alpn_protocols = vec![b"http/1.1".to_vec()];
        let listener = AddressEndpoint::new("127.0.0.1:0".parse().unwrap())
            .bind(&scheduler, BindOptions::default(), &Cancellation::never())
            .await
            .unwrap()
            .remove(0);
        let address = scheduler.socket_local_address(&listener).unwrap();
        let stop = Cancellation::new();
        let stopped = stop.clone();
        let peer = std::thread::spawn(move || {
            let socket = TcpStream::connect(address).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let connection = rustls::ClientConnection::new(
                client_config,
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            )
            .unwrap();
            let mut stream = rustls::StreamOwned::new(connection, socket);
            stream.write_all(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello").unwrap();
            stream.flush().unwrap();
            let mut response = Vec::new();
            // read_to_end requires authenticated TLS closure, not a bare TCP EOF.
            stream.read_to_end(&mut response).unwrap();
            assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
            assert!(response.ends_with(b"\r\n\r\nhello"));
            assert_eq!(stream.conn.alpn_protocol(), Some(b"http/1.1".as_slice()));
            stopped.cancel();
        });
        let options = Default::default();
        let statistics = scheduler
            .timeout(
                Duration::from_secs(15),
                base_http_server::serve_tls(
                    &scheduler,
                    listener,
                    server_config,
                    &Echo,
                    &options,
                    &stop,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(statistics.requests, 1);
        assert_eq!(statistics.failed_connections, 0);
        peer.join().unwrap();
        scheduler.shutdown().await;
    });
}

#[test]
fn https_client_does_not_send_http_after_server_identity_verification_fails() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    let (client_config, mut server_config) = configs(false);
    Arc::make_mut(&mut server_config).alpn_protocols = vec![b"http/1.1".to_vec()];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut stream = rustls::StreamOwned::new(
            rustls::ServerConnection::new(server_config).unwrap(),
            socket,
        );
        let mut data = [0];
        assert!(stream.read(&mut data).is_err());
    });
    block_on(async {
        scheduler
            .timeout(Duration::from_secs(15), async {
                let endpoint = Endpoint::new(address).with_tls(client_config, "wrong.invalid");
                let (client, driver) =
                    Client::new(&scheduler, endpoint, Options::default()).unwrap();
                let application = async {
                    let result = client
                        .call(
                            Request::new(Method::GET, "/", EmptyBody),
                            &Cancellation::never(),
                        )
                        .await;
                    assert!(matches!(
                        result,
                        Err(Error::Protocol(_)) | Err(Error::Io(_))
                    ));
                    assert_eq!(client.pool_stats().total(), 0);
                    client.begin_close();
                };
                let (result, ()) = zip(driver.run(&Cancellation::never()), application).await;
                result.unwrap();
            })
            .await
            .unwrap();
        scheduler.shutdown().await;
    });
    peer.join().unwrap();
}
