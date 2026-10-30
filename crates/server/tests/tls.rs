// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(feature = "rustls")]

use base_endpoint::tls::{TlsAcceptor, rustls::RustlsAcceptor};
use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_protocol::{HttpProtocols, NegotiationError};
use base_http_server::{Error, Handler, Options};
use base_scheduler::{Cancellation, SocketAccept, SocketClose};
use futures_lite::future::block_on;
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::{
    io::{self, BufReader, Read, Write},
    net::TcpStream,
    sync::Arc,
};

mod support;
use support::*;

// A fixed self-signed identity for `localhost`, valid from 2026 until 2126.
const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----
MIIBljCCATygAwIBAgIUAO/XbmZ+owyWoSE4TWZKstkfpjowCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDEwMTAwMDAwMFoYDzIxMjYwMTAx
MDAwMDAwWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAAQhX1uZCLbVkz07BDBiB0XAww/P8Py3NfEPtuvU9sSW8xh0hghwu/Mo
4yw4X66HHBFcbV0t82Tn9Joeg2FEY1coo2owaDAdBgNVHQ4EFgQUMnX6feDhfc11
pTKQt/9yRHM6is4wDAYDVR0TAQH/BAIwADAUBgNVHREEDTALgglsb2NhbGhvc3Qw
DgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMAoGCCqGSM49BAMC
A0gAMEUCIQC6pOXK8IC7h5Are9PQhyVoSPXEw6v8Q2+iEhhi6QileAIgch0brvvw
U3MGwFZgYTuE5aSl7XypIT84TAiPpxEoLbU=
-----END CERTIFICATE-----
";

const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgOmzuHrKbm9iurYfL
MYXhHbI9MTwEhIB1FhDYIDdEp1qhRANCAAQhX1uZCLbVkz07BDBiB0XAww/P8Py3
NfEPtuvU9sSW8xh0hghwu/Mo4yw4X66HHBFcbV0t82Tn9Joeg2FEY1co
-----END PRIVATE KEY-----
";

type Peer = StreamOwned<ClientConnection, TcpStream>;

fn server_config(alpn: Vec<Vec<u8>>) -> Arc<ServerConfig> {
    let certificate = CertificateDer::from_pem_slice(CERTIFICATE.as_bytes()).unwrap();
    let key = PrivateKeyDer::from_pem_slice(PRIVATE_KEY.as_bytes()).unwrap();
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .unwrap();
    config.alpn_protocols = alpn;
    Arc::new(config)
}

// Connect and complete a handshake offering the given application protocols.
fn handshake(address: std::net::SocketAddr, alpn: Vec<Vec<u8>>) -> Peer {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(CERTIFICATE.as_bytes()).unwrap())
        .unwrap();
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn;
    let connection =
        ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap()).unwrap();
    let mut peer = StreamOwned::new(connection, connect(address).into_inner());
    while peer.conn.is_handshaking() {
        peer.conn.complete_io(&mut peer.sock).unwrap();
    }
    peer
}

// Accept one TLS connection and serve it directly, with the server configured by the caller.
fn tls_connection<T: Send + 'static>(
    config: Arc<ServerConfig>,
    options: Options,
    client_alpn: Vec<Vec<u8>>,
    peer: impl FnOnce(Peer) -> T + Send + 'static,
) -> (Result<u64, Error>, T) {
    let scheduler = scheduler();
    let result = block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer = std::thread::spawn(move || peer(handshake(address, client_alpn)));
        let (socket, _) = scheduler
            .socket_accept(&listener, &cancellation)
            .await
            .unwrap();
        scheduler.socket_close(listener).await.unwrap();
        let connection = RustlsAcceptor::new(config)
            .accept(&scheduler, socket, &cancellation)
            .await
            .unwrap();
        let served = base_http_server::serve_tls_connection(
            &scheduler,
            connection,
            &Hello,
            &options,
            &cancellation,
        )
        .await;
        (served, peer.join().unwrap())
    });
    block_on(scheduler.shutdown());
    result
}

// A rejected connection is aborted after the handshake without HTTP output.
fn rejected(mut peer: Peer) {
    let mut received = Vec::new();
    let _ = peer.read_to_end(&mut received);
    assert!(received.is_empty(), "{received:?}");
}

struct Hello;
impl Handler for Hello {
    type Error = io::Error;
    type Body<B: ReadableBody> = BufferedBody;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> Result<Response<BufferedBody>, io::Error> {
        let scheme = request
            .scheme
            .as_deref()
            .unwrap_or_default()
            .as_bytes()
            .to_vec();
        Ok(Response::new(Status::OK, BufferedBody::from(scheme)))
    }
}

#[test]
fn negotiated_http1_serves_https_requests_on_an_accepted_connection() {
    let protocols = HttpProtocols { require_alpn: true };
    let options = Options {
        protocols,
        ..Default::default()
    };
    let (served, (negotiated, response)) = tls_connection(
        server_config(protocols.alpn_protocols()),
        options,
        protocols.alpn_protocols(),
        |mut peer| {
            let negotiated = peer.conn.alpn_protocol().map(<[u8]>::to_vec);
            peer.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut peer = BufReader::new(peer);
            let response = head(&mut peer);
            assert_eq!(response.status, 200);
            let (data, _) = body(&mut peer, &response);
            assert_eq!(peer.read(&mut [0]).unwrap(), 0);
            (negotiated, data)
        },
    );
    assert_eq!(served.unwrap(), 1);
    assert_eq!(negotiated.as_deref(), Some(b"http/1.1".as_slice()));
    assert_eq!(response, b"https");
}

#[test]
fn missing_alpn_is_rejected_when_negotiation_is_required() {
    let protocols = HttpProtocols { require_alpn: true };
    let options = Options {
        protocols,
        ..Default::default()
    };
    let (served, ()) = tls_connection(
        server_config(protocols.alpn_protocols()),
        options,
        Vec::new(),
        rejected,
    );
    let error = served.unwrap_err();
    assert!(matches!(
        error,
        Error::Negotiation(NegotiationError::Missing)
    ));
    assert_eq!(
        error.to_string(),
        "HTTP application protocol was not negotiated"
    );
}

#[test]
fn unsupported_negotiated_protocol_is_never_served_as_http1() {
    let (served, ()) = tls_connection(
        server_config(vec![b"h2".to_vec()]),
        Options::default(),
        vec![b"h2".to_vec()],
        rejected,
    );
    let error = served.unwrap_err();
    assert!(matches!(
        &error,
        Error::Negotiation(NegotiationError::Unsupported(name)) if name == b"h2"
    ));
    assert_eq!(
        error.to_string(),
        "unsupported HTTP application protocol: \"h2\""
    );
}

#[test]
fn invalid_http1_options_reject_a_negotiated_connection() {
    let options = Options {
        http1: base_http1::Options {
            read_size: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let (served, ()) = tls_connection(
        server_config(HttpProtocols::default().alpn_protocols()),
        options,
        HttpProtocols::default().alpn_protocols(),
        rejected,
    );
    assert!(matches!(
        served,
        Err(Error::Protocol(base_http1::Error::Invalid(_)))
    ));
}

#[test]
fn listener_counts_a_connection_without_required_alpn_as_failed() {
    let scheduler = scheduler();
    block_on(async {
        let (listener, address) = listener(&scheduler).await;
        let cancellation = Cancellation::new();
        let peer_cancel = cancellation.clone();
        let peer = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| {
                rejected(handshake(address, Vec::new()));
                let mut peer = handshake(address, HttpProtocols::default().alpn_protocols());
                peer.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .unwrap();
                let mut peer = BufReader::new(peer);
                let response = head(&mut peer);
                assert_eq!(body(&mut peer, &response).0, b"https");
                assert_eq!(peer.read(&mut [0]).unwrap(), 0);
            });
            peer_cancel.cancel();
            result
        });
        let options = Options {
            protocols: HttpProtocols { require_alpn: true },
            ..Default::default()
        };
        // The listener replaces any configured ALPN list with the enabled protocols.
        let statistics = base_http_server::serve_tls(
            &scheduler,
            listener,
            server_config(vec![b"h2".to_vec()]),
            &Hello,
            &options,
            &cancellation,
        )
        .await
        .unwrap();
        peer.join().unwrap().unwrap();
        assert_eq!(statistics.connections, 2);
        assert_eq!(statistics.requests, 1);
        assert_eq!(statistics.failed_connections, 1);
    });
    block_on(scheduler.shutdown());
}
