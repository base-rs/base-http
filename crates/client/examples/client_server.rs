// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! `cargo run --example client_server` starts a local server, sends three POSTs
//! through the pooled client, and joins the HTTP and transport cleanup.
use base_endpoint::{AddressEndpoint, BindEndpoint, BindOptions};
use base_http::{
    InterimHandler, InterimResponses, Method, Request, RequestMetadata, Response, Status,
    body::{BodyFrame, BufferedBody, ReadableBody},
};
use base_http_client::{Client, Endpoint};
use base_http_server::Handler;
use base_scheduler::{Cancellation, SocketAccept, SocketClose, SocketLocalAddress};
use base_scheduler_direct::DirectScheduler;
use futures_lite::future::{block_on, zip};
use std::{convert::Infallible, future::Future, net::SocketAddr};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Echo;
impl Handler for Echo {
    type Error = Infallible;
    type Body<B: ReadableBody> = B;
    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        request: Request<B>,
        _: &mut InterimResponses<I>,
        _: &Cancellation,
    ) -> std::result::Result<Response<B>, Infallible> {
        Ok(Response::new(Status::OK, request.body))
    }
}
async fn supervised<T>(
    work: impl Future<Output = Result<T>>,
    lifetime: &Cancellation,
) -> Result<T> {
    let result = work.await;
    if result.is_err() {
        lifetime.cancel();
    }
    result
}
async fn run(scheduler: &DirectScheduler, encrypted: bool) -> Result<()> {
    #[cfg(not(feature = "rustls"))]
    if encrypted {
        return Err("enable the rustls feature for --https".into());
    }
    #[cfg(feature = "rustls")]
    let tls = if encrypted {
        Some(tls_configuration()?)
    } else {
        None
    };
    let lifetime = Cancellation::new();
    let address: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = AddressEndpoint::new(address)
        .bind(scheduler, BindOptions::default(), &lifetime)
        .await?
        .remove(0);
    let address = scheduler.socket_local_address(&listener)?;
    let endpoint = Endpoint::new(address);
    #[cfg(feature = "rustls")]
    let (endpoint, tls_server) = match tls {
        Some((client, server)) => (endpoint.with_tls(client, "localhost"), Some(server)),
        None => (endpoint, None),
    };
    // Borrowed request storage can outlive the scoped driver without being static.
    let message = String::from("Hello World");
    // This demo accepts one connection, so cap the pool at one resource.
    let options = base_http_client::Options {
        pool: base_pool::Options {
            max_resources: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let (client, driver) = Client::new(scheduler, endpoint, options)?;
    let server = async {
        let accepted = scheduler.socket_accept(&listener, &lifetime).await;
        scheduler.socket_close(listener).await?;
        let (socket, _) = accepted?;
        #[cfg(feature = "rustls")]
        if let Some(config) = tls_server {
            use base_endpoint::tls::{TlsAcceptor, rustls::RustlsAcceptor};
            let connection = RustlsAcceptor::new(config)
                .accept(scheduler, socket, &lifetime)
                .await?;
            base_http_server::serve_tls_connection(
                scheduler,
                connection,
                &Echo,
                &Default::default(),
                &lifetime,
            )
            .await?;
            return Ok(());
        }
        base_http_server::serve_connection(
            scheduler,
            socket,
            &Echo,
            &Default::default(),
            &lifetime,
        )
        .await?;
        Ok(())
    };
    let application = async {
        // Pull a static body, keeping the existing call interface.
        let request = Request::new(
            Method::POST,
            "/upload",
            BufferedBody::from(message.as_bytes()),
        );
        print_response(client.call(request, &lifetime).await?, &lifetime).await?;

        // Imperative sequential upload: writes go straight into HTTP framing.
        // Suitable when the peer consumes the upload before sending its body.
        let mut request = client.open(Method::POST, "/upload", &lifetime).await?;
        request.write(b"Hello ", &lifetime).await?;
        request.write(b"World", &lifetime).await?;
        print_response(request.finish(&lifetime).await?, &lifetime).await?;

        // Split for a streaming echo/proxy or a peer sending an early response.
        let mut metadata = RequestMetadata::new(Method::POST);
        metadata.path = Some("/echo".into());
        metadata.persistent = Some(false);
        let (mut output, response) = client.open_request(metadata, &lifetime).await?.split();
        let upload = async {
            output.write(message.as_bytes(), &lifetime).await?;
            output.finish(&lifetime).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let download = async { print_response(response.await?, &lifetime).await };
        let (sent, received) = zip(
            supervised(upload, &lifetime),
            supervised(download, &lifetime),
        )
        .await;
        sent?;
        received?;
        client.begin_close();
        Ok(())
    };
    // Applications may instead spawn the driver when all captures are owned.
    // Here everything is scoped and joined, with no detached tasks.
    let (served, (driven, applied)) = zip(
        supervised(server, &lifetime),
        zip(
            supervised(
                async { driver.run(&lifetime).await.map_err(Into::into) },
                &lifetime,
            ),
            supervised(application, &lifetime),
        ),
    )
    .await;
    applied?;
    driven?;
    served?;
    Ok(())
}
async fn print_response<B: ReadableBody>(
    mut response: Response<B>,
    cancellation: &Cancellation,
) -> Result<()> {
    println!("Response: {}", response.status);
    loop {
        let frame = response.read(cancellation).await?;
        let end = frame.is_end();
        if let BodyFrame::Data { data, .. } = frame {
            print!("{}", String::from_utf8_lossy(data.as_ref()));
        }
        if end {
            break;
        }
    }
    println!();
    Ok(())
}
fn main() -> Result<()> {
    let scheduler = DirectScheduler::new(Default::default())?;
    let encrypted = std::env::args().any(|argument| argument == "--https");
    let result = block_on(run(&scheduler, encrypted));
    block_on(scheduler.shutdown());
    result
}

// The local demo trusts exactly its generated server identity. Real callers
// supply their own trust roots and server certificate configuration.
#[cfg(feature = "rustls")]
fn tls_configuration() -> Result<(
    std::sync::Arc<rustls::ClientConfig>,
    std::sync::Arc<rustls::ServerConfig>,
)> {
    use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::PrivatePkcs8KeyDer};
    use std::sync::Arc;
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = identity.cert.der().clone();
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let key = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());
    let mut server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key.into())?;
    server.alpn_protocols = base_http_protocol::HttpProtocols::default().alpn_protocols();
    Ok((Arc::new(client), Arc::new(server)))
}
