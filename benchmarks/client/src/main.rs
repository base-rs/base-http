// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::{
    Method, Request, RequestMetadata,
    body::{BodyFrame, EmptyBody, ReadableBody},
};
use base_http_client::{Client as BaseClient, Endpoint, Options};
use base_scheduler::{Cancellation, Clock, Spawn};
use base_scheduler_native::{NativeOptions, NativeScheduler};
use futures_util::{StreamExt, future::join, stream::FuturesUnordered};
use hdrhistogram::Histogram;
use http_body_util::{BodyExt, Empty, Full};
use hyper::{body::Bytes, service::service_fn};
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::{TokioExecutor, TokioIo},
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use serde_json::json;
use std::{
    convert::Infallible,
    future::Future,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

mod allocation;
mod established;
mod pool;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const PAYLOAD: &[u8] = b"Hello World";

#[derive(Clone)]
struct Args {
    role: String,
    implementation: String,
    address: SocketAddr,
    identity: PathBuf,
    tls: bool,
    fresh: bool,
    concurrency: usize,
    seconds: f64,
    warmup: f64,
}
impl Args {
    fn parse() -> Result<Self> {
        let mut args = std::env::args().skip(1);
        let mut config = Self {
            role: args.next().ok_or("missing role")?,
            implementation: "base-open".into(),
            address: "127.0.0.1:0".parse()?,
            identity: PathBuf::from("/tmp/base-http-benchmark-identity"),
            tls: false,
            fresh: false,
            concurrency: 1,
            seconds: 5.0,
            warmup: 1.0,
        };
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--tls" => config.tls = true,
                "--fresh" => config.fresh = true,
                "--implementation" => {
                    config.implementation = args.next().ok_or("missing implementation")?
                }
                "--address" => config.address = args.next().ok_or("missing address")?.parse()?,
                "--identity" => config.identity = args.next().ok_or("missing identity")?.into(),
                "--concurrency" => {
                    config.concurrency = args.next().ok_or("missing concurrency")?.parse()?
                }
                "--seconds" => config.seconds = args.next().ok_or("missing seconds")?.parse()?,
                "--warmup" => config.warmup = args.next().ok_or("missing warmup")?.parse()?,
                _ => return Err(format!("unknown flag {flag}").into()),
            }
        }
        if config.concurrency == 0 || config.seconds <= 0.0 || config.warmup <= 0.0 {
            return Err("invalid measurement limits".into());
        }
        Ok(config)
    }
}
fn certificate(args: &Args) -> Result<CertificateDer<'static>> {
    Ok(std::fs::read(args.identity.join("certificate.der"))?.into())
}
fn client_config(args: &Args) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(certificate(args)?)?;
    let mut config = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.resumption = rustls::client::Resumption::disabled();
    Ok(config)
}
fn server_config(args: &Args) -> Result<ServerConfig> {
    let key = PrivateKeyDer::try_from(std::fs::read(args.identity.join("private-key.der"))?)?;
    let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_no_client_auth()
        .with_single_cert(vec![certificate(args)?], key)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config.send_tls13_tickets = 0;
    Ok(config)
}
fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // Process accounting only; no pointers escape the synchronous syscall.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0
}
fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}
fn check_data(offset: &mut usize, bytes: &[u8]) -> Result<()> {
    if PAYLOAD.get(*offset..*offset + bytes.len()) != Some(bytes) {
        return Err("incorrect response data".into());
    }
    *offset += bytes.len();
    Ok(())
}
trait BenchClient: Sync + Sized {
    fn request(&self) -> impl Future<Output = Result<()>> + Send;
    fn settle(&self) -> impl Future<Output = Result<()>> + Send;
    fn phase(
        &self,
        concurrency: usize,
        seconds: f64,
    ) -> impl Future<Output = Result<Histogram<u64>>> + Send {
        phase(self, concurrency, seconds)
    }
}
struct Base<'a> {
    scheduler: &'a NativeScheduler,
    client: BaseClient<'a>,
    direct: bool,
    fresh: bool,
}
impl BenchClient for Base<'_> {
    async fn settle(&self) -> Result<()> {
        self.scheduler
            .sleep(Duration::from_millis(50), &Cancellation::never())
            .await?;
        Ok(())
    }
    async fn request(&self) -> Result<()> {
        let cancel = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::GET);
        metadata.path = Some("/hello".into());
        metadata.content_length = Some(0);
        if self.fresh {
            metadata.persistent = Some(false);
        }
        if self.direct {
            let request = self.client.open_request(metadata, &cancel).await?;
            let response = request.finish(&cancel).await?;
            if response.status.as_u16() != 200 {
                return Err("incorrect status".into());
            }
            consume_base(response.body).await
        } else {
            let response = self
                .client
                .call(Request::from_parts(metadata, EmptyBody), &cancel)
                .await?;
            if response.status.as_u16() != 200 {
                return Err("incorrect status".into());
            }
            consume_base(response.body).await
        }
    }
}
async fn consume_base(mut body: impl ReadableBody) -> Result<()> {
    let mut offset = 0;
    loop {
        let frame = body.read(&Cancellation::never()).await?;
        let end = frame.is_end();
        if let BodyFrame::Data { data, .. } = frame {
            check_data(&mut offset, data.as_ref())?;
        }
        if end {
            break;
        }
    }
    if offset != PAYLOAD.len() {
        return Err("truncated response".into());
    }
    Ok(())
}
#[derive(Clone)]
struct Hyper {
    client: HyperClient<hyper_rustls::HttpsConnector<HttpConnector>, Empty<Bytes>>,
    uri: hyper::Uri,
    fresh: bool,
}
impl BenchClient for Hyper {
    async fn settle(&self) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }
    async fn phase(&self, concurrency: usize, seconds: f64) -> Result<Histogram<u64>> {
        let deadline = Instant::now() + Duration::from_secs_f64(seconds);
        let mut tasks = FuturesUnordered::new();
        for _ in 0..concurrency {
            let client = self.clone();
            tasks.push(tokio::spawn(async move { lane(&client, deadline).await }));
        }
        let mut total = histogram();
        while let Some(task) = tasks.next().await {
            total.add(task.map_err(|error| error.to_string())??)?;
        }
        Ok(total)
    }
    async fn request(&self) -> Result<()> {
        let mut builder = hyper::Request::get(self.uri.clone()).header("Content-Length", "0");
        if self.fresh {
            builder = builder.header("Connection", "close");
        }
        let response = self.client.request(builder.body(Empty::new())?).await?;
        if response.status().as_u16() != 200 {
            return Err("incorrect status".into());
        }
        let mut body = response.into_body();
        let mut offset = 0;
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame?.into_data() {
                check_data(&mut offset, &data)?;
            }
        }
        if offset != PAYLOAD.len() {
            return Err("truncated response".into());
        }
        Ok(())
    }
}
async fn lane(client: &impl BenchClient, deadline: Instant) -> Result<Histogram<u64>> {
    let mut latency = histogram();
    while Instant::now() < deadline {
        let start = Instant::now();
        client.request().await?;
        latency.record(start.elapsed().as_nanos().max(1) as u64)?;
    }
    Ok(latency)
}
async fn phase(
    client: &impl BenchClient,
    concurrency: usize,
    duration: f64,
) -> Result<Histogram<u64>> {
    let deadline = Instant::now() + Duration::from_secs_f64(duration);
    let mut lanes = FuturesUnordered::new();
    for _ in 0..concurrency {
        lanes.push(lane(client, deadline));
    }
    let mut total = histogram();
    while let Some(lane) = lanes.next().await {
        total.add(lane?)?;
    }
    Ok(total)
}
async fn measure(client: &impl BenchClient, args: &Args) -> Result<serde_json::Value> {
    let mut opening = FuturesUnordered::new();
    for _ in 0..args.concurrency {
        opening.push(client.request());
    }
    while let Some(request) = opening.next().await {
        request?;
    }
    client.settle().await?;
    let warm = client.phase(args.concurrency, args.warmup).await?;
    let start = Instant::now();
    let cpu_start = cpu_seconds();
    let allocations = allocation::Snapshot::now();
    let measured = client.phase(args.concurrency, args.seconds).await?;
    let allocations = allocations.finish();
    let cpu = cpu_seconds() - cpu_start;
    let elapsed = start.elapsed().as_secs_f64();
    let mut result = result_json(
        args,
        warm.len() + args.concurrency as u64,
        &measured,
        elapsed,
        cpu,
    );
    allocations.attach(&mut result);
    Ok(result)
}
fn result_json(
    args: &Args,
    warmup_requests: u64,
    measured: &Histogram<u64>,
    elapsed: f64,
    cpu: f64,
) -> serde_json::Value {
    json!({
        "implementation": args.implementation, "tls": args.tls, "fresh": args.fresh,
        "concurrency": args.concurrency, "warmup_requests": warmup_requests, "requests": measured.len(),
        "elapsed_seconds": elapsed, "requests_per_second": measured.len() as f64 / elapsed,
        "client_cpu_seconds": cpu, "client_cpu_us_per_request": cpu * 1e6 / measured.len() as f64,
        "latency_us": {"p50": measured.value_at_quantile(0.5) as f64 / 1000.0,
            "p90": measured.value_at_quantile(0.9) as f64 / 1000.0,
            "p99": measured.value_at_quantile(0.99) as f64 / 1000.0,
            "mean": measured.mean() / 1000.0, "max": measured.max() as f64 / 1000.0},
    })
}
async fn base_client(scheduler: &NativeScheduler, args: &Args) -> Result<serde_json::Value> {
    let mut endpoint = Endpoint::new(args.address);
    if args.tls {
        endpoint = endpoint.with_tls(Arc::new(client_config(args)?), "127.0.0.1");
    }
    let options = Options {
        pool: base_pool::Options {
            max_resources: args.concurrency * 2,
            max_constructing: args.concurrency,
            max_waiters: args.concurrency * 4,
        },
        max_requests: args.concurrency * 4,
        ..Default::default()
    };
    let (client, driver) = BaseClient::new(scheduler, endpoint, options)?;
    let client = Base {
        scheduler,
        client,
        direct: args.implementation == "base-open",
        fresh: args.fresh,
    };
    let lifetime = Cancellation::new();
    let work = async {
        let result = measure(&client, args).await;
        if result.is_err() {
            lifetime.cancel();
        }
        client.client.begin_close();
        result
    };
    let (driven, measured) = join(driver.run(&lifetime), work).await;
    driven?;
    assert_eq!(client.client.pool_stats().total(), 0);
    measured
}
async fn hyper_client(args: &Args) -> Result<serde_json::Value> {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    // Match the Base client's default; the server uses NODELAY.
    http.set_nodelay(true);
    let mut tls = client_config(args)?;
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = hyper_rustls::HttpsConnector::from((http, Arc::new(tls)));
    let mut builder = HyperClient::builder(TokioExecutor::new());
    builder
        .pool_max_idle_per_host(args.concurrency * 2)
        .pool_idle_timeout(None)
        .retry_canceled_requests(false);
    let client = Hyper {
        client: builder.build(connector),
        uri: format!(
            "{}://{}/hello",
            if args.tls { "https" } else { "http" },
            args.address
        )
        .parse()?,
        fresh: args.fresh,
    };
    measure(&client, args).await
}
async fn serve_io<I>(io: I, requests: Arc<AtomicU64>) -> Result<()>
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
        assert_eq!(request.method(), hyper::Method::GET);
        assert_eq!(request.uri().path(), "/hello");
        requests.fetch_add(1, Ordering::Relaxed);
        async { Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::from_static(PAYLOAD)))) }
    });
    hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(io), service)
        .await?;
    Ok(())
}
async fn server(args: &Args) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(args.address).await?;
    let acceptor = if args.tls {
        Some(tokio_rustls::TlsAcceptor::from(Arc::new(server_config(
            args,
        )?)))
    } else {
        None
    };
    let connections = Arc::new(AtomicU64::new(0));
    let requests = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    println!("READY {}", listener.local_addr()?);
    std::io::stdout().flush()?;
    loop {
        tokio::select! {
            _ = stop.recv() => break,
            socket = listener.accept() => {
                let (socket, _) = socket?;
                socket.set_nodelay(true)?;
                connections.fetch_add(1, Ordering::Relaxed);
                let (requests, failures, acceptor) = (requests.clone(), failures.clone(), acceptor.clone());
                tokio::spawn(async move {
                    let result = async {
                        if let Some(acceptor) = acceptor {
                            let mut tls = acceptor.accept(socket).await?;
                            assert_eq!(tls.get_ref().1.protocol_version(), Some(rustls::ProtocolVersion::TLSv1_3));
                            assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"http/1.1".as_slice()));
                            let result = serve_io(&mut tls, requests).await;
                            let _ = tokio::io::AsyncWriteExt::shutdown(&mut tls).await;
                            result
                        } else { serve_io(socket, requests).await }
                    }.await;
                    if let Err(error) = result {
                        failures.fetch_add(1, Ordering::Relaxed);
                        eprintln!("connection error: {error}");
                    }
                });
            }
        }
    }
    println!(
        "STATS {}",
        json!({"connections": connections.load(Ordering::Relaxed), "requests": requests.load(Ordering::Relaxed), "failures": failures.load(Ordering::Relaxed)})
    );
    Ok(())
}
fn main() -> Result<()> {
    if std::env::var_os("BENCH_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter("hyper=trace,hyper_util=trace")
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .init();
    }
    let args = Args::parse()?;
    match args.role.as_str() {
        "pool" => {
            futures_lite::future::block_on(pool::run(&args)).map(|result| println!("{result}"))
        }
        "identity" => {
            std::fs::create_dir_all(&args.identity)?;
            let identity =
                rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
            std::fs::write(args.identity.join("certificate.der"), identity.cert.der())?;
            std::fs::write(
                args.identity.join("private-key.der"),
                identity.signing_key.serialize_der(),
            )?;
            Ok(())
        }
        "server" => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(server(&args)),
        "client" if args.implementation.starts_with("hyper") => {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async move {
                    tokio::spawn(async move {
                        if args.implementation == "hyper-connection" {
                            established::hyper(&args).await
                        } else {
                            hyper_client(&args).await
                        }
                    })
                    .await?
                })
                .map(|result| println!("{result}"))
        }
        "client"
            if ["base-call", "base-open", "base-connection"]
                .contains(&args.implementation.as_str()) =>
        {
            let scheduler = Arc::new(NativeScheduler::with_options(NativeOptions {
                workers: 1,
                threads: 1,
                capacity: 1024,
                timer_capacity: 1024,
            })?);
            let worker = scheduler.clone();
            let task = scheduler.spawn(async move {
                if args.implementation == "base-connection" {
                    established::base(&worker, &args).await
                } else {
                    base_client(&worker, &args).await
                }
            })?;
            let result = scheduler.block_on(task).map_err(|error| error.to_string());
            scheduler.block_on(scheduler.shutdown());
            println!("{}", result??);
            Ok(())
        }
        _ => Err("expected identity, server, or client with base-open/base-call/hyper".into()),
    }
}
