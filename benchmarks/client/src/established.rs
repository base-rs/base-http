//! Connection-only measurements: persistent codecs/transports without a pool.
use super::*;
use base_endpoint::{
    AddressEndpoint, ConnectEndpoint,
    stream::{ReadableStream, WritableStream},
};
use base_scheduler::{SocketClose, SocketStreamReading, SocketStreamWriting};
use std::sync::Mutex;
use tokio::sync::Barrier;

trait ConnectionClient {
    fn request(&mut self) -> impl Future<Output = Result<()>> + Send;
}
struct Phases {
    barrier: Barrier,
    deadline: Mutex<Instant>,
    results: Mutex<(u64, Histogram<u64>)>,
}
impl Phases {
    fn new(concurrency: usize) -> Self {
        Self {
            barrier: Barrier::new(concurrency + 1),
            deadline: Mutex::new(Instant::now()),
            results: Mutex::new((concurrency as u64, histogram())),
        }
    }
    async fn exercise(&self, client: &mut impl ConnectionClient) -> Result<()> {
        client.request().await?;
        self.barrier.wait().await; // Prepared.
        self.barrier.wait().await; // Start warmup.
        let deadline = *self.deadline.lock().unwrap();
        let warm = connection_phase(client, deadline).await?;
        self.results.lock().unwrap().0 += warm.len();
        self.barrier.wait().await; // Warmup completed.
        self.barrier.wait().await; // Start measurement.
        let deadline = *self.deadline.lock().unwrap();
        let measured = connection_phase(client, deadline).await?;
        self.results.lock().unwrap().1.add(measured)?;
        self.barrier.wait().await; // Measured responses all consumed.
        self.barrier.wait().await; // Controller captured time/CPU; cleanup may start.
        Ok(())
    }
    async fn measure(&self, args: &Args) -> Result<serde_json::Value> {
        self.barrier.wait().await;
        *self.deadline.lock().unwrap() = Instant::now() + Duration::from_secs_f64(args.warmup);
        self.barrier.wait().await;
        self.barrier.wait().await;
        let start = Instant::now();
        let cpu_start = cpu_seconds();
        let allocations = allocation::Snapshot::now();
        *self.deadline.lock().unwrap() = start + Duration::from_secs_f64(args.seconds);
        self.barrier.wait().await;
        self.barrier.wait().await;
        let cpu = cpu_seconds() - cpu_start;
        let allocations = allocations.finish();
        let elapsed = start.elapsed().as_secs_f64();
        let mut result = {
            let results = self.results.lock().unwrap();
            result_json(args, results.0, &results.1, elapsed, cpu)
        };
        allocations.attach(&mut result);
        self.barrier.wait().await;
        Ok(result)
    }
}
async fn connection_phase(
    client: &mut impl ConnectionClient,
    deadline: Instant,
) -> Result<Histogram<u64>> {
    let mut measured = histogram();
    while Instant::now() < deadline {
        let start = Instant::now();
        client.request().await?;
        measured.record(start.elapsed().as_nanos().max(1) as u64)?;
    }
    Ok(measured)
}
struct BaseConnection<R, W> {
    input: base_http1::ResponseReader<R>,
    output: base_http1::RequestWriter<W>,
    authority: String,
}
impl<R: ReadableStream, W: WritableStream> BaseConnection<R, W> {
    fn new(reader: R, writer: W, args: &Args) -> Result<Self> {
        Ok(Self {
            input: base_http1::ResponseReader::new(reader, Default::default())?,
            output: base_http1::RequestWriter::new(writer),
            authority: args.address.to_string(),
        })
    }
    async fn run(mut self, phases: &Phases) -> Result<()> {
        let result = phases.exercise(&mut self).await;
        let a = self.input.close().await;
        let b = self.output.close().await;
        result?;
        a?;
        b?;
        Ok(())
    }
}
impl<R: ReadableStream, W: WritableStream> ConnectionClient for BaseConnection<R, W> {
    async fn request(&mut self) -> Result<()> {
        let cancel = Cancellation::never();
        let mut metadata = RequestMetadata::new(Method::GET);
        metadata.path = Some("/hello".into());
        metadata.authority = Some(self.authority.as_str().into());
        metadata.content_length = Some(0);
        self.output.begin_request(&metadata, &cancel).await?;
        self.output
            .write_frame(BodyFrame::<Vec<u8>>::End, &cancel)
            .await?;
        let mut interim =
            base_http::InterimResponses::new(|_: base_http::ResponseMetadata| async {
                Ok::<_, Infallible>(())
            });
        let response = self
            .input
            .read_response(&metadata, &mut interim, &cancel)
            .await?;
        if response.status.as_u16() != 200 {
            return Err("incorrect status".into());
        }
        consume_base(response.body).await
    }
}
async fn base_lane(scheduler: &NativeScheduler, args: &Args, phases: &Phases) -> Result<()> {
    let endpoint = AddressEndpoint::new(args.address);
    let cancel = Cancellation::never();
    if args.tls {
        use base_endpoint::tls::{TlsClientEndpoint, rustls::RustlsConnector};
        let mut config = client_config(args)?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let endpoint = TlsClientEndpoint::new(
            endpoint,
            RustlsConnector::new(Arc::new(config)),
            "127.0.0.1",
        );
        let connection = endpoint.connect(scheduler, &cancel).await?;
        connection
            .with_duplex(scheduler, &cancel, |reader, writer| async move {
                BaseConnection::new(reader, writer, args)?.run(phases).await
            })
            .await
    } else {
        let socket = endpoint.connect(scheduler, &cancel).await?;
        let reader = scheduler.socket_stream_reader(&socket)?;
        let writer = scheduler.socket_stream_writer(&socket)?;
        let result = BaseConnection::new(reader, writer, args)?.run(phases).await;
        scheduler.socket_close(socket).await?;
        result
    }
}
pub(super) async fn base(scheduler: &NativeScheduler, args: &Args) -> Result<serde_json::Value> {
    if args.fresh {
        return Err("connection-only benchmark requires reuse".into());
    }
    let phases = Phases::new(args.concurrency);
    let mut jobs = FuturesUnordered::new();
    for _ in 0..args.concurrency {
        jobs.push(base_lane(scheduler, args, &phases));
    }
    let connections = async {
        while let Some(result) = jobs.next().await {
            result?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (_, measured) = futures_util::future::try_join(connections, phases.measure(args)).await?;
    Ok(measured)
}
struct HyperConnection {
    sender: hyper::client::conn::http1::SendRequest<Empty<Bytes>>,
    authority: String,
}
impl ConnectionClient for HyperConnection {
    async fn request(&mut self) -> Result<()> {
        let request = hyper::Request::get("/hello")
            .header("Host", &self.authority)
            .header("Content-Length", "0")
            .body(Empty::new())?;
        let response = self.sender.send_request(request).await?;
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
async fn hyper_io<I>(io: I, args: &Args, phases: &Phases) -> Result<()>
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let task = tokio::spawn(connection);
    let mut client = HyperConnection {
        sender,
        authority: args.address.to_string(),
    };
    let result = phases.exercise(&mut client).await;
    drop(client);
    task.await??;
    result
}
pub(super) async fn hyper(args: &Args) -> Result<serde_json::Value> {
    if args.fresh {
        return Err("connection-only benchmark requires reuse".into());
    }
    let phases = Arc::new(Phases::new(args.concurrency));
    let mut jobs = FuturesUnordered::new();
    for _ in 0..args.concurrency {
        let args = args.clone();
        let phases = phases.clone();
        jobs.push(tokio::spawn(async move {
            let socket = tokio::net::TcpStream::connect(args.address).await?;
            socket.set_nodelay(false)?;
            if args.tls {
                let mut config = client_config(&args)?;
                config.alpn_protocols = vec![b"http/1.1".to_vec()];
                let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
                    .connect("127.0.0.1".try_into()?, socket)
                    .await?;
                hyper_io(tls, &args, &phases).await
            } else {
                hyper_io(socket, &args, &phases).await
            }
        }));
    }
    let connections = async {
        while let Some(result) = jobs.next().await {
            result??;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (_, measured) = futures_util::future::try_join(connections, phases.measure(args)).await?;
    Ok(measured)
}
