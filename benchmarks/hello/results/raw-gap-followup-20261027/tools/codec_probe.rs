// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Count allocations in the HTTP/1 codec independently of socket/runtime work.
//! Input supplies exactly one GET per read; output is a non-allocating sink.

use base_http::{Response, Status, body::BufferedBody};
use base_http1::{RequestReader, ResponseWriter};
use base_scheduler::{
    BufferLease, Cancellation, IoBufSequence, IoBufSequenceMut, OperationResult, SocketRecvFlags,
    SocketRecvResult, SocketSendFlags, SocketStreamReader, SocketStreamWriter,
};
use futures_lite::future::block_on;
use std::{
    io::{self, IoSlice, IoSliceMut},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n\r\n";
const BODY: &[u8] = b"Hello, world!";

fn unsupported<T>() -> OperationResult<T> {
    Err(io::Error::from(io::ErrorKind::Unsupported).into())
}

struct Input;
impl SocketStreamReader for Input {
    type Buffer = BufferLease;
    async fn read(&mut self, _: &Cancellation) -> OperationResult<Option<BufferLease>> {
        unsupported()
    }
    async fn read_into(&mut self, buffer: &mut [u8], _: &Cancellation) -> OperationResult<usize> {
        assert!(buffer.len() >= REQUEST.len());
        buffer[..REQUEST.len()].copy_from_slice(REQUEST);
        Ok(REQUEST.len())
    }
    async fn read_vectored(
        &mut self,
        _: &mut [IoSliceMut<'_>],
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unsupported()
    }
    async fn read_vectored_with_flags(
        &mut self,
        _: &mut [IoSliceMut<'_>],
        _: SocketRecvFlags,
        _: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        unsupported()
    }
    async fn read_owned_vectored<B: IoBufSequenceMut>(
        &mut self,
        buffers: B,
        _: SocketRecvFlags,
        _: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        (unsupported(), buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Stats { writes: AtomicUsize, bytes: AtomicUsize }
struct Output(Arc<Stats>);
impl SocketStreamWriter for Output {
    type Buffer = Vec<u8>;
    async fn write(&mut self, bytes: &[u8], _: &Cancellation) -> OperationResult<usize> {
        self.0.writes.fetch_add(1, Ordering::Relaxed);
        self.0.bytes.fetch_add(bytes.len(), Ordering::Relaxed);
        std::hint::black_box(bytes);
        Ok(bytes.len())
    }
    async fn write_vectored(
        &mut self,
        _: &[IoSlice<'_>],
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unsupported()
    }
    async fn submit(
        &mut self,
        buffer: Vec<u8>,
        _: &Cancellation,
    ) -> Result<(), (base_scheduler::OperationError, Vec<u8>)> {
        Err((io::Error::from(io::ErrorKind::Unsupported).into(), buffer))
    }
    async fn flush(&mut self, _: &Cancellation) -> OperationResult<()> {
        Ok(())
    }
    async fn write_vectored_with_flags(
        &mut self,
        _: &[IoSlice<'_>],
        _: SocketSendFlags,
        _: &Cancellation,
    ) -> OperationResult<usize> {
        unsupported()
    }
    async fn write_owned_vectored<B: IoBufSequence>(
        &mut self,
        buffers: B,
        _: SocketSendFlags,
        _: &Cancellation,
    ) -> (OperationResult<usize>, B) {
        (unsupported(), buffers)
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}


struct HyperIo { remaining: usize, stats: Arc<Stats> }
impl hyper::rt::Read for HyperIo {
    fn poll_read(mut self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>, mut buffer: hyper::rt::ReadBufCursor<'_>) -> std::task::Poll<io::Result<()>> {
        if self.remaining != 0 {
            assert!(buffer.remaining() >= REQUEST.len());
            buffer.put_slice(REQUEST); self.remaining -= 1;
        }
        std::task::Poll::Ready(Ok(()))
    }
}
impl hyper::rt::Write for HyperIo {
    fn poll_write(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>, bytes: &[u8]) -> std::task::Poll<io::Result<usize>> {
        std::hint::black_box(bytes);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes.fetch_add(bytes.len(), Ordering::Relaxed);
        std::task::Poll::Ready(Ok(bytes.len()))
    }
    fn is_write_vectored(&self) -> bool { true }
    fn poll_write_vectored(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>, buffers: &[IoSlice<'_>]) -> std::task::Poll<io::Result<usize>> {
        let mut bytes=0; for buffer in buffers { bytes+=buffer.len();std::hint::black_box(buffer.as_ref()); }
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes.fetch_add(bytes, Ordering::Relaxed);
        std::task::Poll::Ready(Ok(bytes))
    }
    fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> { std::task::Poll::Ready(Ok(())) }
    fn poll_shutdown(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> { std::task::Poll::Ready(Ok(())) }
}
fn run_base(requests: usize) -> Arc<Stats> {
    let stats=Arc::new(Stats::default());
    let mut input=RequestReader::new(Input,Default::default()).unwrap();
    let mut output=ResponseWriter::new(Output(stats.clone()));let never=Cancellation::never();
    block_on(async {
        for _ in 0..requests {
            let (request,context)=input.read_request(&never).await.unwrap().unwrap();drop(request);
            let mut response=Response::new(Status::OK,BufferedBody::from(BODY));
            response.fields.add("Content-Type","text/plain").unwrap();
            output.write_response(&context,&mut response,&never).await.unwrap();
        }
    });
    stats
}
fn run_hyper(requests: usize) -> Arc<Stats> {
    let stats=Arc::new(Stats::default());let io=HyperIo {remaining:requests,stats:stats.clone()};
    let mut builder=hyper::server::conn::http1::Builder::new();builder.auto_date_header(false);builder.half_close(true);
    block_on(builder.serve_connection(io,hyper::service::service_fn(|_:hyper::Request<hyper::body::Incoming>| async {
        let mut response=hyper::Response::new(http_body_util::Full::new(hyper::body::Bytes::from_static(BODY)));
        response.headers_mut().insert(hyper::header::CONTENT_TYPE,hyper::header::HeaderValue::from_static("text/plain"));
        Ok::<_,std::convert::Infallible>(response)
    }))).unwrap();
    stats
}
fn run_parse(requests: usize) -> Arc<Stats> {
    let mut input=RequestReader::new(Input,Default::default()).unwrap();let never=Cancellation::never();
    block_on(async {for _ in 0..requests {
        let (request,context)=input.read_request(&never).await.unwrap().unwrap();drop(request);
        assert!(context.is_complete());
    }});
    Arc::new(Stats::default())
}
fn run_encode(requests: usize) -> Arc<Stats> {
    let stats=Arc::new(Stats::default());let mut input=RequestReader::new(Input,Default::default()).unwrap();
    let mut output=ResponseWriter::new(Output(stats.clone()));let never=Cancellation::never();
    block_on(async {
        let (request,context)=input.read_request(&never).await.unwrap().unwrap();drop(request);
        for _ in 0..requests {
            let mut response=Response::new(Status::OK,BufferedBody::from(BODY));
            response.fields.add("Content-Type","text/plain").unwrap();
            output.write_response(&context,&mut response,&never).await.unwrap();
        }
    });stats
}
fn main() {
    let args:Vec<_>=std::env::args().collect();
    let run:fn(usize)->Arc<Stats>=match args.get(1).map(String::as_str) {Some("base")=>run_base,Some("hyper")=>run_hyper,Some("base-parse")=>run_parse,Some("base-encode")=>run_encode,_=>panic!("usage: codec-probe base|hyper|base-parse|base-encode REQUESTS")};
    let requests:usize=args.get(2).unwrap().parse().unwrap();assert!(requests>0);
    run(1000);let start=std::time::Instant::now();let stats=run(requests);let elapsed=start.elapsed();
    let responses=if args[1]=="base-parse" {0} else {requests};
    assert_eq!(stats.writes.load(Ordering::Relaxed),responses);
    assert_eq!(stats.bytes.load(Ordering::Relaxed),78*responses);
    let alloc=allocation_counter::measure(|| {std::hint::black_box(run(10000));});
    println!("{{\"variant\":\"{}\",\"requests\":{},\"ns_per_request\":{},\"allocations_per_request\":{},\"allocated_bytes_per_request\":{},\"writes_per_request\":{}}}",args[1],requests,elapsed.as_nanos() as f64/requests as f64,alloc.count_total as f64/10000.0,alloc.bytes_total as f64/10000.0,responses as f64/requests as f64);
}
