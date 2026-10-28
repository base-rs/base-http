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

struct Output(Arc<AtomicUsize>);
impl SocketStreamWriter for Output {
    type Buffer = Vec<u8>;
    async fn write(&mut self, bytes: &[u8], _: &Cancellation) -> OperationResult<usize> {
        self.0.fetch_add(1, Ordering::Relaxed);
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

fn main() {
    let requests = 10_000u64;
    let writes = Arc::new(AtomicUsize::new(0));
    let mut input = RequestReader::new(Input, Default::default()).unwrap();
    let mut output = ResponseWriter::new(Output(writes.clone()));
    let never = Cancellation::never();
    let layout = std::env::args().any(|argument| argument == "--layout");
    let read_future_bytes = std::mem::size_of_val(&input.read_request(&never));
    // Warm the executor and connection-local capacity before measurement.
    block_on(async {
        let (request, context) = input.read_request(&never).await.unwrap().unwrap();
        drop(request);
        let mut response = Response::new(Status::OK, BufferedBody::from(BODY));
        response.fields.add("Content-Type", "text/plain").unwrap();
        let write = output.write_response(&context, &mut response, &never);
        if layout {
            println!(
                "{{\"fields_bytes\":{},\"request_head_bytes\":{},\"incoming_body_bytes\":{},\"request_bytes\":{},\"request_reader_bytes\":{},\"read_future_bytes\":{},\"write_future_bytes\":{}}}",
                std::mem::size_of::<base_http::Fields>(),
                std::mem::size_of::<base_http::RequestHead>(),
                std::mem::size_of::<base_http1::IncomingBody<'_, Input>>(),
                std::mem::size_of::<base_http::Request<base_http1::IncomingBody<'_, Input>>>(),
                std::mem::size_of::<RequestReader<Input>>(),
                read_future_bytes,
                std::mem::size_of_val(&write),
            );
        }
        write.await.unwrap();
    });
    writes.store(0, Ordering::Relaxed);
    let measured = allocation_counter::measure(|| {
        block_on(async {
            for _ in 0..requests {
                let (request, context) = input.read_request(&never).await.unwrap().unwrap();
                drop(request);
                let mut response = Response::new(Status::OK, BufferedBody::from(BODY));
                response.fields.add("Content-Type", "text/plain").unwrap();
                output
                    .write_response(&context, &mut response, &never)
                    .await
                    .unwrap();
            }
        })
    });
    println!(
        "{{\"requests\":{requests},\"allocations_per_request\":{},\"allocated_bytes_per_request\":{},\"writes_per_response\":{}}}",
        measured.count_total as f64 / requests as f64,
        measured.bytes_total as f64 / requests as f64,
        writes.load(Ordering::Relaxed) as f64 / requests as f64
    );
}
