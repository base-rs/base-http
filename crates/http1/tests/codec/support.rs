// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_endpoint::stream::{ReadableStream, WritableStream};
use base_http1::{Error, Exchange, RequestReader, ResponseWriter};
use base_scheduler::{Cancellation, OperationResult};
use futures_lite::future::block_on;
use std::{
    io,
    sync::{Arc, Mutex},
};

/// Delivers `bytes` in reads of at most `step` bytes, then ends cleanly or, when `fail` is set, fails.
pub struct Input {
    pub bytes: &'static [u8],
    pub step: usize,
    pub fail: bool,
}

impl ReadableStream for Input {
    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        cancellation.check()?;
        if self.bytes.is_empty() && self.fail {
            return Err(io::Error::from(io::ErrorKind::ConnectionReset).into());
        }
        let n = self.bytes.len().min(buffer.len()).min(self.step);
        buffer[..n].copy_from_slice(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Ok(n)
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Records written bytes, refusing every write when `fail` is set and every flush when `fail_flush` is set.
pub struct Output {
    pub bytes: Arc<Mutex<Vec<u8>>>,
    pub fail: bool,
    pub fail_flush: bool,
}

impl WritableStream for Output {
    async fn write(&mut self, bytes: &[u8], cancellation: &Cancellation) -> OperationResult<usize> {
        cancellation.check()?;
        if self.fail {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    async fn flush(&mut self, cancellation: &Cancellation) -> OperationResult<()> {
        cancellation.check()?;
        if self.fail_flush {
            return Err(io::Error::from(io::ErrorKind::TimedOut).into());
        }
        Ok(())
    }
    async fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn input(bytes: &'static [u8], step: usize) -> Input {
    Input {
        bytes,
        step,
        fail: false,
    }
}

pub fn failing_input(bytes: &'static [u8]) -> Input {
    Input {
        bytes,
        step: usize::MAX,
        fail: true,
    }
}

pub fn reader(bytes: &'static [u8], step: usize) -> RequestReader<Input> {
    RequestReader::new(input(bytes, step), Default::default()).unwrap()
}

pub fn exchange(bytes: &'static [u8]) -> Exchange {
    block_on(reader(bytes, usize::MAX).read_request(&Cancellation::never()))
        .unwrap()
        .unwrap()
        .1
}

pub fn output() -> (ResponseWriter<Output>, Arc<Mutex<Vec<u8>>>) {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = ResponseWriter::new(Output {
        bytes: bytes.clone(),
        fail: false,
        fail_flush: false,
    });
    (writer, bytes)
}

pub fn failing_output() -> ResponseWriter<Output> {
    ResponseWriter::new(Output {
        bytes: Default::default(),
        fail: true,
        fail_flush: false,
    })
}

pub fn unflushable_output() -> (ResponseWriter<Output>, Arc<Mutex<Vec<u8>>>) {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = ResponseWriter::new(Output {
        bytes: bytes.clone(),
        fail: false,
        fail_flush: true,
    });
    (writer, bytes)
}

pub fn is_io(result: Result<impl Sized, Error>, kind: io::ErrorKind) -> bool {
    matches!(result, Err(Error::Io(error)) if error.kind() == kind)
}

pub fn is_invalid(result: Result<impl Sized, Error>, reason: &str) -> bool {
    matches!(result, Err(Error::Invalid(r)) if r == reason)
}
