// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A scripted request body and an in-memory upload store, shared by the integration and unit tests.

#![allow(dead_code)]

use base_http::{
    Fields, Method, Request,
    body::{BodyError, BodyFrame, ReadableBody},
};
use base_http_content::UploadStore;
use base_scheduler::Cancellation;
use base_web_content::UploadHead;
use std::{collections::VecDeque, io};

/// One step of a [`TestBody`].
#[derive(Clone)]
pub enum Step {
    Data(Vec<u8>),
    /// Data that ends the body.
    Last(Vec<u8>),
    Trailers,
    Fail,
    /// Cancel this token, then deliver the data, as a cancellation that arrives while the frame is handled.
    CancelThen(Cancellation, Vec<u8>),
    /// Cancel this token and fail the read, as a body observing cancellation.
    Cancel(Cancellation),
}

/// A body delivering scripted frames, then the end.
pub struct TestBody {
    steps: VecDeque<Step>,
    length: Option<u64>,
    /// The number of reads.
    pub reads: usize,
    pub closed: bool,
}

impl TestBody {
    pub fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            length: None,
            reads: 0,
            closed: false,
        }
    }

    /// The body divided into frames at these positions.
    pub fn split(body: &[u8], positions: &[usize]) -> Self {
        let mut steps = Vec::new();
        let mut start = 0;
        for &position in positions.iter().chain([body.len()].iter()) {
            steps.push(Step::Data(body[start..position].to_vec()));
            start = position;
        }
        Self::new(steps)
    }

    pub fn with_length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }
}

impl ReadableBody for TestBody {
    type Data = Vec<u8>;
    type Error = BodyError;

    fn length(&self) -> Option<u64> {
        self.length
    }

    fn close(&mut self, _: Option<BodyError>) {
        self.closed = true;
    }

    async fn read(&mut self, cancellation: &Cancellation) -> Result<BodyFrame<Vec<u8>>, BodyError> {
        self.reads += 1;
        cancellation.check()?;
        Ok(match self.steps.pop_front() {
            None => BodyFrame::End,
            Some(Step::Data(data)) => BodyFrame::Data { data, end: false },
            Some(Step::Last(data)) => BodyFrame::Data { data, end: true },
            Some(Step::Trailers) => BodyFrame::Headers {
                fields: Fields::new(),
                end: true,
            },
            Some(Step::Fail) => return Err(BodyError::Abandoned),
            Some(Step::CancelThen(token, data)) => {
                token.cancel();
                BodyFrame::Data { data, end: false }
            }
            Some(Step::Cancel(token)) => {
                token.cancel();
                return Err(BodyError::Cancelled);
            }
        })
    }
}

/// A POST request with this `Content-Type` and body.
pub fn request(content_type: &str, body: TestBody) -> Request<TestBody> {
    let mut request = Request::new(Method::POST, "/", body);
    request.fields.add("Content-Type", content_type).unwrap();
    request
}

/// A multipart body, with boundary `b`, of these parts: `(name, Some(filename), content)` for a file.
pub fn multipart(parts: &[(&str, Option<&str>, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, filename, content) in parts {
        body.extend_from_slice(b"--b\r\nContent-Disposition: form-data; name=\"");
        body.extend_from_slice(name.as_bytes());
        body.push(b'"');
        if let Some(filename) = filename {
            body.extend_from_slice(b"; filename=\"");
            body.extend_from_slice(filename.as_bytes());
            body.extend_from_slice(b"\"\r\nContent-Type: text/plain");
        }
        body.extend_from_slice(b"\r\n\r\n");
        body.extend_from_slice(content);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--b--\r\n");
    body
}

pub const MULTIPART: &str = "multipart/form-data; boundary=b";

/// A store operation that fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fail {
    Create,
    Write,
    Finish,
    Abort,
    Remove,
}

/// What happened to one upload in a [`MemoryStore`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stored {
    pub path: String,
    pub content: Vec<u8>,
    pub finished: bool,
    pub aborted: bool,
    pub removed: bool,
}

/// Uploads in memory, by index, with one operation that fails.
#[derive(Debug, Default)]
pub struct MemoryStore {
    pub uploads: Vec<Stored>,
    pub fail: Option<Fail>,
    /// The number of `write` calls.
    pub writes: usize,
}

impl MemoryStore {
    pub fn failing(fail: Fail) -> Self {
        Self {
            fail: Some(fail),
            ..Self::default()
        }
    }

    /// Uploads neither aborted nor removed.
    pub fn live(&self) -> Vec<&Stored> {
        self.uploads
            .iter()
            .filter(|stored| !stored.aborted && !stored.removed)
            .collect()
    }

    fn check(&self, operation: Fail) -> io::Result<()> {
        if self.fail == Some(operation) {
            return Err(io::Error::other(format!("{operation:?} failed")));
        }
        Ok(())
    }
}

impl UploadStore for MemoryStore {
    type Handle = usize;
    type Upload = usize;
    type Error = io::Error;

    async fn create(
        &mut self,
        head: &UploadHead,
        cancellation: &Cancellation,
    ) -> io::Result<usize> {
        cancellation.check().map_err(io::Error::other)?;
        self.check(Fail::Create)?;
        self.uploads.push(Stored {
            path: head.path().to_string(),
            ..Stored::default()
        });
        Ok(self.uploads.len() - 1)
    }

    async fn write(
        &mut self,
        upload: &mut usize,
        content: &[u8],
        cancellation: &Cancellation,
    ) -> io::Result<()> {
        cancellation.check().map_err(io::Error::other)?;
        self.writes += 1;
        self.uploads[*upload].content.extend_from_slice(content);
        self.check(Fail::Write)
    }

    async fn finish(
        &mut self,
        upload: &mut usize,
        cancellation: &Cancellation,
    ) -> io::Result<usize> {
        cancellation.check().map_err(io::Error::other)?;
        self.check(Fail::Finish)?;
        self.uploads[*upload].finished = true;
        Ok(*upload)
    }

    async fn abort(&mut self, upload: usize) -> io::Result<()> {
        self.check(Fail::Abort)?;
        self.uploads[upload].aborted = true;
        Ok(())
    }

    async fn remove(&mut self, handle: usize) -> io::Result<()> {
        self.check(Fail::Remove)?;
        self.uploads[handle].removed = true;
        Ok(())
    }
}
