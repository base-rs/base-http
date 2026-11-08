// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Reading content in a `base-http-server` handler, over real connections.

use base_http::{
    InterimHandler, InterimResponses, Request, Response, Status,
    body::{BufferedBody, ReadableBody},
};
use base_http_content::{FileStore, read_content};
use base_http_fixtures::{RawPeer, ServerContext};
use base_http_server::Handler;
use base_scheduler::Cancellation;
use base_scheduler_native::NativeScheduler;
use base_web_content::{Content, ContentFormat, FieldPath, Upload};
use std::{
    convert::Infallible,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Content)]
struct Profile {
    #[content(size_limit = 64)]
    name: String,
    #[content(accept("text/plain"), size_limit = 1024)]
    avatar: Option<Upload<PathBuf>>,
}

/// Responds with the profile's name and the size of its saved avatar, or with the read's status and error.
struct Profiles {
    /// The scheduler whose blocking threads save uploads.
    scheduler: Arc<NativeScheduler>,
    directory: PathBuf,
    /// The number of uploads created.
    next: Arc<AtomicUsize>,
}

impl Handler for Profiles {
    type Error = Infallible;
    type Body<B: ReadableBody> = BufferedBody<Vec<u8>>;

    async fn call<B: ReadableBody, I: InterimHandler>(
        &self,
        mut request: Request<B>,
        _: &mut InterimResponses<I>,
        cancellation: &Cancellation,
    ) -> Result<Response<Self::Body<B>>, Infallible> {
        let mut store = FileStore::new(&*self.scheduler, |_: &FieldPath| {
            let index = self.next.fetch_add(1, Ordering::Relaxed);
            self.directory.join(format!("upload-{index}"))
        });
        let (status, text) =
            match read_content::<Profile, _, _>(&mut request, &mut store, cancellation).await {
                Ok(accepted) => {
                    let profile = accepted.content;
                    let size = profile
                        .avatar
                        .map_or(0, |avatar| fs::read(avatar.handle()).unwrap().len());
                    (Status::OK, format!("{} {size}", profile.name))
                }
                Err(error) => (error.status(), error.to_string()),
            };
        Ok(Response::new(status, BufferedBody::from(text.into_bytes())))
    }
}

const BODY: &[u8] = b"--b\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\nAda\r\n--b\r\nContent-Disposition: form-data; name=\"avatar\"; filename=\"me.txt\"\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b--\r\n";

/// Serve `Profiles`, spawned onto a scheduler's workers so that the handler's future must be `Send`, and run `peer` against it, returning its result, the number of uploads created, and the names of the files left in the upload directory.
fn serve<T: Send + 'static>(
    peer: impl FnOnce(RawPeer) -> T + Send + 'static,
) -> (T, usize, Vec<String>) {
    let scheduler = Arc::new(NativeScheduler::new().unwrap());
    let directory = tempfile::tempdir().unwrap();
    let next = Arc::new(AtomicUsize::new(0));
    let profiles = Profiles {
        scheduler: scheduler.clone(),
        directory: directory.path().to_owned(),
        next: next.clone(),
    };
    let output = ServerContext::new()
        .handler(profiles)
        .run_spawned(async |context| context.raw(peer).await);
    futures_lite::future::block_on(scheduler.shutdown());
    let mut names: Vec<String> = fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    (output, next.load(Ordering::Relaxed), names)
}

fn head(length: &str) -> String {
    format!(
        "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: multipart/form-data; boundary=b\r\n{length}\r\n\r\n"
    )
}

#[test]
fn reads_content_with_a_content_length() {
    let ((status, text), _, names) = serve(|mut peer| {
        peer.write(head(&format!("Content-Length: {}", BODY.len())));
        peer.trickle(BODY);
        let (head, body) = peer.read_message();
        (head.status(), String::from_utf8(body.data).unwrap())
    });
    assert_eq!((status, text.as_str()), (200, "Ada 5"));
    assert_eq!(names, ["upload-0"]);
}

#[test]
fn reads_chunked_content() {
    let ((status, text), _, _) = serve(|mut peer| {
        peer.write(head("Transfer-Encoding: chunked"));
        for chunk in BODY.chunks(17) {
            peer.write(format!("{:x}\r\n", chunk.len()));
            peer.write(chunk);
            peer.write("\r\n");
        }
        peer.write("0\r\n\r\n");
        let (head, body) = peer.read_message();
        (head.status(), String::from_utf8(body.data).unwrap())
    });
    assert_eq!((status, text.as_str()), (200, "Ada 5"));
}

#[test]
fn refuses_a_content_length_over_the_maximum_before_the_body_is_sent() {
    let limit = Profile::body_size_limit(ContentFormat::MultipartFormData).unwrap();
    let (status, created, names) = serve(move |mut peer| {
        // The body is never sent: the response comes first, and the connection closes, since its input was not read.
        peer.write(head(&format!("Content-Length: {}", limit + 1)));
        let (head, _) = peer.read_message();
        peer.expect_closed();
        head.status()
    });
    assert_eq!(status, 413);
    assert_eq!(created, 0);
    assert!(names.is_empty());
}

#[test]
fn refuses_chunked_content_over_a_limit_and_removes_its_uploads() {
    let (status, created, names) = serve(|mut peer| {
        // The body without its closing boundary, then another name, over its limit.
        let mut body = BODY[..BODY.len() - b"--b--\r\n".len()].to_vec();
        body.extend_from_slice(b"--b\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\n");
        body.extend_from_slice(&[b'x'; 100]);
        peer.write(head("Transfer-Encoding: chunked"));
        peer.write(format!("{:x}\r\n", body.len()));
        peer.write(body);
        peer.write("\r\n");
        let (head, _) = peer.read_message();
        head.status()
    });
    assert_eq!(status, 413);
    // The avatar was saved before the name over its limit arrived, and was removed.
    assert_eq!(created, 1);
    assert!(names.is_empty());
}
