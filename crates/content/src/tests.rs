// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Tests of `FileStore`, including failures injected into its file operations.

#[path = "../tests/support/mod.rs"]
mod support;

use crate::{
    FileStore, ReadError, ReadErrorKind, UploadStore,
    file_store::faults::{self, Operation},
    read_content,
};
use base_http::body::BodyError;
use base_scheduler::Cancellation;
use base_scheduler_native::NativeScheduler;
use base_web_content::{
    Accepted, Content, ContentEvent, ContentParser, FieldPath, Rejection, Upload, UploadHead,
};
use base_web_media_type::MediaType;
use futures_lite::future::block_on;
use std::{
    fs, io,
    path::{Path, PathBuf},
};
use support::{MULTIPART, Step, TestBody, multipart, request};
use tempfile::TempDir;

// Every field is declared, though not every test reads it.
#[allow(dead_code)]
#[derive(Content, Debug)]
struct Files {
    #[content(accept("text/plain"), size_limit = 300_000)]
    first: Option<Upload<PathBuf>>,
    #[content(accept("text/plain"), count_limit = 4, size_limit = 300_000)]
    more: Option<Vec<Upload<PathBuf>>>,
    #[content(size_limit = 64)]
    title: String,
}

type Result = std::result::Result<Accepted<Files, PathBuf>, ReadError<BodyError, io::Error>>;

struct Fixture {
    scheduler: NativeScheduler,
    directory: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            scheduler: NativeScheduler::new().unwrap(),
            directory: tempfile::tempdir().unwrap(),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    /// Read `body` into files named after their fields, returning the result and the field paths the destination was given.
    fn read(&self, body: TestBody) -> (Result, Vec<String>) {
        self.read_with(body, Cancellation::never(), None)
    }

    fn read_with(
        &self,
        body: TestBody,
        cancellation: Cancellation,
        permissions: Option<u32>,
    ) -> (Result, Vec<String>) {
        let mut paths = Vec::new();
        let mut request = request(MULTIPART, body);
        let result = {
            let mut store = FileStore::new(&self.scheduler, |path: &FieldPath| {
                paths.push(path.to_string());
                self.path(&path.to_string())
            });
            #[cfg(unix)]
            if let Some(permissions) = permissions {
                store = store.with_permissions(permissions);
            }
            #[cfg(not(unix))]
            let _ = permissions;
            block_on(read_content::<Files, _, _>(
                &mut request,
                &mut store,
                &cancellation,
            ))
        };
        (result, paths)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        block_on(self.scheduler.shutdown());
    }
}

fn store_error(result: Result) -> io::Error {
    match result.expect_err("the read fails").kind {
        ReadErrorKind::Store(error) => error,
        kind => panic!("a store error, not {kind:?}"),
    }
}

fn names(fixture: &Fixture) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(fixture.directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[cfg(unix)]
fn mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

fn large() -> Vec<u8> {
    (0..200_000u32).map(|index| (index % 251) as u8).collect()
}

#[test]
fn saves_uploads_in_new_files_named_by_the_application() {
    let fixture = Fixture::new();
    let large = large();
    let body = multipart(&[
        ("title", None, b"Files"),
        ("first", Some("../../../etc/passwd"), &large),
        ("more[]", Some("a.txt"), b"small"),
        ("more[]", Some("b.txt"), b""),
    ]);
    let positions: Vec<usize> = (1..body.len()).step_by(1000).collect();
    let (result, paths) = fixture.read(TestBody::split(&body, &positions));
    let files = result.expect("the files are accepted").content;
    assert_eq!(paths, ["first", "more[0]", "more[1]"]);
    assert_eq!(names(&fixture), ["first", "more[0]", "more[1]"]);
    assert_eq!(files.first.unwrap().handle(), &fixture.path("first"));
    assert_eq!(fs::read(fixture.path("first")).unwrap(), large);
    assert_eq!(fs::read(fixture.path("more[0]")).unwrap(), b"small");
    assert_eq!(fs::read(fixture.path("more[1]")).unwrap(), b"");
    #[cfg(unix)]
    for name in ["first", "more[0]", "more[1]"] {
        assert_eq!(mode(&fixture.path(name)), 0o600);
    }
}

#[test]
fn saves_an_upload_of_exactly_one_write() {
    let fixture = Fixture::new();
    let content = vec![b'x'; 64 * 1024];
    let body = multipart(&[
        ("title", None, b"Files"),
        ("first", Some("a.txt"), &content),
    ]);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    result.expect("the files are accepted");
    assert_eq!(fs::read(fixture.path("first")).unwrap(), content);
}

#[cfg(unix)]
#[test]
fn creates_files_with_the_permissions_the_application_chooses() {
    let fixture = Fixture::new();
    let body = multipart(&[("title", None, b"Files"), ("first", Some("a.txt"), b"x")]);
    let (result, _) = fixture.read_with(
        TestBody::split(&body, &[]),
        Cancellation::never(),
        // The setuid bit is never set.
        Some(0o4640),
    );
    result.expect("the files are accepted");
    assert_eq!(mode(&fixture.path("first")), 0o640);
}

#[test]
fn refuses_a_destination_that_exists_without_changing_it() {
    let fixture = Fixture::new();
    fs::write(fixture.path("first"), b"original").unwrap();
    let body = multipart(&[
        ("more[]", Some("a.txt"), b"stored"),
        ("first", Some("b.txt"), b"replacement"),
        ("title", None, b"Files"),
    ]);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    assert_eq!(store_error(result).kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read(fixture.path("first")).unwrap(), b"original");
    // The upload stored before it was removed.
    assert_eq!(names(&fixture), ["first"]);
}

#[cfg(unix)]
#[test]
fn refuses_a_symbolic_link_at_the_destination() {
    let fixture = Fixture::new();
    let target = tempfile::tempdir().unwrap();
    let outside = target.path().join("outside");
    fs::write(&outside, b"outside").unwrap();
    std::os::unix::fs::symlink(&outside, fixture.path("first")).unwrap();
    let body = multipart(&[("first", Some("a.txt"), b"replacement")]);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    assert_eq!(store_error(result).kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read(&outside).unwrap(), b"outside");
    assert!(
        fs::symlink_metadata(fixture.path("first"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn fails_when_the_destination_cannot_be_created() {
    let fixture = Fixture::new();
    let body = multipart(&[("first", Some("a.txt"), b"content")]);
    fs::remove_dir_all(fixture.directory.path()).unwrap();
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    assert_eq!(store_error(result).kind(), io::ErrorKind::NotFound);
}

fn assert_removed_after(operation: Operation, content: &[u8]) {
    let fixture = Fixture::new();
    faults::inject(&fixture.path("first"), operation);
    let body = multipart(&[
        ("more[]", Some("a.txt"), b"stored"),
        ("first", Some("b.txt"), content),
    ]);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    let error = store_error(result);
    assert_eq!(
        names(&fixture),
        Vec::<String>::new(),
        "{operation:?}: {error}"
    );
}

#[test]
fn removes_the_file_when_a_write_fails() {
    assert_removed_after(Operation::Write, &large());
}

#[test]
fn removes_the_file_when_the_final_write_fails() {
    assert_removed_after(Operation::Write, b"small");
}

#[test]
fn removes_the_file_when_writing_panics() {
    let fixture = Fixture::new();
    faults::inject(&fixture.path("first"), Operation::Panic);
    let body = multipart(&[("first", Some("a.txt"), b"content")]);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    assert_eq!(store_error(result).to_string(), "task panicked");
    assert_eq!(names(&fixture), Vec::<String>::new());
}

#[test]
fn removes_the_partial_file_when_the_content_is_refused() {
    let fixture = Fixture::new();
    let mut body = multipart(&[
        ("more[]", Some("a.txt"), b"stored"),
        ("first", Some("b.txt"), &large()),
    ]);
    body.truncate(body.len() - 100);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    assert!(matches!(
        result.expect_err("the body is incomplete").kind,
        ReadErrorKind::Rejected(Rejection::Error(_))
    ));
    assert_eq!(names(&fixture), Vec::<String>::new());
}

#[test]
fn removes_stored_files_when_fields_are_violations() {
    let fixture = Fixture::new();
    let body = multipart(&[
        ("first", Some("a.txt"), b"one"),
        ("more[]", Some("b.txt"), b"two"),
    ]);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    assert!(matches!(
        result.expect_err("the title is missing").kind,
        ReadErrorKind::Rejected(Rejection::Violations(_))
    ));
    assert_eq!(names(&fixture), Vec::<String>::new());
}

#[test]
fn reports_a_stored_file_it_cannot_remove() {
    let fixture = Fixture::new();
    faults::inject(&fixture.path("first"), Operation::Remove);
    let body = multipart(&[("first", Some("a.txt"), b"one")]);
    let (result, _) = fixture.read(TestBody::split(&body, &[]));
    let error = result.expect_err("the title is missing");
    assert!(matches!(error.kind, ReadErrorKind::Rejected(_)));
    assert_eq!(error.cleanup.len(), 1);
    assert_eq!(error.cleanup[0].to_string(), "injected failure");
    assert_eq!(names(&fixture), ["first"]);
}

#[test]
fn reports_a_partial_file_it_cannot_remove() {
    let fixture = Fixture::new();
    faults::inject(&fixture.path("first"), Operation::Remove);
    let body = multipart(&[("first", Some("a.txt"), b"content")]);
    let (result, _) = fixture.read(TestBody::new([
        Step::Data(body[..body.len() - 15].to_vec()),
        Step::Fail,
    ]));
    let error = result.expect_err("the body fails");
    assert!(matches!(error.kind, ReadErrorKind::Body(_)));
    assert_eq!(error.cleanup.len(), 1);
}

/// The position just after the header section of the part for `first`.
fn content_of_first(body: &[u8]) -> usize {
    let start = body
        .windows(12)
        .position(|window| window == b"name=\"first\"")
        .unwrap();
    start
        + body[start..]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
        + 4
}

#[test]
fn removes_the_file_when_finishing_is_cancelled() {
    for content in [&b"content"[..], b""] {
        let fixture = Fixture::new();
        let body = multipart(&[("first", Some("a.txt"), content)]);
        // The upload begins, and its content may arrive, before the read is cancelled.
        let position = content_of_first(&body) + content.len() / 2;
        let cancellation = Cancellation::new();
        let steps = [
            Step::Data(body[..position].to_vec()),
            Step::CancelThen(cancellation.clone(), body[position..].to_vec()),
        ];
        let (result, _) = fixture.read_with(TestBody::new(steps), cancellation, None);
        store_error(result);
        assert_eq!(names(&fixture), Vec::<String>::new());
    }
}

/// The head of an upload for `first`, as the parser reports it.
fn head() -> UploadHead {
    let media_type = MediaType::parse(MULTIPART).unwrap();
    let mut parser = ContentParser::<Files, PathBuf>::new(Some(&media_type)).unwrap();
    let body = multipart(&[("first", Some("secret.txt"), b"")]);
    match parser.parse(&body).unwrap().event {
        ContentEvent::Upload(head) => head,
        event => panic!("an upload, not {event:?}"),
    }
}

#[test]
fn uploads_show_their_path_but_not_what_the_client_declared() {
    let fixture = Fixture::new();
    let mut store = FileStore::new(&fixture.scheduler, |_: &FieldPath| fixture.path("upload"));
    assert_eq!(format!("{store:?}"), "FileStore { .. }");
    let never = Cancellation::never();
    let mut upload = block_on(store.create(&head(), &never)).unwrap();
    assert_eq!(upload.path(), fixture.path("upload"));
    block_on(store.write(&mut upload, b"abc", &never)).unwrap();
    let debug = format!("{upload:?}");
    assert!(debug.contains("pending: 3"), "{debug}");
    assert!(!debug.contains("secret"), "{debug}");
    block_on(store.abort(upload)).unwrap();
    assert_eq!(names(&fixture), Vec::<String>::new());
}

#[test]
fn removing_a_file_that_is_gone_succeeds() {
    let fixture = Fixture::new();
    let mut store = FileStore::new(&fixture.scheduler, |_: &FieldPath| fixture.path("upload"));
    block_on(store.remove(fixture.path("missing"))).unwrap();
    // Anything else that prevents removal is reported.
    fs::create_dir(fixture.path("directory")).unwrap();
    block_on(store.remove(fixture.path("directory"))).unwrap_err();
}

#[test]
fn refuses_to_write_an_upload_that_failed_or_finished() {
    let fixture = Fixture::new();
    let mut store = FileStore::new(&fixture.scheduler, |_: &FieldPath| fixture.path("upload"));
    let never = Cancellation::never();
    let mut upload = block_on(store.create(&head(), &never)).unwrap();
    faults::inject(&fixture.path("upload"), Operation::Write);
    block_on(store.write(&mut upload, &large(), &never)).unwrap_err();
    let error = block_on(store.write(&mut upload, &large(), &never)).unwrap_err();
    assert_eq!(
        error.to_string(),
        "the upload's file is closed, after it failed or finished"
    );
    block_on(store.abort(upload)).unwrap();
    assert_eq!(names(&fixture), Vec::<String>::new());
}
