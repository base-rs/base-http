// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use base_http::{Status, body::BodyError};
use base_http_content::{
    FileStore, NoUploads, NoUploadsError, ReadError, ReadErrorKind, ReadOptions, UploadStore,
    read_content, read_content_with,
};
use base_scheduler::Cancellation;
use base_web_content::{
    Accepted, Content, ContentErrorKind, ContentHandles, FieldPath, ParameterRules, Rejection,
    Upload,
};
use futures_lite::future::block_on;
use std::{error::Error as _, io, path::PathBuf};
use support::{Fail, MULTIPART, MemoryStore, Step, TestBody, multipart, request};

#[derive(Content, Debug)]
struct Post {
    #[content(size_limit = 64)]
    title: String,
    #[content(accept("text/*"), size_limit = 32)]
    attachment: Option<Upload<usize>>,
    #[content(accept("text/*"), count_limit = 4, size_limit = 32)]
    files: Option<Vec<Upload<usize>>>,
}

#[derive(Content, Debug)]
struct Search {
    #[content(size_limit = 16)]
    q: String,
    page: Option<u32>,
}

type Result<T, S = MemoryStore> =
    std::result::Result<Accepted<T, <S as UploadStore>::Handle>, ReadError<BodyError, io::Error>>;

fn read<T: ContentHandles<usize>>(
    content_type: &str,
    body: TestBody,
    store: &mut MemoryStore,
) -> (Result<T>, TestBody) {
    let mut request = request(content_type, body);
    let result = block_on(read_content::<T, _, _>(
        &mut request,
        store,
        &Cancellation::never(),
    ));
    (result, request.body)
}

fn error<T: std::fmt::Debug>(result: Result<T>) -> ReadError<BodyError, io::Error> {
    result.expect_err("the read fails")
}

/// A position within the content of the upload `second`.
fn within_second(body: &[u8]) -> usize {
    body.windows(6)
        .position(|window| window == b"second")
        .unwrap()
        + 3
}

fn post_body() -> Vec<u8> {
    multipart(&[
        ("title", None, b"Hello"),
        ("attachment", Some("notes.txt"), b"Some notes."),
        ("files[]", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), b""),
        ("files[]", Some("c.txt"), b"third"),
    ])
}

fn check_post(result: Result<Post>, store: &MemoryStore) {
    let post = result.expect("the post is accepted").content;
    assert_eq!(post.title, "Hello");
    let attachment = post.attachment.expect("an attachment");
    assert_eq!(attachment.untrusted_filename(), "notes.txt");
    assert_eq!(store.uploads[*attachment.handle()].content, b"Some notes.");
    let files: Vec<&[u8]> = post
        .files
        .expect("files")
        .iter()
        .map(|file| &store.uploads[*file.handle()].content[..])
        .collect();
    assert_eq!(files, [&b"first"[..], b"", b"third"]);
    let paths: Vec<&str> = store
        .uploads
        .iter()
        .map(|stored| &stored.path[..])
        .collect();
    assert_eq!(paths, ["attachment", "files[0]", "files[1]", "files[2]"]);
    assert!(store.uploads.iter().all(|stored| stored.finished));
    assert_eq!(store.live().len(), 4);
}

#[test]
fn reads_multipart_content_and_streams_uploads() {
    let mut store = MemoryStore::default();
    let (result, body) = read::<Post>(MULTIPART, TestBody::split(&post_body(), &[]), &mut store);
    check_post(result, &store);
    assert!(!body.closed);
}

#[test]
fn reads_multipart_content_divided_at_every_position() {
    let body = post_body();
    for position in 0..=body.len() {
        let mut store = MemoryStore::default();
        let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &[position]), &mut store);
        check_post(result, &store);
    }
}

#[test]
fn reads_multipart_content_one_byte_at_a_time() {
    let body = post_body();
    let positions: Vec<usize> = (1..body.len()).collect();
    let mut store = MemoryStore::default();
    let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &positions), &mut store);
    check_post(result, &store);
    // Upload content is streamed as it arrives, rather than gathered.
    assert!(store.writes > 3);
}

#[test]
fn reads_content_whose_last_frame_ends_the_body() {
    let body = post_body();
    let (first, last) = body.split_at(40);
    let mut store = MemoryStore::default();
    let steps = [Step::Data(first.to_vec()), Step::Last(last.to_vec())];
    let (result, body) = read::<Post>(MULTIPART, TestBody::new(steps), &mut store);
    check_post(result, &store);
    // The body's end came with its last data, so it was not read again.
    assert_eq!(body.reads, 2);
}

#[test]
fn ignores_trailers() {
    let mut store = MemoryStore::default();
    let steps = [Step::Data(post_body()), Step::Trailers];
    let (result, _) = read::<Post>(MULTIPART, TestBody::new(steps), &mut store);
    check_post(result, &store);
}

#[test]
fn reads_url_encoded_content_divided_at_every_position() {
    let body = b"q=caf%C3%A9&page=2";
    for position in 0..=body.len() {
        let mut store = MemoryStore::default();
        let content_type = "application/x-www-form-urlencoded";
        let (result, _) =
            read::<Search>(content_type, TestBody::split(body, &[position]), &mut store);
        let search = result.expect("the search is accepted").content;
        assert_eq!((search.q.as_str(), search.page), ("café", Some(2)));
    }
}

#[test]
fn reads_url_encoded_content_one_byte_at_a_time() {
    let body = b"q=caf%C3%A9&page=2";
    let positions: Vec<usize> = (1..body.len()).collect();
    let content_type = "application/x-www-form-urlencoded";
    let mut store = MemoryStore::default();
    let (result, _) = read::<Search>(content_type, TestBody::split(body, &positions), &mut store);
    let search = result.expect("the search is accepted").content;
    assert_eq!((search.q.as_str(), search.page), ("café", Some(2)));
}

#[test]
fn reads_a_body_followed_by_an_epilogue_divided_at_every_position() {
    let mut body = post_body();
    body.extend_from_slice(b"\r\nan epilogue");
    for position in 0..=body.len() {
        let mut store = MemoryStore::default();
        let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &[position]), &mut store);
        check_post(result, &store);
    }
}

#[test]
fn refuses_an_epilogue_over_its_limit_divided_at_every_position() {
    let mut body = post_body();
    body.extend_from_slice(&[b'x'; 1100]);
    for position in 0..=body.len() {
        let mut store = MemoryStore::default();
        let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &[position]), &mut store);
        let error = error(result);
        assert!(matches!(
            &error.kind,
            ReadErrorKind::Rejected(Rejection::Error(error)) if error.kind() == ContentErrorKind::EpilogueSizeLimit
        ));
        assert!(store.live().is_empty());
    }
}

#[test]
fn refuses_an_epilogue_over_its_limit() {
    let mut body = post_body();
    body.extend_from_slice(&[b'x'; 2048]);
    let mut store = MemoryStore::default();
    let (result, body) = read::<Post>(MULTIPART, TestBody::split(&body, &[]), &mut store);
    let error = error(result);
    assert!(matches!(
        &error.kind,
        ReadErrorKind::Rejected(Rejection::Error(error)) if error.kind() == ContentErrorKind::EpilogueSizeLimit
    ));
    assert_eq!(error.status(), Status::CONTENT_TOO_LARGE);
    // Every upload was finished before the epilogue, and is removed.
    assert_eq!(store.uploads.len(), 4);
    assert!(store.live().is_empty());
    assert!(body.closed);
}

#[test]
fn accepts_content_when_the_body_fails_in_its_epilogue() {
    let mut store = MemoryStore::default();
    let steps = [Step::Data(post_body()), Step::Fail];
    let (result, body) = read::<Post>(MULTIPART, TestBody::new(steps), &mut store);
    check_post(result, &store);
    assert!(body.closed);
}

/// The request's length, from its body here, as an HTTP/1 body gives its `Content-Length`.
fn read_with_length(length: u64) -> (Result<Search>, TestBody) {
    let body = TestBody::split(b"q=rust", &[]).with_length(length);
    read::<Search>(
        "application/x-www-form-urlencoded",
        body,
        &mut MemoryStore::default(),
    )
}

fn search_limit() -> u64 {
    Search::body_size_limit(base_web_content::ContentFormat::UrlEncoded).unwrap() as u64
}

#[test]
fn reads_a_request_whose_length_is_the_largest_its_declaration_accepts() {
    let (result, body) = read_with_length(search_limit());
    assert_eq!(result.expect("the search is accepted").content.q, "rust");
    assert!(body.reads > 0);
}

#[test]
fn refuses_a_request_longer_than_its_declaration_accepts_before_reading_it() {
    let limit = search_limit();
    let (result, body) = read_with_length(limit + 1);
    let error = error(result);
    assert!(matches!(
        error.kind,
        ReadErrorKind::ContentTooLarge { length, limit: maximum } if length == limit + 1 && maximum == limit
    ));
    assert_eq!(error.status(), Status::CONTENT_TOO_LARGE);
    assert_eq!(
        error.to_string(),
        format!(
            "request length {} exceeds the largest body its declaration accepts, {limit}",
            limit + 1
        )
    );
    assert_eq!(body.reads, 0);
    assert!(body.closed);
}

#[test]
fn refuses_a_declared_content_length_longer_than_its_declaration_accepts() {
    let mut request = request(
        "application/x-www-form-urlencoded",
        TestBody::split(b"q=rust", &[]),
    );
    request.content_length = Some(search_limit() + 1);
    let result = block_on(read_content::<Search, _, _>(
        &mut request,
        &mut MemoryStore::default(),
        &Cancellation::never(),
    ));
    assert!(matches!(
        error(result).kind,
        ReadErrorKind::ContentTooLarge { .. }
    ));
    assert_eq!(request.body.reads, 0);
}

#[test]
fn refuses_a_body_of_unknown_length_as_it_exceeds_its_declaration() {
    let mut body = b"q=".to_vec();
    body.resize(search_limit() as usize + 100, b'a');
    let positions: Vec<usize> = (1..body.len()).step_by(7).collect();
    let (result, body) = read::<Search>(
        "application/x-www-form-urlencoded",
        TestBody::split(&body, &positions),
        &mut MemoryStore::default(),
    );
    let error = error(result);
    assert_eq!(error.status(), Status::CONTENT_TOO_LARGE);
    assert!(matches!(
        error.kind,
        ReadErrorKind::Rejected(Rejection::Error(_))
    ));
    // Reading stopped as soon as the field was over its limit, long before the body's end.
    assert!(body.reads < 10);
    assert!(body.closed);
}

// The content is refused, so the struct is never built.
#[allow(dead_code)]
#[derive(Content, Debug)]
struct Note {
    #[content(size_limit = 8)]
    text: String,
}

#[test]
fn refuses_a_body_of_unknown_length_as_it_exceeds_the_largest_its_declaration_accepts() {
    let limit = Note::body_size_limit(base_web_content::ContentFormat::UrlEncoded).unwrap();
    // Each pair is within the field's limit, and only the last is kept, but together they exceed the body's maximum.
    let body = b"text=abcd&".repeat(20);
    let positions: Vec<usize> = (1..body.len()).collect();
    let (result, body) = read::<Note>(
        "application/x-www-form-urlencoded",
        TestBody::split(&body, &positions),
        &mut MemoryStore::default(),
    );
    let error = error(result);
    assert!(matches!(
        &error.kind,
        ReadErrorKind::Rejected(Rejection::Error(error))
            if error.kind() == ContentErrorKind::BodySizeLimit && error.offset() == limit as u64
    ));
    assert_eq!(error.status(), Status::CONTENT_TOO_LARGE);
    // Reading stopped at the byte over the maximum.
    assert_eq!(body.reads, limit + 1);
    assert!(body.closed);
}

fn read_with_content_types(content_types: &[&[u8]]) -> (Result<Search>, TestBody) {
    let mut request = request("text/plain", TestBody::split(b"q=rust", &[]));
    request.fields.remove("content-type");
    for content_type in content_types {
        request.fields.add("Content-Type", content_type).unwrap();
    }
    let result = block_on(read_content::<Search, _, _>(
        &mut request,
        &mut MemoryStore::default(),
        &Cancellation::never(),
    ));
    (result, request.body)
}

#[test]
fn refuses_a_request_without_a_media_type_before_reading_it() {
    let (result, body) = read_with_content_types(&[]);
    let error = error(result);
    assert!(matches!(
        &error.kind,
        ReadErrorKind::Rejected(Rejection::Error(error)) if error.kind() == ContentErrorKind::MissingMediaType
    ));
    assert_eq!(error.status().as_u16(), 415);
    assert_eq!(body.reads, 0);
    assert!(body.closed);
}

#[test]
fn refuses_an_unsupported_media_type() {
    let (result, _) = read_with_content_types(&[b"application/json"]);
    assert_eq!(error(result).status().as_u16(), 415);
}

#[test]
fn refuses_a_repeated_or_malformed_content_type_before_reading_it() {
    let form = b"application/x-www-form-urlencoded";
    for content_types in [
        &[&form[..], &form[..]][..],
        &[b"not a media type"],
        &[b"text/\xffplain"],
    ] {
        let (result, body) = read_with_content_types(content_types);
        let error = error(result);
        assert!(matches!(error.kind, ReadErrorKind::InvalidMediaType));
        assert_eq!(error.status(), Status::BAD_REQUEST);
        assert_eq!(error.to_string(), "request has an invalid Content-Type");
        assert!(error.source().is_none());
        assert_eq!(body.reads, 0);
        assert!(body.closed);
    }
}

#[test]
fn removes_stored_uploads_and_aborts_the_upload_in_progress_after_a_fatal_error() {
    let mut body = multipart(&[
        ("attachment", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), &[b'x'; 64]),
    ]);
    body.truncate(body.len() - 20);
    let mut store = MemoryStore::default();
    let (result, body) = read::<Post>(MULTIPART, TestBody::split(&body, &[]), &mut store);
    let error = error(result);
    assert!(matches!(
        &error.kind,
        ReadErrorKind::Rejected(Rejection::Error(error)) if error.kind() == ContentErrorKind::FieldSizeLimit
    ));
    assert_eq!(error.status(), Status::CONTENT_TOO_LARGE);
    assert!(error.cleanup.is_empty());
    assert!(error.source().is_none());
    assert!(store.uploads[0].finished && store.uploads[0].removed);
    assert!(!store.uploads[1].finished && store.uploads[1].aborted);
    assert!(body.closed);
}

#[test]
fn removes_stored_uploads_when_the_body_ends_early() {
    let mut body = multipart(&[("attachment", Some("a.txt"), b"first")]);
    body.truncate(body.len() - 4);
    let mut store = MemoryStore::default();
    let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &[]), &mut store);
    let error = error(result);
    assert!(matches!(
        &error.kind,
        ReadErrorKind::Rejected(Rejection::Error(error)) if error.kind() == ContentErrorKind::UnexpectedEnd
    ));
    assert_eq!(error.status(), Status::BAD_REQUEST);
    assert!(store.uploads[0].removed);
}

#[test]
fn removes_stored_uploads_when_fields_are_violations_divided_at_every_position() {
    let body = multipart(&[
        ("attachment", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), b"second"),
    ]);
    for position in 0..=body.len() {
        let mut store = MemoryStore::default();
        let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &[position]), &mut store);
        assert_eq!(error(result).status(), Status::UNPROCESSABLE_CONTENT);
        assert_eq!(store.uploads.len(), 2);
        assert!(store.live().is_empty());
    }
}

#[test]
fn removes_stored_uploads_when_fields_are_violations() {
    let body = multipart(&[("attachment", Some("a.txt"), b"first")]);
    let mut store = MemoryStore::default();
    let (result, body) = read::<Post>(MULTIPART, TestBody::split(&body, &[]), &mut store);
    let error = error(result);
    let ReadErrorKind::Rejected(Rejection::Violations(violations)) = &error.kind else {
        panic!("violations");
    };
    assert_eq!(violations.len(), 1);
    assert_eq!(error.status(), Status::UNPROCESSABLE_CONTENT);
    assert_eq!(
        error.to_string(),
        "content has invalid fields: title: required"
    );
    assert!(store.live().is_empty());
    // The body was read to its end.
    assert!(!body.closed);
}

#[test]
fn returns_the_uploads_later_ones_replaced() {
    let body = multipart(&[
        ("title", None, b"Hello"),
        ("attachment", Some("a.txt"), b"first"),
        ("attachment", Some("b.txt"), b"second"),
    ]);
    let mut store = MemoryStore::default();
    let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &[]), &mut store);
    let accepted = result.expect("the post is accepted");
    assert_eq!(*accepted.content.attachment.unwrap().handle(), 1);
    // The replaced upload belongs to the application, which removes it if it is not wanted.
    assert_eq!(accepted.discarded, [0]);
    assert_eq!(store.live().len(), 2);
}

#[test]
fn aborts_and_removes_uploads_when_the_body_fails() {
    let body = multipart(&[
        ("attachment", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), b"second"),
    ]);
    let steps = [
        Step::Data(body[..within_second(&body)].to_vec()),
        Step::Fail,
    ];
    let mut store = MemoryStore::default();
    let (result, body) = read::<Post>(MULTIPART, TestBody::new(steps), &mut store);
    let error = error(result);
    assert!(matches!(
        error.kind,
        ReadErrorKind::Body(BodyError::Abandoned)
    ));
    assert_eq!(error.status(), Status::BAD_REQUEST);
    assert_eq!(error.to_string(), "failed to read the request body");
    assert!(error.source().unwrap().is::<BodyError>());
    assert!(store.uploads[0].removed);
    assert!(store.uploads[1].aborted);
    assert!(body.closed);
}

#[test]
fn aborts_and_removes_uploads_when_the_read_is_cancelled() {
    let body = multipart(&[
        ("attachment", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), b"second"),
    ]);
    let cancellation = Cancellation::new();
    let steps = [
        Step::Data(body[..within_second(&body)].to_vec()),
        Step::Cancel(cancellation.clone()),
    ];
    let mut store = MemoryStore::default();
    let mut request = request(MULTIPART, TestBody::new(steps));
    let result = block_on(read_content::<Post, _, _>(
        &mut request,
        &mut store,
        &cancellation,
    ));
    assert!(matches!(
        error(result).kind,
        ReadErrorKind::Body(BodyError::Cancelled)
    ));
    assert!(store.uploads[0].removed);
    assert!(store.uploads[1].aborted);
    assert!(request.body.closed);
}

#[test]
fn aborts_and_removes_uploads_when_a_store_operation_is_cancelled() {
    let body = multipart(&[
        ("attachment", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), b"second"),
    ]);
    let cancellation = Cancellation::new();
    let steps = [
        Step::Data(body[..within_second(&body)].to_vec()),
        Step::CancelThen(cancellation.clone(), body[within_second(&body)..].to_vec()),
    ];
    let mut store = MemoryStore::default();
    let mut request = request(MULTIPART, TestBody::new(steps));
    let result = block_on(read_content::<Post, _, _>(
        &mut request,
        &mut store,
        &cancellation,
    ));
    let error = error(result);
    assert!(matches!(error.kind, ReadErrorKind::Store(_)));
    assert!(store.uploads[0].removed);
    assert!(store.uploads[1].aborted);
}

#[test]
fn removes_stored_uploads_when_storing_fails() {
    for fail in [Fail::Create, Fail::Write, Fail::Finish] {
        let body = multipart(&[
            ("title", None, b"Hello"),
            ("attachment", Some("a.txt"), b"first"),
        ]);
        let mut store = MemoryStore::failing(fail);
        let (result, body) = read::<Post>(MULTIPART, TestBody::split(&body, &[]), &mut store);
        let error = error(result);
        assert!(matches!(error.kind, ReadErrorKind::Store(_)), "{fail:?}");
        assert_eq!(error.status(), Status::INTERNAL_SERVER_ERROR);
        assert_eq!(error.to_string(), "failed to store an upload");
        assert_eq!(
            error.source().unwrap().to_string(),
            format!("{fail:?} failed")
        );
        // A failed create stores nothing; a failed write or finish is aborted.
        assert!(store.live().is_empty(), "{fail:?}");
        assert_eq!(store.uploads.len(), usize::from(fail != Fail::Create));
        assert!(body.closed);
    }
}

#[test]
fn reports_failures_to_clean_up_with_the_cause() {
    let body = multipart(&[
        ("attachment", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), b"second"),
    ]);
    for fail in [Fail::Abort, Fail::Remove] {
        let steps = [
            Step::Data(body[..within_second(&body)].to_vec()),
            Step::Fail,
        ];
        let mut store = MemoryStore::failing(fail);
        let (result, _) = read::<Post>(MULTIPART, TestBody::new(steps), &mut store);
        let error = error(result);
        assert!(matches!(error.kind, ReadErrorKind::Body(_)));
        assert_eq!(error.cleanup.len(), 1);
        assert_eq!(error.cleanup[0].to_string(), format!("{fail:?} failed"));
        assert_eq!(
            error.to_string(),
            "failed to read the request body, and 1 failure to remove a stored upload"
        );
    }
}

#[test]
fn reports_each_failure_to_clean_up() {
    let body = multipart(&[
        ("attachment", Some("a.txt"), b"first"),
        ("files[]", Some("b.txt"), b"second"),
    ]);
    let mut store = MemoryStore::failing(Fail::Remove);
    let (result, _) = read::<Post>(MULTIPART, TestBody::split(&body, &[]), &mut store);
    let error = error(result);
    assert_eq!(error.cleanup.len(), 2);
    assert_eq!(
        error.to_string(),
        "content has invalid fields: title: required, and 2 failures to remove stored uploads"
    );
}

#[test]
fn the_read_can_be_sent_between_threads() {
    fn assert_send<F: std::future::Future + Send>(_: F) {}
    let scheduler = base_scheduler_native::NativeScheduler::new().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut request = request(MULTIPART, TestBody::split(b"", &[]));
    let mut store = FileStore::new(&scheduler, |_: &FieldPath| directory.path().join("upload"));
    let cancellation = Cancellation::never();
    assert_send(read_content::<Document, _, _>(
        &mut request,
        &mut store,
        &cancellation,
    ));
    block_on(scheduler.shutdown());
}

#[test]
fn reads_with_parameter_rules() {
    let body = b"--b\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nHello\r\n--b\r\nContent-Disposition: form-data; name=\"attachment\"; filename*=UTF-8''notes.txt\r\nContent-Type: text/plain\r\n\r\nnotes\r\n--b--\r\n";
    let refused = read::<Post>(
        MULTIPART,
        TestBody::split(body, &[]),
        &mut MemoryStore::default(),
    )
    .0;
    assert!(matches!(
        &error(refused).kind,
        ReadErrorKind::Rejected(Rejection::Error(error)) if error.kind() == ContentErrorKind::InvalidPartHeader
    ));

    let mut request = request(MULTIPART, TestBody::split(body, &[]));
    let mut options = ReadOptions::default();
    options.parameter_rules = Some(ParameterRules::Mime);
    let mut store = MemoryStore::default();
    let result = block_on(read_content_with::<Post, _, _>(
        &mut request,
        &mut store,
        &options,
        &Cancellation::never(),
    ));
    let post = result.expect("the post is accepted").content;
    assert_eq!(post.attachment.unwrap().untrusted_filename(), "notes.txt");
}

#[test]
fn reads_a_declaration_without_uploads_with_no_store() {
    let mut request = request(
        "application/x-www-form-urlencoded",
        TestBody::split(b"q=rust", &[]),
    );
    let result = block_on(read_content::<Search, _, _>(
        &mut request,
        &mut NoUploads,
        &Cancellation::never(),
    ));
    assert_eq!(result.expect("the search is accepted").content.q, "rust");
}

// The upload is refused, so the struct is never built.
#[allow(dead_code)]
#[derive(Content)]
struct Avatar {
    avatar: Option<Upload<()>>,
}

#[test]
fn refuses_an_upload_with_no_store() {
    let body = multipart(&[("avatar", Some("a.txt"), b"first")]);
    let mut request = request(MULTIPART, TestBody::split(&body, &[]));
    let result = block_on(read_content::<Avatar, _, _>(
        &mut request,
        &mut NoUploads,
        &Cancellation::never(),
    ));
    let error = result.expect_err("the upload is refused");
    assert!(matches!(error.kind, ReadErrorKind::Store(NoUploadsError)));
    assert_eq!(
        error.source().unwrap().to_string(),
        "no store was given for uploads"
    );
}

#[test]
fn no_store_refuses_every_upload_operation() {
    let never = Cancellation::never();
    let mut store = NoUploads;
    block_on(async {
        assert_eq!(
            store.write(&mut (), b"x", &never).await,
            Err(NoUploadsError)
        );
        assert_eq!(store.finish(&mut (), &never).await, Err(NoUploadsError));
        assert_eq!(store.abort(()).await, Ok(()));
        assert_eq!(store.remove(()).await, Ok(()));
    });
}

#[test]
fn errors_do_not_show_submitted_content() {
    let body = multipart(&[("attachment", Some("secret-name.txt"), b"secret-content")]);
    let (result, _) = read::<Post>(
        MULTIPART,
        TestBody::split(&body, &[]),
        &mut MemoryStore::default(),
    );
    let error = error(result);
    for text in [error.to_string(), format!("{error:?}")] {
        assert!(!text.contains("secret"), "{text}");
    }
}

// Every field is declared, though the test reads only the upload.
#[allow(dead_code)]
#[derive(Content, Debug)]
struct Document {
    #[content(size_limit = 256)]
    title: String,
    #[content(size_limit = 200_000)]
    file: Upload<PathBuf>,
}

#[test]
fn saves_uploads_to_files() {
    let scheduler = base_scheduler_native::NativeScheduler::new().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let content: Vec<u8> = (0..150_000u32).map(|index| index as u8).collect();
    let body = multipart(&[
        ("title", None, b"Report"),
        ("file", Some("../../etc/passwd"), &content),
    ]);
    let positions: Vec<usize> = (1..body.len()).step_by(4096).collect();
    let mut request = request(MULTIPART, TestBody::split(&body, &positions));
    let mut paths = Vec::new();
    let result = {
        let mut store = FileStore::new(&scheduler, |path: &FieldPath| {
            paths.push(path.to_string());
            directory.path().join("upload")
        });
        block_on(read_content::<Document, _, _>(
            &mut request,
            &mut store,
            &Cancellation::never(),
        ))
    };
    let document = result.expect("the document is accepted").content;
    assert_eq!(document.file.handle(), &directory.path().join("upload"));
    assert_eq!(std::fs::read(document.file.handle()).unwrap(), content);
    assert_eq!(paths, ["file"]);
    block_on(scheduler.shutdown());
}
