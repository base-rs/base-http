// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_http::body::{BufferedBody, EmptyBody, HeadBody, ReadableBody};
use base_scheduler::{Cancellation, Cancelled};
use futures_lite::future::block_on;
use std::sync::Arc;

#[test]
fn buffered_length_survives_consumption_close_and_rewind() {
    let mut body = BufferedBody::new([b"hello".to_vec(), b"world".to_vec()]);
    assert_eq!(body.length(), Some(10));
    assert!(body.is_ready());
    assert!(!body.is_empty());
    assert_eq!(body.read_chunk(), Some(b"hello".to_vec()));
    assert_eq!(body.length(), Some(10));
    body.close(None);
    assert!(body.is_empty());
    assert_eq!(body.read_chunk(), None);
    assert!(body.is_rewindable());
    assert!(body.rewind());
    assert_eq!(body.read_chunk(), Some(b"hello".to_vec()));
    assert_eq!(body.read_chunk(), Some(b"world".to_vec()));
    assert_eq!(body.read_chunk(), None);
    assert_eq!(body.length(), Some(10));
    assert!(body.is_ready());
}

#[test]
fn empty_chunks_are_data_even_with_zero_representation_length() {
    let mut body = BufferedBody::new([Vec::<u8>::new()]);
    assert_eq!(body.length(), Some(0));
    assert!(!body.is_empty());
    assert_eq!(body.read_chunk(), Some(Vec::new()));
    assert!(body.is_empty());
    assert_eq!(body.read_chunk(), None);
}

#[test]
fn returned_owned_chunks_do_not_mutate_the_replay_source() {
    let mut body = BufferedBody::from(b"hello".to_vec());
    let mut chunk = body.read_chunk().unwrap();
    chunk.fill(b'x');
    body.rewind();
    assert_eq!(body.read_chunk(), Some(b"hello".to_vec()));
}

#[test]
fn immutable_shared_chunks_do_not_require_copying_payloads() {
    let original: Arc<[u8]> = Arc::from(&b"hello"[..]);
    let mut body = BufferedBody::from(Arc::clone(&original));
    let returned = body.read_chunk().unwrap();
    assert!(Arc::ptr_eq(&original, &returned));
}

#[test]
fn buffered_representation_has_an_independent_cursor() {
    let mut body = BufferedBody::new([&b"one"[..], &b"two"[..]]);
    assert_eq!(body.read_chunk(), Some(&b"one"[..]));
    let mut copy = body.buffered();
    assert_eq!(copy.read_chunk(), Some(&b"one"[..]));
    assert_eq!(body.read_chunk(), Some(&b"two"[..]));
}

#[test]
fn append_and_clear_keep_length_and_cursor_consistent() {
    let mut body = BufferedBody::from(b"one".to_vec());
    assert_eq!(body.length(), Some(3));
    body.close(None);
    body.write(b"two".to_vec());
    assert!(!body.is_empty());
    assert_eq!(body.length(), Some(6));
    assert_eq!(body.read_chunk(), Some(b"two".to_vec()));
    body.clear();
    assert_eq!(body.length(), Some(0));
    assert!(body.is_empty());
    assert!(body.chunks().is_empty());
    assert!(body.rewind());
    assert_eq!(body.read_chunk(), None);
}

#[test]
fn unpolled_and_precancelled_reads_leave_the_body_unchanged() {
    let mut body = BufferedBody::from(b"hello".to_vec());
    let cancelled = Cancellation::new();
    cancelled.cancel();
    drop(body.read(&Cancellation::never()));
    assert!(!body.is_empty());
    assert_eq!(block_on(body.read(&cancelled)), Err(Cancelled));
    assert_eq!(
        block_on(body.read(&Cancellation::never())),
        Ok(Some(b"hello".to_vec()))
    );
}

#[test]
fn memory_body_reads_need_only_cancellation_and_return_send_futures() {
    fn require_send<T: Send>(future: T) -> T {
        future
    }
    let mut body = BufferedBody::from(&b"hello"[..]);
    let cancellation = Cancellation::never();
    let future = require_send(body.read(&cancellation));
    assert_eq!(block_on(future), Ok(Some(&b"hello"[..])));
    let future = require_send(body.read(&cancellation));
    assert_eq!(block_on(future), Ok(None));
}

#[test]
fn head_captures_total_length_and_closes_its_source() {
    let mut source = BufferedBody::new([b"hello".to_vec(), b"world".to_vec()]);
    source.read_chunk();
    let mut head = HeadBody::for_body(&mut source);
    assert!(source.is_empty());
    assert_eq!(head.length(), Some(10));
    assert!(head.is_ready());
    assert!(head.is_empty());
    assert_eq!(block_on(head.read(&Cancellation::never())), Ok(None));
    assert_eq!(head.length(), Some(10));
}

#[test]
fn empty_and_unknown_length_head_remain_distinct() {
    let mut empty = EmptyBody;
    assert_eq!(empty.length(), Some(0));
    assert!(empty.is_ready());
    assert!(empty.is_empty());
    assert_eq!(block_on(empty.read(&Cancellation::never())), Ok(None));
    let mut head = HeadBody::new(None);
    assert_eq!(head.length(), None);
    assert!(head.is_ready());
    assert!(head.is_empty());
    assert_eq!(block_on(head.read(&Cancellation::never())), Ok(None));
}

#[test]
fn buffered_storage_transitions_preserve_slices_replay_and_append_after_eof() {
    let mut body = BufferedBody::<&[u8]>::default();
    assert_eq!(body.chunks(), &[] as &[&[u8]]);
    body.write(b"one");
    assert_eq!(body.chunks(), &[&b"one"[..]]);
    assert_eq!(body.read_chunk(), Some(&b"one"[..]));
    body.write(b"two");
    body.write(b"three");
    assert_eq!(body.chunks(), &[&b"one"[..], &b"two"[..], &b"three"[..]]);
    assert_eq!(body.length(), Some(11));
    assert_eq!(body.read_chunk(), Some(&b"two"[..]));
    let mut replay = body.buffered();
    body.clear();
    assert!(body.chunks().is_empty());
    body.write(b"");
    assert_eq!(body.read_chunk(), Some(&b""[..]));
    assert_eq!(replay.read_chunk(), Some(&b"one"[..]));
    assert_eq!(BufferedBody::<&[u8]>::new([]).chunks(), &[] as &[&[u8]]);
}
