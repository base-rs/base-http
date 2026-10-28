// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{IoBuf, IoBufSequence, IoBufSequenceMut, IoBuffers, IoBuffersMut};
use std::io::{IoSlice, IoSliceMut, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};

struct ReadOnly(Vec<u8>);

impl AsRef<[u8]> for ReadOnly {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// SAFETY: This private owner retains its immutable allocation and exposes no mutable aliases.
unsafe impl IoBuf for ReadOnly {}

#[test]
fn prepared_read_vectors_reuse_storage_across_moves_and_partial_advancement() {
    let source = [ReadOnly(b"header".to_vec()), ReadOnly(b"body".to_vec())];
    let pointers = source.each_ref().map(|buffer| buffer.as_ref().as_ptr());
    let mut package = None;
    let setup = allocation_counter::measure(|| package = Some(IoBuffers::new(source)));
    assert_eq!(setup.count_total, 1);
    let buffers = package.unwrap();
    let base = buffers.io_slices().unwrap().as_ptr().cast::<()>();
    // Move the entire package to another polling thread; its prepared array stays put.
    let mut buffers = std::thread::spawn(move || buffers).join().unwrap();
    assert_eq!(buffers.io_slices().unwrap().as_ptr().cast::<()>(), base);
    let mut output = [0; 10];
    let transfers = allocation_counter::measure(|| {
        let mut writer = &mut output[..];
        assert_eq!(
            writer.write_vectored(buffers.io_slices().unwrap()).unwrap(),
            10
        );
        assert_eq!(buffers.buffer_count(), 2);
        assert_eq!(buffers.buffer(0).as_ptr(), pointers[0]);
        buffers.advance(2);
        assert_eq!(buffers.buffer(0), b"ader");
        assert_eq!(buffers.io_slices().unwrap().as_ptr().cast::<()>(), base);
        assert_eq!(buffers.buffer(0).as_ptr(), pointers[0].wrapping_add(2));
        buffers.advance(5);
        assert_eq!(buffers.buffer_count(), 1);
        assert_eq!(buffers.buffer(0), b"ody");
        assert_eq!(
            buffers.io_slices().unwrap().as_ptr().cast::<()>(),
            base.wrapping_byte_add(size_of::<IoSlice<'_>>())
        );
        assert_eq!(buffers.buffer(0).as_ptr(), pointers[1].wrapping_add(1));
        buffers.advance(3);
        assert_eq!(buffers.buffer_count(), 0);
        assert!(buffers.io_slices().unwrap().is_empty());
        buffers.advance(0);
    });
    assert_eq!(transfers.count_total, 0);
    assert_eq!(&output, b"headerbody");
    let source = buffers.into_inner();
    assert_eq!(source[0].as_ref(), b"header");
    assert_eq!(source[1].as_ref(), b"body");
    assert_eq!(
        source.each_ref().map(|buffer| buffer.as_ref().as_ptr()),
        pointers
    );
}

#[test]
fn writable_vectors_allow_repeated_byte_access_and_recover_original_owners() {
    let source = [vec![0; 2], vec![0; 4]]
        .into_iter()
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let pointers = [source[0].as_ptr(), source[1].as_ptr()];
    let mut buffers = IoBuffersMut::new(source);
    let base = buffers.io_slices_mut().unwrap().as_ptr().cast::<()>();
    let mut buffers = std::thread::spawn(move || buffers).join().unwrap();
    assert_eq!(buffers.io_slices_mut().unwrap().as_ptr().cast::<()>(), base);
    let allocations = allocation_counter::measure(|| {
        buffers.buffer_mut(0).copy_from_slice(b"ab");
        assert_eq!(buffers.buffer(0), b"ab");
        buffers.buffer_mut(0)[0] = b'A';
        assert_eq!(&*buffers.io_slices_mut().unwrap()[0], b"Ab");
        #[cfg(any(unix, windows))]
        {
            let vectors = buffers.io_slices().unwrap();
            assert_eq!(&*vectors[0], b"Ab");
            assert_eq!(vectors[0].as_ptr(), pointers[0]);
        }
        buffers.advance(1);
        buffers.buffer_mut(0)[0] = b'B';
        buffers.advance(3);
        assert_eq!(buffers.buffer_count(), 1);
        assert_eq!(buffers.buffer(0).len(), 2);
        assert_eq!(buffers.buffer(0).as_ptr(), pointers[1].wrapping_add(2));
        assert_eq!(
            buffers.io_slices_mut().unwrap().as_ptr().cast::<()>(),
            base.wrapping_byte_add(size_of::<IoSliceMut<'_>>())
        );
        buffers.buffer_mut(0).copy_from_slice(b"cd");
        buffers.advance(2);
        buffers.advance(0);
        assert!(buffers.io_slices_mut().unwrap().is_empty());
    });
    assert_eq!(allocations.count_total, 0);
    let source = buffers.into_inner();
    assert_eq!(&*source[0], b"AB");
    assert_eq!(&*source[1], b"\0\0cd");
    assert_eq!([source[0].as_ptr(), source[1].as_ptr()], pointers);
}

#[test]
fn advancement_handles_empty_ranges_exact_boundaries_and_invalid_counts() {
    let mut read = IoBuffers::new([vec![], vec![1, 2], vec![], vec![3], vec![]]);
    let mut write = IoBuffersMut::new([vec![], vec![1, 2], vec![], vec![3], vec![]]);
    for count in [4, usize::MAX] {
        assert!(catch_unwind(AssertUnwindSafe(|| read.advance(count))).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| write.advance(count))).is_err());
        assert_eq!(read.buffer_count(), 5);
        assert_eq!(write.buffer_count(), 5);
        assert_eq!(read.buffer(1), &[1, 2]);
        assert_eq!(write.buffer(1), &[1, 2]);
    }
    read.advance(0);
    write.advance(0);
    assert_eq!(read.buffer_count(), 4);
    assert_eq!(write.buffer_count(), 4);
    read.advance(2);
    write.advance(2);
    assert_eq!(read.buffer_count(), 2);
    assert_eq!(write.buffer_count(), 2);
    assert_eq!(read.buffer(0), &[3]);
    assert_eq!(write.buffer(0), &[3]);
    assert!(catch_unwind(AssertUnwindSafe(|| read.advance(2))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| write.advance(2))).is_err());
    assert_eq!(read.buffer(0), &[3]);
    assert_eq!(write.buffer(0), &[3]);
    read.advance(1);
    write.advance(1);
    assert_eq!(read.buffer_count(), 0);
    assert_eq!(write.buffer_count(), 0);
    assert!(catch_unwind(AssertUnwindSafe(|| read.advance(1))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| write.advance(1))).is_err());
    let mut read = IoBuffers::new([] as [Vec<u8>; 0]);
    let mut write = IoBuffersMut::new(vec![Vec::<u8>::new()]);
    read.advance(0);
    write.advance(0);
    assert!(read.io_slices().unwrap().is_empty());
    assert!(write.io_slices_mut().unwrap().is_empty());
}

#[test]
fn plain_collections_leave_vector_storage_to_the_backend() {
    let mut source = [vec![1]];
    assert!(source.io_slices().is_none());
    assert!(source.io_slices_mut().is_none());
}

#[cfg(unix)]
#[test]
fn unix_syscalls_use_the_prepared_arrays_directly() {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    let (mut left, mut right) = UnixStream::pair().unwrap();
    let send = IoBuffers::new([b"head".to_vec(), b"body".to_vec()]);
    let vectors = send.io_slices().unwrap();
    assert_eq!(size_of::<IoSlice<'_>>(), size_of::<libc::iovec>());
    assert_eq!(align_of::<IoSlice<'_>>(), align_of::<libc::iovec>());
    // SAFETY: IoSlice arrays are ABI-compatible with iovec. The package keeps every source alive for the synchronous syscall.
    assert_eq!(
        unsafe {
            libc::writev(
                left.as_raw_fd(),
                vectors.as_ptr().cast(),
                vectors.len() as _,
            )
        },
        8
    );
    let mut received = [0; 8];
    right.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"headbody");
    left.write_all(b"response").unwrap();
    let mut receive = IoBuffersMut::new([vec![0; 3], vec![0; 5]]);
    let vectors = receive.io_slices_mut().unwrap();
    assert_eq!(size_of::<IoSliceMut<'_>>(), size_of::<libc::iovec>());
    assert_eq!(align_of::<IoSliceMut<'_>>(), align_of::<libc::iovec>());
    // SAFETY: IoSliceMut has native iovec layout and its exclusive package borrow covers the syscall. The kernel writes only initialized byte ranges in the retained source buffers.
    assert_eq!(
        unsafe {
            libc::readv(
                right.as_raw_fd(),
                vectors.as_ptr().cast(),
                vectors.len() as _,
            )
        },
        8
    );
    assert_eq!(receive.buffer(0), b"res");
    assert_eq!(receive.buffer(1), b"ponse");
    receive.buffer_mut(0)[0] = b'R';
    assert_eq!(receive.into_inner(), [b"Res".to_vec(), b"ponse".to_vec()]);
}

#[cfg(windows)]
#[test]
fn windows_vectors_match_wsabuf_and_expose_writable_native_ranges() {
    #[repr(C)]
    struct WsaBuf {
        len: u32,
        buf: *mut u8,
    }
    assert_eq!(size_of::<IoSlice<'_>>(), size_of::<WsaBuf>());
    assert_eq!(align_of::<IoSlice<'_>>(), align_of::<WsaBuf>());
    assert_eq!(size_of::<IoSliceMut<'_>>(), size_of::<WsaBuf>());
    assert_eq!(align_of::<IoSliceMut<'_>>(), align_of::<WsaBuf>());
    let mut buffers = IoBuffersMut::new([vec![0; 2], vec![0; 3]]);
    let vectors = buffers.io_slices_mut().unwrap();
    // SAFETY: These native records have standard WSABUF layout and describe initialized, exclusive ranges retained throughout this synchronous access.
    let native =
        unsafe { std::slice::from_raw_parts(vectors.as_ptr().cast::<WsaBuf>(), vectors.len()) };
    assert_eq!(
        native.iter().map(|vector| vector.len).collect::<Vec<_>>(),
        [2, 3]
    );
    for (index, vector) in native.iter().enumerate() {
        unsafe { std::ptr::write_bytes(vector.buf, b'a' + index as u8, vector.len as usize) };
    }
    assert_eq!(buffers.buffer(0), b"aa");
    assert_eq!(buffers.buffer(1), b"bbb");
}

#[test]
fn prepared_packages_retain_boxed_buffers_and_owned_subranges() {
    let source = [
        Box::<[u8]>::from(*b"abcd").slice(1..3),
        Box::<[u8]>::from(*b"efgh").slice(2..),
    ];
    let writable = IoBuffersMut::new(source);
    let mut writable = std::thread::spawn(move || writable).join().unwrap();
    writable.buffer_mut(0).copy_from_slice(b"BC");
    writable.advance(1);
    let readable = IoBuffers::new(writable);
    assert_eq!(&*readable.io_slices().unwrap()[0], b"C");
    assert_eq!(&*readable.io_slices().unwrap()[1], b"gh");
    let mut writable = readable.into_inner();
    writable.buffer_mut(0)[0] = b'!';
    let [first, second] = writable.into_inner();
    assert_eq!(&*first.into_inner(), b"aB!d");
    assert_eq!(&*second.into_inner(), b"efgh");
}

#[test]
fn prepared_packages_keep_boxed_outer_collections_stable() {
    let source: Box<[Box<[u8]>]> = vec![Box::from(*b"ab"), Box::from(*b"cd")].into_boxed_slice();
    let readable = IoBuffers::new(source);
    let readable = std::thread::spawn(move || readable).join().unwrap();
    assert_eq!(&*readable.io_slices().unwrap()[0], b"ab");
    assert_eq!(&*readable.io_slices().unwrap()[1], b"cd");
    let writable = IoBuffersMut::new(readable.into_inner());
    let mut writable = std::thread::spawn(move || writable).join().unwrap();
    writable.buffer_mut(0).copy_from_slice(b"AB");
    writable.buffer_mut(1).copy_from_slice(b"CD");
    assert_eq!(&*writable.io_slices_mut().unwrap()[0], b"AB");
    assert_eq!(&*writable.io_slices_mut().unwrap()[1], b"CD");
    let source = writable.into_inner();
    assert_eq!(&*source[0], b"AB");
    assert_eq!(&*source[1], b"CD");
}
