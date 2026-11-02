// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// Other platforms cannot observe acknowledgement, so `is_acknowledged` cannot report data in flight there.
#![cfg(any(target_os = "linux", target_os = "macos"))]

use base_http_test_socket::{is_acknowledged, wait_acknowledged};
use std::{
    io::{ErrorKind, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    thread,
    time::Duration,
};

// A peer which does not read leaves written data unacknowledged once its receive buffer fills, so the wait holds until it reads.
#[test]
fn waits_until_the_peer_acknowledges_data_and_the_fin() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (stream, _) = listener.accept().unwrap();
    assert!(is_acknowledged(&stream));

    stream.set_nonblocking(true).unwrap();
    let mut written = 0;
    loop {
        match (&stream).write(&[0; 65536]) {
            Ok(count) => written += count,
            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
            Err(error) => panic!("{error}"),
        }
    }
    stream.shutdown(Shutdown::Write).unwrap();
    assert!(!is_acknowledged(&stream));

    let reader = thread::spawn(move || {
        // Start reading only after the wait has begun.
        thread::sleep(Duration::from_millis(50));
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).unwrap();
        bytes.len()
    });
    wait_acknowledged(&stream);
    assert!(is_acknowledged(&stream));
    assert_eq!(reader.join().unwrap(), written);
}
