// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::io;

#[test]
fn bound_factories_validate_kind_and_preserve_pool_ownership() {
    let scheduler = DirectScheduler::new(Default::default()).unwrap();
    futures_lite::future::block_on(async {
        let never = Cancellation::never();
        let stream = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
            .await
            .unwrap();
        let packet = scheduler
            .socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never)
            .await
            .unwrap();
        let pool = scheduler
            .buffer_pool(BufferPoolOptions::new(2, 16))
            .unwrap();
        assert!(scheduler.socket_packet_reader(&stream).is_err());
        assert!(scheduler.socket_packet_writer(&stream).is_err());
        assert!(scheduler.socket_stream_reader(&packet).is_err());
        assert!(scheduler.socket_stream_writer(&packet).is_err());
        for packet_reader in [false, true] {
            crate::socket_io::SOCKET_TYPE_ERROR
                .with(|slot| slot.set(Some(io::ErrorKind::PermissionDenied)));
            let error = if packet_reader {
                scheduler.socket_packet_reader(&packet).err().unwrap()
            } else {
                scheduler.socket_stream_reader(&stream).err().unwrap()
            };
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        }
        let mut reader = scheduler
            .socket_stream_reader_with_pool(&stream, pool.clone())
            .unwrap();
        reader.close().await.unwrap();
        let mut reader = scheduler
            .socket_packet_reader_with_pool(&packet, pool)
            .unwrap();
        reader.close().await.unwrap();
        scheduler.shutdown().await;
        assert!(scheduler.socket_stream_reader(&stream).is_err());
        assert!(scheduler.socket_packet_reader(&packet).is_err());
        assert!(scheduler.socket_packet_writer(&packet).is_err());
    });
}
