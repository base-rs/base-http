// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Resolver validation, hint, error, and owned-result assertions.

use crate::AddressResolutionFixture;
use base_scheduler::*;
use std::io;

/// Unpolled and pre-cancelled requests perform no lookup. Pre-cancellation also precedes validation.
pub async fn pre_cancelled<F: AddressResolutionFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let baseline = fixture.lookups();
    let never = Cancellation::never();
    drop(scheduler.address_resolve(
        Some(fixture.name()),
        AddressService::Port(80),
        AddressHints::default(),
        &never,
    ));
    assert_eq!(fixture.lookups(), baseline);
    let token = Cancellation::new();
    token.cancel();
    for name in [fixture.name(), "", "bad\0name"] {
        assert!(matches!(
            scheduler
                .address_resolve(
                    Some(name),
                    AddressService::Port(80),
                    AddressHints::default(),
                    &token
                )
                .await,
            Err(OperationError::Cancelled(_))
        ));
    }
    assert_eq!(fixture.lookups(), baseline);
}

/// Empty and embedded-NUL names/services are input errors, before lookup submission.
pub async fn validation<F: AddressResolutionFixture>(fixture: &F) {
    let scheduler = fixture.scheduler();
    let baseline = fixture.lookups();
    for (name, service) in [
        (Some(""), AddressService::Port(80)),
        (Some("bad\0name"), AddressService::Port(80)),
        (Some(fixture.name()), AddressService::Name("")),
        (Some(fixture.name()), AddressService::Name("bad\0service")),
    ] {
        let result = scheduler
            .address_resolve(
                name,
                service,
                AddressHints::default(),
                &Cancellation::never(),
            )
            .await;
        assert!(
            matches!(result, Err(OperationError::Io(error)) if error.kind() == io::ErrorKind::InvalidInput)
        );
    }
    assert_eq!(fixture.lookups(), baseline);
}

/// Results preserve configured order, duplicates, fields, and reusable borrowed views, including after moving the owner to another thread.
pub async fn results<F: AddressResolutionFixture>(fixture: &F) {
    let result = fixture
        .scheduler()
        .address_resolve(
            Some(fixture.name()),
            AddressService::Port(80),
            AddressHints::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    let result = std::thread::spawn(move || result).join().unwrap();
    for _ in 0..2 {
        let actual: Vec<_> = result
            .entries()
            .map(|entry| entry.to_address_info())
            .collect();
        assert_eq!(actual, fixture.expected());
    }
    // A second lookup must not invalidate a previous result's borrowed entries.
    let earlier = result.entries().next();
    let other = fixture
        .scheduler()
        .address_resolve(
            Some(fixture.name()),
            AddressService::Port(80),
            AddressHints::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    drop(other);
    assert_eq!(
        earlier.map(|entry| entry.to_address_info()),
        fixture.expected().first().copied()
    );
    assert_eq!(
        result
            .entries()
            .map(|entry| entry.to_address_info())
            .collect::<Vec<_>>(),
        fixture.expected()
    );
}

/// Configured service names select the same endpoints as their numeric service.
pub async fn service_name<F: AddressResolutionFixture>(fixture: &F) {
    let result = fixture
        .scheduler()
        .address_resolve(
            Some(fixture.name()),
            AddressService::Name(fixture.service_name()),
            AddressHints::default(),
            &Cancellation::never(),
        )
        .await
        .unwrap();
    assert_eq!(
        result
            .entries()
            .map(|entry| entry.to_address_info())
            .collect::<Vec<_>>(),
        fixture.expected()
    );
}

/// All hint fields reach the actual resolver boundary. A configured-family lookup may fail on hosts without that family; native flag forwarding remains independently observable.
pub async fn hints<F: AddressResolutionFixture>(fixture: &F) {
    let base = AddressHints {
        domain: Some(SocketDomain::Ipv4),
        socket_type: Some(SocketType::Datagram),
        protocol: Some(SocketProtocol::Udp),
        ..AddressHints::default()
    };
    // Exercise flags separately as well as together, so swapped native bits cannot pass.
    for hints in [
        AddressHints::default(),
        base,
        AddressHints {
            passive: true,
            ..base
        },
        AddressHints {
            numeric_host: true,
            ..base
        },
        AddressHints {
            address_configured: true,
            ..base
        },
        AddressHints {
            passive: true,
            numeric_host: true,
            address_configured: true,
            ..base
        },
    ] {
        let _ = fixture
            .scheduler()
            .address_resolve(
                Some("127.0.0.1"),
                AddressService::Port(8080),
                hints,
                &Cancellation::never(),
            )
            .await;
        fixture.assert_hints(hints);
    }
}

/// Requiring a numeric host rejects the configured nonnumeric name instead of performing a successful name lookup.
pub async fn numeric_only<F: AddressResolutionFixture>(fixture: &F) {
    let hints = AddressHints {
        numeric_host: true,
        ..AddressHints::default()
    };
    let result = fixture
        .scheduler()
        .address_resolve(
            Some(fixture.name()),
            AddressService::Port(80),
            hints,
            &Cancellation::never(),
        )
        .await;
    assert!(
        matches!(result, Err(OperationError::Io(_))),
        "numeric_host accepted a nonnumeric name"
    );
}

/// Numeric IPv4/IPv6 literals respect requested family, port, and transport without DNS or service-name lookup. Invoke for supported families.
pub async fn numeric<S: AddressResolution>(scheduler: &S, domain: SocketDomain) {
    let name = match domain {
        SocketDomain::Ipv4 => "127.0.0.1",
        SocketDomain::Ipv6 => "::1",
    };
    for (socket_type, protocol) in [
        (SocketType::Stream, SocketProtocol::Tcp),
        (SocketType::Datagram, SocketProtocol::Udp),
    ] {
        let hints = AddressHints {
            domain: Some(domain),
            socket_type: Some(socket_type),
            protocol: Some(protocol),
            numeric_host: true,
            ..AddressHints::default()
        };
        let result = scheduler
            .address_resolve(
                Some(name),
                AddressService::Port(43210),
                hints,
                &Cancellation::never(),
            )
            .await
            .unwrap();
        let entries: Vec<_> = result
            .entries()
            .map(|entry| entry.to_address_info())
            .collect();
        assert!(
            !entries.is_empty(),
            "fixture must support the selected numeric family/transport"
        );
        for entry in entries {
            assert_eq!(
                entry.address.ip(),
                name.parse::<std::net::IpAddr>().unwrap()
            );
            assert_eq!(entry.address.port(), 43210);
            assert_eq!(entry.socket_type, socket_type);
            assert_eq!(entry.protocol, Some(protocol));
        }
    }
}

/// An absent name selects loopback or wildcard endpoints according to `passive`.
pub async fn absent_name<S: AddressResolution>(scheduler: &S, domain: SocketDomain) {
    for passive in [false, true] {
        let hints = AddressHints {
            domain: Some(domain),
            socket_type: Some(SocketType::Stream),
            protocol: Some(SocketProtocol::Tcp),
            passive,
            ..AddressHints::default()
        };
        let result = scheduler
            .address_resolve(
                None,
                AddressService::Port(8080),
                hints,
                &Cancellation::never(),
            )
            .await
            .unwrap();
        let entries: Vec<_> = result
            .entries()
            .map(|entry| entry.to_address_info())
            .collect();
        assert!(
            !entries.is_empty(),
            "fixture must support the selected family"
        );
        for entry in entries {
            assert_eq!(SocketDomain::from(entry.address), domain);
            assert_eq!(entry.address.port(), 8080);
            assert_eq!(entry.socket_type, SocketType::Stream);
            assert_eq!(entry.protocol, Some(SocketProtocol::Tcp));
            if passive {
                assert!(entry.address.ip().is_unspecified());
            } else {
                assert!(entry.address.ip().is_loopback());
            }
        }
    }
}

/// Resolver error detail is preserved inside the I/O error.
pub async fn errors<F: AddressResolutionFixture>(fixture: &F) {
    let result = fixture
        .scheduler()
        .address_resolve(
            Some(fixture.failure_name()),
            AddressService::Port(80),
            AddressHints::default(),
            &Cancellation::never(),
        )
        .await;
    let Err(OperationError::Io(error)) = result else {
        panic!("expected resolver I/O error");
    };
    fixture.assert_error(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<AddressResolveError>())
            .expect("lost structured resolver error"),
    );
}
