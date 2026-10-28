// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;
use base_scheduler::*;
use base_scheduler_conformance::{AddressResolutionFixture, address_resolution};
use futures_lite::future::block_on;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    Eager,
    IgnoreCancellation,
    IgnoreValidation,
    Reorder,
    Deduplicate,
    LoseError,
    IgnoreHints,
    SwapFlags,
    FalseSuccess,
}
struct Fixture {
    hints: Mutex<Option<AddressHints>>,
    entries: Vec<AddressInfo>,
    calls: AtomicUsize,
    fault: Fault,
}
impl Fixture {
    fn new(fault: Fault) -> Self {
        let first = AddressInfo {
            address: "127.0.0.1:80".parse().unwrap(),
            socket_type: SocketType::Stream,
            protocol: Some(SocketProtocol::Tcp),
        };
        Self {
            hints: Mutex::default(),
            entries: vec![
                first,
                AddressInfo {
                    address: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 80, 42, 7).into(),
                    socket_type: SocketType::Other(123),
                    protocol: Some(SocketProtocol::Other(456)),
                },
                first,
            ],
            calls: AtomicUsize::new(0),
            fault,
        }
    }
}
impl AddressResolution for Fixture {
    type Addresses = Vec<AddressInfo>;
    fn address_resolve(
        &self,
        name: Option<&str>,
        service: AddressService<'_>,
        hints: AddressHints,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Addresses>> + Send {
        if self.fault == Fault::Eager {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        async move {
            if self.fault != Fault::IgnoreCancellation {
                cancellation.check()?;
            }
            if self.fault != Fault::IgnoreValidation
                && (name.is_some_and(|name| name.is_empty() || name.contains('\0'))
                    || matches!(service, AddressService::Name(name) if name.is_empty() || name.contains('\0')))
            {
                return Err(
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid resolver input").into(),
                );
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut hints = if self.fault == Fault::IgnoreHints {
                AddressHints::default()
            } else {
                hints
            };
            if self.fault == Fault::SwapFlags {
                std::mem::swap(&mut hints.passive, &mut hints.numeric_host);
            }
            *self.hints.lock().unwrap() = Some(hints);
            if hints.numeric_host && name.is_some_and(|name| name.parse::<IpAddr>().is_err()) {
                return Err(io::Error::from(AddressResolveError {
                    kind: AddressResolveErrorKind::NameNotFound,
                    code: None,
                    message: "numeric host required".into(),
                    system_error: None,
                })
                .into());
            }
            if name == Some("failure.test") {
                if self.fault == Fault::FalseSuccess {
                    return Ok(vec![]);
                }
                if self.fault == Fault::LoseError {
                    return Err(io::Error::other("lost resolver detail").into());
                }
                return Err(io::Error::from(AddressResolveError {
                    kind: AddressResolveErrorKind::System,
                    code: Some(42),
                    message: "fixture resolver failure".into(),
                    system_error: Some(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "native detail",
                    )),
                })
                .into());
            }
            if name == Some("conformance.test") {
                let mut entries = self.entries.clone();
                if self.fault == Fault::Reorder {
                    entries.reverse();
                    entries.rotate_left(1);
                }
                if self.fault == Fault::Deduplicate {
                    entries.truncate(2);
                }
                return Ok(entries);
            }
            let hints = if self.fault == Fault::IgnoreHints {
                AddressHints::default()
            } else {
                hints
            };
            let domain = hints.domain.unwrap_or(SocketDomain::Ipv4);
            let address: IpAddr = match name {
                Some(name) => name.parse().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                None => match (domain, hints.passive) {
                    (SocketDomain::Ipv4, false) => Ipv4Addr::LOCALHOST.into(),
                    (SocketDomain::Ipv4, true) => Ipv4Addr::UNSPECIFIED.into(),
                    (SocketDomain::Ipv6, false) => Ipv6Addr::LOCALHOST.into(),
                    (SocketDomain::Ipv6, true) => Ipv6Addr::UNSPECIFIED.into(),
                },
            };
            let port = match service {
                AddressService::Port(port) => port,
                AddressService::Name(_) => 80,
            };
            Ok(vec![AddressInfo {
                address: (address, port).into(),
                socket_type: hints.socket_type.unwrap_or(SocketType::Stream),
                protocol: hints.protocol.or(Some(SocketProtocol::Tcp)),
            }])
        }
    }
}
impl AddressResolutionFixture for Fixture {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn name(&self) -> &str {
        "conformance.test"
    }
    fn service_name(&self) -> &str {
        "conformance-http"
    }
    fn assert_hints(&self, hints: AddressHints) {
        assert_eq!(*self.hints.lock().unwrap(), Some(hints));
    }
    fn expected(&self) -> &[AddressInfo] {
        &self.entries
    }
    fn failure_name(&self) -> &str {
        "failure.test"
    }
    fn assert_error(&self, error: &AddressResolveError) {
        assert_eq!(error.kind, AddressResolveErrorKind::System);
        assert_eq!(error.code, Some(42));
        assert_eq!(error.message, "fixture resolver failure");
        let underlying = error.system_error.as_ref().unwrap();
        assert_eq!(underlying.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(underlying.to_string(), "native detail");
    }
    fn lookups(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
fn run_case(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    support::run(move || block_on(test(&Fixture::new(Fault::None))));
}
mod generated {
    base_scheduler_conformance::address_resolution_tests!(run: super::run_case);
}
mod ipv4 {
    base_scheduler_conformance::address_resolution_ip_tests!(run: super::run_case, domain: base_scheduler::SocketDomain::Ipv4);
}
mod ipv6 {
    base_scheduler_conformance::address_resolution_ip_tests!(run: super::run_case, domain: base_scheduler::SocketDomain::Ipv6);
}
#[test]
fn empty_successful_results_are_allowed() {
    let fixture = Fixture {
        hints: Mutex::default(),
        entries: vec![],
        calls: AtomicUsize::new(0),
        fault: Fault::None,
    };
    block_on(address_resolution::results(&fixture));
}
#[test]
fn rejects_resolver_contract_violations() {
    for fault in [
        Fault::Eager,
        Fault::IgnoreCancellation,
        Fault::IgnoreValidation,
        Fault::Reorder,
        Fault::Deduplicate,
        Fault::LoseError,
        Fault::IgnoreHints,
        Fault::FalseSuccess,
    ] {
        support::run(move || {
            let fixture = Fixture::new(fault);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                block_on(async {
                    match fault {
                        Fault::Eager | Fault::IgnoreCancellation => {
                            address_resolution::pre_cancelled(&fixture).await
                        }
                        Fault::IgnoreValidation => address_resolution::validation(&fixture).await,
                        Fault::Reorder | Fault::Deduplicate => {
                            address_resolution::results(&fixture).await
                        }
                        Fault::LoseError | Fault::FalseSuccess => {
                            address_resolution::errors(&fixture).await
                        }
                        _ => address_resolution::numeric(&fixture, SocketDomain::Ipv6).await,
                    }
                })
            }));
            assert!(
                result.is_err(),
                "resolver suite accepted an incorrect backend"
            );
        });
    }
}

#[test]
fn rejects_dropped_hint_fields_and_numeric_host_fallback() {
    for query in [false, true] {
        let fixture = Fixture::new(Fault::IgnoreHints);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(async {
                if query {
                    address_resolution::numeric_only(&fixture).await;
                } else {
                    address_resolution::hints(&fixture).await;
                }
            })))
            .is_err()
        );
    }
}

#[test]
fn rejects_swapped_native_hint_flags() {
    let fixture = Fixture::new(Fault::SwapFlags);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(
            address_resolution::hints(&fixture)
        )))
        .is_err()
    );
}
