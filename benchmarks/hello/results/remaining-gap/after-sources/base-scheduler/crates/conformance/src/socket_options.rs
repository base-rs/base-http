// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Typed socket options, requiring only the direction under test.

use crate::SocketOptionFixture;
use base_scheduler::{Cancellation, OperationError, SocketGetOption, SocketSetOption};

/// Getters observe independent native changes and preserve execution errors.
pub async fn get<F: SocketOptionFixture>(fixture: &F)
where
    F::Scheduler: SocketGetOption<F::Option>,
{
    for value in fixture.values() {
        fixture.set_native(value.clone());
        let actual = fixture
            .scheduler()
            .socket_get_option(fixture.socket(), fixture.option(), &Cancellation::never())
            .await
            .unwrap();
        assert_eq!(actual, value);
    }
    let result = fixture
        .scheduler()
        .socket_get_option(
            fixture.failing_socket(),
            fixture.option(),
            &Cancellation::never(),
        )
        .await;
    assert!(
        matches!(result, Err(OperationError::Io(error)) if error.kind() == fixture.error_kind())
    );
}

/// Lazy/pre-cancelled getters make no native call, including on a failing socket.
pub async fn get_cancellation<F: SocketOptionFixture>(fixture: &F)
where
    F::Scheduler: SocketGetOption<F::Option>,
{
    let calls = fixture.calls();
    let never = Cancellation::never();
    drop(
        fixture
            .scheduler()
            .socket_get_option(fixture.socket(), fixture.option(), &never),
    );
    assert_eq!(fixture.calls(), calls);
    let token = Cancellation::new();
    token.cancel();
    for socket in [fixture.socket(), fixture.failing_socket()] {
        assert!(matches!(
            fixture
                .scheduler()
                .socket_get_option(socket, fixture.option(), &token)
                .await,
            Err(OperationError::Cancelled(_))
        ));
    }
    assert_eq!(fixture.calls(), calls);
}

/// Setters change independently observed native state and preserve execution errors.
pub async fn set<F: SocketOptionFixture>(fixture: &F)
where
    F::Scheduler: SocketSetOption<F::Option>,
{
    let [first, second] = fixture.values();
    assert_ne!(first, second);
    for value in [first, second] {
        fixture
            .scheduler()
            .socket_set_option(
                fixture.socket(),
                fixture.option(),
                value.clone(),
                &Cancellation::never(),
            )
            .await
            .unwrap();
        assert_eq!(fixture.get_native(), value);
    }
    let result = fixture
        .scheduler()
        .socket_set_option(
            fixture.failing_socket(),
            fixture.option(),
            fixture.values()[0].clone(),
            &Cancellation::never(),
        )
        .await;
    assert!(
        matches!(result, Err(OperationError::Io(error)) if error.kind() == fixture.error_kind())
    );
}

/// Unpolled/pre-cancelled setters neither dispatch nor change native state.
pub async fn set_cancellation<F: SocketOptionFixture>(fixture: &F)
where
    F::Scheduler: SocketSetOption<F::Option>,
{
    let [first, second] = fixture.values();
    fixture.set_native(first.clone());
    let calls = fixture.calls();
    let never = Cancellation::never();
    drop(fixture.scheduler().socket_set_option(
        fixture.socket(),
        fixture.option(),
        second.clone(),
        &never,
    ));
    assert_eq!(fixture.calls(), calls);
    assert_eq!(fixture.get_native(), first);
    let token = Cancellation::new();
    token.cancel();
    assert!(matches!(
        fixture
            .scheduler()
            .socket_set_option(fixture.socket(), fixture.option(), second, &token)
            .await,
        Err(OperationError::Cancelled(_))
    ));
    assert_eq!(fixture.calls(), calls);
    assert_eq!(fixture.get_native(), first);
}
