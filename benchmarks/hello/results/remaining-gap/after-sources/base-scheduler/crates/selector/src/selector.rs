// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::future::Future;

/// Lifecycle shared by independently driven I/O capabilities.
///
/// Implement the individual `base-scheduler` capability traits for operations. This trait supplies no task executor, resource representation, or driver thread. Constructors document how the selector makes progress and who owns its driver.
pub trait Selector: Send + Sync {
    /// Stop admission and notify pending operations without synchronously waiting. Repeated calls are harmless. Accepted cleanup and kernel-accessible storage remain owned until retirement.
    fn close(&self);

    /// Close admission and await retirement of all accepted work, including abandoned operation futures. The driver must continue progressing during this wait. Dropping this future must not release in-flight storage or stop its retirement.
    fn shutdown(&self) -> impl Future<Output = ()> + Send;
}
