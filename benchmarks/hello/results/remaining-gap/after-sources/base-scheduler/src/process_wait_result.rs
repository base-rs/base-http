// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::process::ExitStatus;

/// One child's status. Only a termination status means the child has exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessWaitResult {
    /// The actual positive process ID, including when waiting for any child or a group.
    pub pid: u32,
    /// Native status, compatible with standard-library exit status extensions. On Unix, use `ExitStatusExt` to distinguish signal termination, stops, and continuation.
    pub status: ExitStatus,
}
