// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Selection of child state changes. The default waits for termination.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProcessWaitFlags {
    /// Return immediately with `None` if a matching child has no reportable state change.
    pub nohang: bool,
    /// Also report stopped children. Unsupported on Windows.
    pub untraced: bool,
    /// Also report children resumed by `SIGCONT`. Unsupported on Windows.
    pub continued: bool,
}
