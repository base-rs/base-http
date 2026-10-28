// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// Shared expansion only; public suite macros own the case lists. The runner owns fixture setup, deadlines, teardown, and panic propagation.
#[doc(hidden)]
#[macro_export]
macro_rules! __conformance_test_cases {
    ($run:path; $($name:ident => |$($argument:ident),+| $body:block),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                $run(async |$($argument),+| $body);
            }
        )+
    };
}
