// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate complete regular-file capability tests using a `FileTestFixture`.
///
/// Invoke inside a dedicated module. The runner receives one async closure taking a fixture reference, and creates a fresh fixture with an absent path for each invocation. It must enforce deadlines, await cleanup and scheduler shutdown, and propagate panics. Every case creates its own file, seeds it with known contents, and closes it after its assertions. The backend observes contents independently through the fixture.
#[macro_export]
macro_rules! file_tests {
    (run: $run:path $(,)?) => {
        $crate::__file_test_cases! { $run;
            create => |fixture, scheduler, file| {
                fixture.assert_contents(b"abcdef");
            },
            read => |fixture, scheduler, file| {
                $crate::file::read(scheduler, &file, b"abcdef").await;
            },
            write => |fixture, scheduler, file| {
                $crate::file::write(scheduler, &file, b"UVWXYZ").await;
                fixture.assert_contents(b"UVWXYZ");
            },
            seek => |fixture, scheduler, file| {
                $crate::file::seek(scheduler, &file, 6).await;
            },
            metadata => |fixture, scheduler, file| {
                $crate::file::metadata(scheduler, &file, 6).await;
            },
            read_at => |fixture, scheduler, file| {
                $crate::file::read_at(scheduler, &file, 1, b"bcde").await;
            },
            write_at => |fixture, scheduler, file| {
                $crate::file::write_at(scheduler, &file, 2, b"XY").await;
                fixture.assert_contents(b"abXYef");
            },
            truncate_grow => |fixture, scheduler, file| {
                $crate::file::truncate(scheduler, &file, 9).await;
                fixture.assert_contents(b"abcdef\0\0\0");
            },
            truncate_shrink => |fixture, scheduler, file| {
                $crate::file::truncate(scheduler, &file, 3).await;
                fixture.assert_contents(b"abc");
            },
            sync => |fixture, scheduler, file| {
                $crate::file::sync(scheduler, &file).await;
            },
            write_error => |fixture, scheduler, file| {
                let read_only = $crate::__base_scheduler::FileOpen::file_open(scheduler, fixture.path(), $crate::__base_scheduler::FileOptions::READ, &$crate::__base_scheduler::Cancellation::never()).await.unwrap();
                $crate::file::write_error(scheduler, &read_only).await;
                scheduler.file_close_submit(read_only).await.unwrap();
            },
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __file_test_cases {
    ($run:path; $($name:ident => |$fixture:ident, $scheduler:ident, $file:ident| $body:block),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                $run(async |$fixture| {
                    use $crate::FileTestFixture as _;
                    use $crate::__base_scheduler::{FileClose as _, FileSeek as _};
                    let $scheduler = $fixture.scheduler();
                    let $file = $crate::file::create($scheduler, $fixture.path(), || $fixture.assert_absent()).await;
                    $fixture.assert_contents(b"");
                    $crate::file::write($scheduler, &$file, b"abcdef").await;
                    $fixture.assert_contents(b"abcdef");
                    $scheduler.file_seek(&$file, ::std::io::SeekFrom::Start(0), &$crate::__base_scheduler::Cancellation::never()).await.unwrap();
                    $body
                    $scheduler.file_close($file).await.unwrap();
                });
            }
        )+
    };
}
