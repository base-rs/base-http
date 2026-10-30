// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::process::Command;

#[test]
fn executable_lists_shared_tasks_and_reports_unknown_tasks() {
    let output = Command::new(env!("CARGO_BIN_EXE_base-http-bake"))
        .arg("--list")
        .output()
        .unwrap();
    assert!(output.status.success());
    let listing = String::from_utf8(output.stdout).unwrap();
    for task in ["agent:context:install", "test:coverage", "cargo:release"] {
        assert!(listing.contains(task), "missing shared task {task}");
    }
    let output = Command::new(env!("CARGO_BIN_EXE_base-http-bake"))
        .arg("unknown-task")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unknown task")
    );
}
