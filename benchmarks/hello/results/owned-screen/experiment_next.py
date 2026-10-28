#!/usr/bin/env python3
"""Profile follow-up HTTP storage and readiness-driver experiments in copies.

The combined baseline retains the first experiment's static-only output policy.
Root/reactor modes are architectural probes, not replacements for NativeScheduler.
"""

import argparse
import datetime
import difflib
import json
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
from types import SimpleNamespace
import tomllib

from experiment import ROOT, HTTP, coalesce_output, digest, replace_once, reuse_input
from run import sample


VARIANTS = ("combined", "http-storage", "native-receive", "all", "writev", "owned-reactor")


def parser_storage(source):
    return replace_once(
        source,
        "        let mut headers = vec![httparse::EMPTY_HEADER; self.options.max_fields];\n"
        "        let mut parsed = httparse::Request::new(&mut headers);",
        "        let mut local = [httparse::EMPTY_HEADER; 100];\n"
        "        let mut overflow;\n"
        "        let headers = if self.options.max_fields <= local.len() {\n"
        "            &mut local[..self.options.max_fields]\n"
        "        } else {\n"
        "            overflow = vec![httparse::EMPTY_HEADER; self.options.max_fields];\n"
        "            &mut overflow\n        };\n"
        "        let mut parsed = httparse::Request::new(headers);",
    )


def response_storage(source):
    source = replace_once(
        source,
        "    let mut head = response.clone();\n"
        "    let has_trailers = head.fields.has_trailers();\n"
        "    validate_trailers(&head.fields)?;\n"
        "    // Wire-generated fields belong to the initial section even when the\n"
        "    // semantic message already contains a trailer boundary.\n"
        "    head.fields = base_http::Fields::try_from_pairs(\n"
        "        head.fields.headers().iter().map(|f| (f.name(), f.value())),\n    )?;",
        "    let has_trailers = response.fields.has_trailers();\n"
        "    validate_trailers(&response.fields)?;\n"
        "    let mut head = ResponseHead {\n"
        "        status: response.status, version: response.version,\n"
        "        protocol: response.protocol.clone(),\n"
        "        fields: base_http::Fields::try_from_pairs(\n"
        "            response.fields.headers().iter().map(|f| (f.name(), f.value())),\n"
        "        )?,\n    };",
    )
    source = replace_once(source, "        let bytes = head_bytes(&plan.head)?;", "        encode_head(&plan.head, &mut self.pending)?;")
    source = replace_once(
        source,
        "        self.write_all(&bytes, cancellation).await?;\n        // DIAGNOSTIC ONLY:",
        "        // DIAGNOSTIC ONLY:",
    )
    # Keep the original allocating helper for rejection/interim paths; the main
    # response encodes directly into already retained connection-local capacity.
    return source + '''

fn encode_head(head: &ResponseHead, bytes: &mut Vec<u8>) -> Result<(), Error> {
    use std::io::Write;
    assert!(bytes.is_empty());
    let version = match head.version {
        Some(Version::Http10) => "HTTP/1.0",
        Some(Version::Http11) => "HTTP/1.1",
        _ => return Err(Error::Invalid("unsupported response version")),
    };
    write!(bytes, "{version} {} {}\\r\\n", head.status.as_u16(),
        head.status.description().unwrap_or(""))?;
    wire::append_fields(bytes, head.fields.headers());
    bytes.extend_from_slice(b"\\r\\n");
    Ok(())
}
'''


def native_receive(source):
    return replace_once(
        source,
        "        match interest {\n"
        "            Interest::Readable => self.inner.read_with(|socket| operation(socket)).await,\n"
        "            Interest::Writable => self.inner.write_with(|socket| operation(socket)).await,\n"
        "        }",
        "        loop {\n            match interest {\n"
        "                Interest::Readable => self.inner.readable().await?,\n"
        "                Interest::Writable => self.inner.writable().await?,\n"
        "            }\n            let result = operation(self.get_ref());\n"
        "            if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {\n"
        "                return result;\n            }\n        }",
    )


def send_writev(source):
    return replace_once(
        source,
        "    socket.send_with_flags(bytes, POLL_FLAGS | send_flags(flags))\n}",
        "    #[cfg(target_os = \"macos\")]\n"
        "    if !flags.dont_route {\n"
        "        let vectors = [IoSlice::new(bytes)];\n"
        "        // SAFETY: The registered socket is nonblocking, the stream writer\n"
        "        // configured SO_NOSIGPIPE, and the borrowed vector lives through this call.\n"
        "        return result_count(unsafe { libc::writev(socket.as_raw_fd(), vectors.as_ptr().cast(), 1) });\n"
        "    }\n"
        "    socket.send_with_flags(bytes, POLL_FLAGS | send_flags(flags))\n}",
    )


def root_modes(source):
    return replace_once(
        source, '        Some("base-direct") => {',
        '''        Some(mode @ ("base-root" | "base-reactor")) => {
            let scheduler = NativeScheduler::with_options(NativeOptions {
                workers: 1, threads: 1, capacity: MAX_CONNECTIONS,
                ..Default::default()
            })?;
            let result = if mode == "base-reactor" {
                async_io::block_on(base_server(&scheduler))
            } else {
                futures_lite::future::block_on(base_server(&scheduler))
            };
            futures_lite::future::block_on(scheduler.shutdown());
            result
        }
        Some("base-direct") => {''',
    )


def owned_mode(source):
    return replace_once(source, '        Some("base-native") => {', '''        Some("base-owned") => {
            let selector = base_scheduler_selector::ReadinessSelector::with_options(
                base_scheduler_selector::SelectorOptions { threads: 1, capacity: MAX_CONNECTIONS }
            )?;
            let scheduler = Arc::new(NativeScheduler::with_drivers(
                selector, vec![base_scheduler_selector::owned_reactor::Driver::new()?],
                base_scheduler_native::NativeExecutorOptions { workers: 1, timer_capacity: 1024 }
            )?);
            let shared = scheduler.clone();
            let server = scheduler.spawn(async move { base_server(shared.as_ref()).await })?;
            let result = scheduler.block_on(server)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            scheduler.block_on(scheduler.shutdown());
            result
        }
        Some("base-native") => {''')


def registry_source(name):
    candidates = list((Path.home() / ".cargo/registry/src").glob(f"index.crates.io-*/{name}-0.1.0"))
    assert len(candidates) == 1, candidates
    return candidates[0]


def build(args, variants):
    args.stage.mkdir(parents=True, exist_ok=True)
    provenance = {}
    original_packages = {
        (p["name"], p["version"], p.get("source"), p.get("checksum"))
        for p in tomllib.loads((ROOT / "Cargo.lock").read_text())["package"]
    }
    for variant in variants:
        stage = args.stage / variant
        changed = []
        for name in ("base-http1", "base-http-server"):
            original = HTTP.parent / name
            copied = stage / name
            shutil.copytree(original / "src", copied / "src", dirs_exist_ok=True)
            shutil.copyfile(original / "readme.md", copied / "readme.md")
            (copied / "Cargo.toml").write_text((original / "Cargo.toml").read_text().replace(
                'path = "../base-http"', f'path = "{HTTP}"'
            ))
        codec = stage / "base-http1/src"
        input_file = codec / "input.rs"
        output_file = codec / "output.rs"
        input_file.write_text(reuse_input(input_file.read_text()))
        output_file.write_text(coalesce_output(output_file.read_text()))
        if variant in ("http-storage", "all", "writev", "owned-reactor"):
            input_file.write_text(parser_storage(input_file.read_text()))
            output_file.write_text(response_storage(output_file.read_text()))
        for name in ("input.rs", "output.rs"):
            changed.append((HTTP.parent / "base-http1/src" / name, codec / name, f"base-http1/src/{name}"))
        bench = stage / "hello"
        shutil.copytree(ROOT / "src", bench / "src", dirs_exist_ok=True)
        main = bench / "src/main.rs"
        main.write_text(root_modes(main.read_text()))
        if variant == "owned-reactor":
            main.write_text(owned_mode(main.read_text()))
        changed.append((ROOT / "src/main.rs", main, "hello/src/main.rs"))
        manifest = (ROOT / "Cargo.toml").read_text()
        manifest = manifest.replace('path = "../.."', f'path = "{HTTP}"')
        manifest = manifest.replace('path = "../../../base-http-server"', 'path = "../base-http-server"')
        manifest = manifest.replace('path = "../../../base-http1"', 'path = "../base-http1"')
        manifest = manifest.replace('[dependencies]\n', '[dependencies]\nasync-io = "=2.6.0"\n')
        if variant == "owned-reactor":
            manifest = manifest.replace('[dependencies]\n', '[dependencies]\nbase-scheduler-selector = "=0.1.0"\n')
        if variant in ("native-receive", "all", "writev", "owned-reactor"):
            manifest += '\n[patch.crates-io]\n'
            names = ["base-scheduler-selector"]
            if variant in ("writev", "owned-reactor"):
                names.append("base-scheduler-tokio")
            for name in names:
                original = registry_source(name)
                copied = stage / name
                shutil.copytree(original, copied, dirs_exist_ok=True)
                manifest += f'{name} = {{ path = "../{name}" }}\n'
                if name == "base-scheduler-selector":
                    native = copied / "src/native_socket.rs"
                    native.write_text(native_receive(native.read_text()))
                    changed.append((original / "src/native_socket.rs", native, f"{name}/src/native_socket.rs"))
                if variant in ("writev", "owned-reactor"):
                    file = copied / "src/socket_io.rs"
                    file.write_text(send_writev(file.read_text()))
                    changed.append((original / "src/socket_io.rs", file, f"{name}/src/socket_io.rs"))
                    filename = "readiness_socket_stream_writer.rs" if name == "base-scheduler-selector" else "tokio_stream_writer.rs"
                    file = copied / "src" / filename
                    source = file.read_text()
                    if name == "base-scheduler-selector":
                        source = replace_once(source, "        Ok(Self {\n            scheduler,",
                            "        #[cfg(target_os = \"macos\")]\n        socket.get_ref().set_nosigpipe(true)?;\n        Ok(Self {\n            scheduler,")
                    else:
                        source = replace_once(source, "        Ok(TokioStreamWriter {",
                            "        #[cfg(target_os = \"macos\")]\n        socket.get_ref().set_nosigpipe(true)?;\n        Ok(TokioStreamWriter {")
                    file.write_text(source)
                    changed.append((original / "src" / filename, file, f"{name}/src/{filename}"))
                if variant == "owned-reactor" and name == "base-scheduler-selector":
                    file = copied / "Cargo.toml"
                    file.write_text(file.read_text() + '\n[dependencies.polling]\nversion = "=3.11.0"\n')
                    prototype = copied / "src/owned_reactor.rs"
                    shutil.copyfile(ROOT / "experiments/owned_reactor.rs", prototype)
                    changed.append((Path("/dev/null"), prototype, f"{name}/src/owned_reactor.rs"))
                    file = copied / "src/lib.rs"
                    file.write_text(file.read_text() + "\npub mod owned_reactor;\n")
                    changed.append((original / "src/lib.rs", file, f"{name}/src/lib.rs"))
                    native = copied / "src/native_socket.rs"
                    source = replace_once(native.read_text(),
                        "        // Invalid socket states must return their immediate OS error instead of waiting for readiness that cannot occur.",
                        "        if let Some(registration) = crate::owned_reactor::registration(self) {\n"
                        "            return crate::owned_reactor::io(registration?, interest, operation).await;\n        }\n"
                        "        // Invalid socket states must return their immediate OS error instead of waiting for readiness that cannot occur.")
                    source = replace_once(source,
                        "    pub(crate) fn into_close(self) -> impl FnOnce() -> io::Result<()> + Send + 'static {",
                        "    pub(crate) fn into_close(self) -> impl FnOnce() -> io::Result<()> + Send + 'static {\n"
                        "        crate::owned_reactor::retire(&self);")
                    native.write_text(source)
        (bench / "Cargo.toml").write_text(manifest)
        shutil.copyfile(ROOT / "Cargo.lock", bench / "Cargo.lock")
        subprocess.run([
            "cargo", "build", "--release", "--offline", "--features", "profile-tools",
            "--bin", "base-http-hello-benchmark", "--bin", "allocation-probe",
            "--manifest-path", str(bench / "Cargo.toml"), "--target-dir", str(args.stage / "build"),
        ], check=True)
        # The direct async-io dependency and optional local selector patch must
        # not silently update any registry package used in the comparison.
        for package in tomllib.loads((bench / "Cargo.lock").read_text())["package"]:
            if package.get("source"):
                key = (package["name"], package["version"], package.get("source"), package.get("checksum"))
                assert key in original_packages, key
        binary = stage / "server"
        shutil.copyfile(args.stage / "build/release/base-http-hello-benchmark", binary)
        binary.chmod(0o755)
        allocation = subprocess.check_output([str(args.stage / "build/release/allocation-probe")], text=True)
        (args.output / f"{variant}.allocations.json").write_text(allocation)
        patch = "".join("".join(difflib.unified_diff(
            old.read_text().splitlines(keepends=True), new.read_text().splitlines(keepends=True),
            fromfile=name, tofile=name,
        )) for old, new, name in changed)
        (args.output / f"{variant}.patch").write_text(patch)
        files = list(stage.rglob("*.rs")) + list(stage.rglob("Cargo.toml")) + [bench / "Cargo.lock", Path(__file__), ROOT / "experiment.py", ROOT / "run.py"]
        files.extend((HTTP / "src").rglob("*.rs"))
        files.append(HTTP / "Cargo.toml")
        provenance[variant] = {
            "binary_sha256": digest(binary), "patch_sha256": digest(args.output / f"{variant}.patch"),
            "sources_sha256": {str(file): digest(file) for file in sorted(files)},
        }
    (args.output / "sources.json").write_text(json.dumps(provenance, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wrk", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "results/next-screen")
    parser.add_argument("--stage", type=Path, default=ROOT / "target/next-experiments")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--duration", type=int, default=3)
    parser.add_argument("--jobs", nargs="+", default=[
        "combined:base-native", "combined:base-tokio", "combined:base-root",
        "combined:base-reactor", "combined:hyper-tokio", "http-storage:base-native",
        "http-storage:base-tokio", "http-storage:base-reactor", "native-receive:base-native",
        "native-receive:base-reactor", "all:base-native", "all:base-reactor",
    ])
    args = parser.parse_args()
    for key in ("wrk", "output", "stage"):
        setattr(args, key, getattr(args, key).resolve())
    args.output.mkdir(parents=True, exist_ok=True)
    jobs = [job.split(":") for job in args.jobs]
    assert all(len(job) == 2 and job[0] in VARIANTS for job in jobs)
    if not args.skip_build:
        build(args, sorted({variant for variant, _ in jobs}))
    if args.build_only:
        return
    metadata = {
        "utc_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "configuration": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "uname": list(os.uname()), "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "concurrency": 64, "load_threads": 4, "warmup_seconds": 1, "order_seed": 20261028,
        "binary_sha256": {variant: digest(args.stage / variant / "server") for variant, _ in jobs},
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    samples = []
    rng = random.Random(20261028)
    with (args.output / "samples.jsonl").open("w") as journal:
        for repetition in range(1, args.repetitions + 1):
            ordered = jobs.copy()
            rng.shuffle(ordered)
            for variant, backend in ordered:
                output = args.output / variant
                output.mkdir(exist_ok=True)
                print(f"{variant:16} ", end="", flush=True)
                result = sample(SimpleNamespace(
                    binary=args.stage / variant / "server", wrk=args.wrk, output=output,
                    threads=4, warmup=1, duration=args.duration,
                ), backend, 64, repetition)
                result["variant"] = variant
                samples.append(result)
                journal.write(json.dumps(result) + "\n")
                journal.flush()
    summary = []
    for variant, backend in sorted({(row["variant"], row["backend"]) for row in samples}):
        group = [row for row in samples if row["variant"] == variant and row["backend"] == backend]
        summary.append({
            "variant": variant, "backend": backend,
            "requests_per_second_min": min(row["requests_per_second"] for row in group),
            "requests_per_second_max": max(row["requests_per_second"] for row in group),
            **{key: statistics.median(row[key] for row in group) for key in (
                "requests_per_second", "latency_p99_ms", "server_cpu_percent_one_core",
            )},
        })
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
