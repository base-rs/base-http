#!/usr/bin/env bash
# Run on Hana after all throughput runs; compression/coverage export must not overlap them.
set -euo pipefail
root=/tmp/base-http-ring-overhead-20261027
cd "$root/scheduler-deferred"
export PATH=/tmp/base-http-hana-epoll-uring-7nxlaica/tools/bin:$PATH
export LLVM_COV=/usr/bin/llvm-cov LLVM_PROFDATA=/usr/bin/llvm-profdata
CARGO_TARGET_DIR="$root/coverage-target" cargo llvm-cov report --package base-scheduler-iouring --json --ignore-filename-regex='(/usr/src/debug/rust|/tests/|/tests.rs|_tests.rs)' --output-path "$root/coverage-final.json" > "$root/coverage-export.log" 2>&1
cd "$root"
python3 - <<'PY'
from pathlib import Path
import hashlib,json
expected=json.loads(Path('expected-scheduler-sha256.json').read_text())
actual={k:hashlib.sha256((Path('scheduler-deferred')/k).read_bytes()).hexdigest() for k in expected['files']}
assert actual==expected['files'],[k for k in actual if actual[k]!=expected['files'][k]]
Path('verified-scheduler-sha256.json').write_text(json.dumps({'files_checked':len(actual),'all_match':True,'files':actual},indent=2)+'\n')
Path('trace-owned/rejected.json').write_text(json.dumps({'reason':'Scalar send interposer recorded one fewer 64-byte message than receive CQE bytes. Payload-validating client passed; normalized counters rejected and a fresh trace-owned-repeat reconciles exactly.','received_bytes':6677440,'sent_bytes':6677376,'excluded_from_normalized_conclusions':True},indent=2)+'\n')
PY
mkdir -p retained/binaries
cp deferred-final-echo minimal-final network-before network-after retained/binaries/
cp binaries/http-after retained/binaries/http-after
cp probe/Cargo.lock retained/minimal-Cargo.lock
cp scheduler-deferred/benchmarks/echo/src/server.rs retained/network-after-server.rs
cp scheduler-network-before/benchmarks/echo/src/server.rs retained/network-before-server.rs
cp /tmp/base-http-layers-20261027/raw-followup-tools/counts.c retained/
cp /tmp/base-http-layers-20261027/raw-followup-tools/counts.so retained/
(cd retained && sha256sum binaries/* counts.so > binary-sha256.txt)
tar -czf retained/diagnostics.tar.gz trace-* profile-*
tar --exclude target --exclude .git -czf retained/http-source.tar.gz http-deferred
printf 'Retention complete\n'
