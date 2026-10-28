set -euo pipefail
cd /tmp/base-http-fixed-rate-20261028
cargo generate-lockfile --offline --manifest-path client/Cargo.toml > build.log 2>&1
cargo test --offline --locked --manifest-path client/Cargo.toml >> build.log 2>&1
cargo build --offline --locked --release --manifest-path client/Cargo.toml >> build.log 2>&1
cp client/target/release/base-http-fixed-rate-client binaries/client
sha256sum binaries/* > expected-binaries.sha256
