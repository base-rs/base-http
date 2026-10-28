set -euo pipefail
root=/tmp/base-http-fixed-rate-20261028
mkdir "$root"
mkdir -p "$root/client/src" "$root/binaries"
cp /tmp/base-http-owned-receive-20261028/binaries/candidate "$root/binaries/server"
cp /tmp/base-http-owned-receive-20261028/expected-binaries.sha256 "$root/previous-binaries.sha256"
mpstat -P ALL 1 3 > "$root/initial-cpu.txt"
uptime > "$root/initial-load.txt"
