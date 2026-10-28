set -euo pipefail
cd /tmp/base-http-owned-receive-20261028
sha256sum --check expected-binaries.sha256 > preserved-binary-verification.log
tar -czf source.tar.gz -C source http before
tar -czf diagnostics.tar.gz trace-initial-worker-name trace-borrowed trace-owned profile-borrowed profile-owned
mkdir -p retained-binaries
gzip -c binaries/candidate > retained-binaries/candidate.gz
sha256sum source.tar.gz diagnostics.tar.gz retained-binaries/candidate.gz > archive-sha256.txt
