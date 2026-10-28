set -euo pipefail
cd /tmp/base-http-task-topology-20261028
sha256sum binaries/topology binaries/client wrk/wrk > production-binaries.sha256
tar --exclude='*/target' --exclude='*.o' -czf production-source.tar.gz http scheduler wrk
bash diagnostic-build.sh
python3 summarize-diagnostics.py > diagnostic-analysis.log
