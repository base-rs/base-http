set -euo pipefail
cd /tmp/base-http-receive-lifecycle-20261028
cleanup() {
 sudo -n perf probe --del 'base_http_lifecycle:*' > probe-cleanup.log 2>&1
 sudo -n perf probe --list > probes-after-cleanup.txt
}
trap cleanup EXIT
sudo -n perf probe --list > probe-definitions.txt
for event in /sys/kernel/tracing/events/base_http_lifecycle/{tcp_recvmsg,tcp_recvmsg_return32__return}/format /sys/kernel/tracing/events/io_uring/io_uring_{submit_req,poll_arm,complete,task_add,local_work_run,queue_async_work}/format; do
 name=$(basename "$(dirname "$event")")
 sudo -n cat "$event" > "format-$name.txt"
done
cat /proc/loadavg > clean-initial-load.txt
ps -eo pid,pcpu,args --sort=-pcpu > clean-initial-processes.txt
python3 clean.py --mode counts --repetitions 1 > clean.log 2>&1
sudo -n cat /sys/kernel/tracing/kprobe_profile | grep base_http_lifecycle > kprobe-profile.txt
objdump -d --start-address=0xa08c0 --stop-address=0xa08f3 /usr/lib/libc.so.6 > libc-syscall-disassembly.txt
