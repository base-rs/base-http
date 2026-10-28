for sample in {1..10}; do
netstat -anv -p tcp > /tmp/base-http-tcp-resource-snapshot.txt
netstat -m > /tmp/base-http-mbuf-resource-snapshot.txt
sysctl kern.num_files > /tmp/base-http-file-resource-snapshot.txt
python3 benchmarks/hello/results/http-hot-path/sample_socket_resources.py
sleep 3
done
