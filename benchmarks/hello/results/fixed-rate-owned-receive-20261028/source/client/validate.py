#!/usr/bin/env python3
"""Check payload rejection and that a server stall stays in scheduled latency."""
import argparse,json,socket,subprocess,threading,time
from pathlib import Path

RESPONSE=b'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, world!'
p=argparse.ArgumentParser();p.add_argument('--client',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
a.output.mkdir(exist_ok=False)
results=[]
for mode,delay,corrupt,connections,cpus in [('healthy',0,False,4,'2,3'),('stall',0.04,False,1,'2'),('corrupt',0,True,1,'2')]:
    listener=socket.socket();listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);listener.bind(('127.0.0.1',0));listener.listen();listener.settimeout(10)
    host,port=listener.getsockname();threads=[];errors=[]
    def serve(connection):
        try:
            with connection:
                connection.settimeout(10);pending=b'';first=True
                while True:
                    data=connection.recv(4096)
                    if not data:break
                    pending+=data
                    while b'\r\n\r\n' in pending:
                        request,pending=pending.split(b'\r\n\r\n',1)
                        assert request==f'GET / HTTP/1.1\r\nHost: {host}:{port}'.encode(),request
                        if first:time.sleep(delay);first=False
                        connection.sendall(RESPONSE[:-1]+b'?' if corrupt else RESPONSE)
        except Exception as e:errors.append(repr(e))
    def accept():
        for _ in range(connections):
            connection,_=listener.accept();t=threading.Thread(target=serve,args=(connection,));t.start();threads.append(t)
    accepting=threading.Thread(target=accept);accepting.start()
    client=subprocess.run([str(a.client.resolve()),f'{host}:{port}',str(connections),'1000','0','1',cpus],capture_output=True,text=True,timeout=20)
    accepting.join(5)
    for thread in threads:thread.join(5)
    listener.close()
    (a.output/f'{mode}.stdout').write_text(client.stdout);(a.output/f'{mode}.stderr').write_text(client.stderr)
    assert not errors,errors
    if corrupt:
        assert client.returncode!=0 and 'fixed response mismatch' in client.stderr,client.stderr
        results.append({'case':mode,'payload_mismatch_rejected':True})
    else:
        assert client.returncode==0,client.stderr
        row=json.loads(client.stdout)
        assert row['measured_responses_validated']==1000 and row['total_responses_validated']==1000
        if delay:
            assert row['scheduled_latency']['max_us']>=35000,row
            assert row['send_delay']['max_us']>=30000,row
            assert row['scheduled_latency']['p99_us']>=20000,row
        results.append({'case':mode,'all_scheduled_requests_validated':True,'scheduled_p99_us':row['scheduled_latency']['p99_us'],'send_delay_max_us':row['send_delay']['max_us']})
(a.output/'summary.json').write_text(json.dumps(results,indent=2)+'\n');print(json.dumps(results,indent=2))
