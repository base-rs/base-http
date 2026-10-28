#!/usr/bin/env python3
"""Controlled loopback urgent-data test against the frozen benchmark server."""
import json,os,signal,socket,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent

def read_response(peer):
    reply=b''
    while not reply.endswith(b'Hello, world!'):
        chunk=peer.recv(4096)
        if not chunk:
            return reply
        reply+=chunk
    return reply

for backend in ['base-native','base-iouring']:
    for urgent in [False,True]:
        process=subprocess.Popen(['taskset','-c','0',str(root/'binaries/oob-default'),backend],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            address=process.stdout.readline().strip()
            host,port=address.rsplit(':',1)
            with socket.create_connection((host,int(port)),timeout=1) as peer:
                peer.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                peer.sendall(b'GET /prime HTTP/1.1\r\nHost: localhost\r\n\r\n')
                assert read_response(peer).startswith(b'HTTP/1.1 200')
                time.sleep(.02)
                os.kill(process.pid,signal.SIGSTOP)
                peer.sendall(b'GET /target HTTP/1.1\r\nHost: localhost\r\n')
                if urgent:
                    assert peer.send(b'!',socket.MSG_OOB)==1
                peer.sendall(b'\r\n')
                time.sleep(.02)
                start=time.monotonic()
                os.kill(process.pid,signal.SIGCONT)
                try:
                    response=read_response(peer)
                    result='response' if response.endswith(b'Hello, world!') else 'closed'
                except ConnectionResetError:
                    result='reset'
                elapsed=time.monotonic()-start
                assert result in (['closed','reset'] if backend=='base-native' and urgent else ['response']), (backend,urgent,result)
                print(json.dumps(dict(backend=backend,urgent=urgent,result=result,elapsed_seconds=elapsed)),flush=True)
        finally:
            os.kill(process.pid,signal.SIGCONT)
            process.terminate()
            try:
                process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill();process.communicate()
