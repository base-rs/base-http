"""Count exported libc copy calls on matched validated sequential requests."""
from pathlib import Path
import argparse,hashlib,json,mmap,os,select,socket,struct,subprocess,sys,tempfile,time
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'diagnostics'))
from count_socket_calls import call
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--variants',type=Path,required=True)
p.add_argument('--library',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--requests',type=int,default=10000)
a=p.parse_args()
variants=json.loads(a.variants.read_text());rows=[]
with tempfile.TemporaryDirectory(prefix='base-http-memory-') as directory:
 for name,variant in variants.items():
  binary=Path(variant['binary']);counter=Path(directory)/name
  environment=dict(os.environ,DYLD_INSERT_LIBRARIES=str(a.library.resolve()),BASE_HTTP_MEMORY_COUNTS=str(counter))
  server=subprocess.Popen([str(binary),variant['mode']],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=environment)
  try:
   assert select.select([server.stdout],[],[],10)[0],'server readiness timeout'
   address=server.stdout.readline().decode().strip()
   if not address:raise RuntimeError(server.stderr.read().decode())
   host,port=address.rsplit(':',1)
   with counter.open('r+b') as f,mmap.mmap(f.fileno(),0) as memory,socket.create_connection((host,int(port)),timeout=5) as connection:
    connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    request=f'GET / HTTP/1.1\r\nHost: {address}\r\n\r\n'.encode()
    for _ in range(100):call(connection,request)
    time.sleep(.02);before=struct.unpack('=4Q',memory[:])
    for _ in range(a.requests):call(connection,request)
    time.sleep(.02);after=struct.unpack('=4Q',memory[:])
    counts=[b-a for a,b in zip(before,after)]
   row={'variant':name,'requests':a.requests,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'memcpy_calls':counts[0],'memcpy_bytes':counts[1],'memmove_calls':counts[2],'memmove_bytes':counts[3],'exported_copy_calls_per_request':(counts[0]+counts[2])/a.requests,'exported_copy_bytes_per_request':(counts[1]+counts[3])/a.requests}
   rows.append(row);print(json.dumps(row),flush=True)
  finally:
   server.terminate();_,error=server.communicate(timeout=10)
   if rows and rows[-1]['variant']==name:rows[-1]['server_stderr']=error.decode(errors='replace')
a.output.write_text(json.dumps({'diagnostic_only':True,'limitations':'Counts exported libc memcpy/memmove calls, not inlined loads/stores or kernel copies. Copy implementations may delegate internally; results are comparative diagnostic counts, not timings.','library_sha256':hashlib.sha256(a.library.read_bytes()).hexdigest(),'results':rows},indent=2)+'\n')
