import json, pathlib, subprocess, tempfile, signal
root=pathlib.Path('/Users/samuel/Developer/base-rs/base-http/benchmarks/client/results/tls-demand-profiles-20261028')
binary='/tmp/base-http-tls-demand-io-20261028'
root.mkdir(exist_ok=False)
with tempfile.TemporaryDirectory(prefix='base-http-profile-identity-') as identity:
 subprocess.run([binary,'identity','--identity',identity],check=True)
 for tls,impl,c in [(True,'base-open',32),(True,'hyper',32),(True,'base-connection',32),(True,'hyper-connection',32)]:
  label=f'{"tls" if tls else "tcp"}-{impl}-c{c}'
  suffix=['--tls'] if tls else []
  server=subprocess.Popen([binary,'server','--identity',identity,*suffix],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  client=None
  try:
   address=server.stdout.readline().strip().removeprefix('READY ')
   client=subprocess.Popen([binary,'client','--identity',identity,'--address',address,'--implementation',impl,'--concurrency',str(c),'--seconds','9','--warmup','0.5',*suffix],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
   sampled=subprocess.run(['sample',str(client.pid),'6','1','-file',str(root/f'{label}.sample.txt')],capture_output=True,text=True,timeout=15)
   (root/f'{label}.sample.stderr').write_text(sampled.stdout+sampled.stderr)
   out,err=client.communicate(timeout=20)
   if client.returncode: raise RuntimeError(err)
   result=json.loads(out); result['profiled']=True; result['sample_exit_code']=sampled.returncode
   (root/f'{label}.json').write_text(json.dumps(result,indent=2)+'\n')
   print(label, 'profile_exit',sampled.returncode, 'requests',result['requests'],flush=True)
  finally:
   if client is not None and client.poll() is None:
    client.kill();client.communicate()
   server.send_signal(signal.SIGTERM);out,err=server.communicate(timeout=10)
   (root/f'{label}.server.stdout').write_text(out)
   (root/f'{label}.server.stderr').write_text(err)
