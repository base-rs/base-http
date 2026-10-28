from pathlib import Path
import json, subprocess, tempfile, time, signal, shutil
root=Path('/Users/samuel/Developer/base-rs/base-http/benchmarks/client/results/exchange-reuse-profiles-20261029'); root.mkdir(exist_ok=False)
new='/tmp/base-http-exchange-reuse-atomic-20261029'
with tempfile.TemporaryDirectory(prefix='base-http-profile-') as temp:
 subprocess.run([new,'identity','--identity',temp],check=True)
 shutil.copyfile(Path(temp)/'certificate.der',root/'certificate.der')
 for label,binary in [('before','/tmp/base-http-exchange-compact-20261029'),('atomic',new)]:
  server=subprocess.Popen([new,'server','--identity',temp,'--tls'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  ready=server.stdout.readline().strip();assert ready.startswith('READY '),ready
  command=[binary,'client','--implementation','base-open','--address',ready[6:],'--identity',temp,'--tls','--concurrency','32','--seconds','6','--warmup','0.5']
  client=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  try:
   time.sleep(1)
   sample=subprocess.run(['/usr/bin/sample',str(client.pid),'3','1','-mayDie','-file',str(root/f'{label}.sample.txt')],capture_output=True,text=True)
   (root/f'{label}.sample.log').write_text(sample.stdout+sample.stderr)
   out,err=client.communicate(timeout=15);assert client.returncode==0,err
   result=json.loads(out);(root/f'{label}.client.json').write_text(json.dumps(result,indent=2)+'\n')
  finally:
   if client.poll() is None:client.kill();client.wait()
   server.send_signal(signal.SIGTERM);out,err=server.communicate(timeout=10)
   (root/f'{label}.server.txt').write_text(out+err)
  print(label,'sample exit',sample.returncode,'responses',result['requests'],flush=True)
