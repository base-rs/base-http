import collections, json, sys
from pathlib import Path
r=Path(__file__).resolve().parent
sys.path.insert(0,str(r/"sources/base-http/benchmarks/hello"))
import run
out=r/"results/confirmation"
rows=[json.loads(line) for line in (out/"samples.jsonl").read_text().splitlines()]
assert len(rows)==75, len(rows)
assert len({(x["variant"],x["concurrency"],x["repetition"]) for x in rows})==75
assert all(x["errors"]==0 and len(x["validation"])==3 for x in rows)
logs=sorted(list(out.rglob("*.wrk.txt"))+list(out.rglob("*.warmup.txt")))
assert len(logs)==150, len(logs)
for p in logs:run.parse_wrk(p.read_text())
stderr=collections.Counter(line for p in out.rglob("*.server.stderr") for line in p.read_text().splitlines() if line)
expected="connection: Connection reset by peer (os error 104)"
unexpected={line:n for line,n in stderr.items() if not expected.startswith(line)}
assert not unexpected, unexpected
result={"samples":len(rows),"preflight_exchanges":sum(len(x["validation"]) for x in rows),"wrk_logs_reparsed":len(logs),"wrk_validation_passed":True,"server_stderr":dict(stderr),"unexpected_server_stderr":unexpected}
(r/"validation.json").write_text(json.dumps(result,indent=2)+"\n")
print(json.dumps(result,indent=2))
