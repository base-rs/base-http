import json, sys, hashlib
from pathlib import Path
r=Path(__file__).resolve().parent
graphs={n:json.loads((r/f"graphs/{n}.json").read_text()) for n in ["baseline","updated"]}
checks={}
for n,g in graphs.items():
    packages=[p for p in g["packages"] if p["name"].startswith("base-scheduler")]
    expected="0.1.1" if n=="baseline" else "0.1.2"
    assert len(packages)==7, [(p["name"],p["version"]) for p in packages]
    assert all(p["version"]==expected and p["source"] and p["source"].startswith("registry+") for p in packages)
    checks[n]={p["name"]:{"version":p["version"],"source":p["source"]} for p in packages}
def dependencies(g):
    return sorted((p["name"],p["version"],p["source"]) for p in g["packages"] if not p["name"].startswith("base-scheduler"))
assert dependencies(graphs["baseline"])==dependencies(graphs["updated"])
def features(g):
    names={p["id"]:(p["name"],p["version"]) for p in g["packages"]}
    return {names[n["id"]]:n["features"] for n in g["resolve"]["nodes"] if not names[n["id"]][0].startswith("base-scheduler")}
assert features(graphs["baseline"])==features(graphs["updated"])
checks["same_non_scheduler_dependencies_and_features"]=True
checks["library_source_sha256"]={}
for name in ["base-http","base-http1","base-http-server"]:
    for p in sorted((r/"sources"/name/"src").rglob("*.rs")):
        relative=p.relative_to(r/"sources")
        assert p.read_bytes()==(r/"baseline-sources"/relative).read_bytes()
        checks["library_source_sha256"][str(relative)]=hashlib.sha256(p.read_bytes()).hexdigest()
assert (r/"sources/base-http/benchmarks/hello/src/main.rs").read_bytes()==(r/"baseline-sources/base-http/benchmarks/hello/src/main.rs").read_bytes()
(r/"graph-verification.json").write_text(json.dumps(checks,indent=2)+"\n")
print("Verified: all seven scheduler crates match their registry cohort; other dependency versions/features and HTTP sources are identical.")
