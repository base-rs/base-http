import json, sys
d = json.load(open("/tmp/cperf.json")); n = d["requests"] + d.get("warmup_requests", 0)
p = {}
for l in open("/tmp/cperf.perf"):
    f = l.strip().split(",")
    if len(f) > 3 and f[0] and f[0][0].isdigit():
        p[f[2].removesuffix(":u")] = float(f[0])
alloc = {k: round(v, 2) if isinstance(v, float) else v for k, v in d.items() if "alloc" in k}
ev = " ".join(f"{k}={v / n:.1f}" for k, v in p.items())
print(f"{sys.argv[1]:16} c{sys.argv[2]:3} rps={d['requests_per_second']:7.0f} {ev} {alloc}")
