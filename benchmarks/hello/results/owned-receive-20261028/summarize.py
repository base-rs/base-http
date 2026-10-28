#!/usr/bin/env python3
"""Validate and summarize the owned-receive control without dropping samples."""
import hashlib,json,statistics
from pathlib import Path

root=Path(__file__).resolve().parent
cohort=root/'comparison'
rows=[json.loads(line) for line in (cohort/'samples.jsonl').read_text().splitlines()]
expected={(name,c,r) for name in ('borrowed','owned','frozen','tokio') for c in (1,16,64) for r in range(1,6)}
actual=[(row['variant'],row['concurrency'],row['repetition']) for row in rows]
assert len(actual)==len(expected)==60 and set(actual)==expected
teardown={}
for row in rows:
    assert row['errors']==0 and row['requests']>0
    assert len(row['validation'])==3
    assert all(v['status']==200 and v['body']=='Hello, world!' and v['fields']=={'content-type':'text/plain','content-length':'13'} for v in row['validation'])
    stem=f"{row['backend']}-c{row['concurrency']}-r{row['repetition']}"
    folder=cohort/row['variant']
    for suffix in ('wrk.txt','warmup.txt'):
        text=(folder/f'{stem}.{suffix}').read_text()
        assert 'Socket errors' not in text and 'Non-2xx' not in text
    stderr=(folder/f'{stem}.server.stderr').read_text()
    errors=stderr.splitlines()
    allowed=('connection: Connection reset by peer (os error 104)','connection: Broken pipe (os error 32)')
    partial=None
    if errors and not stderr.endswith('\n') and errors[-1] not in allowed:
        partial=errors.pop()
        assert any(line.startswith(partial) for line in allowed),stderr
    assert all(line in allowed for line in errors),stderr
    assert len(errors)+(partial is not None)<=2*row['concurrency'],stderr
    teardown[str((folder/f'{stem}.server.stderr').relative_to(root))]={'complete_lines':len(errors),'partial_final_line':partial}
summary=json.loads((cohort/'summary.json').read_text())
metrics=('requests_per_second','total_cpu_us_per_request','latency_p99_ms','server_rss_kib_after')
for item in summary:
    c=item['concurrency']
    for name in ('borrowed','owned','frozen','tokio'):
        group=sorted((row for row in rows if row['variant']==name and row['concurrency']==c),key=lambda row:row['repetition'])
        for metric in metrics:assert item['medians'][name][metric]==statistics.median(row[metric] for row in group)
    for baseline in ('borrowed','frozen','tokio'):
        a=sorted((row for row in rows if row['variant']=='owned' and row['concurrency']==c),key=lambda row:row['repetition'])
        b=sorted((row for row in rows if row['variant']==baseline and row['concurrency']==c),key=lambda row:row['repetition'])
        for metric in metrics:
            values=[100*(x[metric]/y[metric]-1) for x,y in zip(a,b)]
            assert item['paired']['owned_vs_'+baseline][metric]['values_percent']==values
lines=['# Owned receive raw HTTP control — 2026-10-28','','All 60 uninstrumented samples validate. Rates and latency below are medians of five rounds. Paired changes are medians of within-round ratios, with their complete ranges.','','| Connections | Variant | Requests/s | CPU µs/request | p99 ms | RSS KiB after |','| ---: | --- | ---: | ---: | ---: | ---: |']
for item in summary:
    for name in ('borrowed','owned','frozen','tokio'):
        m=item['medians'][name]
        lines.append(f"| {item['concurrency']} | {name} | {m['requests_per_second']:,.0f} | {m['total_cpu_us_per_request']:.3f} | {m['latency_p99_ms']:.3f} | {m['server_rss_kib_after']:,.0f} |")
lines+=['','| Connections | Owned comparison | Throughput change [range] | CPU/request change [range] | p99 change [range] |','| ---: | --- | ---: | ---: | ---: |']
for item in summary:
    for baseline in ('borrowed','tokio'):
        cells=[]
        for metric in metrics[:3]:
            m=item['paired']['owned_vs_'+baseline][metric]
            cells.append(f"{m['median_percent']:+.2f}% [{m['min_percent']:+.2f}, {m['max_percent']:+.2f}]")
        lines.append(f"| {item['concurrency']} | vs {baseline} | "+' | '.join(cells)+' |')
(root/'tables.md').write_text('\n'.join(lines)+'\n')
(root/'verification.json').write_text(json.dumps({'samples':len(rows),'all_expected_samples_present':True,'all_wrk_outputs_pass':True,'preflight_http_checks_pass':True,'teardown_peer_errors':teardown,'total_complete_teardown_peer_errors':sum(v['complete_lines'] for v in teardown.values()),'partial_final_teardown_lines':sum(v['partial_final_line'] is not None for v in teardown.values())},indent=2)+'\n')
print('\n'.join(lines))
