#!/usr/bin/env python3
"""Validate exact scheduled cohorts and summarize equal offered-load latency."""
import hashlib,json,statistics
from pathlib import Path

root=Path(__file__).resolve().parent
folder=root/'comparison'
rows=[json.loads(line) for line in (folder/'samples.jsonl').read_text().splitlines()]
expected={(v,r,rep) for v in ('borrowed','owned','tokio') for r in (200000,220000,240000) for rep in range(1,6)}
keys=[(r['variant'],r['rate'],r['repetition']) for r in rows]
assert len(rows)==45 and len(keys)==len(set(keys)) and set(keys)==expected
for row in rows:
    target=row['rate']*row['seconds']
    assert row['seconds']==8 and row['warmup_seconds']==2 and row['connections']==64
    assert row['measured_responses_validated']==row['planned_measured_requests']==target
    assert row['total_responses_validated']==10*row['rate'] and row['payload_validation_errors']==0
    assert sum(row['completed_per_second'])==row['completed_in_window']
    assert sum(h['count'] for h in row['per_second_scheduled_latency'])==target
    assert all(h['count']==row['rate'] for h in row['per_second_scheduled_latency'])
    for key in ('scheduled_latency','wire_rtt','send_delay','dispatch_lag'):
        assert row[key]['count']==target
        assert 0<row[key]['p50_us']<=row[key]['p95_us']<=row[key]['p99_us']<=row[key]['p999_us']<=row[key]['max_us']
    # HdrHistogram quantization allows small rounding differences between means.
    residual=abs(row['scheduled_latency']['mean_us']-row['wire_rtt']['mean_us']-row['send_delay']['mean_us'])
    assert residual<max(0.5,0.003*row['scheduled_latency']['mean_us']),residual
    stem=f"{row['variant']}-{row['rate']}-r{row['repetition']}"
    assert not (folder/f'{stem}.client.stderr').read_text()
    assert not (folder/f'{stem}.server.stderr').read_text()
    raw=json.loads((folder/f'{stem}.client.json').read_text())
    for k,v in raw.items():assert row[k]==v
summary=json.loads((folder/'summary.json').read_text())
for item in summary:
    for variant in ('borrowed','owned','tokio'):
        group=[r for r in rows if r['rate']==item['rate'] and r['variant']==variant]
        assert item['medians'][variant]['scheduled_latency']['p99_us']==statistics.median(r['scheduled_latency']['p99_us'] for r in group)
    for baseline in ('borrowed','tokio'):
        a=sorted([r for r in rows if r['rate']==item['rate'] and r['variant']=='owned'],key=lambda r:r['repetition'])
        b=sorted([r for r in rows if r['rate']==item['rate'] and r['variant']==baseline],key=lambda r:r['repetition'])
        paired=[100*(x['scheduled_latency']['p99_us']/y['scheduled_latency']['p99_us']-1) for x,y in zip(a,b)]
        assert item['paired']['owned_vs_'+baseline]['scheduled_p99_percent']['values']==paired
lines=['# Equal offered-rate raw server latency — 2026-10-28','','All 45 samples validate. Each cell is the median of five per-run p99 values, after merging all client workers within each run. Scheduled latency includes waiting for the connection and client dispatch.','','| Offered requests/s | Borrowed p99 ms | Owned p99 ms | Tokio p99 ms |','| ---: | ---: | ---: | ---: |']
for item in summary:
    lines.append(f"| {item['rate']:,} | "+' | '.join(f"{item['medians'][v]['scheduled_latency']['p99_us']/1000:.3f}" for v in ('borrowed','owned','tokio'))+' |')
lines+=['','| Rate | Variant | Scheduled p99 range ms | RTT p99 µs | Dispatch p99 µs | Completed/s median | Delivery fraction range |','| ---: | --- | ---: | ---: | ---: | ---: | ---: |']
for item in summary:
    for variant in ('borrowed','owned','tokio'):
        m=item['medians'][variant]
        g=[r for r in rows if r['rate']==item['rate'] and r['variant']==variant]
        q=[r['scheduled_latency']['p99_us']/1000 for r in g]
        lines.append(f"| {item['rate']:,} | {variant} | {min(q):.3f}–{max(q):.3f} | {m['wire_rtt']['p99_us']:.2f} | {m['dispatch_lag']['p99_us']:.2f} | {m['achieved_rate']:,.1f} | {100*m['target_delivery_min']:.4f}%–{100*m['target_delivery_max']:.4f}% |")
lines+=['','| Rate | Owned comparison | Paired scheduled p99 change | Pair range | Lower p99 rounds |','| ---: | --- | ---: | ---: | ---: |']
for item in summary:
    for baseline in ('borrowed','tokio'):
        p=item['paired']['owned_vs_'+baseline]['scheduled_p99_percent']
        lines.append(f"| {item['rate']:,} | vs {baseline} | {p['median']:+.2f}% | {p['min']:+.2f}%–{p['max']:+.2f}% | {sum(v<0 for v in p['values'])}/5 |")
(root/'tables.md').write_text('\n'.join(lines)+'\n')
verification={'samples':len(rows),'expected_samples_present':True,'all_response_bytes_validated':True,'scheduled_measured_requests':sum(r['planned_measured_requests'] for r in rows),'total_validated_requests_including_warmup':sum(r['total_responses_validated'] for r in rows),'all_clients_and_servers_stderr_empty':True,'all_scheduled_cohorts_completed_without_dropping_late_requests':True,'delivery_within_half_percent_samples':sum(r['target_delivery_within_half_percent'] for r in rows),'delivery_fraction_min':min(r['target_delivery_fraction'] for r in rows),'delivery_fraction_max':max(r['target_delivery_fraction'] for r in rows),'largest_dispatch_p99_us':max(r['dispatch_lag']['p99_us'] for r in rows),'largest_dispatch_p999_us':max(r['dispatch_lag']['p999_us'] for r in rows),'max_drain_ms':max(r['drain_ms'] for r in rows)}
(root/'verification.json').write_text(json.dumps(verification,indent=2)+'\n')
print('\n'.join(lines));print(json.dumps(verification,indent=2))
