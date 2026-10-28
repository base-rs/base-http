#!/usr/bin/env python3
"""Render validated repeat tables without pooling the previous cohort."""
from pathlib import Path
import json
root=Path(__file__).resolve().parent
current=json.loads((root/'validated-summary.json').read_text())
prior=json.loads((root.parent/'ring-overhead-20261027/validated-summary.json').read_text())
labels={'mini-epoll':'Minimal epoll','mini-uring':'Minimal cooperative io_uring','mini-uring-defer':'Minimal deferred io_uring','native-poll':'Base epoll','iouring-poll':'Base borrowed, cooperative','iouring-deferred':'Base borrowed, deferred','tokio':'Tokio','iouring':'Base owned, cooperative','iouring-owned-deferred':'Base owned, deferred'}
def rate(value):return f'{value:,.0f}'
def percent(value):return f'{value:+.2f}%'
def paired(item,pair,metric='requests_per_second'):
 d=item['paired'][pair][metric]
 return f"{percent(d['median_percent'])} ({percent(d['min_percent'])} to {percent(d['max_percent'])})"
lines=['# Full comparison repeat — Hana, 2026-10-28','',
'Fresh output directories, the same frozen executables and harness, and the same pinned CPU placement as the [original completion-policy comparison](../../ring-overhead/readme.md). Candidate scheduler commit `77fe0f8c9082391d7178f720f3f2d6f08c82eaff`, baseline `587e74620d1dc1468a588f02b7ea1fd37b722db7`. No implementation was rebuilt or changed.',
'',
'All rates below are medians. Comparisons use the median of within-round percentage changes; they need not equal ratios of the displayed medians. Parentheses give the complete observed paired range, not a confidence interval.',
'']
for cohort,title in [('confirmation','Concurrent echo: 64 connections'),('single','Single-connection echo')]:
 entries=current[cohort]['results'];by_size={r['bytes']:r for r in entries}
 lines+=['## '+title,'',f"{current[cohort]['samples']} samples; {5 if cohort=='confirmation' else 3} rounds, 1-second warmup and 4-second timed window. Every echo payload validates and stderr is empty.",'','| Server | 64 B echoes/s | 4 KiB echoes/s |','| --- | ---: | ---: |']
 for variant in entries[0]['medians']:
  lines.append('| '+labels[variant]+' | '+' | '.join(rate(by_size[b]['medians'][variant]['requests_per_second']) for b in [64,4096])+' |')
 pairs=['iouring-deferred_vs_iouring-poll','iouring-deferred_vs_native-poll','iouring-deferred_vs_tokio','iouring-owned-deferred_vs_iouring','iouring-owned-deferred_vs_tokio','native-poll_vs_tokio']
 if cohort=='confirmation':pairs+=['mini-uring_vs_mini-epoll','mini-uring-defer_vs_mini-epoll']
 lines+=['','| Throughput comparison | 64 B paired change (range) | 4 KiB paired change (range) |','| --- | ---: | ---: |']
 for pair in pairs:
  v,ref=pair.split('_vs_');lines.append('| '+labels[v]+' vs '+labels[ref]+' | '+' | '.join(paired(by_size[b],pair) for b in [64,4096])+' |')
 lines+=['','| Deferred borrowed p99 RTT | 64 B paired change (range) | 4 KiB paired change (range) |','| --- | ---: | ---: |']
 for pair in ['iouring-deferred_vs_iouring-poll','iouring-deferred_vs_tokio']:
  v,ref=pair.split('_vs_');lines.append('| vs '+labels[ref]+' | '+' | '.join(paired(by_size[b],pair,'rtt_p99_us') for b in [64,4096])+' |')
 lines+=['']
for cohort,title,control in [('http','Full HTTP','hyper'),('http-raw','Raw fixed response','tokio')]:
 item=current[cohort]['results'][0]
 names={'before':'Base io_uring, cooperative','after':'Base io_uring, deferred','epoll':'Base epoll','hyper':'Hyper/Tokio, runtime root','tokio':'Tokio, spawned connection tasks'}
 lines+=['## '+title,'','20 samples; five rounds, 64 persistent connections, 1-second warmup and 4-second timed window. All response checks pass; wrk reports no socket/status errors. Expected reset logs at forced teardown are retained and counted separately.','','| Server | Requests/s | Timed CPU µs/request | p99 latency ms |','| --- | ---: | ---: | ---: |']
 for variant,m in item['medians'].items():
  lines.append(f"| {names[variant]} | {rate(m['requests_per_second'])} | {m['total_cpu_us_per_request']:.3f} | {m['latency_p99_ms']:.3f} |")
 lines+=['','| Deferred Base vs | Throughput change (range) | CPU/request change (range) | p99 change (range) |','| --- | ---: | ---: | ---: |']
 for ref in ['before','epoll',control]:
  pair='after_vs_'+ref;lines.append('| '+names[ref]+' | '+paired(item,pair)+' | '+paired(item,pair,'total_cpu_us_per_request')+' | '+paired(item,pair,'latency_p99_ms')+' |')
 lines+=['']
lines+=['## Repetition against the previous cohort','','These are separate measurements of identical executables. Do not pool their absolute rates or substitute one workload for another.','','| Paired throughput comparison | Previous | Repeat |','| --- | ---: | ---: |']
for b in [64,4096]:
 for pair in ['iouring-deferred_vs_iouring-poll','iouring-deferred_vs_tokio','iouring-owned-deferred_vs_tokio']:
  old=next(r for r in prior['confirmation']['results'] if r['bytes']==b);new=next(r for r in current['confirmation']['results'] if r['bytes']==b)
  lines.append(f"| {b} B {pair} | {percent(old['paired'][pair]['requests_per_second']['median_percent'])} | {percent(new['paired'][pair]['requests_per_second']['median_percent'])} |")
for c,pair in [('http','after_vs_before'),('http','after_vs_hyper'),('http-raw','after_vs_before'),('http-raw','after_vs_tokio')]:
 lines.append(f"| {c} {pair} | {percent(prior[c]['results'][0]['paired'][pair]['requests_per_second']['median_percent'])} | {percent(current[c]['results'][0]['paired'][pair]['requests_per_second']['median_percent'])} |")
lines+=['','## Method and limits','','Hana was idle at preflight (about 99% aggregate idle). Server CPU 0, SMT sibling CPU 4 unused, load generator CPUs 2,3,6,7, controller CPU 1. Linux 7.2.11 on Xeon E3-1240 v6, the same Rust-built binaries and Tokio 1.54.1 as before. No builds, profiling or heavy analysis ran concurrently with throughput.',
'',
'Owned-buffer and borrowed-buffer servers use different execution models. Minimal event loops contain no scheduler tasks. Full Hyper uses the existing runtime-root layout while the raw Tokio control uses spawned connection tasks. Keep those distinctions when interpreting comparisons.',
'',
'Echo CPU accounting covers the whole process lifetime, including setup/warmup/cleanup, so it is not timed CPU/request. HTTP CPU/request uses the timed window. The prior off-host Wi-Fi check is not repeated: it did not saturate Hana or establish maximum scaling. This repeat measures loopback echo and static HTTP, not general production capacity.',
'',
'## Retained evidence','','`repeat.sh` contains exact commands. `provenance.json`, `expected-binaries.sha256`, initial/final host/socket state, raw client/wrk output, environment JSON and logs are retained. The binary checks run before and after the matrix. `validated-summary.json` checks complete matrices, raw echo JSON, successful response probes and stderr/error expectations. The frozen executables and source archives remain in the original evidence directory, referenced by hash. `sha256.json` hashes the repeat files.',
'',
'Revalidate from the repository root with `python3 benchmarks/echo/ring-overhead/summarize.py benchmarks/echo/results/ring-overhead-repeat-20261028 --http-cohorts http http-raw`. Run this `report.py` after validation to regenerate the tables.','']
# The interpretation is written separately after examining the full validated results.
(root/'tables.md').write_text('\n'.join(lines))
print('Wrote',root/'tables.md')
