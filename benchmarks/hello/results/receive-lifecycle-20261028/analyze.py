"""Replay receive-lifecycle evidence without running benchmarks."""
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def paired(values, controls):
    changes = [100 * (value / control - 1) for value, control in zip(values, controls)]
    return dict(median=statistics.median(changes), minimum=min(changes),
                maximum=max(changes), values=changes)


def main():
    result = {}
    unpaced = rows(ROOT / 'unpaced/samples.jsonl')
    fixed = rows(ROOT / 'fixed/samples.jsonl')
    assert len(unpaced) == len(fixed) == 30
    variants = ('owned', 'poll-first', 'tokio')
    result['unpaced'] = {}
    for connections in (1, 64):
        groups = {name: sorted((r for r in unpaced if r['concurrency'] == connections
                               and r['variant'] == name), key=lambda r: r['repetition'])
                  for name in variants}
        metrics = ('requests_per_second', 'total_cpu_us_per_request',
                   'user_cpu_us_per_request', 'system_cpu_us_per_request',
                   'latency_p99_ms', 'server_cpu_percent_one_core')
        assert all([r['repetition'] for r in group] == [1, 2, 3, 4, 5]
                   for group in groups.values())
        result['unpaced'][connections] = {
            'medians': {name: {m: statistics.median(r[m] for r in group)
                               for m in metrics} for name, group in groups.items()},
            'paired_percent': {name + '_vs_' + control: {
                m: paired([r[m] for r in groups[name]], [r[m] for r in groups[control]])
                for m in metrics} for name in variants for control in variants
                if name != control}}
    assert all(r['errors'] == 0 and len(r['validation']) == 3 for r in unpaced)
    result['fixed'] = {}
    for rate in (200000, 240000):
        groups = {name: sorted((r for r in fixed if r['rate'] == rate and r['variant'] == name),
                              key=lambda r: r['repetition']) for name in variants}
        assert all([r['repetition'] for r in group] == [1, 2, 3, 4, 5]
                   for group in groups.values())
        result['fixed'][rate] = {
            'medians': {name: {key: {metric: statistics.median(r[key][metric] for r in group)
                                    for metric in ('p50_us', 'p99_us', 'mean_us')}
                               for key in ('wire_rtt', 'scheduled_latency', 'dispatch_lag')}
                        for name, group in groups.items()},
            'paired_percent': {name + '_vs_' + control: {
                key + '.' + metric: paired([r[key][metric] for r in groups[name]],
                                          [r[key][metric] for r in groups[control]])
                for key in ('wire_rtt', 'scheduled_latency')
                for metric in ('p50_us', 'p99_us', 'mean_us')}
                for name in variants for control in variants if name != control}}
    for row in fixed:
        assert row['payload_validation_errors'] == 0
        assert row['total_responses_validated'] == row['rate'] * (row['warmup_seconds'] + row['seconds'])
        assert row['measured_responses_validated'] == row['planned_measured_requests'] == row['rate'] * row['seconds']
        for key in ('wire_rtt', 'scheduled_latency', 'send_delay', 'dispatch_lag'):
            assert row[key]['count'] == row['measured_responses_validated']
    assert all(not path.read_text() for path in (ROOT / 'fixed').glob('*.stderr'))
    result['validation'] = dict(
        unpaced_samples=len(unpaced), fixed_samples=len(fixed),
        fixed_responses=sum(r['total_responses_validated'] for r in fixed),
        fixed_measured_responses=sum(r['measured_responses_validated'] for r in fixed),
        maximum_completion_rate_deviation_percent=max(abs(r['achieved_rate']/r['rate'] - 1)*100 for r in fixed),
        maximum_send_rate_deviation_percent=max(abs(r['send_rate']/r['rate'] - 1)*100 for r in fixed),
        maximum_drain_ms=max(r['drain_ms'] for r in fixed))
    result['diagnostics'] = {}
    for prefix, valid_return in (('', False), ('confirm-', True), ('clean-', True)):
        diagnostic_rows = rows(ROOT / (prefix + 'counts-samples.jsonl'))
        environment = json.loads((ROOT / (prefix + 'counts-environment.json')).read_text())
        expected = environment['events']
        for row in diagnostic_rows:
            assert list(row['counts']) == [e[0] for e in expected]
            for label, event, _ in expected:
                value = row['counts'][label]
                assert value['event'] == event and float(value['raw'][4]) == 100
                assert value['value'] >= 0
        if prefix == 'clean-':
            cases = {(name, connections, rate, 1)
                     for name in (*variants, 'borrowed')
                     for connections, rate in ((1, 50000), (64, 200000), (64, 240000))}
            assert len(diagnostic_rows) == len(cases) == 12
            assert {(r['variant'], r['connections'], r['rate'], r['repetition'])
                    for r in diagnostic_rows} == cases
            for row in diagnostic_rows:
                out = ROOT / f"clean-counts-{row['variant']}-c{row['connections']}-{row['rate']}-r1"
                assert not (out / 'client.stderr').read_text()
                assert not (out / 'server.stderr').read_text()
                assert all(line.endswith('affinity list: 0')
                           for line in (out / 'affinity.txt').read_text().splitlines())
        result['diagnostics'][prefix or 'original-'] = dict(
            valid_return_probe=valid_return,
            build_overlap=prefix == 'confirm-',
            samples=[dict(variant=r['variant'], connections=r['connections'], rate=r['rate'],
                          repetition=r['repetition'], achieved_rate=r['achieved_rate'],
                          drain_ms=r['drain_ms'], ratios={k: v['value']/r['responses']
                                                       for k, v in r['counts'].items()
                                                       if valid_return or k != 'tcp_recv_eagain'})
                     for r in diagnostic_rows])
    profiles = list(ROOT.glob('profile-*/report-self.txt'))
    assert len(profiles) == 6
    assert all('Total Lost Samples: 0' in p.read_text() for p in profiles)
    result['validation']['profiles_with_zero_reported_sample_loss'] = len(profiles)
    output = json.dumps(result, indent=2) + '\n'
    (ROOT / 'analysis.json').write_text(output)
    print(json.dumps(result['validation'], indent=2))


if __name__ == '__main__':
    main()
