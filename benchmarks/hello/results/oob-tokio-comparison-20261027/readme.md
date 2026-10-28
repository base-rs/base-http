# Base versus Tokio comparison evidence

Three interleaved six-second rounds on Hana, 27 October 2026. See the [comparison report](../../oob-tokio-comparison.md).

`benchmark/` contains all 36 samples, environment metadata, response validation, warmup output, measured wrk output, server stderr and paired summaries. `benchmark.log` records execution order and the summary. All samples and warmups completed with zero load-generator errors.

`source/` preserves the benchmark controller, load runner, manifest, lockfile and Rust benchmark source from the staged Hana build. Its absolute scheduler patch paths refer to the task directory on Hana, `/tmp/base-http-linux-gap-vohfc1rh`. The frozen executable remains there as `binaries/oob-default`; its SHA256 is recorded in `benchmark/environment.json`. Scheduler implementation and validation evidence are in the [OOB option evidence directory](../oob-default-20261027/readme.md), including the feature patch based on v0.1.2.

Run from the staged task root:

```sh
taskset -c 1 python3 benchmark.py \
  --variants \
    base-http:oob-default:base-native \
    hyper-tokio:oob-default:hyper-tokio \
    base-raw:oob-default:base-native-raw \
    tokio-raw:oob-default:tokio-task-raw \
  --output results/oob-tokio-comparison \
  --repetitions 3 --duration 6 --concurrency 1 16 64
```

`sha256.json` covers every evidence file except itself. No source changes or new test run were needed for this measurement-only comparison.
