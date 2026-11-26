# Performance

RaftEx ships a repeatable benchmark suite covering the write path, recovery,
log reads, committed-entry application, replication batch construction, and
local queries.

## Running the suite

```sh
mix run scripts/bench.exs
```

The suite runs every scenario, performs one warm-up iteration, then measures
five iterations per scenario. Each run uses a fresh temporary data directory
and removes it afterwards.

Useful options:

```sh
mix run scripts/bench.exs --scenario wal_append_1uid
mix run scripts/bench.exs --iterations 10 --warmup 2
mix run scripts/bench.exs --scale 0.1          # smaller inputs for a quick check
mix run scripts/bench.exs --record             # write bench/baseline.json
mix run scripts/bench.exs --compare bench/baseline.json --threshold 10
```

`--compare` prints the percentage change per scenario and exits non-zero when a
throughput figure drops or a latency figure rises beyond the threshold.

## Scenarios

| Scenario | Measures | Default input |
|----------|----------|---------------|
| `wal_append_1uid` | Sequential synchronous WAL appends, one server | 1,000 appends |
| `wal_append_10uids` | Concurrent WAL appends across 10 servers (5 appenders each) | 500 appends |
| `wal_append_group` | Concurrent synchronous appends to one server (10 appenders) | 1,000 appends |
| `wal_recovery` | Recovering a WAL from disk | 10,000 records |
| `log_fold` | Reading a 100-entry range from a large log | 100,000 entries |
| `apply_committed` | Applying a committed batch to the state machine | 100 entries |
| `replication_batch` | Building AppendEntries RPCs for three peers | 100,000 entries |
| `query` | Local query handling against an applied state machine | 1,000 queries |

## Baseline

The committed baseline lives at `bench/baseline.json`. Record a new baseline on
the target machine with:

```sh
mix run scripts/bench.exs --record
```

### Environment

| | |
|---|---|
| Machine | Apple M1 Pro, 16 GB |
| OS | macOS 27.2.1 (build 26B131) |
| OTP | 29 |
| Elixir | 1.20.5 |
| Revision | `b20f107` |

### Pre-optimization baseline

Recorded before the `optimize-performance` change, with
`mix run scripts/bench.exs --record`.

| Scenario | Iterations | Input | Elapsed | Throughput / latency | Sync |
|----------|-----------:|------:|--------:|----------------------|------|
| `apply_committed` | 5 | 100 | 7.82 ms | 63,914.1 entries/s | none |
| `log_fold` | 5 | 100 | 14.44 ms | 34,626.0 entries/s | none |
| `query` | 5 | 1,000 | 0.25 ms | 20,325,203.3 queries/s | none |
| `replication_batch` | 5 | 30 | 271.50 ms | 552.5 entries/s | none |
| `wal_append_10uids` | 5 | 500 | 509.10 ms | 4,910.6 appends/s | datasync |
| `wal_append_1uid` | 5 | 1,000 | 958.73 ms | 5,215.2 appends/s | datasync |
| `wal_append_group` | 5 | 1,000 | 1,084.74 ms | 4,609.4 appends/s | datasync |
| `wal_recovery` | 5 | 10,000 | 20.39 ms | 4,077.8 us | none |

### Post-optimization baseline

Recorded after the `optimize-performance` change, with
`mix run scripts/bench.exs --record --baseline bench/post.json`.

| Scenario | Iterations | Input | Elapsed | Throughput / latency | Sync |
|----------|-----------:|------:|--------:|----------------------|------|
| `apply_committed` | 5 | 100 | 3.77 ms | 132,590.8 entries/s | none |
| `log_fold` | 5 | 100 | 0.07 ms | 7,352,941.2 entries/s | none |
| `query` | 5 | 1,000 | 0.19 ms | 25,906,735.8 queries/s | none |
| `replication_batch` | 5 | 30 | 0.12 ms | 1,219,512.2 entries/s | none |
| `wal_append_10uids` | 5 | 500 | 474.87 ms | 5,264.6 appends/s | datasync |
| `wal_append_1uid` | 5 | 1,000 | 1456.16 ms | 3,433.7 appends/s | datasync |
| `wal_append_group` | 5 | 1,000 | 957.50 ms | 5,221.9 appends/s | datasync |
| `wal_recovery` | 5 | 10,000 | 12.36 ms | 2,471.6 us | none |

### Before / after

| Scenario | Before | After | Change |
|----------|-------:|------:|-------:|
| `apply_committed` | 63,914.1 entries/s | 132,590.8 entries/s | +107% |
| `log_fold` | 34,626.0 entries/s | 7,352,941.2 entries/s | +21,139% |
| `query` | 20,325,203.3 queries/s | 25,906,735.8 queries/s | +27% |
| `replication_batch` | 552.5 entries/s | 1,219,512.2 entries/s | +220,630% |
| `wal_append_10uids` | 4,910.6 appends/s | 5,264.6 appends/s | +7% |
| `wal_append_1uid` | 5,215.2 appends/s | 3,433.7 appends/s | -34% |
| `wal_append_group` | 4,609.4 appends/s | 5,221.9 appends/s | +13% |
| `wal_recovery` | 4,077.8 us | 2,471.6 us | -39% |

## Interpreting results

- Throughput scenarios report work per second; higher is better.
- Latency scenarios report microseconds per run; lower is better.
- Run comparisons on the same machine and with the same arguments. Absolute
  numbers vary with CPU, filesystem, and background load.
- The suite is intentionally dependency-free so it can run in CI and on a
  developer laptop without extra tooling.
