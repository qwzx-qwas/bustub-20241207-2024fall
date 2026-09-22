# Storage correctness and performance acceptance (T0-A/B/C)

Performance protocol **t0-c-2** is defined in [the approved bounded-observation plan](../../docs/storage_redesign/testing_performance_observation.md). Service errors are recorded; they no longer require production optimization before measurement. Logical deadlines are censored observations, not successful samples. Stage budgets replace the old shared 600-second performance ceiling.

**Status:** prior C1–C5 evidence is retained. The v2 runs have finished: P1/P2/P4 completed measurement and business validation; P3 completed a one-hour observation and business validation but did not cover full turnover. Historical runs recorded 25 passing harness checks; the later design audit found self-check gaps, retained in the [module archive](../archives/README.md). Those passes do not establish complete oracle coverage. See [the v2 execution record](../../docs/storage_redesign/testing_execution_20260921.md) and [the v2 plan/review](../../docs/storage_redesign/testing_performance_observation.md).
See the [final review, execution and archive record](../../docs/storage_redesign/testing_execution_20260920.md).
See [A](../../docs/storage_redesign/testing_batch_a.md), [B and its implementation prompt](../../docs/storage_redesign/testing_batch_b.md),
[C and its prompt](../../docs/storage_redesign/testing_batch_c.md), the [A/B/C review](../../docs/storage_redesign/testing_review_abc.md)
and [full proposal](../../docs/storage_redesign/testing_implementation_proposal.md).
This directory is test-only. No E2E scenario executes on import or in normal CTest discovery.

The production fixes preserve declared column lengths through replicated CREATE and catalog
snapshot recovery, and permit exactly 1024 payload bytes without counting the internal NUL.
C1 now exercises growth and shrinkage (`c1-resize-1`); the initial fixture and P2 stream are unchanged.
As of 2026-09-22, the single-node `sql_storage_contract_test.cpp`, three Python self-check files,
and Go `model_test.go` are compressed in the [T0 module archive](../archives/README.md).
They no longer participate in working-tree CTest/unittest/Go test discovery. C1–C5/P1–P4 and
all runtime tools below remain active and unchanged. Archival does not fix known self-check gaps
or establish that E2E covers every retired boundary; those limits remain in the archive manifest.

## Review order

1. `content.py`: deterministic nonempty records, independent model, C1 changes and P2 access streams.
2. `adapter.py`: business operations to SQL (production length defects fixed); one unresolved write per client. `delivery.py` adds bounded performance retries; correctness policy is unchanged.
3. `client_driver.cc`: fixed worker pool calling the production TCP client and codecs.
4. `correctness.py`: C1–C5 business timelines and fault/recovery conditions.
5. `performance.py`: P1–P4 scenarios and shared windows; `arrival.py` owns bounded fixed arrivals; `workload.py` shares loading/verification.
6. `faults.py`, `history.py`, `linearizability/`: external evidence, retained attempts and project model.
7. `report.py`: window throughput, tail latencies, missing returns, resource observations.
8. `runtime.py`, `cluster.sh`, `run.py`: owned processes, existing harness reuse, budgets and archival.

## Build and run, after review

The commands below are instructions for after review and approval of qualification,
not a record of execution. Build only one heavy target at a time.

```bash
cmake -S . -B /tmp/bustub-t0-release -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14
cmake --build /tmp/bustub-t0-release --target build-storage-e2e -j1

# Review and authorize qualification before running. Each output directory must be new.
python3 test/storage_acceptance/run.py C1 --build /tmp/bustub-t0-release \
  --output /tmp/t0-c1-qualification --role qualification --port-base 33100

python3 test/storage_acceptance/run.py P2 --build /tmp/bustub-t0-release \
  --output /tmp/t0-p2-qualification --role qualification --port-base 33100 \
  --clients 1 --seconds 10

# Only after qualification and configuration freeze; twelve independent runs.
python3 test/storage_acceptance/run.py P2 --build /tmp/bustub-t0-release \
  --output /tmp/t0-p2-baseline --role baseline --port-base 33100 --matrix
```

On WSL, also supply `--host-free-gib N` from a separate check of the Windows volume
holding the virtual disk. Guest filesystem free space does not establish host headroom.
The runner checks Linux `/proc`, binaries, Release build metadata, ports, memory and filesystem space.
It never builds, changes machine settings, drops OS caches, kills unrelated processes, or deletes artifacts.
A per-user file lock prevents concurrent instances of this suite; it does not control unrelated workloads.

Performance defaults: logical request 60 s, preparation 1800 s, verification 600 s;
P1/P2 measurement 180 s, P3 work observation 3600 s, P4 lag formation/recovery 1800/600 s.
The outer deadline is their sum plus startup/cleanup allowance. All are recorded and frozen
before old/new comparison. P4 accepts `--rate-source` from a validated v2 P2 qualification,
or an explicit `--fixed-rate` (default 1 op/s with no source). A short qualification is
not a formal baseline. Correctness scenarios retain their existing budgets and retry policy.

## B: correctness scenarios

- C2: four clients × sixteen alternating operations on one key plus a final read; three independent seeds
  (`--repeat 1/2/3`, seeds 20260920/21/22). Check real invocation overlap and the full history.
- C3: `--variant kill|isolate|drop`. Kill/isolate follow sixteen acknowledged increments; drop uses
  a committed-but-lost response and the independent 10 → 11 expectation. Unknown writes retain identity/SQL.
- C4: snapshot threshold 64, bounded preparation, confirmed suffix, SIGKILL all nodes, original-directory
  reopen, full results and replay of the last confirmed identity, followed by new writes.
- C5: `--variant short|long`. Verify matched AppendEntries or non-stale completed InstallSnapshot,
  a fixed published boundary and direct target-replica contents before continuing business.

C2/C3 need a separately built [Porcupine checker](linearizability/README.md); no Go dependency enters production.
The checker and business model remain here; only handwritten model self-tests are archived.
Example **after review and qualification approval** (fresh output each time):

```bash
go build -C test/storage_acceptance/linearizability -mod=readonly -p=1 -o /tmp/bustub-check-history .
python3 test/storage_acceptance/run.py C2 --build /tmp/bustub-t0-release \
  --checker /tmp/bustub-check-history --output /tmp/t0-c2-r1 --role qualification --port-base 33100 --repeat 1
python3 test/storage_acceptance/run.py C3 --variant isolate --build /tmp/bustub-t0-release \
  --checker /tmp/bustub-check-history --output /tmp/t0-c3-isolate --role qualification --port-base 33100
python3 test/storage_acceptance/run.py C4 --build /tmp/bustub-t0-release \
  --output /tmp/t0-c4 --role qualification --port-base 33100
python3 test/storage_acceptance/run.py C5 --variant long --build /tmp/bustub-t0-release \
  --output /tmp/t0-c5-long --role qualification --port-base 33100
```

After qualification/configuration freeze, `--matrix --role baseline` selects the predefined P1 distribution/repeat grid, P2 concurrency grid,
three C2 seeds, three C3 variants, or both C5 paths. It does not retry failures until passing.
A correctness matrix has per-run outcomes and an overall completion flag, not a performance median.

Raw faults remain in `faults.jsonl` and proxy events. For C2/C3, the checker runs after cluster cleanup;
`linearizability.json`, `check.json`, `history.html` retain its input, decision and visualization.
Search limit: 30 s; whole checker process: 45 s; observed RSS: 1 GiB. Unknown/incomplete is never a pass.
A successful history also needs work/concurrency/fault coverage. At most 128 business attempts are retained,
including retries; status polling is separate diagnostic evidence. No history is truncated to fit the checker.

## C: remaining performance scenarios

- P1: four strong-read workers, `--variant uniform|hotspot`, 30-second warmup, 180-second measurement.
  `--matrix` means both distributions × three fresh repetitions. Hotspot draws target the first 5% of keys with 80% probability.
- P3: one writer; ten rounds of 20,000 operations in 14-update/3-delete/3-reinsert groups.
  Each group preserves live rows/payload bytes. Full verification separates round windows; observe 60 seconds after writes.
  The work-observation budget defaults to 3600 seconds; preparation/verification have separate budgets. Partial rounds are retained, never counted as completed rounds.
- P4: fixed 8-read/2-write arrival blocks, four read workers and two disjoint writers; six in flight, 64 queued.
  Thirty seconds healthy, stop a Follower, accumulate at least 256 acknowledged writes, drain a write barrier, restart,
  observe recovery to fixed K0 and thirty seconds thereafter, then verify the final target boundary and full contents.
  `--rate-source` accepts a fully observed, business-validated old v2 one-client P2 qualification; service errors are allowed. Use 50% of its logical goodput for both versions, or freeze an explicit absolute rate (default 1 op/s without a source).

Examples **after review** (add WSL host-space evidence where required):

```bash
python3 test/storage_acceptance/run.py P1 --variant hotspot --build /tmp/bustub-t0-release \
  --output /tmp/t0-p1-hot --role qualification --port-base 33100 --seconds 10
python3 test/storage_acceptance/run.py P3 --build /tmp/bustub-t0-release \
  --output /tmp/t0-p3 --role baseline --port-base 33100
python3 test/storage_acceptance/run.py P4 --build /tmp/bustub-t0-release \
  --output /tmp/t0-p4 --role qualification --port-base 33100 \
  --rate-source /tmp/t0-p2-qualification/P2-c1-r1/result.json
```

P3 records `space_rounds.jsonl`, `space_after_idle.json` and round metrics/trends. Steady-state assessment needs
five complete final rounds and three retention advances in that window; sampled flatness alone is insufficient.
P4 records `arrivals.jsonl`, `arrival_dispatch.json`, `recovery_timeline.json`, `recovery_progress.jsonl` and
plan-cohort latency statistics. Queue rejection, unproduced arrivals and missing responses remain visible.
Queued requests remaining at window close are recorded as unissued. Already in-flight operations have bounded drain; completions after the window do not increase window throughput.
No P4 result claims that K0 is the continuously moving latest state. Final direct replica validation is separate.

## Fixed content

- Seed `20260920`, content version `t0-a-1`; separate deterministic streams for data and access.
- C1: exactly 2,048 rows; update `id % 4 == 0`, delete `id % 4 == 1`, reinsert `id % 8 == 1` with distinct values.
  Every phase checks full rows; the independent final cardinality is 1,792.
- P2: 20,000 rows, 1,000 sequential preprocessing increments, then 1/2/4/8 clients with disjoint key ranges.
  Every client follows a fixed uniform access stream; a faster version consumes a longer prefix.
- Payload lengths 64/256/1,024, proportions 60/30/10 (largest remainder for integer counts).
  Half the rows use shared synthetic templates; half add record-specific fields. Shared values are not cache hits.
- Only the primary index; 256 BufferPool pages/node; snapshot threshold 10,000; node timeout 5 s, client timeout 6 s.
  These are explicit test configurations, not changes to production defaults.
- Schema/load/preprocessing use clients 1/2/3; measured writers use 100 onward. Each write session starts at 1.
  Reads and status queries have a separate correlation counter, and do not consume write sequence numbers.

Performance delivery retains one logical operation across retries and records every attempt.
Unknown writes keep their original identity; that writer parks at the logical deadline while
other clients continue. Final reconciliation is a separate phase; its confirmations never
backfill window goodput. Unresolved final state is labelled validation-incomplete.

## What is recorded and timed

`inputs.jsonl` records operation content, exact SQL, client/request identity, attempt and phase.
`history.jsonl` records separate call/return events from the same C++ steady clock, raw payload hex,
decoded results and errors. A submitted input or call without a return remains visible after interruption.
The test bridge uses nine tab-separated fields:

```text
T0A1  correlation  kind  client_id  request_id  timeout_ms  cutoff_ns  endpoint  sql_hex
```

Fields are separated by literal tabs; SQL is hex-encoded UTF-8, so embedded whitespace is preserved.
`-` encodes absent SQL/endpoint for status/clock commands. This is private test IPC, not the production wire protocol.
`CLOCK` timestamps define the performance window; `cutoff_ns` prevents new driver invocations after it closes.
Inputs accepted too late are `skipped`, not successful requests. The worker pool and its queue each cap at eight.

Performance results distinguish raw C++ call timing from logical-operation timing in
`logical_history.jsonl`. Logical timing uses one Python monotonic clock and includes all
attempts, rediscovery and backoff; P4 also includes waiting since its scheduled arrival.
Clock translation bounds for arrival schedules are retained in `arrival_clock.json`.

Throughput counts distinct successful logical operations completed inside the full fixed
window. Late successes, exact failures, unissued requests and deadline-censored observations
are separate. A censored value is displayed as elapsed-time-plus, never inserted as an exact
successful latency. Fewer than 10,000 successes makes p99 exploratory; success-only percentiles
are not labelled as whole-population percentiles. Matrix medians require all planned repeats
to have complete observation and business validation; zero goodput is retained when valid.

Each run includes:

- `fixture.jsonl`, `fixture.json`: exact starting business data and content digest.
- `config.json`, `measurement.json`, `result.json`: versioned configuration, timing boundaries and conclusions.
- `inputs.jsonl`, `history.jsonl`: every transport attempt, including preparation/checks.
- `logical_history.jsonl`, `stages.jsonl`: business observations, absolute deadlines, attempts, censoring and stage boundaries.
- `resources.jsonl`: once-per-second process CPU/RSS/IO, host memory/device counters and artifact occupancy.
- `nodes/`, stderr files: original node data/logs, launcher and driver diagnostics.

The parent archive includes source revision/diff and copies/hashes of test sources, build metadata, binary hashes,
environment, all run results and a matrix summary. No failed run is replaced with a retry.

## Resource and conclusion boundaries

Three nodes: 3 GiB RSS; Python + driver: 512 MiB; experiment data and artifacts: 10 GiB.
These are sampled stop thresholds, **not hard quotas**. Node startup, brief allocation spikes and storage caches
can exceed samples; preserve this limitation. Host MemAvailable and filesystem headroom must stay above 1 GiB.
The total output budget includes previous parameter points in the same invocation.

Linux process IO counters and host-wide disk statistics have different scopes; observer writes and unrelated IO
can affect device counters. Do not derive precise write amplification or cache attribution from them.
Resource summaries use samples inside the measurement window, report the actual observation duration, and
never equate fixed device capacity with live storage usage.

Node-stage elapsed time and offline checker time are separate; process samples are grouped by PID plus birth time
across intentional restarts. Proxy processes count toward the observer budget.

The current update-preparation path scans the table. P2 measures that full production path, not pure device IOPS.
C2/C3 now check the specified single-key linearizability model; C4/C5 check their stated process recovery
and catch-up boundaries. C1–C5 have now been executed; the latest performance execution status is recorded separately. This suite does not prove power-loss safety, torn-write
recovery, cold-device performance, arbitrary SQL transaction linearizability, or the future per-client request window.

## Reference mechanisms

- [etcd test interfaces](https://github.com/etcd-io/etcd/blob/main/tests/framework/interfaces/interface.go):
  capability/content separation becomes this suite's content/adapter/runtime boundary.
- [YCSB CoreWorkload](https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/workloads/CoreWorkload.java):
  separate data shape and key distribution; applied in `content.py`, with a project-specific independent oracle.
- [Ceph CBT](https://github.com/ceph/cbt/blob/master/benchmark/radosbench.py): preparation, measurement,
  monitoring and archival become explicit phases. Its global cache and process controls are not adopted.
- [etcd unknown-result model](https://github.com/etcd-io/etcd/blob/main/tests/robustness/model/non_deterministic.go):
  an uncertain response does not mean no effect; applied in `adapter.py` and retained histories.

The existing project harness owns node startup/shutdown; the only extension is an optional test-side client
timeout setting whose old default stays 3 seconds. The production client and codecs own TCP framing and validation.

## Cleanup review

No production interface was added. No old caller or production path was made obsolete by this test-only target.
Old test files remain: existing protocol, session and recovery tests cover boundaries healthy C1/P2 do not.
Whole-UPDATE spelling assertions were removed from this suite's self-checks; literal payload preservation,
independent expected rows, unknown-write handling and window accounting remain.
The earlier `test/legacy_raft/storage_baseline.py` is an exploratory script, not a second official entry point.
It also contains read/churn/recovery workloads, whose common-suite replacements are implemented in B/C; review retirement
after those replacements have been reviewed and validated. This suite does not call it or copy its protocol decoder. Prior raw artifacts remain
preserved and are not accepted baseline evidence.
