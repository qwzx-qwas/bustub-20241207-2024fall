# Storage correctness and performance acceptance (T0-A/B/C)

## 2026-10-12 当前验收：仅恢复流水线与长连接

**2026-10-13 状态更新：** 用户已完成下面的 13 点流程，见[结果与限制](../../docs/storage_redesign/final_performance_fixes_20261011.md#10-2026-10-13用户验收结果readindex-分析与提交前清理)。已清理结束测试的设备、可重建 fixture 和初轮重复证据；结果、请求记录、最终验证与程序仍保留。不要对已完成目录再次运行 `selected`；只有确需新一轮时才从 `prepare` 建立新目录。本次清理没有重新执行负载。

基线 `1c651c7`。复用原测试输入、预算、种子及业务校验，不开 perf/bpf。具体实现和定向检查见[修正记录 §9](../../docs/storage_redesign/final_performance_fixes_20261011.md#9-2026-10-12在恢复后的基线上仅接入恢复流水线与长连接)。下面旧日期的完整验收流程保留为历史说明，本轮使用 `selected`。

```bash
cd /home/qwzx/projects/bustub-20241207-2024fall
bash test/storage_acceptance/run_final_fixes.sh prepare &&
bash test/storage_acceptance/run_final_fixes.sh verify
```

准备与定向检查通过后，只需一个命令启动本轮性能验收：

```bash
bash /home/qwzx/projects/bustub-20241207-2024fall/test/storage_acceptance/run_final_fixes.sh selected
```

`selected` 按顺序执行 P1 均匀/热点各三轮、P4 固定 1 次/秒一轮、P2 四/八客户端各三轮；不执行 P3 或 P2 一/二客户端。共 13 点；每点继续使用原预热、测量、业务核对及关闭流程。阶段打印日志和结果位置，不因失败改参数重跑。重复执行同一个已开始的 `selected` 会拒绝；需用 `prepare` 建立新目录，保留失败证据。

`prepare` 只构建和冻结源码/程序；`verify` 运行既有 CRC、恢复/快照/GC、Raft、TCP、节点与报告器检查，以及长连接代理定向测试。这些是短的正确性检查，不是正式 P1/P2/P4。结果目录由 `.cache/final-acceptance/fixes-run-root` 记录，换 Bash 不丢失；Windows 卷空闲空间由既有脚本查询，不需要手设变量。


<a id="final-fixes-2026-10-11"></a>
## 2026-10-11 收尾修正：由用户执行

本轮代理只清理、审查、编码和编译，**没有代用户运行以下测试**。用户首次执行已通过 CRC 四项、恢复和快照两项；GC 测试因漏调 Bootstrap Open 在初始化时失败，已修正测试装配、等待复跑，C/P 尚未启动。原 C/P 业务输入、缓存、种子、时限与判据不变；此次不附加 perf/gdb，不与带采样的诊断数字混算。具体改动、测试设计和限制见[修正记录](../../docs/storage_redesign/final_performance_fixes_20261011.md)。

按顺序执行，每阶段成功后进入下一阶段。脚本自行定位仓库、配置 GCC 16 库路径、查询 WSL 所在 Windows 卷空闲空间；不依赖之前 Bash 的变量。`prepare` 建立全新结果目录并冻结源码、配置、程序指纹，后续阶段核对指纹。第一次准备输出保存在 `.cache/final-acceptance/fixes-run-root`；重新打开 Bash 也能继续读取它。

```bash
cd /home/qwzx/projects/bustub-20241207-2024fall
bash test/storage_acceptance/run_final_fixes.sh prepare &&
bash test/storage_acceptance/run_final_fixes.sh verify &&
bash test/storage_acceptance/run_final_fixes.sh correctness &&
bash test/storage_acceptance/run_final_fixes.sh performance
```

- `prepare`：构建正式节点、客户端、驱动及两个定向测试程序，冻结指纹；没有启动负载。
- `verify`：CRC 旧值兼容/组合、恢复候选、捕获后更新与 GC 分组/pin 四项新增契约，复用已有 codec 样例及报告器检查。此处定向 IO 禁用周期维护，避免测试依赖后台扫描时机；背景协作由后面的原 C/P 验证。
- `correctness`：C1、C2 三种种子、C3 三种故障、C4、C5 两种追赶，共 10 点；C2/C3 复用现有 Porcupine 检查器。任一场景未通过就停止，性能阶段要求本阶段通过标记。
- `performance`：P3 原一小时观察、P4 固定 1 次/秒形成落后并恢复、P1 六点、P2 十二点。准备和核对仍受原阶段预算；P3 未覆盖或单场景失败时保留结果，继续其他独立场景，最后返回非零，绝不重复直到通过。

查看本次保存位置：

```bash
cat /home/qwzx/projects/bustub-20241207-2024fall/.cache/final-acceptance/fixes-run-root
```

该目录下 `build.log`、`crc.log`、`contracts.log`、`report-contracts.log`、`C1.log`…`P2.log` 保存阶段输出；各场景子目录保留 `result.json`、`history.jsonl` 等原证据。脚本在每项开始/成功时打印状态；失败直接显示退出码、日志末尾 40 行和完整路径。缺少前置通过标记会明确说明哪个阶段尚未通过。失败后先分析；修正代码/脚本后重新 `prepare` 建立新目录和指纹，旧失败目录保留。上面的 `&&` 链会在首次失败时停止。脚本不会清理本次新失败设备或旧基线。

2026-10-11 已按用户要求删除过期开发期源码/结果压缩包，并清理旧设备与无用中间产物。下文较早的阶段压缩归档说明仅是历史记录；原共同 C/P 对比基线仍保留，本轮不重新制造开发期压缩包。

## Historical protocol and general runner reference

Performance protocol **t0-c-2** is defined in [the approved bounded-observation plan](../../docs/storage_redesign/testing_performance_observation.md). Service errors are recorded; they no longer require production optimization before measurement. Logical deadlines are censored observations, not successful samples. Stage budgets replace the old shared 600-second performance ceiling.

**Historical file-backend status:** prior C1–C5 evidence is retained. The v2 runs have finished: P1/P2/P4 completed measurement and business validation; P3 completed a one-hour observation and business validation but did not cover full turnover. Historical runs recorded 25 passing harness checks; the later design audit found self-check gaps, retained in the [module archive](../archives/README.md). Those passes do not establish complete oracle coverage. See [the v2 execution record](../../docs/storage_redesign/testing_execution_20260921.md) and [the v2 plan/review](../../docs/storage_redesign/testing_performance_observation.md).
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

The original file-backend test target added no production interface. The S12 adapter below uses the new, public operational storage-status request; it is not a test hook.
Old test files remain: existing protocol, session and recovery tests cover boundaries healthy C1/P2 do not.
Whole-UPDATE spelling assertions were removed from this suite's self-checks; literal payload preservation,
independent expected rows, unknown-write handling and window accounting remain.
The earlier `test/legacy_raft/storage_baseline.py` is an exploratory script, not a second official entry point.
It also contains read/churn/recovery workloads, whose common-suite replacements are implemented in B/C; review retirement
after those replacements have been reviewed and validated. This suite does not call it or copy its protocol decoder. Prior raw artifacts remain
preserved and are not accepted baseline evidence.

## S12 object deployment adapter

Add `--storage-template docs/storage_redesign/node-storage.example.conf` to run the
same scenarios through the production object backend. The runner sizes three new,
test-owned devices, gives each node independent persistent identities, invokes
`bustub-node --storage-action initialize` once, then uses Open for every start.
No business input, oracle, fault timeline or fixed row count changes.

The production config is explicit `key=value`; see [the deployment protocol](../../docs/storage_redesign/s12_node_deployment.md).
`bustub-client status --storage 1` carries a versioned storage JSON payload. At a target 1 Hz the adapter uses the
existing TCP driver to sample committed Data allocation bytes and B cache/scrub
progress. Missing observations are marked incomplete. Host file blocks are kept
as artifact usage, not mistaken for internal space reuse. This extra status
traffic is part of the recorded observer overhead; Data usage excludes the fixed
Metadata and Journal regions, so it is not directly comparable with old file totals.

Use a compiler/library supporting the repository's C++23 zero-page lifetime
check (the old clang-14 build example above describes the historical baseline).

本轮代理观察同时识别原文、增量和压缩快照请求；逻辑进度按原始/复用长度计算，仍要求匹配 type 6 的非 stale 完成响应，随后验证目标数据。C5 业务与故障判据不变；只识别旧 type 5 会漏判已经发生的压缩安装。

## External diagnostics (2026-10-11)

用户运行，agent 本轮没有启动用例。先审查现有调用链、报告和观察字段，再新增必要测试器验证；本节不改变业务内容、原期限、缓存或成功判据，也不新增生产 hook/API。

### 1. 报告器自检（只读人工小型时间线，无集群）

```bash
cd /home/qwzx/projects/bustub-20241207-2024fall
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s test/storage_acceptance -p test_report_contracts.py -v
```

两项覆盖 P3 多轮统计及单窗口尾部规则。已完成逻辑/语法审查，未运行；详见[八项设计审查](../../docs/storage_redesign/final_acceptance_20261010.md#7-报告修正原则与八项审查)。已有 P3 历史仅离线重算，原 `result.json`、二进制和原始事件均保留。

### 2. 单点负载与外部采样

每条命令在新 Bash 中可独立执行。脚本按 `WSL_DISTRO_NAME` 从 Windows 注册表找到当前发行版的虚拟磁盘目录，再查询所在卷的空闲 GiB；不再要求手填。2026-10-11 已只读核对 Ubuntu-24.04 位于 C 盘，当时空闲约 35 GiB，每轮仍重新查询。第二个参数可显式传入另行查询的真实值；guest `df` 不能替代这个证据。新目录由 mktemp 创建，不覆盖旧结果。不同时执行多项。

```bash
cd /home/qwzx/projects/bustub-20241207-2024fall
bash test/storage_acceptance/run_diagnostic.sh P3
```

其余点将最后一个参数替换为 `P1-uniform`、`P1-hotspot`、`P2-c4`、`P2-c8` 或 `P4`。这些是原 `run.py` 的单个 qualification 点，不是新负载；每个 P1/P2 仍测 180 秒，不是三次正式矩阵。P3 仍观察 3600 秒，P4 仍固定 1 请求/秒及 256 次落后写。运行目录独立标注 `DIAGNOSTIC_ONLY.txt`，有 profiler/ptrace 开销，不能覆盖或混入正式性能成绩。

推荐顺序是报告器两项自检 → P3 → P4 → P1-uniform → P1-hotspot → P2-c4 → P2-c8，分别定位多轮报告、长停顿、启动恢复、两种读取分布及并发平台期；无需重复旧正确性全矩阵、P2-c1/c2 或三次性能重复。每个命令结束再运行下一个。报告器失败先停；`observer_exit` 非零先检查 `capture.log`，不反复重跑业务。P3 的 `test_exit=1` 可能仍是原一小时目标未完成，应保留其原结果。

2026-10-11 用户要求收尾清理：旧阶段源码/结果压缩包及已结束测试的设备文件已删除，现有 Release 程序、工具链、共同 C/P 基线和全部 JSON/JSONL 证据保留。清单见 [cleanup-final-20261011.json](../../test-results/cleanup-final-20261011.json)。无需为此次诊断重编译或重新展开阶段包。

| 点 | 触发 | 本轮采集 |
| --- | --- | --- |
| P3 | 某个 turnover 请求 10 秒仍未结束 | 最多三个不同请求，各 60 秒 perf/pidstat；每次附加三个节点的线程栈 |
| P1/P2 | 原 measure 阶段开始 | 一次 60 秒 perf/pidstat |
| P4 | 既有 restart_acknowledged | 一次 60 秒 perf/pidstat，覆盖新 Follower 与其余节点 |

`capture_diagnostics.py` 只读取原 `faults.jsonl`、`stages.jsonl`、`logical_history.jsonl` 来选择时间/PID，不复制业务、Oracle、B 计数或资源监测逻辑。附加前核对 PID 的命令行仍属于该运行目录。保留原 1Hz 的 resources.jsonl，不额外向节点发状态请求。10 秒仅为诊断触发器，不把原 60 秒业务期限改为 10 秒；未触发记 `not_triggered`，不当作无性能问题。

依赖已静态核对：原 `.cache/final-acceptance/build` 三个可执行文件、GCC16.2 动态库、Python3、pidstat、GDB。`/usr/bin/perf` 在当前 WSL 上是失效的内核版本包装器，脚本使用实际 `/usr/lib/linux-tools/6.8.0-142-generic/perf`（6.8.12，DWARF/unwind 已编入）；可通过 `DIAG_PERF` 显式覆盖。没有在本轮尝试内核采样。

脚本在开始负载前执行 `sudo perf stat -e cpu-clock:u -- true` 检查事件支持。若内核不支持，先停止，不静默丢掉证据。此时可显式使用 `bash ... P3 <实际空闲GiB> --no-perf` 收集 pidstat 和 GDB；这不能给出 CPU 热点比例。

sudo 只用于外部观察进程和归还新诊断文件所有权，测试节点仍为普通用户；不更改全局 ptrace/perf 设置。观察者启动时取得权限，避免等待 P3 后期时凭据过期。GDB 会短暂停止节点，可能影响选举/延迟；这是带干扰的定位证据，必须结合采样前已有慢请求判断。CPU 样本只代表运行时间，阻塞等待不能直接从 CPU 火焰图比例计算。

### 3. 读取证据

脚本打印唯一结果目录。其下 `P*/.../` 是原负载产物，`profile/capture-N/` 包含 `capture.json`（PID、触发请求、单调时钟、实际命令和退出码）、`pidstat.log`、`perf.data`/`perf.log`，P3 还有 `stack-1.log` 等。`capture.log` 与 `profile/summary.json` 区分成功、工具失败和未触发。原测试退出 1 可能是 P3 仍未完成，而非采样脚本坏了。

```bash
# 将路径换成脚本打印的实际目录；只分析已有 perf.data，不重跑负载。
/usr/lib/linux-tools/6.8.0-142-generic/perf report --stdio \
  -i /实际结果目录/profile/capture-1/perf.data --sort comm,dso,symbol \
  > /实际结果目录/profile/capture-1/perf-report.txt
```

P3 关联慢请求/应用边界、B loads/evictions 增量、CPU 栈及等待栈；观察 Capture/Clone、Reclaim、MetadataCache、Commit 等路径。P1/P2 关联取页/读取校验/元数据查询、CPU 与设备读取、线程等待；当前没有所有业务阶段的精确队列等待指标，不能仅靠一次 CPU 样本宣称测出了排队时长。

P4 针对当前生产恢复链拆解：NodeStorage 恢复 B/Journal 与对象组件 → Raft Store 的目录/日志打开 → A 本地 checkpoint 或空工作空间 → Raft 构造阶段逐条 Apply 已提交日志 → 开放监听并补远端后缀。入口在 `DistributedNode::Initialize/Start`、`NodeStorage::Begin`、`OpenLocalCheckpoint`、`RecoverRaftPersistentState`、`RaftNode` 构造函数。采样帮助定位热点，不自动产生每个阶段精确耗时；约 28 秒“首次状态”包含轮询间隙。没有在未确认根因前修改恢复顺序或把业务服务提前开放。

代码上已确认的定位候选：`LogStore::ProbeObjects()` 与随后的 `OpenObjects()` 都构造 `ObjectLogStore`，构造过程扫描片段、读取/校验正文并重建位置索引；这是重复工作候选，需测量其占比及复用条件，不能删掉恢复边界验证。`RaftNode` 构造时按 `(LastApplied, commit_index]` 同步重放；本次 P4 停机边界约 321，而 A checkpoint 触发阈值为 10000，需特别观察空工作空间重放建表/加载日志的成本。已有 Data 页面不能绕过一致恢复入口直接作为完整业务状态使用。这里记录代码事实与待测假设，未据此修改生产。
