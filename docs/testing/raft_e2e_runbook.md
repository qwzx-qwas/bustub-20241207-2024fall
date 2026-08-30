# Raft M0-M8 E2E runbook

## Build outside the repository

```bash
cmake -S . -B /tmp/bustub-raft-build-clang \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang-14 \
  -DCMAKE_CXX_COMPILER=clang++-14 \
  -DBUSTUB_SANITIZER=address,undefined

cmake --build /tmp/bustub-raft-build-clang --target \
  build-raft-component-gates bustub-node bustub-client -j1
```

On the current resource-constrained VSCode/WSL host, run only one heavy build/test command at a time and keep the
example's `-j1`. Do not overlap compiler, sanitizer, E2E, or test-agent processes. This is a local resource
scheduling rule, not permission to retry a failed scenario or weaken its oracle; independent CI runners retain the
workflow's complete gate set.

Loopback sockets can be forbidden by a development sandbox even though the code is valid. Run TCP and process tests in
an environment that permits only local `127.0.0.1` connections. Tests allocate unique temporary directories; the six
shell timelines take an isolated low port base, so parallel invocations must use non-overlapping bases and every derived
listener must remain below the host ephemeral-port range.

Use one common election interval for all voters (the default is `[300,600]` ms). Production nodes independently draw a
new value on every deadline reset; do not assign fixed per-node constants. Protocol tests that need an exact timeline
inject a fixed timeout source or `MakeSeededElectionTimeoutSource(seed)` instead of exposing a production test flag.

## Component gate

```bash
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 \
  python3 test/e2e/raft_gtest_gate.py \
  /tmp/bustub-raft-build-clang /tmp/bustub-raft-component-logs
```

The runner executes the 27 M0-M8 binaries exactly once and writes one log plus the GoogleTest JSON report per binary
under the caller-owned external artifact root. Preserve them through result summarization/upload; then delete successful
raw artifacts at the cleanup gate. Preserve failures only through diagnosis, handoff, or explicit external archival.
Before starting, it reverse-scans every recovery/Raft/distributed test source and rejects an incomplete or stale binary
manifest. A zero exit is accepted only when the JSON report proves that at least one concrete test ran. Every nonzero
exit, signal, sanitizer report, assertion, bind failure, malformed report, or 120-second timeout fails immediately;
there is no process retry, including for an empty pre-main exit 139.

`build-raft-component-gates` is the clean-tree aggregate for exactly these component executables. The runner still
reverse-scans the source tree independently, so a source added without updating its execution manifest fails instead
of silently becoming an unbuilt or unrun test.

## Stable TCP integration

```bash
UBSAN_OPTIONS=halt_on_error=1 ASAN_OPTIONS=detect_leaks=0 \
  /tmp/bustub-raft-build-clang/test/tcp_transport_test --gtest_color=no

UBSAN_OPTIONS=halt_on_error=1 ASAN_OPTIONS=detect_leaks=0 \
  /tmp/bustub-raft-build-clang/test/distributed_node_test --gtest_color=no
```

`distributed_node_test` covers normal batches, invalid admission, response replay, AppendEntries and InstallSnapshot
catch-up, all-node restart, damaged-newest fallback, isolated-Leader write/read timeouts, atomic multi-row visibility,
and a fully chunked delayed stale snapshot. The stale snapshot is injected by the test-owned external TCP sender; no
production RPC, configuration flag, or default path is added for fault injection.

## Formal process timelines

```bash
bash test/e2e/raft_m6_smoke.sh \
  /tmp/bustub-raft-build-clang 18100 /tmp/bustub-raft-m6-smoke-current

bash test/e2e/raft_m7_snapshot_crash.sh \
  /tmp/bustub-raft-build-clang 19100 /tmp/bustub-raft-m7-snapshot-crash-current

bash test/e2e/raft_m7_snapshot_transfer.sh \
  /tmp/bustub-raft-build-clang 20100 /tmp/bustub-raft-m7-snapshot-transfer-current

bash test/e2e/raft_m7_recovery_matrix.sh \
  /tmp/bustub-raft-build-clang 21100 /tmp/bustub-raft-m7-recovery-matrix-current

bash test/e2e/raft_m0_m7_chain.sh \
  /tmp/bustub-raft-build-clang 25100 /tmp/bustub-raft-m0-m7-chain-current

bash test/e2e/raft_m8_payload_binding.sh \
  /tmp/bustub-raft-build-clang 26100 /tmp/bustub-raft-m8-payload-binding-current
```

Keep every scenario's listener, client, proxy, and derived sub-scenario port below the host's ephemeral allocation
range. On Linux the usual range begins at 32768; an all-message proxy creates many short-lived outbound connections, so
using an ephemeral source port as a fixed listener can create a real bind collision during a one-shot node restart.

All timelines source `test/e2e/raft_process_harness.sh`; node startup, the common randomized election interval, Leader
discovery/redirection, narrowly classified delivery retry for identified requests, status waits, shutdown, and PID
cleanup live only there. Each requested node launch has exactly one process attempt; a crash/signal is never treated as
a startup retry. A `strict` client assertion has one process and destination, while `eventually` may resend the same
identified request only after a named transport/leadership outcome. This is request-level at-least-once delivery with
an unchanged identity, not a rerun of the test or process. The harness only signals PIDs it started and now requires the
expected exit status from nodes, proxies, and registered background helpers; a proxy parse/invariant error fails the
timeline at cleanup. M6 performs formal client writes/ReadIndex, kills the Leader, retries
an uncertain request at the new Leader, restarts the old directory, and waits for catch-up. M7 uses a wider randomized timeout interval under
ASan, waits until the preceding snapshot generation is published, then watches the next formal canonical capture,
kills its Leader, stops both peers, and verifies standalone restart through the committed suffix, literal PK/index rows,
and byte-identical Session replay. The crash fixture uses 1600 meaningful rows and separately checks rows whose values
come from the complete snapshot, the pre-crash bridge suffix, and the entry that opened the killed capture. Session
duplicate replay is attempted only after restoring quorum; standalone restart is used for durable read recovery. The transfer timeline
uses a test-owned external Raft proxy for lag, chunk recording and stale replay. The recovery matrix covers minority
loss with the old Leader kept online, higher-term demotion after healing, fresh ReadIndex isolation, all-node restart,
newest-snapshot corruption, and concurrent reads. The historically named `raft_m0_m7_chain.sh` starts with fresh
distributed directories and keeps one M3–M7 production-process state alive across admission, response loss, Leader replacement,
multi-chunk snapshot catch-up, stale-snapshot replay, identity rejection, full restart, and post-recovery OID/index use.
It cumulatively rechecks earlier logical recovery properties; it does not migrate term-0 physical state and complements rather than replaces the focused fault timelines. These helpers only
forward/drop formal frames or edit stopped test files; they add no production test API. Runs temporarily retain node
logs and directories at the requested external artifact root. The cleanup gate removes successful raw state after
summary/upload and removes failed state after diagnosis, handoff, or explicit archival; process/PID/port cleanup is
mandatory at the end of every scenario.

The independent M8 timeline uses one fresh homogeneous three-node state. It proves with fixed-endpoint one-shot client
calls that a changed reuse of the latest request ID returns the exact stable rejection text without changing literal
rows or the serving node's `LOG-MUTATIONS` size/SHA-256. It repeats that proof after a dropped response and Leader kill,
after independently parsing a covering V1 `CURRENT` record, on a pre-request follower forced through a recorded
InstallSnapshot and then elected Leader, and after all three directories cold-open. The threshold stays above the
pre-snapshot rejection boundary; another client commits three real SQL writes to trigger compaction, so background log
rewrites cannot race the earlier journal oracle. Two consecutive full three-node cold reopens repeat both the changed
and exact retry checks. Exact retries must return the byte-identical cached `WriteResponseV1` and independently leave
stable Raft fields, literal rows, and `LOG-MUTATIONS` size/SHA-256 unchanged. Only Leader discovery and status
advancement may poll; none of the safety requests reroute or retry.

Do not copy process helpers back into the scenario scripts. New process cases should be timeline functions or scripts
that source the same harness; if a client receives `NOT_LEADER`, the shared retry path follows the advertised Leader or
performs a new ready-Leader search.

The process-level atomic-read case intentionally uses only production clients and scheduling; it proves that every
observed result is the complete old or complete new set. The deterministic critical-section proof lives in
`BusTubStateMachineTest.ReaderBlocksUntilDataIndexSessionAndWatermarkPublishTogether`, whose test-owned gate forces a
reader against the publication boundary. `DistributedNodeTest.ConcurrentReadsObserveOnlyWholeMultiRowBatch` adds the
production `DistributedNode` worker/read overlap. Do not add a production Apply-pause RPC merely to force that timing
in a process test.

## Experimental file/chunk snapshot checks

The formal path intentionally avoids an aggregate snapshot byte vector. The state machine writes a bundle file,
`SnapshotStore` copies and checksums it in bounded blocks, `InstallSnapshot` sends 64 KiB chunks, and the follower appends
them to a durable staging file before installing a file slice. During review, check that only bounded catalog/session
metadata and test convenience codecs use `ReadFile`; database payload paths must use `ReadFileRange`/`AppendFile`.
Offsets and follower durable high-water marks must be monotonic. While an fsync ACK is outstanding, every heartbeat
retransmission must retain the exact snapshot ID, request ID, offset, done flag, and bytes; it must not silently allocate
a new chunk identity. A lost final COMPLETE ACK must converge through the published follower's stale-complete response.
`SnapshotStoreTest.RejectsPayloadBeyondExperimentalFileLimitWithoutAllocation` fixes the formal file limit at 1 GiB.
Do not add online backup, incremental/COW capture, compression, resume, rate control, or throughput benchmarks to this
gate: the chunks exist to test Raft offsets and crash boundaries, not to claim large-database scalability.

## ASan/UBSan on WSL with an incompatible randomized address layout

Run normally first. Only after preserving the original failure and confirming all of the following may the validation
environment change: the host is WSL, the process died before `Running main`, stdout/stderr are empty, and the host log
identifies an ASan signal-handler/address-layout failure such as `overflowed sigaltstack`. Do not retry the individual
binary or failed node. Instead, run the entire component gate or entire E2E parent process tree exactly once in a fresh
artifact root under a stable child-only personality, for example:

```bash
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
  setarch x86_64 -R python3 test/e2e/raft_gtest_gate.py \
  /tmp/bustub-raft-build-clang /tmp/bustub-raft-component-logs-wsl

ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
  setarch x86_64 -R bash test/e2e/raft_m0_m7_chain.sh \
  /tmp/bustub-raft-build-clang 25100 /tmp/raft-m0-m7-chain-wsl
```

This is one full acceptance attempt in a diagnosed environment and must still report `process_retries=0`; it is not a
permission to hide a failed test. Any nonempty failure, sanitizer report, assertion, timeout, bind/protocol error, or
crash after a test starts remains final. Do not use this workaround for Release or native Linux CI. `setarch -R` changes
only the child process tree and does not disable ASLR system-wide.

## TSan on hosts with an incompatible randomized address layout

Run the core tests normally first. If, and only if, ThreadSanitizer exits before `Running main` with
`FATAL: ThreadSanitizer: unexpected memory mapping`, retry the same binaries with ASLR disabled for that child process:

```bash
TSAN_OPTIONS=halt_on_error=1:history_size=7 \
  setarch x86_64 -R /tmp/bustub-raft-build-tsan/test/tcp_transport_test --gtest_color=no

TSAN_OPTIONS=halt_on_error=1:history_size=7 \
  setarch x86_64 -R /tmp/bustub-raft-build-tsan/test/raft_node_test --gtest_color=no

TSAN_OPTIONS=halt_on_error=1:history_size=7 \
  setarch x86_64 -R /tmp/bustub-raft-build-tsan/test/session_table_test --gtest_color=no

TSAN_OPTIONS=halt_on_error=1:history_size=7 \
  setarch x86_64 -R /tmp/bustub-raft-build-tsan/test/bustub_state_machine_test --gtest_color=no

TSAN_OPTIONS=halt_on_error=1:history_size=7 \
  setarch x86_64 -R /tmp/bustub-raft-build-tsan/test/distributed_node_test --gtest_color=no
```

The TSan core label includes `session_table_test` for M8 fingerprint/session locking and `distributed_node_test` because
it exercises production tick, listener, client-worker, snapshot, restart, and `Stop()`/join concurrency that the
deterministic single-threaded Raft harness does not create.

Do not use this retry for a data-race report, assertion, timeout, or failure after a test starts. `setarch -R` changes
only the child process personality; it does not change a system-wide kernel setting.

## Reproducing a failure

1. Use a fresh artifact root and port base; never reuse a production directory.
2. Record the exact command, compiler, sanitizer options, test filter, and (for randomized tests) seed.
3. Preserve `node-N.log`, `node-N/raft/HARD_STATE`, `node-N/raft/log/LOG-MUTATIONS`, the snapshot directory, and the
   failing client output. Do not edit the only copy.
4. Compare each node's term, `commit_index`, `last_applied`, `published_applied_index`, and `snapshot_base_index` before
   inspecting physical database pages. Page IDs and derived index layouts are not cross-node invariants.
5. Copy a retained failure artifact outside the source tree. Delete it after the defect is closed or after a handoff has
   recorded its location and purpose.

An empty ASan pre-main exit is still a failed attempt: preserve it and diagnose it. If it matches the exact WSL address-
layout condition above, use that section's one-shot whole-gate environment; otherwise move the same revision to a
suitable sanitizer host. Never rerun an individual failed binary or node launch. Assertions, sanitizer reports,
timeouts, rejected SQL, bind failures, and protocol failures are never retried.

## Snapshot corruption procedure

Stop the target node before modifying its test directory. Truncate only the highest numbered formal `SNAPSHOT-*` file,
not `CURRENT`, the previous generation, or the bridge log. Start that node while peers remain stopped and issue an
explicit stale query. The matrix independently parses both snapshot headers before truncation, proves the latest file
is durably reduced to an invalid 16-byte prefix, then requires the recovered `snapshot_base_index` to equal the parsed
previous index. Recovery must install that generation, replay through the durable commit index, and expose no listener
before that finishes. This procedure belongs in the external harness;
there is intentionally no production "corrupt", "force snapshot", or "skip fsync" endpoint.

## Cleanup

Build trees belong in `/tmp` (or another operator-selected directory outside the repository). At a completed milestone,
audit with:

```bash
git status --short --untracked-files=all
git clean -ndX
du -sh . /tmp/bustub-raft-build-clang /tmp/bustub-raft-build-tsan
rg -n "test/include|gtest|InMemoryRaftTransport|ManualClock" src tools
```

Remove only exact, reviewed test artifact roots and core files. Keep source, registered tests, reusable harnesses,
protocol/operations documents, explicit acceptance records, and checked-in fixtures whose provenance is known.
Generated-file scans must distinguish tracked files; do not delete legitimate names such as `core.h` or checked-in XML
merely because a broad pattern matches. Conversely, tracked status alone is not proof that a file is a fixture: the M8
provenance audit found an unreferenced obsolete nested source duplicate and four tracked root runtime/diagnostic outputs,
which `2a1d2ce` removed after explicit scope approval. Never recursively delete the repository root or a path resolved
only through an unchecked variable/glob.

Before deletion, enumerate all task-owned candidates (for example, `find /tmp -maxdepth 1 -type d -name 'bustub*'`),
resolve and review every printed path, and record its size. Delete only that explicit list; do not turn the enumeration
pattern into a broad recursive removal command. Re-run the enumeration, process scan, and source-tree generated-file
scan after cleanup.
