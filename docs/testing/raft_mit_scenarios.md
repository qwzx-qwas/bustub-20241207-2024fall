# MIT 6.5840-inspired Raft fault schedules

`test/raft/raft_fault_schedule_test.cpp` adds 16 component cases inspired by the election, agreement, persistence,
and compaction scenarios in the [MIT 6.5840 Raft lab](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html).
These are independently written C++ tests against BusTub's production Raft core, KV state machine, durable stores,
and recovery entry point. They complement the existing SQL, TCP, process, and filesystem power-loss tests.

## Scenario mapping

| MIT lab scenario family | BusTub test (suite prefix omitted) | Required result |
| --- | --- | --- |
| 3A: initial election | `InitialElectionRemainsStableWithoutFailures` | One ready leader; heartbeats keep the term stable for 3,000 logical ms |
| 3A: elections and quorum | `NoLeaderWithoutQuorumThenElectionAfterHealing` | Three isolated candidates cannot elect a leader; healing permits a committed write |
| 3A: repeated re-election | `RepeatedLeaderPartitionsElectAndRejoin` | Six leader isolations, majority writes, higher-term elections, and old-leader rejoin |
| 3B: no agreement without a majority | `NoAgreementWithoutMajorityThenRecovery` | An isolated leader's proposal is neither committed nor applied; healing preserves acknowledged data and restores writes |
| 3B: follower reconnection | `DisconnectedFollowerCatchesUpAfterManyCommands` | A follower misses 24 commands, then catches up to the same state |
| 3B: partitioned leader rejoin | `PartitionedLeaderLosesOnlyItsUncommittedSuffix` | The minority-only command disappears while both earlier and replacement-leader commits survive |
| 3C: unreliable agreement | `AgreementWithDroppedDuplicatedAndReorderedRpc` | Twenty commands reach all replicas with actual drops, duplicates, and out-of-order delivery |
| 3C: churn | `CrashAndPartitionChurnPreservesAcknowledgedCommands` | Alternating leader crashes and partitions preserve acknowledged commands through six replacements |
| 3D: snapshot installation and restart | `LaggingFollowerInstallsSnapshotOverUnreliableNetworkThenRestarts` | A stopped follower falls behind the retained log, installs a multi-chunk snapshot through faulty links, applies a suffix, and recovers it after another restart |
| 3D: all nodes restart | `FullClusterRestartPreservesSnapshotAndCommittedSuffix` | Two full cold restarts recover snapshot plus suffix and still accept new commands |

The three `RaftSeededFaultTest` scenarios each run with seeds `824`, `5840`, and `202409`: seven ordinary cases plus
nine parameterized cases. Seed names appear in GoogleTest and CTest output.

## Scheduling and assertions

The test owns the transport and advances every running node in 10 ms logical steps, without wall-clock sleeps.
Unreliable delivery independently drops 15% of sent messages, duplicates 10% of surviving messages, and delays each
copy by 0–60 logical ms. Later sends can overtake delayed messages. Both RPC directions use this scheduler.
Partitions also discard in-flight traffic across their boundary. Crashing a node discards its pending traffic and
volatile state; restarting reopens its original files with `RecoverRaftPersistentState` and a fresh state machine.
Each incarnation has its own monotonic clock origin.

The committed-history oracle survives restarts and compaction. After each tick, delivered RPC, proposal, or restart,
it checks monotonic terms and commit indices, at most one observed leader per term, contiguous nonduplicate Apply
within each incarnation, byte-identical entries at each applied index, matching published watermarks, and KV state
equal to a replay of the corresponding committed prefix. Explicit expected values separately check submitted commands.
A partition may leave leaders in different terms; the election assertion correctly permits that.

Leader discovery and replication waits have a 5,000 logical ms bound. Discovery waits for the highest observed term
and no unresolved proposal, since `LeaderReady()` alone only fences the current-term NOOP.
A submitted command is proposed once, and the
test checks its payload at that exact index on the required replicas. It is never silently resubmitted to another
leader or accepted with a smaller replica count. Deliberately unacknowledged partitioned proposals may be overwritten.
Failures include the seed, logical time, queued-message and fault counts, and every running node's term, role, commit,
applied, and last-log indices. Temporary directories are unique per cluster and removed on ordinary teardown.

## Run

From the repository root:

```bash
cmake -S . -B /tmp/bustub-mit-raft-build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14 \
  -DBUSTUB_SANITIZER=address,undefined
cmake --build /tmp/bustub-mit-raft-build --target raft_fault_schedule_test -j2
ctest --test-dir /tmp/bustub-mit-raft-build -L raft-mit-scenarios --output-on-failure
```

Select one reproducible scenario directly:

```bash
/tmp/bustub-mit-raft-build/test/raft_fault_schedule_test \
  --gtest_filter='FixedSeeds/RaftSeededFaultTest.CrashAndPartitionChurnPreservesAcknowledgedCommands/Seed5840'
```

The target also belongs to `build-raft-component-gates`, the `raft` CTest label, and the checked component manifest
in `test/e2e/raft_gtest_gate.py`, so existing component CI runs it. Reconfigure CMake after adding a test source.
For diagnosed sanitizer startup failures on WSL, follow the whole-gate procedure in
[the E2E runbook](raft_e2e_runbook.md); retain the failed attempt and do not retry individual failed tests to obtain a pass.

## Local verification (2026-09-09)

Clang 14 Debug with ASan/UBSan passed all 28 component binaries (162 cases), including the 16 new cases in 11.2 seconds.
CTest discovery selected exactly 16 cases with `raft-mit-scenarios`. Clang-format 14, repository-configured cpplint,
clang-tidy 14, and the component source/manifest consistency check passed.

The successful gate used child-only `setarch x86_64 -R` after diagnosing a WSL pre-main `overflowed sigaltstack` failure,
and an execution environment permitting loopback sockets after the sandbox returned `EPERM` on socket creation.
Earlier failed attempts were retained separately; the final component gate ran each binary once.

## Scope

BusTub currently requires exactly three voters and permits one unresolved proposal per leader. The tests retain these
contracts: they do not claim coverage of MIT's five/seven-node schedules, concurrent `Start()` API, long conflicting
uncommitted tails, RPC byte/count limits, or the full Figure 8 suite. Existing proposal-gate and SQL concurrency tests
cover this project's admission behavior. Snapshot transfer uses BusTub's chunked file protocol.

The seeded schedules test protocol interleavings in one thread, not thread safety or arbitrary fault schedules.
Filesystem operations complete before a simulated node crash; torn writes, real process termination, SQL retry
semantics, and linearizable reads remain covered by the separate tests listed in
[the Raft matrix](raft_test_matrix.md). Passing this suite is not a claim of passing MIT's original grader.
