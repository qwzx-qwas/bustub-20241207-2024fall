# Raft V1 requirement and test matrix

## Test layers

| Layer | Meaning | Primary targets |
| --- | --- | --- |
| Unit | Pure codec, durable record, invariant, and power-loss decisions | `*_codec_test`, `stable_store_test`, `log_store_test`, `snapshot_store_test`, recovery tests |
| Component | Multiple production modules with deterministic test-owned transport/storage | `raft_node_test`, `bustub_state_machine_test`, `raft_bustub_cluster_test` |
| TCP integration | Three production `DistributedNode` assemblies, stable client/Raft frames, real loopback TCP, and real per-node directories | `distributed_node_test`, `tcp_transport_test` |
| Process E2E | Formal `bustub-node` and `bustub-client` executables controlled only by one shared external process harness | `raft_process_harness.sh` + four focused fault timelines + `raft_m0_m7_chain.sh` |

The TCP integration target is production-like but is not described as a three-process test: its three nodes share the
GoogleTest process. The five shell timelines are the process boundary evidence. Low-cost algorithm combinations remain
in unit/component tests; process E2E is reserved for lifecycle, filesystem, signal, and formal CLI/protocol boundaries.

## E2E-01 through E2E-15

| ID | Required failure/property | Primary automated evidence | Layer | CI job |
| --- | --- | --- | --- | --- |
| E2E-01 | Valid primary-key/secondary-index batches replicate; invalid no-PK and secondary UNIQUE DDL have no side effect | `raft_m6_smoke.sh`; SQL admission component tests | Process + component | `raft-asan-ubsan`, `raft-process-release` |
| E2E-02 | Minority-only Leader suffix never commits and is overwritten | `raft_m7_recovery_matrix.sh`; `RaftNodeTest.OldLeaderConflictingSuffixIsReplaced` | Process + component | `raft-asan-ubsan`, `raft-process-release` |
| E2E-03 | Majority commit survives lost response; exact request retry is byte-identical | response-dropping proxy in `raft_m6_smoke.sh`; cluster retry component | Process + component | `raft-asan-ubsan`, `raft-process-release` |
| E2E-04 | Reopened lagging Follower catches up through AppendEntries | `DistributedNodeTest.TcpWriteReadLeaderChangeRetryAndReopenCatchup`; `raft_m6_smoke.sh` | TCP integration + process | `raft-asan-ubsan`, `raft-process-release` |
| E2E-05 | Compacted Follower installs the exact recorded multi-chunk Snapshot@S, then applies a strictly later suffix | `raft_m7_snapshot_transfer.sh`; `RaftNodeTest.CompactedFollowerCatchesUpBySnapshot` | Process + component | `raft-asan-ubsan`, `raft-process-release` |
| E2E-06 | A still-running isolated old Leader observes higher term after healing, steps down, rejects a fixed-endpoint write, and loses its conflicting suffix | online `drop`/`drop-from-N` topology in `raft_m7_recovery_matrix.sh`; component conflict tests | Process + component | `raft-asan-ubsan`, `raft-process-release` |
| E2E-07 | All nodes restart from snapshot + suffix; SessionTable/epoch/OIDs continue | `raft_m7_recovery_matrix.sh`; same-process restart integration | Process + TCP integration | `raft-asan-ubsan`, `raft-process-release` |
| E2E-08 | Explicit Follower stale read reports its published timestamp | `raft_m6_smoke.sh`; read timestamp unit test | Process + unit | `raft-asan-ubsan`, `raft-process-release` |
| E2E-09 | Isolated old Leader cannot reuse heartbeat ACK for a new ReadIndex | `raft_m7_recovery_matrix.sh`; fresh-context and one-way-loss RaftNode tests | Process + component | `raft-asan-ubsan`, `raft-process-release` |
| E2E-10 | `SIGKILL` during canonical capture recovers one complete generation, literal PK/index rows, Catalog identity, byte-stable Session state, and the dedicated pre-crash suffix rows | `raft_m7_snapshot_crash.sh`; `SnapshotManagerTest.PublicationPowerLossMatrix` | Process + power-loss unit | `raft-asan-ubsan`, `raft-process-release` |
| E2E-11 | Durably truncated newest snapshot selects the independently parsed previous index and replays its bridge to commit | external truncation in `raft_m7_recovery_matrix.sh`; component/unit fallback tests | Process + component + unit | `raft-asan-ubsan`, `raft-process-release` |
| E2E-12 | Multi-row batch is atomically visible to concurrent reads | continuously scheduled formal clients in `raft_m7_recovery_matrix.sh`; deterministic publication-latch gate in `BusTubStateMachineTest.ReaderBlocksUntilDataIndexSessionAndWatermarkPublishTogether`; worker/read overlap in `DistributedNodeTest.ConcurrentReadsObserveOnlyWholeMultiRowBatch` | Process + concurrency component + TCP integration | `raft-asan-ubsan`, `raft-process-release`, `raft-tsan-core` |
| E2E-13 | Current-term NOOP advances published watermarks before Leader serves | `raft_m6_smoke.sh`; RaftNode NOOP component | Process + component | `raft-asan-ubsan`, `raft-process-release` |
| E2E-14 | Snapshot preserves row commit timestamp needed by suffix UPDATE | Snapshot S + UPDATE S+1 in `raft_m7_snapshot_transfer.sh`; canonical snapshot unit test | Process + recovery unit | `raft-asan-ubsan`, `raft-process-release` |
| E2E-15 | Complete delayed old snapshot is a no-op and later Apply continues | complete-frame replay in `raft_m7_snapshot_transfer.sh`; stale-guard component | Process + component | `raft-asan-ubsan`, `raft-process-release` |

## Durable and deterministic contracts

| Contract | Primary tests |
| --- | --- |
| CommandLog/StableStore/LogStore use synchronous durability APIs: successful return means durable and failure throws directly | `CommandLogTest.*`, `StableStoreTest.*`, `LogStoreTest.*`, `SingleNodeRuntimeTest.*`, `RaftNodeTest.*` |
| Failed term persistence or local append cannot emit dependent RPC; InstallSnapshot crash composition is old-or-new across Snapshot/HardState/Log/FSM | `RaftNodeTest.FailedElectionTermPersistenceSendsNoVoteRequest`, `FailedHigherTermPersistenceSendsNoRpcResponseForEveryEntryPoint`, `FailedLocalProposalAppendSendsNothingAndCannotCommit`, `InstallSnapshotCrashMatrixRecoversOnlyCompleteOldOrNewState` |
| At most one unresolved SQL proposal exists per node; duplicates and unrelated clients share that gate across timeout, leadership loss, overwrite, retry at another index, and snapshot reconciliation | `RaftNodeTest.RejectsASecondProposalUntilTheFirstProposalIsResolved`, `DistributedNodeTest.ConcurrentWritesShareOneUnresolvedProposalGate`, `DistributedNodeTest.LiveOldLeaderClearsOverwrittenProposalAfterRetryCommitsElsewhere` |
| Named `before_write / after_fsync / after_rename / after_dir_fsync` faults recover only a complete old or complete new state through one shared oracle; every scenario must match its literal event kind/path/order topology, and fsync of one directory cannot publish a sibling entry | `PowerLossStorageTest.DirectorySyncDoesNotPublishSiblingEntries`, `SnapshotManagerTest.PublicationPowerLossMatrix`, `StableStoreTest.NamedPowerLossMatrix`, `CommandLogTest.NamedPowerLossMatrix`, `LogStoreTest.ReplaceSuffixNamedPowerLossMatrix`, `LogStoreTest.InstallSnapshotBaseNamedPowerLossMatrix` |
| Shared versioned/checksummed framing rejects bad magic, version, length, truncation, payload corruption, and CRC without changing protocol-specific bytes | `VersionedFrameTest.*`, `CommandBatchCodecTest.*`, `ClientProtocolTest.*`, `RaftRpcCodecTest.*`, `CatalogSnapshotTest.*`, `StateManifestTest.*`, `StableStoreTest.*` |
| Fixed V1 wire/disk layouts cannot drift through matching encoder/decoder bugs | Independent golden frames in `LogCodecTest`, `CommandBatchCodecTest`, `ClientProtocolTest`, `RaftRpcCodecTest`, `SessionTableTest`, `StableStoreTest`, `StateManifestTest`, and `CatalogSnapshotTest` |
| Stable term/vote/commit generation is atomic | `StableStoreTest.*` |
| Append/ReplaceSuffix/snapshot-base records reject corrupt or torn state | `LogStoreTest.*`, `CommandLogTest.*` |
| A damaged committed replacement cannot revive an obsolete suffix; rebuilding the journal is allowed only from an independently verified snapshot boundary and uses the same atomic named-fault oracle | `LogStoreTest.CommittedReplacementDamageCannotReviveOldSuffix`, `LogStoreTest.VerifiedSnapshotRebuildIsExplicitAndStrict`, `LogStoreTest.VerifiedSnapshotRebuildNamedPowerLossRetryMatrix` |
| Physical log is bounded without deleting the previous snapshot bridge | `LogStoreTest.SnapshotBasePhysicallyCompactsJournal`, `RaftNodeTest.CorruptLatestSnapshotRecoversPreviousPlusBridgeLog` |
| CURRENT and immutable snapshot validation/fallback | `SnapshotStoreTest.*`, `StateManifestTest.*`, `SnapshotManagerTest.*` |
| Snapshot fallback may repair a damaged log bridge only when the fully decoded latest generation exactly covers the durable commit; recovery then retains that verified latest generation alone | `DistributedNodeTest.CorruptLatestSnapshotFallsBackAndReplaysBridge`, `DistributedNodeTest.FullyCoveringLatestSnapshotRebuildsDamagedBridgeLatestOnly`, `RaftNodeTest.CorruptLatestSnapshotRecoversPreviousPlusBridgeLog` |
| Snapshot capture is self-contained and preserves row timestamps | `CanonicalSnapshotTest.*`, `CatalogSnapshotTest.*` |
| Formal snapshot capture, durable publication, 64 KiB transfer, follower staging, restart, and BusTub install use file slices/bounded chunks instead of a full payload vector | `SnapshotStoreTest.*`, `BusTubRaftStateMachineTest.CanonicalPayloadInstallAndSuffixApply`, `RaftNodeTest.CompactedFollowerCatchesUpBySnapshot`, `DistributedNodeTest.TcpSnapshotCatchupThenSuffixApply`, `raft_m7_snapshot_crash.sh`, `raft_m7_snapshot_transfer.sh` |
| A duplicate old snapshot chunk reports the follower's actual durable high-water; a heartbeat retains the active request identity; delayed fsync and lost final ACKs cannot starve, rewind, or reinstall a real multi-chunk snapshot | `SnapshotStoreTest.DuplicateOldChunkReportsDurableHighWater`, `RaftNodeTest.HeartbeatsCannotStarveMultiChunkSnapshotProgress` |
| File snapshots reject payload metadata beyond the 1 GiB experimental boundary without allocating it | `SnapshotStoreTest.RejectsPayloadBeyondExperimentalFileLimitWithoutAllocation`; compile-time equality with the BusTub bundle limit |
| Command/OID/key/tuple bytes are canonical | `CommandBatchCodecTest.*`, `PrimaryKeyCodecV1Test.*` |
| Same committed command sequence yields the same catalog/data/index/session state | `BusTubStateMachineTest.*`, `RaftBusTubClusterTest.*` |
| A response-lost entry that is durable on a follower but whose commit notification is lost becomes committed through the replacement Leader's NOOP; retry returns the literal cached response and applies the non-idempotent effect once | `RaftBusTubClusterTest.AmbiguousCommitNotificationLossStillRestoresExactOnceSessionOnNewLeader`; M6 and continuous-chain response-drop scenarios |
| A client cannot accept a valid but cross-wired response for another request; response-drop evidence checks both outer and inner request IDs | `ClientProtocolTest.DistributedClientRequiresResponseRequestCorrelation`; `raft_drop_response_proxy.py` in M6 and the continuous chain |
| Election deadlines use a bounded production random source and replayable tests | `RaftNodeTest.SeededElectionTimeoutSourceIsDeterministicAndBounded`, `RaftNodeTest.ElectionReplicationMajorityCommitAndApply`, `DistributedNodeTest.IsolatedLeaderTimesOutAndItsUncommittedSuffixIsReplaced` |
| A data directory has a durable immutable node/group/voter identity | `DistributedNodeConfigTest.NodeDirectoryIdentityIsDurableAndImmutable`; every process restart timeline reopens the same `node.conf` |
| Production and test dependencies point in one direction | stage-end source/dependency audit; `InMemoryRaftTransport` exists only in `test/include` |
| Ordered index scans consume owned key/RID values rather than references to temporary leaf-page return values | compile-time contract in `b_plus_tree_insert_test`; exact `ORDER BY` results in `distributed_node_test`, M6, recovery matrix, and the continuous M0-M7 chain |
| Independent database instances do not share WAL double-buffer state, and B+Tree root publication does not acquire a new parent while retaining child page latches | full `raft-tsan-core`; `DistributedNodeTest.ConcurrentReadsObserveOnlyWholeMultiRowBatch`; original `b_plus_tree_insert_test` |
| Transport shutdown discards queued frames and joins workers instead of draining stale backlog; test proxies restore terminable SIGTERM behavior even when inherited as ignored | `TcpRaftTransportTest.StopDiscardsQueuedFramesInsteadOfDrainingStaleBacklog`; full `raft-tsan-core`; all five process timelines and cleanup-status scan |
| One durable state crosses admission rejection, exact-once failover, snapshot + suffix, stale replay, identity rejection, full restart, and continued Catalog/index allocation | `raft_m0_m7_chain.sh`; explicit `check-raft-process-chain` CMake target |

## Gate commands

All builds are outside the source tree. See `raft_e2e_runbook.md` for the no-process-retry rule and artifact handling.

```bash
cmake --build /tmp/bustub-raft-build-clang --target \
  build-raft-component-gates bustub-node bustub-client -j2

python3 test/e2e/raft_gtest_gate.py \
  /tmp/bustub-raft-build-clang /tmp/raft-component-logs

cmake --build /tmp/bustub-raft-build-release --target sqllogictest -j2
ctest --test-dir /tmp/bustub-raft-build-release \
  --output-on-failure --parallel 2 --timeout 900 -L sqllogic

cmake --build /tmp/bustub-raft-build-tsan --target \
  tcp_transport_test raft_node_test bustub_state_machine_test distributed_node_test -j2
ctest --test-dir /tmp/bustub-raft-build-tsan --output-on-failure -L raft-tsan-core

bash test/e2e/raft_m6_smoke.sh /tmp/bustub-raft-build-clang 18100 /tmp/raft-m6
bash test/e2e/raft_m7_snapshot_crash.sh /tmp/bustub-raft-build-clang 19100 /tmp/raft-m7-crash
bash test/e2e/raft_m7_snapshot_transfer.sh /tmp/bustub-raft-build-clang 20100 /tmp/raft-m7-transfer
bash test/e2e/raft_m7_recovery_matrix.sh /tmp/bustub-raft-build-clang 21100 /tmp/raft-m7-matrix
bash test/e2e/raft_m0_m7_chain.sh /tmp/bustub-raft-build-clang 25100 /tmp/raft-m0-m7-chain

cmake --build /tmp/bustub-raft-build-clang --target check-raft-process-chain
```

The dated 2026-08-29 sections below are chronological evidence for their own revisions. In particular, references to
the former sanitizer startup-retry policy are not current: the present component runner and every node launch have one
process attempt. Only the final 2026-08-30 section is authoritative for the current worktree.

## Historical test-gap closure record (2026-08-29, superseded retry policy)

At that historical revision, the post-audit source passed 25 ASan/UBSan test executables containing 81 tests. Its final scripted run hit 17
host-specific empty pre-main SIGSEGV attempts and passed on strictly classified retries; no nonempty failure was
retried. The same runner first rejected a sandbox-denied TCP bind as a real test failure, proving that it does not retry
arbitrary exit codes or assertion failures. All four
formal process timelines then passed, giving process-boundary coverage for E2E-01 through E2E-15. The new process
evidence includes true client-response loss, full all-node restart, external one-way message loss, external newest-file
corruption, concurrent formal reads during that historical revision's 800-row fixture, snapshot catch-up, and delayed
replay of the exact old InstallSnapshot frames. TSan passed `raft_node_test` 15/15 and `bustub_state_machine_test` 4/4
under the documented child-only ASLR workaround. The earlier Release SQLLogicTest 40/40 result was not rerun because
this closure changes
Raft/recovery tests, node identity, harnesses, client diagnostics, and CI only—not single-node SQL execution semantics.
That revision of CI invoked the former retry-capable runner. The current runner executes all 26 binaries once, requires
a nonempty GoogleTest JSON execution count, and treats every pre-main exit 139 as a failed gate.
After recording the results, the 2.4 GiB out-of-tree builds, process artifacts, and logs were removed. At that revision,
the source tree contained no generated build/cache/core file or running Raft process; all 164 remaining worktree entries were formal
source, registered tests/test support, tools, or documentation.

## Historical M7 acceptance record (2026-08-29)

An earlier M7 revision was built out of tree with Clang 14.0.6. The following historical gates completed in the local
acceptance environment; later corrections are not retroactively covered by these numbers:

| Gate | Result |
| --- | --- |
| Existing GoogleTest executables under ASan/UBSan | 61 discovered; 60 valid executables passed, and only `trie_debug_test` was skipped because the course repository intentionally replaces the Gradescope answer with a throwing placeholder |
| Final M0-M7 ASan/UBSan target set after lint fixes | 23/23 executables passed; retries were permitted only for exit 139 before `Running main`, never for a test-body failure, sanitizer report, or timeout |
| Existing SQLLogicTest suite | 40/40 `.slt` files passed in one sorted Release run, including all leaderboard timing passes |
| Formal M6 process timeline | Passed with three `bustub-node` processes and the formal client: committed writes, ReadIndex, Leader loss, byte-stable retry, new-Leader write, and restarted old-node catch-up to index 7 |
| Formal M7 process timeline | Passed with three formal processes: snapshot base 2, capture-window `SIGKILL`, committed suffix through index 4, and offline recovery |
| TSan core | `raft_node_test` 7/7 and `bustub_state_machine_test` 4/4 passed with `halt_on_error=1` |

This ptrace-managed host exhibits nondeterministic ASan crashes before `main`; zero-test-body attempts were retried by
the bounded rule documented in the runbook. TSan initially rejected the host address layout with `unexpected memory
mapping`; running the same binaries under `setarch x86_64 -R` made the runtime start, after which all 11 tests passed
without a race report.

All 134 C/C++ files changed or added by this implementation pass clang-format 14 dry-run. All 82 newly added C/C++
files pass the repository cpplint configuration. The repaired CMake targets now invoke repository-owned Python scripts
through `Python3_EXECUTABLE`, so they no longer fail on their intentionally non-executable file mode. A full legacy-tree
format scan still reports 17 pre-existing course files outside the Raft change set; they were not mechanically rewritten
as part of this milestone.

## Randomized election-timeout correction (2026-08-29)

The production assembly now samples every election deadline from one configured inclusive interval and no longer
accepts a single fixed election timeout. Deterministic component tests inject either a fixed timeout source or an
explicit seed. Targeted Clang 14 ASan/UBSan verification passed `raft_node_test` 8/8, `node_config_test` 1/1,
`raft_bustub_cluster_test` 1/1, and the production-source TCP re-election case
`DistributedNodeTest.IsolatedLeaderTimesOutAndItsUncommittedSuffixIsReplaced` 1/1. No full-suite acceptance result above
is retroactively relabeled as having tested this later correction.

## Synchronous durability and named fault-model correction (2026-08-29)

The three storage boundaries no longer perform synchronous I/O, wrap the result in a ready `DurableFuture`, and make
callers immediately invoke `.get()`. `CommandLog`, `StableStore`, and `LogStore` now expose synchronous `void` methods:
normal return is the durability acknowledgement, while write/sync failures propagate directly. This keeps the Raft
ordering invariant explicit without pretending that V1 has an asynchronous storage scheduler.

The test-only `PowerLossStorage` records the four named durability events and their per-event occurrence. Snapshot
publication, HardState replacement, CommandLog append, LogStore suffix replacement, and snapshot-base installation
all replay every observed event through one old-or-new recovery oracle. The event model also distinguishes fsync of an
existing file from durability of a newly created directory entry, which requires a parent-directory fsync.

Targeted Clang 14 ASan/UBSan verification passed `stable_store_test` 3/3, `log_store_test` 6/6,
`command_log_test` 6/6, `snapshot_manager_test` 3/3, `single_node_runtime_test` 1/1, and `raft_node_test` 8/8.
The first attempts for `log_store_test` and `single_node_runtime_test` exited 139 with empty output before
`Running main`; each passed on the single permitted pre-main retry. No test-body failure or sanitizer report was retried.
All 14 C/C++ files touched by this correction pass Clang 14 format dry-run and the repository cpplint configuration.
The 811 MiB out-of-tree build was removed after verification and did not change the 155-item formal worktree inventory.

## Shared harness, framing, and streamed-snapshot correction (2026-08-29)

M6 and M7 now source one test-only process harness for node lifecycle, randomized election ranges, Leader discovery and
redirection, sanitizer startup retry, status waits, shutdown, and PID cleanup. The scenario files contain only their
timeline and assertions. M7 additionally waits for the prior snapshot generation before arming its capture observer,
so the `SIGKILL` is attached to the generation triggered by that revision's 800-row fixture rather than a stale
capture.

The common byte codec now owns the `magic/version/length/payload/CRC` and legacy `magic/body/CRC` validation skeletons.
Protocol-specific tags and exact existing bytes remain unchanged. Snapshot payload ownership is metadata plus an
on-disk slice: publication and recovery checksum fixed-size blocks, Raft sends 64 KiB chunks, follower staging appends
to a durable temporary file, and BusTub streams the database portion into a candidate generation before atomically
switching the FSM. Bounded catalog/session metadata may still be materialized for decoding.

Targeted Clang 14 ASan/UBSan verification passed 16 executables / 55 tests: shared framing and all migrated codecs,
StableStore/CommandLog/LogStore, snapshot stores, streamed BusTub FSM, Raft node, loopback distributed node, cluster,
and single-node recovery. Four attempts exited 139 with empty output before `Running main` and passed under the bounded
retry rule; the initial loopback run was rejected by the filesystem/network sandbox before port allocation and passed
6/6 when local loopback was allowed. No test-body or sanitizer failure was retried. That revision's M6 and M7 process
timelines both passed; M7 killed the observed capture owner and recovered the complete committed update with both peers
offline. All touched C/C++ files pass Clang 14 dry-run and repository cpplint, and all three shell files pass `bash -n`.
The final review also removed the aggregate snapshot methods from the formal BusTub FSM; its small compatibility
codec remains test-only while the FSM interface is file-only. The reviewed 1.5 GiB build and all eight process artifact
roots were removed after validation.

## Experimental snapshot-scope correction (2026-08-29)

The project now states its intended scale explicitly: it is a teaching BusTub instance with a simple static three-node
Raft layer, not a general-purpose production backup system. Full paused canonical snapshots and 64 KiB
InstallSnapshot chunks remain because they expose Raft offset, duplicate/partial transfer, publication-crash, and suffix
recovery behavior. Base-backup/WAL, online or fuzzy/COW snapshots, cross-process resume, compression, rate control,
parallel streams, pipeline, and low-pause performance gates are explicitly outside V1.

The formal file payload safety limit is 1 GiB, while the in-memory compatibility codec remains 128 MiB. A compile-time
assertion keeps `SnapshotStore` and the BusTub file-bundle limit equal. The new boundary test supplies only oversized
metadata, proving rejection before allocation or staging. Clang 14 ASan/UBSan targeted verification passed
`snapshot_store_test` 5/5, `raft_state_machine_test` 2/2, `raft_node_test` 8/8, and `distributed_node_test` 6/6. The TCP
binary's first empty attempt exited 139 before `Running main` and its only permitted retry passed. The M7 formal process
timeline also passed through committed index 4 and recovered all 800 rows in that historical fixture with both peers
offline; two initial node child attempts exited empty before service startup and were handled by the harness's bounded
startup retry. No test-body failure or sanitizer report was retried. The four touched C/C++ files pass Clang 14 format
dry-run and the
repository's configured cpplint gate.

## Prior production-oracle acceptance (2026-08-29, superseded by the final section)

This section was authoritative for that revision and is retained as a chronological record. The component runner at
that revision had no retry path: each executable had one process attempt,
must emit a nonempty GoogleTest JSON report, and fails on every signal, timeout, malformed report, or nonzero exit.
Request-level `eventually` calls in process scenarios are not test reruns: they may resend only the same stable
client/request identity after a named transport or leadership result, which is the production at-least-once delivery
surface being tested. Safety assertions and the continuous chain's critical transitions use `strict` one-process,
one-destination calls.

| Prior gate | One-shot result at that revision |
| --- | --- |
| Release M0-M7 component manifest | 26/26 binaries, 102 concrete tests, 0 failed, 0 disabled, `process_retries=0` |
| Original BusTub iterator regression | `b_plus_tree_insert_test` 3/3 after changing iterator dereference to an owning key/RID pair |
| M6 process timeline | Passed response loss, Leader kill/replacement, byte-identical exact-once retry, exact ordered rows, stale reads, and old-node catch-up |
| M7 capture-crash timeline | Passed with 1600 meaningful rows; killed the observed `capture-*` owner and recovered literal PK rows 1/2/1600, the named secondary lookup, Catalog identity, and byte-identical Session response without a second log entry |
| M7 transfer/replay timeline | Passed a recorded 3-chunk, 135,485-byte Snapshot@S with recorded `last_included_index == S < suffix`, then observed the matching stale-complete response and continued Apply |
| M7 recovery matrix | Passed online old-Leader demotion, full restart, durable truncation of the newest immutable snapshot with exact prior-index selection, bridge replay, and production-scheduled atomic reads |
| Continuous M0-M7 process chain | Passed on one durable state with a recorded 5-chunk, 266,737-byte Snapshot@12 term 2, suffix, stale replay, identity rejection, restart order 3/1/2, and post-recovery BIGINT PK/secondary-index use |

The oracle review also added independent fixed bytes for `BusTubSnapshotBundleV1` across aggregate encode/decode,
streamed `EncodeFiles`, and nonzero-offset `DecodeFile`; validates outer and inner committed request IDs plus successful
response/request-kind coherence in `DistributedClient`; proves a first Session gap creates no record; fixes
`IndexIterator::operator*` to return owned values; and makes proxy parse/invariant errors plus unexpected node, proxy, or
auxiliary-process exits fail the timeline. Codec expected values are handwritten literals and never constructed with
the production encoder or checksum under test.

The 26/102 result above was the **Release** result for that revision. At that time, the later oracle corrections had not
been relabeled as a new full ASan/UBSan, TSan, or SQLLogic acceptance run; those sanitizer, TSan, and 40/40 SQLLogic
numbers remain historical evidence only for their recorded revisions. CI builds the aggregate
`build-raft-component-gates` target and runs
all five timelines in both ASan/UBSan and Release jobs, so the newer revision will be revalidated there without a local
retry exception. The separate `sqllogic-release` job registers and runs every checked-in `.slt`; `raft-tsan-core` also
runs `distributed_node_test`, covering production tick/listener/client workers, snapshot/restart, and shutdown joins.

That cleanup reviewed and removed 49 task-owned `/tmp` build/log/artifact directories plus six zero-byte test
logs, totaling 2,154,049,193 bytes (2.006 GiB). The follow-up scan found no matching artifact, running node/client/proxy,
or source-tree build/cache/object/report/core output. The worktree then contained 169 formal entries: 64 tracked
modifications, 105 new source/test/tool/document files, and no deletions.

## Final M0-M7 acceptance (2026-08-30, authoritative)

This section is the acceptance record for the current worktree. The previous dated sections remain only as revision
history and must not be used as current test counts.

The final implementation review corrected per-instance WAL buffering, B+Tree root-publication lock order, transport
shutdown queue handling, and test-proxy SIGTERM inheritance. InstallSnapshot now owns one transfer per peer: a heartbeat
retransmits the same in-flight chunk and request identity, duplicate old chunks report the follower's actual durable
high-water, and only a matching monotonic ACK advances the transfer. The production-path component test uses a 192 KiB
patterned business value and real staging/fsync/publication/FSM installation. It holds an ACK across a heartbeat,
pre-stages two real chunks, drops the final COMPLETE ACK, exercises failure/restart/stale-complete convergence, verifies
the independent value, and then applies a suffix. It is neither an empty fixture nor a same-codec round trip.

| Final gate | Current one-shot result |
| --- | --- |
| Clang 14 ASan/UBSan component manifest | 26/26 binaries, 122/122 concrete tests; 0 failed/errors/disabled/not-run; 26 nonempty parseable JSON reports and logs; `process_retries=0`; no sanitizer marker |
| TSan core | `tcp_transport_test` 4/4, `raft_node_test` 17/17, `bustub_state_machine_test` 4/4, `distributed_node_test` 9/9; total 34/34; no race or lock-order report |
| Release SQLLogicTest | 40/40, 0 failed; `leaderboard-q1-index` completed normally in 669.72 seconds |
| ASan/UBSan five-process-scenario matrix | All five fresh scenarios passed once, 24 timeline steps total; no scenario/node retry, unexpected cleanup, process/port residue, or sanitizer/protocol report |
| Release five-process-scenario matrix | All five fresh scenarios passed once, 24 timeline steps total; no scenario/node retry, unexpected cleanup, process/port residue, or protocol report |
| Snapshot transfer evidence in both builds | Focused transfer: 3 chunks, 135,485 bytes, Snapshot@4 term 1; continuous durable-state chain: 5 chunks, 266,737 bytes, Snapshot@12 term 2 |
| Static gates | Full `src/` and `test/` Clang 14 format dry-run and repository cpplint passed; six shell files passed `bash -n`; four Python helpers passed AST parsing; `git diff --check` and production-to-test dependency scan passed |

The initial normal-layout sanitizer attempts were preserved and diagnosed before any environment change: on this WSL
host Clang 14 may fail before `Running main` with an empty log while the host reports `overflowed sigaltstack`. The final
ASan component gate and each whole E2E parent process tree ran once in fresh artifacts under child-only
`setarch x86_64 -R`, so `process_retries=0` remains literal. This workaround is forbidden for Release, native Linux CI,
or any nonempty/test-body/sanitizer/assertion/timeout/bind/protocol failure; the runbook records the exact boundary.

The stage cleanup removed 21 reviewed external build/log/artifact targets totaling 2,676,096,997 bytes (2.492 GiB).
The post-clean scan found no task-prefixed `/tmp` item, relevant process/listener, ignored source-tree artifact, or
suspicious untracked generated file. The final worktree has 208 formal entries: 103 tracked modifications, 105 new
source/test/tool/document files, and no deletions. The tracked nested 1,347-file course baseline was retained.

An independent read-only audit found no blocker under the stated crash-stop, non-Byzantine V1 model. M0-M7 are complete;
the execution stops at M7 and does not enter M8/V2 without a new user command.
