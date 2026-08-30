# BusTub Raft V1 operations

## Build and static configuration

Build the production entry points with the same library used by the tests:

```bash
cmake -S . -B /tmp/bustub-raft-build-clang \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang-14 \
  -DCMAKE_CXX_COMPILER=clang++-14 \
  -DBUSTUB_SANITIZER=address,undefined
cmake --build /tmp/bustub-raft-build-clang --target bustub-node bustub-client -j2
```

Each node requires its own data directory, one Raft address, one client address, the same nonempty group ID, and exactly
two peers. A peer value includes both of that peer's addresses so a Follower can return a usable Leader client address.
For example, node 1 is:

```bash
/tmp/bustub-raft-build-clang/bin/bustub-node \
  --node-id 1 \
  --group-id demo \
  --data-dir /var/lib/bustub/node-1 \
  --raft-listen 127.0.0.1:7101 \
  --client-listen 127.0.0.1:7201 \
  --peer 2=127.0.0.1:7102,127.0.0.1:7202 \
  --peer 3=127.0.0.1:7103,127.0.0.1:7203 \
  --election-timeout-min-ms 300 \
  --election-timeout-max-ms 600 \
  --snapshot-threshold-entries 10000
```

Nodes 2 and 3 use their own IDs/directories/listen addresses and list the other two peers. All three nodes may and
normally should use the same election interval: every node independently samples an inclusive timeout from that
interval at startup and whenever its election deadline is reset. The defaults are `[300,600]` ms; the minimum must be
greater than twice the heartbeat interval, and the maximum must be greater than the minimum. Operators must not
manufacture collision avoidance by assigning a different fixed constant to each node. `NodeDirectory` holds an
exclusive `LOCK`, so a second process cannot open the same directory. Startup
reconstructs the working database from the latest authoritative snapshot and committed log before opening the client
listener. `SIGINT` and `SIGTERM` stop both listeners and join worker threads; `SIGKILL` recovery uses the same authority
on the next start and discards the old working database.

`--snapshot-threshold-entries` is a normal production compaction policy, not a test hook. It defaults to 10,000 and
must be non-zero. A node snapshots only at a stable boundary where
`commit_index == last_applied == published_applied_index`; write admission pauses while the canonical logical state is
captured. The capture includes Catalog/OID allocators, rows with their original latest commit timestamps, index
definitions, schema epoch, and SessionTable. Derived index pages and the disposable working database are not authority.

Stable storage keeps at most two valid immutable snapshot generations. The Raft log's physical recovery base stays at
the older retained generation, so the older snapshot has a continuous bridge through the current committed index. A
new local generation is published before that bridge is compacted. A remotely installed snapshot is allowed to leave
only one generation when the old bridge cannot be proven. Startup validates `CURRENT` and the named generation; if the
newest image is damaged it selects the previous checksummed generation and replays the retained bridge. Temporary
capture/download files and an unpublished torn generation are never mixed into recovered state.

## Client commands

Status, writes, linearizable reads, and explicit Follower stale reads use the stable binary client protocol:

```bash
/tmp/bustub-raft-build-clang/bin/bustub-client status \
  --endpoint 127.0.0.1:7201 --request-id 100

/tmp/bustub-raft-build-clang/bin/bustub-client write \
  --endpoint 127.0.0.1:7201 --client-id 44 --request-id 1 \
  --sql "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));"

/tmp/bustub-raft-build-clang/bin/bustub-client read \
  --endpoint 127.0.0.1:7201 --request-id 101 --consistency linearizable \
  --sql "SELECT id, name FROM accounts ORDER BY id;"

/tmp/bustub-raft-build-clang/bin/bustub-client read \
  --endpoint 127.0.0.1:7202 --request-id 102 --consistency stale \
  --sql "SELECT count(*) FROM accounts;"
```

`NOT_LEADER` includes the known `leader_id` and `leader_address`. Status also exposes `leader_ready`; clients route
writes and linearizable reads only when it is true, which means the current-term NOOP has committed and applied. A write is `COMMITTED` only after the Leader's local
FSM has applied its log index; its payload is the exact `WriteResponseV1` also stored in the replicated SessionTable.
Retry the latest uncertain write with the same `(client_id, request_id)` and SQL at the new Leader. The returned payload
retains the original entry term and index byte-for-byte. A request ID gap or older-than-last ID is `REJECTED`.

A linearizable read is accepted only after the current-term Leader NOOP is committed and applied. Every request starts
a new `{term, read_context}` AppendEntries probe; only matching ACKs from a current majority produce `R=commit_index`.
The node then reads at a held-latch `P` satisfying `R <= P <= last_applied`. Ordinary heartbeat ACKs and completed old
contexts are not reusable. A stale read is allowed on any healthy node only when explicitly requested; its response
states `read_timestamp = published_applied_index` and never claims linearizability.

Status and every response include `snapshot_base_index`. This is the oldest retained recovery boundary—the point below
which AppendEntries is unavailable—not necessarily the newest snapshot index. It advances only after the newer
generation is durable and the old generation is pruned. Monitor it together with `commit_index`, `last_applied`, and
`published_applied_index`; all three applied/commit watermarks must be monotonic.

## Wire framing

All integers are unsigned big-endian. Both protocols use a 16-byte prefix (`8-byte magic`, `u32 version`, `u32 payload
size`) followed by the payload and `u32 CRC32C(payload)`. Decoders reject unknown versions/tags, invalid enum/boolean
values, trailing bytes, non-canonical re-encoding, oversized frames, and checksum errors.

The prefix and checksum checks are implemented once by the common byte codec and reused by CommandBatch, client/Raft
wire frames, Catalog, Manifest, HardState, and Session records. Protocol types and magic values remain separate. Legacy
snapshot/query-result formats that already use `magic + typed body + CRC(body)` share only the checksummed-frame helper;
their persisted bytes and version fields were not silently rewritten.

- `BRAFT001` payload begins with `from:u64`, `to:u64`, `group_id:string`, and a Raft message tag/body. TCP transport
  also checks the configured peer identity, destination, and exact group ID. AppendEntries carries an optional read
  context that a current-term Follower echoes in its response.
- `BCLNT001` carries exactly one tagged write/read/status request or one response. A response includes request/node,
  `leader_ready`, optional Leader identity, term, commit/last-applied/published watermarks, `snapshot_base_index`,
  optional read timestamp, and a bounded byte payload. SELECT rows use the versioned, checksummed `BQRES001`
  column/row codec.

The command batch and exact write response formats are specified in `docs/raft_v1_command_protocol.md`.

## Experimental snapshot boundary

The formal snapshot path is file-backed. `RaftSnapshot` contains identity, index/term, length, and checksum, while
its bytes remain in an immutable `SNAPSHOT-*` file. The Leader reads at most 64 KiB for each `InstallSnapshot` RPC; the
Follower validates offsets, appends and synchronizes a staging file, verifies the final checksum, publishes the formal
generation, and passes a file slice to the BusTub FSM. The FSM streams `db.bustub` into a candidate generation and only
materializes bounded Catalog and Session metadata. This removes the old aggregate 128 MiB vector ceiling from the
formal path without changing Raft's full-state `lastIncludedIndex/Term` model.

This is an experimental protocol boundary, not an online-backup or large-database feature. The full canonical capture
may pause writes and perform multiple sequential checksum/copy passes. V1 deliberately has no base-backup/WAL mode,
incremental/fuzzy/COW snapshot, compression, rate limiting, parallel streams, or cross-process transfer resume.

The in-memory bundle codec and KV example methods remain conveniences for small unit tests. The formal BusTub state
machine exposes only file creation/installation. The compatibility codec is capped at 128 MiB and the file payload at
1 GiB; both are defensive experiment limits, not supported-capacity claims.

## Reproducible process failure timelines

Run the production-binary smoke test outside a sandbox that blocks loopback sockets:

```bash
bash test/e2e/raft_m6_smoke.sh \
  /tmp/bustub-raft-build-clang 28300 /tmp/bustub-raft-m6-smoke-current

bash test/e2e/raft_m7_snapshot_crash.sh \
  /tmp/bustub-raft-build-clang 29100 /tmp/bustub-raft-m7-snapshot-crash-current

bash test/e2e/raft_m7_snapshot_transfer.sh \
  /tmp/bustub-raft-build-clang 30100 /tmp/bustub-raft-m7-snapshot-transfer-current

bash test/e2e/raft_m7_recovery_matrix.sh \
  /tmp/bustub-raft-build-clang 32100 /tmp/bustub-raft-m7-recovery-matrix-current
```

The M6 script starts three `bustub-node` binaries and drives only the formal client protocol. It commits DDL and multi-row DML, runs
a ReadIndex query, sends `SIGKILL` to the Leader, waits for the remaining majority's current-term NOOP, retries the
uncertain request ID, commits a DELETE, restarts the old node from its original directory, and waits for its
`commit_index/last_applied` to reach the new Leader. It preserves all three data directories and node logs at the
reported artifact path; the trap only stops processes started by that run.

The M7 script first creates Snapshot@2, commits a large suffix, watches the production working directory for the next
canonical capture, and sends `SIGKILL` to that script-owned Leader during the capture window. It then stops the two
healthy peers and starts only the killed node from its original directory. A stale query must show the complete
committed 1600-row update, proving recovery selected either the complete old generation plus bridge or the complete new
generation, never a mixture. The exact observed capture path and all node logs remain under the reported `/tmp`
artifact directory.

The snapshot-transfer script places test-owned TCP proxies in front of the formal Raft listeners, holds one Follower
behind Snapshot S, records the real InstallSnapshot frames, lets the Follower apply S+1, and then replays the complete
old transfer. The recovery-matrix script covers minority loss/fresh ReadIndex, all-node restart, offline newest-file
corruption, and concurrent formal reads during a 1600-row batch. These are external fault controls; the node has no
force-election, drop-message, corrupt-file, or skip-fsync API.

The complete requirement-to-test mapping and reproducible commands are in
`docs/testing/raft_test_matrix.md` and `docs/testing/raft_e2e_runbook.md`.
