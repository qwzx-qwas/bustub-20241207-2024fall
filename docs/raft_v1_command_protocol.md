# BusTub Raft V1 command protocol

This document describes the committed-byte contract implemented by `src/distributed`. It is deliberately narrower than
the SQL surface: a leader prepares one autocommit write statement against committed state, records a canonical logical
mutation batch, and followers only decode and apply that batch.

## Framing and compatibility

Every multibyte integer is unsigned big-endian on the wire. Signed integer values preserve their two's-complement bit
pattern inside that unsigned field. Strings and blobs are `u32 length` followed by exactly that many bytes.

A `TransactionCommandBatch` frame is:

```text
8 bytes  magic = "BCMDBAT1"
u32      format_version = 1
u32      payload_size
bytes    payload
u32      CRC32C(payload)
```

The payload is `client_id:u64`, `request_id:u64`, `expected_start_schema_epoch:u64`, `command_count:u32`, then repeated
`command_type:u32`, `command_body_size:u32`, `command_body`. The maximum payload is 64 MiB and the maximum count is
1,000,000. V1 rejects an unknown frame version, command tag, enum value, trailing bytes, non-canonical encoding, length
mismatch, or checksum mismatch; it does not attempt a best-effort downgrade. A future format therefore needs a new
version and an explicit cluster upgrade gate before any node can propose it.

Command tags are fixed as `CREATE_TABLE=1`, `CREATE_INDEX=2`, `INSERT_ROW=3`, `UPDATE_ROW=4`, and `DELETE_ROW=5`.
`CREATE_TABLE` carries both `table_oid` and `primary_index_oid`; `CREATE_INDEX` carries `index_oid` and `table_oid`.
Followers never allocate replacement OIDs. A V1 batch contains either one DDL command, a canonical DML list, or zero
DML commands for a committed zero-row statement. DDL and DML cannot be mixed.

## Logical row identity and tuple bytes

Every DML command identifies a row with `{codec_version:u32, type:u32, key_blob}` rather than a RID:

- `INTEGER`: exactly four bytes, signed two's-complement big-endian.
- `BIGINT`: exactly eight bytes, signed two's-complement big-endian.
- `VARCHAR`: `u32 byte_length` plus raw bytes; equality is byte equality and ordering is unsigned lexicographic byte
  order. There is no locale folding, Unicode normalization, or trailing-space folding.

Canonical DML order is `(table_oid, signed numeric key or raw VARCHAR key)`. A batch may mutate a logical key at most
once. Complete tuples have their own V1 frame: version, column count, then for each column its type, null marker, and
stable value bytes. UPDATE and DELETE also carry the complete expected old tuple and its expected Raft-derived commit
timestamp. UPDATE additionally carries the complete replacement tuple.

## SQL-to-command mapping

| Autocommit SQL | Prepared command | Proposal-time work |
|---|---|---|
| `CREATE TABLE` | one `CREATE_TABLE` | Bind schema; require one supported non-null single-column primary key; reserve candidate table/primary-index OIDs without publishing them. |
| `CREATE INDEX` | one `CREATE_INDEX` | Resolve table/columns/type and candidate OID; require ordinary non-unique semantics. |
| `INSERT ... VALUES` | zero or more `INSERT_ROW` | Privately evaluate VALUES/projection expressions, encode complete tuples, and reject existing or duplicate primary keys. |
| `UPDATE ... WHERE` | zero or more `UPDATE_ROW` | Scan committed rows, evaluate predicate and target expressions privately, capture old tuple/timestamp, and reject primary-key changes. |
| `DELETE ... WHERE` | zero or more `DELETE_ROW` | Scan committed rows, evaluate the predicate privately, and capture old tuple/timestamp. |

Only one statement is accepted per request. INSERT is limited to a private VALUES source; UPDATE and DELETE are limited
to one base table and a deterministic filter. A zero-row DML still commits an empty batch so request deduplication has a
stable result. V2 must define a transaction-wide read set, write set, schema/OID allocation rules, and one atomic
publication boundary before enabling multi-statement transactions or DDL followed by DML in one batch.

## Apply, visibility, and responses

The committed Raft index is the row `commit_ts`. `BusTubStateMachine::Apply` owns the exclusive
`StateVisibilityLatch` while it updates rows, every derived index, Catalog/schema epoch, SessionTable, and finally both
`published_applied_index` and `last_applied`. A SQL read owns the shared latch for its entire execution and is created
with `TransactionManager::BeginReadAt(published_applied_index)`. Constraint/precondition drift in a committed batch is a
fail-stop error, not a transaction abort.

The exact cached write response is 32 big-endian bytes:

```text
format_version:u32 = 1
status:u32 = COMMITTED(1)
request_id:u64
entry_term:u64
commit_index:u64
```

For each nonzero `client_id`, request IDs start at 1 and exactly increment by one. Repeating the latest request returns
the byte-identical cached response (including its original term and index); an older request or a gap is rejected. The
SessionTable and complete response bytes are part of every canonical snapshot.

## Proposal-time rejection list

The following conditions must not append a Raft entry, advance a public OID, change a page/index, or update a session:

- parse/bind/type errors, multiple statements, and non-write statements at the write endpoint;
- missing, nullable, composite, multiple, or non-`INTEGER`/`BIGINT`/`VARCHAR` primary keys;
- `CREATE UNIQUE INDEX`, secondary UNIQUE constraints, or a recovered V1 Catalog containing them;
- missing tables/columns, duplicate object names, unsupported index types/options, or an index key larger than the V1
  64-byte scalar-key adapter limit;
- primary-key collision, request gaps/old IDs, schema epoch mismatch, or explicit OID mismatch;
- primary-key UPDATE, an UPDATE/DELETE old tuple or old commit timestamp mismatch, and duplicate logical keys in one
  batch;
- unsupported INSERT sources, multi-table mutations, and values that cannot be represented by the stable tuple codec.

Non-deterministic expressions may only be accepted when the leader evaluates them during private prepare and stores the
resulting constants in the command. Apply never evaluates SQL, reads a clock/random source, or depends on RID/page order.
