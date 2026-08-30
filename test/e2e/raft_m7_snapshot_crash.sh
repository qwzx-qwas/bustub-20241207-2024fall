#!/usr/bin/env bash

set -euo pipefail

build_dir=${1:-/tmp/bustub-raft-build-clang}
port_base=${2:-29100}
artifact_root=${3:-"/tmp/bustub-raft-m7-snapshot-crash-$$"}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "${script_dir}/raft_process_harness.sh"
# ASan makes a full database snapshot much slower than a release build. Keep
# the timeout randomized, but use a deterministic test range wide enough that
# the capture itself does not manufacture unrelated elections.
RAFT_ELECTION_TIMEOUT_MIN_MS=2000
RAFT_ELECTION_TIMEOUT_MAX_MS=4000
raft_harness_init "${build_dir}" "${port_base}" "${artifact_root}" m7-snapshot-crash \
  --snapshot-threshold-entries 2

# Client status exposes the oldest retained recovery base. Read only the fixed
# index field of the V1 CURRENT record so this test can distinguish the latest
# complete generation from that intentionally older bridge base.
current_snapshot_index() {
  python3 -c 'import pathlib, struct, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
if len(data) < 28 or data[:8] != b"BRCURR01" or struct.unpack(">I", data[8:12])[0] != 1:
    raise SystemExit("invalid V1 Raft CURRENT record")
print(struct.unpack(">Q", data[20:28])[0])' "$1"
}

await_current_snapshot_exact() {
  local node_id=$1
  local expected=$2
  local current_path="${artifact_root}/node-${node_id}/raft/snapshots/CURRENT"
  local observed=unavailable
  for _ in $(seq 1 600)
  do
    if [[ -f ${current_path} ]] && observed=$(current_snapshot_index "${current_path}") && \
       [[ ${observed} -eq ${expected} ]]
    then
      return
    fi
    sleep 0.05
  done
  echo "node ${node_id} did not publish exact Snapshot@${expected}; latest=${observed}" >&2
  return 1
}

kill_during_capture() {
  local node_id=$1
  local round
  local candidate
  for round in $(seq 1 5000)
  do
    for candidate in "${artifact_root}/node-${node_id}/working/bustub-raft-fsm"/capture-*
    do
      if [[ -d ${candidate} ]]
      then
        printf '%s\n' "${candidate}" >"${artifact_root}/killed-capture.txt"
        kill -KILL "${RAFT_NODE_PIDS[${node_id}]}"
        return
      fi
    done
    sleep 0.002
  done
  echo "snapshot capture window was not observed" >&2
  return 1
}

raft_timeline_step "E2E-10: start production processes and force a low snapshot threshold"
raft_start_all_nodes

leader=$(raft_find_leader)
raft_client_call_eventually "${leader}" write --client-id 700 --request-id 1 \
  --sql "CREATE TABLE records(id int PRIMARY KEY, balance int, tag varchar(48));"
raft_await_status_at_least "${leader}" snapshot_base_index 2 300 200000

# Stay inside the V1 scalar-index adapter's 64-byte key limit while retaining
# enough actual tuple data to make capture observable in a Release process.
large_tail=abcdefghijklmnopqrstuvwxyzABCDEFGHI
insert_sql="INSERT INTO records VALUES "
for row_id in $(seq 1 1600)
do
  if [[ ${row_id} -ne 1 ]]
  then
    insert_sql+=","
  fi
  printf -v row_tag 'record-%04d-%s' "${row_id}" "${large_tail}"
  insert_sql+="(${row_id}, 10, '${row_tag}')"
done
insert_sql+=";"
insert_result=$(raft_client_call_eventually "${leader}" write --client-id 700 --request-id 2 --sql "${insert_sql}")
printf '%s\n' "${insert_result}"
if [[ ! ${insert_result} =~ committed_index=([0-9]+) ]]
then
  echo "large insert did not report its committed index" >&2
  exit 1
fi
insert_index=${BASH_REMATCH[1]}

# First publish a complete generation containing the real Catalog, secondary
# index, and a separate client's non-idempotent Session record. The later kill
# therefore tests recovery of all three payload components, not only row count.
leader=$(raft_find_leader)
index_result=$(raft_client_call_strict "${leader}" write --client-id 700 --request-id 3 \
  --sql "CREATE INDEX records_tag ON records(tag);")
index_index=$(raft_status_field "${index_result}" committed_index)
if [[ ${index_index} -le ${insert_index} ]]
then
  echo "secondary index did not commit after the bulk insert" >&2
  exit 1
fi
await_current_snapshot_exact "${leader}" "${index_index}"

session_result=$(raft_client_call_strict "${leader}" write --client-id 701 --request-id 1 \
  --sql "UPDATE records SET balance = balance + 7 WHERE id = 1;")
session_index=$(raft_status_field "${session_result}" committed_index)
session_response_bytes=$(raft_status_field "${session_result}" response_bytes)
padding_result=$(raft_client_call_strict "${leader}" write --client-id 702 --request-id 1 \
  --sql "UPDATE records SET balance = balance + 3 WHERE id = 2;")
published_snapshot_index=$(raft_status_field "${padding_result}" committed_index)
if [[ ${session_index} -le ${index_index} ]] || [[ ${published_snapshot_index} -le ${session_index} ]]
then
  echo "pre-crash Catalog/Session snapshot sequence is not strictly ordered" >&2
  exit 1
fi
leader=$(raft_find_leader)
await_current_snapshot_exact "${leader}" "${published_snapshot_index}"

# Add one suffix entry. With threshold two, only the following update can open
# the next capture generation observed by kill_during_capture.
pre_crash_result=$(raft_client_call_strict "${leader}" write --client-id 700 --request-id 4 \
  --sql "UPDATE records SET balance = balance + 11 WHERE id = 1600;")
pre_crash_index=$(raft_status_field "${pre_crash_result}" committed_index)
if [[ ${pre_crash_index} -le ${published_snapshot_index} ]]
then
  echo "pre-crash suffix did not commit after the complete snapshot" >&2
  exit 1
fi

raft_timeline_step "E2E-10: kill the Leader while canonical snapshot capture is observable"
kill_during_capture "${leader}" &
watcher_pid=$!
raft_register_aux_process snapshot-capture-watcher "${watcher_pid}"
# The node is intentionally killed while this request is in flight. Accept
# only success or a named transport ambiguity; a client crash, sanitizer
# report, rejection, or unrelated protocol status must still fail the test.
raft_client_call_allow_transport_ambiguity "${leader}" write --client-id 700 --request-id 5 \
  --sql "UPDATE records SET balance = balance + 1 WHERE id <= 1600;"
raft_wait_aux_process snapshot-capture-watcher 0
raft_stop_node "${leader}" KILL

for node_id in 1 2 3
do
  if [[ ${node_id} -ne ${leader} ]]
  then
    raft_stop_node "${node_id}"
  fi
done

raft_timeline_step "E2E-10: restart the killed node and validate complete durable state"
raft_start_node "${leader}"
expected_update_index=$((pre_crash_index + 1))
raft_await_status_at_least "${leader}" last_applied "${expected_update_index}" 300 300000

recovered_pk=$(raft_client_call_strict "${leader}" read --request-id 300 --consistency stale \
  --sql "SELECT id, balance, tag FROM records WHERE id = 1;")
printf -v tag_1 'record-%04d-%s' 1 "${large_tail}"
raft_assert_query_exact "${recovered_pk}" \
  $'records.id\trecords.balance\trecords.tag\n1\t18\t'"${tag_1}"

# An equality lookup on the named secondary key, plus rejection of recreating
# that name below, makes Catalog/index recovery observable independently of PK.
printf -v tag_517 'record-%04d-%s' 517 "${large_tail}"
recovered_secondary=$(raft_client_call_strict "${leader}" read --request-id 301 --consistency stale \
  --sql "SELECT id, balance, tag FROM records WHERE tag = '${tag_517}';")
raft_assert_query_exact "${recovered_secondary}" \
  $'records.id\trecords.balance\trecords.tag\n517\t11\t'"${tag_517}"

# Row 2 distinguishes the complete pre-crash snapshot (+3) from the original
# bulk image, while row 1600 additionally requires the committed bridge suffix
# (+11). Both also contain the +1 entry whose Apply opened the killed capture.
printf -v tag_2 'record-%04d-%s' 2 "${large_tail}"
printf -v tag_1600 'record-%04d-%s' 1600 "${large_tail}"
recovered_row_2=$(raft_client_call_strict "${leader}" read --request-id 302 --consistency stale \
  --sql "SELECT id, balance, tag FROM records WHERE id = 2;")
raft_assert_query_exact "${recovered_row_2}" \
  $'records.id\trecords.balance\trecords.tag\n2\t14\t'"${tag_2}"
recovered_row_1600=$(raft_client_call_strict "${leader}" read --request-id 303 --consistency stale \
  --sql "SELECT id, balance, tag FROM records WHERE id = 1600;")
raft_assert_query_exact "${recovered_row_1600}" \
  $'records.id\trecords.balance\trecords.tag\n1600\t22\t'"${tag_1600}"
recovered_count=$(raft_client_call_strict "${leader}" read --request-id 304 --consistency stale \
  --sql "SELECT count(*) AS row_count FROM records;")
raft_assert_query_exact "${recovered_count}" $'row_count\n1600'

# Restore quorum, then replay the exact last request of a client whose Session
# record was in the completed pre-crash snapshot. The response must be the
# original bytes and index, the retry must append nothing, and row 1 must not
# receive the non-idempotent +7 a second time.
for node_id in 1 2 3
do
  if [[ ${node_id} -ne ${leader} ]]
  then
    raft_start_node "${node_id}"
  fi
done
recovered_leader=$(raft_find_leader)
before_retry=$(raft_status "${recovered_leader}" 310)
session_retry=$(raft_client_call_strict "${recovered_leader}" write --client-id 701 --request-id 1 \
  --sql "UPDATE records SET balance = balance + 7 WHERE id = 1;")
after_retry=$(raft_status "${recovered_leader}" 311)
if [[ $(raft_status_field "${session_retry}" response_bytes) != "${session_response_bytes}" ]] || \
   [[ $(raft_status_field "${session_retry}" committed_index) -ne ${session_index} ]]
then
  echo "recovered Session did not return the byte-identical committed response" >&2
  exit 1
fi
if [[ $(raft_status_field "${before_retry}" commit_index) -ne \
      $(raft_status_field "${after_retry}" commit_index) ]]
then
  echo "Session retry appended a second log entry" >&2
  exit 1
fi

before_index_rejection=$(raft_status "${recovered_leader}" 312)
raft_client_expect_status "${recovered_leader}" REJECTED write --client-id 703 --request-id 1 \
  --sql "CREATE INDEX records_tag ON records(tag);" >/dev/null
after_index_rejection=$(raft_status "${recovered_leader}" 313)
if [[ $(raft_status_field "${before_index_rejection}" commit_index) -ne \
      $(raft_status_field "${after_index_rejection}" commit_index) ]]
then
  echo "duplicate recovered index name changed the Raft commit index" >&2
  exit 1
fi

post_retry=$(raft_client_call_strict "${recovered_leader}" read --request-id 314 --consistency linearizable \
  --sql "SELECT id, balance, tag FROM records WHERE id = 1;")
raft_assert_query_exact "${post_retry}" \
  $'records.id\trecords.balance\trecords.tag\n1\t18\t'"${tag_1}"

echo "M7 snapshot-crash binary E2E passed; artifacts: ${artifact_root}"
