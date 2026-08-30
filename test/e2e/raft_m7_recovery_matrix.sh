#!/usr/bin/env bash

set -euo pipefail

build_dir=${1:-/tmp/bustub-raft-build-clang}
port_base=${2:-21100}
artifact_root=${3:-"/tmp/bustub-raft-m7-recovery-matrix-$$"}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

run_minority_and_fresh_read() (
  source "${script_dir}/raft_process_harness.sh"
  raft_harness_init "${build_dir}" "${port_base}" "${artifact_root}/minority" m7-minority
  for node_id in 1 2 3
  do
    raft_start_message_proxy "${node_id}" "$((port_base + 300 + node_id))" \
      "${artifact_root}/minority/proxy-${node_id}"
  done
  raft_timeline_step "E2E-02/06/09: prepare one live Leader and majority-preserving message proxies"
  raft_start_all_nodes
  leader=$(raft_find_leader)
  raft_client_call_eventually "${leader}" write --client-id 1000 --request-id 1 \
    --sql "CREATE TABLE minority(id int PRIMARY KEY, value varchar(32));"
  raft_timeline_step "E2E-09 prerequisite: complete one fresh ReadIndex round before isolating the live Leader"
  fresh_before_partition=$(raft_client_call_strict "${leader}" read --request-id 100000 \
    --consistency linearizable --sql "SELECT count(*) AS row_count FROM minority;")
  raft_assert_query_exact "${fresh_before_partition}" $'row_count\n0'
  raft_timeline_step "E2E-02/09: bidirectionally isolate the live old Leader while peers retain a majority link"
  touch "${artifact_root}/minority/proxy-${leader}/drop"
  for node_id in 1 2 3
  do
    if [[ ${node_id} -ne ${leader} ]]
    then
      touch "${artifact_root}/minority/proxy-${node_id}/drop-from-${leader}"
    fi
  done
  raft_client_expect_status "${leader}" TIMEOUT write --client-id 1000 --request-id 2 \
    --sql "INSERT INTO minority VALUES (1, 'must-not-commit');"
  isolated_read=$(raft_client_expect_status "${leader}" TIMEOUT read --request-id 100001 \
    --consistency linearizable --sql "SELECT count(*) FROM minority;")
  if [[ ${isolated_read} == *"read_timestamp="* ]]
  then
    echo "isolated old Leader assigned a read timestamp without a fresh quorum" >&2
    exit 1
  fi

  if ! raft_node_alive "${leader}"
  then
    echo "isolated old Leader exited instead of remaining online" >&2
    exit 1
  fi
  new_leader=$(raft_find_leader "${leader}")
  absent=$(raft_client_call_eventually "${new_leader}" read --request-id 100002 --consistency linearizable \
    --sql "SELECT count(*) AS row_count FROM minority;")
  raft_assert_query_exact "${absent}" $'row_count\n0'
  committed=$(raft_client_call_eventually "${new_leader}" write --client-id 1000 --request-id 2 \
    --sql "INSERT INTO minority VALUES (1, 'committed-after-retry');")
  committed_index=$(raft_status_field "${committed}" committed_index)
  new_term=$(raft_status_field "$(raft_status "${new_leader}" 109000)" term)

  raft_timeline_step "E2E-06: heal the partition; old Leader must observe the higher term, step down, and reject a write"
  rm "${artifact_root}/minority/proxy-${leader}/drop"
  for node_id in 1 2 3
  do
    if [[ ${node_id} -ne ${leader} ]]
    then
      rm "${artifact_root}/minority/proxy-${node_id}/drop-from-${leader}"
    fi
  done
  demoted=0
  for round in $(seq 1 400)
  do
    if old_status=$(raft_status "${leader}" "$((109100 + round))")
    then
      old_term=$(raft_status_field "${old_status}" term)
      old_ready=$(raft_status_field "${old_status}" leader_ready)
      if [[ ${old_term} -ge ${new_term} ]] && [[ ${old_ready} -eq 0 ]] && \
         [[ ${old_status} == *"leader_id=${new_leader}"* ]]
      then
        demoted=1
        break
      fi
    else
      status=$?
      [[ ${status} -ne 70 ]] || exit 1
    fi
    sleep 0.02
  done
  if [[ ${demoted} -ne 1 ]] || ! raft_node_alive "${leader}"
  then
    echo "live old Leader did not demote after partition healing; last status: ${old_status:-unavailable}" >&2
    exit 1
  fi
  raft_client_expect_status "${leader}" NOT_LEADER write --client-id 1001 --request-id 1 \
    --sql "INSERT INTO minority VALUES (2, 'old-leader-must-reject');"
  raft_await_status_at_least "${leader}" last_applied "${committed_index}" 500 110000
  for node_id in 1 2 3
  do
    rows=$(raft_client_call_strict "${node_id}" read --request-id "$((110100 + node_id))" --consistency stale \
      --sql "SELECT id, value FROM minority ORDER BY id;")
    raft_assert_query_exact "${rows}" $'minority.id\tminority.value\n1\tcommitted-after-retry'
  done
)

run_full_restart() (
  local scenario_port=$((port_base + 1000))
  source "${script_dir}/raft_process_harness.sh"
  raft_harness_init "${build_dir}" "${scenario_port}" "${artifact_root}/restart" m7-restart \
    --snapshot-threshold-entries 4
  raft_timeline_step "E2E-07: publish a snapshot, retain a committed suffix, and stop every process"
  raft_start_all_nodes
  leader=$(raft_find_leader)
  raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 1 \
    --sql "CREATE TABLE restart_accounts(id int PRIMARY KEY, value varchar(32));"
  raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 2 \
    --sql "INSERT INTO restart_accounts VALUES (1, 'before');"
  raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 3 \
    --sql "CREATE INDEX restart_value ON restart_accounts(value);"
  raft_await_status_at_least "${leader}" snapshot_base_index 4 600 120000
  original=$(raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 4 \
    --sql "UPDATE restart_accounts SET value = 'after' WHERE id = 1;")
  original_bytes=$(raft_status_field "${original}" response_bytes)
  original_index=$(raft_status_field "${original}" committed_index)
  for node_id in 1 2 3
  do
    raft_await_status_at_least "${node_id}" last_applied "${original_index}" 400 "$((121000 + node_id * 1000))"
  done
  raft_stop_all_nodes

  raft_timeline_step "E2E-07: restart in order 3/1/2, deduplicate, then allocate and Apply new DDL"
  raft_start_node 3
  raft_start_node 1
  raft_start_node 2
  leader=$(raft_find_leader)
  replay=$(raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 4 \
    --sql "UPDATE restart_accounts SET value = 'after' WHERE id = 1;")
  if [[ $(raft_status_field "${replay}" response_bytes) != "${original_bytes}" ]]
  then
    echo "all-node restart changed the cached WriteResponseV1 bytes" >&2
    exit 1
  fi
  raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 5 \
    --sql "CREATE TABLE recovered_ledger(code bigint PRIMARY KEY, note varchar(32));"
  raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 6 \
    --sql "INSERT INTO recovered_ledger VALUES (10, 'ten');"
  last=$(raft_client_call_eventually "${leader}" write --client-id 1100 --request-id 7 \
    --sql "CREATE INDEX recovered_note ON recovered_ledger(note);")
  last_index=$(raft_status_field "${last}" committed_index)
  for node_id in 1 2 3
  do
    raft_await_status_at_least "${node_id}" last_applied "${last_index}" 500 "$((130000 + node_id * 1000))"
    accounts=$(raft_client_call_strict "${node_id}" read --request-id "$((134000 + node_id))" --consistency stale \
      --sql "SELECT id, value FROM restart_accounts ORDER BY id;")
    ledger=$(raft_client_call_strict "${node_id}" read --request-id "$((135000 + node_id))" --consistency stale \
      --sql "SELECT code, note FROM recovered_ledger ORDER BY code;")
    raft_assert_query_exact "${accounts}" $'restart_accounts.id\trestart_accounts.value\n1\tafter'
    raft_assert_query_exact "${ledger}" $'recovered_ledger.code\trecovered_ledger.note\n10\tten'
  done
)

run_corrupt_latest() (
  local scenario_port=$((port_base + 2000))
  source "${script_dir}/raft_process_harness.sh"
  raft_harness_init "${build_dir}" "${scenario_port}" "${artifact_root}/corrupt" m7-corrupt \
    --snapshot-threshold-entries 2
  raft_timeline_step "E2E-11: retain two snapshot generations and a bridge suffix"
  raft_start_all_nodes
  leader=$(raft_find_leader)
  raft_client_call_eventually "${leader}" write --client-id 1200 --request-id 1 \
    --sql "CREATE TABLE fallback(id int PRIMARY KEY, value varchar(32));"
  raft_client_call_eventually "${leader}" write --client-id 1200 --request-id 2 \
    --sql "INSERT INTO fallback VALUES (1, 'one');"
  raft_client_call_eventually "${leader}" write --client-id 1200 --request-id 3 \
    --sql "CREATE INDEX fallback_value ON fallback(value);"
  suffix=$(raft_client_call_eventually "${leader}" write --client-id 1200 --request-id 4 \
    --sql "UPDATE fallback SET value = 'from-bridge' WHERE id = 1;")
  suffix_index=$(raft_status_field "${suffix}" committed_index)
  for node_id in 1 2 3
  do
    raft_await_status_at_least "${node_id}" last_applied "${suffix_index}" 600 "$((140000 + node_id * 1000))"
  done
  for _ in $(seq 1 600)
  do
    snapshot_count=$(find "${artifact_root}/corrupt/node-1/raft/snapshots" -maxdepth 1 -type f \
      -name 'SNAPSHOT-[0-9]*' | wc -l)
    [[ ${snapshot_count} -ge 2 ]] && break
    sleep 0.02
  done
  [[ ${snapshot_count} -ge 2 ]]

  # A completed SNAPSHOT-* rename proves that the image is durable, but the
  # same Tick may still be advancing the retained bridge-log boundary.  Use a
  # production status request as a publication fence before measuring graceful
  # shutdown.  HandleStatus takes the node mutex held by MaybeCreateSnapshot,
  # so a successful reply proves that the complete snapshot transition has
  # returned; merely observing our own filesystem stimulus does not.
  if ! snapshot_publish_status=$(raft_status 1 149000 30000)
  then
    echo "node 1 did not answer the post-snapshot publication fence" >&2
    exit 1
  fi
  if [[ ! ${snapshot_publish_status} =~ ^status=OK([[:space:]]|$) ]] || \
     [[ $(raft_status_field "${snapshot_publish_status}" last_applied) -lt ${suffix_index} ]]
  then
    echo "post-snapshot publication fence did not cover the committed bridge: ${snapshot_publish_status}" >&2
    exit 1
  fi
  raft_stop_all_nodes
  snapshot_directory="${artifact_root}/corrupt/node-1/raft/snapshots"
  readarray -t snapshot_files < <(find "${snapshot_directory}" -maxdepth 1 -type f \
    -name 'SNAPSHOT-[0-9]*' | sort)
  previous_snapshot=${snapshot_files[$((${#snapshot_files[@]} - 2))]}
  latest_snapshot=${snapshot_files[$((${#snapshot_files[@]} - 1))]}
  previous_snapshot_index=$(python3 -c 'import pathlib, struct, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
if len(data) < 32 or data[:8] != b"BRSNAP01" or struct.unpack(">I", data[8:12])[0] != 1:
    raise SystemExit("invalid V1 Raft snapshot header")
name_size = struct.unpack(">I", data[20:24])[0]
offset = 24 + name_size
if offset + 8 > len(data):
    raise SystemExit("truncated V1 Raft snapshot header")
print(struct.unpack(">Q", data[offset:offset + 8])[0])' "${previous_snapshot}")
  latest_snapshot_index=$(python3 -c 'import pathlib, struct, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
name_size = struct.unpack(">I", data[20:24])[0]
print(struct.unpack(">Q", data[24 + name_size:32 + name_size])[0])' "${latest_snapshot}")
  if [[ ${previous_snapshot_index} -ge ${latest_snapshot_index} ]]
  then
    echo "snapshot generations do not have increasing included indexes" >&2
    exit 1
  fi
  python3 "${script_dir}/raft_corrupt_latest_snapshot.py" \
    "${snapshot_directory}" >"${artifact_root}/corrupt/corrupted-file.txt"
  corrupted_snapshot=$(<"${artifact_root}/corrupt/corrupted-file.txt")
  if [[ ${corrupted_snapshot} != "${latest_snapshot}" ]] || [[ $(stat -c '%s' "${corrupted_snapshot}") -ne 16 ]]
  then
    echo "newest snapshot was not durably truncated as requested" >&2
    exit 1
  fi

  raft_timeline_step "E2E-11: recover node 1 from the prior snapshot plus bridge log"
  raft_start_node 1
  raft_await_status_at_least 1 last_applied "${suffix_index}" 400 150000
  recovered_status=${RAFT_LAST_STATUS}
  if [[ $(raft_status_field "${recovered_status}" snapshot_base_index) -ne ${previous_snapshot_index} ]]
  then
    echo "node 1 did not select the independently parsed prior snapshot index ${previous_snapshot_index}" >&2
    exit 1
  fi
  recovered=$(raft_client_call_strict 1 read --request-id 151000 --consistency stale \
    --sql "SELECT id, value FROM fallback ORDER BY id;")
  raft_assert_query_exact "${recovered}" $'fallback.id\tfallback.value\n1\tfrom-bridge'
  raft_start_node 2
  raft_start_node 3
  raft_find_leader >/dev/null
)

run_atomic_batch_reads() (
  local scenario_port=$((port_base + 3000))
  # This scenario measures atomic visibility during a 1600-row Apply, not
  # election latency. Keep the production random source but give real
  # catalog/index work under sanitizer instrumentation the same bounded
  # heartbeat headroom as the canonical-capture scenario.
  RAFT_ELECTION_TIMEOUT_MIN_MS=2000
  RAFT_ELECTION_TIMEOUT_MAX_MS=4000
  source "${script_dir}/raft_process_harness.sh"
  raft_harness_init "${build_dir}" "${scenario_port}" "${artifact_root}/atomic" m7-atomic
  raft_timeline_step "E2E-12: read continuously while a 1600-row batch is prepared and applied"
  raft_start_all_nodes
  leader=$(raft_find_leader)
  raft_client_call_eventually "${leader}" write --client-id 1300 --request-id 1 \
    --sql "CREATE TABLE bulk(id int PRIMARY KEY, value varchar(32));"
  bulk_rows=1600
  insert_sql="INSERT INTO bulk VALUES "
  for row_id in $(seq 1 "${bulk_rows}")
  do
    [[ ${row_id} -eq 1 ]] || insert_sql+=","
    insert_sql+="(${row_id}, 'before')"
  done
  insert_sql+=";"
  raft_client_call_eventually "${leader}" write --client-id 1300 --request-id 2 --sql "${insert_sql}"

  raft_client_call_eventually "${leader}" write --client-id 1300 --request-id 3 \
    --sql "UPDATE bulk SET value = 'after' WHERE id <= ${bulk_rows};" \
    >"${artifact_root}/atomic/update.out" 2>"${artifact_root}/atomic/update.err" &
  update_pid=$!
  raft_register_aux_process atomic-update-client "${update_pid}"
  observations=0
  overlapping_read_starts=0
  for round in $(seq 1 80)
  do
    if ! kill -0 "${update_pid}" 2>/dev/null
    then
      break
    fi
    overlapping_read_starts=$((overlapping_read_starts + 1))
    observed=$(raft_client_call_eventually "${leader}" read --request-id "$((160000 + round))" \
      --consistency linearizable --sql "SELECT count(*) AS after_count FROM bulk WHERE value = 'after';")
    if [[ ${observed} != status=OK*$'\n'* ]]
    then
      echo "concurrent read did not return a query result: ${observed}" >&2
      exit 1
    fi
    logical_result=${observed#*$'\n'}
    if [[ ${logical_result} == $'after_count\n0' ]]
    then
      count=0
    elif [[ ${logical_result} == $'after_count\n'"${bulk_rows}" ]]
    then
      count=${bulk_rows}
    else
      echo "multi-row Apply exposed a non-literal result: ${logical_result}" >&2
      exit 1
    fi
    if [[ ${count} != 0 ]] && [[ ${count} != ${bulk_rows} ]]
    then
      echo "multi-row Apply exposed a partial count: ${count}" >&2
      exit 1
    fi
    printf '%s\n' "${count}" >>"${artifact_root}/atomic/read-counts.txt"
    observations=$((observations + 1))
    kill -0 "${update_pid}" 2>/dev/null || break
  done
  raft_wait_aux_process atomic-update-client 0
  if [[ ${observations} -eq 0 ]] || [[ ${overlapping_read_starts} -eq 0 ]]
  then
    echo "no read request was started while the update client was still in flight" >&2
    exit 1
  fi
  update_index=$(raft_status_field "$(<"${artifact_root}/atomic/update.out")" committed_index)
  final=$(raft_client_call_eventually "$(raft_find_leader)" read --request-id 161000 --consistency linearizable \
    --sql "SELECT count(*) AS after_count FROM bulk WHERE value = 'after';")
  raft_assert_query_exact "${final}" $'after_count\n'"${bulk_rows}"
  if [[ $(raft_status_field "${final}" read_timestamp) -lt ${update_index} ]]
  then
    echo "post-batch linearizable read timestamp is behind the committed batch" >&2
    exit 1
  fi
)

mkdir -p "$(dirname -- "${artifact_root}")"
if ! mkdir "${artifact_root}" 2>/dev/null
then
  echo "M7 recovery artifact root must not already exist: ${artifact_root}" >&2
  exit 1
fi
run_minority_and_fresh_read
run_full_restart
run_corrupt_latest
run_atomic_batch_reads
echo "M7 recovery/fault matrix E2E passed; artifacts: ${artifact_root}"
