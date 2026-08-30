#!/usr/bin/env bash

set -euo pipefail

build_dir=${1:-/tmp/bustub-raft-build-clang}
port_base=${2:-26100}
artifact_root=${3:-"/tmp/bustub-raft-m8-payload-binding-$$"}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "${script_dir}/raft_process_harness.sh"

# Start every voter before the first election deadline. A common production
# range also lets the test deterministically give an already-running voter an
# election head start without assigning fixed per-node timeout constants.
RAFT_ELECTION_TIMEOUT_MIN_MS=2000
RAFT_ELECTION_TIMEOUT_MAX_MS=3000
raft_harness_init "${build_dir}" "${port_base}" "${artifact_root}" m8-payload-binding \
  --snapshot-threshold-entries 8

for node_id in 1 2 3
do
  raft_start_message_proxy "${node_id}" "$((port_base + 300 + node_id))" \
    "${artifact_root}/proxy-${node_id}"
done

raft_log_mutations_fingerprint() {
  if [[ $# -ne 1 ]]
  then
    echo "raft_log_mutations_fingerprint requires a node ID" >&2
    return 1
  fi
  local path="${artifact_root}/node-$1/raft/log/LOG-MUTATIONS"
  local hash
  local size
  if [[ ! -f ${path} ]]
  then
    echo "missing durable Raft mutation journal: ${path}" >&2
    return 1
  fi
  size=$(stat -c '%s' -- "${path}")
  hash=$(sha256sum -- "${path}")
  printf '%s %s\n' "${size}" "${hash%% *}"
}

raft_require_stable_status() {
  if [[ $# -ne 2 ]]
  then
    echo "raft_require_stable_status requires a node ID and request ID" >&2
    return 1
  fi
  local output
  local commit
  output=$(raft_status "$1" "$2")
  commit=$(raft_status_field "${output}" commit_index)
  if [[ $(raft_status_field "${output}" last_applied) != "${commit}" ]] || \
     [[ $(raft_status_field "${output}" published_applied_index) != "${commit}" ]]
  then
    echo "node $1 is not at a stable published boundary: ${output}" >&2
    return 1
  fi
  printf '%s\n' "${output}"
}

raft_expect_payload_mismatch_strict() {
  if [[ $# -ne 6 ]]
  then
    echo "raft_expect_payload_mismatch_strict requires node, client ID, request ID, SQL, phase, and output path" >&2
    return 1
  fi
  local node_id=$1
  local client_id=$2
  local request_id=$3
  local sql=$4
  local phase=$5
  local output_path=$6
  local output
  local message
  output=$(raft_client_expect_status "${node_id}" REJECTED write --client-id "${client_id}" \
    --request-id "${request_id}" --sql "${sql}")
  if [[ ${output} != *$'\n'* ]]
  then
    printf '%s payload mismatch response has no message line:\n%s\n' "${phase}" "${output}" >&2
    return 1
  fi
  message=${output#*$'\n'}
  if [[ ${message} != "message=request payload does not match request identity" ]]
  then
    printf '%s payload mismatch text is not byte-exact:\n%s\n' "${phase}" "${output}" >&2
    return 1
  fi
  printf '%s\n' "${output}" >"${output_path}"
}

# Prove one changed retry is rejected at a fixed serving node without changing
# the stable Raft boundary, the durable journal bytes, or the literal business
# rows. Every client stimulus below is a one-shot strict call.
raft_assert_payload_mismatch_no_effect() {
  if [[ $# -ne 7 ]]
  then
    echo "raft_assert_payload_mismatch_no_effect requires phase, node, client ID, request ID, SQL, expected rows, and request base" >&2
    return 1
  fi
  local phase=$1
  local node_id=$2
  local client_id=$3
  local request_id=$4
  local changed_sql=$5
  local expected_rows=$6
  local request_base=$7
  local after_hash
  local after_query
  local after_size
  local after_status
  local before_hash
  local before_query
  local before_size
  local before_status
  local field

  before_status=$(raft_require_stable_status "${node_id}" "${request_base}")
  before_query=$(raft_client_call_strict "${node_id}" read --request-id "$((request_base + 1))" \
    --consistency linearizable --sql "SELECT id, note, balance FROM accounts ORDER BY id;")
  raft_assert_query_exact "${before_query}" "${expected_rows}"
  read -r before_size before_hash < <(raft_log_mutations_fingerprint "${node_id}")

  raft_expect_payload_mismatch_strict "${node_id}" "${client_id}" "${request_id}" "${changed_sql}" \
    "${phase}" "${artifact_root}/${phase}-rejected.out"

  read -r after_size after_hash < <(raft_log_mutations_fingerprint "${node_id}")
  after_status=$(raft_require_stable_status "${node_id}" "$((request_base + 2))")
  after_query=$(raft_client_call_strict "${node_id}" read --request-id "$((request_base + 3))" \
    --consistency linearizable --sql "SELECT id, note, balance FROM accounts ORDER BY id;")
  raft_assert_query_exact "${after_query}" "${expected_rows}"

  for field in term commit_index last_applied published_applied_index snapshot_base_index
  do
    if [[ $(raft_status_field "${before_status}" "${field}") != \
          $(raft_status_field "${after_status}" "${field}") ]]
    then
      echo "${phase} changed stable Raft field ${field}: ${before_status} -> ${after_status}" >&2
      return 1
    fi
  done
  if [[ ${after_size} != "${before_size}" ]]
  then
    echo "${phase} changed LOG-MUTATIONS size: ${before_size} -> ${after_size}" >&2
    return 1
  fi
  if [[ ${after_hash} != "${before_hash}" ]]
  then
    echo "${phase} changed LOG-MUTATIONS SHA-256: ${before_hash} -> ${after_hash}" >&2
    return 1
  fi
  printf 'phase=%s node=%s before_size=%s after_size=%s before_sha256=%s after_sha256=%s\n' \
    "${phase}" "${node_id}" "${before_size}" "${after_size}" "${before_hash}" "${after_hash}" \
    >>"${artifact_root}/log-mutation-evidence.txt"
}

# A byte-identical cached response is necessary but not sufficient: an
# incorrect implementation could append the duplicate and rely on Apply-side
# deduplication. Bind the same strict request to independent stable-state,
# durable-journal, and literal-row no-change oracles.
raft_assert_exact_retry_no_append() {
  if [[ $# -ne 9 ]]
  then
    echo "raft_assert_exact_retry_no_append requires phase, node, client ID, request ID, SQL, expected rows, expected response, expected commit (or -), and request base" >&2
    return 1
  fi
  local phase=$1
  local node_id=$2
  local client_id=$3
  local request_id=$4
  local exact_sql=$5
  local expected_rows=$6
  local expected_response=$7
  local expected_commit=$8
  local request_base=$9
  local after_hash
  local after_query
  local after_size
  local after_status
  local before_hash
  local before_query
  local before_size
  local before_status
  local field
  local retry_commit
  local retry_output
  local retry_response

  before_status=$(raft_require_stable_status "${node_id}" "${request_base}")
  before_query=$(raft_client_call_strict "${node_id}" read --request-id "$((request_base + 1))" \
    --consistency linearizable --sql "SELECT id, note, balance FROM accounts ORDER BY id;")
  raft_assert_query_exact "${before_query}" "${expected_rows}"
  read -r before_size before_hash < <(raft_log_mutations_fingerprint "${node_id}")

  retry_output=$(raft_client_call_strict "${node_id}" write --client-id "${client_id}" \
    --request-id "${request_id}" --sql "${exact_sql}")
  retry_response=$(raft_status_field "${retry_output}" response_bytes)
  retry_commit=$(raft_status_field "${retry_output}" committed_index)
  if [[ ${retry_response} != "${expected_response}" ]] || \
     [[ $(raft_status_field "${retry_output}" request_id) -ne ${request_id} ]]
  then
    echo "${phase} did not return the byte-identical cached WriteResponseV1" >&2
    return 1
  fi
  if [[ ${expected_commit} != "-" ]] && [[ ${retry_commit} -ne ${expected_commit} ]]
  then
    echo "${phase} changed cached commit index: expected ${expected_commit}, got ${retry_commit}" >&2
    return 1
  fi

  read -r after_size after_hash < <(raft_log_mutations_fingerprint "${node_id}")
  after_status=$(raft_require_stable_status "${node_id}" "$((request_base + 2))")
  after_query=$(raft_client_call_strict "${node_id}" read --request-id "$((request_base + 3))" \
    --consistency linearizable --sql "SELECT id, note, balance FROM accounts ORDER BY id;")
  raft_assert_query_exact "${after_query}" "${expected_rows}"

  for field in term commit_index last_applied published_applied_index snapshot_base_index
  do
    if [[ $(raft_status_field "${before_status}" "${field}") != \
          $(raft_status_field "${after_status}" "${field}") ]]
    then
      echo "${phase} exact retry changed stable Raft field ${field}: ${before_status} -> ${after_status}" >&2
      return 1
    fi
  done
  if [[ ${after_size} != "${before_size}" ]]
  then
    echo "${phase} exact retry changed LOG-MUTATIONS size: ${before_size} -> ${after_size}" >&2
    return 1
  fi
  if [[ ${after_hash} != "${before_hash}" ]]
  then
    echo "${phase} exact retry changed LOG-MUTATIONS SHA-256: ${before_hash} -> ${after_hash}" >&2
    return 1
  fi
  printf '%s\n' "${retry_output}" >"${artifact_root}/${phase}-exact-retry.out"
  printf 'phase=%s node=%s cached_commit=%s before_size=%s after_size=%s before_sha256=%s after_sha256=%s\n' \
    "${phase}" "${node_id}" "${retry_commit}" "${before_size}" "${after_size}" "${before_hash}" "${after_hash}" \
    >>"${artifact_root}/exact-retry-log-evidence.txt"
  RAFT_LAST_EXACT_RETRY_COMMIT=${retry_commit}
}

raft_timeline_step "M8: start one fresh homogeneous three-process cluster and commit real CREATE/INSERT SQL"
raft_start_all_nodes
leader=$(raft_find_leader)

create_sql="CREATE TABLE accounts(id int PRIMARY KEY, note varchar(32), balance int);"
insert_sql="INSERT INTO accounts VALUES (2, 'peer', 20), (1, 'seed', 10);"
create_output=$(raft_client_call_strict "${leader}" write --client-id 8100 --request-id 1 --sql "${create_sql}")
insert_output=$(raft_client_call_strict "${leader}" write --client-id 8100 --request-id 2 --sql "${insert_sql}")
create_index=$(raft_status_field "${create_output}" committed_index)
insert_index=$(raft_status_field "${insert_output}" committed_index)
if [[ ${insert_index} -le ${create_index} ]]
then
  echo "real INSERT did not commit strictly after CREATE" >&2
  exit 1
fi
for node_id in 1 2 3
do
  raft_await_status_at_least "${node_id}" last_applied "${insert_index}" 300 "$((100000 + node_id * 1000))"
done

followers=()
for node_id in 1 2 3
do
  if [[ ${node_id} -ne ${leader} ]]
  then
    followers+=("${node_id}")
  fi
done
lagging_follower=${followers[0]}
quorum_follower=${followers[1]}

initial_rows=$'accounts.id\taccounts.note\taccounts.balance\n1\tseed\t10\n2\tpeer\t20'
raft_timeline_step "M8: reject an ordinary changed reuse of the latest request identity before prepare or append"
raft_assert_payload_mismatch_no_effect ordinary-latest "${leader}" 8100 2 \
  "INSERT INTO accounts VALUES (3, 'wrong', 999);" "${initial_rows}" 110000

raft_timeline_step "M8: stop one caught-up follower before the retained UPDATE, lose its response, and kill its Leader"
raft_stop_node "${lagging_follower}"
update_sql="UPDATE accounts SET balance = balance + 7 WHERE id <= 2;"
changed_update_sql="UPDATE accounts SET balance = balance + 100 WHERE id <= 2;"
drop_result="${artifact_root}/dropped-update-response.txt"
drop_port=$((port_base + 250))
original_response_bytes=$(raft_client_write_with_dropped_response "${leader}" "${drop_port}" "${drop_result}" \
  8100 3 "${update_sql}")
serving_leader=${leader}
raft_stop_node "${serving_leader}" KILL
# The continuously running up-to-date follower has at least 1.5 seconds of
# election-clock advantage over the restarted old Leader. Both still use the
# same [2000,3000] ms production random range.
sleep 1.5
raft_start_node "${serving_leader}"
replacement_leader=$(raft_find_leader "${serving_leader}")
if [[ ${replacement_leader} -ne ${quorum_follower} ]]
then
  echo "the continuously running up-to-date follower did not become the replacement Leader" >&2
  exit 1
fi

updated_rows=$'accounts.id\taccounts.note\taccounts.balance\n1\tseed\t17\n2\tpeer\t27'
raft_timeline_step "M8: replacement Leader first rejects the changed payload with no append or business mutation"
raft_assert_payload_mismatch_no_effect replacement-leader "${replacement_leader}" 8100 3 \
  "${changed_update_sql}" "${updated_rows}" 120000

raft_timeline_step "M8: retry the exact lost-response SQL once and require byte-identical cached WriteResponseV1"
raft_assert_exact_retry_no_append replacement-leader "${replacement_leader}" 8100 3 "${update_sql}" \
  "${updated_rows}" "${original_response_bytes}" - 125000
update_index=${RAFT_LAST_EXACT_RETRY_COMMIT}

# The lagging follower is still stopped at the pre-UPDATE boundary. Three
# unrelated, real SQL writes advance the replacement cluster from index 5 to
# the configured threshold at index 8; no retry or empty padding request is
# used to manufacture the snapshot.
raft_timeline_step "M8: use another client and real SQL to trigger compaction while the pre-UPDATE follower stays offline"
padding_create=$(raft_client_call_strict "${replacement_leader}" write --client-id 8200 --request-id 1 \
  --sql "CREATE TABLE snapshot_padding(id int PRIMARY KEY, note varchar(32));")
padding_insert_one=$(raft_client_call_strict "${replacement_leader}" write --client-id 8200 --request-id 2 \
  --sql "INSERT INTO snapshot_padding VALUES (1, 'first');")
padding_insert_two=$(raft_client_call_strict "${replacement_leader}" write --client-id 8200 --request-id 3 \
  --sql "INSERT INTO snapshot_padding VALUES (2, 'second');")
padding_create_index=$(raft_status_field "${padding_create}" committed_index)
padding_one_index=$(raft_status_field "${padding_insert_one}" committed_index)
padding_index=$(raft_status_field "${padding_insert_two}" committed_index)
if [[ ${padding_create_index} -le ${update_index} ]] || [[ ${padding_one_index} -le ${padding_create_index} ]] || \
   [[ ${padding_index} -le ${padding_one_index} ]]
then
  echo "real snapshot-trigger writes did not commit in a strict sequence" >&2
  exit 1
fi

# Only status progress is polled. Once the durable recovery base covers all
# three trigger writes, parse CURRENT exactly once with the independent V1
# helper. The higher threshold keeps every pre-snapshot no-append oracle free
# from background snapshot/LogStore compaction.
raft_timeline_step "M8: wait until a quiescent published CURRENT snapshot covers the retained request"
raft_await_status_at_least "${replacement_leader}" snapshot_base_index "${padding_index}" 600 130000
current_path="${artifact_root}/node-${replacement_leader}/raft/snapshots/CURRENT"
if [[ ! -f ${current_path} ]]
then
  echo "snapshot recovery base advanced without a CURRENT record" >&2
  exit 1
fi
current_snapshot_index=$(raft_current_snapshot_index "${current_path}")
if [[ ${current_snapshot_index} -lt ${update_index} ]]
then
  echo "CURRENT Snapshot@${current_snapshot_index} does not cover committed request @${update_index}" >&2
  exit 1
fi
if [[ ${current_snapshot_index} -lt ${padding_index} ]]
then
  echo "CURRENT Snapshot@${current_snapshot_index} does not cover trigger write @${padding_index}" >&2
  exit 1
fi
printf 'request_index=%s current_snapshot_index=%s\n' "${update_index}" "${current_snapshot_index}" \
  >"${artifact_root}/snapshot-binding-evidence.txt"

raft_timeline_step "M8: reject the changed payload again after snapshot publication"
raft_assert_payload_mismatch_no_effect after-snapshot "${replacement_leader}" 8100 3 \
  "${changed_update_sql}" "${updated_rows}" 140000

lagging_proxy_controls="${artifact_root}/proxy-${lagging_follower}"
if [[ -e ${lagging_proxy_controls}/snapshot-recorded ]]
then
  echo "the stopped follower proxy recorded a snapshot before deliberate recovery" >&2
  exit 1
fi

raft_timeline_step "M8: restart the pre-UPDATE follower and require a recorded InstallSnapshot covering the request"
raft_start_node "${lagging_follower}"
raft_await_status_at_least "${lagging_follower}" snapshot_base_index "${current_snapshot_index}" 600 145000
raft_await_status_at_least "${lagging_follower}" last_applied "${padding_index}" 600 146000
if [[ ! -f ${lagging_proxy_controls}/snapshot-recorded ]]
then
  echo "the pre-UPDATE follower reached the compacted boundary without a recorded InstallSnapshot" >&2
  exit 1
fi
snapshot_summary=$(<"${lagging_proxy_controls}/snapshot-recorded")
recorded_snapshot_index=$(raft_status_field "${snapshot_summary}" last_included_index)
first_snapshot_request=$(<"${lagging_proxy_controls}/snapshot-first-request.meta")
if [[ ${recorded_snapshot_index} -lt ${padding_index} ]] || \
   [[ $(raft_status_field "${first_snapshot_request}" to) -ne ${lagging_follower} ]] || \
   [[ $(raft_status_field "${first_snapshot_request}" offset) -ne 0 ]]
then
  echo "recorded InstallSnapshot does not prove the stopped follower received the covering image: ${snapshot_summary}" >&2
  exit 1
fi
installed_padding=$(raft_client_call_strict "${lagging_follower}" read --request-id 147000 --consistency stale \
  --sql "SELECT id, note FROM snapshot_padding ORDER BY id;")
raft_assert_query_exact "${installed_padding}" $'snapshot_padding.id\tsnapshot_padding.note\n1\tfirst\n2\tsecond'

# Stop the current Leader first, then the remaining peer. The installed node's
# election clock keeps running for 1.5 seconds before one voter cold-starts;
# with the same [2000,3000] ms range, the installed node must campaign first.
raft_timeline_step "M8: make the snapshot-installed follower the next ready Leader"
raft_stop_node "${replacement_leader}"
raft_stop_node "${serving_leader}"
sleep 1.5
raft_start_node "${serving_leader}"
installed_leader=$(raft_find_leader)
if [[ ${installed_leader} -ne ${lagging_follower} ]]
then
  echo "the snapshot-installed follower did not become Leader after its deterministic election head start" >&2
  exit 1
fi

raft_timeline_step "M8: the snapshot-installed Leader enforces payload binding and returns the cached response"
raft_assert_payload_mismatch_no_effect installed-follower-leader "${installed_leader}" 8100 3 \
  "${changed_update_sql}" "${updated_rows}" 148000
raft_assert_exact_retry_no_append installed-follower-leader "${installed_leader}" 8100 3 "${update_sql}" \
  "${updated_rows}" "${original_response_bytes}" "${update_index}" 149000

raft_timeline_step "M8: stop all voters, cold-open all three directories, and elect a replacement from restored state"
installed_term_commit=$(raft_status_field "$(raft_require_stable_status "${installed_leader}" 149100)" commit_index)
for node_id in 1 2 3
do
  if raft_node_alive "${node_id}"
  then
    raft_await_status_at_least "${node_id}" last_applied "${installed_term_commit}" 300 "$((150000 + node_id * 1000))"
  fi
done
raft_stop_all_nodes
raft_start_node 3
raft_start_node 1
raft_start_node 2
restart_leader=$(raft_find_leader)

raft_timeline_step "M8: cold-restarted Leader rejects changed SQL without append, then replays the exact cached response"
raft_assert_payload_mismatch_no_effect after-cold-restart "${restart_leader}" 8100 3 \
  "${changed_update_sql}" "${updated_rows}" 160000
raft_assert_exact_retry_no_append after-cold-restart "${restart_leader}" 8100 3 "${update_sql}" \
  "${updated_rows}" "${original_response_bytes}" "${update_index}" 165000

restart_commit=$(raft_status_field "$(raft_require_stable_status "${restart_leader}" 170000)" commit_index)
for node_id in 1 2 3
do
  raft_await_status_at_least "${node_id}" last_applied "${restart_commit}" 300 "$((171000 + node_id * 1000))"
  final_rows=$(raft_client_call_strict "${node_id}" read --request-id "$((175000 + node_id))" \
    --consistency stale --sql "SELECT id, note, balance FROM accounts ORDER BY id;")
  raft_assert_query_exact "${final_rows}" "${updated_rows}"
done

raft_timeline_step "M8: perform a second consecutive three-node cold reopen and repeat both payload-binding oracles"
raft_stop_all_nodes
raft_start_node 2
raft_start_node 3
raft_start_node 1
second_restart_leader=$(raft_find_leader)
raft_assert_payload_mismatch_no_effect after-second-cold-restart "${second_restart_leader}" 8100 3 \
  "${changed_update_sql}" "${updated_rows}" 180000
raft_assert_exact_retry_no_append after-second-cold-restart "${second_restart_leader}" 8100 3 "${update_sql}" \
  "${updated_rows}" "${original_response_bytes}" "${update_index}" 185000

second_restart_commit=$(raft_status_field "$(raft_require_stable_status "${second_restart_leader}" 190000)" commit_index)
for node_id in 1 2 3
do
  raft_await_status_at_least "${node_id}" last_applied "${second_restart_commit}" 300 "$((191000 + node_id * 1000))"
  second_final_rows=$(raft_client_call_strict "${node_id}" read --request-id "$((195000 + node_id))" \
    --consistency stale --sql "SELECT id, note, balance FROM accounts ORDER BY id;")
  raft_assert_query_exact "${second_final_rows}" "${updated_rows}"
done

echo "M8 payload-binding production-process E2E passed; artifacts: ${artifact_root}"
