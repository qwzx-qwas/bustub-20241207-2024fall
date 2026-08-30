#!/usr/bin/env bash

set -euo pipefail

build_dir=${1:-/tmp/bustub-raft-build-clang}
port_base=${2:-30100}
artifact_root=${3:-"/tmp/bustub-raft-m7-snapshot-transfer-$$"}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "${script_dir}/raft_process_harness.sh"
RAFT_ELECTION_TIMEOUT_MIN_MS=800
RAFT_ELECTION_TIMEOUT_MAX_MS=1600
raft_harness_init "${build_dir}" "${port_base}" "${artifact_root}" m7-snapshot-transfer \
  --snapshot-threshold-entries 4

for node_id in 1 2 3
do
  raft_start_message_proxy "${node_id}" "$((port_base + 300 + node_id))" "${artifact_root}/proxy-${node_id}"
done

raft_timeline_step "E2E-05/14: hold one Follower below Snapshot@S, then recover it with Snapshot S plus UPDATE S+1"
raft_start_all_nodes
leader=$(raft_find_leader)
lagger=$((leader % 3 + 1))
# Stop the follower instead of creating a one-way partition. A live isolated
# follower keeps increasing its durable term; reconnecting it can then preempt
# the healthy Leader between InstallSnapshot chunks and mix two snapshot
# identities into this transfer-only oracle. Online term churn is exercised by
# the recovery matrix, while this scenario models an ordinary crashed follower
# that restarts from its genuinely stale durable state.
raft_stop_node "${lagger}"

raft_client_call_eventually "${leader}" write --client-id 900 --request-id 1 \
  --sql "CREATE TABLE versions(id int PRIMARY KEY, value varchar(32));"
insert_sql="INSERT INTO versions VALUES "
for row_id in $(seq 1 1600)
do
  [[ ${row_id} -eq 1 ]] || insert_sql+=","
  insert_sql+="(${row_id}, 'snapshot-payload')"
done
insert_sql+=";"
at_k=$(raft_client_call_eventually "${leader}" write --client-id 900 --request-id 2 --sql "${insert_sql}")
k_index=$(raft_status_field "${at_k}" committed_index)
if [[ ${k_index} -le 0 ]]
then
  echo "bulk snapshot state did not receive a positive committed index" >&2
  exit 1
fi
at_s=$(raft_client_call_eventually "${leader}" write --client-id 900 --request-id 3 \
  --sql "CREATE INDEX versions_value ON versions(value);")
s_index=$(raft_status_field "${at_s}" committed_index)
if [[ ${s_index} -le ${k_index} ]]
then
  echo "snapshot trigger did not follow the bulk row commit: K=${k_index}, S=${s_index}" >&2
  exit 1
fi
raft_await_status_at_least "${leader}" snapshot_base_index "${s_index}" 600 310000
suffix=$(raft_client_call_eventually "${leader}" write --client-id 900 --request-id 4 \
  --sql "UPDATE versions SET value = 'after-suffix' WHERE id = 1;")
suffix_index=$(raft_status_field "${suffix}" committed_index)
if [[ ${suffix_index} -le ${s_index} ]]
then
  echo "snapshot suffix did not commit after S: S=${s_index}, suffix=${suffix_index}" >&2
  exit 1
fi

raft_start_node "${lagger}"
raft_await_status_at_least "${lagger}" snapshot_base_index "${s_index}" 800 320000
raft_await_status_at_least "${lagger}" last_applied "${suffix_index}" 800 330000
for _ in $(seq 1 500)
do
  [[ -f ${artifact_root}/proxy-${lagger}/snapshot-recorded ]] && break
  sleep 0.01
done
[[ -f ${artifact_root}/proxy-${lagger}/snapshot-recorded ]]
snapshot_summary=$(<"${artifact_root}/proxy-${lagger}/snapshot-recorded")
snapshot_chunks=$(raft_status_field "${snapshot_summary}" chunks)
snapshot_size=$(raft_status_field "${snapshot_summary}" total_size)
recorded_snapshot_index=$(raft_status_field "${snapshot_summary}" last_included_index)
if [[ ${snapshot_chunks} -lt 2 ]] || [[ ${snapshot_size} -le 65536 ]]
then
  echo "formal InstallSnapshot did not exercise multiple 64 KiB chunks: ${snapshot_summary}" >&2
  exit 1
fi
if [[ ${recorded_snapshot_index} -ne ${s_index} ]] || [[ ${recorded_snapshot_index} -ge ${suffix_index} ]]
then
  echo "formal InstallSnapshot did not transfer exact Snapshot@S=${s_index} before suffix=${suffix_index}: ${snapshot_summary}" >&2
  exit 1
fi
recovered=$(raft_client_call_strict "${lagger}" read --request-id 340000 --consistency stale \
  --sql "SELECT id, value FROM versions WHERE id <= 2 ORDER BY id;")
raft_assert_query_exact "${recovered}" \
  $'versions.id\tversions.value\n1\tafter-suffix\n2\tsnapshot-payload'

raft_timeline_step "E2E-15: replay the complete old Snapshot@S after the Follower has already applied S+1"
current_path="${artifact_root}/node-${lagger}/raft/snapshots/CURRENT"
current_before=$(sha256sum "${current_path}" | cut -d' ' -f1)
status_before=$(raft_status "${lagger}" 350000)
first_request=$(<"${artifact_root}/proxy-${lagger}/snapshot-first-request.meta")
snapshot_sender=$(raft_status_field "${first_request}" from)
snapshot_target=$(raft_status_field "${first_request}" to)
snapshot_request_id=$(raft_status_field "${first_request}" request_id)
snapshot_offset=$(raft_status_field "${first_request}" offset)
if [[ ${snapshot_target} -ne ${lagger} ]] || [[ ${snapshot_offset} -ne 0 ]]
then
  echo "recorded snapshot topology does not begin at the lagging node: ${first_request}" >&2
  exit 1
fi
printf 'from=%s to=%s request_id=%s success=1 stale=1 complete=1\n' \
  "${lagger}" "${snapshot_sender}" "${snapshot_request_id}" \
  >"${artifact_root}/proxy-${snapshot_sender}/snapshot-response-watch"
touch "${artifact_root}/proxy-${lagger}/replay"
for _ in $(seq 1 500)
do
  if [[ -f ${artifact_root}/proxy-${lagger}/replay-error ]]
  then
    cat "${artifact_root}/proxy-${lagger}/replay-error" >&2
    exit 1
  fi
  [[ -f ${artifact_root}/proxy-${lagger}/replayed ]] && break
  sleep 0.01
done
[[ -f ${artifact_root}/proxy-${lagger}/replayed ]]
replayed_chunks=$(<"${artifact_root}/proxy-${lagger}/replayed")
if [[ ${replayed_chunks} -ne ${snapshot_chunks} ]]
then
  echo "stale replay did not send every recorded chunk: ${replayed_chunks}/${snapshot_chunks}" >&2
  exit 1
fi
for _ in $(seq 1 500)
do
  [[ -f ${artifact_root}/proxy-${snapshot_sender}/snapshot-response-observed ]] && break
  sleep 0.01
done
if [[ ! -f ${artifact_root}/proxy-${snapshot_sender}/snapshot-response-observed ]]
then
  echo "replayed stale snapshot did not produce the expected formal InstallSnapshotResponse" >&2
  exit 1
fi
stale_response=$(<"${artifact_root}/proxy-${snapshot_sender}/snapshot-response-observed")
if [[ $(raft_status_field "${stale_response}" next_offset) -ne 0 ]] || \
   [[ $(raft_status_field "${stale_response}" match_index) -lt \
      $(raft_status_field "${status_before}" published_applied_index) ]]
then
  echo "stale snapshot response did not acknowledge the follower's published state: ${stale_response}" >&2
  exit 1
fi
status_after=$(raft_status "${lagger}" 350001)
current_after=$(sha256sum "${current_path}" | cut -d' ' -f1)
if [[ ${current_after} != "${current_before}" ]]
then
  echo "delayed stale snapshot changed CURRENT" >&2
  exit 1
fi
for field in snapshot_base_index commit_index last_applied published_applied_index
do
  before=$(raft_status_field "${status_before}" "${field}")
  after=$(raft_status_field "${status_after}" "${field}")
  if [[ ${after} -lt ${before} ]]
  then
    echo "delayed stale snapshot regressed ${field}: ${before} -> ${after}" >&2
    exit 1
  fi
done

next=$(raft_client_call_eventually "$(raft_find_leader)" write --client-id 900 --request-id 5 \
  --sql "INSERT INTO versions VALUES (1601, 'after-replay');")
next_index=$(raft_status_field "${next}" committed_index)
raft_await_status_at_least "${lagger}" last_applied "${next_index}" 400 360000
continued=$(raft_client_call_strict "${lagger}" read --request-id 360500 --consistency stale \
  --sql "SELECT id, value FROM versions WHERE id <= 2 ORDER BY id;")
raft_assert_query_exact "${continued}" \
  $'versions.id\tversions.value\n1\tafter-suffix\n2\tsnapshot-payload'
new_row=$(raft_client_call_strict "${lagger}" read --request-id 360501 --consistency stale \
  --sql "SELECT id, value FROM versions WHERE id = 1601;")
raft_assert_query_exact "${new_row}" $'versions.id\tversions.value\n1601\tafter-replay'

echo "M7 snapshot transfer/replay E2E passed; artifacts: ${artifact_root}"
