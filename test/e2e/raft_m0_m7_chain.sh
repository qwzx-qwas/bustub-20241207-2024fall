#!/usr/bin/env bash

set -euo pipefail

build_dir=${1:-/tmp/bustub-raft-build-clang}
# Keep all fixed listen/proxy ports below Linux's usual ephemeral range. The
# all-message proxies create many short-lived outbound connections, so choosing
# an ephemeral source port as a listener can make a legitimate one-shot restart
# lose a bind race.
port_base=${2:-24100}
artifact_root=${3:-"/tmp/bustub-raft-m0-m7-chain-$$"}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "${script_dir}/raft_process_harness.sh"

# A longer randomized election window keeps the test focused on the deliberate
# Leader kill. Four committed entries give us enough room to prove that the
# transferred snapshot predates the two-entry suffix.
RAFT_ELECTION_TIMEOUT_MIN_MS=800
RAFT_ELECTION_TIMEOUT_MAX_MS=1600
raft_harness_init "${build_dir}" "${port_base}" "${artifact_root}" m0-m7-chain \
  --snapshot-threshold-entries 4

for node_id in 1 2 3
do
  raft_start_message_proxy "${node_id}" "$((port_base + 300 + node_id))" \
    "${artifact_root}/proxy-${node_id}"
done

raft_timeline_step "Cumulative prerequisites: start fresh distributed state and validate admission has no Catalog side effects"
raft_start_all_nodes
leader=$(raft_find_leader)

before_table_rejection=$(raft_status "${leader}" 1001)
raft_client_expect_status "${leader}" REJECTED write --client-id 500 --request-id 1 \
  --sql "CREATE TABLE accounts(value int);" >/dev/null
after_table_rejection=$(raft_status "${leader}" 1002)
if [[ $(raft_status_field "${before_table_rejection}" commit_index) != \
      $(raft_status_field "${after_table_rejection}" commit_index) ]]
then
  echo "rejected no-primary-key DDL changed the Raft commit index" >&2
  exit 1
fi

# Reuse both the rejected request identity and object name. A parser-only
# rejection that accidentally reserved the name in the Catalog is observable.
raft_client_call_strict "${leader}" write --client-id 500 --request-id 1 \
  --sql "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32), balance int);" >/dev/null
raft_client_call_strict "${leader}" write --client-id 500 --request-id 2 \
  --sql "INSERT INTO accounts VALUES (2, 'shared', 20), (1, 'shared', 10);" >/dev/null

before_index_rejection=$(raft_status "${leader}" 1003)
raft_client_expect_status "${leader}" REJECTED write --client-id 500 --request-id 3 \
  --sql "CREATE UNIQUE INDEX accounts_name_idx ON accounts(name);" >/dev/null
after_index_rejection=$(raft_status "${leader}" 1004)
if [[ $(raft_status_field "${before_index_rejection}" commit_index) != \
      $(raft_status_field "${after_index_rejection}" commit_index) ]]
then
  echo "rejected UNIQUE secondary index changed the Raft commit index" >&2
  exit 1
fi
raft_client_call_strict "${leader}" write --client-id 500 --request-id 3 \
  --sql "CREATE INDEX accounts_name_idx ON accounts(name);" >/dev/null

initial_accounts=$(raft_client_call_strict "${leader}" read --request-id 1010 --consistency linearizable \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${initial_accounts}" \
  $'accounts.id\taccounts.name\taccounts.balance\n1\tshared\t10\n2\tshared\t20'
initial_index_lookup=$(raft_client_call_strict "${leader}" read --request-id 1011 --consistency linearizable \
  --sql "SELECT id, name, balance FROM accounts WHERE name = 'shared' ORDER BY id;")
raft_assert_query_exact "${initial_index_lookup}" \
  $'accounts.id\taccounts.name\taccounts.balance\n1\tshared\t10\n2\tshared\t20'

raft_timeline_step "M6: commit a non-idempotent write, drop its entire response, kill the Leader, and retry exactly once"
drop_result="${artifact_root}/dropped-balance-response.txt"
drop_port=$((port_base + 250))
original_response_bytes=$(raft_client_write_with_dropped_response "${leader}" "${drop_port}" "${drop_result}" \
  500 4 "UPDATE accounts SET balance = balance + 7 WHERE id <= 2;")

old_leader=${leader}
raft_stop_node "${old_leader}" KILL
new_leader=$(raft_find_leader "${old_leader}")
retry_output=$(raft_client_call_strict "${new_leader}" write --client-id 500 --request-id 4 \
  --sql "UPDATE accounts SET balance = balance + 7 WHERE id <= 2;")
retry_response_bytes=$(raft_status_field "${retry_output}" response_bytes)
if [[ ${retry_response_bytes} != "${original_response_bytes}" ]] || \
   [[ $(raft_status_field "${retry_output}" request_id) -ne 4 ]]
then
  echo "Leader replacement did not return the byte-identical cached response" >&2
  exit 1
fi
once_index=$(raft_status_field "${retry_output}" committed_index)
once_only=$(raft_client_call_strict "${new_leader}" read --request-id 1020 --consistency linearizable \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${once_only}" \
  $'accounts.id\taccounts.name\taccounts.balance\n1\tshared\t17\n2\tshared\t27'
if [[ $(raft_status_field "${once_only}" read_timestamp) -lt ${once_index} ]]
then
  echo "post-failover linearizable read is behind the deduplicated write" >&2
  exit 1
fi

if raft_node_alive "${old_leader}"
then
  echo "the original Leader unexpectedly restarted before snapshot construction" >&2
  exit 1
fi

raft_timeline_step "M7: keep the old Leader offline and publish a canonical snapshot larger than one 64 KiB chunk"
raft_client_call_strict "${new_leader}" write --client-id 600 --request-id 1 \
  --sql "CREATE TABLE chain_bulk(id int PRIMARY KEY, tag varchar(48));" >/dev/null

# The declared VARCHAR plus its tuple slot remains inside the V1 scalar-index
# adapter's 64-byte bound. Meanwhile 1600 * 45 bytes of unique literal data
# guarantees that canonical state, rather than page metadata alone, exceeds one
# 64 KiB transfer chunk.
bulk_tail=abcdefghijklmnopqrstuvwxyzABCDEFGHIJ
bulk_sql="INSERT INTO chain_bulk VALUES "
for row_id in $(seq 1 1600)
do
  printf -v row_tag 'row-%04d-%s' "${row_id}" "${bulk_tail}"
  [[ ${row_id} -eq 1 ]] || bulk_sql+=","
  bulk_sql+="(${row_id}, '${row_tag}')"
done
bulk_sql+=";"
bulk_insert=$(raft_client_call_strict "${new_leader}" write --client-id 600 --request-id 2 --sql "${bulk_sql}")
snapshot_head=$(raft_status_field "${bulk_insert}" committed_index)
bulk_request_id=2
padding_count=0
snapshot_index=
snapshot_status=

# Stop issuing entries only when a published snapshot covers the exact current
# head and the oldest retained recovery base has moved past the old Leader's
# last committed entry. The latter is essential with two-generation retention:
# without it the returning node can catch up from bridge logs and the intended
# InstallSnapshot stimulus is never exercised. The following two writes are
# then provably a log suffix rather than part of the transferred image.
for snapshot_attempt in $(seq 0 12)
do
  if [[ ${snapshot_attempt} -gt 0 ]]
  then
    padding_count=$((padding_count + 1))
    padding_id=$((1600 + padding_count))
    bulk_request_id=$((bulk_request_id + 1))
    printf -v padding_tag 'pad-%04d-%s' "${padding_id}" "${bulk_tail}"
    padding_write=$(raft_client_call_strict "${new_leader}" write --client-id 600 \
      --request-id "${bulk_request_id}" \
      --sql "INSERT INTO chain_bulk VALUES (${padding_id}, '${padding_tag}');")
    snapshot_head=$(raft_status_field "${padding_write}" committed_index)
  fi

  for poll_round in $(seq 1 60)
  do
    current_manifest="${artifact_root}/node-${new_leader}/raft/snapshots/CURRENT"
    if [[ -f ${current_manifest} ]] && observed_snapshot=$(raft_current_snapshot_index "${current_manifest}") && \
       [[ ${observed_snapshot} -ge ${snapshot_head} ]]
    then
      snapshot_status=$(raft_status "${new_leader}" "$((2000 + snapshot_attempt * 100 + poll_round))")
      observed_commit=$(raft_status_field "${snapshot_status}" commit_index)
      oldest_recovery_base=$(raft_status_field "${snapshot_status}" snapshot_base_index)
      if [[ ${observed_snapshot} -ne ${snapshot_head} ]] || [[ ${observed_commit} -ne ${snapshot_head} ]]
      then
        echo "snapshot did not publish at the quiescent log head: ${snapshot_status}" >&2
        exit 1
      fi
      if [[ ${oldest_recovery_base} -gt ${once_index} ]]
      then
        snapshot_index=${observed_snapshot}
        break 2
      fi
      break
    fi
    sleep 0.05
  done
done
if [[ -z ${snapshot_index} ]]
then
  echo "new Leader did not publish a snapshot at a quiescent log head" >&2
  exit 1
fi

bulk_row_count=$((1600 + padding_count))
bulk_request_id=$((bulk_request_id + 1))
index_suffix=$(raft_client_call_strict "${new_leader}" write --client-id 600 \
  --request-id "${bulk_request_id}" --sql "CREATE INDEX chain_bulk_tag_idx ON chain_bulk(tag);")
first_suffix_index=$(raft_status_field "${index_suffix}" committed_index)
if [[ ${first_suffix_index} -le ${snapshot_index} ]]
then
  echo "secondary-index suffix did not follow the published snapshot" >&2
  exit 1
fi
balance_suffix=$(raft_client_call_strict "${new_leader}" write --client-id 500 --request-id 5 \
  --sql "UPDATE accounts SET balance = balance + 5 WHERE id = 2;")
suffix_index=$(raft_status_field "${balance_suffix}" committed_index)
if [[ ${suffix_index} -le ${first_suffix_index} ]]
then
  echo "balance suffix did not follow the secondary-index suffix" >&2
  exit 1
fi
before_recovery_status=$(raft_status "${new_leader}" 3000)
latest_snapshot_after_suffix=$(raft_current_snapshot_index \
  "${artifact_root}/node-${new_leader}/raft/snapshots/CURRENT")
if [[ ${latest_snapshot_after_suffix} -ne ${snapshot_index} ]] || \
   [[ $(raft_status_field "${before_recovery_status}" commit_index) -ne ${suffix_index} ]]
then
  echo "a later snapshot swallowed the suffix before follower recovery" >&2
  exit 1
fi

old_proxy_controls="${artifact_root}/proxy-${old_leader}"
if [[ -e ${old_proxy_controls}/snapshot-recorded ]]
then
  echo "old Leader proxy recorded a snapshot before the deliberate recovery" >&2
  exit 1
fi
raft_timeline_step "M7: restart the old Leader and require a recorded multi-chunk Snapshot@S plus its later suffix"
raft_start_node "${old_leader}"
raft_await_status_at_least "${old_leader}" snapshot_base_index "${snapshot_index}" 800 310000
raft_await_status_at_least "${old_leader}" last_applied "${suffix_index}" 800 320000
for _ in $(seq 1 500)
do
  [[ -f ${old_proxy_controls}/snapshot-recorded ]] && break
  sleep 0.01
done
if [[ ! -f ${old_proxy_controls}/snapshot-recorded ]]
then
  echo "old Leader caught up without a completely recorded InstallSnapshot transfer" >&2
  exit 1
fi
snapshot_summary=$(<"${old_proxy_controls}/snapshot-recorded")
snapshot_chunks=$(raft_status_field "${snapshot_summary}" chunks)
snapshot_size=$(raft_status_field "${snapshot_summary}" total_size)
recorded_snapshot_index=$(raft_status_field "${snapshot_summary}" last_included_index)
if [[ ${snapshot_chunks} -lt 2 ]] || [[ ${snapshot_size} -le 65536 ]]
then
  echo "InstallSnapshot did not carry at least two chunks and more than 64 KiB: ${snapshot_summary}" >&2
  exit 1
fi
if [[ ${recorded_snapshot_index} -ne ${snapshot_index} ]] || \
   [[ ${recorded_snapshot_index} -ge ${first_suffix_index} ]]
then
  echo "recorded image is not the expected pre-suffix snapshot: ${snapshot_summary}" >&2
  exit 1
fi

printf -v tag_1 'row-%04d-%s' 1 "${bulk_tail}"
printf -v tag_800 'row-%04d-%s' 800 "${bulk_tail}"
printf -v tag_1600 'row-%04d-%s' 1600 "${bulk_tail}"
expected_accounts_after_suffix=$'accounts.id\taccounts.name\taccounts.balance\n1\tshared\t17\n2\tshared\t32'
expected_bulk_sample=$'chain_bulk.id\tchain_bulk.tag\n1\t'"${tag_1}"$'\n800\t'"${tag_800}"$'\n1600\t'"${tag_1600}"
expected_bulk_index=$'chain_bulk.id\tchain_bulk.tag\n800\t'"${tag_800}"
expected_bulk_count=$'row_count\n'"${bulk_row_count}"

for node_id in 1 2 3
do
  raft_await_status_at_least "${node_id}" last_applied "${suffix_index}" 500 "$((330000 + node_id * 1000))"
  accounts=$(raft_client_call_strict "${node_id}" read --request-id "$((340000 + node_id * 10))" \
    --consistency stale --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
  if [[ $(raft_status_field "${accounts}" read_timestamp) != \
        $(raft_status_field "${accounts}" published_applied_index) ]]
  then
    echo "node ${node_id} stale read did not use its published watermark" >&2
    exit 1
  fi
  raft_assert_query_exact "${accounts}" "${expected_accounts_after_suffix}"
  bulk_count=$(raft_client_call_strict "${node_id}" read --request-id "$((340001 + node_id * 10))" \
    --consistency stale --sql "SELECT count(*) AS row_count FROM chain_bulk;")
  raft_assert_query_exact "${bulk_count}" "${expected_bulk_count}"
  bulk_sample=$(raft_client_call_strict "${node_id}" read --request-id "$((340002 + node_id * 10))" \
    --consistency stale \
    --sql "SELECT id, tag FROM chain_bulk WHERE id = 1 OR id = 800 OR id = 1600 ORDER BY id;")
  raft_assert_query_exact "${bulk_sample}" "${expected_bulk_sample}"
  bulk_index_lookup=$(raft_client_call_strict "${node_id}" read --request-id "$((340003 + node_id * 10))" \
    --consistency stale --sql "SELECT id, tag FROM chain_bulk WHERE tag = '${tag_800}';")
  raft_assert_query_exact "${bulk_index_lookup}" "${expected_bulk_index}"
done

raft_timeline_step "M7 stale guard: replay every frame of Snapshot@S and observe its matching stale-complete response"
current_path="${artifact_root}/node-${old_leader}/raft/snapshots/CURRENT"
current_before=$(sha256sum "${current_path}" | cut -d' ' -f1)
stale_status_before=$(raft_status "${old_leader}" 350000)
first_snapshot_request=$(<"${old_proxy_controls}/snapshot-first-request.meta")
snapshot_sender=$(raft_status_field "${first_snapshot_request}" from)
snapshot_target=$(raft_status_field "${first_snapshot_request}" to)
snapshot_request_id=$(raft_status_field "${first_snapshot_request}" request_id)
snapshot_offset=$(raft_status_field "${first_snapshot_request}" offset)
if [[ ${snapshot_target} -ne ${old_leader} ]] || [[ ${snapshot_offset} -ne 0 ]]
then
  echo "recorded snapshot does not start at offset zero for the recovering node: ${first_snapshot_request}" >&2
  exit 1
fi
printf 'from=%s to=%s request_id=%s success=1 stale=1 complete=1\n' \
  "${old_leader}" "${snapshot_sender}" "${snapshot_request_id}" \
  >"${artifact_root}/proxy-${snapshot_sender}/snapshot-response-watch"
touch "${old_proxy_controls}/replay"
for _ in $(seq 1 500)
do
  if [[ -f ${old_proxy_controls}/replay-error ]]
  then
    cat "${old_proxy_controls}/replay-error" >&2
    exit 1
  fi
  [[ -f ${old_proxy_controls}/replayed ]] && break
  sleep 0.01
done
if [[ ! -f ${old_proxy_controls}/replayed ]]
then
  echo "external proxy did not replay the recorded snapshot" >&2
  exit 1
fi
replayed_chunks=$(<"${old_proxy_controls}/replayed")
if [[ ${replayed_chunks} -ne ${snapshot_chunks} ]]
then
  echo "stale replay sent ${replayed_chunks}/${snapshot_chunks} recorded chunks" >&2
  exit 1
fi

response_observed="${artifact_root}/proxy-${snapshot_sender}/snapshot-response-observed"
for _ in $(seq 1 500)
do
  [[ -f ${response_observed} ]] && break
  sleep 0.01
done
if [[ ! -f ${response_observed} ]]
then
  echo "sender proxy did not observe the matching stale InstallSnapshotResponse" >&2
  exit 1
fi
stale_response=$(<"${response_observed}")
if [[ $(raft_status_field "${stale_response}" from) -ne ${old_leader} ]] || \
   [[ $(raft_status_field "${stale_response}" to) -ne ${snapshot_sender} ]] || \
   [[ $(raft_status_field "${stale_response}" request_id) -ne ${snapshot_request_id} ]] || \
   [[ $(raft_status_field "${stale_response}" success) -ne 1 ]] || \
   [[ $(raft_status_field "${stale_response}" stale) -ne 1 ]] || \
   [[ $(raft_status_field "${stale_response}" complete) -ne 1 ]] || \
   [[ $(raft_status_field "${stale_response}" next_offset) -ne 0 ]] || \
   [[ $(raft_status_field "${stale_response}" match_index) -lt \
      $(raft_status_field "${stale_status_before}" published_applied_index) ]]
then
  echo "stale snapshot response does not acknowledge the follower's published state: ${stale_response}" >&2
  exit 1
fi

stale_status_after=$(raft_status "${old_leader}" 350001)
current_after=$(sha256sum "${current_path}" | cut -d' ' -f1)
if [[ ${current_after} != "${current_before}" ]]
then
  echo "stale snapshot replay changed CURRENT" >&2
  exit 1
fi
for field in snapshot_base_index commit_index last_applied published_applied_index
do
  before=$(raft_status_field "${stale_status_before}" "${field}")
  after=$(raft_status_field "${stale_status_after}" "${field}")
  if [[ ${after} -ne ${before} ]]
  then
    echo "stale snapshot replay changed ${field}: ${before} -> ${after}" >&2
    exit 1
  fi
done
unchanged_accounts=$(raft_client_call_strict "${old_leader}" read --request-id 351000 --consistency stale \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${unchanged_accounts}" "${expected_accounts_after_suffix}"
unchanged_bulk=$(raft_client_call_strict "${old_leader}" read --request-id 351001 --consistency stale \
  --sql "SELECT id, tag FROM chain_bulk WHERE id = 1 OR id = 800 OR id = 1600 ORDER BY id;")
raft_assert_query_exact "${unchanged_bulk}" "${expected_bulk_sample}"
unchanged_count=$(raft_client_call_strict "${old_leader}" read --request-id 351002 --consistency stale \
  --sql "SELECT count(*) AS row_count FROM chain_bulk;")
raft_assert_query_exact "${unchanged_count}" "${expected_bulk_count}"

active_leader=$(raft_find_leader)
latest_output=$(raft_client_call_strict "${active_leader}" write --client-id 500 --request-id 6 \
  --sql "INSERT INTO accounts VALUES (3, 'three', 30);")
latest_response_bytes=$(raft_status_field "${latest_output}" response_bytes)
latest_index=$(raft_status_field "${latest_output}" committed_index)
expected_final_accounts=$'accounts.id\taccounts.name\taccounts.balance\n1\tshared\t17\n2\tshared\t32\n3\tthree\t30'
raft_await_status_at_least "${old_leader}" last_applied "${latest_index}" 500 352000
continued=$(raft_client_call_strict "${old_leader}" read --request-id 353000 --consistency stale \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${continued}" "${expected_final_accounts}"

raft_timeline_step "M7 restart: stop all nodes, reject a real CLI identity drift, then restart the same state in order 3/1/2"
for node_id in 1 2 3
do
  raft_await_status_at_least "${node_id}" last_applied "${latest_index}" 500 "$((360000 + node_id * 1000))"
done
raft_stop_all_nodes

identity_node=1
identity_before=$(sha256sum "${artifact_root}/node-${identity_node}/node.conf" | cut -d' ' -f1)
identity_args=(
  --node-id "${identity_node}"
  --group-id "${RAFT_GROUP_ID}-wrong"
  --data-dir "${artifact_root}/node-${identity_node}"
  --raft-listen "127.0.0.1:$(raft_port "${identity_node}")"
  --client-listen "127.0.0.1:$(raft_client_port "${identity_node}")"
  --election-timeout-min-ms "${RAFT_ELECTION_TIMEOUT_MIN_MS}"
  --election-timeout-max-ms "${RAFT_ELECTION_TIMEOUT_MAX_MS}"
  --client-timeout-ms 3000
  "${RAFT_EXTRA_NODE_ARGS[@]}"
)
for peer_id in 1 2 3
do
  if [[ ${peer_id} -ne ${identity_node} ]]
  then
    identity_args+=(--peer \
      "${peer_id}=127.0.0.1:$(raft_advertised_port "${peer_id}"),127.0.0.1:$(raft_client_port "${peer_id}")")
  fi
done
set +e
identity_output=$(timeout 10s env UBSAN_OPTIONS=halt_on_error=1 ASAN_OPTIONS=detect_leaks=0 \
  "${RAFT_NODE_BIN}" "${identity_args[@]}" 2>&1)
identity_status=$?
set -e
printf '%s\n' "${identity_output}" >"${artifact_root}/identity-mismatch.out"
if [[ ${identity_status} -ne 1 ]] || \
   [[ ${identity_output} != *"node directory identity does not match configured node_id/group/voters"* ]]
then
  printf 'wrong-group CLI start did not fail with the identity mismatch (exit=%s):\n%s\n' \
    "${identity_status}" "${identity_output}" >&2
  exit 1
fi
identity_after=$(sha256sum "${artifact_root}/node-${identity_node}/node.conf" | cut -d' ' -f1)
if [[ ${identity_after} != "${identity_before}" ]]
then
  echo "wrong-group CLI start modified the durable node identity" >&2
  exit 1
fi

raft_start_node 3
raft_start_node 1
raft_start_node 2
restart_leader=$(raft_find_leader)
latest_retry=$(raft_client_call_strict "${restart_leader}" write --client-id 500 --request-id 6 \
  --sql "INSERT INTO accounts VALUES (3, 'three', 30);")
if [[ $(raft_status_field "${latest_retry}" response_bytes) != "${latest_response_bytes}" ]]
then
  echo "all-node restart changed the latest cached WriteResponseV1 bytes" >&2
  exit 1
fi

raft_timeline_step "M7 final: allocate new Catalog objects after recovery and validate BIGINT PK plus secondary-index access"
raft_client_call_strict "${restart_leader}" write --client-id 500 --request-id 7 \
  --sql "CREATE TABLE audit_ledger(code bigint PRIMARY KEY, owner_id int, note varchar(32));" >/dev/null
raft_client_call_strict "${restart_leader}" write --client-id 500 --request-id 8 \
  --sql "CREATE INDEX audit_owner_idx ON audit_ledger(owner_id);" >/dev/null
ledger_insert=$(raft_client_call_strict "${restart_leader}" write --client-id 500 --request-id 9 \
  --sql "INSERT INTO audit_ledger VALUES (9000000002, 3, 'restart'), (9000000001, 2, 'carry');")
final_index=$(raft_status_field "${ledger_insert}" committed_index)
expected_ledger=$'audit_ledger.code\taudit_ledger.owner_id\taudit_ledger.note\n9000000001\t2\tcarry\n9000000002\t3\trestart'
expected_ledger_index=$'audit_ledger.code\taudit_ledger.owner_id\taudit_ledger.note\n9000000002\t3\trestart'

final_linear=$(raft_client_call_strict "${restart_leader}" read --request-id 400000 --consistency linearizable \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${final_linear}" "${expected_final_accounts}"
if [[ $(raft_status_field "${final_linear}" read_timestamp) -lt ${final_index} ]]
then
  echo "final linearizable read is behind the post-restart Catalog writes" >&2
  exit 1
fi

for node_id in 1 2 3
do
  raft_await_status_at_least "${node_id}" last_applied "${final_index}" 600 "$((410000 + node_id * 1000))"
  final_accounts=$(raft_client_call_strict "${node_id}" read --request-id "$((420000 + node_id * 10))" \
    --consistency stale --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
  final_bulk_count=$(raft_client_call_strict "${node_id}" read --request-id "$((420001 + node_id * 10))" \
    --consistency stale --sql "SELECT count(*) AS row_count FROM chain_bulk;")
  final_bulk_sample=$(raft_client_call_strict "${node_id}" read --request-id "$((420002 + node_id * 10))" \
    --consistency stale \
    --sql "SELECT id, tag FROM chain_bulk WHERE id = 1 OR id = 800 OR id = 1600 ORDER BY id;")
  final_ledger=$(raft_client_call_strict "${node_id}" read --request-id "$((420003 + node_id * 10))" \
    --consistency stale --sql "SELECT code, owner_id, note FROM audit_ledger ORDER BY code;")
  final_ledger_index=$(raft_client_call_strict "${node_id}" read --request-id "$((420004 + node_id * 10))" \
    --consistency stale --sql "SELECT code, owner_id, note FROM audit_ledger WHERE owner_id = 3;")
  raft_assert_query_exact "${final_accounts}" "${expected_final_accounts}"
  raft_assert_query_exact "${final_bulk_count}" "${expected_bulk_count}"
  raft_assert_query_exact "${final_bulk_sample}" "${expected_bulk_sample}"
  raft_assert_query_exact "${final_ledger}" "${expected_ledger}"
  raft_assert_query_exact "${final_ledger_index}" "${expected_ledger_index}"
done

echo "M3-M7 distributed cumulative production chain passed; artifacts: ${artifact_root}"
