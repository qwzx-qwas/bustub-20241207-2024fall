#!/usr/bin/env bash

set -euo pipefail

build_dir=${1:-/tmp/bustub-raft-build-clang}
port_base=${2:-27100}
artifact_root=${3:-"/tmp/bustub-raft-m6-smoke-$$"}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "${script_dir}/raft_process_harness.sh"
raft_harness_init "${build_dir}" "${port_base}" "${artifact_root}" m6-smoke

raft_timeline_step "E2E-01/03/08/13: start three production processes and elect a ready Leader"
raft_start_all_nodes

leader=$(raft_find_leader)
raft_timeline_step "E2E-01/03: replicate canonical DDL/DML and byte-stable client identities"
before_rejection=$(raft_status "${leader}" 190)
raft_client_expect_status "${leader}" REJECTED write --client-id 500 --request-id 1 \
  --sql "CREATE TABLE accounts(value int);"
after_rejection=$(raft_status "${leader}" 191)
if [[ $(raft_status_field "${before_rejection}" commit_index) != $(raft_status_field "${after_rejection}" commit_index) ]]
then
  echo "rejected DDL changed the Raft commit index" >&2
  exit 1
fi
# Reusing the rejected object name proves that admission did not mutate the
# production Catalog before the Raft proposal boundary.
raft_client_call_eventually "${leader}" write --client-id 500 --request-id 1 \
  --sql "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32), balance int);"
before_unique_rejection=$(raft_status "${leader}" 192)
raft_client_expect_status "${leader}" REJECTED write --client-id 500 --request-id 2 \
  --sql "CREATE UNIQUE INDEX rejected_unique ON accounts(name);"
after_unique_rejection=$(raft_status "${leader}" 193)
if [[ $(raft_status_field "${before_unique_rejection}" commit_index) != \
      $(raft_status_field "${after_unique_rejection}" commit_index) ]]
then
  echo "rejected secondary UNIQUE DDL changed the Raft commit index" >&2
  exit 1
fi
# Reusing the rejected index name independently checks that failed admission
# left no hidden index-name reservation behind.
raft_client_call_eventually "${leader}" write --client-id 500 --request-id 2 \
  --sql "INSERT INTO accounts VALUES (2, 'two', 20), (1, 'one', 10);"
raft_client_call_eventually "${leader}" write --client-id 500 --request-id 3 \
  --sql "CREATE INDEX rejected_unique ON accounts(name);"

raft_timeline_step "E2E-03: commit through a proxy that drops the complete response before the client receives it"
proxy_result="${artifact_root}/dropped-response.txt"
proxy_port=$((port_base + 250))
first_response_bytes=$(raft_client_write_with_dropped_response "${leader}" "${proxy_port}" "${proxy_result}" \
  500 4 "UPDATE accounts SET balance = balance + 7 WHERE id <= 2;")

raft_timeline_step "E2E-03/04: kill Leader, elect replacement, retry ambiguous request"
raft_stop_node "${leader}" KILL
new_leader=$(raft_find_leader "${leader}")

raft_timeline_step "E2E-13: inspect the ready replacement Leader before any client proposal after election"
new_leader_status=$(raft_status "${new_leader}" 247)
noop_index=$(raft_status_field "${new_leader_status}" commit_index)
if [[ $(raft_status_field "${new_leader_status}" last_applied) != "${noop_index}" ]] || \
   [[ $(raft_status_field "${new_leader_status}" published_applied_index) != "${noop_index}" ]]
then
  echo "ready replacement Leader did not publish its current-term NOOP boundary" >&2
  exit 1
fi
noop_read=$(raft_client_call_strict "${new_leader}" read --request-id 248 --consistency linearizable \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${noop_read}" $'accounts.id\taccounts.name\taccounts.balance\n1\tone\t17\n2\ttwo\t27'
if [[ $(raft_status_field "${noop_read}" read_timestamp) -lt ${noop_index} ]]
then
  echo "linearizable read timestamp is behind the replacement Leader NOOP" >&2
  exit 1
fi

retry_output=$(raft_client_call_eventually "${new_leader}" write --client-id 500 --request-id 4 \
  --sql "UPDATE accounts SET balance = balance + 7 WHERE id <= 2;")
retry_response_bytes=$(raft_status_field "${retry_output}" response_bytes)
if [[ ${retry_response_bytes} != "${first_response_bytes}" ]]
then
  echo "deduplicated WriteResponseV1 bytes differ after Leader replacement" >&2
  exit 1
fi
once_only=$(raft_client_call_eventually "${new_leader}" read --request-id 249 --consistency linearizable \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${once_only}" $'accounts.id\taccounts.name\taccounts.balance\n1\tone\t17\n2\ttwo\t27'

raft_client_call_eventually "${new_leader}" write --client-id 500 --request-id 5 \
  --sql "DELETE FROM accounts WHERE id = 1;"

linear_read=$(raft_client_call_eventually "${new_leader}" read --request-id 251 --consistency linearizable \
  --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
raft_assert_query_exact "${linear_read}" $'accounts.id\taccounts.name\taccounts.balance\n2\ttwo\t27'

raft_timeline_step "E2E-04/08: restart old node, await durable catch-up, then issue explicit stale read"
raft_start_node "${leader}"
final_commit=$(raft_status_field "$(raft_status "${new_leader}" 299)" commit_index)
raft_await_status_at_least "${leader}" commit_index "${final_commit}" 200 200000
raft_await_status_at_least "${leader}" last_applied "${final_commit}" 200 210000

for node_id in 1 2 3
do
  stale_read=$(raft_client_call_strict "${node_id}" read --request-id "$((300 + node_id))" --consistency stale \
    --sql "SELECT id, name, balance FROM accounts ORDER BY id;")
  stale_ts=$(raft_status_field "${stale_read}" read_timestamp)
  stale_published=$(raft_status_field "${stale_read}" published_applied_index)
  if [[ ${stale_ts} != "${stale_published}" ]]
  then
    echo "node ${node_id} stale read timestamp differs from its published index" >&2
    exit 1
  fi
  raft_assert_query_exact "${stale_read}" $'accounts.id\taccounts.name\taccounts.balance\n2\ttwo\t27'
  secondary_read=$(raft_client_call_strict "${node_id}" read --request-id "$((400 + node_id))" --consistency stale \
    --sql "SELECT id, name, balance FROM accounts WHERE name = 'two';")
  raft_assert_query_exact "${secondary_read}" $'accounts.id\taccounts.name\taccounts.balance\n2\ttwo\t27'
done

echo "M6 binary smoke passed; artifacts: ${artifact_root}"
