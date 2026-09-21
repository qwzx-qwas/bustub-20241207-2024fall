#!/usr/bin/env bash
# Test-only control channel. The shared harness owns processes and wait statuses.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/../support/raft_process_harness.sh"
if [[ $# -ne 6 ]]; then
  echo "usage: cluster.sh BUILD PORT_BASE NEW_NODE_ROOT BUFFER_PAGES SNAPSHOT_THRESHOLD PROXIES" >&2
  exit 2
fi
export RAFT_CLIENT_TIMEOUT_MS=5000
export RAFT_ELECTION_TIMEOUT_MIN_MS=300
export RAFT_ELECTION_TIMEOUT_MAX_MS=600
raft_harness_init "$1" "$2" "$3" storage-acceptance \
  --buffer-pool-size "$4" --snapshot-threshold-entries "$5" \
  --heartbeat-interval-ms 50 --tick-interval-ms 10
trap 'exit 143' TERM
trap 'exit 130' INT
if [[ $6 == 1 ]]; then
  for node in 1 2 3; do
    raft_start_message_proxy "$node" "$((RAFT_PORT_BASE + 200 + node))" \
      "${RAFT_ARTIFACT_ROOT}/proxy-${node}" --observe --node-id "$node"
  done
fi
raft_start_all_nodes
printf 'READY\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "${RAFT_NODE_PIDS[1]}" "${RAFT_NODE_PIDS[2]}" "${RAFT_NODE_PIDS[3]}" \
  "${RAFT_PROXY_PIDS[1]:-0}" "${RAFT_PROXY_PIDS[2]:-0}" "${RAFT_PROXY_PIDS[3]:-0}"
while IFS=' ' read -r command node rest; do
  [[ -z ${rest:-} ]] || exit 2
  if [[ $command == STOP && -z ${node:-} ]]; then
    break
  fi
  [[ ${node:-} =~ ^[123]$ ]] || exit 2
  case "$command" in
    KILL)
      pid=${RAFT_NODE_PIDS[$node]:-}
      [[ -n $pid ]] || exit 2
      raft_stop_node "$node" KILL
      # raft_stop_node checked wait's 137 status before this ACK.
      printf 'EXIT\t%s\t%s\t137\n' "$node" "$pid"
      ;;
    START)
      [[ -z ${RAFT_NODE_PIDS[$node]:-} ]] || exit 2
      raft_start_node "$node"
      printf 'STARTED\t%s\t%s\n' "$node" "${RAFT_NODE_PIDS[$node]}"
      ;;
    *) exit 2 ;;
  esac
done
