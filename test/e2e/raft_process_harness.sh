#!/usr/bin/env bash

# Shared process-boundary controls for the M6/M7 timeline scenarios. This file
# is sourced by test scripts and is never linked into a production target.

declare -Ag RAFT_NODE_PIDS=()
declare -Ag RAFT_ADVERTISED_RAFT_PORTS=()
declare -Ag RAFT_PROXY_PIDS=()
declare -Ag RAFT_PROXY_CONTROLS=()
declare -Ag RAFT_AUX_PIDS=()
declare -ag RAFT_EXTRA_NODE_ARGS=()

raft_harness_init() {
  if [[ $# -lt 4 ]]
  then
    echo "raft_harness_init requires build dir, port base, artifact root, and group id" >&2
    return 1
  fi
  RAFT_BUILD_DIR=$1
  RAFT_PORT_BASE=$2
  RAFT_ARTIFACT_ROOT=$3
  RAFT_GROUP_ID=$4
  RAFT_ELECTION_TIMEOUT_MIN_MS=${RAFT_ELECTION_TIMEOUT_MIN_MS:-250}
  RAFT_ELECTION_TIMEOUT_MAX_MS=${RAFT_ELECTION_TIMEOUT_MAX_MS:-500}
  shift 4
  RAFT_EXTRA_NODE_ARGS=("$@")
  RAFT_NODE_BIN="${RAFT_BUILD_DIR}/bin/bustub-node"
  RAFT_CLIENT_BIN="${RAFT_BUILD_DIR}/bin/bustub-client"
  if [[ ! -x ${RAFT_NODE_BIN} ]] || [[ ! -x ${RAFT_CLIENT_BIN} ]]
  then
    echo "Raft E2E binaries are missing from ${RAFT_BUILD_DIR}/bin" >&2
    return 1
  fi
  mkdir -p "$(dirname -- "${RAFT_ARTIFACT_ROOT}")"
  if ! mkdir "${RAFT_ARTIFACT_ROOT}" 2>/dev/null
  then
    echo "Raft E2E artifact root must not already exist: ${RAFT_ARTIFACT_ROOT}" >&2
    return 1
  fi
  trap 'raft_harness_cleanup $?' EXIT
}

raft_timeline_step() {
  printf '[raft-e2e] %s\n' "$*" >&2
}

raft_port() {
  echo $((RAFT_PORT_BASE + $1))
}

raft_client_port() {
  echo $((RAFT_PORT_BASE + 100 + $1))
}

raft_advertised_port() {
  local node_id=$1
  echo "${RAFT_ADVERTISED_RAFT_PORTS[${node_id}]:-$(raft_port "${node_id}")}"
}

raft_node_alive() {
  local node_id=$1
  [[ -n ${RAFT_NODE_PIDS[${node_id}]:-} ]] && kill -0 "${RAFT_NODE_PIDS[${node_id}]}" 2>/dev/null
}

raft_wait_for_process_exit() {
  local pid=$1
  local state
  for _ in $(seq 1 500)
  do
    if ! kill -0 "${pid}" 2>/dev/null
    then
      return
    fi
    state=$(ps -o stat= -p "${pid}" 2>/dev/null || true)
    if [[ ${state} == Z* ]]
    then
      return
    fi
    sleep 0.02
  done
  return 1
}

raft_register_aux_process() {
  local name=$1
  local pid=$2
  if [[ -n ${RAFT_AUX_PIDS[${name}]:-} ]]
  then
    echo "duplicate Raft E2E auxiliary process name: ${name}" >&2
    return 1
  fi
  RAFT_AUX_PIDS[${name}]=${pid}
}

raft_wait_aux_process() {
  local name=$1
  local expected_status=${2:-0}
  local pid=${RAFT_AUX_PIDS[${name}]:-}
  local status
  if [[ -z ${pid} ]]
  then
    echo "unknown Raft E2E auxiliary process: ${name}" >&2
    return 1
  fi
  if wait "${pid}"
  then
    status=0
  else
    status=$?
  fi
  unset 'RAFT_AUX_PIDS['"${name}"']'
  if [[ ${status} -ne ${expected_status} ]]
  then
    echo "Raft E2E auxiliary process ${name} exited ${status}, expected ${expected_status}" >&2
    return 1
  fi
}

raft_stop_all_aux_processes() {
  local failed=0
  local name
  local pid
  local status
  for name in "${!RAFT_AUX_PIDS[@]}"
  do
    pid=${RAFT_AUX_PIDS[${name}]}
    if kill -0 "${pid}" 2>/dev/null
    then
      kill -TERM "${pid}" 2>/dev/null || true
      if ! raft_wait_for_process_exit "${pid}"
      then
        kill -KILL "${pid}" 2>/dev/null || true
      fi
    fi
    if wait "${pid}" 2>/dev/null
    then
      status=0
    else
      status=$?
    fi
    echo "Raft E2E auxiliary process ${name} was still registered at cleanup (exit=${status})" >&2
    failed=1
    unset 'RAFT_AUX_PIDS['"${name}"']'
  done
  return "${failed}"
}

raft_start_message_proxy() {
  local node_id=$1
  local proxy_port=$2
  local controls=$3
  mkdir -p "${controls}"
  python3 "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/raft_message_proxy.py" \
    --listen-port "${proxy_port}" --target-port "$(raft_port "${node_id}")" --controls "${controls}" \
    >"${controls}/proxy.log" 2>&1 &
  RAFT_PROXY_PIDS[${node_id}]=$!
  RAFT_PROXY_CONTROLS[${node_id}]=${controls}
  RAFT_ADVERTISED_RAFT_PORTS[${node_id}]=${proxy_port}
  for _ in $(seq 1 200)
  do
    if [[ -f ${controls}/ready ]] && kill -0 "${RAFT_PROXY_PIDS[${node_id}]}" 2>/dev/null
    then
      return
    fi
    sleep 0.01
  done
  echo "Raft message proxy for node ${node_id} did not become ready" >&2
  return 1
}

raft_stop_all_proxies() {
  local failed=0
  local node_id
  local controls
  local proxy_status
  for node_id in "${!RAFT_PROXY_PIDS[@]}"
  do
    if kill -0 "${RAFT_PROXY_PIDS[${node_id}]}" 2>/dev/null
    then
      kill -TERM "${RAFT_PROXY_PIDS[${node_id}]}" 2>/dev/null || true
      if ! raft_wait_for_process_exit "${RAFT_PROXY_PIDS[${node_id}]}"
      then
        echo "Raft message proxy ${node_id} did not stop after TERM" >&2
        kill -KILL "${RAFT_PROXY_PIDS[${node_id}]}" 2>/dev/null || true
        failed=1
      fi
    fi
    if wait "${RAFT_PROXY_PIDS[${node_id}]}" 2>/dev/null
    then
      proxy_status=0
    else
      proxy_status=$?
    fi
    if [[ ${proxy_status} -ne 143 ]]
    then
      echo "Raft message proxy ${node_id} exited ${proxy_status}, expected TERM status 143" >&2
      failed=1
    fi
    controls=${RAFT_PROXY_CONTROLS[${node_id}]}
    if [[ -f ${controls}/last-error ]]
    then
      echo "Raft message proxy ${node_id} observed a malformed frame or invariant failure:" >&2
      cat "${controls}/last-error" >&2
      failed=1
    fi
    if [[ -f ${controls}/replay-error ]]
    then
      echo "Raft message proxy ${node_id} failed snapshot replay:" >&2
      cat "${controls}/replay-error" >&2
      failed=1
    fi
    unset 'RAFT_PROXY_PIDS['"${node_id}"']'
    unset 'RAFT_PROXY_CONTROLS['"${node_id}"']'
  done
  return "${failed}"
}

raft_harness_cleanup() {
  local original_status=${1:-0}
  local cleanup_status=0
  trap - EXIT
  set +e
  raft_stop_all_aux_processes
  [[ $? -eq 0 ]] || cleanup_status=1
  raft_stop_all_nodes
  [[ $? -eq 0 ]] || cleanup_status=1
  raft_stop_all_proxies
  [[ $? -eq 0 ]] || cleanup_status=1
  if [[ ${original_status} -ne 0 ]]
  then
    exit "${original_status}"
  fi
  exit "${cleanup_status}"
}

raft_start_node() {
  local node_id=$1
  local exit_status
  local log_path="${RAFT_ARTIFACT_ROOT}/node-${node_id}.log"
  local peer_id
  local peer_args=()
  for peer_id in 1 2 3
  do
    if [[ ${peer_id} -ne ${node_id} ]]
    then
      peer_args+=(--peer "${peer_id}=127.0.0.1:$(raft_advertised_port "${peer_id}"),127.0.0.1:$(raft_client_port "${peer_id}")")
    fi
  done
  UBSAN_OPTIONS=halt_on_error=1 ASAN_OPTIONS=detect_leaks=0 "${RAFT_NODE_BIN}" \
    --node-id "${node_id}" \
    --group-id "${RAFT_GROUP_ID}" \
    --data-dir "${RAFT_ARTIFACT_ROOT}/node-${node_id}" \
    --raft-listen "127.0.0.1:$(raft_port "${node_id}")" \
    --client-listen "127.0.0.1:$(raft_client_port "${node_id}")" \
    --election-timeout-min-ms "${RAFT_ELECTION_TIMEOUT_MIN_MS}" \
    --election-timeout-max-ms "${RAFT_ELECTION_TIMEOUT_MAX_MS}" \
    --client-timeout-ms 3000 \
    "${RAFT_EXTRA_NODE_ARGS[@]}" \
    "${peer_args[@]}" >>"${log_path}" 2>&1 &
  RAFT_NODE_PIDS[${node_id}]=$!
  sleep 0.25
  if raft_node_alive "${node_id}"
  then
    return
  fi
  set +e
  wait "${RAFT_NODE_PIDS[${node_id}]}" 2>/dev/null
  exit_status=$?
  set -e
  unset 'RAFT_NODE_PIDS['"${node_id}"']'
  echo "node ${node_id} exited during its only startup attempt: status=${exit_status}; log=${log_path}" >&2
  return 1
}

raft_start_all_nodes() {
  local node_id
  for node_id in 1 2 3
  do
    raft_start_node "${node_id}"
  done
}

raft_stop_node() {
  local node_id=$1
  local signal=${2:-TERM}
  local expected_status=0
  local exit_status
  local pid=${RAFT_NODE_PIDS[${node_id}]:-}
  if [[ -z ${pid} ]]
  then
    return
  fi
  if [[ ${signal} == KILL ]]
  then
    expected_status=137
  fi
  if raft_node_alive "${node_id}"
  then
    kill "-${signal}" "${pid}" 2>/dev/null || true
    if ! raft_wait_for_process_exit "${pid}"
    then
      echo "node ${node_id} did not stop after ${signal}; forcing KILL" >&2
      kill -KILL "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
      unset 'RAFT_NODE_PIDS['"${node_id}"']'
      return 1
    fi
  fi
  if wait "${pid}" 2>/dev/null
  then
    exit_status=0
  else
    exit_status=$?
  fi
  unset 'RAFT_NODE_PIDS['"${node_id}"']'
  if [[ ${exit_status} -ne ${expected_status} ]]
  then
    echo "node ${node_id} exited ${exit_status}, expected ${expected_status} after ${signal}; log=${RAFT_ARTIFACT_ROOT}/node-${node_id}.log" >&2
    return 1
  fi
}

raft_stop_all_nodes() {
  local failed=0
  local node_id
  for node_id in 1 2 3
  do
    if raft_node_alive "${node_id}"
    then
      kill -TERM "${RAFT_NODE_PIDS[${node_id}]}" 2>/dev/null || true
    fi
  done
  for node_id in 1 2 3
  do
    if [[ -n ${RAFT_NODE_PIDS[${node_id}]:-} ]]
    then
      raft_stop_node "${node_id}" TERM || failed=1
    fi
  done
  return "${failed}"
}

raft_client_once() {
  local node_id=$1
  shift
  UBSAN_OPTIONS=halt_on_error=1 ASAN_OPTIONS=detect_leaks=0 "${RAFT_CLIENT_BIN}" "$@" \
    --endpoint "127.0.0.1:$(raft_client_port "${node_id}")" --timeout-ms 3000 2>&1
}

raft_is_retryable_transport_failure() {
  local output=$1
  [[ ${output} == "bustub-client: cannot connect to client endpoint "* ]] || \
    [[ ${output} == "bustub-client: distributed client request write failed" ]] || \
    [[ ${output} == "bustub-client: distributed client response prefix is unavailable" ]] || \
    [[ ${output} == "bustub-client: distributed client response is truncated" ]]
}

# Exactly one client process and one destination. Use this for assertions whose
# stimulus would be weakened by transparently trying a later Leader.
raft_client_call_strict() {
  local node_id=$1
  shift
  local output
  local status
  if output=$(raft_client_once "${node_id}" "$@")
  then
    printf '%s\n' "${output}"
    return
  else
    status=$?
  fi
  printf 'strict client call failed (node=%s, exit=%s):\n%s\n' "${node_id}" "${status}" "${output}" >&2
  return 1
}

# Bounded delivery retry for requests whose identity and semantics make an
# uncertain transport outcome safe to repeat. Signals, sanitizer failures,
# malformed responses, and every unclassified local failure remain fatal.
raft_client_call_eventually() {
  local node_id=$1
  shift
  local attempt
  local output
  local replacement
  local replacement_status
  local suggested_leader
  local status
  local status_line
  for attempt in 1 2 3 4 5 6 7 8 9 10
  do
    if output=$(raft_client_once "${node_id}" "$@")
    then
      printf '%s\n' "${output}"
      return
    else
      status=$?
    fi
    status_line=${output%%$'\n'*}
    if [[ ${status} -eq 1 ]] && raft_is_retryable_transport_failure "${output}"
    then
      suggested_leader=0
    elif [[ ${status} -eq 2 ]] && \
         [[ ${status_line} =~ ^status=(NOT_LEADER|TIMEOUT|UNAVAILABLE)([[:space:]]|$) ]]
    then
      suggested_leader=0
      if [[ ${output} =~ leader_id=([1-3]) ]]
      then
        suggested_leader=${BASH_REMATCH[1]}
      fi
    else
      printf 'unexpected client failure (exit=%s):\n%s\n' "${status}" "${output}" >&2
      return 1
    fi
    if raft_node_alive "${suggested_leader}"
    then
      node_id=${suggested_leader}
    elif replacement=$(raft_find_leader)
    then
      node_id=${replacement}
    else
      replacement_status=$?
      if [[ ${replacement_status} -eq 70 ]]
      then
        echo "fatal client failure while rediscovering Leader" >&2
        return 1
      fi
    fi
    sleep 0.05
  done
  printf '%s\n' "${output}" >&2
  return 1
}

# Backward-compatible spelling for out-of-tree scenarios. Formal scenarios in
# this repository use the explicit strict/eventually names.
raft_client_call() { raft_client_call_eventually "$@"; }

# One call whose outcome may be success or a narrowly classified transport
# ambiguity because the scenario deliberately kills the serving node. A local
# crash/signal and a protocol rejection are never accepted.
raft_client_call_allow_transport_ambiguity() {
  local node_id=$1
  shift
  local output
  local status
  if output=$(raft_client_once "${node_id}" "$@")
  then
    printf '%s\n' "${output}"
    return
  else
    status=$?
  fi
  if { [[ ${status} -eq 1 ]] && raft_is_retryable_transport_failure "${output}"; } || \
     { [[ ${status} -eq 2 ]] && [[ ${output} =~ ^status=(TIMEOUT|UNAVAILABLE)([[:space:]]|$) ]]; }
  then
    printf '%s\n' "${output}"
    return
  fi
  printf 'unclassified ambiguous client outcome (node=%s, exit=%s):\n%s\n' \
    "${node_id}" "${status}" "${output}" >&2
  return 1
}

# Send one identified write through the one-shot response-dropping proxy. The
# production server must commit and emit a complete response, while the one
# strict client process must fail only because no response byte was forwarded.
# Prints the committed WriteResponseV1 payload as lowercase hex.
raft_client_write_with_dropped_response() {
  if [[ $# -ne 6 ]]
  then
    echo "raft_client_write_with_dropped_response requires node, proxy port, result path, client ID, request ID, SQL" >&2
    return 1
  fi
  local node_id=$1
  local proxy_port=$2
  local result_path=$3
  local client_id=$4
  local request_id=$5
  local sql=$6
  local helper_dir
  local output
  local proxy_pid
  local status
  local response_record
  helper_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
  python3 "${helper_dir}/raft_drop_response_proxy.py" --listen-port "${proxy_port}" \
    --target-port "$(raft_client_port "${node_id}")" --result "${result_path}" --request-id "${request_id}" \
    >"${result_path}.proxy.log" 2>&1 &
  proxy_pid=$!
  raft_register_aux_process "response-drop-${request_id}" "${proxy_pid}"
  for _ in $(seq 1 100)
  do
    if [[ -f ${result_path}.ready ]] && kill -0 "${proxy_pid}" 2>/dev/null
    then
      break
    fi
    sleep 0.01
  done
  if [[ ! -f ${result_path}.ready ]] || ! kill -0 "${proxy_pid}" 2>/dev/null
  then
    raft_wait_aux_process "response-drop-${request_id}" 0 2>/dev/null || true
    echo "response-dropping proxy did not become ready; log=${result_path}.proxy.log" >&2
    return 1
  fi

  if output=$(UBSAN_OPTIONS=halt_on_error=1 ASAN_OPTIONS=detect_leaks=0 "${RAFT_CLIENT_BIN}" write \
    --endpoint "127.0.0.1:${proxy_port}" --client-id "${client_id}" --request-id "${request_id}" \
    --sql "${sql}" --timeout-ms 3000 2>&1)
  then
    status=0
  else
    status=$?
  fi
  if [[ ${status} -ne 1 ]] || \
     [[ ${output} != "bustub-client: distributed client response prefix is unavailable" ]]
  then
    kill -TERM "${proxy_pid}" 2>/dev/null || true
    raft_wait_aux_process "response-drop-${request_id}" 143 2>/dev/null || true
    printf 'response-drop client failed for an unexpected reason (exit=%s):\n%s\n' "${status}" "${output}" >&2
    return 1
  fi
  if ! raft_wait_aux_process "response-drop-${request_id}" 0
  then
    echo "response-dropping proxy failed; log=${result_path}.proxy.log" >&2
    return 1
  fi
  if [[ ! -f ${result_path} ]]
  then
    echo "response-dropping proxy did not record a committed response" >&2
    return 1
  fi
  response_record=$(<"${result_path}")
  if [[ ! ${response_record} =~ ^response_bytes=([0-9a-f]+)$ ]]
  then
    echo "invalid response-dropping proxy result: ${response_record}" >&2
    return 1
  fi
  printf '%s\n' "${BASH_REMATCH[1]}"
}

# One request for which a non-success protocol status is the expected result.
# Unlike raft_client_call_eventually this never reroutes or retries and therefore cannot
# turn a safety assertion into a later success.
raft_client_expect_status() {
  local node_id=$1
  local expected=$2
  shift 2
  local output
  local exit_status
  if output=$(raft_client_once "${node_id}" "$@")
  then
    exit_status=0
  else
    exit_status=$?
  fi
  if [[ ${exit_status} -ne 2 ]] || [[ ! ${output} =~ ^status=${expected}([[:space:]]|$) ]]
  then
    printf 'expected status=%s from node %s, got exit=%s:\n%s\n' \
      "${expected}" "${node_id}" "${exit_status}" "${output}" >&2
    return 1
  fi
  printf '%s\n' "${output}"
}

raft_status_field() {
  local output=$1
  local field=$2
  if [[ ${output} =~ (^|[[:space:]])${field}=([^[:space:]]+) ]]
  then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return
  fi
  echo "missing ${field} in client output: ${output}" >&2
  return 1
}

# Read the fixed index field of a V1 Raft CURRENT record independently of the
# production decoder. Status exposes the oldest retained recovery base, not
# necessarily the latest published image, so snapshot-boundary tests need this
# narrow read-only oracle.
raft_current_snapshot_index() {
  if [[ $# -ne 1 ]]
  then
    echo "raft_current_snapshot_index requires a CURRENT path" >&2
    return 1
  fi
  python3 -c 'import pathlib, struct, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
if len(data) < 28 or data[:8] != b"BRCURR01" or struct.unpack(">I", data[8:12])[0] != 1:
    raise SystemExit("invalid V1 Raft CURRENT record")
print(struct.unpack(">Q", data[20:28])[0])' "$1"
}

# Compare the complete ordered logical result (header and every row), mirroring
# BusTub's SQLLogicTest literal-result oracle. The volatile status line is
# checked for success and excluded from the comparison.
raft_assert_query_exact() {
  local output=$1
  local expected=$2
  local status_line=${output%%$'\n'*}
  local actual
  if [[ ! ${status_line} =~ ^status=OK([[:space:]]|$) ]] || [[ ${status_line} != *" read_timestamp="* ]] || \
     [[ ${output} != *$'\n'* ]]
  then
    printf 'query did not return a successful result frame:\n%s\n' "${output}" >&2
    return 1
  fi
  actual=${output#*$'\n'}
  if [[ ${actual} != "${expected}" ]]
  then
    printf 'query result mismatch\nexpected: %q\nactual:   %q\nfull output:\n%s\n' \
      "${expected}" "${actual}" "${output}" >&2
    return 1
  fi
}

raft_status() {
  local node_id=$1
  local request_id=$2
  local timeout_ms=${3:-300}
  local output
  local exit_status
  if output=$(UBSAN_OPTIONS=halt_on_error=1 ASAN_OPTIONS=detect_leaks=0 "${RAFT_CLIENT_BIN}" status \
    --endpoint "127.0.0.1:$(raft_client_port "${node_id}")" --request-id "${request_id}" \
    --timeout-ms "${timeout_ms}" 2>&1)
  then
    printf '%s\n' "${output}"
    return
  else
    exit_status=$?
  fi
  if { [[ ${exit_status} -eq 1 ]] && raft_is_retryable_transport_failure "${output}"; } || \
     { [[ ${exit_status} -eq 2 ]] && \
       [[ ${output} =~ ^status=(NOT_LEADER|TIMEOUT|UNAVAILABLE)([[:space:]]|$) ]]; }
  then
    return 1
  fi
  printf 'fatal status-client failure (node=%s, exit=%s):\n%s\n' \
    "${node_id}" "${exit_status}" "${output}" >&2
  return 70
}

raft_find_leader() {
  local excluded=${1:-0}
  local round
  local node_id
  local output
  local status
  for round in $(seq 1 200)
  do
    for node_id in 1 2 3
    do
      if [[ ${node_id} -eq ${excluded} ]] || ! raft_node_alive "${node_id}"
      then
        continue
      fi
      if output=$(raft_status "${node_id}" "$((100000 + round * 10 + node_id))")
      then
        status=0
      else
        status=$?
        if [[ ${status} -eq 70 ]]
        then
          return 70
        fi
      fi
      if [[ ${output} =~ ^status=OK([[:space:]]|$) ]] && [[ ${output} == *"node_id=${node_id}"* ]] && \
         [[ ${output} == *"leader_ready=1"* ]] && [[ ${output} == *"leader_id=${node_id}"* ]]
      then
        echo "${node_id}"
        return
      fi
    done
    sleep 0.05
  done
  echo "timed out finding Leader" >&2
  return 1
}

raft_await_status_at_least() {
  local node_id=$1
  local field=$2
  local expected=$3
  local rounds=${4:-300}
  local request_base=${5:-200000}
  local round
  local output
  local status
  local pattern="${field}=([0-9]+)"
  for round in $(seq 1 "${rounds}")
  do
    if output=$(raft_status "${node_id}" "$((request_base + round))")
    then
      status=0
    else
      status=$?
      if [[ ${status} -eq 70 ]]
      then
        return 70
      fi
    fi
    if [[ ${output} =~ ^status=OK([[:space:]]|$) ]] && [[ ${output} =~ ${pattern} ]] && \
       [[ ${BASH_REMATCH[1]} -ge ${expected} ]]
    then
      RAFT_LAST_STATUS=${output}
      return
    fi
    sleep 0.05
  done
  echo "node ${node_id} did not reach ${field} >= ${expected}; last status: ${output:-unavailable}" >&2
  return 1
}
