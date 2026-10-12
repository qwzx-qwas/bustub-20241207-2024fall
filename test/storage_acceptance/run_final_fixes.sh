#!/usr/bin/env bash
# User-run handoff. Reuses the original runner and business oracles unchanged.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH="$PWD/.cache/toolchains/gcc-16.2.0/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
phase=${1:?usage: run_final_fixes.sh prepare|verify|correctness|performance}
build=.cache/final-acceptance/build
marker=.cache/final-acceptance/fixes-run-root
fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}
run_logged() {
  local label=$1 log=$2 code
  shift 2
  printf '[RUN] %s\nLog: %s\n' "$label" "$log"
  if "$@" > "$log" 2>&1; then
    printf '[OK] %s\n' "$label"
  else
    code=$?
    printf '[FAILED] %s (exit=%s)\nLast 40 log lines:\n' "$label" "$code" >&2
    tail -n 40 -- "$log" >&2
    printf 'Full log: %s\n' "$log" >&2
    return "$code"
  fi
}
case "$phase" in
  prepare|verify|correctness|performance) ;;
  *) fail 'Usage: run_final_fixes.sh prepare|verify|correctness|performance' ;;
esac
if [[ $phase == prepare ]]; then
  mkdir -p .cache/final-acceptance
  run_root=$(mktemp -d "$PWD/test-results/storage-fixed-$(date +%Y%m%d-%H%M%S)-XXXXXX")
  printf '%s\n' "$run_root" > "$marker"
  printf 'Results: %s\n' "$run_root"
  run_logged 'Configure Release build' "$run_root/configure.log" \
    cmake -S . -B "$build" -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="$PWD/.cache/toolchains/gcc-16.2.0/bin/gcc" \
    -DCMAKE_CXX_COMPILER="$PWD/.cache/toolchains/gcc-16.2.0/bin/g++"
  run_logged 'Build nodes and contract tests' "$run_root/build.log" \
    cmake --build "$build" --target final_performance_test versioned_frame_test build-storage-e2e -j 4
  test -x .cache/final-acceptance/check-history || fail 'Missing .cache/final-acceptance/check-history'
  python3 - "$run_root" <<'PY'
import hashlib,json,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); files=set()
for source in ('src','tools','test/storage_acceptance','test/storage'):
    files.update(p for p in Path(source).rglob('*') if p.is_file() and p.suffix in ('.h','.cpp','.cc','.py','.sh','.txt'))
files.update(Path(p) for p in (
    'CMakeLists.txt','test/CMakeLists.txt','test/legacy_raft/recovery/versioned_frame_test.cpp',
    'test/include/raft/synchronous_test_node.h',
    'docs/storage_redesign/node-storage.example.conf','.cache/final-acceptance/build/CMakeCache.txt',
    '.cache/final-acceptance/build/bin/bustub-node','.cache/final-acceptance/build/bin/bustub-client',
    '.cache/final-acceptance/build/test/storage-e2e-driver','.cache/final-acceptance/build/test/final_performance_test',
    '.cache/final-acceptance/build/test/versioned_frame_test','.cache/final-acceptance/check-history'))
def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()
(root/'SHA256SUMS').write_text(''.join(f'{sha(p)}  {p}\n' for p in sorted(files)))
(root/'freeze.json').write_text(json.dumps({'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
    'source':'HEAD plus working tree; SHA256SUMS is authoritative',
    'scope':'same Release build; original inputs, cache limits and deadlines; no profiling'},indent=2)+'\n')
PY
  printf 'Prepared. Next: bash test/storage_acceptance/run_final_fixes.sh verify\n'
  exit 0
fi
test -s "$marker" || fail 'No prepared run. First run: bash test/storage_acceptance/run_final_fixes.sh prepare'
IFS= read -r run_root < "$marker"
printf 'Results: %s\n' "$run_root"
test -f "$run_root/SHA256SUMS" || fail "Prepare did not finish. See $run_root/configure.log and build.log"
if ! run_logged 'Check frozen source and binaries' "$run_root/fingerprint-check.log" \
    sha256sum -c --quiet "$run_root/SHA256SUMS"; then
  fail 'Source or binaries changed. Preserve this run; use prepare to create a new result directory.'
fi
if [[ $phase == verify ]]; then
  run_logged 'CRC compatibility' "$run_root/crc.log" \
    "$build/test/versioned_frame_test" --gtest_output="xml:$run_root/crc.xml"
  run_logged 'Recovery, snapshot and GC contracts' "$run_root/contracts.log" \
    "$build/test/final_performance_test" --gtest_output="xml:$run_root/contracts.xml"
  run_logged 'Acceptance report contracts' "$run_root/report-contracts.log" \
    python3 test/storage_acceptance/test_report_contracts.py
  touch "$run_root/verify.ok"
  printf 'Contract checks passed. Next: bash test/storage_acceptance/run_final_fixes.sh correctness\n'
  exit 0
fi
run_case() {
  local scenario=$1
  shift
  local host_free
  host_free=$(bash test/storage_acceptance/run_diagnostic.sh host-free) || return $?
  printf 'Starting %s; host free=%s GiB; log=%s/%s.log\n' "$scenario" "$host_free" "$run_root" "$scenario"
  run_logged "$scenario acceptance" "$run_root/$scenario.log" \
    python3 test/storage_acceptance/run.py "$scenario" --build "$build" --output "$run_root/$scenario" \
    --role baseline --port-base 39100 --host-free-gib "$host_free" \
    --storage-template docs/storage_redesign/node-storage.example.conf "$@" || return $?
  # The runner can finish an observation without meeting the acceptance goal.
  python3 - "$run_root/$scenario" "$scenario" <<'PY'
import json,sys
from pathlib import Path
files=sorted(Path(sys.argv[1]).glob('*/result.json'))
expected={'C1':1,'C2':3,'C3':3,'C4':1,'C5':2,'P1':6,'P2':12,'P3':1,'P4':1}[sys.argv[2]]
ok=len(files)==expected
if not ok:
    print(f'Expected {expected} result files; found {len(files)} in {sys.argv[1]}', file=sys.stderr)
for p in files:
    d=json.loads(p.read_text())
    passed=(all(d.get(k) is True for k in ('measurement_complete','measurement_valid','business_validated'))
            if sys.argv[2].startswith('P') else d.get('status')=='passed')
    print(p.parent.name, 'PASS' if passed else 'NOT PASSED',
          'status='+str(d.get('status')), 'business='+str(d.get('business_validated')))
    ok &= passed
sys.exit(0 if ok else 1)
PY
}
if [[ $phase == correctness ]]; then
  test -f "$run_root/verify.ok" || fail "Cannot start correctness: verify has not passed. See $run_root/crc.log, contracts.log and report-contracts.log."
  run_case C1
  run_case C2 --matrix --checker .cache/final-acceptance/check-history
  run_case C3 --matrix --checker .cache/final-acceptance/check-history
  run_case C4
  run_case C5 --matrix
  touch "$run_root/correctness.ok"
  printf 'C1-C5 passed. Next: bash test/storage_acceptance/run_final_fixes.sh performance\n'
elif [[ $phase == performance ]]; then
  test -f "$run_root/correctness.ok" || fail "Cannot start performance: correctness has not passed. Complete verify and correctness first. Results: $run_root"
  failures=0
  # Independent performance evidence is still useful when P3 misses its goal.
  run_case P3 || failures=$((failures+1))
  run_case P4 --fixed-rate 1 || failures=$((failures+1))
  run_case P1 --matrix || failures=$((failures+1))
  run_case P2 --matrix || failures=$((failures+1))
  printf 'Performance scenarios not passed: %s\nResults: %s\n' "$failures" "$run_root"
  test "$failures" -eq 0
fi
