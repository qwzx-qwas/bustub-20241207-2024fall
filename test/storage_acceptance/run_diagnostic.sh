#!/usr/bin/env bash
# One unchanged acceptance point plus an external observer. Never rebuilds nodes.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH="$PWD/.cache/toolchains/gcc-16.2.0/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

point=${1:?usage: run_diagnostic.sh P3|P1-uniform|P1-hotspot|P2-c4|P2-c8|P4|host-free [host-free-GiB] [--no-perf]}
host_free=${2:-}
if [[ -z $host_free ]]; then
  # Locate this distribution's VHD volume, not the guest's virtual free space.
  # Encode the command so distro names cannot become PowerShell syntax.
  host_query=$(python3 - <<'PY'
import base64
import os
distro = os.environ['WSL_DISTRO_NAME'].replace("'", "''")
script = r"""
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$distro = '%s'
$entry = @(Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss' |
    Get-ItemProperty | Where-Object { $_.DistributionName -eq $distro })
if ($entry.Count -ne 1) { throw 'Cannot locate the WSL distribution volume' }
$volume = Get-Volume -FilePath $entry[0].BasePath
if ($null -eq $volume.SizeRemaining) { throw 'Cannot read host free space' }
[Console]::Error.WriteLine('WSL host volume: ' + $volume.DriveLetter + ':')
[Console]::WriteLine([math]::Floor($volume.SizeRemaining / 1GB))
""" % distro
print(base64.b64encode(script.encode('utf-16le')).decode())
PY
  )
  host_free=$(powershell.exe -NoProfile -NonInteractive -EncodedCommand "$host_query" | tr -d '\r\n')
fi
if [[ $point == host-free ]]; then
  printf '%s\n' "$host_free"
  exit 0
fi
printf 'Host free space supplied: %s GiB (minimum 11)\n' "$host_free"
extra=()
observer=()
case "$point" in
  P3) scenario=P3; case_name=P3-c1-r1; observer=(--when stall --stall-seconds 10 --captures 3 --stacks) ;;
  P1-uniform|P1-hotspot)
    scenario=P1; case_name="${point}-c4-r1"; extra=(--variant "${point#P1-}"); observer=(--when measure) ;;
  P2-c4|P2-c8)
    scenario=P2; case_name="${point}-r1"; extra=(--clients "${point#P2-c}"); observer=(--when measure) ;;
  P4) scenario=P4; case_name=P4-c6-r1; extra=(--fixed-rate 1); observer=(--when restart) ;;
  *) echo "Unknown diagnostic point: $point" >&2; exit 2 ;;
esac
diag_perf=${DIAG_PERF:-/usr/lib/linux-tools/6.8.0-142-generic/perf}
if [[ ${3:-} == --no-perf ]]; then
  observer+=(--no-perf --stacks)
elif [[ -n ${3:-} ]]; then
  echo 'Third argument must be --no-perf or omitted' >&2
  exit 2
else
  "$diag_perf" --version
  observer+=(--perf "$diag_perf")
fi
command -v pidstat >/dev/null
command -v gdb >/dev/null
# Authenticate now. Run only the observer as root so credentials cannot expire
# while it waits for P3's later checkpoint; database processes stay unprivileged.
sudo -v
if [[ ${3:-} != --no-perf ]]; then
  # Executed only when the user runs this script. Refuse to spend an hour on a
  # workload when this WSL kernel cannot provide the selected sampling event.
  sudo -n -- "$diag_perf" stat -e cpu-clock:u -- true
fi
diag_root=$(mktemp -d "$PWD/test-results/storage-diagnostic-$(date +%Y%m%d-%H%M%S)-${point}-XXXXXX")
stop_file="$diag_root/observer.stop"
trap 'touch -- "$stop_file"' EXIT
printf '%s\n' 'Diagnostic only: external profiling/ptrace overhead; do not merge with historical baseline.' \
  > "$diag_root/DIAGNOSTIC_ONLY.txt"
printf 'Results: %s\n' "$diag_root"
sudo -n -- python3 test/storage_acceptance/capture_diagnostics.py \
  --run "$diag_root/$scenario/$case_name" --output "$diag_root/profile" \
  --stop-file "$stop_file" --seconds 60 "${observer[@]}" \
  > "$diag_root/capture.log" 2>&1 &
observer_pid=$!
test_code=0
python3 test/storage_acceptance/run.py "$scenario" \
  --build .cache/final-acceptance/build --output "$diag_root/$scenario" \
  --role qualification --port-base 39100 --host-free-gib "$host_free" \
  --storage-template docs/storage_redesign/node-storage.example.conf "${extra[@]}" \
  > "$diag_root/run.log" 2>&1 || test_code=$?
touch -- "$stop_file"
observer_code=0
wait "$observer_pid" || observer_code=$?
printf 'test_exit=%s observer_exit=%s\nResults: %s\n' "$test_code" "$observer_code" "$diag_root"
if [[ -d $diag_root/profile ]]; then
  # Only this new diagnostic directory, never the database or historic results.
  sudo chown -R -- "$(id -u):$(id -g)" "$diag_root/profile"
fi
exit "$((test_code != 0 || observer_code != 0))"
