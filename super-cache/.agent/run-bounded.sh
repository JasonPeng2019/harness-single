#!/usr/bin/env bash
set -u

usage() {
  cat <<'EOF'
Usage: run-bounded.sh --command CMD --expected-upper-bound-seconds N
       --cleanup-allowance-seconds N --timeout-basis TEXT [options]

Options:
  --working-directory DIR          Default: current directory
  --maximum-lifetime-seconds N     Must equal expected bound plus cleanup allowance
  --heartbeat-interval-seconds N   Default: 30, maximum: 60
  --result-path PATH               Default: unique .agent-runtime result
EOF
}

command_text=''
working_directory=$PWD
expected=''
cleanup=''
maximum=''
heartbeat=30
timeout_basis=''
result_path=''

while (($#)); do
  case "$1" in
    --command) command_text=${2:?}; shift 2 ;;
    --working-directory) working_directory=${2:?}; shift 2 ;;
    --expected-upper-bound-seconds) expected=${2:?}; shift 2 ;;
    --cleanup-allowance-seconds) cleanup=${2:?}; shift 2 ;;
    --maximum-lifetime-seconds) maximum=${2:?}; shift 2 ;;
    --heartbeat-interval-seconds) heartbeat=${2:?}; shift 2 ;;
    --timeout-basis) timeout_basis=${2:?}; shift 2 ;;
    --result-path) result_path=${2:?}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$command_text" && -n "$expected" && -n "$cleanup" && -n "$timeout_basis" ]] || { usage >&2; exit 2; }
[[ "$expected" =~ ^[1-9][0-9]*$ ]] || { printf 'Expected upper bound must be positive.\n' >&2; exit 2; }
[[ "$cleanup" =~ ^[1-9][0-9]*$ && "$cleanup" -le 120 ]] || { printf 'Cleanup allowance must be 1..120.\n' >&2; exit 2; }
maximum_reasonable_cleanup=$(((expected + 3) / 4))
((maximum_reasonable_cleanup >= 5)) || maximum_reasonable_cleanup=5
((maximum_reasonable_cleanup <= 120)) || maximum_reasonable_cleanup=120
((cleanup <= maximum_reasonable_cleanup)) || {
  printf 'Cleanup allowance %s is excessive for expected upper bound %s; maximum reasonable cleanup allowance is %s seconds.\n' "$cleanup" "$expected" "$maximum_reasonable_cleanup" >&2
  exit 2
}
[[ "$heartbeat" =~ ^[1-9][0-9]*$ && "$heartbeat" -le 60 ]] || { printf 'Heartbeat interval must be 1..60.\n' >&2; exit 2; }
[[ -d "$working_directory" ]] || { printf 'Working directory does not exist: %s\n' "$working_directory" >&2; exit 2; }

working_directory=$(cd "$working_directory" && pwd)
computed_maximum=$((expected + cleanup))
if [[ -z "$maximum" ]]; then
  maximum=$computed_maximum
elif [[ ! "$maximum" =~ ^[1-9][0-9]*$ || "$maximum" -ne "$computed_maximum" ]]; then
  printf 'Maximum lifetime must equal expected upper bound plus cleanup allowance (%s + %s = %s).\n' "$expected" "$cleanup" "$computed_maximum" >&2
  exit 2
fi
if [[ -z "$result_path" ]]; then
  root=$(git -C "$working_directory" rev-parse --show-toplevel 2>/dev/null || printf '%s' "$working_directory")
  runtime="$root/.agent-runtime"
  mkdir -p "$runtime"
  result_path="$runtime/run-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM.json"
fi
mkdir -p "$(dirname "$result_path")"
result_path=$(cd "$(dirname "$result_path")" && printf '%s/%s' "$PWD" "$(basename "$result_path")")
stem=${result_path%.json}
stdout_path="$stem.stdout.log"
stderr_path="$stem.stderr.log"

json_escape() {
  local value=$1
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  value=${value//$'\r'/\\r}
  printf '%s' "$value"
}

descendants() {
  local parent=$1 child
  if command -v pgrep >/dev/null 2>&1; then
    while read -r child; do
      [[ -n "$child" ]] || continue
      printf '%s\n' "$child"
      descendants "$child"
    done < <(pgrep -P "$parent" 2>/dev/null || true)
  fi
}

observed_alive() {
  local pid
  for pid in $observed; do
    kill -0 "$pid" 2>/dev/null && return 0
  done
  return 1
}

signal_observed() {
  local signal=$1 pid
  for pid in $(printf '%s\n' "$observed" | awk '{for(i=NF;i>=1;i--) print $i}'); do
    kill -"$signal" "$pid" 2>/dev/null || true
  done
}

started_epoch=$(date +%s)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
used_setsid=false
if command -v setsid >/dev/null 2>&1; then
  (cd "$working_directory" && exec setsid bash -c "$command_text") >"$stdout_path" 2>"$stderr_path" &
  root_pid=$!
  used_setsid=true
else
  (cd "$working_directory" && exec bash -c "$command_text") >"$stdout_path" 2>"$stderr_path" &
  root_pid=$!
fi

observed="$root_pid"
printf 'RUNNING pid=%s elapsed_seconds=0 remaining_seconds=%s\n' "$root_pid" "$expected"
status=FAILED
exit_code=1
cleanup_verified=true
execution_deadline=$((started_epoch + expected))
cleanup_deadline=$((started_epoch + maximum))
next_heartbeat=$((started_epoch + heartbeat))

while kill -0 "$root_pid" 2>/dev/null; do
  now=$(date +%s)
  ((now < execution_deadline)) || break
  if ((now >= next_heartbeat)); then
    while read -r child; do
      [[ " $observed " == *" $child "* ]] || observed+=" $child"
    done < <(descendants "$root_pid")
    printf 'RUNNING pid=%s elapsed_seconds=%s remaining_seconds=%s stdout_bytes=%s stderr_bytes=%s\n' \
      "$root_pid" "$((now - started_epoch))" "$((execution_deadline > now ? execution_deadline - now : 0))" \
      "$(wc -c <"$stdout_path")" "$(wc -c <"$stderr_path")"
    next_heartbeat=$((now + heartbeat))
  fi
  sleep 0.2
done

if kill -0 "$root_pid" 2>/dev/null; then
  while read -r child; do
    [[ " $observed " == *" $child "* ]] || observed+=" $child"
  done < <(descendants "$root_pid")
  if [[ "$used_setsid" == true ]]; then
    kill -TERM -- "-$root_pid" 2>/dev/null || true
  else
    signal_observed TERM
  fi
  if [[ "$used_setsid" == true ]]; then
    while kill -0 -- "-$root_pid" 2>/dev/null && (( $(date +%s) < cleanup_deadline )); do sleep 0.2; done
    kill -0 -- "-$root_pid" 2>/dev/null && kill -KILL -- "-$root_pid" 2>/dev/null || true
  else
    # macOS does not ship setsid. Track the captured descendant PIDs until all
    # have exited, even if the root shell exits first; otherwise a child that
    # ignores TERM can survive the timeout unnoticed.
    while observed_alive && (( $(date +%s) < cleanup_deadline )); do sleep 0.2; done
    observed_alive && signal_observed KILL
  fi
  wait "$root_pid" 2>/dev/null || true
  if [[ "$used_setsid" == true ]]; then
    kill -0 -- "-$root_pid" 2>/dev/null && cleanup_verified=false
  else
    observed_alive && cleanup_verified=false
  fi
  status=TIMED_OUT
  exit_code=124
else
  wait "$root_pid"
  exit_code=$?
  if ((exit_code == 0)); then status=PASSED; else status=FAILED; fi
fi

finished_epoch=$(date +%s)
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
{
  printf '{\n'
  printf '  "schema": "portable-bounded-run-v1",\n'
  printf '  "status": "%s",\n' "$status"
  printf '  "command": "%s",\n' "$(json_escape "$command_text")"
  printf '  "working_directory": "%s",\n' "$(json_escape "$working_directory")"
  printf '  "expected_upper_bound_seconds": %s,\n' "$expected"
  printf '  "cleanup_allowance_seconds": %s,\n' "$cleanup"
  printf '  "maximum_lifetime_seconds": %s,\n' "$maximum"
  printf '  "heartbeat_interval_seconds": %s,\n' "$heartbeat"
  printf '  "timeout_basis": "%s",\n' "$(json_escape "$timeout_basis")"
  printf '  "started_at": "%s",\n' "$started_at"
  printf '  "finished_at": "%s",\n' "$finished_at"
  printf '  "elapsed_seconds": %s,\n' "$((finished_epoch - started_epoch))"
  printf '  "root_process_id": %s,\n' "$root_pid"
  printf '  "cleanup_verified": %s,\n' "$cleanup_verified"
  printf '  "exit_code": %s,\n' "$exit_code"
  printf '  "stdout_path": "%s",\n' "$(json_escape "$stdout_path")"
  printf '  "stderr_path": "%s"\n' "$(json_escape "$stderr_path")"
  printf '}\n'
} >"$result_path"

printf '%s exit_code=%s elapsed_seconds=%s result=%s cleanup_verified=%s\n' \
  "$status" "$exit_code" "$((finished_epoch - started_epoch))" "$result_path" "$cleanup_verified"
exit "$exit_code"
