#!/usr/bin/env bash
set -u

[[ "${AGENT_STOP_GATE_ENABLED:-}" == 1 ]] || exit 0

event=${1:-}
if [[ "$event" != "session-start" && "$event" != "stop" ]]; then
  printf '%s\n' '{"systemMessage":"Portable Stop verification received an unknown event and took no action."}'
  exit 0
fi

write_warning() {
  jq -cn --arg message "$1" '{systemMessage:$message}'
}

write_stop_block() {
  jq -cn --arg reason "$1" '{continue:false,stopReason:$reason,systemMessage:$reason}'
}

hash_files() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum -- "$@"; return; fi
  if command -v shasum >/dev/null 2>&1; then shasum -a 256 -- "$@"; return; fi
  return 127
}

flush_hash_batch() {
  local output line hash index count=0 expected=${#batch_paths[@]}
  ((expected > 0)) || return 0
  output=$(hash_files "${batch_paths[@]}") || return $?
  while IFS= read -r line || [[ -n "$line" ]]; do
    ((count < expected)) || return 2
    hash=${line%%[[:space:]]*}
    [[ "$hash" =~ ^[[:xdigit:]]{64}$ ]] || return 3
    index=${batch_indexes[$count]}
    snapshot_hashes[$index]=$hash
    count=$((count + 1))
  done <<<"$output"
  ((count == expected)) || return 4
  batch_paths=()
  batch_indexes=()
}

add_workspace_venv_to_path() {
  local root=$1 candidate resolved
  for candidate in "$root/.venv/bin" "$root/.venv/Scripts"; do
    [[ -d "$candidate" ]] || continue
    resolved=$candidate
    if command -v cygpath >/dev/null 2>&1; then resolved=$(cygpath -u "$candidate") || return 1; fi
    PATH="$resolved:$PATH"
  done
  export PATH
}

remove_superseded_snapshot_states() {
  local path=$1 runtime_directory=${1%/*} candidate filename remove
  for candidate in "$runtime_directory"/*.json; do
    [[ -f "$candidate" ]] || continue
    [[ "$candidate" == "$path" ]] && continue
    filename=${candidate##*/}
    case "$filename" in
      run-*.json|current-*.json|comparison-*.json) continue ;;
    esac
    remove=false
    [[ "$filename" =~ ^[[:xdigit:]]{64}\.json$ ]] && remove=true
    if [[ "$remove" == false ]] && jq -e '
      .schema == "portable-stop-verify-v1" or
      .schema == "portable-stop-verification-snapshot/v1"
    ' "$candidate" >/dev/null 2>&1; then
      remove=true
    fi
    [[ "$remove" == false ]] || rm -- "$candidate" || return 1
  done
}

write_state() {
  local path=$1 contents=$2 temporary="$1.$$.tmp"
  printf '%s\n' "$contents" >"$temporary" || return 1
  remove_superseded_snapshot_states "$path" || {
    rm -f -- "$temporary"
    return 1
  }
  mv -f -- "$temporary" "$path" || {
    rm -f -- "$temporary"
    return 1
  }
}

collect_snapshot() {
  local root=$1 output status line code relative kind index i
  local -a snapshot_paths=() snapshot_kinds=() snapshot_hashes=() batch_paths=() batch_indexes=()
  output=$(git -C "$root" -c core.quotepath=false status --porcelain=v1 --untracked-files=all 2>/dev/null)
  status=$?
  ((status == 0)) || return "$status"
  [[ -n "$output" ]] || { printf '[]\n'; return; }
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ ${#line} -ge 4 ]] || continue
    code=${line:0:2}
    relative=${line:3}
    kind=present
    if [[ "$code" == *R* || "$code" == *C* ]]; then
      kind=renamed
      [[ "$relative" == *" -> "* ]] && relative=${relative##* -> }
    elif [[ "$code" == *D* ]]; then
      kind=deleted
    fi
    case "$relative" in
      /*|../*|..\\*) return 2 ;;
      .agent-runtime/stop-verify/*) continue ;;
    esac
    index=${#snapshot_paths[@]}
    if [[ "$kind" == present ]]; then
      if [[ ! -f "$root/$relative" ]]; then
        kind=deleted
      else
        batch_paths+=("$root/$relative")
        batch_indexes+=("$index")
      fi
    fi
    snapshot_paths+=("$relative")
    snapshot_kinds+=("$kind")
    snapshot_hashes+=("")
    if ((${#batch_paths[@]} >= 64)); then flush_hash_batch || return $?; fi
  done <<<"$output"
  flush_hash_batch || return $?
  for ((i = 0; i < ${#snapshot_paths[@]}; i++)); do
    printf '%s\t%s\t%s\n' "${snapshot_paths[$i]}" "${snapshot_kinds[$i]}" "${snapshot_hashes[$i]}"
  done | jq -Rn '[inputs | split("\t") | {path:.[0],kind:.[1],hash:(if .[2] == "" then null else .[2] end)}]'
}

new_verification_snapshot_state() {
  local root=$1 baseline
  baseline=$(collect_snapshot "$root") || return 1
  printf '%s\n' "$baseline" | jq -c '{schema:"portable-stop-verification-snapshot/v1",baseline:.,last_verified:[]}'
}

if ! command -v jq >/dev/null 2>&1; then
  printf '%s\n' '{"systemMessage":"Portable Stop verification requires jq on this Unix-like host; it took no action."}'
  exit 0
fi

payload=$(cat)
if ! printf '%s' "$payload" | jq -e . >/dev/null 2>&1; then
  write_warning 'Portable Stop verification received malformed JSON and took no action.'
  exit 0
fi

cwd=$(printf '%s' "$payload" | jq -r '.cwd // empty')
root=$(git -C "${cwd:-$PWD}" rev-parse --show-toplevel 2>/dev/null || true)
[[ -n "$root" ]] || exit 0
config_path="$root/.agent/stop-verify.json"
[[ -f "$config_path" ]] || exit 0
if ! jq -e . "$config_path" >/dev/null 2>&1; then
  write_warning 'Portable Stop verification is disabled because .agent/stop-verify.json is not valid JSON.'
  exit 0
fi
enabled=$(jq -r '.enabled // empty' "$config_path")
[[ "$enabled" == false ]] && exit 0
if [[ "$enabled" != true ]]; then
  write_warning 'Portable Stop verification is disabled because .agent/stop-verify.json must set boolean "enabled".'
  exit 0
fi
if ! jq -e '
  (.source_extensions | type == "array" and length > 0 and all(.[]; type == "string" and startswith("."))) and
  (.commands | type == "array" and length > 0 and all(.[]; type == "array" and length > 0 and all(.[]; type == "string") and ([.[] | select(. == "{files}")] | length == 1))) and
  (.full_commands | type == "array" and length > 0 and all(.[]; type == "array" and length > 0 and all(.[]; type == "string") and ([.[] | select(. == "{files}")] | length == 0))) and
  (.expected_upper_bound_seconds | type == "number" and floor == . and . >= 1) and
  (.cleanup_allowance_seconds | type == "number" and floor == . and . >= 1 and . <= 120) and
  (.heartbeat_interval_seconds | type == "number" and floor == . and . >= 1 and . <= 60) and
  (.timeout_basis | type == "string" and length > 0) and
  ([(.commands | length), (.full_commands | length)] | max) * (.expected_upper_bound_seconds + .cleanup_allowance_seconds) <= 240
' "$config_path" >/dev/null; then
  write_warning 'Portable Stop verification is disabled by invalid configuration. Changed-file commands need exactly one standalone "{files}" entry, full commands must not contain it, and either command lifetime total must fit the 300-second hook ceiling.'
  exit 0
fi

add_workspace_venv_to_path "$root"

expected=$(jq -r '.expected_upper_bound_seconds' "$config_path")
cleanup=$(jq -r '.cleanup_allowance_seconds' "$config_path")
heartbeat=$(jq -r '.heartbeat_interval_seconds' "$config_path")
basis=$(jq -r '.timeout_basis' "$config_path")
runtime="$root/.agent-runtime/stop-verify"
mkdir -p "$runtime"
state_path="$runtime/verification-snapshot.json"

run_command_templates() {
  local selector=$1 include_files=$2 template argument command_text result_path log_path index=0
  # Every `read -r` below strips a trailing "\r" from jq's output: some Git
  # for Windows jq builds emit CRLF-terminated lines even for -r/-c output,
  # and read -r only strips the trailing "\n", leaving a stray "\r" appended
  # to each captured value otherwise.
  while IFS= read -r template; do
    command_text=''
    while IFS= read -r argument; do
      if [[ "$argument" == '{files}' ]]; then
        if [[ "$include_files" != true ]]; then
          RUN_DETAILS='A full verification command unexpectedly contained {files}.'
          return 2
        fi
        for file in "${files[@]}"; do printf -v quoted '%q' "$file"; command_text+="$quoted "; done
      else
        printf -v quoted '%q' "$argument"
        command_text+="$quoted "
      fi
    done < <(printf '%s' "$template" | jq -r '.[]' | tr -d '\r')
    result_path="$runtime/run-$(date -u +%Y%m%dT%H%M%SZ)-$$-$index.json"
    log_path="$result_path.hook.log"
    if ! bash "$root/.agent/run-bounded.sh" --command "$command_text" --working-directory "$root" \
      --expected-upper-bound-seconds "$expected" --cleanup-allowance-seconds "$cleanup" \
      --heartbeat-interval-seconds "$heartbeat" --timeout-basis "$basis" --result-path "$result_path" >"$log_path" 2>&1; then
      RUN_COMMAND=$command_text
      RUN_RESULT_PATH=$result_path
      RUN_DETAILS=$(tail -c 3000 "$log_path" 2>/dev/null || true)
      return 1
    fi
    index=$((index + 1))
  done < <(jq -c "$selector" "$config_path" | tr -d '\r')
}

run_full_verification_fallback() {
  local reason=$1 state
  if ! run_command_templates '.full_commands[]' false; then
    write_stop_block "Full verification fallback failed because no valid durable verification snapshot is available: $reason. Fix the reported failure before stopping.\n\nCommand failed: ${RUN_COMMAND:-unknown}\n${RUN_RESULT_PATH:-unknown}\n${RUN_DETAILS:-unknown}"
  else
    state=$(new_verification_snapshot_state "$root") && write_state "$state_path" "$state" || {
      write_stop_block 'Full verification passed, but the durable verification snapshot could not be published.'
      return
    }
    write_warning "No valid durable verification snapshot was available: $reason. Ran the configured full verification fallback and published the new authoritative snapshot."
  fi
}

if [[ "$event" == session-start ]]; then
  state=$(new_verification_snapshot_state "$root") || {
    write_warning 'Portable Stop verification could not create its SessionStart baseline and took no action.'
    exit 0
  }
  write_state "$state_path" "$state" || {
    write_warning 'Portable Stop verification could not publish its authoritative SessionStart baseline and took no action.'
    exit 0
  }
  exit 0
fi

if [[ ! -f "$state_path" ]]; then
  run_full_verification_fallback 'the durable verification snapshot is unavailable'
  exit 0
fi
if ! jq -e '
  def valid_entries:
    type == "array" and all(.[];
      (keys | sort) == ["hash", "kind", "path"] and
      (.path | type == "string" and length > 0) and
      (.kind == "present" or .kind == "renamed" or .kind == "deleted") and
      (if .kind == "present" then (.hash | type == "string" and test("^[0-9a-f]{64}$")) else .hash == null end)
    );
  (keys | sort) == ["baseline", "last_verified", "schema"] and
  .schema == "portable-stop-verification-snapshot/v1" and
  (.baseline | valid_entries) and
  (.last_verified | valid_entries)
' "$state_path" >/dev/null 2>&1; then
  run_full_verification_fallback 'the durable verification snapshot does not have the provider-neutral schema'
  exit 0
fi

current_path="$runtime/current-$$.json"
comparison_path="$runtime/comparison-$$.json"
cleanup_comparison_files() {
  rm -f -- "$current_path" "$comparison_path"
}
trap cleanup_comparison_files EXIT
collect_snapshot "$root" >"$current_path" || {
  write_stop_block 'Changed-file verification could not inspect the Git worktree.'
  exit 0
}
if ! jq -n --slurpfile state "$state_path" --slurpfile current "$current_path" --slurpfile config "$config_path" '
  def fingerprint: (.kind // "") + "|" + (.hash // "");
  def eligible($settings):
    (.path | ascii_downcase) as $lower |
    (($lower | test("(^|/)docs?/")) or ($lower | test("\\.(md|mdx|rst|txt|adoc|asciidoc|jsonl)$"))) as $documentation |
    ($documentation | not) and
      any($settings.source_extensions[]; . as $extension | $lower | endswith($extension | ascii_downcase));
  (reduce $state[0].baseline[] as $entry ({}; .[$entry.path] = $entry)) as $baseline |
  (reduce $state[0].last_verified[] as $entry ({}; .[$entry.path] = $entry)) as $last_verified |
  reduce $current[0][] as $entry ({pending:[],incomplete:[]};
    if ($entry | eligible($config[0]) | not) then .
    elif (($baseline[$entry.path] // null) as $before |
      $before != null and (($entry | fingerprint) == ($before | fingerprint))) then .
    elif $entry.kind != "present" then .incomplete += [$entry.path]
    elif (($last_verified[$entry.path] // null) as $previous |
      $previous != null and (($entry | fingerprint) == ($previous | fingerprint))) then .
    else .pending += [$entry]
    end)
' >"$comparison_path"; then
  write_stop_block 'Changed-file verification could not compare the current worktree with its durable snapshot.'
  exit 0
fi

if jq -e '.incomplete | length > 0' "$comparison_path" >/dev/null; then
  files=$(jq -r '.incomplete[]' "$comparison_path" | paste -sd ', ')
  write_stop_block "Changed-file verification is incomplete for eligible deleted or renamed files: $files. Restore them or run the repository's appropriate targeted check explicitly."
  exit 0
fi
if jq -e '.pending | length == 0' "$comparison_path" >/dev/null; then exit 0; fi

files=()
while IFS= read -r file; do files+=("$file"); done < <(jq -r '.pending[].path' "$comparison_path" | tr -d '\r' | sort -u)
if ! run_command_templates '.commands[]' true; then
  write_stop_block "Changed-file verification failed for: ${files[*]}. Fix the reported failure and let the Stop hook rerun it.\n\nCommand failed: ${RUN_COMMAND:-unknown}\n${RUN_RESULT_PATH:-unknown}\n${RUN_DETAILS:-unknown}"
  exit 0
fi

if ! updated=$(jq --slurpfile comparison "$comparison_path" '
  (.last_verified // []) as $old |
  (reduce $old[] as $entry ({}; .[$entry.path] = $entry)) as $previous |
  (reduce $comparison[0].pending[] as $entry ($previous; .[$entry.path] = $entry)) as $merged |
  .last_verified = [$merged | to_entries[] | .value]
 ' "$state_path"); then
  write_stop_block 'Changed-file verification passed, but the durable verification snapshot could not be updated.'
  exit 0
fi
write_state "$state_path" "$updated" || {
  write_stop_block 'Changed-file verification passed, but the durable verification snapshot could not be published.'
  exit 0
}
