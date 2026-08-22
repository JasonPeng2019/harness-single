#!/usr/bin/env bash
set -u

event=${1:-}
if [[ "$event" != "session-start" && "$event" != "pre-tool-use" && "$event" != "pre-compact" ]]; then
  printf '%s\n' '{"systemMessage":"Portable workflow hook received an unknown event and took no action."}'
  exit 0
fi

if ! command -v jq >/dev/null 2>&1; then
  printf '%s\n' '{"systemMessage":"Portable workflow hooks require jq on this Unix-like host; this hook took no action."}'
  exit 0
fi

payload=$(cat)
if ! printf '%s' "$payload" | jq -e . >/dev/null 2>&1; then
  printf '%s\n' '{"systemMessage":"Portable workflow hook received malformed JSON and took no action."}'
  exit 0
fi

cwd=$(printf '%s' "$payload" | jq -r '.cwd // empty')
root=$(git -C "${cwd:-$PWD}" rev-parse --show-toplevel 2>/dev/null || true)
if [[ -z "$root" ]]; then
  root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fi

if [[ "$event" == "session-start" ]]; then
  context='Portable workflow active: use repository instructions and relevant skills before acting.\nRoute only finite non-agent commands explicitly selected in .agent/bounded-commands.txt through .agent/run-bounded.sh.'
  if [[ -f "$root/.agent/verify.toml" ]]; then
    context+='\nA local verification override exists at .agent/verify.toml.'
  fi
  if [[ -f "$root/HANDOFF.md" ]]; then
    preview=$(head -n 30 "$root/HANDOFF.md" | head -c 3000)
    context+=$'\nHANDOFF.md exists. Read it before continuing.\n'
    context+="$preview"
  fi
  jq -cn --arg value "$context" '{hookSpecificOutput:{hookEventName:"SessionStart",additionalContext:$value}}'
  exit 0
fi

if [[ "$event" == "pre-compact" ]]; then
  if [[ -f "$root/HANDOFF.md" ]] || [[ -n "$(git -C "$root" status --porcelain 2>/dev/null)" ]]; then
    jq -cn '{continue:true,systemMessage:"Work may be in progress. If continuity matters, use the checkpoint skill to refresh HANDOFF.md before compaction."}'
  fi
  exit 0
fi

command_text=$(printf '%s' "$payload" | jq -r '.tool_input.command // empty')
[[ -z "$command_text" ]] && exit 0

agent_session=false
if grep -Eqi 'codex(\.exe)?[[:space:]]+exec|claude(\.exe)?[[:space:]]+(-p|--print)|\.agent[\\/](launch-agent\.(sh|ps1)|multi_agent\.py[[:space:]]+(launch|supervise))' <<<"$command_text"; then
  agent_session=true
fi

policy_config="$root/.agent/bounded-script-policy.json"
if [[ "$agent_session" == false && -f "$policy_config" ]]; then
  policy_enabled=$(jq -r 'if (.enabled | type) == "boolean" then .enabled else "invalid" end' "$policy_config" 2>/dev/null || printf 'invalid')
  if [[ "$policy_enabled" == invalid ]]; then
    jq -cn '{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"deny",permissionDecisionReason:"Optional bounded script policy configuration error: enabled must be a Boolean. Fix it or disable the policy."}}'
    exit 0
  fi
  if [[ "$policy_enabled" == true ]]; then
    adapter="$root/.agent/bounded-script-adapter.py"
    if [[ ! -f "$adapter" ]]; then
      jq -cn '{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"deny",permissionDecisionReason:"Optional bounded script policy is enabled but .agent/bounded-script-adapter.py is missing. Restore it or disable the policy."}}'
      exit 0
    fi
    python_command=''
    for candidate in python3 python; do
      # command -v only confirms a file exists on PATH: Windows ships a
      # python3 App Execution Alias stub by default that resolves this way
      # but exits nonzero telling the user to install from the Microsoft
      # Store. Actually invoke the candidate to confirm it runs.
      if command -v "$candidate" >/dev/null 2>&1; then
        version=$("$candidate" --version 2>&1 || true)
        if [[ "$version" =~ ^Python\ 3\.([0-9]+) ]] && ((10#${BASH_REMATCH[1]} >= 10)); then
          python_command=$candidate
          break
        fi
      fi
    done
    if [[ -z "$python_command" ]]; then
      jq -cn '{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"deny",permissionDecisionReason:"Optional bounded script policy is enabled but Python 3.10 or later is unavailable to the hook. Install Python, restore the adapter, or disable the policy."}}'
      exit 0
    fi
    policy_output=$(printf '%s' "$payload" | "$python_command" "$adapter" pre-tool-use 2>&1)
    policy_status=$?
    if ((policy_status != 0)); then
      jq -cn --arg value "Optional bounded script policy could not run: $policy_output. Fix it or disable the policy." '{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"deny",permissionDecisionReason:$value}}'
      exit 0
    fi
    if printf '%s' "$policy_output" | jq -e '.hookSpecificOutput.permissionDecision == "deny"' >/dev/null 2>&1; then
      printf '%s\n' "$policy_output"
      exit 0
    fi
  fi
fi

uses_wrapper=false
if grep -Eqi '\.agent/(run-bounded\.sh|run-bounded\.ps1)' <<<"$command_text"; then
  uses_wrapper=true
fi

reason=''
repo_pattern=$(printf '%s' "$root" | sed 's/[][(){}.^$+*?|\\/]/\\&/g')
home_pattern=$(printf '%s' "$HOME" | sed 's/[][(){}.^$+*?|\\/]/\\&/g')
if grep -Eqi "^[[:space:]]*(rm|rmdir)[[:space:]].*(-r|-rf|-fr|--recursive).*((['\"]?/[\*]?['\"]?)([[:space:]]|$)|(['\"]?~/?\*?['\"]?)([[:space:]]|$)|['\"]?${repo_pattern}/?\*?['\"]?([[:space:]]|$)|['\"]?${home_pattern}/?\*?['\"]?([[:space:]]|$))" <<<"$command_text"; then
  reason='Blocked a recursive deletion aimed at a filesystem root, home directory, or repository root. Resolve and select the intended narrower target.'
elif grep -Eqi "^[[:space:]]*(rm|rmdir|mv)[[:space:]].*(^|[[:space:]'\"/])\.git([/[:space:]'\"]|$)|(^|[^>])>>?[[:space:]]*[^;&|]*\.git(/|$)" <<<"$command_text"; then
  reason='Blocked direct mutation of .git internals. Use Git commands for repository metadata operations.'
elif [[ "$agent_session" == true && "$uses_wrapper" == true ]]; then
  reason='Agent and subagent sessions must remain unbounded. Launch them directly; reserve .agent/run-bounded.sh for finite commands explicitly listed in .agent/bounded-commands.txt.'
elif [[ "$agent_session" == false && "$uses_wrapper" == false && -f "$root/.agent/bounded-commands.txt" ]]; then
  command_lower=$(printf '%s' "$command_text" | tr '[:upper:]' '[:lower:]')
  while IFS= read -r fragment; do
    # A CRLF-saved config file leaves a trailing carriage return that read -r
    # does not strip; without this, every fragment would silently fail to match.
    fragment=${fragment%$'\r'}
    [[ -z "$fragment" || "$fragment" =~ ^[[:space:]]*# ]] && continue
    # Bash 3.2 is still the system shell on supported macOS releases, so use
    # tr instead of Bash 4 lowercase expansion. A plain Bash substring test
    # also avoids a Git-for-Windows grep crash seen with -Fi.
    fragment_lower=$(printf '%s' "$fragment" | tr '[:upper:]' '[:lower:]')
    if [[ "$command_lower" == *"$fragment_lower"* ]]; then
      reason='This configured long-running command must run through .agent/run-bounded.sh with a justified runtime bound.'
      break
    fi
  done < "$root/.agent/bounded-commands.txt"
fi

if [[ -n "$reason" ]]; then
  jq -cn --arg value "$reason" '{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"deny",permissionDecisionReason:$value}}'
fi
