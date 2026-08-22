# Optional bounded script-routing policy

The normal portable workflow requires the bounded launcher only for command
fragments explicitly named in `bounded-commands.txt`. Open-ended CLI agent
workers and every subagent session are never bounded. That is the right default
for most repositories.

Enable this policy only when the repository has repeatedly suffered from scripts
that hang, orphan child processes, or run without visible output. It adds a
mechanical PreToolUse guard for direct Python-script, PowerShell `-File`, and
POSIX-shell script launches, plus Python `-m`/`-c` and POSIX-shell `-c` forms.
It does not classify tests, build tools, or product meaning.

This optional broader script policy never applies to a recognized agent/session
launcher. Keep ordinary finite command selection in `bounded-commands.txt`; do
not add `codex exec`, `claude -p`, or an agent-launch wrapper there.

## Enable deliberately

1. Ensure Python 3 is available to the hook host.
2. Copy `bounded-script-policy.json.example` to
   `.agent/bounded-script-policy.json` and set `enabled` to `true`.
3. Review `bounded-launchers.json` for the actual interpreter names in use.
4. Add only narrow, truthful Git-ignore patterns to
   `bounded-exclusions.gitignore` for scripts that must launch directly.
5. Commit the enabled configuration and exclusions. The configuration is
   intentionally not ignored: if this guard matters, every agent working in the
   repository should see the same policy.

When enabled, a covered direct script command is denied unless its resolved
script path is excluded. Run the command through `run-bounded.ps1` or
`run-bounded.sh` with an evidence-based expected runtime, cleanup allowance, and
timeout basis. The policy uses `git check-ignore --no-index` from the current
repository root; it never scans the entire repository.

The launchers themselves are already excluded. If a command has unusual quoting,
environment setup, or exit normalization, create a small task-local wrapper in
`.agent-runtime/` and run that wrapper through the same launcher. The wrapper may
adapt invocation only; it must not reimplement heartbeats, timeout, cleanup, or
terminal evidence.

An enabled but malformed policy, unavailable Python interpreter, missing adapter,
or non-Git working tree denies the covered launch with an actionable explanation.
The policy is otherwise absent and inert by default.
