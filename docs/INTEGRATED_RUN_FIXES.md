# Integrated harness fixes

Both reusable harnesses include launcher safety, ROOT lifecycle enforcement,
worker permissions and a project-configured build/test interface. The updated
harness retains memory support; the generic harness retains its strict task-card
contract and review/resume recovery guards.

## Implemented behavior

- Managed ROOT Stop rejects every OPEN runtime, including an empty queue with a
  running worker or no current epoch. Unreadable managed state fails closed.
  Handle events, settle lanes, shut down and verify CLOSED before exit. Plain
  mode and an already CLOSED runtime retain their existing behavior.
- Each worker launcher invokes its provider once. An invalid RESULT or startup
  failure preserves transcript, stderr, usage and cleanup evidence. It never
  triggers an automatic correction, resume, provider switch or repair.
- A lane/native session permits its initial assignment plus at most one explicit
  manual retry. Its append-only attempt ledger spans run IDs; missing/corrupt
  saved-session history cannot reset the budget. Diagnose failures before a
  separate launch and use a fresh lane/session after the budget is spent.
- ROOT hooks and the native monitor are installed before the ROOT process starts.
  Fresh launches and resumes validate the run/workspace/config/hook-bound setup
  receipt. Installing hooks inside ROOT is too late for that session's discovery.
- Windows Codex launches require a read-only sandbox health gate by default.
  Missing local policy does not disable it. Marker, CLI binary, Codex home, local
  cache and retained probe evidence must match an operator-issued receipt.
  The gate never calls Codex, setup, elevation or fallback. A setup error or
  changed marker records a run-wide block and stops only the exact owned process
  boundary, including failures detected after fast exit.
- Worker isolation never denies an ancestor of ROOT/the lane. Linked workers
  receive writes for their own Git metadata and shared object/branch stores;
  ROOT's index, config and hooks remain ungranted. Harness/operator state and
  protected sibling workspaces remain denied.
- The optional deterministic build/test service preserves committed snapshots,
  exact request/revision identities, stdout/stderr/exit status, direct worker
  feedback, bounded checks, resume rebinding and shutdown cleanup. Its commands,
  input selection and backend come from operator configuration. No task adapter,
  dataset, fixed container image or evaluation runner ships in the harness.

## Operator preparation

Configure a fresh harness copy and ROOT workspace. For Windows Codex, choose a
process-local LOCALAPPDATA cache and keep it unchanged for setup, ROOT,
controllers and workers. Obtain a real isolated-command/protected-path probe for
the exact CLI/home/cache before issuing `local-config/windows-sandbox-health.json`.
`examples/windows-sandbox-health.example.json` is an UNVERIFIED schema template;
a copied PASS receipt is not a new probe.

Preserve sandbox errors and exact ownership evidence, diagnose and repair before
a separate launch. At most one separately authorized setup request is allowed
per run; verified launches need zero. Never run speculative `codex sandbox ...
--help`, retry setup, switch implementations or broaden permissions to bypass
the gate. Machine ACL repair is an operator responsibility.

Before starting Codex ROOT:

```powershell
python -B scripts/check_windows_sandbox.py --harness-dir . --workspace <absolute-root>
python -B scripts/prepare_root_launch.py --harness-dir . --workspace <absolute-root> --run-id <id>
```

Preparation invokes native setup once, validates hooks and writes
`<root>/.agent-workspace/ROOT_PRELAUNCH_SETUP.json`. Repeating preparation for an
identical prepared run only revalidates that receipt; it never repeats setup.
Existing runtime without the matching receipt is refused. Use the same run ID
and paths with `scripts/collect_codex_usage.py`.

Configure the optional [build/test interface](BUILD_TEST_INTERFACE.md) before
preparation. No service, backend, sandbox or provider startup is automatically
retried. ROOT resumes revalidate the original hooks, config, sandbox health,
run-wide block and service. Quiescent OPEN review needs the live service; CLOSED
resume requires the original service STOPPED with cleanup proved. Resume does
not restart it or reopen lanes.

## Remaining work

CLOSED proves operational cleanup, not task completion. Task IDs/overviews,
registered task-card refresh and harness-owned final validation remain separate
unimplemented features. [ROOT task policy](ROOT_TASK_POLICY.md) supplies fresh
validator, initial test evidence and incremental integration/repair guidance;
include it in the actual ROOT assignment. It is not a completion gate.
Cancellation terminal-state/token accounting remains a separate gap. Source
changes alone do not prove a new live sandbox or agent run is healthy.
