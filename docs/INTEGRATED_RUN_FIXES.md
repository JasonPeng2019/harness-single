# Integrated harness fixes

This portable harness integrates launcher safety, ROOT lifecycle enforcement,
worker permissions and a project-configured build/test interface. It retains the
strict task-card contract, watch acknowledgement behavior and review/resume
recovery guards.

## Implemented behavior

- Managed ROOT Stop rejects every OPEN runtime, including an empty queue with a
  running worker or no current epoch. Unreadable managed state fails closed.
  Handle events, settle lanes, shut down and verify CLOSED before exit. Plain
  mode and an already CLOSED runtime retain their existing behavior.
- Each worker launcher invokes its provider once. An invalid RESULT or startup
  failure preserves transcript, stderr, usage and cleanup evidence. It never
  triggers an automatic correction, resume, provider switch or repair.
- A lane/native session permits its initial assignment plus at most one explicit
  manual retry. Protected operator launch history spans run IDs; worker-local
  attempt logs are diagnostic mirrors. Missing/corrupt trusted saved-session
  history cannot reset the budget. Diagnose failures before a
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
  receive directory writes for their own private Git metadata, shared objects,
  and an exclusive branch/ref/reflog parent. The default branch is
  `lane/<epoch>/<lane>/work`; explicit isolated branches need a dedicated parent
  with no ROOT/peer branches or retained reservation. ROOT retains and checks
  the Git directory, pointer/backlink, branch and origin assignment before use;
  ROOT's index, config and hooks remain ungranted. Harness/operator state and
  protected sibling workspaces remain denied.
- The optional deterministic build/test service preserves committed snapshots,
  exact request/revision identities, stdout/stderr/exit status, direct worker
  feedback, bounded checks, resume rebinding and shutdown cleanup. Its commands,
  input selection and backend come from operator configuration. No task adapter,
  dataset, fixed container image or evaluation runner ships in the harness.
- Controller, worker Stop, monitor, ROOT review/replay and retirement share the
  complete current-assignment RESULT contract. ROOT uses protected authority;
  a worker mirror cannot authorize acceptance. Acceptance requires clean project
  source at the exact assigned tip, with Git object contents verified and index
  flags unable to hide changes. FAIL/BLOCKED preserve dirty-source diagnostics.
  See the package README for explicit bookkeeping exclusions and unproven
  link/submodule/external-filter cases.

## Operator preparation

Configure a fresh harness copy and ROOT workspace. For Windows Codex, choose a
process-local LOCALAPPDATA cache and keep it unchanged for setup, ROOT,
controllers and workers. Obtain a real isolated-command/protected-path probe for
the exact CLI/home/cache before issuing `local-config/windows-sandbox-health.json`.
`examples/windows-sandbox-health.example.json` is an UNVERIFIED schema template;
a copied PASS receipt is not a new probe.

Preserve sandbox errors and exact ownership evidence, diagnose and repair before
a separate launch. The harness makes zero sandbox setup requests and has no setup
retry or fallback loop. The pinned `max_setup_requests_per_run: 1` policy describes
the operator's separately authorized setup budget; it is not a counter over setup
requests made inside the native CLI. Verified launches need zero requests. The gate
and failure latch do not prove that a native CLI cannot attempt internal setup
before emitting a diagnostic. Never run speculative `codex sandbox ...
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
and paths with `scripts/collect_codex_usage.py`. Supply `--results-dir` explicitly
for a fresh run; the collector does not choose an evidence folder from its caller's cwd.

The collector holds an exact native ROOT process boundary, records its members,
and retains it after ROOT exits. Its existing watchdog also covers descendants
that retain stdout. Windows ROOT starts suspended in a kill-on-close Job before
execution; cleanup targets that held boundary rather than a numeric process tree.
The final manifest records cleanup proof and boundary diagnostics. Unproven
cleanup returns an error, prevents saved-session resume, and prevents new-run
accounting from granting launch credit while retaining countable token usage.
Historical manifests remain readable diagnostics; they cannot supply missing
native cleanup proof for resume.

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
