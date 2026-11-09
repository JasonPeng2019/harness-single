# Resume profiles

`resume-lane` accepts two profile modes for a stopped, unaccepted lane. Both keep the lane's
worktree, branch, assignment, commits, uncommitted project files, prior run evidence, resource
history, and retry counts. The harness never transfers a provider conversation between sessions.

## Continue the saved session

Omit `--model` and `--provider-option` to resume the saved native provider session with the recorded
model and canonical options:

```powershell
python -m orchestrator_harness.operator_launch resume-lane `
  --lane-id <lane-id> --resume-task-card <fresh-card.json> --rationale <reason>
python -m orchestrator_harness.operator_launch lane launch --lane-id <lane-id>
```

The provider must support native session continuation. A missing saved session is an error; there
is no fresh-context fallback. This is the ordinary retry path and remains subject to the lane's
initial-attempt-plus-one-manual-retry allowance.

## Change the model or launch profile

Pass `--model` and the complete provider option set to request a profile change. Validation uses
the lane's existing provider adapter and happens before mutation. For example, Codex requires both
`reasoning_effort` and `service_tier`, Claude Code requires `effort`, and Qwen Code accepts an empty
option set. Restate every required model option on a changed profile. If Codex `permission_mode` is
omitted, resume preserves the lane's current effective mode, including strict mode for historical
records that never recorded a mode. For a Windows worker, to change permission mode, supply it explicitly (this example selects the
write-restricted `worker-workspace` mode):

```powershell
python -m orchestrator_harness.operator_launch resume-lane `
  --lane-id <lane-id> --resume-task-card <fresh-card.json> --rationale <reason> `
  --model <replacement-model> `
  --provider-option reasoning_effort=<effort> `
  --provider-option service_tier=<tier> `
  --provider-option permission_mode=worker-workspace
python -m orchestrator_harness.operator_launch lane launch --lane-id <lane-id>
```

Here `permission_mode=worker-workspace` explicitly changes the saved mode to the reduced Windows
write-restricted profile. Omit that option when you want to preserve the lane's existing mode.
For Claude Code, use its complete explicit profile, for example `--provider-option effort=<effort>`.
For Qwen Code, supply `--model <replacement-model>` and no provider options. An unchanged effective
profile keeps the existing session even when explicitly restated. A changed effective profile
archives the prior invocation/session/profile in protected history, records the relationship and
rationale, publishes a fresh run, and clears the active session so launch creates a new native
session in the same worktree. The new session does not inherit the old conversation; include needed
context in the fresh task card and preserved files. The `worker-workspace` default applies only to a
fresh Windows bootstrap. A resume does not adopt that default implicitly.

The transition is limited to stopped, unaccepted lanes eligible for native resume. It does not
change ROOT's model, the worker provider adapter, task assignment, worktree, or retry allowance.
The worker permission mode changes only when the replacement options explicitly set a new mode. Live/unknown processes, unresolved cleanup, accepted/retired lanes, malformed profiles,
and incomplete required options continue to refuse. Permission-profile changes are explicit profile
changes and do not silently alter an ordinary resume. Retained failure blocks and health-gate failures
remain in force; changing a model or permission mode cannot bypass or clear them.

