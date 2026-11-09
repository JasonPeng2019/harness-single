# Provider adapter catalog

This directory is the shipped adapter catalog.  Each provider has one tree
under `adapters/<provider-id>/`:

- `README.md` — what to put below (per-tree).
- `root/` — installed into the ROOT workspace as the provider's ROOT payload
  (`<provider dotdir>/` with config, hooks, binding, and the eight ROOT
  skills).
- `super-cache/` — installed into `<rt>/super-cache/adapter-payloads/<provider-id>/`
  and copied into every managed worktree for that provider (worker hooks,
  config, binding, and the two worker skills).
- `harness/launcher_binding.py` — registered under
  `orchestrator_harness/provider_adapters/<provider-id>/` by setup.
- `shipped-machinery/` — documentation for the adapter author.

## Binding contract

Each `harness/launcher_binding.py` must define the strict symbols:

- `PROVIDER_ID` — the provider id, matching the catalog directory name.
- `ADAPTER_VERSION` — a version string for the binding.
- `validate_launch_config(*, model, launch_config)` — reject missing, unknown, or invalid provider
  preferences and return their canonical string mapping before any lane mutation.
- `build_argv(*, model, launch_config, worktree, prompt_path, session_id=None, resume=False)`
  — assemble the provider's headless launch argument vector.
- `parse_line(line)` — read one provider output line into a dict with
  optional `message`, `session_id`, and `non_retryable_failure` keys, or `None`.

Each binding implements native continuation in its own `build_argv`: ordinary resume uses the
saved provider session and `resume=True`; a missing session is an honest error, never a
fresh-context fallback. The exact model and canonical launch configuration are recorded at
bootstrap and reused on continuation. A stopped, unaccepted lane may explicitly replace its model
and complete adapter-required options through `resume-lane`. A changed effective profile starts a
fresh provider session under the same adapter and worktree; it does not transfer conversation
history. Codex requires `reasoning_effort` and `service_tier`, Claude Code requires `effort`, and
Qwen Code has a valid empty option set. Bindings may hard-code transport and safety flags required
by the harness contract, but never a model, reasoning effort, service tier, fallback model, or
other model preference. Provider startup/auth/process failures are marked non-retryable.

For a fresh Windows Codex bootstrap, omitted `permission_mode` selects and records the write-
restricted `worker-workspace` profile. On resume, omitted `permission_mode` preserves the lane's
effective mode, including strict legacy mode; changing it requires the explicit provider option. Explicit `worker-isolated` retains the strict broad
read-denial profile; explicit `yolo` disables OS sandbox enforcement. Non-Windows defaults stay as
they were. Historical invocation records without a mode retain the strict interpretation. ROOT's
sandbox mode is an independent setting. `worker-workspace` has no read-confidentiality guarantee
and its mode-specific Windows receipt does not qualify strict mode.

The controller loads the single registered binding and checks that its
`PROVIDER_ID` matches before use.

## Skill contract

Each ROOT payload ships exactly eight thin-wrapper skills
(`manager-notification-watch`, `acknowledge-manager-notification`,
`close-manager-notification`, `review-lane-completion`,
`send-lane-notification`, `resume-lane`, `force-stop-lane`,
`harness-shutdown`), each wrapping one existing public command.  Each worker
payload ships exactly two worker skills (`manager-notify`, `lane-assignment`)
that call only the provider-neutral helpers
`.agent-workspace/manager-notify.py` and `.agent-workspace/lane-queue.py`.

## Installation

`harness setup` installs ROOT payloads into the root workspace, stages the
active super-cache (workspace base plus per-provider payloads), and registers
the launcher bindings.  `lane bootstrap` materializes the workspace base and
the selected provider payload into each new worktree.

ROOT installation is additive when `.codex/` or `.claude/` already exists.
Harness-owned skills, scripts, and bindings remain strictly owned files.
Provider hook JSON is shared configuration: setup preserves existing fields and
hook groups and appends missing harness hook groups. Existing Codex TOML is
preserved and must already enable hooks. `--overwrite` does not replace shared
provider configuration.
