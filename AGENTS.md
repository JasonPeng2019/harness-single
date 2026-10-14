# Portable harness operating rules

Before any live use, read `QUICK_RULES.md` and follow `QUICK_START.md`. Consult `README.md`,
`orchestrator_harness/SPEC.md`, and `docs/HARNESS_WATCHER_GUIDE.md` for the full contracts.

- Use the native harness directly. Do not add a runner, wrapper, scheduler, retry controller,
  collaboration relay, or AI watcher subagent.
- Keep the deterministic watcher diagnostic-only with `evaluator_enabled: false` unless the user
  explicitly authorizes a different topology.
- The harness and watcher never decide, schedule, repair, operate hardware, or replace the root
  orchestrator.
- The orchestrator must discover requests only through the native blocking wait.
- Keep source directories free of runtime output. Put local configuration under `local-config/`
  and all portable runtime output under `runtime/`.
- Keep the entire product portable: do not hardcode absolute machine, user,
  installation, workspace, or project paths in code, scripts, tests, or shipped
  configuration. Relative path constants are allowed only for files internal to
  the harness. Obtain external paths from explicit configuration, arguments,
  environment, operating-system discovery, or owned temporary fixtures. Never
  ship a configuration that silently selects a developer's external workspace.
- For implementation and review, scan the entire product, including unchanged
  code and preserved features. Continue discovery after finding an issue and
  consolidate all findings before repairs. Fix every independently validated
  defect in a coordinated pass, then run broad validation and fresh independent
  whole-product review. Do not limit discovery to one failing test, one hunk,
  one subsystem, or the original merge targets.
- Use a fresh epoch and fresh runtime directories for every live run.
- Preserve exact event identities: `data.signal_id` identifies the worker request; the envelope's
  top-level `event_id` is used for native acknowledgement.
- Validate response identities before atomic publication. Acknowledge only after successful
  handling.
- Stop cooperatively and verify exact PID plus creation identity. Never kill by broad name or
  command matching.
- Treat harness or watcher difficulty as a defect/configuration signal; do not hide it with support
  machinery.
