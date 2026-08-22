
<!-- BEGIN ORCHESTRATOR-HARNESS LANE RULES -->
# Harness lane rules

- Work only in this assigned Git worktree and do not switch branches or reuse another live lane.
- Complete the assigned task; do not create a manager, scheduler, retry controller, relay, or
  watcher subagent.
- Treat `PARALLEL_CHECKPOINT.md` as progress only and write a valid `RESULT.json` only when the
  lane is ready for the manager to validate.
- Keep generated state under the lane's `.agent-workspace` or the declared runtime directory.
- Do not acknowledge manager events, claim unrelated resources, merge branches, or promote code.
- Stop cooperatively. Do not kill processes by broad name or command matching.
- Preserve errors as evidence and report blocked external requirements to the manager.
<!-- END ORCHESTRATOR-HARNESS LANE RULES -->
