# Claude Code adapter catalog

Shipped adapter tree for the `claude-code` provider.

- `root/.claude/` — ROOT payload: `settings.json` (permissions and hook
  declaration), `hooks/post-tool-use.py`,
  `orchestrator-harness-binding.json`, and the eight ROOT skills.
- `super-cache/.claude/` — managed worker payload: worker settings, hook
  declaration, `hooks/orchestrator_harness_post_tool_use.py`,
  `hooks/orchestrator_harness_stop.py`, worker binding, and the two
  worker skills (`manager-notify`, `lane-assignment`).
- `harness/launcher_binding.py` — registered launcher binding
  (`PROVIDER_ID = "claude-code"`, `ADAPTER_VERSION = "claude-code-v1"`).
- `shipped-machinery/` — adapter-author notes for the Claude Code transport.
