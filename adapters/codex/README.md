# Codex adapter catalog

Shipped adapter tree for the `codex` provider.

- `root/.codex/` — ROOT payload: `config.toml`, `hooks.json`,
  `hooks/post-tool-use.py`, `orchestrator-harness-binding.json`, and the
  eight ROOT skills.
- `super-cache/.codex/` — managed worker payload: worker config, hook
  declaration, the two worker hook wrappers
  (`hooks/orchestrator_harness_post_tool_use.py`,
  `hooks/orchestrator_harness_stop.py`), worker binding, and the two worker
  skills (`manager-notify`, `lane-assignment`).
- `harness/launcher_binding.py` — registered launcher binding
  (`PROVIDER_ID = "codex"`, `ADAPTER_VERSION = "codex-v1"`).
- `shipped-machinery/` — adapter-author notes for the Codex transport.
