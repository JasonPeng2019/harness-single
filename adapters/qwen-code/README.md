# Qwen Code adapter catalog

Shipped adapter tree for the `qwen-code` provider.

- `root/.qwen/` — ROOT payload: `settings.json`, `hooks.json`,
  `hooks/post-tool-use.py`, `orchestrator-harness-binding.json`, and the
  eight ROOT skills.
- `super-cache/.qwen/` — managed worker payload: worker settings, hook
  declaration, `hooks/post-tool-use.py`, worker binding, and the two worker
  skills (`manager-notify`, `lane-assignment`).
- `harness/launcher_binding.py` — registered launcher binding
  (`PROVIDER_ID = "qwen-code"`, `ADAPTER_VERSION = "qwen-code-v1"`).
- `shipped-machinery/` — adapter-author notes for the Qwen Code transport.
