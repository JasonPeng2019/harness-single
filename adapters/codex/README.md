# Codex adapter catalog

Shipped adapter tree for the `codex` provider.

## Ollama-hosted models through Codex

The provider ID describes the CLI/hook protocol: use `--provider codex` with
`--provider-option launcher=ollama` for Ollama-hosted Codex workers. For example:

```powershell
python -m orchestrator_harness.operator_launch lane bootstrap --lane-id example --provider codex --model <ollama-model> --provider-option launcher=ollama --provider-option reasoning_effort=<effort> --provider-option service_tier=<tier> --task-card <card.json>
```

The optional `launcher` accepts only `codex` or `ollama`; omission preserves
direct Codex behavior. Model, effort and tier remain explicit. The Ollama branch
executes `ollama launch codex --model <model> --yes -- exec ...`, omitting the
duplicate Codex model flag that Ollama rejects. It preserves stdin, JSON output,
worker hooks and exact-session resume. Both executables must be installed.
Bootstrap records this option unchanged; invalid values fail before lane creation.
There is no model substitution, provider fallback, scheduler or extra runner.

Ollama manages its own profile/catalog in the user's Codex configuration directory.
Serialize Ollama launches sharing that directory, or supply separately isolated
provider environments; the harness does not manage that directory. Authentication,
capacity or unsupported-model errors remain failures of the selected invocation.
After correcting a non-resumable configuration issue, ROOT starts a fresh lane.
A launch alone is not proof: require an identity-valid result and native cleanup.

## Payload

- `root/.codex/` — ROOT payload: `config.toml`, `hooks.json`,
  `hooks/post-tool-use.py`, `orchestrator-harness-binding.json`, and the
  eight ROOT skills.
- `super-cache/.codex/` — managed worker payload: worker config, hook
  declaration, the two worker hook wrappers
  (`hooks/orchestrator_harness_post_tool_use.py`,
  `hooks/orchestrator_harness_stop.py`), worker binding, and the two worker
  skills (`manager-notify`, `lane-assignment`).
- `harness/launcher_binding.py` — registered launcher binding
  (`PROVIDER_ID = "codex"`, `ADAPTER_VERSION = "codex-v2"`).
- `shipped-machinery/` — adapter-author notes for the Codex transport.
