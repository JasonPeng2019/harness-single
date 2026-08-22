# `coding.claude.invocation.example.json` â€” annotated

A canonical (`orchestrator-worker-invocation/v1`) invocation for one **claude-code**
lane on an Ollama Anthropic-compatible backend. JSON carries no comments, so
this file documents every non-obvious field. The JSON is structurally valid as
written; the `*_sha256`/`size` values and absolute paths are placeholders that a
caller materializes against a real worktree. The caller can then prove the file
loads end-to-end through `lane_controller.load_invocation`.

## Top-level fields

| Field | Meaning |
| --- | --- |
| `schema` | **`orchestrator-worker-invocation/v1`** â€” the canonical provider-neutral schema. A claude-code lane uses this schema and its provider block identifies `provider.id="claude-code"`. |
| `action` | `"start"` (or `"resume"` with a `resume.session_id`). |
| `run_root` | The Git **worktree** the provider edits. Must already exist and be a directory; the provider process starts with this as its working directory. |
| `runtime_root` | A runtime directory **separate from** `run_root` (the controller rejects a runtime_root nested inside run_root or vice-versa). Holds the lane event log. |
| `lane_id` | Logical lane name (also the Git branch name in this example). |
| `worker_invocation_id` | Unique worker identity for this invocation. |
| `cohort_id` | Stage cohort identity, used for resume/admission identity. |
| `workflow` | `{id, version}`. `prompt_bundle.workflow_id` must equal `workflow.id`. |
| `task_card` | `{id, revision, sha256}`. `sha256` must be a 64-hex SHA-256 of the task-card record (content-bound identity); `prompt_bundle.task_card_id` must equal `task_card.id`. |
| `role` | Role string; must equal `profile.role`. |
| `repository` | Optional Git declaration. `worktree_root` must equal `run_root`, `common_dir` the real `.git` common dir, `base_commit` a full 40-hex commit ID. |

## `provider` block (claude-code specifics)

| Field | Meaning |
| --- | --- |
| `id` | **`"claude-code"`** â€” registered provider. |
| `model` | A model the endpoint actually serves. With the Ollama redirect below, this is an Ollama model tag (here `deepseek-v4-flash:0731-cloud`, a real model on the local Ollama). Passed to the CLI as `--model`. |
| `command` | The real Claude Code CLI. One string or a list; `["claude"]` resolves via `PATH`, or give an absolute path (`C:/Users/Jason/.local/bin/claude.exe`). |
| `allowed_tools` | Optional explicit tool allow-list (`--allowedTools`). With `bypassPermissions` below it is redundant but harmless; it documents intent. |
| `config_overrides` | **The redirect channel.** For claude-code, `config_overrides` have no `-c` config file to write to; instead each entry is translated by `claude_config_override_env` into **child-process environment variables**. The exact alias `"model_provider=\"ollama\""` expands to `ANTHROPIC_BASE_URL=http://localhost:11434`, `ANTHROPIC_AUTH_TOKEN=ollama`, `ANTHROPIC_API_KEY=""`. The lane controller merges these **after** any child-environment isolation, so they always reach the provider child. Any other entry must be an explicit `ANTHROPIC_*` env assignment, or the invocation fails loudly. |
| `permission_mode` | **Omitted deliberately.** An omitted `permission_mode` defaults to `bypassPermissions` (the Claude adapter's documented default), so tools are not interactively blocked. Set it explicitly to override. |
| `service_tier` / `approval_policy` | **Must be absent.** Claude Code has no `--service-tier` or `--approval-policy` flags, so these fields raise `InvocationValidationError` for `provider.id="claude-code"` instead of being silently dropped. |

## `profile`

Must cross-match the invocation: `profile.provider == provider.id`,
`profile.model == provider.model`, `profile.role == role`,
`profile.resources == resources` (as a set/order-insensitive tuple). `tools`
should mirror the provider's `allowed_tools`.

## `prompt_bundle`

The closed content-bound prompt record. `components` are path-bound files under
`run_root`; each `sha256` is the digest of that file's bytes and `size` its
byte count. `final_sha256`/`final_size` describe the concatenated prompt bytes;
`bundle_sha256` digests the whole manifest record. `profile_id` must equal
`profile.id`. The reference implementation to build this record is
`orchestrator_harness.prompt_bundle.prompt_bundle_record_from_paths`.

## `output_paths` / `event_log_path`

`status`, `jsonl`, `stderr`, `last_message` must all be **direct children of
`run_root/.agent-workspace`**; `status` must be a `*.json` file. The provider's
stream-json transcript is written to `jsonl` and parsed for
`PROVIDER_STARTED`/`PROVIDER_EXITED` lifecycle events. `event_log_path` must be
under `runtime_root`.

