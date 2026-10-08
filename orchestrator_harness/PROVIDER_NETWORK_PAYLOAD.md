# Provider network payload (STEP-13-2)

Product ROOT selects an optional `network_profile` in the content-hashed
`memory_handoff.configuration` before bootstrap. The accepted values are
`normal`, `soft_guardrail_network`, `atlas_memory_only`, and
`restricted_local`; absence means `normal`. Bootstrap and resume pass that
request to lane 1's `PreparationService.prepare(network_mode=...)`. The
per-objective preparation row captures requested and effective modes, sources,
limits, and context. The controller reads and validates that durable row against
the accepted dispatch before every first, resumed, or correction spawn. Task
text and provider launch options are not profile authority. Legacy and all-off
cards retain their ordinary path.

`provider_network_payload.install_soft_controls(provider_id, worktree)` composes
Qwen's `.qwen/settings.json` after a managed or plain worktree is installed.
It preserves other settings and refuses malformed settings. Codex and Claude
use launch arguments and need no settings mutation.

For a captured effective mode other than `normal`, the controller applies
`resolve_launch` immediately before `spawn_provider`. A missing, malformed,
redirected, or changed Qwen setting, an unsupported Qwen CLI version, or an
unsupported native control reports `uncontrolled_network`; the controller
refuses that spawn. The controller has no free-standing requested-profile
argument or invocation field.

The durable controller status `network_payload` and matching events report
`captured_requested_mode`, `captured_effective_mode`, captured sources/limits
and context, and the payload's `requested_profile`, `effective_profile`,
enforcement sources, reason, and uncovered surfaces. `inspected_argv` with
`payload_state=inspected_not_started` is a pre-spawn candidate; only
`payload_state=spawned` includes `spawned_argv` after process creation and
boundary inspection. `native_forbidden_call_count=null` means no native call
count was measured. `independent_egress_proven_for_launch=false` prevents this
status from being used as Atlas-only egress proof. STEP-15 must present the
captured and launched facts together, and use the payload's downgraded claim
where it is weaker than the capture-time resolution.

For the installed Codex 0.156.1, Claude Code 2.1.278, and Qwen Code 0.21.10:

| Provider | Verified native control |
| --- | --- |
| Codex | Top-level `web_search="disabled"` via `-c` in the actual exec argv. |
| Claude Code | `--disallowedTools WebSearch WebFetch` in the actual argv. |
| Qwen Code | Worktree `tools.webSearch.enabled=false`, `tools.disabled` and `permissions.deny` for `web_search` and `web_fetch`, read again at spawn; 0.21.10 `--exclude-tools web_search,web_fetch` in the actual argv provides the effective whole-tool deny. |

Qwen may ignore project settings in an untrusted folder, and
`ENABLE_WEB_SEARCH=true` overrides `tools.webSearch.enabled`. Its 0.21.10 CLI
merges `--exclude-tools` into the deny rules after loading settings, and its
permission manager removes whole-tool denies from registration. The launch
claim therefore cites the argv control; raw worktree settings are still
required and checked as installed payload, but are not treated alone as
effective enforcement. The installer checks reparse points and physical
worktree containment before any settings read or write.

The controls suppress those provider-native tools. Shell commands, other
provider tools, and user-configured MCP/plugin/extension tools can still reach
the network. No hardened sandbox or Atlas-only isolation is claimed. An
`atlas_memory_only` request downgrades to soft when those native controls are
verified because independent unrelated-destination blocking is unmeasured.
A `restricted_local` request also reports only soft from this payload seam;
the controls add no optional Atlas task-path access, but the service-boundary
forbidden-call proof is pending. STEP-17/18 native and independent-egress
proof remains pending.

Provider references: [Codex configuration](https://developers.openai.com/codex/config-reference),
[Claude CLI](https://code.claude.com/docs/en/cli-reference),
[Qwen settings](https://qwenlm.github.io/qwen-code-docs/en/users/configuration/settings/),
[Qwen web search](https://qwenlm.github.io/qwen-code-docs/en/developers/tools/web-search/).
