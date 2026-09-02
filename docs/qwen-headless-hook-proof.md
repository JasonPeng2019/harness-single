# Qwen Code headless PostToolUse proof

This proof establishes one narrow claim: Qwen Code invokes the project-local
PostToolUse hook during a native harness-managed headless session, and that
hook can deliver a content-free notice to the exact manager queue to which the
project was explicitly bound.

## Proven run

On Windows, Qwen Code 0.21.10 was launched through the native v2 operator and
lane route in a fresh Git lane. The provider command was:

```text
qwen --approval-mode=yolo --model deepseek-v4-flash:0731-cloud --output-format stream-json
```

Qwen used the machine's existing Qwen configuration for its local
Ollama-compatible provider; the harness passed no credential or provider
configuration. The one-line task asked Qwen to run `git status --short` once
without editing files. The stream recorded that tool call and ended with
`QWEN_HOOK_PROOF_COMPLETE` and a successful result.

Before launch, the proof created a real managed queue and admitted one pending
`MANAGER_SIGNAL`; the controller loaded the shipped direct Qwen binding.

The manager's durable `DELIVERY.jsonl` then recorded `outcome: "DELIVERED"`.
The coordinator receipt identifies `boundary: "post_tool_use"`, reports one
delivery attempt, and preserves the pending event. Delivery is therefore
transport evidence only; it is not an acknowledgement or completion of queue
work.

## Limits

- The proof covers project-local Qwen hooks and the PostToolUse boundary. It
  does not claim that Notification or Stop always run in every Qwen mode.
- The user owns the selected Qwen provider, model, and credentials. The
  harness only records the non-secret model name and uses Qwen's normal
  configuration lookup.
- A source-checkout proof granted `PYTHONPATH` so the installed hook could
  import the uninstalled harness package. Normal installed use does not need
  that temporary grant.
- Windows shared system locations (`SYSTEMDRIVE` and `PROGRAMDATA`) are part
  of the non-secret base child environment so Qwen does not create a literal
  `%SystemDrive%` directory in the lane when it initializes native caches.
