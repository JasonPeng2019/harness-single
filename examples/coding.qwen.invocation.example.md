# `coding.qwen.invocation.example.json` — annotated

This is a canonical `orchestrator-worker-invocation/v1` example for the
runner-owned native `qwen-code` provider.  It is a structural example only:
the placeholder paths and hashes must be materialized by the caller, and no
provider session is launched by this file.

The provider adapter owns the native command shape: `qwen`,
`--approval-mode=yolo`, `--model`, and `--output-format stream-json`; resume
adds `--resume SESSION_ID`.  Qwen-specific optional fields that have no native
equivalent are rejected by invocation validation rather than dropped.

The optional project adapter is installed only in a caller-owned project with:

```powershell
python -m orchestrator_harness adapter install --host qwen --project-root <project>
python -m orchestrator_harness adapter bind --host qwen --project-root <project> --queue-root <registered-manager-queue>
```

It writes `.qwen/settings.json` and `.qwen/hooks/` in that project.  The
settings use Qwen Code’s documented top-level `hooks` object, command-hook
`name` field, and `Notification` matcher `idle_prompt`; unrelated project
settings are preserved.  The runner’s delivery receipt is transport evidence
only and does not acknowledge manager queue work. The route was live-proven
on Windows with Qwen Code 0.21.10; callers continue to own their selected
Qwen model and provider configuration.
