# Task-configurable memory for worker lanes

The completed ZSTD run 05 remains historical evidence. It used operator recall
before ROOT and launched only a validator worker. The current path recalls from
each ROOT-assigned worker task and delivers the result only to that worker.

For any task, prepare a fresh ROOT workspace, run ID, and harness runtime. Set
`harness-config.json` to that workspace with managed coordination enabled. The
ROOT prompt and `AGENTS.md` must not direct ROOT to read an operator memory file.
Use a stable task ID and an Atlas collection assigned to that task:

```powershell
.\tools\Invoke-WorkerMemoryRun.ps1 -RunId <fresh-run-id> `
    -Task <task-id> -Workspace <fresh-root-workspace> `
    -PromptFile <fresh-root-prompt> -AtlasCollection <collection-name>
```

The launcher reads Atlas and embedding credentials from `.secrets/creds/`, uses
the `memory-dev` database by default, and sets the task's EverOS project and
namespace from the task ID. You can override `-AtlasDatabase`, `-AtlasIndex`,
`-AtlasTextKey`, `-AtlasEmbeddingKey`, and the `-Everos*` scope options to use
an existing store. Atlas documents use the `task` metadata field for isolation.
The collection's vector index must match its embedding and text fields.

For the existing ZSTD memory, use the compatibility preset (or pass
`-Task zstd-decoder -AtlasCollection benchmark_memories_zstd_decoder_v1` to the
generic launcher):

```powershell
.\tools\Invoke-WorkerMemoryZstdRun.ps1 -RunId <fresh-run-id> `
    -Workspace <fresh-root-workspace> -PromptFile <fresh-root-prompt>
```

The old ZSTD result can be restored into `memory-dev` with
`.\tools\Restore-SharedZstdMemory.ps1 -AtlasDatabase memory-dev -SourceManifest <reviewed-result.json>`. That script is
only for the reviewed historical ZSTD result; ordinary worker recall does not
depend on it. A new task with no stored history receives an explicit “no similar
experience” message. A backend failure is recorded separately from an empty
search; bootstrap stops if both backends are unavailable.

The launcher requires substantive worker delegation (including an implementing
worker for implementation tasks) and favors two early workers when ROOT can
assign independent, substantial work with disjoint source custody. If one
worker is enough, ROOT records why another would add coordination without
useful parallel work. More workers need distinct work that justifies them.
Each worker receives memory selected for its task card. ROOT handles planning,
integration, review, and final checks. The important log records recall
queries, results, selection, and final worker prompts. Credentials are removed
from worker and controller environments.
