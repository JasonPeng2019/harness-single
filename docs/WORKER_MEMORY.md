# Optional memory for coding workers

Native `lane bootstrap` can query Atlas and EverOS using the exact task assigned
to a worker and append selected historical experience to that worker's prompt.
This feature is opt-in through `MEMORY_HARNESS_WORKER_RECALL=1`. ROOT chooses the
assignments, worker count, providers, models, and review process.

## Install the optional dependencies

Use Python 3.12 or newer for EverOS. From the harness checkout, with your Python
environment activated, install the native harness and memory integrations:

```powershell
python -m pip install -e ./orchestrator_harness
python -m pip install -e ./vendor/everos
python -m pip install -e ".[everos,atlas]"
```

Ordinary coding lanes do not require these memory integrations. Leave
`MEMORY_HARNESS_WORKER_RECALL` unset to use the ordinary bootstrap path.

## Configure recall in the ROOT environment

Set these variables in the environment that invokes the native harness. Obtain
credentials through your own local configuration; no credential filenames are
required. The embedding model, endpoint, and dimensions must match the vectors
already stored in both backends.

```powershell
$env:MEMORY_HARNESS_WORKER_RECALL = '1'
$env:MEMORY_HARNESS_RUN_ID = '<unique-run-id>'
$env:MEMORY_HARNESS_TASK_ID = '<stable-task-id>'

$env:MEMORY_HARNESS_ATLAS_URI = '<MongoDB connection string>'
$env:MEMORY_HARNESS_ATLAS_DATABASE = '<database>'
$env:MEMORY_HARNESS_ATLAS_COLLECTION = '<collection>'
$env:MEMORY_HARNESS_ATLAS_INDEX = '<vector-index>'
$env:MEMORY_HARNESS_ATLAS_TEXT_KEY = 'search_text'
$env:MEMORY_HARNESS_ATLAS_EMBEDDING_KEY = 'procedure_embedding'

$env:MEMORY_HARNESS_EVEROS_BASE_ROOT = Join-Path (Get-Location).Path 'runtime\memory\everos'
$env:EVEROS_EMBEDDING__MODEL = '<embedding-model>'
$env:EVEROS_EMBEDDING__BASE_URL = '<embedding-endpoint>'
$env:EVEROS_EMBEDDING__API_KEY = '<embedding-api-key>'
```

Run these commands from the harness root. Choose the Atlas text and embedding
fields for your collection. Atlas searches filter on the `task` metadata field
using `MEMORY_HARNESS_TASK_ID`. EverOS scope defaults to application
`coding-harness`, project `<stable-task-id>`, namespace
`<stable-task-id>-shared-v1`, and owner `worker-memory`. Override these with
`MEMORY_HARNESS_EVEROS_APPLICATION`, `MEMORY_HARNESS_EVEROS_PROJECT`,
`MEMORY_HARNESS_EVEROS_NAMESPACE`, and `MEMORY_HARNESS_EVEROS_OWNER` when your
stored experiences use a different scope. Keep the same base root and scope
to recall history across runs.

To include memory queries and selected results in the native monitor logs, set
their paths using the ROOT workspace selected by `harness-config.json`:

```powershell
$config = Get-Content -LiteralPath ./harness-config.json -Raw | ConvertFrom-Json
$runtime = Join-Path $config.root_workspace '.harness-runtime'
$env:MEMORY_HARNESS_MONITOR_LOG_PATH = Join-Path $runtime 'monitor\MONITOR.log'
$env:MEMORY_HARNESS_DETAIL_LOG_PATH = Join-Path $runtime 'monitor\MONITOR_DETAIL.log'
$env:MEMORY_HARNESS_IMPORTANT_LOG_PATH = Join-Path $runtime 'monitor\MONITOR_IMPORTANT.log'
$env:MEMORY_HARNESS_OPERATOR_LOG_PATH = Join-Path $runtime 'monitor\MONITOR_OPERATOR_MEMORY.log'
```

## Use the native coding workflow

Follow [QUICK_START.md](../QUICK_START.md) for configuration, setup, bootstrap,
launch, review, and cleanup. Invoke `orchestrator_harness.operator_launch` in
the configured environment. Recall runs during bootstrap and resume using the
worker task, with a resume rationale included when supplied. The selected
context is delivered in the worker prompt as historical evidence.

An empty store yields an explicit message that no similar experience was
found. A backend failure is logged separately; if one backend is available,
its results can still be used. Bootstrap stops if both backends fail. Native
launch strips memory-service configuration and credentials from the worker
and controller environments after recall.

The Codex usage collector and token ledger in `scripts/` remain available for
usage accounting. Worker recall is configured independently of those tools.
