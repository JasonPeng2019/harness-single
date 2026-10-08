[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$RunId,
    [Parameter(Mandatory)] [string]$Task,
    [Parameter(Mandatory)] [string]$Workspace,
    [Parameter(Mandatory)] [string]$PromptFile,
    [Parameter(Mandatory)] [string]$AtlasCollection,
    [string]$PythonExecutable,
    [string]$CredentialRoot,
    [string]$ResultsDir,
    [Parameter(Mandatory)] [string]$AtlasDatabase,
    [Parameter(Mandatory)] [string]$AtlasIndex,
    [string]$AtlasTextKey = 'search_text',
    [string]$AtlasEmbeddingKey = 'procedure_embedding',
    [string]$EverosApplication = 'coding-harness',
    [string]$EverosProject,
    [string]$EverosNamespace,
    [string]$EverosOwner = 'worker-memory',
    [Parameter(Mandatory)] [string]$Model,
    [Parameter(Mandatory)] [ValidateSet('low', 'medium', 'high', 'xhigh', 'max', 'ultra')] [string]$ReasoningEffort,
    [Parameter(Mandatory)] [ValidateSet('auto', 'default', 'flex', 'priority')] [string]$ServiceTier,
    [ValidateSet('read-only', 'workspace-write', 'danger-full-access')] [string]$Sandbox = 'workspace-write',
    [Parameter(Mandatory)] [double]$WatchdogHours
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($RunId -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') {
    throw "RunId must contain only letters, digits, periods, underscores, and hyphens"
}
if ($Task -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') {
    throw "Task must be a stable identifier containing only letters, digits, periods, underscores, and hyphens"
}
if ($AtlasDatabase -notmatch '^[A-Za-z0-9_][A-Za-z0-9_-]*$') {
    throw "AtlasDatabase contains unsupported characters"
}
foreach ($value in @($AtlasCollection, $AtlasIndex, $AtlasTextKey, $AtlasEmbeddingKey)) {
    if ($value -notmatch '^[A-Za-z0-9_][A-Za-z0-9_.-]*$') {
        throw "Atlas collection, index, and field names must be nonempty simple identifiers"
    }
}
if (-not $EverosProject) { $EverosProject = $Task }
if (-not $EverosNamespace) { $EverosNamespace = "$Task-shared-v1" }
if ($WatchdogHours -le 0) { throw "WatchdogHours must be positive" }
$harness = Split-Path -Parent $PSScriptRoot
$productRoot = $harness
$repoRoot = $productRoot
$workspacePath = (Resolve-Path -LiteralPath $Workspace).Path
$promptPath = (Resolve-Path -LiteralPath $PromptFile).Path
if (-not $PythonExecutable) {
    $localPython = Join-Path $productRoot '.venv\Scripts\python.exe'
    $PythonExecutable = if (Test-Path -LiteralPath $localPython) { $localPython } else { 'python' }
}
if (-not $CredentialRoot) { $CredentialRoot = Join-Path $productRoot '.secrets\creds' }
if (-not $ResultsDir) { $ResultsDir = Join-Path $productRoot 'runtime\results' }
$python = $PythonExecutable
$collector = Join-Path $productRoot 'scripts\collect_codex_usage.py'
$runtime = Join-Path $workspacePath '.harness-runtime'
$harnessConfigPath = Join-Path $harness 'harness-config.json'

if (-not (Get-Command $python -ErrorAction SilentlyContinue) -and -not (Test-Path -LiteralPath $python)) {
    throw "Python executable is missing: $python"
}
foreach ($requiredPath in @($collector, (Join-Path $CredentialRoot 'MongoDB.txt'), (Join-Path $CredentialRoot 'DeepInfra.txt'))) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Required launch input is missing: $requiredPath"
    }
}
if (Test-Path -LiteralPath $runtime) {
    throw "Worker memory requires a fresh workspace runtime"
}
$harnessConfig = Get-Content -LiteralPath $harnessConfigPath -Raw | ConvertFrom-Json
if ([IO.Path]::GetFullPath($harnessConfig.root_workspace) -ine $workspacePath -or
    $harnessConfig.managed_coordination -ne 'enabled') {
    throw "harness-config.json must select the fresh ROOT workspace with managed coordination enabled"
}
$prompt = Get-Content -LiteralPath $promptPath -Raw
if ($prompt -match 'memory-recall\.md|operator.*preflight') {
    throw "The fresh ROOT prompt must not tell ROOT to read operator-prepared memory"
}
$agentsPath = Join-Path $workspacePath 'AGENTS.md'
if ((Test-Path -LiteralPath $agentsPath) -and
    ((Get-Content -LiteralPath $agentsPath -Raw) -match 'memory-recall\.md|operator.*preflight')) {
    throw "The fresh AGENTS.md must not tell ROOT to read operator-prepared memory"
}

$mongoUri = Get-Content -LiteralPath (Join-Path $CredentialRoot 'MongoDB.txt') |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -match '^mongodb(?:\+srv)?://' } |
    Select-Object -First 1
$deepInfraApiKey = Get-Content -LiteralPath (Join-Path $CredentialRoot 'DeepInfra.txt') |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith('#') } |
    Select-Object -First 1
if (-not $mongoUri -or -not $deepInfraApiKey) {
    throw "The Atlas or embedder credential could not be loaded"
}

$memoryRuntime = Join-Path $productRoot 'runtime'

$env:MEMORY_HARNESS_RUN_ID = $RunId
$env:MEMORY_HARNESS_TASK_ID = $Task
$env:MEMORY_HARNESS_ATLAS_URI = $mongoUri
$env:MEMORY_HARNESS_ATLAS_DATABASE = $AtlasDatabase
$env:MEMORY_HARNESS_ATLAS_COLLECTION = $AtlasCollection
$env:MEMORY_HARNESS_ATLAS_INDEX = $AtlasIndex
$env:MEMORY_HARNESS_ATLAS_TEXT_KEY = $AtlasTextKey
$env:MEMORY_HARNESS_ATLAS_EMBEDDING_KEY = $AtlasEmbeddingKey
$env:MEMORY_HARNESS_EVEROS_APPLICATION = $EverosApplication
$env:MEMORY_HARNESS_EVEROS_PROJECT = $EverosProject
$env:MEMORY_HARNESS_EVEROS_NAMESPACE = $EverosNamespace
$env:MEMORY_HARNESS_EVEROS_OWNER = $EverosOwner
$env:DEEPINFRA_API_KEY = $deepInfraApiKey
$env:EVEROS_EMBEDDING__MODEL = 'Qwen/Qwen3-Embedding-4B'
$env:EVEROS_EMBEDDING__API_KEY = $deepInfraApiKey
$env:EVEROS_EMBEDDING__BASE_URL = 'https://api.deepinfra.com/v1/openai'
$env:PYTHONPATH = "$(Join-Path $productRoot 'src');$harness"
$env:MEMORY_HARNESS_EVEROS_BASE_ROOT = Join-Path $memoryRuntime 'memory\everos'

$env:MEMORY_HARNESS_MONITOR_LOG_PATH = Join-Path $runtime 'monitor\MONITOR.log'
$env:MEMORY_HARNESS_DETAIL_LOG_PATH = Join-Path $runtime 'monitor\MONITOR_DETAIL.log'
$env:MEMORY_HARNESS_IMPORTANT_LOG_PATH = Join-Path $runtime 'monitor\MONITOR_IMPORTANT.log'
$env:MEMORY_HARNESS_OPERATOR_LOG_PATH = Join-Path $runtime 'monitor\MONITOR_OPERATOR_MEMORY.log'
$env:MEMORY_HARNESS_WORKER_RECALL = '1'
Remove-Item Env:MEMORY_HARNESS_ALWAYS_CONTEXT_PATH -ErrorAction SilentlyContinue
Remove-Item Env:MEMORY_HARNESS_REQUIRE_ROOT_RECALL -ErrorAction SilentlyContinue
Remove-Item Env:MEMORY_HARNESS_ACTOR -ErrorAction SilentlyContinue

$effectivePrompt = Join-Path $workspacePath '.agent-workspace\ROOT_WORKER_MEMORY_PROMPT.md'
New-Item -ItemType Directory -Path (Split-Path -Parent $effectivePrompt) -Force | Out-Null
@"
# Required native worker delegation

After harness setup, launch a native planner lane named planner-<id> before
ROOT writes a substantive implementation plan or code. Give the planner the
full task and ask it for architecture, dependencies, interfaces, bounded
implementation assignments, and test risks. Ask it to find independent,
substantial slices that separate implementers can own. It does not implement. Verify an
actual provider_started event and a valid planner result, then record an
ACCEPTED planner review before launching implementation.
ROOT may do brief triage and assign the planner, but must not substitute its own
plan for this lane.

Next launch at least one distinct native implementer lane named
implementer-<id>, using the reviewed planner output in its task card. The
implementer must do substantive task code, not merely review or validation.
Use as many implementers as the task justifies: one per substantial,
independent assignment with clear source custody and useful parallel progress.
Three or more are welcome when the work supports them. Use one when splitting
would add more coordination than useful progress; briefly record why. Do not
invent assignments just to increase the worker count. Verify actual
provider_started events and reviewable implementation results. Record an
ACCEPTED review for each implementation lane before validation. ROOT
coordinates and integrates; it must not do the substantive implementation itself.

After integration, launch native reviewer(s) in distinct validator-<id> lanes.
Give each a clear specialty and the exact integrated candidate revision. One
reviewer may inspect code, design, and edge cases; another may write and run
tests and builds. When both are substantial, prefer separate reviewers for
them. Add security, performance, or integration reviewers when their separate
work adds meaningful coverage; do not create extra lanes for duplicate or
trivial reviews. Reviewer(s) run checks they can access and return evidence-
backed PASS/FAIL findings. At least one reviewer runs task-specific build/test
checks where it has access. If a host-only command is inaccessible to them,
ROOT relays the exact command and output for independent assessment. If review
or tests change the candidate, validate the final revision. Missing or failed
validation blocks completion until repaired and revalidated. ROOT records a
formal review for every reviewer result and accepts the task only with their
evidence; ROOT's own impression is not validation.

At least one planner, one implementer, and one reviewer/validator must be
distinct provider launches with valid and ACCEPTED results, in that order.
ROOT cannot count as any of them, and a created lane without a provider process
does not count. Give each worker a specific task card. The harness recalls
shared memory from that task and gives it only to that worker. ROOT must not
read worker recall for its planning. Do
not bypass native worker lanes. This requirement supersedes optional worker
wording in the supplied prompt or AGENTS.md.

$prompt
"@ | Set-Content -LiteralPath $effectivePrompt -Encoding UTF8

& $python $collector run `
    --run-id $RunId `
    --task $Task `
    --arm harness `
    --cwd $workspacePath `
    --prompt-file $effectivePrompt `
    --results-dir $ResultsDir `
    --model $Model `
    --reasoning-effort $ReasoningEffort `
    --service-tier $ServiceTier `
    --sandbox $Sandbox `
    --harness-runtime $runtime `
    --harness-dir $harness `
    --watchdog-hours $WatchdogHours `
    --trust-project-hooks
exit $LASTEXITCODE
