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
    [string]$AtlasDatabase = 'memory-dev',
    [string]$AtlasIndex = 'vector_benchmark_memory_v1',
    [string]$AtlasTextKey = 'search_text',
    [string]$AtlasEmbeddingKey = 'procedure_embedding',
    [string]$EverosApplication = 'memory-harness-benchmark',
    [string]$EverosProject,
    [string]$EverosNamespace,
    [string]$EverosOwner = 'root-benchmark',
    [string]$Model = 'gpt-5.6-terra',
    [ValidateSet('low', 'medium', 'high', 'xhigh', 'max', 'ultra')] [string]$ReasoningEffort = 'high',
    [ValidateSet('auto', 'default', 'flex', 'priority')] [string]$ServiceTier = 'priority',
    [double]$WatchdogHours = 1.5
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
# Required worker delegation

Delegate the task's main work to substantive workers through the
native harness; do not complete the task alone. For an implementation task,
at least one worker must do substantive implementation. A validation-only lane
does not satisfy this requirement. ROOT owns
planning, integration, review, and final checks. Give each worker a
specific task card. The harness automatically searches shared memory using
that task and gives the recalled experiences only to that worker. Do not read
or request recalled worker content for ROOT planning. Do not disable worker
memory or bypass native worker lanes.

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
    --sandbox danger-full-access `
    --harness-runtime $runtime `
    --harness-dir $harness `
    --watchdog-hours $WatchdogHours `
    --trust-project-hooks
exit $LASTEXITCODE
