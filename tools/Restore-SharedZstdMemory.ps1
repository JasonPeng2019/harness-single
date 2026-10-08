[CmdletBinding()]
param(
    [string]$AtlasDatabase = 'memory-dev',
    [Parameter(Mandatory)] [string]$SourceManifest,
    [string]$PythonExecutable,
    [string]$CredentialRoot
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if ($AtlasDatabase -notmatch '^[A-Za-z0-9_][A-Za-z0-9_-]*$') {
    throw 'AtlasDatabase contains unsupported characters'
}

$harness = Split-Path -Parent $PSScriptRoot
$productRoot = $harness
$repoRoot = $productRoot
$source = Get-Content -LiteralPath $SourceManifest -Raw | ConvertFrom-Json
if ($source.schema -ne 'shared-benchmark-memory/v1' -or
    $source.task -ne 'zstd-decoder' -or
    -not $source.memory_text) {
    throw 'SourceManifest is not the reviewed ZSTD shared-memory result'
}
$documentId = [string]$source.atlas.document_id
$match = [regex]::Match($documentId, '^benchmark-result-([0-9a-f]{32,64})$')
if (-not $match.Success) {
    throw 'SourceManifest has no valid benchmark-result token'
}

if (-not $CredentialRoot) { $CredentialRoot = Join-Path $repoRoot '.secrets\creds' }
$mongoUri = Get-Content -LiteralPath (Join-Path $CredentialRoot 'MongoDB.txt') |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -match '^mongodb(?:\+srv)?://' } |
    Select-Object -First 1
$deepInfraApiKey = Get-Content -LiteralPath (Join-Path $CredentialRoot 'DeepInfra.txt') |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith('#') } |
    Select-Object -First 1
if (-not $mongoUri -or -not $deepInfraApiKey) {
    throw 'Atlas or embedding credentials are missing'
}

if (-not $PythonExecutable) {
    $localPython = Join-Path $repoRoot '.venv\Scripts\python.exe'
    $PythonExecutable = if (Test-Path -LiteralPath $localPython) { $localPython } else { 'python' }
}
$python = $PythonExecutable
$restoreRoot = Join-Path $productRoot "runtime\atlas-relocation\$AtlasDatabase"
New-Item -ItemType Directory -Path $restoreRoot -Force | Out-Null
$env:PYTHONPATH = "$(Join-Path $productRoot 'src');$harness"
$env:MEMORY_HARNESS_ATLAS_URI = $mongoUri
$env:MEMORY_HARNESS_ATLAS_DATABASE = $AtlasDatabase
$env:MEMORY_HARNESS_EVEROS_BASE_ROOT = Join-Path $productRoot 'runtime\memory\everos'
$env:DEEPINFRA_API_KEY = $deepInfraApiKey
$env:EVEROS_EMBEDDING__MODEL = 'Qwen/Qwen3-Embedding-4B'
$env:EVEROS_EMBEDDING__API_KEY = $deepInfraApiKey
$env:EVEROS_EMBEDDING__BASE_URL = 'https://api.deepinfra.com/v1/openai'
$env:MEMORY_HARNESS_RESULT_RUN_ID = [string]$source.run_id
$env:MEMORY_HARNESS_RESULT_TOKEN = $match.Groups[1].Value
$env:MEMORY_HARNESS_RESULT_TEXT = [string]$source.memory_text
$env:MEMORY_HARNESS_RESULT_MANIFEST = Join-Path $restoreRoot "$documentId.json"
$env:MEMORY_HARNESS_MONITOR_LOG_PATH = Join-Path $restoreRoot 'MONITOR.log'
$env:MEMORY_HARNESS_DETAIL_LOG_PATH = Join-Path $restoreRoot 'MONITOR_DETAIL.log'
$env:MEMORY_HARNESS_IMPORTANT_LOG_PATH = Join-Path $restoreRoot 'MONITOR_IMPORTANT.log'
$env:MEMORY_HARNESS_OPERATOR_LOG_PATH = Join-Path $restoreRoot 'MONITOR_OPERATOR_MEMORY.log'
$env:MEMORY_HARNESS_ACTOR = 'operator'

@'
import os
from pymongo import MongoClient
try:
    with MongoClient(os.environ['MEMORY_HARNESS_ATLAS_URI'], serverSelectionTimeoutMS=10000) as client:
        client.admin.command('ping')
except Exception as exc:
    print(f'Atlas authentication/connection failed: {type(exc).__name__} code={getattr(exc, "code", None)}')
    raise SystemExit(1)
'@ | & $python -
if ($LASTEXITCODE -ne 0) {
    throw 'Atlas connection did not authenticate; no shared memory was written'
}

$outputLog = Join-Path $restoreRoot 'restore-output.log'
& $python (Join-Path $harness 'examples\zstd\store_shared_zstd_result.py') *> $outputLog
if ($LASTEXITCODE -ne 0) {
    throw "Shared-memory restoration failed; see $outputLog"
}
$result = Get-Content -LiteralPath $env:MEMORY_HARNESS_RESULT_MANIFEST -Raw | ConvertFrom-Json
Write-Output "Restored and recalled $($result.atlas.document_id) in $($result.atlas.database).$($result.atlas.collection)"
