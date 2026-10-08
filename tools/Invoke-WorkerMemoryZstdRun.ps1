[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$RunId,
    [Parameter(Mandatory)] [string]$Workspace,
    [Parameter(Mandatory)] [string]$PromptFile,
    [string]$AtlasDatabase = 'memory-dev'
)

$ErrorActionPreference = 'Stop'
& (Join-Path $PSScriptRoot 'Invoke-WorkerMemoryRun.ps1') `
    -RunId $RunId `
    -Task 'zstd-decoder' `
    -Workspace $Workspace `
    -PromptFile $PromptFile `
    -AtlasDatabase $AtlasDatabase `
    -AtlasCollection 'benchmark_memories_zstd_decoder_v1'
exit $LASTEXITCODE
