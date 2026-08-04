[CmdletBinding()]
param(
    [switch]$Run
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Invoke-Git([string[]]$Arguments) {
    $output = & git -C $script:candidateRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "candidate Git check failed: git $($Arguments -join ' '): $output"
    }
    return ($output | Out-String).Trim()
}

$script:candidateRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$expectedCandidateRoot = [IO.Path]::GetFullPath('C:/Users/Jason/Documents/Jason/Orchestrator_Harness/plans/general-coding-harness/runtime/firmware-v2/worktrees/harness-candidate').TrimEnd('\\')
if ($script:candidateRoot.TrimEnd('\\') -ine $expectedCandidateRoot) {
    throw "refusing non-reserved candidate root: $script:candidateRoot"
}

$topLevel = Invoke-Git @('rev-parse', '--show-toplevel')
if ($topLevel.Replace('/', '\').TrimEnd('\') -ine $script:candidateRoot.TrimEnd('\')) {
    throw "refusing ambiguous candidate root: Git top level is $topLevel"
}
if ((Invoke-Git @('branch', '--show-current')) -ne 'firmware/v2-candidate') {
    throw 'refusing candidate root with an unexpected branch'
}
if (Invoke-Git @('status', '--porcelain')) {
    throw 'refusing dirty candidate root'
}
if ((Invoke-Git @('rev-parse', 'HEAD')) -eq '4699d27bd5bf7c0b41bbed9ddb6b0b7d019e215f') {
    throw 'refusing the stable general-harness runner revision'
}

$baseline = Join-Path $script:candidateRoot '.codex/dev/basedpyright-baseline.json'
$pyrightConfig = Join-Path $script:candidateRoot 'pyrightconfig.json'
if (-not (Test-Path -LiteralPath $baseline -PathType Leaf) -or -not (Test-Path -LiteralPath $pyrightConfig -PathType Leaf)) {
    throw 'candidate BasedPyright baseline or configuration is missing'
}
if (-not ((Get-Content -Raw -LiteralPath $pyrightConfig) -match '"baselineFile"\s*:\s*"\.codex/dev/basedpyright-baseline\.json"')) {
    throw 'candidate pyright configuration is not bound to the retained baseline'
}

$checks = @(
    @{ Name = 'ruff'; Arguments = @('-m', 'ruff', 'check', '.') },
    @{ Name = 'format'; Arguments = @('-m', 'ruff', 'format', '--check', '.') },
    @{ Name = 'basedpyright'; Arguments = @('-m', 'basedpyright', '--project', 'pyrightconfig.json') },
    @{ Name = 'compile'; Arguments = @('-m', 'compileall', '-q', 'orchestrator_harness', 'harness_watcher_implementation', 'firmware_acceptance') },
    @{ Name = 'orchestrator-tests'; Arguments = @('-m', 'unittest', 'discover', '-s', 'orchestrator_harness/tests', '-t', '.', '-v') },
    @{ Name = 'watcher-tests'; Arguments = @('-m', 'unittest', 'discover', '-s', 'harness_watcher_implementation/tests', '-t', '.', '-v') },
    @{ Name = 'attention-retention'; Arguments = @('harness_watcher_implementation/tests/run_attention_practical.py') },
    @{ Name = 'codex-integration'; Arguments = @('orchestrator_harness/tests/real_agent_test.py') },
    @{ Name = 'synthetic-cleanup'; Arguments = @('orchestrator_harness/tests/wsl_cleanup_guard_test.py') }
)

if (-not $Run) {
    $checks | ForEach-Object { "READY $($_.Name): python $($_.Arguments -join ' ')" }
    exit 0
}

$baselineHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $baseline).Hash
foreach ($check in $checks) {
    & python @($check.Arguments)
    if ($LASTEXITCODE -ne 0) {
        throw "candidate safeguard failed: $($check.Name)"
    }
}
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $baseline).Hash -ne $baselineHash) {
    throw 'candidate BasedPyright baseline changed during safeguard'
}
