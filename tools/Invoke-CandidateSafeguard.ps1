[CmdletBinding()]
param(
    [switch]$Run,
    [Alias('Root')]
    [string]$RepositoryRoot,
    [string]$ExpectedBranch = 'firmware/v2-candidate',
    [string]$ExpectedTip,
    [string[]]$ChangedPath = @(),
    [string[]]$ChangedDomain = @()
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Normalize-Path([string]$Value) {
    return [IO.Path]::GetFullPath($Value).TrimEnd('\', '/')
}

if ([string]::IsNullOrWhiteSpace($RepositoryRoot)) {
    $RepositoryRoot = Join-Path $PSScriptRoot '..'
}
$script:repositoryRoot = Normalize-Path((Resolve-Path -LiteralPath $RepositoryRoot).Path)

function Invoke-Git([string[]]$Arguments) {
    $output = & git -C $script:repositoryRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "repository Git check failed: git $($Arguments -join ' '): $output"
    }
    return ($output | Out-String).Trim()
}

$topLevel = Normalize-Path (Invoke-Git @('rev-parse', '--show-toplevel'))
if ($topLevel -ine $script:repositoryRoot) {
    throw "refusing ambiguous repository root: Git top level is $topLevel"
}

$branch = Invoke-Git @('branch', '--show-current')
if ([string]::IsNullOrWhiteSpace($ExpectedBranch) -or $branch -cne $ExpectedBranch) {
    throw "refusing repository with an unexpected branch: expected $ExpectedBranch, observed $branch"
}

$head = (Invoke-Git @('rev-parse', 'HEAD')).ToLowerInvariant()
if ($head -notmatch '^[0-9a-f]{40}$') {
    throw "refusing repository without a full HEAD identity: $head"
}
if (-not [string]::IsNullOrWhiteSpace($ExpectedTip) -and $head -cne $ExpectedTip.ToLowerInvariant()) {
    throw "refusing repository with an unexpected tip: expected $ExpectedTip, observed $head"
}
if (Invoke-Git @('status', '--porcelain')) {
    throw 'refusing dirty repository root'
}
if ($head -eq '4699d27bd5bf7c0b41bbed9ddb6b0b7d019e215f') {
    throw 'refusing the stable general-harness runner revision'
}

$baseline = Join-Path $script:repositoryRoot '.codex/dev/basedpyright-baseline.json'
$pyrightConfig = Join-Path $script:repositoryRoot 'pyrightconfig.json'
if (-not (Test-Path -LiteralPath $baseline -PathType Leaf) -or -not (Test-Path -LiteralPath $pyrightConfig -PathType Leaf)) {
    throw 'candidate BasedPyright baseline or configuration is missing'
}
if (-not ((Get-Content -Raw -LiteralPath $pyrightConfig) -match '"baselineFile"\s*:\s*"\.codex/dev/basedpyright-baseline\.json"')) {
    throw 'candidate pyright configuration is not bound to the retained baseline'
}

$selectorArguments = @(
    '-m', 'orchestrator_harness.release_checks', 'select',
    '--intent', 'release', '--root', $script:repositoryRoot,
    '--expected-branch', $branch, '--expected-tip', $head,
    '--exclude-id', 'S6.RELEASE.ACCUMULATED-SAFEGUARD'
)
foreach ($path in $ChangedPath) {
    $selectorArguments += @('--changed-path', $path)
}
foreach ($domain in $ChangedDomain) {
    $selectorArguments += @('--changed-domain', $domain)
}
$selectionOutput = & python @selectorArguments 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "release-check selector failed: $selectionOutput"
}
try {
    $selection = ($selectionOutput -join [Environment]::NewLine) | ConvertFrom-Json
} catch {
    throw "release-check selector returned malformed JSON: $($_.Exception.Message)"
}
if ($selection.schema -ne 'orchestrator-check-selection/v1') {
    throw "release-check selector returned an unexpected schema: $($selection.schema)"
}
$selectedSource = Normalize-Path ([string]$selection.source.source_root)
if ($selectedSource -ine $script:repositoryRoot -or [string]$selection.source.branch -cne $branch -or [string]$selection.source.tip -cne $head) {
    throw 'release-check selection is not bound to the exact requested root, branch, and tip'
}

$checks = @($selection.selected)
if (-not $Run) {
    foreach ($check in $checks) {
        "READY $($check.stable_id): $($check.command -join ' ')"
    }
    exit 0
}

$baselineHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $baseline).Hash
Push-Location $script:repositoryRoot
try {
    foreach ($check in $checks) {
        $command = @($check.command)
        if ($command.Count -lt 1) {
            throw "selected check has no command: $($check.stable_id)"
        }
        $program = [string]$command[0]
        $arguments = if ($command.Count -gt 1) { @($command[1..($command.Count - 1)]) } else { @() }
        if ($program -eq 'python') {
            & python @arguments
        } elseif ($program -eq 'powershell') {
            & powershell @arguments
        } else {
            throw "selected check uses an unsupported runner: $program"
        }
        if ($LASTEXITCODE -ne 0) {
            throw "candidate safeguard failed: $($check.stable_id)"
        }
    }
} finally {
    Pop-Location
}
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $baseline).Hash -ne $baselineHash) {
    throw 'candidate BasedPyright baseline changed during safeguard'
}
if ((Invoke-Git @('rev-parse', 'HEAD')).ToLowerInvariant() -ne $head -or (Invoke-Git @('branch', '--show-current')) -cne $branch) {
    throw 'candidate root identity changed during safeguard'
}
