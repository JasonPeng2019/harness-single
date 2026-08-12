[CmdletBinding()]
param(
    [switch]$Run,
    [Alias('Root')]
    [string]$RepositoryRoot,
    [string]$ExpectedBranch = 'firmware/v2-candidate',
    [string]$ExpectedTip,
    [string[]]$ChangedPath = @(),
    [string[]]$ChangedDomain = @(),
    [string]$CreditFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Import-Module (Join-Path $PSScriptRoot 'CandidateSafeguard.Core.psm1') -Force

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

function Git-CommonDirectory {
    $value = Invoke-Git @('rev-parse', '--git-common-dir')
    if ([IO.Path]::IsPathRooted($value)) {
        return Normalize-Path $value
    }
    return Normalize-Path (Join-Path $script:repositoryRoot $value)
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
$commonDirectory = Git-CommonDirectory
$baselineHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $baseline).Hash
$configHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $pyrightConfig).Hash

$selectorArguments = @(
    '-m', 'orchestrator_harness.release_checks', 'select',
    '--intent', 'release', '--root', $script:repositoryRoot,
    '--expected-branch', $branch, '--expected-tip', $head,
    '--exclude-id', 'S6.RELEASE.ACCUMULATED-SAFEGUARD'
)
if (-not [string]::IsNullOrWhiteSpace($CreditFile)) {
    $selectorArguments += @('--credit-file', (Normalize-Path $CreditFile))
}
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
if (
    $selectedSource -ine $script:repositoryRoot -or
    (Normalize-Path ([string]$selection.source.git_common_dir)) -ine $commonDirectory -or
    [string]$selection.source.branch -cne $branch -or
    [string]$selection.source.tip -cne $head
) {
    throw 'release-check selection is not bound to the exact requested root, branch, and tip'
}

$checks = @($selection.selected)
$checkIds = @($checks | ForEach-Object { [string]$_.stable_id })
if (@($checkIds | Select-Object -Unique).Count -ne @($checkIds).Count) {
    throw 'release-check selector returned duplicate stable IDs'
}
if ($checkIds -contains 'S6.RELEASE.ACCUMULATED-SAFEGUARD') {
    throw 'recursive accumulated safeguard was selected'
}
if (-not $Run) {
    foreach ($check in $checks) {
        "READY $($check.stable_id): $($check.command -join ' ')"
    }
    exit 0
}

Push-Location $script:repositoryRoot
try {
    Invoke-ReleaseChecks -Checks $checks -RepositoryRoot $script:repositoryRoot `
        -ExpectedHead $head -ExpectedBranch $branch -ExpectedCommonDirectory $commonDirectory `
        -Baseline $baseline -PyrightConfig $pyrightConfig -BaselineHash $baselineHash -ConfigHash $configHash
} finally {
    Pop-Location
}
