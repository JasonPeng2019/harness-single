[CmdletBinding()]
param(
    [switch]$Run,
    [Alias('Root')]
    [string]$RepositoryRoot,
    [Parameter(Mandatory = $true)]
    [string]$ExpectedBranch,
    [string]$ExpectedTip,
    [string[]]$ChangedPath = @(),
    [string[]]$ChangedDomain = @(),
    [string]$CreditFile
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
$candidateTools = Normalize-Path (Join-Path $script:repositoryRoot 'tools')
if ((Normalize-Path $PSScriptRoot) -ine $candidateTools) {
    throw 'safeguard script must be executed from the candidate RepositoryRoot tools directory'
}
$corePath = Join-Path $candidateTools 'CandidateSafeguard.Core.psm1'
if (-not (Test-Path -LiteralPath $corePath -PathType Leaf)) {
    throw 'candidate safeguard core is missing beside the candidate safeguard script'
}
Import-Module $corePath -Force

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
$priorPythonPath = $env:PYTHONPATH
$pythonPathSuffix = ''
if (-not [string]::IsNullOrWhiteSpace($priorPythonPath)) {
    $pythonPathSuffix = [IO.Path]::PathSeparator + $priorPythonPath
}
try {
    $env:PYTHONPATH = $script:repositoryRoot + $pythonPathSuffix
    Push-Location $script:repositoryRoot
    $selectorModuleOutput = & python -c "import pathlib, orchestrator_harness.release_checks as module; print(pathlib.Path(module.__file__).resolve())" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "cannot import candidate release selector: $selectorModuleOutput"
    }
    $selectorModule = Normalize-Path (($selectorModuleOutput | Out-String).Trim())
    $expectedSelectorModule = Normalize-Path (Join-Path $script:repositoryRoot 'orchestrator_harness/release_checks.py')
    if ($selectorModule -ine $expectedSelectorModule) {
        throw "release selector was imported from a foreign module: $selectorModule"
    }
    $selectionOutput = & python @selectorArguments 2>&1
} finally {
    Pop-Location
    if ([string]::IsNullOrEmpty($priorPythonPath)) {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    } else {
        $env:PYTHONPATH = $priorPythonPath
    }
}
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
$selectionModule = Normalize-Path ([string]$selection.selector_module)
if ($selectionModule -ine $expectedSelectorModule) {
    throw 'release-check selection is not produced by the candidate selector module'
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

# Reconcile every selected stable ID and command against the candidate's
# immutable registry.  This prevents a foreign or caller-shaped selection
# record from becoming executable merely because its JSON is well formed.
$priorPythonPath = $env:PYTHONPATH
$pythonPathSuffix = ''
if (-not [string]::IsNullOrWhiteSpace($priorPythonPath)) {
    $pythonPathSuffix = [IO.Path]::PathSeparator + $priorPythonPath
}
try {
    $env:PYTHONPATH = $script:repositoryRoot + $pythonPathSuffix
    Push-Location $script:repositoryRoot
    $registryOutput = & python -m orchestrator_harness.release_checks registry 2>&1
} finally {
    Pop-Location
    if ([string]::IsNullOrEmpty($priorPythonPath)) {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    } else {
        $env:PYTHONPATH = $priorPythonPath
    }
}
if ($LASTEXITCODE -ne 0) {
    throw "candidate release registry failed: $registryOutput"
}
try {
    $registry = ($registryOutput -join [Environment]::NewLine) | ConvertFrom-Json
} catch {
    throw "candidate release registry returned malformed JSON: $($_.Exception.Message)"
}
if ($registry.schema -ne 'orchestrator-release-check-registry/v1' -or
    [string]$registry.registry_fingerprint -cne [string]$selection.registry_fingerprint) {
    throw 'release selection registry fingerprint is not the candidate registry'
}
$registryById = @{}
foreach ($record in @($registry.checks)) {
    $id = [string]$record.stable_id
    if ([string]::IsNullOrWhiteSpace($id) -or $registryById.ContainsKey($id)) {
        throw 'candidate release registry has an invalid or duplicate stable ID'
    }
    $registryById[$id] = $record
}
foreach ($check in $checks) {
    $id = [string]$check.stable_id
    if (-not $registryById.ContainsKey($id)) {
        throw "selected check is not in the candidate registry: $id"
    }
    $candidateCommand = ConvertTo-Json -InputObject @($check.command) -Compress
    $registryCommand = ConvertTo-Json -InputObject @($registryById[$id].command) -Compress
    if ($candidateCommand -cne $registryCommand) {
        throw "selected command does not match the candidate registry: $id"
    }
}
if (-not $Run) {
    foreach ($check in $checks) {
        "READY $($check.stable_id): $($check.command -join ' ')"
    }
    exit 0
}

$priorPythonPath = $env:PYTHONPATH
$pythonPathSuffix = ''
if (-not [string]::IsNullOrWhiteSpace($priorPythonPath)) {
    $pythonPathSuffix = [IO.Path]::PathSeparator + $priorPythonPath
}
try {
    $env:PYTHONPATH = $script:repositoryRoot + $pythonPathSuffix
    Push-Location $script:repositoryRoot
    Invoke-ReleaseChecks -Checks $checks -RepositoryRoot $script:repositoryRoot `
        -ExpectedHead $head -ExpectedBranch $branch -ExpectedCommonDirectory $commonDirectory `
        -Baseline $baseline -PyrightConfig $pyrightConfig -BaselineHash $baselineHash -ConfigHash $configHash
} finally {
    Pop-Location
    if ([string]::IsNullOrEmpty($priorPythonPath)) {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    } else {
        $env:PYTHONPATH = $priorPythonPath
    }
}
