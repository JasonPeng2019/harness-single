Set-StrictMode -Version Latest

function Invoke-CoreGit([string]$RepositoryRoot, [string[]]$Arguments) {
    $output = & git -C $RepositoryRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "candidate Git identity check failed: git $($Arguments -join ' '): $output"
    }
    return ($output | Out-String).Trim()
}

function Get-CoreCommonDirectory([string]$RepositoryRoot) {
    $value = Invoke-CoreGit $RepositoryRoot @('rev-parse', '--git-common-dir')
    if ([IO.Path]::IsPathRooted($value)) {
        return [IO.Path]::GetFullPath($value).TrimEnd('\', '/')
    }
    return [IO.Path]::GetFullPath((Join-Path $RepositoryRoot $value)).TrimEnd('\', '/')
}

function Assert-CandidateIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$RepositoryRoot,
        [Parameter(Mandatory)][string]$ExpectedHead,
        [Parameter(Mandatory)][string]$ExpectedBranch,
        [Parameter(Mandatory)][string]$ExpectedCommonDirectory,
        [Parameter(Mandatory)][string]$Baseline,
        [Parameter(Mandatory)][string]$PyrightConfig,
        [Parameter(Mandatory)][string]$BaselineHash,
        [Parameter(Mandatory)][string]$ConfigHash
    )

    $head = (Invoke-CoreGit $RepositoryRoot @('rev-parse', 'HEAD')).ToLowerInvariant()
    if ($head -cne $ExpectedHead.ToLowerInvariant()) {
        throw 'candidate root HEAD changed during safeguard'
    }
    if ((Invoke-CoreGit $RepositoryRoot @('branch', '--show-current')) -cne $ExpectedBranch) {
        throw 'candidate root branch changed during safeguard'
    }
    if ((Get-CoreCommonDirectory $RepositoryRoot) -ine $ExpectedCommonDirectory) {
        throw 'candidate Git common directory changed during safeguard'
    }
    if (Invoke-CoreGit $RepositoryRoot @('status', '--porcelain=v1', '--untracked-files=all')) {
        throw 'candidate repository became dirty during safeguard'
    }
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $Baseline).Hash -cne $BaselineHash) {
        throw 'candidate BasedPyright baseline changed during safeguard'
    }
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $PyrightConfig).Hash -cne $ConfigHash) {
        throw 'candidate pyright configuration changed during safeguard'
    }
    if (-not ((Get-Content -Raw -LiteralPath $PyrightConfig) -match '"baselineFile"\s*:\s*"\.codex/dev/basedpyright-baseline\.json"')) {
        throw 'candidate pyright configuration is no longer bound to the retained baseline'
    }
}

function Invoke-ReleaseChecks {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]]$Checks,
        [Parameter(Mandatory)][string]$RepositoryRoot,
        [Parameter(Mandatory)][string]$ExpectedHead,
        [Parameter(Mandatory)][string]$ExpectedBranch,
        [Parameter(Mandatory)][string]$ExpectedCommonDirectory,
        [Parameter(Mandatory)][string]$Baseline,
        [Parameter(Mandatory)][string]$PyrightConfig,
        [Parameter(Mandatory)][string]$BaselineHash,
        [Parameter(Mandatory)][string]$ConfigHash
    )

    $executedIds = [Collections.Generic.List[string]]::new()
    foreach ($check in @($Checks)) {
        $stableId = [string]$check.stable_id
        if ([string]::IsNullOrWhiteSpace($stableId) -or $stableId -eq 'S6.RELEASE.ACCUMULATED-SAFEGUARD') {
            throw 'release safeguard received an invalid or recursive stable ID'
        }
        if ($executedIds.Contains($stableId)) {
            throw "release safeguard received a duplicate stable ID: $stableId"
        }
        $command = @($check.command)
        if ($command.Count -lt 1) {
            throw "selected check has no command: $stableId"
        }
        $program = [string]$command[0]
        $arguments = if ($command.Count -gt 1) { @($command[1..($command.Count - 1)]) } else { @() }
        Push-Location $RepositoryRoot
        try {
            if ($program -eq 'python') {
                & python @arguments
            } elseif ($program -eq 'powershell') {
                & powershell @arguments
            } else {
                throw "selected check uses an unsupported runner: $program"
            }
            $exitCode = $LASTEXITCODE
        } finally {
            Pop-Location
        }
        if ($exitCode -ne 0) {
            throw "candidate safeguard failed: $stableId"
        }
        $executedIds.Add($stableId)
        Assert-CandidateIdentity -RepositoryRoot $RepositoryRoot -ExpectedHead $ExpectedHead `
            -ExpectedBranch $ExpectedBranch -ExpectedCommonDirectory $ExpectedCommonDirectory `
            -Baseline $Baseline -PyrightConfig $PyrightConfig -BaselineHash $BaselineHash -ConfigHash $ConfigHash
    }
    if ($executedIds.Count -ne @($Checks).Count -or @($executedIds | Select-Object -Unique).Count -ne $executedIds.Count) {
        throw 'candidate safeguard did not execute each selected stable ID exactly once'
    }
    Assert-CandidateIdentity -RepositoryRoot $RepositoryRoot -ExpectedHead $ExpectedHead `
        -ExpectedBranch $ExpectedBranch -ExpectedCommonDirectory $ExpectedCommonDirectory `
        -Baseline $Baseline -PyrightConfig $PyrightConfig -BaselineHash $BaselineHash -ConfigHash $ConfigHash
}

Export-ModuleMember -Function Assert-CandidateIdentity, Invoke-ReleaseChecks
