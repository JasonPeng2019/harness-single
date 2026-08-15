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

function Add-HeldRemainder {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]]$Checks,
        [Parameter(Mandatory)][int]$StartIndex,
        [Parameter(Mandatory)][Collections.Generic.List[object]]$Results,
        [Parameter(Mandatory)][string]$Reason
    )

    for ($i = $StartIndex; $i -lt $Checks.Count; $i++) {
        $heldId = [string]$Checks[$i].stable_id
        $Results.Add([ordered]@{ stable_id = $heldId; status = 'SKIP'; reason = $Reason })
    }
}

function Convert-IdentityUntrustedPass {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][Collections.Generic.List[object]]$Results,
        [Parameter(Mandatory)][string]$Reason
    )

    # Repository/identity uncertainty makes every earlier current-run PASS
    # untrustworthy: convert them before the first identity-uncertain
    # checkpoint so no observable checkpoint can contain source-untrusted
    # PASS.  Returns the earliest converted unit so the caller can set the
    # earliest first-unresolved state.
    $earliest = $null
    foreach ($result in $Results) {
        if ($result.status -eq 'PASS') {
            $result.status = 'UNRESOLVED'
            $result.reason = $Reason
            if ($null -eq $earliest) {
                $earliest = [string]$result.stable_id
            }
        }
    }
    return $earliest
}

function Write-CheckpointSnapshot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][Collections.Generic.List[object]]$Results,
        [object]$Selection = $null,
        [Parameter(Mandatory)][string]$RepositoryRoot,
        [string]$CheckpointPath = '',
        [string]$ResultsPath = ''
    )

    if ([string]::IsNullOrWhiteSpace($CheckpointPath) -or $null -eq $Selection -or
        [string]::IsNullOrWhiteSpace($ResultsPath)) {
        return
    }
    # Persist the current truthful unit dispositions through the release-check
    # checkpoint owner.  The checkpoint file is the existing -CreditFile
    # interface and is the only artifact written; a nonexistent path is an
    # empty cold input and becomes the output here.
    $resultsJson = '[' + (($Results | ForEach-Object { $_ | ConvertTo-Json -Compress -Depth 8 }) -join ',') + ']'
    [IO.File]::WriteAllText($ResultsPath, $resultsJson, [Text.UTF8Encoding]::new($false))
    $selectionJson = $Selection | ConvertTo-Json -Depth 12
    $selectionJsonPath = Join-Path (Split-Path -Parent $ResultsPath) ('safeguard-selection-' + [guid]::NewGuid().ToString('N') + '.json')
    try {
        [IO.File]::WriteAllText($selectionJsonPath, $selectionJson, [Text.UTF8Encoding]::new($false))
        Push-Location $RepositoryRoot
        try {
            $mergeOutput = & python -m orchestrator_harness.release_checks checkpoint `
                --input $CheckpointPath --selection $selectionJsonPath `
                --results $ResultsPath --output $CheckpointPath 2>&1
            if ($LASTEXITCODE -ne 0) {
                throw "candidate checkpoint merge failed: $mergeOutput"
            }
        } finally {
            Pop-Location
        }
    } finally {
        if (Test-Path -LiteralPath $selectionJsonPath -PathType Leaf) {
            Remove-Item -LiteralPath $selectionJsonPath -Force
        }
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
        [Parameter(Mandatory)][string]$ConfigHash,
        [object]$Selection = $null,
        [string]$CheckpointPath = '',
        [string]$ResultsPath = ''
    )

    $results = [Collections.Generic.List[object]]::new()
    $firstUnresolved = $null
    $index = 0
    foreach ($check in @($Checks)) {
        $stableId = [string]$check.stable_id
        if ([string]::IsNullOrWhiteSpace($stableId) -or $stableId -eq 'S6.RELEASE.ACCUMULATED-SAFEGUARD') {
            throw 'release safeguard received an invalid or recursive stable ID'
        }
        if (@($results | Where-Object { [string]$_.stable_id -eq $stableId }).Count -gt 0) {
            throw "release safeguard received a duplicate stable ID: $stableId"
        }
        $command = @($check.command)
        if ($command.Count -lt 1) {
            throw "selected check has no command: $stableId"
        }
        $program = [string]$command[0]
        $arguments = [string[]]@()
        if ($command.Count -gt 1) {
            $arguments = [string[]]@($command[1..($command.Count - 1)])
        }
        Push-Location $RepositoryRoot
        try {
            if ($program -eq 'python') {
                # Stream the unit's diagnostics live instead of buffering a
                # long unit's entire output; Write-Host keeps the text visible
                # without leaking into this function's success stream, which
                # the caller captures as the pool summary.
                & python @arguments 2>&1 | ForEach-Object { Write-Host $_ }
            } elseif ($program -eq 'powershell') {
                & powershell @arguments 2>&1 | ForEach-Object { Write-Host $_ }
            } else {
                throw "selected check uses an unsupported runner: $program"
            }
            $exitCode = $LASTEXITCODE
        } finally {
            Pop-Location
        }
        if ($exitCode -ne 0) {
            # An ordinary nonzero check records FAIL and never cancels later
            # independent runnable units.
            $results.Add([ordered]@{
                stable_id = $stableId
                status = 'FAIL'
                reason = "ordinary nonzero exit code $exitCode"
            })
            try {
                Assert-CandidateIdentity -RepositoryRoot $RepositoryRoot -ExpectedHead $ExpectedHead `
                    -ExpectedBranch $ExpectedBranch -ExpectedCommonDirectory $ExpectedCommonDirectory `
                    -Baseline $Baseline -PyrightConfig $PyrightConfig -BaselineHash $BaselineHash -ConfigHash $ConfigHash
            } catch {
                # The failing unit left the repository untrustworthy: the unit
                # is upgraded to UNRESOLVED (a non-null first unresolved state),
                # every earlier current-run PASS is converted before the first
                # identity-uncertain checkpoint, and every later unit is held
                # with a recorded skip reason.
                $last = $results[$results.Count - 1]
                $last.status = 'UNRESOLVED'
                $last.reason = "ordinary nonzero exit code $exitCode; candidate identity check failed after unit: $($_.Exception.Message)"
                $untrustedReason = "candidate identity uncertainty after unit ${stableId} invalidates this run's PASS: $($_.Exception.Message)"
                $earliestUntrusted = Convert-IdentityUntrustedPass -Results $results -Reason $untrustedReason
                if ($null -ne $earliestUntrusted) {
                    $firstUnresolved = $earliestUntrusted
                } elseif ($null -eq $firstUnresolved) {
                    $firstUnresolved = $stableId
                }
                Add-HeldRemainder -Checks @($Checks) -StartIndex ($index + 1) -Results $results `
                    -Reason "candidate repository became untrustworthy after failing unit ${stableId}: $($_.Exception.Message)"
                Write-CheckpointSnapshot -Results $results -Selection $Selection -RepositoryRoot $RepositoryRoot `
                    -CheckpointPath $CheckpointPath -ResultsPath $ResultsPath
                break
            }
            Write-CheckpointSnapshot -Results $results -Selection $Selection -RepositoryRoot $RepositoryRoot `
                -CheckpointPath $CheckpointPath -ResultsPath $ResultsPath
            $index++
            continue
        }
        try {
            Assert-CandidateIdentity -RepositoryRoot $RepositoryRoot -ExpectedHead $ExpectedHead `
                -ExpectedBranch $ExpectedBranch -ExpectedCommonDirectory $ExpectedCommonDirectory `
                -Baseline $Baseline -PyrightConfig $PyrightConfig -BaselineHash $BaselineHash -ConfigHash $ConfigHash
        } catch {
            # Identity, source, registry, or environment uncertainty never
            # becomes PASS: this unit is UNRESOLVED, every earlier current-run
            # PASS is converted before the first identity-uncertain
            # checkpoint, and every later unit is skipped with a recorded
            # reason because it cannot truthfully run.
            $results.Add([ordered]@{
                stable_id = $stableId
                status = 'UNRESOLVED'
                reason = "candidate identity check failed after unit: $($_.Exception.Message)"
            })
            $untrustedReason = "candidate identity uncertainty after unit ${stableId} invalidates this run's PASS: $($_.Exception.Message)"
            $earliestUntrusted = Convert-IdentityUntrustedPass -Results $results -Reason $untrustedReason
            if ($null -ne $earliestUntrusted) {
                $firstUnresolved = $earliestUntrusted
            } elseif ($null -eq $firstUnresolved) {
                $firstUnresolved = $stableId
            }
            Add-HeldRemainder -Checks @($Checks) -StartIndex ($index + 1) -Results $results `
                -Reason 'candidate identity uncertainty holds this unit; it was not run truthfully'
            Write-CheckpointSnapshot -Results $results -Selection $Selection -RepositoryRoot $RepositoryRoot `
                -CheckpointPath $CheckpointPath -ResultsPath $ResultsPath
            break
        }
        $results.Add([ordered]@{ stable_id = $stableId; status = 'PASS' })
        Write-CheckpointSnapshot -Results $results -Selection $Selection -RepositoryRoot $RepositoryRoot `
            -CheckpointPath $CheckpointPath -ResultsPath $ResultsPath
        $index++
    }

    try {
        Assert-CandidateIdentity -RepositoryRoot $RepositoryRoot -ExpectedHead $ExpectedHead `
            -ExpectedBranch $ExpectedBranch -ExpectedCommonDirectory $ExpectedCommonDirectory `
            -Baseline $Baseline -PyrightConfig $PyrightConfig -BaselineHash $BaselineHash -ConfigHash $ConfigHash
    } catch {
        # A final identity failure makes every recorded PASS untrustworthy.
        foreach ($result in $results) {
            if ($result.status -eq 'PASS') {
                $result.status = 'UNRESOLVED'
                $result.reason = 'candidate identity uncertainty at pool completion: ' + $_.Exception.Message
            }
        }
        if ($null -eq $firstUnresolved) {
            $firstUnresolvedResult = $results | Where-Object { $_.status -eq 'UNRESOLVED' } | Select-Object -First 1
            if ($null -ne $firstUnresolvedResult) {
                $firstUnresolved = [string]$firstUnresolvedResult.stable_id
            }
        }
        Write-CheckpointSnapshot -Results $results -Selection $Selection -RepositoryRoot $RepositoryRoot `
            -CheckpointPath $CheckpointPath -ResultsPath $ResultsPath
    }

    if ($results.Count -ne @($Checks).Count) {
        throw 'candidate safeguard did not record each selected stable ID exactly once'
    }
    $recordedIds = @($results | ForEach-Object { [string]$_.stable_id })
    $checkIds = @($Checks | ForEach-Object { [string]$_.stable_id })
    if (@($recordedIds | Select-Object -Unique).Count -ne $recordedIds.Count -or
        (@($recordedIds | Sort-Object) -join ',') -cne (@($checkIds | Sort-Object) -join ',')) {
        throw 'candidate safeguard recorded a stable ID that was not selected or was recorded more than once'
    }

    $passed = @($results | Where-Object { $_.status -eq 'PASS' }).Count
    $failed = @($results | Where-Object { $_.status -eq 'FAIL' }).Count
    $unresolved = @($results | Where-Object { $_.status -eq 'UNRESOLVED' }).Count
    $skipped = @($results | Where-Object { $_.status -eq 'SKIP' }).Count
    return [pscustomobject]@{
        total = $results.Count
        passed = $passed
        failed = $failed
        unresolved = $unresolved
        skipped = $skipped
        first_unresolved_unit = $firstUnresolved
        incomplete = ($failed + $unresolved + $skipped) -gt 0
    }
}

Export-ModuleMember -Function Assert-CandidateIdentity, Invoke-ReleaseChecks
