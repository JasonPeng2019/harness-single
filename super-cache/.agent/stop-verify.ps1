[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('session-start', 'stop')]
    [string]$Event
)

if ($env:AGENT_STOP_GATE_ENABLED -ne '1') { exit 0 }

$ErrorActionPreference = 'Stop'

function Write-HookJson {
    param([Parameter(Mandatory = $true)][hashtable]$Value)

    $Value | ConvertTo-Json -Depth 12 -Compress
}

function Write-WarningMessage {
    param([Parameter(Mandatory = $true)][string]$Message)

    Write-HookJson -Value @{ systemMessage = $Message }
}

function Write-StopBlock {
    param([Parameter(Mandatory = $true)][string]$Reason)

    Write-HookJson -Value @{ continue = $false; stopReason = $Reason; systemMessage = $Reason }
}

function Get-PropertyValue {
    param([AllowNull()][object]$Object, [Parameter(Mandatory = $true)][string]$Name)

    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Get-RepositoryRoot {
    param([AllowNull()][object]$Payload)

    $start = (Get-Location).Path
    $candidate = Get-PropertyValue -Object $Payload -Name 'cwd'
    if ($candidate) { $start = [string]$candidate }
    $root = & git -C $start rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $root) { return $null }
    return [IO.Path]::GetFullPath(([string]$root).Trim())
}

function Get-FileSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)

    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Get-NormalizedRelativePath {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$RelativePath
    )

    if ([IO.Path]::IsPathRooted($RelativePath)) { return $null }
    $root = [IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/')
    $full = [IO.Path]::GetFullPath((Join-Path $root $RelativePath))
    $prefix = $root + [IO.Path]::DirectorySeparatorChar
    if ($full -ne $root -and -not $full.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        return $null
    }
    return $RelativePath.Replace('\', '/')
}

function Get-WorktreeSnapshot {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $result = [Collections.Generic.List[object]]::new()
    $lines = @(& git -C $RepositoryRoot -c core.quotepath=false status --porcelain=v1 --untracked-files=all 2>$null)
    if ($LASTEXITCODE -ne 0) { throw 'Unable to inspect Git worktree status.' }

    foreach ($line in $lines) {
        if (-not $line -or $line.Length -lt 4) { continue }
        $status = $line.Substring(0, 2)
        $relative = $line.Substring(3)
        $kind = 'present'
        if ($status -match '[RC]') {
            $kind = 'renamed'
            if ($relative -match ' -> ') { $relative = $relative.Substring($relative.LastIndexOf(' -> ') + 4) }
        }
        elseif ($status -match 'D') { $kind = 'deleted' }

        $normalized = Get-NormalizedRelativePath -RepositoryRoot $RepositoryRoot -RelativePath $relative
        if (-not $normalized) { throw "Git returned an unsafe relative path: $relative" }
        if ($normalized.StartsWith('.agent-runtime/stop-verify/', [StringComparison]::OrdinalIgnoreCase) -or
            $normalized.StartsWith('.agent-workspace/', [StringComparison]::OrdinalIgnoreCase)) { continue }
        $full = Join-Path $RepositoryRoot $normalized
        $hash = $null
        if ($kind -eq 'present') {
            if (-not (Test-Path -LiteralPath $full -PathType Leaf)) {
                $kind = 'deleted'
            }
            else { $hash = Get-FileSha256 -Path $full }
        }
        $result.Add([pscustomobject]@{ path = $normalized; kind = $kind; hash = $hash })
    }
    return @($result | Sort-Object path)
}

function Get-StopConfig {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $path = Join-Path $RepositoryRoot '.agent/stop-verify.json'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        return [pscustomobject]@{ state = 'absent'; config = $null; error = $null }
    }
    try { $raw = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json }
    catch { return [pscustomobject]@{ state = 'invalid'; config = $null; error = "Could not parse .agent/stop-verify.json: $($_.Exception.Message)" } }

    $enabled = Get-PropertyValue -Object $raw -Name 'enabled'
    if ($enabled -isnot [bool]) {
        return [pscustomobject]@{ state = 'invalid'; config = $null; error = '.agent/stop-verify.json must set boolean "enabled".' }
    }
    if (-not $enabled) { return [pscustomobject]@{ state = 'disabled'; config = $null; error = $null } }

    $extensions = @(Get-PropertyValue -Object $raw -Name 'source_extensions')
    $commands = @(Get-PropertyValue -Object $raw -Name 'commands')
    $fullCommands = @(Get-PropertyValue -Object $raw -Name 'full_commands')
    $expected = Get-PropertyValue -Object $raw -Name 'expected_upper_bound_seconds'
    $cleanup = Get-PropertyValue -Object $raw -Name 'cleanup_allowance_seconds'
    $heartbeat = Get-PropertyValue -Object $raw -Name 'heartbeat_interval_seconds'
    $basis = Get-PropertyValue -Object $raw -Name 'timeout_basis'
    if ($extensions.Count -eq 0 -or @($extensions | Where-Object { $_ -isnot [string] -or -not $_.StartsWith('.') }).Count -gt 0) {
        return [pscustomobject]@{ state = 'invalid'; config = $null; error = 'source_extensions must be a non-empty array of extensions such as ".py".' }
    }
    if ($commands.Count -eq 0 -or $fullCommands.Count -eq 0 -or $basis -isnot [string] -or -not $basis.Trim()) {
        return [pscustomobject]@{ state = 'invalid'; config = $null; error = 'commands, full_commands, and timeout_basis are required when Stop verification is enabled.' }
    }
    foreach ($command in $commands) {
        $arguments = @($command)
        if ($arguments.Count -eq 0 -or @($arguments | Where-Object { $_ -isnot [string] }).Count -gt 0 -or @($arguments | Where-Object { $_ -eq '{files}' }).Count -ne 1) {
            return [pscustomobject]@{ state = 'invalid'; config = $null; error = 'Each Stop command must be an argument array with exactly one standalone "{files}" entry.' }
        }
    }
    foreach ($command in $fullCommands) {
        $arguments = @($command)
        if ($arguments.Count -eq 0 -or @($arguments | Where-Object { $_ -isnot [string] }).Count -gt 0 -or @($arguments | Where-Object { $_ -eq '{files}' }).Count -ne 0) {
            return [pscustomobject]@{ state = 'invalid'; config = $null; error = 'Each full Stop command must be a non-empty argument array and must not contain "{files}".' }
        }
    }
    try {
        $expected = [int]$expected; $cleanup = [int]$cleanup; $heartbeat = [int]$heartbeat
    }
    catch { return [pscustomobject]@{ state = 'invalid'; config = $null; error = 'Stop timeout values must be integers.' } }
    $total = [Math]::Max($commands.Count, $fullCommands.Count) * ($expected + $cleanup)
    if ($expected -lt 1 -or $cleanup -lt 1 -or $cleanup -gt 120 -or $heartbeat -lt 1 -or $heartbeat -gt 60 -or $total -gt 240) {
        return [pscustomobject]@{ state = 'invalid'; config = $null; error = 'Stop command budgets must fit the 300-second hook ceiling: each cleanup allowance is 1..120 seconds, heartbeat is 1..60 seconds, and either the changed-file or full command lifetime total must be at most 240 seconds.' }
    }
    return [pscustomobject]@{
        state = 'enabled'
        error = $null
        config = [pscustomobject]@{
            extensions = @($extensions | ForEach-Object { ([string]$_).ToLowerInvariant() })
            commands = @($commands | ForEach-Object { ,@($_ | ForEach-Object { [string]$_ }) })
            fullCommands = @($fullCommands | ForEach-Object { ,@($_ | ForEach-Object { [string]$_ }) })
            expected = $expected
            cleanup = $cleanup
            heartbeat = $heartbeat
            basis = $basis.Trim()
        }
    }
}

function Test-EligibleSource {
    param([Parameter(Mandatory = $true)][string]$Path, [Parameter(Mandatory = $true)][object]$Config)

    $lower = $Path.Replace('\', '/').ToLowerInvariant()
    if ($lower -match '(^|/)docs?/' -or $lower -match '\.(md|mdx|rst|txt|adoc|asciidoc|jsonl)$') { return $false }
    foreach ($extension in $Config.extensions) {
        if ($lower.EndsWith($extension, [StringComparison]::Ordinal)) { return $true }
    }
    return $false
}

function Add-WorkspaceVenvToPath {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $candidates = [Collections.Generic.List[string]]::new()
    $candidates.Add((Join-Path $RepositoryRoot '.venv/bin'))
    $candidates.Add((Join-Path $RepositoryRoot '.venv/Scripts'))
    foreach ($line in @(& git -C $RepositoryRoot worktree list --porcelain 2>$null)) {
        if (-not $line.StartsWith('worktree ', [StringComparison]::Ordinal)) {
            continue
        }
        $worktree = $line.Substring('worktree '.Length).Trim()
        if (-not $worktree) {
            continue
        }
        $candidates.Add((Join-Path $worktree '.venv/bin'))
        $candidates.Add((Join-Path $worktree '.venv/Scripts'))
    }
    $added = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate -PathType Container) {
            $resolved = [IO.Path]::GetFullPath($candidate)
            if ($added.Add($resolved)) {
                $env:PATH = $resolved + [IO.Path]::PathSeparator + $env:PATH
            }
        }
    }
}

function Get-StopRuntimeDirectory {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $path = Join-Path $RepositoryRoot '.agent-runtime/stop-verify'
    [IO.Directory]::CreateDirectory($path) | Out-Null
    return $path
}

function Get-StatePath {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    return Join-Path (Get-StopRuntimeDirectory -RepositoryRoot $RepositoryRoot) 'verification-snapshot.json'
}

function Publish-StateFile {
    param(
        [Parameter(Mandatory = $true)][string]$TemporaryPath,
        [Parameter(Mandatory = $true)][string]$DestinationPath
    )

    if (-not (Test-Path -LiteralPath $DestinationPath -PathType Leaf)) {
        [IO.File]::Move($TemporaryPath, $DestinationPath)
        return
    }
    $backup = "$DestinationPath.$PID.backup"
    try { [IO.File]::Replace($TemporaryPath, $DestinationPath, $backup) }
    finally {
        if (Test-Path -LiteralPath $backup -PathType Leaf) { [IO.File]::Delete($backup) }
    }
}

function Remove-SupersededSnapshotStates {
    param(
        [Parameter(Mandatory = $true)][string]$RuntimeDirectory,
        [Parameter(Mandatory = $true)][string]$CanonicalPath
    )

    foreach ($candidate in [IO.Directory]::EnumerateFiles($RuntimeDirectory, '*.json')) {
        if ([IO.Path]::GetFullPath($candidate) -eq [IO.Path]::GetFullPath($CanonicalPath)) { continue }
        $filename = [IO.Path]::GetFileName($candidate)
        if ($filename -like 'run-*.json' -or $filename -like 'current-*.json' -or $filename -like 'comparison-*.json') { continue }
        $remove = $filename -match '^[0-9A-Fa-f]{64}\.json$'
        if (-not $remove) {
            try {
                $candidateState = Get-Content -LiteralPath $candidate -Raw | ConvertFrom-Json
                $candidateSchema = Get-PropertyValue -Object $candidateState -Name 'schema'
                $remove = $candidateSchema -in @(
                    'portable-stop-verify-v1',
                    'portable-stop-verification-snapshot/v1'
                )
            }
            catch { $remove = $false }
        }
        if ($remove) { [IO.File]::Delete($candidate) }
    }
}

function Write-State {
    param([Parameter(Mandatory = $true)][object]$State, [Parameter(Mandatory = $true)][string]$Path)

    $temporary = "$Path.$PID.tmp"
    try {
        [IO.File]::WriteAllText($temporary, ($State | ConvertTo-Json -Depth 12), [Text.UTF8Encoding]::new($false))
        Remove-SupersededSnapshotStates -RuntimeDirectory ([IO.Path]::GetDirectoryName($Path)) -CanonicalPath $Path
        Publish-StateFile -TemporaryPath $temporary -DestinationPath $Path
    }
    finally {
        if (Test-Path -LiteralPath $temporary -PathType Leaf) { [IO.File]::Delete($temporary) }
    }
}

function New-VerificationSnapshotState {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    return [pscustomobject]@{
        schema = 'portable-stop-verification-snapshot/v1'
        baseline = @(Get-WorktreeSnapshot -RepositoryRoot $RepositoryRoot)
        last_verified = @()
    }
}

function Get-Fingerprint {
    param([Parameter(Mandatory = $true)][object]$Entry)

    return ('{0}|{1}' -f [string](Get-PropertyValue $Entry 'kind'), [string](Get-PropertyValue $Entry 'hash'))
}

function Get-EntryMap {
    param([AllowNull()][object[]]$Entries)

    $map = @{}
    foreach ($entry in @($Entries)) {
        $path = Get-PropertyValue -Object $entry -Name 'path'
        if ($path) { $map[[string]$path] = $entry }
    }
    return $map
}

function Test-SnapshotEntries {
    param([AllowNull()][object]$Entries)

    if ($null -eq $Entries -or $Entries -isnot [System.Collections.IEnumerable]) { return $false }
    foreach ($entry in @($Entries)) {
        if ($null -eq $entry) { return $false }
        $keys = @($entry.PSObject.Properties.Name | Sort-Object)
        if (@(Compare-Object -ReferenceObject @('hash', 'kind', 'path') -DifferenceObject $keys).Count -ne 0) {
            return $false
        }
        $path = Get-PropertyValue -Object $entry -Name 'path'
        $kind = Get-PropertyValue -Object $entry -Name 'kind'
        $hash = Get-PropertyValue -Object $entry -Name 'hash'
        if ($path -isnot [string] -or -not $path -or $kind -notin @('present', 'renamed', 'deleted')) {
            return $false
        }
        if ($kind -eq 'present') {
            if ($hash -isnot [string] -or $hash -notmatch '^[0-9a-f]{64}$') { return $false }
        }
        elseif ($null -ne $hash) { return $false }
    }
    return $true
}

function ConvertTo-ShellLiteral {
    param([Parameter(Mandatory = $true)][string]$Value)

    return "'" + $Value.Replace("'", "''") + "'"
}

function Invoke-StopCommandTemplates {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][object]$Config,
        [Parameter(Mandatory = $true)][object[]]$CommandTemplates,
        [string[]]$Files = @()
    )

    $files = @($Files | Sort-Object -Unique)
    $runtime = Get-StopRuntimeDirectory -RepositoryRoot $RepositoryRoot
    $runner = Join-Path $RepositoryRoot '.agent/run-bounded.ps1'
    $shell = (Get-Process -Id $PID).Path
    $index = 0
    foreach ($template in $CommandTemplates) {
        $arguments = [Collections.Generic.List[string]]::new()
        foreach ($argument in $template) {
            if ($argument -eq '{files}') { foreach ($file in $files) { $arguments.Add($file) } }
            else { $arguments.Add($argument) }
        }
        $commandText = '& ' + (($arguments | ForEach-Object { ConvertTo-ShellLiteral -Value $_ }) -join ' ')
        $resultPath = Join-Path $runtime ('run-{0}-{1}.json' -f ([DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')), $index)
        $output = @(& $shell -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $runner `
            -Command $commandText -WorkingDirectory $RepositoryRoot `
            -ExpectedUpperBoundSeconds $Config.expected -CleanupAllowanceSeconds $Config.cleanup `
            -HeartbeatIntervalSeconds $Config.heartbeat -TimeoutBasis $Config.basis -ResultPath $resultPath 2>&1)
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) {
            $details = (($output | Out-String).Trim())
            if (Test-Path -LiteralPath $resultPath -PathType Leaf) {
                try {
                    $record = Get-Content -LiteralPath $resultPath -Raw | ConvertFrom-Json
                    $stderrPath = Get-PropertyValue -Object $record -Name 'stderr_path'
                    if ($stderrPath -and (Test-Path -LiteralPath $stderrPath -PathType Leaf)) {
                        $stderr = Get-Content -LiteralPath $stderrPath -Raw
                        if ($stderr) { $details += "`nChild stderr:`n$stderr" }
                    }
                }
                catch { }
            }
            if ($details.Length -gt 3000) { $details = $details.Substring($details.Length - 3000) }
            return [pscustomobject]@{ passed = $false; files = $files; detail = "Command failed: $commandText`n$resultPath`n$details" }
        }
        $index++
    }
    return [pscustomobject]@{ passed = $true; files = $files; detail = $null }
}

function Invoke-ChangedStopCommands {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][object]$Config,
        [Parameter(Mandatory = $true)][object[]]$Entries
    )

    $files = @($Entries | ForEach-Object { [string]$_.path } | Sort-Object -Unique)
    return Invoke-StopCommandTemplates -RepositoryRoot $RepositoryRoot -Config $Config -CommandTemplates $Config.commands -Files $files
}

function Invoke-FullVerificationFallback {
    param(
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][object]$Config,
        [Parameter(Mandatory = $true)][string]$Reason
    )

    $run = Invoke-StopCommandTemplates -RepositoryRoot $RepositoryRoot -Config $Config -CommandTemplates $Config.fullCommands
    if (-not $run.passed) {
        Write-StopBlock "Full verification fallback failed because no valid durable verification snapshot is available: $Reason. Fix the reported failure before stopping.`n`n$($run.detail)"
        return
    }
    try {
        $statePath = Get-StatePath -RepositoryRoot $RepositoryRoot
        $state = New-VerificationSnapshotState -RepositoryRoot $RepositoryRoot
        Write-State -State $state -Path $statePath
    }
    catch {
        Write-StopBlock "Full verification passed, but the durable verification snapshot could not be published: $($_.Exception.Message)"
        return
    }
    Write-WarningMessage "No valid durable verification snapshot was available: $Reason. Ran the configured full verification fallback and published the new authoritative snapshot."
}

$raw = [Console]::In.ReadToEnd()
try { $payload = if ($raw) { $raw | ConvertFrom-Json } else { [pscustomobject]@{} } }
catch { Write-WarningMessage 'Portable Stop verification received malformed JSON and took no action.'; exit 0 }

try {
    $root = Get-RepositoryRoot -Payload $payload
    if (-not $root) { exit 0 }
    $configResult = Get-StopConfig -RepositoryRoot $root
    if ($configResult.state -in @('absent', 'disabled')) { exit 0 }
    if ($configResult.state -eq 'invalid') { Write-WarningMessage "Portable Stop verification is disabled by invalid configuration: $($configResult.error)"; exit 0 }

    Add-WorkspaceVenvToPath -RepositoryRoot $root

    $statePath = Get-StatePath -RepositoryRoot $root

    if ($Event -eq 'session-start') {
        $state = New-VerificationSnapshotState -RepositoryRoot $root
        Write-State -State $state -Path $statePath
        exit 0
    }

    if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) {
        Invoke-FullVerificationFallback -RepositoryRoot $root -Config $configResult.config -Reason 'the durable verification snapshot is unavailable'
        exit 0
    }
    try {
        $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    }
    catch {
        Invoke-FullVerificationFallback -RepositoryRoot $root -Config $configResult.config -Reason 'the saved SessionStart state is unreadable'
        exit 0
    }
    $baselineProperty = $state.PSObject.Properties['baseline']
    $lastVerifiedProperty = $state.PSObject.Properties['last_verified']
    $stateKeys = @($state.PSObject.Properties.Name | Sort-Object)
    $expectedStateKeys = @('baseline', 'last_verified', 'schema')
    if ((Get-PropertyValue -Object $state -Name 'schema') -ne 'portable-stop-verification-snapshot/v1' -or
        @(Compare-Object -ReferenceObject $expectedStateKeys -DifferenceObject $stateKeys).Count -ne 0 -or
        $null -eq $baselineProperty -or
        $null -eq $lastVerifiedProperty -or
        -not (Test-SnapshotEntries -Entries $baselineProperty.Value) -or
        -not (Test-SnapshotEntries -Entries $lastVerifiedProperty.Value)) {
        Invoke-FullVerificationFallback -RepositoryRoot $root -Config $configResult.config -Reason 'the durable verification snapshot does not have the provider-neutral schema'
        exit 0
    }
    $baseline = Get-EntryMap -Entries @($baselineProperty.Value)
    $lastVerified = Get-EntryMap -Entries @($lastVerifiedProperty.Value)
    $pending = [Collections.Generic.List[object]]::new()
    $incomplete = [Collections.Generic.List[string]]::new()
    foreach ($entry in @(Get-WorktreeSnapshot -RepositoryRoot $root)) {
        if (-not (Test-EligibleSource -Path $entry.path -Config $configResult.config)) { continue }
        $current = Get-Fingerprint -Entry $entry
        $inSession = -not $baseline.ContainsKey($entry.path) -or $current -ne (Get-Fingerprint -Entry $baseline[$entry.path])
        if (-not $inSession) { continue }
        if ($entry.kind -ne 'present') { $incomplete.Add($entry.path); continue }
        if ($lastVerified.ContainsKey($entry.path) -and $current -eq (Get-Fingerprint -Entry $lastVerified[$entry.path])) { continue }
        $pending.Add($entry)
    }
    if ($incomplete.Count -gt 0) {
        Write-StopBlock "Changed-file verification is incomplete for eligible deleted or renamed files: $($incomplete -join ', '). Restore them or run the repository's appropriate targeted check explicitly."
        exit 0
    }
    if ($pending.Count -eq 0) { exit 0 }

    $run = Invoke-ChangedStopCommands -RepositoryRoot $root -Config $configResult.config -Entries @($pending)
    if (-not $run.passed) {
        Write-StopBlock "Changed-file verification failed for: $($run.files -join ', '). Fix the reported failure and let the Stop hook rerun it.`n`n$($run.detail)"
        exit 0
    }
    foreach ($entry in @($pending)) { $lastVerified[$entry.path] = $entry }
    $state.last_verified = @($lastVerified.Values | Sort-Object path)
    Write-State -State $state -Path $statePath
}
catch {
    if ($Event -eq 'stop') { Write-StopBlock "Changed-file verification could not complete: $($_.Exception.Message)" }
    else { Write-WarningMessage "Portable Stop verification could not create its baseline: $($_.Exception.Message)" }
}
