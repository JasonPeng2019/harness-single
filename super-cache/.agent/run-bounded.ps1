[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Command,

    [string]$WorkingDirectory = (Get-Location).Path,

    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 2147483647)]
    [int]$ExpectedUpperBoundSeconds,

    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 120)]
    [int]$CleanupAllowanceSeconds,

    [ValidateRange(0, 2147483647)]
    [int]$MaximumLifetimeSeconds = 0,

    [ValidateRange(1, 60)]
    [int]$HeartbeatIntervalSeconds = 30,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$TimeoutBasis,

    [string]$ResultPath
)

$ErrorActionPreference = 'Stop'

function Write-Heartbeat {
    param([Parameter(Mandatory = $true)][string]$Message)

    [Console]::Out.WriteLine($Message)
    [Console]::Out.Flush()
}

function Get-ObservedProcessIds {
    param([Parameter(Mandatory = $true)][int]$RootProcessId)

    $observed = [Collections.Generic.HashSet[int]]::new()
    [void]$observed.Add($RootProcessId)
    $frontier = [Collections.Generic.Queue[int]]::new()
    $frontier.Enqueue($RootProcessId)
    while ($frontier.Count -gt 0) {
        $parent = $frontier.Dequeue()
        foreach ($child in @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$parent" -ErrorAction SilentlyContinue)) {
            $childId = [int]$child.ProcessId
            if ($observed.Add($childId)) {
                $frontier.Enqueue($childId)
            }
        }
    }
    return @($observed)
}

if (-not (Test-Path -LiteralPath $WorkingDirectory -PathType Container)) {
    throw "Working directory does not exist: $WorkingDirectory"
}

$resolvedWorkingDirectory = (Resolve-Path -LiteralPath $WorkingDirectory).Path
$computedMaximumLifetimeSeconds = $ExpectedUpperBoundSeconds + $CleanupAllowanceSeconds
if ($MaximumLifetimeSeconds -eq 0) {
    $MaximumLifetimeSeconds = $computedMaximumLifetimeSeconds
}
elseif ($MaximumLifetimeSeconds -ne $computedMaximumLifetimeSeconds) {
    throw (
        "MaximumLifetimeSeconds must equal ExpectedUpperBoundSeconds plus " +
        "CleanupAllowanceSeconds ($ExpectedUpperBoundSeconds + $CleanupAllowanceSeconds = " +
        "$computedMaximumLifetimeSeconds), not $MaximumLifetimeSeconds."
    )
}
$maximumReasonableCleanup = [Math]::Max(
    5,
    [Math]::Min(120, [Math]::Ceiling($ExpectedUpperBoundSeconds * 0.25))
)
if ($CleanupAllowanceSeconds -gt $maximumReasonableCleanup) {
    throw (
        "CleanupAllowanceSeconds=$CleanupAllowanceSeconds is excessive for " +
        "ExpectedUpperBoundSeconds=$ExpectedUpperBoundSeconds; maximum reasonable cleanup " +
        "allowance is $maximumReasonableCleanup seconds."
    )
}
if (-not $ResultPath) {
    $gitRoot = & git -C $resolvedWorkingDirectory rev-parse --show-toplevel 2>$null
    $runtimeRoot = if ($LASTEXITCODE -eq 0 -and $gitRoot) {
        Join-Path ([string]$gitRoot.Trim()) '.agent-runtime'
    }
    else {
        Join-Path $resolvedWorkingDirectory '.agent-runtime'
    }
    $name = 'run-{0}-{1}.json' -f ([DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')), ([guid]::NewGuid().ToString('N'))
    $ResultPath = Join-Path $runtimeRoot $name
}

$resolvedResultPath = [IO.Path]::GetFullPath($ResultPath)
$resultDirectory = Split-Path -Parent $resolvedResultPath
[IO.Directory]::CreateDirectory($resultDirectory) | Out-Null
$resultStem = [IO.Path]::GetFileNameWithoutExtension($resolvedResultPath)
$stdoutPath = Join-Path $resultDirectory "$resultStem.stdout.log"
$stderrPath = Join-Path $resultDirectory "$resultStem.stderr.log"
$exitCodePath = Join-Path $resultDirectory "$resultStem.exit-code.txt"

$shellPath = (Get-Process -Id $PID).Path
$innerCommand = @"
& {
$Command
}
if (`$null -ne `$LASTEXITCODE) { exit `$LASTEXITCODE }
if (-not `$?) { exit 1 }
exit 0
"@
$innerEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($innerCommand))
$escapedShell = $shellPath.Replace("'", "''")
$escapedExit = $exitCodePath.Replace("'", "''")
$wrappedCommand = @"
& '$escapedShell' -NoLogo -NoProfile -NonInteractive -EncodedCommand '$innerEncoded'
`$childExitCode = if (`$null -eq `$LASTEXITCODE) { 1 } else { [int]`$LASTEXITCODE }
[IO.File]::WriteAllText('$escapedExit', [string]`$childExitCode, [Text.Encoding]::ASCII)
exit `$childExitCode
"@
$encodedCommand = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($wrappedCommand))
$startedAt = [DateTimeOffset]::UtcNow
$process = Start-Process -FilePath $shellPath `
    -ArgumentList @('-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', $encodedCommand) `
    -WorkingDirectory $resolvedWorkingDirectory `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -WindowStyle Hidden `
    -PassThru

$observedIds = [Collections.Generic.HashSet[int]]::new()
[void]$observedIds.Add([int]$process.Id)
$executionDeadline = $startedAt.AddSeconds($ExpectedUpperBoundSeconds)
$cleanupDeadline = $startedAt.AddSeconds($maximumLifetimeSeconds)
$status = 'FAILED'
$exitCode = 1
$cleanupVerified = $true

Write-Heartbeat "RUNNING pid=$($process.Id) elapsed_seconds=0 remaining_seconds=$ExpectedUpperBoundSeconds"
while (-not $process.HasExited -and [DateTimeOffset]::UtcNow -lt $executionDeadline) {
    foreach ($observedId in @(Get-ObservedProcessIds -RootProcessId $process.Id)) {
        [void]$observedIds.Add([int]$observedId)
    }
    $remaining = [Math]::Max(0, [Math]::Ceiling(($executionDeadline - [DateTimeOffset]::UtcNow).TotalSeconds))
    $waitSeconds = [Math]::Min($HeartbeatIntervalSeconds, $remaining)
    if ($waitSeconds -gt 0) {
        [void]$process.WaitForExit($waitSeconds * 1000)
    }
    if (-not $process.HasExited) {
        $elapsed = [Math]::Floor(([DateTimeOffset]::UtcNow - $startedAt).TotalSeconds)
        $remaining = [Math]::Max(0, [Math]::Ceiling(($executionDeadline - [DateTimeOffset]::UtcNow).TotalSeconds))
        Write-Heartbeat "RUNNING pid=$($process.Id) elapsed_seconds=$elapsed remaining_seconds=$remaining stdout_bytes=$((Get-Item $stdoutPath -ErrorAction SilentlyContinue).Length) stderr_bytes=$((Get-Item $stderrPath -ErrorAction SilentlyContinue).Length) observed_processes=$($observedIds.Count)"
    }
}

if (-not $process.HasExited) {
    foreach ($observedId in @(Get-ObservedProcessIds -RootProcessId $process.Id)) {
        [void]$observedIds.Add([int]$observedId)
    }
    & "$env:SystemRoot\System32\taskkill.exe" /PID $process.Id /T /F *> $null
    while (-not $process.HasExited -and [DateTimeOffset]::UtcNow -lt $cleanupDeadline) {
        [void]$process.WaitForExit(200)
    }
    foreach ($observedId in $observedIds) {
        if (Get-Process -Id $observedId -ErrorAction SilentlyContinue) {
            $cleanupVerified = $false
        }
    }
    $status = 'TIMED_OUT'
    $exitCode = 124
}
else {
    $process.WaitForExit()
    if (Test-Path -LiteralPath $exitCodePath -PathType Leaf) {
        $exitCode = [int](Get-Content -LiteralPath $exitCodePath -Raw)
        [IO.File]::Delete($exitCodePath)
    }
    $status = if ($exitCode -eq 0) { 'PASSED' } else { 'FAILED' }
}

$finishedAt = [DateTimeOffset]::UtcNow
$result = [ordered]@{
    schema = 'portable-bounded-run-v1'
    status = $status
    command = $Command
    working_directory = $resolvedWorkingDirectory
    expected_upper_bound_seconds = $ExpectedUpperBoundSeconds
    cleanup_allowance_seconds = $CleanupAllowanceSeconds
    maximum_lifetime_seconds = $maximumLifetimeSeconds
    heartbeat_interval_seconds = $HeartbeatIntervalSeconds
    timeout_basis = $TimeoutBasis
    started_at = $startedAt.ToString('o')
    finished_at = $finishedAt.ToString('o')
    elapsed_seconds = [Math]::Round(($finishedAt - $startedAt).TotalSeconds, 3)
    root_process_id = $process.Id
    observed_process_ids = @($observedIds | Sort-Object)
    cleanup_verified = $cleanupVerified
    exit_code = $exitCode
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
}
[IO.File]::WriteAllText($resolvedResultPath, ($result | ConvertTo-Json -Depth 5), [Text.UTF8Encoding]::new($false))
Write-Heartbeat "$status exit_code=$exitCode elapsed_seconds=$($result.elapsed_seconds) result=$resolvedResultPath cleanup_verified=$cleanupVerified"
exit $exitCode
