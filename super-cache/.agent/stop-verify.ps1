param()

$ErrorActionPreference = 'Stop'

function Emit-Result([bool]$Continue, [string]$Reason, [object[]]$Checks) {
    [PSCustomObject]@{
        continue = $Continue
        reason = $Reason
        checks = $Checks
        boundary = $env:AGENT_STOP_GATE_BOUNDARY
    } | ConvertTo-Json -Compress
}

if ($env:AGENT_STOP_GATE_ENABLED -ne '1') {
    Emit-Result $true 'gate disabled' @()
    exit 0
}

$root = (& git rev-parse --show-toplevel).Trim()
if (-not $root) { Emit-Result $false 'not in a Git worktree' @(); exit 0 }
$recordDir = Join-Path $root '.agent-runtime\stop-verify'
New-Item -ItemType Directory -Force -Path $recordDir | Out-Null
$snapshot = Join-Path $recordDir 'baseline.json'

# A linked worktree normally has no local virtual environment.  Reuse any
# repository worktree environment without baking a machine-specific path.
$paths = @()
foreach ($line in (& git worktree list --porcelain)) {
    if ($line -like 'worktree *') {
        $worktree = $line.Substring(9)
        foreach ($relative in @('.venv\Scripts', '.venv\bin')) {
            $candidate = Join-Path $worktree $relative
            if (Test-Path -LiteralPath $candidate -PathType Container) { $paths += $candidate }
        }
    }
}
if ($paths.Count) { $env:PATH = (($paths | Select-Object -Unique) -join [IO.Path]::PathSeparator) + [IO.Path]::PathSeparator + $env:PATH }

$status = @(& git status --porcelain | Where-Object {
    $_ -notmatch '\.agent-workspace/' -and $_ -notmatch '^\?\? \.agent-runtime/'
})
if ($env:AGENT_STOP_GATE_BOUNDARY -eq 'baseline') {
    @{ status = $status } | ConvertTo-Json -Compress | Set-Content -LiteralPath $snapshot -Encoding utf8
    Emit-Result $true 'baseline recorded' @()
    exit 0
}
if (-not (Test-Path -LiteralPath $snapshot)) { Emit-Result $false 'baseline snapshot is missing' @(); exit 0 }
$baseline = @((Get-Content -LiteralPath $snapshot -Raw | ConvertFrom-Json).status)
$changed = @($status | Where-Object { $_ -notin $baseline } | ForEach-Object { $_.Substring(3).Trim() } | Where-Object { $_ -match '\.py$' -and (Test-Path -LiteralPath (Join-Path $root $_) -PathType Leaf) })
$checks = @()
foreach ($command in @('ruff', 'pyright')) {
    foreach ($path in $changed) {
        if ($command -eq 'ruff') {
            & ruff check $path 2>$null
        } else {
            & pyright $path 2>$null
        }
        $code = $LASTEXITCODE
        $checks += @{ command = $command; path = $path; exit_code = $code }
        if ($code -ne 0) { Emit-Result $false "$command failed for $path" $checks; exit 0 }
    }
}
Emit-Result $true 'checks passed' $checks
exit 0
