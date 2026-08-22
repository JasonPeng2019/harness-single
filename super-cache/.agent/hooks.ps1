[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('session-start', 'pre-tool-use', 'pre-compact')]
    [string]$Event
)

$ErrorActionPreference = 'Stop'

function Write-HookJson {
    param([Parameter(Mandatory = $true)][hashtable]$Value)

    $Value | ConvertTo-Json -Depth 8 -Compress
}

function Get-RepositoryRoot {
    param([AllowNull()][object]$Payload)

    $start = (Get-Location).Path
    if ($null -ne $Payload -and $Payload.PSObject.Properties.Name -contains 'cwd' -and $Payload.cwd) {
        $start = [string]$Payload.cwd
    }
    $root = & git -C $start rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -eq 0 -and $root) {
        return [IO.Path]::GetFullPath(([string]$root).Trim())
    }
    return [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
}

function Test-BoundedWrapper {
    param([Parameter(Mandatory = $true)][string]$Command)

    return $Command -match '(?i)\.agent[\\/]run-bounded\.(?:ps1|sh)\b'
}

function Test-AgentSession {
    param([Parameter(Mandatory = $true)][string]$Command)

    return $Command -match '(?i)\bcodex(?:\.exe)?\s+exec\b|\bclaude(?:\.exe)?\s+(?:-p|--print)\b|\.agent[\\/]launch-agent\.(?:ps1|sh)\b|\.agent[\\/]multi_agent\.py\s+(?:launch|supervise)\b'
}

function Get-CommandTokens {
    param([Parameter(Mandatory = $true)][string]$Command)

    $errors = $null
    return @([Management.Automation.PSParser]::Tokenize($Command, [ref]$errors) | ForEach-Object Content)
}

function Test-DangerousDelete {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot
    )

    $segments = $Command -split '[;\r\n]'
    $protected = @(
        [IO.Path]::GetFullPath($RepositoryRoot).TrimEnd('\', '/'),
        [IO.Path]::GetPathRoot($RepositoryRoot).TrimEnd('\', '/'),
        [Environment]::GetFolderPath('UserProfile').TrimEnd('\', '/')
    ) | Where-Object { $_ }

    foreach ($segment in $segments) {
        if ($segment -notmatch '(?i)^\s*(?:&\s*)?(?:Remove-Item|rm|rmdir|rd)\b') {
            continue
        }
        if ($segment -notmatch '(?i)(?:-Recurse\b|\s-(?:[A-Za-z]*r[A-Za-z]*f?|[A-Za-z]*f[A-Za-z]*r)[A-Za-z]*\b)') {
            continue
        }
        if ($segment -match '(?i)(?:\$HOME|\$env:USERPROFILE|%USERPROFILE%)(?:[\\/]\*)?(?:\s|$)') {
            return $true
        }
        foreach ($token in @(Get-CommandTokens -Command $segment)) {
            $candidate = ([string]$token).Trim('"', "'").TrimEnd('\', '/')
            if ($candidate -in @('/', '~') -or $candidate -match '^[A-Za-z]:$') {
                return $true
            }
            foreach ($target in $protected) {
                if ($candidate -ieq $target -or $candidate -ieq "$target\*" -or $candidate -ieq "$target/*") {
                    return $true
                }
            }
        }
    }
    return $false
}

function Test-DirectGitMutation {
    param([Parameter(Mandatory = $true)][string]$Command)

    foreach ($segment in @($Command -split '[;\r\n]')) {
        if ($segment -match '(?i)^\s*(?:&\s*)?(?:Remove-Item|rm|rmdir|rd|Move-Item|mv|move|Set-Content|Add-Content|Out-File|New-Item)\b' -and
            $segment -match '(?i)(?:^|[\\/\s"''])\.git(?:[\\/\s"'']|$)') {
            return $true
        }
        if ($segment -match '(?i)(?:>|>>|2>)\s*["'']?[^\r\n;|]*[\\/]?\.git(?:[\\/]|$)') {
            return $true
        }
    }
    return $false
}

function Test-ConfiguredBoundedCommand {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot
    )

    $config = Join-Path $RepositoryRoot '.agent/bounded-commands.txt'
    if (-not (Test-Path -LiteralPath $config -PathType Leaf)) {
        return $false
    }
    foreach ($line in Get-Content -LiteralPath $config) {
        $fragment = $line.Trim()
        if (-not $fragment -or $fragment.StartsWith('#')) {
            continue
        }
        if ($Command.IndexOf($fragment, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $true
        }
    }
    return $false
}

function Invoke-BoundedScriptPolicy {
    param(
        [Parameter(Mandatory = $true)][string]$PayloadJson,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot
    )

    $configPath = Join-Path $RepositoryRoot '.agent/bounded-script-policy.json'
    if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) {
        return $null
    }
    try {
        $config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
        if (-not ($config.PSObject.Properties.Name -contains 'enabled') -or $config.enabled -isnot [bool]) {
            throw 'the enabled property must be a Boolean'
        }
        if (-not $config.enabled) {
            return $null
        }
    }
    catch {
        return (Write-HookJson -Value @{
                hookSpecificOutput = @{
                    hookEventName = 'PreToolUse'
                    permissionDecision = 'deny'
                    permissionDecisionReason = "Optional bounded script policy configuration error: $($_.Exception.Message). Fix it or disable the policy."
                }
            })
    }

    $adapter = Join-Path $RepositoryRoot '.agent/bounded-script-adapter.py'
    if (-not (Test-Path -LiteralPath $adapter -PathType Leaf)) {
        return (Write-HookJson -Value @{
                hookSpecificOutput = @{
                    hookEventName = 'PreToolUse'
                    permissionDecision = 'deny'
                    permissionDecisionReason = 'Optional bounded script policy is enabled but .agent/bounded-script-adapter.py is missing. Restore it or disable the policy.'
                }
            })
    }

    $python = Get-Command python -ErrorAction SilentlyContinue
    $arguments = @($adapter, 'pre-tool-use')
    if (-not $python) {
        $python = Get-Command py -ErrorAction SilentlyContinue
        $arguments = @('-3', $adapter, 'pre-tool-use')
    }
    if (-not $python) {
        return (Write-HookJson -Value @{
                hookSpecificOutput = @{
                    hookEventName = 'PreToolUse'
                    permissionDecision = 'deny'
                    permissionDecisionReason = 'Optional bounded script policy is enabled but Python 3 is unavailable to the hook. Install Python, restore the adapter, or disable the policy.'
                }
            })
    }

    $output = $PayloadJson | & $python.Path @arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        return (Write-HookJson -Value @{
                hookSpecificOutput = @{
                    hookEventName = 'PreToolUse'
                    permissionDecision = 'deny'
                    permissionDecisionReason = "Optional bounded script policy could not run: $($output -join ' '). Fix it or disable the policy."
                }
            })
    }
    $text = ($output -join "`n").Trim()
    if (-not $text) {
        return $null
    }
    try {
        $parsed = $text | ConvertFrom-Json
        if ($parsed.hookSpecificOutput.permissionDecision -eq 'deny') {
            return $text
        }
    }
    catch {
        return (Write-HookJson -Value @{
                hookSpecificOutput = @{
                    hookEventName = 'PreToolUse'
                    permissionDecision = 'deny'
                    permissionDecisionReason = 'Optional bounded script policy returned malformed output. Fix it or disable the policy.'
                }
            })
    }
    return $null
}

$raw = [Console]::In.ReadToEnd()
$payload = $null
try {
    $payload = if ($raw) { $raw | ConvertFrom-Json } else { [pscustomobject]@{} }
}
catch {
    Write-HookJson -Value @{ systemMessage = 'Portable workflow hook received malformed JSON and took no action.' }
    exit 0
}

$root = Get-RepositoryRoot -Payload $payload

if ($Event -eq 'session-start') {
    $context = [Collections.Generic.List[string]]::new()
    $context.Add('Portable workflow active: use repository instructions and relevant skills before acting.')
    $context.Add('Route only finite non-agent commands explicitly selected in .agent/bounded-commands.txt through .agent/run-bounded.ps1.')
    if (Test-Path -LiteralPath (Join-Path $root '.agent/verify.toml') -PathType Leaf) {
        $context.Add('A local verification override exists at .agent/verify.toml.')
    }
    $handoff = Join-Path $root 'HANDOFF.md'
    if (Test-Path -LiteralPath $handoff -PathType Leaf) {
        $preview = ((Get-Content -LiteralPath $handoff -TotalCount 30) -join "`n")
        if ($preview.Length -gt 3000) {
            $preview = $preview.Substring(0, 3000) + "`n[handoff preview truncated]"
        }
        $context.Add("HANDOFF.md exists. Read it before continuing.`n$preview")
    }
    Write-HookJson -Value @{
        hookSpecificOutput = @{
            hookEventName = 'SessionStart'
            additionalContext = ($context -join "`n")
        }
    }
    exit 0
}

if ($Event -eq 'pre-compact') {
    $dirty = @(& git -C $root status --porcelain 2>$null).Count -gt 0
    $hasHandoff = Test-Path -LiteralPath (Join-Path $root 'HANDOFF.md') -PathType Leaf
    if ($dirty -or $hasHandoff) {
        Write-HookJson -Value @{
            continue = $true
            systemMessage = 'Work may be in progress. If continuity matters, use the checkpoint skill to refresh HANDOFF.md before compaction.'
        }
    }
    exit 0
}

$command = ''
if ($payload -and $payload.tool_input -and $payload.tool_input.command) {
    $command = [string]$payload.tool_input.command
}
if (-not $command) {
    exit 0
}

$agentSession = Test-AgentSession -Command $command
if (-not $agentSession) {
    $policyOutput = Invoke-BoundedScriptPolicy -PayloadJson $raw -RepositoryRoot $root
    if ($policyOutput) {
        Write-Output $policyOutput
        exit 0
    }
}

$reason = $null
if (Test-DangerousDelete -Command $command -RepositoryRoot $root) {
    $reason = 'Blocked a recursive deletion aimed at a filesystem root, home directory, or repository root. Resolve and select the intended narrower target.'
}
elseif (Test-DirectGitMutation -Command $command) {
    $reason = 'Blocked direct mutation of .git internals. Use Git commands for repository metadata operations.'
}
elseif ($agentSession -and (Test-BoundedWrapper -Command $command)) {
    $reason = 'Agent and subagent sessions must remain unbounded. Launch them directly; reserve .agent/run-bounded.ps1 for finite commands explicitly listed in .agent/bounded-commands.txt.'
}
elseif (-not $agentSession -and -not (Test-BoundedWrapper -Command $command) -and
    (Test-ConfiguredBoundedCommand -Command $command -RepositoryRoot $root)) {
    $reason = 'This configured long-running command must run through .agent/run-bounded.ps1 with a justified runtime bound.'
}

if ($reason) {
    Write-HookJson -Value @{
        hookSpecificOutput = @{
            hookEventName = 'PreToolUse'
            permissionDecision = 'deny'
            permissionDecisionReason = $reason
        }
    }
}
