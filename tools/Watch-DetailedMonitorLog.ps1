[CmdletBinding()]
param(
    [string]$RuntimeRoot,
    [ValidateRange(0, 10000)]
    [int]$Tail = 100,
    [switch]$Raw,
    [switch]$All,
    [switch]$NoWait
)

$ErrorActionPreference = "Stop"

function Test-ScalarValue {
    param($Value)
    return (
        $null -eq $Value -or
        $Value -is [string] -or
        $Value -is [bool] -or
        $Value -is [char] -or
        $Value -is [datetime] -or
        $Value -is [decimal] -or
        $Value -is [double] -or
        $Value -is [single] -or
        $Value -is [byte] -or
        $Value -is [int16] -or
        $Value -is [int32] -or
        $Value -is [int64] -or
        $Value -is [uint16] -or
        $Value -is [uint32] -or
        $Value -is [uint64]
    )
}

function Write-ReadableValue {
    param(
        $Value,
        [int]$Indent = 0,
        [string]$Label = ""
    )

    $padding = " " * $Indent
    $prefix = if ($Label) { "${padding}${Label}:" } else { $padding }
    if ($null -eq $Value) {
        "$prefix null"
        return
    }

    if (Test-ScalarValue $Value) {
        $text = [string]$Value
        if ($text.Contains("`n") -or $text.Contains("`r")) {
            "$prefix |"
            $text = $text -replace "`r`n", "`n" -replace "`r", "`n"
            foreach ($line in ($text -split "`n", -1)) {
                (" " * ($Indent + 2)) + $line
            }
        } else {
            "$prefix $text"
        }
        return
    }

    if ($Value -is [System.Collections.IDictionary]) {
        if ($Label) { $prefix }
        foreach ($key in $Value.Keys) {
            Write-ReadableValue -Value $Value[$key] -Indent ($Indent + 2) -Label ([string]$key)
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [pscustomobject]) {
        $items = @($Value)
        if ($items.Count -eq 0) {
            "$prefix []"
            return
        }
        if ($Label) { $prefix }
        foreach ($item in $items) {
            $itemPadding = " " * ($Indent + 2)
            if (Test-ScalarValue $item) {
                if ($item -is [string] -and ($item.Contains("`n") -or $item.Contains("`r"))) {
                    "${itemPadding}- |"
                    $itemText = ([string]$item) -replace "`r`n", "`n" -replace "`r", "`n"
                    foreach ($line in ($itemText -split "`n", -1)) {
                        (" " * ($Indent + 4)) + $line
                    }
                } else {
                    "${itemPadding}- $item"
                }
            } else {
                "${itemPadding}-"
                Write-ReadableValue -Value $item -Indent ($Indent + 4)
            }
        }
        return
    }

    $properties = @($Value.PSObject.Properties)
    if ($Label) { $prefix }
    foreach ($property in $properties) {
        Write-ReadableValue -Value $property.Value -Indent ($Indent + 2) -Label $property.Name
    }
}

function Test-ImportantRecord {
    param($Record)

    $eventName = [string]$Record.event
    if ($eventName -like 'memory.*' -and $Record.actor -eq 'operator') { return $false }
    if ($eventName -match '^memory\..*recall\.(started|attempt|results|vector_results|completed|failed)$') { return $true }
    if ($eventName -match '^memory\.(always_context\.(materialized|injected)|worker\.context\.injected|preparation\.(started|completed|failed))$') { return $true }
    if ($eventName -in @('prompt.root_to_worker', 'launch.worker.requested', 'queue.manager.added')) { return $true }
    if ($eventName -eq 'worker.lifecycle') {
        return $Record.worker_event -in @('provider_started', 'provider_exited', 'result_valid', 'result_invalid', 'provider_exited_no_result', 'acceptance_copied')
    }
    if ($eventName -eq 'worker.native_event') {
        $native = $Record.native_event
        return $native.type -eq 'item.completed' -and $native.item.type -in @('agent_message', 'error')
    }
    if ($eventName -match '\.failed$') { return $true }
    if ($eventName -in @('process.stop_request.completed', 'process.stopped')) { return $true }
    return $false
}

function Write-ImportantRecord {
    param($Record)

    $eventName = [string]$Record.event
    "[$($Record.timestamp)] $eventName"
    if ($eventName -eq 'worker.native_event') {
        Write-ReadableValue -Value $Record.lane_id -Indent 2 -Label 'lane_id'
        $item = $Record.native_event.item
        if ($item.type -eq 'agent_message') {
            Write-ReadableValue -Value $item.text -Indent 2 -Label 'worker_message'
        } else {
            Write-ReadableValue -Value $item.message -Indent 2 -Label 'worker_error'
        }
    } elseif ($eventName -eq 'queue.manager.added') {
        $added = $Record.added_event
        Write-ReadableValue -Value $added.event_class -Indent 2 -Label 'event_class'
        Write-ReadableValue -Value $added.data.lane_id -Indent 2 -Label 'lane_id'
    } else {
        $names = if ($eventName -eq 'worker.lifecycle') {
            @('lane_id', 'worker_event', 'detail')
        } elseif ($eventName -eq 'launch.worker.requested') {
            @('lane_id', 'provider', 'attempt', 'resume', 'prompt')
        } elseif ($eventName -eq 'prompt.root_to_worker') {
            @('prompt_kind', 'prompt', 'worktree')
        } else {
            @('actor', 'source_actor', 'recipient', 'lane_id', 'run_id', 'worker_task', 'query', 'attempt', 'collection', 'case_id', 'score', 'atlas_hits', 'everos_hits', 'unavailable_stores', 'selected', 'results', 'context', 'context_path', 'mode', 'stores', 'error_type', 'outcome')
        }
        foreach ($name in $names) {
            $property = $Record.PSObject.Properties[$name]
            if ($null -ne $property -and $null -ne $property.Value) {
                if ($eventName -eq 'worker.lifecycle' -and $name -eq 'detail' -and $Record.worker_event -eq 'provider_started') {
                    continue # The full process command remains in the raw trace.
                }
                Write-ReadableValue -Value $property.Value -Indent 2 -Label $name
            }
        }
    }
    ""
}

$harnessRoot = Split-Path -Parent $PSScriptRoot
if (-not $RuntimeRoot) {
    $config = Get-Content -LiteralPath (Join-Path $harnessRoot "harness-config.json") -Raw |
        ConvertFrom-Json
    $RuntimeRoot = Join-Path $config.root_workspace ".harness-runtime"
}
$detailPath = Join-Path $RuntimeRoot "monitor\MONITOR_DETAIL.log"
$importantPath = Join-Path $RuntimeRoot "monitor\MONITOR_IMPORTANT.log"
$logPath = if (-not $All -and (Test-Path -LiteralPath $importantPath)) {
    $importantPath
} else {
    $detailPath
}
Write-Host "Reading $logPath (Ctrl+C to stop following; -All shows the complete trace)"
if (-not $NoWait) {
    while (-not (Test-Path -LiteralPath $logPath)) {
        Start-Sleep -Milliseconds 200
    }
} elseif (-not (Test-Path -LiteralPath $logPath)) {
    throw "Monitor trace does not exist: $logPath"
}

Get-Content -LiteralPath $logPath -Tail $Tail -Wait:(!$NoWait) | ForEach-Object {
    try {
        $record = $_ | ConvertFrom-Json
        if (-not $All -and -not (Test-ImportantRecord $record)) { return }
        if ($Raw) { $_; return }
        if (-not $All) { Write-ImportantRecord $record; return }
        $header = "[$($record.timestamp)] $($record.component) $($record.event) pid=$($record.pid)"
        $header
        foreach ($property in $record.PSObject.Properties) {
            if ($property.Name -notin @("schema", "timestamp", "pid", "component", "event")) {
                Write-ReadableValue -Value $property.Value -Indent 2 -Label $property.Name
            }
        }
        ""
    } catch {
        $_
    }
}
