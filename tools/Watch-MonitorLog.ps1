[CmdletBinding()]
param(
    [string]$RuntimeRoot,
    [ValidateRange(0, 10000)]
    [int]$Tail = 100,
    [switch]$NoWait
)

$ErrorActionPreference = "Stop"
$watcher = Join-Path $PSScriptRoot "Watch-DetailedMonitorLog.ps1"
& $watcher -RuntimeRoot $RuntimeRoot -Tail $Tail -NoWait:$NoWait
