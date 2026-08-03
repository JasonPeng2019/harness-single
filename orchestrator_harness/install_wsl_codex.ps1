param(
    [string]$Distro = "OrchestratorHarness-Test",
    [string]$InstallRoot = "/opt/orchestrator-harness-codex",
    [string]$Release = "0.146.0"
)

$ErrorActionPreference = "Stop"
$script = @"
set -eu
export HOME='$InstallRoot/user-home'
export CODEX_HOME='$InstallRoot/home'
export CODEX_INSTALL_DIR='$InstallRoot/bin'
export CODEX_NON_INTERACTIVE=1
export CODEX_RELEASE='$Release'
mkdir -p "`$HOME" "`$CODEX_HOME" "`$CODEX_INSTALL_DIR"
curl -fsSL https://chatgpt.com/codex/install.sh | sh
"`$CODEX_INSTALL_DIR/codex" --version
du -sh '$InstallRoot'
"@
$script = $script -replace "`r", ""
$script | wsl.exe -d $Distro -u root -- sh
if ($LASTEXITCODE -ne 0) {
    throw "WSL Codex installation failed with exit code $LASTEXITCODE"
}

Write-Host "Removal:"
Write-Host "  wsl.exe -d $Distro -u root -- rm -rf '$InstallRoot'"
