param(
    [string]$Distro = "OrchestratorHarness-Test",
    [string]$InstallRoot = "/opt/orchestrator-harness-codex",
    [string]$Release = "0.146.0"
)

$ErrorActionPreference = "Stop"
if ($Distro -notmatch '^[A-Za-z0-9_.-]+$') {
    throw "Distro must be a simple WSL distribution name"
}

$script = @"
set -eu
export HOME='$InstallRoot/user-home'
export CODEX_HOME='$InstallRoot/home'
export CODEX_INSTALL_DIR='$InstallRoot/bin'
export CODEX_NON_INTERACTIVE=1
export CODEX_RELEASE='$Release'
mkdir -p "`$HOME" "`$CODEX_HOME" "`$CODEX_INSTALL_DIR"
# Already installed releases are verified and reused byte-for-byte.
if [ -x "`$CODEX_INSTALL_DIR/codex" ]; then
  version="`$(`$CODEX_INSTALL_DIR/codex --version)"
  printf '%s\n' "`$version"
  case "`$version" in
    *"`$CODEX_RELEASE"*)
      du -sh '$InstallRoot'
      exit 0
      ;;
    *)
      printf 'installed Codex version does not match requested %s\n' "`$CODEX_RELEASE" >&2
      exit 42
      ;;
  esac
fi
installer="`$(mktemp)"
trap 'rm -f "`$installer"' EXIT
curl -fsSL https://chatgpt.com/codex/install.sh > "`$installer"
sh "`$installer"
"`$CODEX_INSTALL_DIR/codex" --version
du -sh '$InstallRoot'
"@
$script = $script -replace "`r`n?", "`n"
$bytes = [Text.Encoding]::UTF8.GetBytes($script)

$startInfo = [Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = "wsl.exe"
$startInfo.Arguments = "-d `"$Distro`" -u root -- sh"
$startInfo.UseShellExecute = $false
$startInfo.RedirectStandardInput = $true
$process = [Diagnostics.Process]::Start($startInfo)
try {
    $input = $process.StandardInput.BaseStream
    $input.Write($bytes, 0, $bytes.Length)
    $input.Flush()
    $input.Close()
    $process.WaitForExit()
    $exitCode = $process.ExitCode
} finally {
    if (-not $process.HasExited) {
        $process.Kill()
        $process.WaitForExit()
    }
    $process.Dispose()
}
if ($exitCode -ne 0) {
    throw "WSL Codex installation failed with exit code $exitCode"
}

Write-Host "Removal:"
Write-Host "  wsl.exe -d $Distro -u root -- rm -rf '$InstallRoot'"
