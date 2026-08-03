from __future__ import annotations

import argparse
import json
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path


HARNESS_ROOT = Path(__file__).resolve().parents[1]
SUPPORT = Path(__file__).resolve().parent / "support"
DEFAULT_DISTRO = "OrchestratorHarness-Test"
DEFAULT_CODEX_ROOT = "/opt/orchestrator-harness-codex"


def wsl_path(distro: str, path: Path) -> str:
    del distro
    resolved = path.resolve()
    drive = resolved.drive
    if len(drive) != 2 or drive[1] != ":":
        raise RuntimeError(f"real-agent test requires a drive-letter path: {resolved}")
    tail = resolved.as_posix()[2:].lstrip("/")
    return f"/mnt/{drive[0].lower()}/{tail}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the OS-isolated real-Codex orchestration-harness test."
    )
    parser.add_argument(
        "--distro", default=os.environ.get("ORCH_HARNESS_WSL_DISTRO", DEFAULT_DISTRO)
    )
    parser.add_argument(
        "--codex-root",
        default=os.environ.get("ORCH_HARNESS_WSL_CODEX_ROOT", DEFAULT_CODEX_ROOT),
    )
    parser.add_argument("--model", default="gpt-5.6-terra")
    args = parser.parse_args()

    if os.name != "nt":
        raise RuntimeError(
            "this wrapper is for Windows; invoke wsl_real_agent_driver.py as root on Linux"
        )
    auth = Path.home() / ".codex" / "auth.json"
    if not auth.is_file():
        raise RuntimeError(f"Codex authentication file is unavailable: {auth}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = uuid.uuid4().hex
    evidence = (HARNESS_ROOT / ".real-agent" / stamp).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    command = [
        "wsl.exe",
        "-d",
        args.distro,
        "-u",
        "root",
        "--",
        "python3",
        wsl_path(args.distro, SUPPORT / "wsl_guarded_entry.py"),
        "--run-id",
        run_id,
        "--driver",
        wsl_path(args.distro, SUPPORT / "wsl_real_agent_driver.py"),
        "--",
        "--source-root",
        wsl_path(args.distro, HARNESS_ROOT.parent),
        "--evidence-dir",
        wsl_path(args.distro, evidence),
        "--auth-json",
        wsl_path(args.distro, auth),
        "--codex-root",
        args.codex_root,
        "--model",
        args.model,
    ]
    completed = subprocess.run(
        command,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    transcript = completed.stdout
    (evidence / "wsl-driver.log").write_text(transcript, encoding="utf-8")
    print(transcript, end="")
    result_path = evidence / "REAL_AGENT_TEST_RESULT.json"
    if completed.returncode != 0:
        raise RuntimeError(
            f"isolated real-agent driver failed with exit code {completed.returncode}; "
            f"evidence: {evidence}"
        )
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if result.get("status") != "PASS":
        raise AssertionError(f"real-agent result is not PASS: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
