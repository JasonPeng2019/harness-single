"""Board-free lifecycle proof for duplicate start, exact identity, stop, and owner loss."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / "harness_watcher_implementation" / "test_results" / "lifecycle_canary"


def config(runtime: str) -> Path:
    path = DEST / f"{runtime}.json"
    path.write_text(
        json.dumps(
            {
                "runtime_root": f"harness_watcher_implementation/test_results/lifecycle_canary/{runtime}",
                "observed_log_roots": [
                    "harness_watcher_implementation/test_results/lifecycle_canary/empty.log"
                ],
                "poll_interval_seconds": 2,
                "evaluator_command": [
                    sys.executable,
                    "-c",
                    "import json; print(json.dumps({'defect':False,'kind':None,'severity':'warning','summary':'healthy','implicated':[],'evidence':[]}))",
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def call(path: Path, command: str, *extra: str) -> tuple[int, dict[str, object]]:
    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "harness_watcher_implementation",
            "--config",
            str(path),
            command,
            *extra,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=20,
    )
    return run.returncode, json.loads(run.stdout)


def wait_not_running(path: Path, reason: str | None = None) -> dict[str, object]:
    deadline = time.monotonic() + 10
    state: dict[str, object] = {}
    while time.monotonic() < deadline:
        _, state = call(path, "status")
        if not state["running"] and (
            reason is None or state.get("state", {}).get("exit_reason") == reason
        ):
            return state
        time.sleep(0.1)
    raise RuntimeError("watcher did not stop cooperatively")


def main() -> None:
    shutil.rmtree(DEST, ignore_errors=True)
    DEST.mkdir(parents=True)
    (DEST / "empty.log").write_text("", encoding="utf-8")
    ordinary = config("ordinary_runtime")
    start_code, start = call(ordinary, "start", "--owner-pid", str(os.getpid()))
    duplicate_code, duplicate = call(ordinary, "start", "--owner-pid", str(os.getpid()))
    status_code, status = call(ordinary, "status")
    stop_code, stop = call(ordinary, "stop")
    stopped = wait_not_running(ordinary, "stop-requested")

    # The wrapper is deliberately the service owner's parent and exits immediately after start.
    owner_loss = config("owner_loss_runtime")
    wrapper = "import subprocess,sys; subprocess.run([sys.executable,'-m','harness_watcher_implementation','--config',sys.argv[1],'start'],check=True)"
    subprocess.run(
        [sys.executable, "-c", wrapper, str(owner_loss)],
        cwd=ROOT,
        check=True,
        timeout=20,
    )
    lost = wait_not_running(owner_loss, "owner-identity-lost")
    result = {
        "start": [start_code, start],
        "duplicate": [duplicate_code, duplicate],
        "live_status": [status_code, status],
        "stop": [stop_code, stop],
        "stopped_status": stopped,
        "owner_loss_status": lost,
        "live_has_pid_and_creation": bool(
            status.get("state", {}).get("watcher", {}).get("pid")
        )
        and isinstance(
            status.get("state", {}).get("watcher", {}).get("created_utc"), str
        )
        and ":" in status.get("state", {}).get("watcher", {}).get("created_utc", ""),
        "cooperative_stop_reason": status.get("state", {}).get("exit_reason") is None
        and stopped.get("state", {}).get("exit_reason") == "stop-requested",
        "owner_loss_reason": lost.get("state", {}).get("exit_reason")
        == "owner-identity-lost",
    }
    (DEST / "result.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
