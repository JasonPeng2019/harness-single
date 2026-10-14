"""Board-free lifecycle proof for duplicate start, exact identity, stop, and owner loss."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from orchestrator_harness.processes import identity_exit_proven

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / "runtime" / "merge-validation" / "watcher-canary" / uuid.uuid4().hex


def config(runtime: str) -> Path:
    path = DEST / f"{runtime}.json"
    path.write_text(
        json.dumps(
            {
                "runtime_root": str((DEST / runtime).relative_to(ROOT)),
                "observed_log_roots": [
                    str((DEST / "empty.log").relative_to(ROOT))
                ],
                "poll_interval_seconds": 2,
                "evaluator_enabled": False,
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
        watcher = (state.get("state") or {}).get("watcher")
        gone = (isinstance(watcher, dict) and identity_exit_proven(watcher.get("pid"), watcher.get("created_utc")))
        if not state["running"] and not state.get("identity_unproven") and gone and (
            reason is None or state.get("state", {}).get("exit_reason") == reason
        ):
            return state
        time.sleep(0.1)
    raise RuntimeError("watcher did not stop cooperatively")


def _run_canary() -> None:
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
    if (any(code != 0 for code in (start_code, duplicate_code, status_code, stop_code))
            or duplicate.get("result") != "already-started"
            or not all(result[key] for key in ("live_has_pid_and_creation", "cooperative_stop_reason", "owner_loss_reason"))):
        raise RuntimeError("native watcher lifecycle assertions failed; inspect retained result")


def main() -> None:
    try:
        _run_canary()
    finally:
        proof = []
        failures = []
        for name in ("ordinary_runtime", "owner_loss_runtime"):
            path = DEST / f"{name}.json"
            if not path.exists():
                continue
            try:
                _, current = call(path, "status")
                state = current.get("state")
                if state is None:
                    proof.append({"config": str(path), "never_published": True})
                    continue
                watcher = state.get("watcher")
                if not isinstance(watcher, dict):
                    raise RuntimeError("published startup has no provable watcher identity")
                if not identity_exit_proven(watcher.get("pid"), watcher.get("created_utc")):
                    code, requested = call(path, "stop")
                    if code != 0:
                        raise RuntimeError(f"cooperative stop refused: {requested}")
                    current = wait_not_running(path)
                gone = identity_exit_proven(watcher.get("pid"), watcher.get("created_utc"))
                proof.append({"config": str(path), "watcher": watcher, "exact_exit_proven": gone,
                              "terminal": current.get("state")})
                if not gone:
                    raise RuntimeError("exact watcher exit remains unproven")
            except Exception as exc:
                failures.append({"config": str(path), "error": str(exc)})
        DEST.mkdir(parents=True, exist_ok=True)
        (DEST / "cleanup.json").write_text(json.dumps({"proof": proof, "failures": failures}, indent=2), encoding="utf-8")
        if failures:
            raise RuntimeError("canary cleanup is unproven; retained fixtures were not deleted")


if __name__ == "__main__":
    main()
