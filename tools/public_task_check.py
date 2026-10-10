#!/usr/bin/env python3
"""Run public ZSTD commands against this harness's ROOT or one native lane.

Copies task-visible inputs only into a pinned, disposable, offline container.
This is a deterministic build/test bridge; it never launches model workers.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path

IMAGE = "swe-marathon-zstd-decoder:lf-20260925"
IMAGE_ID = "sha256:3a43d4f3658c4a21115c518937b692692e9e721d3b6e4a3205972917b8bc2e8a"
TASK_INPUTS = ("src", "test_artifacts", "rfc8878.txt", "test.sh", "timer.sh")


def validate_worktree(harness: Path, selected: Path) -> Path:
    config = json.loads((harness / "harness-config.json").read_text(encoding="utf-8"))
    root = Path(config["root_workspace"]).resolve(strict=True)
    worktree = selected.resolve(strict=True)
    lane_parent = root / ".harness-runtime" / "worktrees"
    is_root = worktree == root and (root / ".git").is_dir()
    is_lane = worktree.parent.parent == lane_parent and (worktree / ".git").is_file()
    if not worktree.is_dir() or not (is_root or is_lane):
        raise ValueError("select this harness's ROOT or one exact native lane")
    common = subprocess.check_output(
        ["git", "-C", str(worktree), "rev-parse", "--path-format=absolute", "--git-common-dir"],
        text=True,
    ).strip()
    if Path(common).resolve() != (root / ".git").resolve():
        raise ValueError("worktree Git identity does not belong to this ROOT")
    if (worktree / ".codex" / "auth.json").exists():
        raise ValueError("credential file found in selected worktree")
    for name in TASK_INPUTS:
        path = worktree / name
        if not path.exists() or path.is_symlink() or not path.resolve().is_relative_to(worktree):
            raise ValueError(f"missing or external task-visible input: {name}")
        if path.is_dir():
            for child in path.rglob("*"):
                if child.is_symlink() or not child.resolve().is_relative_to(worktree):
                    raise ValueError("task input contains an external link")
    return worktree


def run_public_command(worktree: Path, command: list[str], *, timeout: float = 120,
                       cancel=None) -> dict:
    """Execute a task command with no host mounts or worker-controlled Docker flags."""
    image_id = subprocess.check_output(
        ["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"], text=True, timeout=20,
    ).strip()
    if image_id != IMAGE_ID:
        raise ValueError("pinned task image is missing or changed")
    created = subprocess.check_output(
        ["docker", "create", "--network", "none", "--cpus", "4", "--memory", "16g",
         "--pids-limit", "512", IMAGE, "bash", "-lc", "sleep 1200"], text=True, timeout=20,
    ).strip()
    if not created or len(created) != 64 or any(char not in "0123456789abcdef" for char in created):
        raise ValueError("Docker returned an invalid container identity")
    result = {"image_id": image_id, "container": created, "command": command,
              "stdout": "", "stderr": "", "exit_code": None, "cleanup_proven": False}
    try:
        subprocess.run(["docker", "start", created], capture_output=True, check=True, timeout=20)
        inputs = [worktree / name for name in TASK_INPUTS]
        scratch = worktree / "scratch"
        if scratch.is_dir():
            inputs.append(scratch)
        for path in inputs:
            subprocess.run(["docker", "cp", str(path), f"{created}:/app/"],
                           capture_output=True, check=True, timeout=30)
        with tempfile.TemporaryDirectory(prefix="public-check-output-") as output:
            stdout_path, stderr_path = Path(output) / "stdout", Path(output) / "stderr"
            with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                process = subprocess.Popen(
                    ["docker", "exec", "--workdir", "/app", created, *command],
                    stdout=stdout, stderr=stderr,
                )
                deadline = time.monotonic() + timeout
                try:
                    while process.poll() is None:
                        if cancel is not None and cancel.is_set():
                            result["exit_code"] = 125
                            result["error"] = "public check canceled during runtime shutdown"
                            break
                        if time.monotonic() >= deadline:
                            result["exit_code"] = 124
                            result["error"] = "public check exceeded its time limit"
                            break
                        time.sleep(0.1)
                finally:
                    if process.poll() is None:
                        process.terminate()
                    process.wait(timeout=10)
                if result["exit_code"] is None:
                    result["exit_code"] = process.returncode
            result["stdout"] = stdout_path.read_text(encoding="utf-8", errors="replace")
            result["stderr"] = stderr_path.read_text(encoding="utf-8", errors="replace")
    except (OSError, subprocess.SubprocessError) as exc:
        result["error"] = str(exc)
        result["exit_code"] = 2
    finally:
        try:
            removed = subprocess.run(["docker", "rm", "-f", created], capture_output=True, timeout=20)
            result["cleanup_proven"] = removed.returncode == 0
        except (OSError, subprocess.SubprocessError):
            result["cleanup_proven"] = False
        if not result["cleanup_proven"]:
            result["error"] = "cannot prove disposable container cleanup"
            result["exit_code"] = 2
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worktree", required=True, type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise ValueError("a public build/test command is required")
    harness = Path(__file__).resolve().parent.parent
    worktree = validate_worktree(harness, args.worktree)
    scratch = worktree / "scratch"
    if scratch.is_dir() and (scratch.is_symlink() or any(
        child.is_symlink() or not child.resolve().is_relative_to(worktree)
        for child in scratch.rglob("*")
    )):
        raise ValueError("scratch contains an external link")
    result = run_public_command(worktree, command)
    result["worktree"] = str(worktree)
    print(json.dumps({key: value for key, value in result.items() if key not in {"stdout", "stderr"}},
                     default=str), flush=True)
    print(result["stdout"], end="", flush=True)
    import sys
    print(result["stderr"], end="", file=sys.stderr, flush=True)
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
