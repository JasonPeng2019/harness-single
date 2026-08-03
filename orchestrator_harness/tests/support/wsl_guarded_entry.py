from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Outermost cleanup guard for one WSL real-agent test."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--driver", required=True)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{32}", args.run_id):
        raise RuntimeError("run-id must be exactly 32 lowercase hexadecimal characters")
    arguments = args.arguments
    if arguments and arguments[0] == "--":
        arguments = arguments[1:]
    cgroup = Path("/sys/fs/cgroup") / f"orchestrator-harness-{args.run_id}"
    namespace = f"oh-{args.run_id[:8]}"
    temporary = Path(f"/tmp/orchestrator-harness-real-agent-{args.run_id}")
    try:
        return subprocess.run(
            [
                "/usr/bin/python3",
                args.driver,
                "--run-id",
                args.run_id,
                *arguments,
            ]
        ).returncode
    finally:
        if cgroup.exists():
            try:
                if any(cgroup.rglob("cgroup.procs")):
                    (cgroup / "cgroup.kill").write_text("1\n", encoding="ascii")
                    deadline = time.monotonic() + 10
                    while (
                        cgroup / "cgroup.procs"
                    ).read_text().strip() and time.monotonic() < deadline:
                        time.sleep(0.05)
            except OSError:
                pass
            try:
                cgroup.rmdir()
            except OSError:
                pass
        subprocess.run(
            ["ip", "netns", "del", namespace],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        shutil.rmtree(temporary, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
