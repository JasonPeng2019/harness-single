from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    cgroup = Path("/sys/fs/cgroup") / f"orchestrator-harness-{args.run_id}"
    temporary = Path(f"/tmp/orchestrator-harness-real-agent-{args.run_id}")
    cgroup.mkdir()
    (cgroup / "pids.max").write_text("8\n", encoding="ascii")
    temporary.mkdir()
    (temporary / "auth.json").write_text("synthetic cleanup sentinel\n")
    (cgroup / "cgroup.procs").write_text(f"{os.getpid()}\n", encoding="ascii")
    subprocess.Popen(
        [
            "/usr/bin/python3",
            "-c",
            "import time; time.sleep(120)",
            f"orchestrator-cleanup-{args.run_id}",
        ]
    )
    # Leave the child behind in the dedicated cgroup while this fixture exits.
    Path("/sys/fs/cgroup/cgroup.procs").write_text(f"{os.getpid()}\n", encoding="ascii")
    return 7


if __name__ == "__main__":
    raise SystemExit(main())
