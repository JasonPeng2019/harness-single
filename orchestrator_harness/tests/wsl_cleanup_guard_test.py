from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import subprocess
import uuid

from orchestrator_harness.tests.real_agent_test import (
    DEFAULT_DISTRO,
    SUPPORT,
    wsl_path,
)


def main() -> int:
    run_id = uuid.uuid4().hex
    completed = subprocess.run(
        [
            "wsl.exe",
            "-d",
            DEFAULT_DISTRO,
            "-u",
            "root",
            "--",
            "python3",
            wsl_path(DEFAULT_DISTRO, SUPPORT / "wsl_guarded_entry.py"),
            "--run-id",
            run_id,
            "--driver",
            wsl_path(DEFAULT_DISTRO, SUPPORT / "wsl_cleanup_fixture_driver.py"),
        ]
    )
    if completed.returncode != 7:
        raise AssertionError(
            f"fixture exit code was {completed.returncode}, expected 7"
        )
    probe = subprocess.run(
        [
            "wsl.exe",
            "-d",
            DEFAULT_DISTRO,
            "-u",
            "root",
            "--",
            "sh",
            "-c",
            " && ".join(
                [
                    f"test ! -e /sys/fs/cgroup/orchestrator-harness-{run_id}",
                    f"test ! -e /tmp/orchestrator-harness-real-agent-{run_id}",
                    f"! ps -eo args | grep 'orchestrator-cleanup-{run_id}' | grep -v grep",
                ]
            ),
        ]
    )
    if probe.returncode:
        raise AssertionError(
            "outer guard left cgroup, credential temp, or descendant behind"
        )
    print("WSL cleanup guard: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
