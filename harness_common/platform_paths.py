"""Discover Linux kernel mounts without assuming their installation paths.

findmnt's explicit JSON columns are the OS mount-table interface. Discovery
failures are unknown state, never proof that a process or cgroup is absent.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=8)
def _mount_candidates(filesystem: str, configured: str | None) -> tuple[Path, ...]:
    executable = shutil.which("findmnt")
    if executable is None:
        raise RuntimeError("Linux mount discovery requires findmnt on PATH")
    argv = [executable, "--json", "--list", "--types", filesystem, "--output", "TARGET,FSTYPE"]
    if configured:
        if not Path(configured).is_absolute():
            raise RuntimeError("configured kernel mount must be absolute")
        argv.extend(("--mountpoint", configured))
    completed = subprocess.run(argv, capture_output=True, text=True, timeout=5, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"cannot discover {filesystem} mount: {completed.stderr.strip()}")
    try:
        records = json.loads(completed.stdout)["filesystems"]
        roots = tuple(
            Path(record["target"])
            for record in records
            if record["fstype"] == filesystem and Path(record["target"]).is_absolute()
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("invalid kernel mount discovery output") from exc
    if not roots:
        raise RuntimeError(f"no {filesystem} mount is available")
    return roots


def procfs_root() -> Path:
    roots = _mount_candidates("proc", os.environ.get("HARNESS_PROCFS_ROOT"))
    valid: list[Path] = []
    for root in roots:
        try:
            record = (root / "self" / "stat").read_text(encoding="ascii")
            if int(record.split(" ", 1)[0]) == os.getpid():
                valid.append(root)
        except (OSError, ValueError):
            continue
    if len(valid) != 1:
        raise RuntimeError("process filesystem is unavailable or ambiguous; configure HARNESS_PROCFS_ROOT")
    return valid[0]


def cgroup_root() -> Path:
    roots = _mount_candidates("cgroup2", os.environ.get("HARNESS_CGROUP_ROOT"))
    valid = [root for root in roots if (root / "cgroup.controllers").is_file()]
    if len(valid) != 1:
        raise RuntimeError("cgroup-v2 mount is unavailable or ambiguous; configure HARNESS_CGROUP_ROOT")
    return valid[0]
