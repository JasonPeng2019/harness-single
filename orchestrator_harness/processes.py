from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from .models import ProcessInfo, ProcessSnapshot, parse_utc


WINDOWS_CIM_SCRIPT = r"""
$ErrorActionPreference='Stop'
@(Get-CimInstance Win32_Process | ForEach-Object {
  [pscustomobject]@{
    pid=[int]$_.ProcessId
    ppid=[int]$_.ParentProcessId
    name=[string]$_.Name
    command_line=[string]$_.CommandLine
    created_utc=if($_.CreationDate){$_.CreationDate.ToUniversalTime().ToString('o')}else{$null}
  }
}) | ConvertTo-Json -Compress -Depth 4
""".strip()


def windows_process_snapshot(
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    timeout_seconds: float = 12.0,
) -> ProcessSnapshot:
    powershell = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    argv = [
        powershell,
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        WINDOWS_CIM_SCRIPT,
    ]
    try:
        completed = runner(
            argv,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
        )
    except Exception as exc:
        return ProcessSnapshot(
            False, (), (f"CIM invocation failed: {exc}",), "windows-cim"
        )
    if completed.returncode != 0:
        return ProcessSnapshot(
            False,
            (),
            (f"CIM returned {completed.returncode}: {completed.stderr.strip()}",),
            "windows-cim",
        )
    try:
        raw = json.loads(completed.stdout.lstrip("\ufeff") or "[]")
        if isinstance(raw, dict):
            raw = [raw]
        if not isinstance(raw, list):
            raise ValueError("CIM JSON root is not a list")
        processes = []
        missing_time = 0
        for item in raw:
            if not isinstance(item, dict):
                continue
            created = parse_utc(item.get("created_utc"))
            if created is None:
                missing_time += 1
            processes.append(
                ProcessInfo(
                    pid=int(item["pid"]),
                    ppid=int(item.get("ppid", 0)),
                    name=str(item.get("name") or ""),
                    command_line=str(item.get("command_line") or ""),
                    created_utc=created,
                )
            )
        errors = (
            (f"{missing_time} process records lack creation time",)
            if missing_time
            else ()
        )
        return ProcessSnapshot(
            True, tuple(sorted(processes, key=lambda p: p.pid)), errors, "windows-cim"
        )
    except Exception as exc:
        return ProcessSnapshot(
            False, (), (f"invalid CIM output: {exc}",), "windows-cim"
        )


def _linux_boot_time() -> datetime:
    for line in Path("/proc/stat").read_text(encoding="ascii").splitlines():
        if line.startswith("btime "):
            return datetime.fromtimestamp(int(line.split()[1]), tz=timezone.utc)
    raise RuntimeError("/proc/stat has no btime")


def linux_process_snapshot() -> ProcessSnapshot:
    errors: list[str] = []
    processes: list[ProcessInfo] = []
    try:
        boot = _linux_boot_time()
        ticks = int(os.sysconf("SC_CLK_TCK"))
    except Exception as exc:
        return ProcessSnapshot(False, (), (f"/proc setup failed: {exc}",), "linux-proc")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat_text = (entry / "stat").read_text(encoding="ascii")
            close = stat_text.rfind(")")
            fields = stat_text[close + 2 :].split()
            ppid = int(fields[1])
            start_ticks = int(fields[19])
            name = stat_text[stat_text.find("(") + 1 : close]
            raw_cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").strip()
            command = raw_cmd.decode("utf-8", errors="replace")
            created = boot + timedelta(seconds=start_ticks / ticks)
            processes.append(ProcessInfo(int(entry.name), ppid, name, command, created))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as exc:
            errors.append(f"{entry}: {exc}")
        except Exception as exc:
            errors.append(f"{entry}: {exc}")
    return ProcessSnapshot(
        complete=not errors,
        processes=tuple(sorted(processes, key=lambda p: p.pid)),
        errors=tuple(errors),
        provider="linux-proc",
    )


def process_snapshot() -> ProcessSnapshot:
    if os.name == "nt":
        return windows_process_snapshot()
    if Path("/proc").is_dir():
        return linux_process_snapshot()
    return ProcessSnapshot(False, (), ("unsupported process platform",), "unsupported")
