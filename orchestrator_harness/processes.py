from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Sequence, cast

from .models import (
    ProcessBoundaryInventory,
    ProcessInfo,
    ProcessQuery,
    ProcessSnapshot,
    iso_utc,
    parse_utc,
)

WINDOWS_CREATE_NO_WINDOW = 0x08000000
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


def _windows_cim_identity_script(pid: int) -> str:
    # ``pid`` is converted to an integer before it is placed in the fixed CIM
    # filter, so this remains an argv-only, non-shell query.
    return f"""
$ErrorActionPreference='Stop'
@(Get-CimInstance Win32_Process -Filter 'ProcessId = {int(pid)}' | ForEach-Object {{
  [pscustomobject]@{{
    pid=[int]$_.ProcessId
    ppid=[int]$_.ParentProcessId
    name=[string]$_.Name
    command_line=[string]$_.CommandLine
    created_utc=if($_.CreationDate){{$_.CreationDate.ToUniversalTime().ToString('o')}}else{{$null}}
  }}
}}) | ConvertTo-Json -Compress -Depth 4
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
            creationflags=WINDOWS_CREATE_NO_WINDOW,
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


def windows_process_query(
    pid: int,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    timeout_seconds: float = 12.0,
) -> ProcessQuery:
    """Query one known Windows PID without taking a process inventory."""

    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return ProcessQuery(True, None, ("PID is invalid",))
    powershell = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    argv = [
        powershell,
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        _windows_cim_identity_script(pid),
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
            creationflags=WINDOWS_CREATE_NO_WINDOW,
        )
    except Exception as exc:
        return ProcessQuery(False, None, (f"CIM identity query failed: {exc}",))
    if completed.returncode != 0:
        return ProcessQuery(
            False,
            None,
            (
                f"CIM identity query returned {completed.returncode}: {completed.stderr.strip()}",
            ),
        )
    try:
        raw = json.loads(completed.stdout.lstrip("\ufeff") or "[]")
        if isinstance(raw, dict):
            raw = [raw]
        if not isinstance(raw, list):
            raise ValueError("CIM JSON root is not a list")
        if not raw:
            return ProcessQuery(True, None)
        item = raw[0]
        if not isinstance(item, dict):
            raise ValueError("CIM identity record is not an object")
        created = parse_utc(item.get("created_utc"))
        return ProcessQuery(
            True,
            ProcessInfo(
                pid=int(item["pid"]),
                ppid=int(item.get("ppid", 0)),
                name=str(item.get("name") or ""),
                command_line=str(item.get("command_line") or ""),
                created_utc=created,
            ),
            ("process creation time is unavailable",) if created is None else (),
        )
    except Exception as exc:
        return ProcessQuery(False, None, (f"invalid CIM identity output: {exc}",))


def _linux_boot_time() -> datetime:
    for line in Path("/proc/stat").read_text(encoding="ascii").splitlines():
        if line.startswith("btime "):
            return datetime.fromtimestamp(int(line.split()[1]), tz=timezone.utc)
    raise RuntimeError("/proc/stat has no btime")


def _linux_clock_ticks() -> int:
    sysconf = getattr(os, "sysconf", None)
    if not callable(sysconf):
        raise RuntimeError("os.sysconf is unavailable")
    read_sysconf = cast(Callable[[str], int], sysconf)
    return int(read_sysconf("SC_CLK_TCK"))


def _linux_process_query(
    pid: int,
    *,
    boot: datetime | None = None,
    ticks: int | None = None,
) -> ProcessQuery:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return ProcessQuery(True, None, ("PID is invalid",))
    try:
        boot = boot or _linux_boot_time()
        ticks = ticks or _linux_clock_ticks()
        entry = Path("/proc") / str(pid)
        stat_text = (entry / "stat").read_text(encoding="ascii")
        close = stat_text.rfind(")")
        fields = stat_text[close + 2 :].split()
        if close < 0 or len(fields) <= 19:
            raise ValueError("/proc stat record is incomplete")
        ppid = int(fields[1])
        process_group_id = int(fields[2])
        session_id = int(fields[3])
        start_ticks = int(fields[19])
        name = stat_text[stat_text.find("(") + 1 : close]
        raw_cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").strip()
        command = raw_cmd.decode("utf-8", errors="replace")
        created = boot + timedelta(seconds=start_ticks / ticks)
        return ProcessQuery(
            True,
            ProcessInfo(
                pid,
                ppid,
                name,
                command,
                created,
                process_group_id=process_group_id,
                session_id=session_id,
            ),
        )
    except (FileNotFoundError, ProcessLookupError):
        return ProcessQuery(True, None)
    except PermissionError as exc:
        return ProcessQuery(False, None, (f"/proc/{pid}: {exc}",))
    except Exception as exc:
        return ProcessQuery(False, None, (f"/proc/{pid}: {exc}",))


def targeted_process_query(
    pid: int,
    *,
    expected_parent_pid: int | None = None,
) -> ProcessQuery:
    """Query one known PID and optionally its parent identity directly."""

    if os.name == "nt":
        query = windows_process_query(pid)
    elif Path("/proc").is_dir():
        query = _linux_process_query(pid)
    else:
        return ProcessQuery(False, None, ("unsupported process platform",))
    if (
        expected_parent_pid is not None
        and query.parent_matches(expected_parent_pid) is False
    ):
        return ProcessQuery(
            query.complete,
            query.process,
            (*query.errors, f"PID {pid} parent does not match {expected_parent_pid}"),
        )
    return query


def linux_process_snapshot() -> ProcessSnapshot:
    errors: list[str] = []
    processes: list[ProcessInfo] = []
    try:
        boot = _linux_boot_time()
        ticks = _linux_clock_ticks()
    except Exception as exc:
        return ProcessSnapshot(False, (), (f"/proc setup failed: {exc}",), "linux-proc")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            query = _linux_process_query(int(entry.name), boot=boot, ticks=ticks)
            if query.process is not None:
                processes.append(query.process)
            if not query.complete:
                errors.extend(query.errors)
            elif query.process is None:
                # A directory listed in /proc disappeared before its stat could
                # be read.  The remaining records are useful, but this scan
                # cannot prove absence for any PID it did not observe.
                errors.append(f"{entry}: process disappeared during observation")
        except (FileNotFoundError, ProcessLookupError):
            errors.append(f"{entry}: process disappeared during observation")
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


def process_group_inventory(
    process_group_id: int | None,
    *,
    snapshot_provider: Callable[[], ProcessSnapshot] = process_snapshot,
    boundary_identity: str | None = None,
    session_id: int | None = None,
    root_pid: int | None = None,
    root_identity: ProcessInfo | None = None,
    controller_pid: int | None = None,
    owned_history: Sequence[ProcessInfo] = (),
) -> ProcessBoundaryInventory:
    """Return a complete subreaper ownership inventory or refuse.

    A process group is only one selector in the Linux boundary.  The
    controller also supplies the provider root, session, controller PID, and
    exact identities observed in earlier snapshots.  That history is what
    makes a descendant that leaves its original group and is later adopted by
    the subreaper remain owned.  Calling this function without that context is
    deliberately incomplete: group-only emptiness is not terminal evidence.
    """

    valid_group = process_group_id is None or (
        isinstance(process_group_id, int)
        and not isinstance(process_group_id, bool)
        and process_group_id > 0
    )
    valid_session = session_id is None or (
        isinstance(session_id, int)
        and not isinstance(session_id, bool)
        and session_id > 0
    )
    valid_root = root_pid is None or (
        isinstance(root_pid, int) and not isinstance(root_pid, bool) and root_pid > 0
    )
    if not valid_group or not valid_session or not valid_root:
        return ProcessBoundaryInventory(
            False,
            "linux-subreaper",
            boundary_identity,
            errors=("process boundary identity is invalid",),
            source="/proc",
        )
    if (
        root_pid is None
        or root_identity is None
        or root_identity.created_utc is None
        or (process_group_id is None and session_id is None)
    ):
        return ProcessBoundaryInventory(
            False,
            "linux-subreaper",
            boundary_identity,
            errors=("complete descendant/adoption boundary identity is unavailable",),
            source="/proc",
        )
    snapshot = snapshot_provider()
    for _ in range(2):
        if isinstance(snapshot, ProcessSnapshot) and snapshot.complete:
            break
        time.sleep(0.01)
        snapshot = snapshot_provider()
    if not isinstance(snapshot, ProcessSnapshot) or not snapshot.complete:
        return ProcessBoundaryInventory(
            False,
            "linux-subreaper",
            boundary_identity or f"pgid:{process_group_id}",
            errors=tuple(
                getattr(snapshot, "errors", ("process snapshot is incomplete",))
            ),
            source="/proc",
        )

    by_pid = snapshot.by_pid
    root_key = (root_identity.pid, iso_utc(root_identity.created_utc) or "")
    known_by_key: dict[tuple[int, str], ProcessInfo] = {}
    for item in tuple(owned_history) + (root_identity,):
        if item.created_utc is not None:
            known_by_key[(item.pid, iso_utc(item.created_utc) or "")] = item

    errors: list[str] = []
    current_root = by_pid.get(root_pid)
    if current_root is not None and current_root.created_utc is None:
        errors.append(f"provider root {root_pid} lacks a creation identity")
    elif (
        current_root is not None
        and (current_root.pid, iso_utc(current_root.created_utc) or "") != root_key
    ):
        errors.append(f"provider root {root_pid} creation identity was reused")

    selected: dict[tuple[int, str], ProcessInfo] = {}

    def select(item: ProcessInfo) -> None:
        if item.created_utc is None:
            errors.append(f"owned PID {item.pid} lacks a creation identity")
            return
        selected[(item.pid, iso_utc(item.created_utc) or "")] = item

    for item in snapshot.processes:
        if (
            process_group_id is not None and item.process_group_id == process_group_id
        ) or (session_id is not None and item.session_id == session_id):
            select(item)
        if (item.pid, iso_utc(item.created_utc) or "") in known_by_key:
            select(item)

    # Descendant closure is calculated only through a currently observed
    # parent identity.  A child seen for the first time after its parent has
    # disappeared cannot be safely attributed, so it remains an explicit
    # incomplete observation rather than being silently treated as unrelated.
    changed = True
    while changed:
        changed = False
        selected_pids = {item.pid for item in selected.values()}
        for item in snapshot.processes:
            if item.pid in selected_pids:
                continue
            if item.ppid not in selected_pids:
                continue
            parent = by_pid.get(item.ppid)
            if (
                parent is None
                or (parent.pid, iso_utc(parent.created_utc) or "") not in selected
            ):
                errors.append(
                    f"descendant PID {item.pid} has no exact owned parent observation"
                )
                continue
            before = len(selected)
            select(item)
            changed = len(selected) != before

    if controller_pid is not None:
        for item in snapshot.processes:
            key = (item.pid, iso_utc(item.created_utc) or "")
            if item.ppid != controller_pid:
                continue
            if key in known_by_key:
                select(item)
            else:
                errors.append(
                    f"adopted PID {item.pid} is outside the known ownership history"
                )

    # A process still claiming the provider root as parent after the root has
    # disappeared must have been captured in history before adoption.  Refuse
    # the snapshot if it was not, rather than losing a daemonizing child.
    if current_root is None:
        for item in snapshot.processes:
            if (
                item.ppid == root_pid
                and (item.pid, iso_utc(item.created_utc) or "") not in known_by_key
            ):
                errors.append(
                    f"unobserved descendant PID {item.pid} cannot be attributed after root exit"
                )

    members = tuple(sorted(selected.values(), key=lambda item: item.pid))
    observed: dict[tuple[int, str], ProcessInfo] = dict(known_by_key)
    for item in members:
        if item.created_utc is not None:
            observed[(item.pid, iso_utc(item.created_utc) or "")] = item
    if errors:
        return ProcessBoundaryInventory(
            False,
            "linux-subreaper",
            boundary_identity or f"pgid:{process_group_id}:sid:{session_id}",
            processes=members,
            observed_processes=tuple(
                sorted(
                    observed.values(),
                    key=lambda item: (item.pid, iso_utc(item.created_utc) or ""),
                )
            ),
            errors=tuple(errors),
            source="/proc",
        )
    return ProcessBoundaryInventory(
        True,
        "linux-subreaper",
        boundary_identity or f"pgid:{process_group_id}:sid:{session_id}",
        processes=members,
        observed_processes=tuple(
            sorted(
                observed.values(),
                key=lambda item: (item.pid, iso_utc(item.created_utc) or ""),
            )
        ),
        source="/proc",
    )
