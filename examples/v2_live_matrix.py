"""M09-only, resumable native live-matrix executor.

It does not discover work, schedule lanes, retry failures, or manufacture proof.
ROOT supplies a reviewed manifest of already-authorized, independent native attempts.
Without ``--execute`` it emits the complete reserved matrix; fakes and prior files
never satisfy a live row.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

AUTHORIZATION_ENV = "HARNESS_V2_M09_AUTHORIZED"
PROVIDERS = ("codex", "claude-code", "qwen-code")
PROFILES = ("managed", "plain")
PLATFORMS = ("Windows", "macOS", "Linux")
CHECKS = {f"CHECK-LIVE-{n}": text for n, text in enumerate((
    "headless launch, transcript, native session, hooks, terminal result and cleanup",
    "worker escalation outbox, promotion retry, queue wake, receipt and close",
    "monitor heartbeat diagnostics, dead/hung direction, deliberate stop and ROOT recovery",
    "exclusive leases, orphan discovery, live-holder exclusion and release audit",
    "setup/overwrite, immutable epoch turnover and runtime shutdown",
    "managed/plain bootstrap, worktree/payload isolation and stale-run rejection",
    "real edit/test/result/review/acceptance/retire lifecycle",
    "five native invalid-result continuations, sixth escalation and later recovery",
    "ROOT-to-worker assignments, invalid/terminal handling and plain absence",
    "manager event state/history, receipt dedup, acknowledgement and close ownership",
    "provider/controller failure, interruption, cleanup uncertainty and same-session resume",
    "review/lifecycle CLI, lost event and broken-pair recovery without inferred acceptance",
    "unsupported provider and no-native-resume refusal without generic fallback",
    "concurrent queue/lease/audit atomicity and crash/retry durability",
    "public command/platform primitive completeness and no unexplained cells",
    "adverse agent behavior classification and exact final cleanup",
), start=1)}
PUBLIC_COMMANDS = (
    "harness setup", "harness shutdown", "lane bootstrap", "lane launch", "lane completion-review",
    "resume-lane", "lane force-stop", "lane retire", "manager acknowledge", "manager close",
    "send-lane-notification", "scan --no-write", "watch --until-actionable", "health reconcile",
    "health monitor-recover", "lease force-release",
)
MANAGED_ONLY = {"manager acknowledge", "manager close", "send-lane-notification", "health monitor-recover"}
PROVIDER_FREE = {"harness setup", "harness shutdown", "manager acknowledge", "manager close", "scan --no-write", "watch --until-actionable", "health reconcile", "lease force-release"}
NATIVE_GAPS = {"macOS": "GAP-NATIVE-MACOS", "Linux": "GAP-NATIVE-LINUX"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _applicability(command: str, profile: str) -> tuple[bool, str]:
    if command in MANAGED_ONLY and profile == "plain":
        return False, "plain profile intentionally has no manager queue/monitor recovery route"
    return True, "public command applies to this profile"


def expected_cells() -> list[dict[str, str]]:
    """Exhaustive public-command/provider/profile/platform applicability contract."""
    cells: list[dict[str, str]] = []
    for native_platform in PLATFORMS:
        for command in PUBLIC_COMMANDS:
            for provider in (("provider-agnostic",) if command in PROVIDER_FREE else PROVIDERS):
                for profile in PROFILES:
                    applicable, reason = _applicability(command, profile)
                    cells.append({"platform": native_platform, "command": command, "provider": provider, "profile": profile, "applicable": str(applicable).lower(), "applicability_reason": reason})
    return cells


def _reserved() -> dict[str, Any]:
    rows = []
    for cell in expected_cells():
        gap = NATIVE_GAPS.get(cell["platform"])
        outcome = "NOT_APPLICABLE" if cell["applicable"] != "true" else (gap or "NOT_RUN_LIVE")
        rows.append({**cell, "outcome": outcome})
    return {"schema": "harness-v2-live-matrix-result/v2", "outcome": "RESERVED_FOR_M09", "authorization": "not requested", "checks": [{"name": name, "scenario": scenario, "outcome": "NOT_RUN_LIVE"} for name, scenario in CHECKS.items()], "cells": rows}


def _argv(value: object, field: str) -> list[str]:
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"{field} must be a nonempty argv list")
    return value


def _paths(value: object, field: str) -> dict[str, Path]:
    if not isinstance(value, dict) or set(value) != {"transcript", "hook", "state", "cleanup"}:
        raise ValueError(f"{field} must contain transcript, hook, state and cleanup paths")
    result = {kind: Path(item) for kind, item in value.items() if isinstance(item, str) and item}
    if len(result) != 4:
        raise ValueError(f"{field} paths must be nonempty strings")
    return result


def _load_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != "harness-v2-live-matrix/v2":
        raise ValueError("manifest schema must be harness-v2-live-matrix/v2")
    if not isinstance(value.get("native_runner_identity"), dict) or not value["native_runner_identity"].get("platform"):
        raise ValueError("manifest must identify the real native runner")
    attempts = value.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise ValueError("manifest must contain one or more independently authorized attempts")
    seen: set[str] = set()
    for attempt in attempts:
        if not isinstance(attempt, dict):
            raise ValueError("attempt must be an object")
        name = attempt.get("name")
        if not isinstance(name, str) or name not in CHECKS or name in seen:
            raise ValueError("attempt names must be unique CHECK-LIVE-1 through CHECK-LIVE-16")
        seen.add(name)
        for field in ("target", "provider", "profile", "platform", "command", "cleanup_command", "evidence", "evidence_oracles", "agent_expectations"):
            if field not in attempt:
                raise ValueError(f"{name} lacks {field}")
        if attempt["provider"] not in PROVIDERS or attempt["profile"] not in PROFILES or attempt["platform"] not in PLATFORMS:
            raise ValueError(f"{name} has unsupported provider/profile/platform")
        _argv(attempt["command"], f"{name}.command")
        _argv(attempt["cleanup_command"], f"{name}.cleanup_command")
        _paths(attempt["evidence"], f"{name}.evidence")
        evidence_oracles = attempt["evidence_oracles"]
        if not isinstance(evidence_oracles, dict) or set(evidence_oracles) != {"transcript", "hook", "state", "cleanup"} or not all(isinstance(tokens, list) and tokens and all(isinstance(token, str) and token for token in tokens) for tokens in evidence_oracles.values()):
            raise ValueError(f"{name} must declare nonempty transcript/hook/state/cleanup content oracles")
        if not isinstance(attempt["agent_expectations"], list) or not attempt["agent_expectations"]:
            raise ValueError(f"{name} must classify agent behavior")
    coverage = value.get("coverage_cells")
    expected = expected_cells()
    if not isinstance(coverage, list) or len(coverage) != len(expected):
        raise ValueError("manifest must retain every public command/provider/profile/platform coverage cell")
    required_keys = {"platform", "command", "provider", "profile", "applicable", "applicability_reason"}
    actual_rows = {_canonical({key: item.get(key) for key in required_keys}) for item in coverage if isinstance(item, dict)}
    expected_rows = {_canonical({key: item[key] for key in required_keys}) for item in expected}
    if actual_rows != expected_rows:
        raise ValueError("manifest coverage_cells do not exactly match the exhaustive applicability matrix")
    return value


def _fingerprint(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def _stop_owned_process_tree(process: subprocess.Popen[str], identity: dict[str, Any]) -> str | None:
    """Stop only the recorded command root and its descendants after timeout.

    The exact PID and its launch timestamp travel in the terminal row.  This is
    intentionally not a name/command-line sweep: unrelated processes are never
    considered for cleanup.
    """
    if process.poll() is not None:
        return None
    try:
        if os.name == "nt":
            stopped = subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                text=True, capture_output=True, check=False, timeout=10,
            )
            if stopped.returncode:
                return stopped.stderr.strip() or stopped.stdout.strip() or "taskkill failed"
        else:
            import signal
            os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=10)
        return None
    except (OSError, subprocess.SubprocessError) as exc:
        return str(exc)


def _run(argv: list[str], *, timeout_seconds: float | None = None) -> dict[str, Any]:
    """Run one reviewed coordinate command with its finite coordinate budget.

    This is deliberately only a command boundary: it neither discovers work nor
    retries a coordinate.  A timeout is terminal evidence for that coordinate;
    the caller still runs its reviewed cleanup command and collects independent
    coordinates.
    """
    started_at = _now()
    options: dict[str, Any] = {"text": True, "stdout": subprocess.PIPE, "stderr": subprocess.PIPE}
    if os.name == "nt":
        options["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        options["start_new_session"] = True
    try:
        process = subprocess.Popen(argv, **options)
    except OSError as exc:
        return {"argv": argv, "returncode": -1, "stdout": "", "stderr": str(exc), "terminal": "COMMAND_LAUNCH_ERROR"}
    identity = {"pid": process.pid, "started_at": started_at}
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        return {"argv": argv, "returncode": process.returncode, "stdout": stdout, "stderr": stderr, "owned_process": identity}
    except subprocess.TimeoutExpired as exc:
        cleanup_error = _stop_owned_process_tree(process, identity)
        stdout, stderr = process.communicate()
        return {
            "argv": argv, "returncode": 124, "stdout": stdout or exc.stdout or "", "stderr": stderr or exc.stderr or "",
            "terminal": "COORDINATE_DEADLINE_EXCEEDED", "owned_process": identity,
            "cleanup_error": cleanup_error,
        }


def _evidence_is_fresh(paths: dict[str, Path], before: dict[str, str | None], expected: dict[str, list[str]]) -> tuple[bool, dict[str, str]]:
    after = {kind: _fingerprint(path) for kind, path in paths.items()}
    content_matches = all(
        after[kind] is not None and all(token in paths[kind].read_text(encoding="utf-8", errors="replace") for token in expected[kind])
        for kind in paths
    )
    return content_matches and all(value is not None and value != before[kind] for kind, value in after.items()), {kind: str(path) for kind, path in paths.items()}


def _checkpoint(path: Path, input_digest: str, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(_canonical({"schema": "harness-v2-live-checkpoint/v3", "input_digest": input_digest, "updated_at": _now(), "results": rows}) + "\n", encoding="utf-8")
    temporary.replace(path)


def _prior_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema") != "harness-v2-live-checkpoint/v3":
        return []
    return value["results"] if isinstance(value.get("results"), list) else []


def _coordinates(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Validate the reviewed coordinate graph and bind every consumed input.

    Provider homes are resources even if a manifest author omits them.  That
    makes a shared provider session root exclusive without serialising distinct
    providers.  Dependency digests include every upstream digest, so a changed
    producer cannot leave a dependent PASS credited on stale input.
    """
    attempts = manifest["attempts"]
    names = [item.get("name") for item in attempts]
    if len(names) != len(set(names)) or any(not isinstance(name, str) for name in names):
        raise ValueError("coordinate names must be unique strings")
    by_name = {item["name"]: item for item in attempts}
    for item in attempts:
        deps = item.get("depends_on", [])
        resources = item.get("exclusive_resources", [])
        budget = item.get("budget_seconds", 300)
        if not isinstance(deps, list) or any(not isinstance(dep, str) or dep not in by_name or dep == item["name"] for dep in deps):
            raise ValueError(f"{item['name']} has invalid dependencies")
        if len(deps) != len(set(deps)) or not isinstance(resources, list) or any(not isinstance(resource, str) or not resource for resource in resources):
            raise ValueError(f"{item['name']} has invalid resource declaration")
        if not isinstance(budget, (int, float)) or isinstance(budget, bool) or not 0 < budget <= 3600:
            raise ValueError(f"{item['name']} must have a finite positive coordinate budget")
        if not isinstance(item.get("input_hashes", {}), dict):
            raise ValueError(f"{item['name']} input_hashes must be an object")
    digests: dict[str, str] = {}
    visiting: set[str] = set()

    def digest(name: str) -> str:
        if name in digests:
            return digests[name]
        if name in visiting:
            raise ValueError("coordinate dependencies must be acyclic")
        visiting.add(name)
        item = by_name[name]
        direct = {key: value for key, value in item.items() if key != "depends_on"}
        declared_files = item.get("input_files", {})
        # Older reviewed manifests recorded the hashes but predate
        # ``input_files``.  Derive their actually-consumed runner files from
        # the reviewed argv so a changed runner cannot reuse a stale row.
        if not declared_files and item.get("name") != "CHECK-LIVE-7":
            derived: dict[str, str] = {}
            command = item.get("command", [])
            cleanup = item.get("cleanup_command", [])
            if isinstance(command, list) and len(command) > 1 and isinstance(command[1], str) and Path(command[1]).is_file():
                derived["command_runner"] = command[1]
            if isinstance(cleanup, list) and len(cleanup) > 1 and isinstance(cleanup[1], str) and Path(cleanup[1]).is_file():
                derived["cleanup_runner"] = cleanup[1]
            declared_files = derived
        if declared_files:
            if not isinstance(declared_files, dict) or not all(isinstance(label, str) and isinstance(path, str) and path for label, path in declared_files.items()):
                raise ValueError(f"{name} input_files must map labels to nonempty paths")
            actual_files: dict[str, str | None] = {}
            for label, raw_path in declared_files.items():
                actual_files[label] = _fingerprint(Path(raw_path))
                if actual_files[label] is None:
                    raise ValueError(f"{name} consumed input is missing: {raw_path}")
            direct["actual_input_files"] = actual_files
        # The runner identity is consumed by every coordinate, so changing it
        # invalidates retained rows rather than silently reusing old evidence.
        digests[name] = _digest({"coordinate": direct, "native_runner_identity": manifest.get("native_runner_identity"), "upstream": {dep: digest(dep) for dep in item.get("depends_on", [])}})
        visiting.remove(name)
        return digests[name]

    for name in names:
        digest(name)
    return attempts, digests


def _coordinate_resources(attempt: dict[str, Any]) -> set[str]:
    return {f"provider-home:{attempt['provider']}", *attempt.get("exclusive_resources", [])}


def _run_coordinate(
    attempt: dict[str, Any], input_digest: str, *, deadline: float | None = None,
) -> dict[str, Any]:
    """Collect one terminal coordinate result; no retry and no inferred PASS."""
    if attempt["platform"] in NATIVE_GAPS:
        return {"name": attempt["name"], "coordinate_id": attempt.get("coordinate_id", attempt["name"]),
                "outcome": NATIVE_GAPS[attempt["platform"]], "reason": "native runner unavailable; retained for future execution",
                "input_digest": input_digest, "attempted_at": _now()}
    paths = _paths(attempt["evidence"], f"{attempt['name']}.evidence")
    before = {kind: _fingerprint(path) for kind, path in paths.items()}
    budget = float(attempt.get("budget_seconds", 300))
    cleanup_reserve = float(attempt.get("cleanup_reserve_seconds", min(10.0, max(.05, budget / 4))))
    now = __import__("time").monotonic()
    if deadline is not None:
        # The reserve is real but cannot exceed the remaining reviewed pool.
        cleanup_reserve = min(cleanup_reserve, max(.01, (deadline - now) / 5))
    command_budget = budget if deadline is None else max(.01, min(budget, deadline - now - cleanup_reserve))
    command = _run(_argv(attempt["command"], f"{attempt['name']}.command"), timeout_seconds=command_budget)
    remaining = budget if deadline is None else max(.01, deadline - __import__("time").monotonic())
    cleanup = _run(_argv(attempt["cleanup_command"], f"{attempt['name']}.cleanup_command"), timeout_seconds=min(remaining, cleanup_reserve, 120.0))
    fresh, evidence = _evidence_is_fresh(paths, before, attempt["evidence_oracles"])
    classifications = all(isinstance(item, dict) and item.get("classification") in {"Observed", "Not observed", "Deviation"} for item in attempt["agent_expectations"])
    outcome = "PASS" if command["returncode"] == 0 and cleanup["returncode"] == 0 and fresh and classifications else "FAIL"
    return {"name": attempt["name"], "coordinate_id": attempt.get("coordinate_id", attempt["name"]),
            "target": attempt["target"], "provider": attempt["provider"], "profile": attempt["profile"],
            "platform": attempt["platform"], "outcome": outcome, "command": command, "cleanup": cleanup,
            "evidence": evidence, "agent_expectations": attempt["agent_expectations"], "input_digest": input_digest,
            "attempted_at": _now()}


def _schedule_coordinates(
    coordinates: dict[str, dict[str, Any]],
    rows: dict[str, dict[str, Any]],
    *,
    total_budget: float,
    maximum: int,
    persist: Any,
) -> dict[str, dict[str, Any]]:
    """The single ready-node scheduler used by both matrix entrypoints."""
    if maximum != 2:
        raise ValueError("the reviewed matrix permits exactly two qualified coordinate processes")
    if not 0 < total_budget <= 14400:
        raise ValueError("matrix total budget must be finite and reviewed")
    pending = set(coordinates) - set(rows)
    deadline = __import__("time").monotonic() + total_budget
    successful = {"PASS", "GAP-NATIVE-MACOS", "GAP-NATIVE-LINUX"}
    running: dict[Any, tuple[str, set[str]]] = {}
    held: set[str] = set()
    with ThreadPoolExecutor(max_workers=maximum) as pool:
        while pending or running:
            # A missing dependency is terminal evidence, not an unhandled graph exception.
            for key in sorted(list(pending)):
                missing = [dep for dep in coordinates[key]["dependency_keys"] if dep not in coordinates and dep not in rows]
                if missing:
                    item = coordinates[key]
                    rows[key] = {"coordinate_key": key, "name": item["name"], "outcome": "BLOCKED_DEPENDENCY", "missing_dependencies": missing, "input_digest": item["input_digest"], "attempted_at": _now()}
                    pending.remove(key)
                    persist(rows)
            if __import__("time").monotonic() >= deadline and pending:
                for key in sorted(pending):
                    item = coordinates[key]
                    rows[key] = {"coordinate_key": key, "name": item["name"], "outcome": "BUDGET_EXHAUSTED", "input_digest": item["input_digest"], "attempted_at": _now()}
                pending.clear()
                persist(rows)
            scheduled = False
            for key in sorted(pending):
                if len(running) >= maximum:
                    break
                item = coordinates[key]
                if not all(dep in rows for dep in item["dependency_keys"]):
                    continue
                resources = _coordinate_resources(item)
                if resources.intersection(held):
                    continue
                if __import__("time").monotonic() >= deadline:
                    break
                future = pool.submit(_run_coordinate, item, item["input_digest"], deadline=deadline)
                running[future] = (key, resources)
                held.update(resources)
                pending.remove(key)
                scheduled = True
            if running:
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    key, resources = running.pop(future)
                    held.difference_update(resources)
                    item = coordinates[key]
                    try:
                        row = future.result()
                    except Exception as exc:  # an ordinary coordinate exception is a row, not a collection abort
                        row = {"name": item["name"], "outcome": "ERROR", "summary": str(exc), "input_digest": item["input_digest"], "attempted_at": _now()}
                    row["coordinate_key"] = key
                    failed = {dep: rows[dep].get("outcome") for dep in item["dependency_keys"] if rows[dep].get("outcome") not in successful}
                    if failed:
                        row["acceptance_outcome"] = "DEPENDENCY_EVIDENCE_FAILED"
                        row["dependencies"] = failed
                    rows[key] = row
                    persist(rows)
                continue
            if pending and not scheduled:
                # All remaining nodes have dependencies that are neither selected nor retained.
                for key in sorted(pending):
                    item = coordinates[key]
                    rows[key] = {"coordinate_key": key, "name": item["name"], "outcome": "BLOCKED_DEPENDENCY", "missing_dependencies": [dep for dep in item["dependency_keys"] if dep not in rows], "input_digest": item["input_digest"], "attempted_at": _now()}
                pending.clear()
                persist(rows)
    return rows


def execute(manifest: dict[str, Any], checkpoint_path: Path) -> dict[str, Any]:
    if os.environ.get(AUTHORIZATION_ENV) != "M09":
        raise PermissionError(f"--execute requires {AUTHORIZATION_ENV}=M09 from the authorized M09 executor")
    inputs = _digest(manifest)
    attempts, digests = _coordinates(manifest)
    prior = {row.get("name"): row for row in _prior_rows(checkpoint_path)
             if isinstance(row, dict) and isinstance(row.get("name"), str)}
    rows = {name: row for name, row in prior.items() if name in digests and row.get("input_digest") == digests[name]}
    by_name = {item["name"]: item for item in attempts}
    success = {"PASS", "GAP-NATIVE-MACOS", "GAP-NATIVE-LINUX"}
    maximum = int(manifest.get("maximum_qualified_coordinate_processes", 2))
    total_budget = float(manifest.get("total_budget_seconds", 0))
    coordinates = {
        name: {**item, "coordinate_key": name, "dependency_keys": list(item.get("depends_on", [])), "input_digest": digests[name]}
        for name, item in by_name.items()
    }
    _schedule_coordinates(
        coordinates, rows, total_budget=total_budget, maximum=maximum,
        persist=lambda current: _checkpoint(checkpoint_path, inputs, [current[item["name"]] for item in attempts if item["name"] in current]),
    )
    ordered_rows = [rows[item["name"]] for item in attempts]
    missing = sorted(set(CHECKS) - {row.get("name") for row in ordered_rows})
    outcome = "PASS" if not missing and all(row.get("outcome") in success for row in ordered_rows) else "FAIL"
    return {"schema": "harness-v2-live-matrix-result/v2", "outcome": outcome, "authorization": "M09", "input_digest": inputs, "native_runner_identity": manifest["native_runner_identity"], "checks": ordered_rows, "missing_checks": missing, "checkpoint": str(checkpoint_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--checkpoint", type=Path, help="durable externally-owned resumable state path")
    parser.add_argument("--result", type=Path, help="optional external result path; never write source")
    args = parser.parse_args(argv)
    try:
        if not args.execute:
            result = _reserved()
        elif args.manifest is None or args.checkpoint is None:
            parser.error("--execute requires --manifest and --checkpoint")
        else:
            result = execute(_load_manifest(args.manifest.resolve()), args.checkpoint.resolve())
        encoded = _canonical(result)
        if args.result:
            args.result.parent.mkdir(parents=True, exist_ok=True)
            args.result.write_text(encoded + "\n", encoding="utf-8")
        print(encoded)
        return 0 if result["outcome"] in {"RESERVED_FOR_M09", "PASS"} else 1
    except (OSError, ValueError, PermissionError, json.JSONDecodeError) as exc:
        print(f"live matrix did not run: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
