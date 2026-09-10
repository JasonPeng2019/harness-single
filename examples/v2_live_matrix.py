"""M09-only, resumable native live-matrix executor.

It does not discover work, schedule lanes, retry failures, or manufacture proof.
ROOT supplies a reviewed manifest of already-authorized, independent native attempts.
Without ``--execute`` it emits the complete reserved matrix; fakes and prior files
never satisfy a live row.
"""

from __future__ import annotations

import argparse
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
        outcome = "NOT_RUN_LIVE" if cell["applicable"] == "true" and not gap else (gap or "NOT_APPLICABLE")
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


def _run(argv: list[str]) -> dict[str, Any]:
    completed = subprocess.run(argv, text=True, capture_output=True, check=False)
    return {"argv": argv, "returncode": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr}


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
    temporary.write_text(_canonical({"schema": "harness-v2-live-checkpoint/v2", "input_digest": input_digest, "updated_at": _now(), "results": rows}) + "\n", encoding="utf-8")
    temporary.replace(path)


def _prior_rows(path: Path, input_digest: str) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema") != "harness-v2-live-checkpoint/v2" or value.get("input_digest") != input_digest:
        return []
    return value["results"] if isinstance(value.get("results"), list) else []


def execute(manifest: dict[str, Any], checkpoint_path: Path) -> dict[str, Any]:
    if os.environ.get(AUTHORIZATION_ENV) != "M09":
        raise PermissionError(f"--execute requires {AUTHORIZATION_ENV}=M09 from the authorized M09 executor")
    inputs = _digest(manifest)
    rows = _prior_rows(checkpoint_path, inputs)
    complete = {row.get("name") for row in rows if row.get("outcome") in {"PASS", "GAP-NATIVE-MACOS", "GAP-NATIVE-LINUX"}}
    for attempt in manifest["attempts"]:
        if attempt["name"] in complete:
            continue
        if attempt["platform"] in NATIVE_GAPS:
            row = {"name": attempt["name"], "outcome": NATIVE_GAPS[attempt["platform"]], "reason": "native runner unavailable; retained for future execution", "attempted_at": _now()}
        else:
            paths = _paths(attempt["evidence"], f"{attempt['name']}.evidence")
            before = {kind: _fingerprint(path) for kind, path in paths.items()}
            command = _run(_argv(attempt["command"], f"{attempt['name']}.command"))
            cleanup = _run(_argv(attempt["cleanup_command"], f"{attempt['name']}.cleanup_command"))
            fresh, evidence = _evidence_is_fresh(paths, before, attempt["evidence_oracles"])
            classifications = all(isinstance(item, dict) and item.get("classification") in {"Observed", "Not observed", "Deviation"} for item in attempt["agent_expectations"])
            row = {"name": attempt["name"], "target": attempt["target"], "provider": attempt["provider"], "profile": attempt["profile"], "platform": attempt["platform"], "outcome": "PASS" if command["returncode"] == 0 and cleanup["returncode"] == 0 and fresh and classifications else "FAIL", "command": command, "cleanup": cleanup, "evidence": evidence, "agent_expectations": attempt["agent_expectations"], "attempted_at": _now()}
        rows.append(row)
        _checkpoint(checkpoint_path, inputs, rows)
    missing = sorted(set(CHECKS) - {row.get("name") for row in rows})
    outcome = "PASS" if not missing and all(row.get("outcome") in {"PASS", "GAP-NATIVE-MACOS", "GAP-NATIVE-LINUX"} for row in rows) else "FAIL"
    return {"schema": "harness-v2-live-matrix-result/v2", "outcome": outcome, "authorization": "M09", "input_digest": inputs, "native_runner_identity": manifest["native_runner_identity"], "checks": rows, "missing_checks": missing, "checkpoint": str(checkpoint_path)}


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
