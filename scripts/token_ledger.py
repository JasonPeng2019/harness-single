"""Offline Codex JSONL accounting and native-harness attempt discovery.

This module only reads existing task evidence. It never starts an agent or reads
Codex authentication. ``turn.completed.usage`` is a cumulative session value;
resumed processes therefore contribute the delta from the preceding process in
the same thread.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

TASK_SCHEMA = "codex-token-task/v1"
LEDGER_SCHEMA = "codex-token-ledger/v1"
RUN_SCHEMA = "codex-token-run/v1"
USAGE_GAPS_SCHEMA = "codex-usage-gaps/v1"
ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "reported_total_tokens",
)


class LedgerError(ValueError):
    """Evidence is missing, inconsistent, or unsafe to count."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def valid_id(value: Any, name: str) -> str:
    if not isinstance(value, str) or not ID_PATTERN.fullmatch(value):
        raise LedgerError(f"{name} must use letters, digits, dot, underscore, or dash")
    return value


def nonempty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LedgerError(f"{name} must be a non-empty string")
    return value


def launch_preferences(argv: Any) -> dict[str, str]:
    """Read the model, effort, and tier actually passed to a Codex launch."""
    if not isinstance(argv, list) or not argv or any(not isinstance(item, str) for item in argv):
        raise LedgerError("provider launch argv is missing or invalid")
    model: str | None = None
    settings: dict[str, str] = {}
    for index, word in enumerate(argv[:-1]):
        if word == "-m" or (word == "--model" and argv[0] == "ollama"):
            model = argv[index + 1]
        if word == "-c" and "=" in argv[index + 1]:
            key, raw = argv[index + 1].split("=", 1)
            if key in ("model_reasoning_effort", "service_tier"):
                try:
                    value = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise LedgerError(f"invalid {key} in provider argv") from exc
                settings[key] = nonempty_string(value, key)
    return {
        "model": nonempty_string(model, "launch model"),
        "reasoning_effort": nonempty_string(settings.get("model_reasoning_effort"), "launch reasoning_effort"),
        "service_tier": nonempty_string(settings.get("service_tier"), "launch service_tier"),
    }


def nonnegative_int(value: Any, name: str, *, required: bool = False) -> int | None:
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LedgerError(f"{name} must be a non-negative integer")
    return value


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LedgerError(f"cannot read JSON object {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LedgerError(f"JSON root must be an object: {path}")
    return value


def read_lines(path: Path, *, allow_partial_tail: bool = False) -> list[dict[str, Any]]:
    """Read a small control JSONL file. A live transcript uses ``extract_usage``."""
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    raw_lines = path.read_bytes().splitlines(keepends=True)
    for index, raw in enumerate(raw_lines, 1):
        if not raw.strip():
            continue
        try:
            item = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            if allow_partial_tail and index == len(raw_lines) and not raw.endswith(b"\n"):
                break
            raise LedgerError(f"malformed JSONL at {path}:{index}: {exc}") from exc
        if not isinstance(item, dict):
            raise LedgerError(f"JSONL record must be an object at {path}:{index}")
        records.append(item)
    return records


def _field(raw: dict[str, Any], primary: str, *aliases: str) -> int | None:
    for key in (primary, *aliases):
        if key in raw:
            return nonnegative_int(raw[key], key, required=True)
    return None


def normalize_usage(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise LedgerError("turn.completed.usage must be an object")
    values: dict[str, Any] = {
        "input_tokens": _field(raw, "input_tokens", "prompt_tokens"),
        "cached_input_tokens": _field(raw, "cached_input_tokens", "cached_tokens"),
        "output_tokens": _field(raw, "output_tokens", "completion_tokens"),
        "reasoning_output_tokens": _field(raw, "reasoning_output_tokens"),
        "reported_total_tokens": _field(raw, "total_tokens"),
    }
    supplied = values["reported_total_tokens"]
    input_tokens = values["input_tokens"]
    output_tokens = values["output_tokens"]
    derived = (
        input_tokens + output_tokens
        if input_tokens is not None and output_tokens is not None
        else None
    )
    values["derived_input_plus_output_tokens"] = derived
    values["comparison_total_tokens"] = supplied if supplied is not None else derived
    values["comparison_total_source"] = (
        "provider_reported_total_tokens"
        if supplied is not None
        else "derived_input_plus_output"
        if derived is not None
        else None
    )
    cached = values["cached_input_tokens"]
    if cached is not None and input_tokens is not None and cached > input_tokens:
        raise LedgerError("cached input exceeds input tokens")
    return values


def extract_usage(
    path: Path, *, completed: bool = True, segment_index: int | None = None
) -> dict[str, Any]:
    """Select the final top-level usage from a file or one provider segment.

    Native resume-lane appends multiple Codex processes to one transcript.
    Each process begins with ``thread.started``; ``segment_index`` is its
    one-based occurrence, even when every process resumes the same thread.
    """
    if not path.is_file():
        return {"status": "missing_transcript", "transcript_sha256": None}
    digest = hashlib.sha256()
    segment_digest = hashlib.sha256()
    segment_number = 0
    thread_id: str | None = None
    selected: dict[str, Any] | None = None
    selected_line: int | None = None
    terminal_count = 0
    record_count = 0
    final_turn_type: str | None = None
    with path.open("rb") as stream:
        for line_number, raw_line in enumerate(stream, 1):
            digest.update(raw_line)
            if not raw_line.strip():
                continue
            try:
                event = json.loads(raw_line.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                if not completed and not raw_line.endswith(b"\n"):
                    break
                raise LedgerError(f"malformed transcript at {path}:{line_number}: {exc}") from exc
            if not isinstance(event, dict):
                raise LedgerError(f"transcript record must be an object at {path}:{line_number}")
            record_count += 1
            kind = event.get("type")
            if kind == "thread.started":
                segment_number += 1
            if segment_index is not None and segment_number != segment_index:
                record_count -= 1
                continue
            segment_digest.update(raw_line)
            if kind == "thread.started":
                candidate = event.get("thread_id") or event.get("threadId")
                if isinstance(candidate, str) and candidate:
                    if thread_id is not None and candidate != thread_id:
                        raise LedgerError(f"multiple Codex thread IDs in {path}")
                    thread_id = candidate
            if kind == "turn.completed":
                selected = normalize_usage(event.get("usage"))
                selected_line = line_number
                terminal_count += 1
                final_turn_type = kind
            elif kind in ("turn.failed", "turn.cancelled"):
                final_turn_type = str(kind)
    status = "ok" if selected is not None else "in_flight" if not completed else "missing_usage"
    if segment_index is not None and segment_number < segment_index:
        status = "in_flight" if not completed else "missing_segment"
    if completed and final_turn_type in ("turn.failed", "turn.cancelled"):
        status = "terminal_failure_after_usage"
    if selected is not None and selected["comparison_total_tokens"] is None:
        status = "incomplete_usage"
    return {
        "status": status,
        "thread_id": thread_id,
        "selected_line": selected_line,
        "terminal_records": terminal_count,
        "json_records": record_count,
        "transcript_sha256": digest.hexdigest(),
        "transcript_segment_sha256": segment_digest.hexdigest(),
        **(selected or {}),
    }


def load_task(path: Path) -> dict[str, Any]:
    task = read_object(path)
    if task.get("schema") != TASK_SCHEMA:
        raise LedgerError(f"task file requires schema {TASK_SCHEMA}")
    valid_id(task.get("run_id"), "run_id")
    if not isinstance(task.get("task"), str) or not task["task"].strip():
        raise LedgerError("task must be a non-empty string")
    if task.get("arm") not in ("harness", "raw"):
        raise LedgerError("arm must be harness or raw")
    if task.get("interruption_policy") not in (None, "planned_resume"):
        raise LedgerError("unknown interruption policy")
    budget = task.get("budget_tokens")
    if budget is not None:
        budget = nonnegative_int(budget, "budget_tokens", required=True)
    if budget == 0:
        raise LedgerError("budget_tokens must be positive")
    root_dir = Path(nonempty_string(task.get("root_run_dir"), "root_run_dir")).resolve()
    if not root_dir.is_dir():
        raise LedgerError(f"root run directory does not exist: {root_dir}")
    task["root_run_dir"] = str(root_dir)
    if task["arm"] == "harness":
        runtime = Path(nonempty_string(task.get("harness_runtime"), "harness_runtime")).resolve()
        if not runtime.is_dir():
            # Before setup, it is valid for the runtime directory not to exist.
            if runtime.exists():
                raise LedgerError(f"harness runtime is not a directory: {runtime}")
        task["harness_runtime"] = str(runtime)
    elif task.get("harness_runtime") is not None:
        raise LedgerError("raw arm must not name a harness runtime")
    epoch = task.get("epoch_id")
    if epoch is not None:
        valid_id(epoch, "epoch_id")
    return task


def _root_invocations(task: dict[str, Any]) -> list[dict[str, Any]]:
    run_dir = Path(task["root_run_dir"])
    first_launch = read_object(run_dir / "launch.json")
    segment_dirs = [run_dir]
    segment_root = run_dir / "segments"
    if segment_root.is_dir():
        children = sorted(item for item in segment_root.iterdir() if item.is_dir())
        for number, child in enumerate(children, 2):
            if child.name != f"{number:04d}":
                raise LedgerError(f"nonconsecutive ROOT segment directory: {child}")
        segment_dirs.extend(children)
    if len(segment_dirs) > 1 and task.get("interruption_policy") != "planned_resume":
        raise LedgerError("ROOT segments require declared planned_resume policy")
    rows: list[dict[str, Any]] = []
    for number, directory in enumerate(segment_dirs, 1):
        launch = read_object(directory / "launch.json")
        if launch.get("schema") != RUN_SCHEMA or launch.get("run_id") != task["run_id"]:
            raise LedgerError("root launch record does not match task run")
        if launch.get("task") != task["task"] or launch.get("arm") != task["arm"]:
            raise LedgerError("root task or arm does not match task file")
        if launch.get("budget_tokens") != task["budget_tokens"]:
            raise LedgerError("root launch budget differs from task file")
        if task["arm"] == "harness" and launch.get("harness_runtime") != task["harness_runtime"]:
            raise LedgerError("root launch runtime differs from task file")
        for key in ("model", "reasoning_effort", "service_tier", "started_at_utc"):
            nonempty_string(launch.get(key), f"root {key}")
        if launch_preferences(launch.get("command")) != {
            "model": launch["model"],
            "reasoning_effort": launch["reasoning_effort"],
            "service_tier": launch["service_tier"],
        }:
            raise LedgerError("root command differs from declared model/effort/tier")
        if number > 1:
            for key in ("model", "reasoning_effort", "service_tier", "sandbox", "workspace", "harness_runtime"):
                if launch.get(key) != first_launch.get(key):
                    raise LedgerError(f"ROOT resume changed {key}")
            if launch.get("segment_number") != number:
                raise LedgerError("ROOT resume segment number mismatch")
            if not nonempty_string(launch.get("resume_session_id"), "resume_session_id"):
                raise LedgerError("ROOT resume session missing")
            command = launch["command"]
            if command[:3] != ["codex", "exec", "resume"] or launch["resume_session_id"] not in command:
                raise LedgerError("ROOT resume command does not name saved session")
            prior = rows[-1]
            if not prior["completed"]:
                raise LedgerError("ROOT resume began before prior segment was sealed")
            prior_usage = extract_usage(Path(prior["transcript_path"]))
            if prior_usage.get("thread_id") != launch["resume_session_id"]:
                raise LedgerError("ROOT resume session differs from prior transcript")
        elif launch != first_launch:
            raise LedgerError("first ROOT launch changed while reading")
        final_path = directory / "manifest.json"
        final = read_object(final_path) if final_path.is_file() else None
        if final is not None and any(final.get(key) != value for key, value in launch.items()):
            raise LedgerError("root final manifest differs from launch record")
        rows.append({
            "invocation_id": f"{task['run_id']}.root" if number == 1 else f"{task['run_id']}.root.segment-{number}",
            "role": "root" if task["arm"] == "harness" else "raw",
            "parent_id": None,
            "lane_id": None,
            "lane_run_id": None,
            "attempt": number,
            "model": launch["model"],
            "reasoning_effort": launch["reasoning_effort"],
            "service_tier": launch["service_tier"],
            "started_at_utc": launch["started_at_utc"],
            "ended_at_utc": final.get("ended_at_utc") if final else None,
            "exit_code": final.get("exit_code") if final else None,
            "completed": final is not None,
            "resume": number > 1,
            "resume_session_id": launch.get("resume_session_id"),
            "launch_argv": launch["command"],
            "transcript_path": str(directory / "codex-events.jsonl"),
            "source_manifest_paths": [str(directory / "launch.json"), str(final_path)] if final else [str(directory / "launch.json")],
        })
    return rows


def _within(path: Path, directory: Path) -> bool:
    return path.resolve().is_relative_to(directory.resolve())


def _worker_invocations(task: dict[str, Any]) -> list[dict[str, Any]]:
    run_dir = Path(task["root_run_dir"])
    root_rows = _root_invocations(task)

    def timestamp(value: str) -> dt.datetime:
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise LedgerError(f"invalid worker/run timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            raise LedgerError(f"worker/run timestamp lacks timezone: {value!r}")
        return parsed

    windows = [
        (timestamp(row["started_at_utc"]), timestamp(row["ended_at_utc"]) if row["ended_at_utc"] else None, row["invocation_id"])
        for row in root_rows
    ]
    runtime = Path(task["harness_runtime"])
    if not runtime.is_dir():
        return []
    root = runtime / "worktrees"
    if not root.is_dir():
        return []
    epochs = sorted(item for item in root.iterdir() if item.is_dir())
    if task.get("epoch_id"):
        epochs = [item for item in epochs if item.name == task["epoch_id"]]
    results: list[dict[str, Any]] = []
    for epoch in epochs:
        for lane_dir in sorted(item for item in epoch.iterdir() if item.is_dir()):
            workspace = lane_dir / ".agent-workspace"
            invocation_path = workspace / "invocation.json"
            if not invocation_path.is_file():
                continue
            invocation = read_object(invocation_path)
            if invocation.get("schema") != "controller-invocation/v1":
                raise LedgerError(f"unsupported harness invocation: {invocation_path}")
            lane_id = valid_id(invocation.get("lane_id"), "lane_id")
            lane_run_id = valid_id(invocation.get("run_id"), "lane run_id")
            if lane_id != lane_dir.name:
                raise LedgerError(f"lane ID differs from worktree directory: {invocation_path}")
            provider = invocation.get("provider")
            if not isinstance(provider, dict) or provider.get("id") != "codex":
                raise LedgerError(f"uncounted non-Codex worker lane: {invocation_path}")
            launch_config = provider.get("launch_config") or {}
            if not isinstance(launch_config, dict):
                raise LedgerError(f"invalid worker launch_config: {invocation_path}")
            nonempty_string(provider.get("model"), "worker model")
            nonempty_string(launch_config.get("reasoning_effort"), "worker reasoning_effort")
            nonempty_string(launch_config.get("service_tier"), "worker service_tier")
            paths = invocation.get("paths") or {}
            if not isinstance(paths, dict):
                raise LedgerError(f"invalid worker paths: {invocation_path}")
            attempts_path = Path(paths.get("attempts") or workspace / "controller.attempts.jsonl")
            if not _within(attempts_path, workspace):
                raise LedgerError(f"attempt record escapes worker workspace: {attempts_path}")
            attempt_rows: list[dict[str, Any]] = []
            for row in read_lines(attempts_path, allow_partial_tail=True):
                if row.get("schema") == "controller-attempts/v1":
                    continue
                number = row.get("attempt")
                if isinstance(number, bool) or not isinstance(number, int) or number < 1:
                    raise LedgerError(f"invalid attempt number in {attempts_path}")
                if row.get("argv") is not None:
                    attempt_rows.append(row)
            transcript_paths: dict[int, Path] = {}
            first = Path(paths.get("transcript") or workspace / "provider-transcript.jsonl")
            if first.is_file():
                transcript_paths[1] = first
            for candidate in (workspace / "attempts").glob("attempt-*/provider-transcript.jsonl"):
                try:
                    number = int(candidate.parent.name.removeprefix("attempt-"))
                except ValueError:
                    continue
                if number < 2 or number in transcript_paths:
                    raise LedgerError(f"invalid or duplicate attempt path: {candidate}")
                transcript_paths[number] = candidate
            events_path = workspace / "controller.events.jsonl"
            starts = [
                row
                for row in read_lines(events_path, allow_partial_tail=True)
                if row.get("event_type") == "provider_started"
            ]
            if len(attempt_rows) > len(starts):
                raise LedgerError(f"provider attempt lacks a start event in {events_path}")
            shared_segments: dict[Path, int] = {}
            for number, start_event in enumerate(starts, 1):
                row = attempt_rows[number - 1] if len(attempt_rows) >= number else None
                started = start_event.get("ts")
                when = timestamp(started)
                appending_first = (
                    row is None and number not in transcript_paths and bool(attempt_rows)
                    and Path(attempt_rows[-1].get("transcript_path") or first).resolve()
                    == first.resolve()
                )
                default_transcript = (
                    first if number == 1 or appending_first else
                    workspace / "attempts" / f"attempt-{number}" / "provider-transcript.jsonl"
                )
                transcript = Path(row["transcript_path"]) if row and row.get("transcript_path") else transcript_paths.get(number, default_transcript)
                if not _within(transcript, workspace):
                    raise LedgerError(f"transcript escapes worker workspace: {transcript}")
                if row and number in transcript_paths and transcript.resolve() != transcript_paths[number].resolve():
                    raise LedgerError(f"attempt transcript path conflict: {transcript}")
                resolved_transcript = transcript.resolve()
                shared_segments[resolved_transcript] = shared_segments.get(resolved_transcript, 0) + 1
                owner = next((root_id for start, end, root_id in windows if when >= start and (end is None or when <= end)), None)
                if owner is None:
                    if when >= windows[0][0] and (windows[-1][1] is None or when <= windows[-1][1]):
                        raise LedgerError(f"worker activity occurred during paused interval: {events_path}")
                    continue
                argv = row.get("argv") if row else None
                resume = isinstance(argv, list) and "resume" in argv[:5]
                controller_run_id = valid_id(
                    start_event.get("run_id") or lane_run_id, "controller run_id"
                )
                results.append({
                    "invocation_id": f"{task['run_id']}.{epoch.name}.{lane_id}.{controller_run_id}.attempt-{number}",
                    "role": "worker",
                    "parent_id": owner,
                    "epoch_id": epoch.name,
                    "lane_id": lane_id,
                    "lane_run_id": controller_run_id,
                    "attempt": number,
                    "model": provider.get("model"),
                    "reasoning_effort": launch_config.get("reasoning_effort"),
                    "service_tier": launch_config.get("service_tier"),
                    "started_at_utc": started,
                    "ended_at_utc": row.get("at") if row else None,
                    "exit_code": row.get("exit_code") if row else None,
                    "completed": row is not None and isinstance(row.get("exit_code"), int),
                    "resume": resume or number > 1,
                    "launch_argv": argv,
                    "attempt_provider": row.get("provider") if row else None,
                    "transcript_path": str(transcript),
                    "transcript_segment_index": shared_segments[resolved_transcript],
                    "source_manifest_paths": [
                        str(path) for path in (invocation_path, attempts_path, events_path)
                        if path.is_file()
                    ],
                })
    return results


def discover_invocations(task: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _root_invocations(task)
    if task["arm"] == "harness":
        rows.extend(_worker_invocations(task))
    ids = [row["invocation_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise LedgerError("duplicate invocation IDs")
    return rows


def usage_gap_receipts(task: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], Path | None]:
    """Read explicit operator acknowledgements of terminated, unmetered workers."""
    path = Path(task["root_run_dir"]) / "usage-gaps.json"
    if not path.is_file():
        return {}, None
    record = read_object(path)
    if record.get("schema") != USAGE_GAPS_SCHEMA or record.get("run_id") != task["run_id"]:
        raise LedgerError("usage-gap receipt does not match the task run")
    gaps = record.get("gaps")
    if not isinstance(gaps, list) or not gaps:
        raise LedgerError("usage-gap receipt requires nonempty gaps")
    receipts: dict[str, dict[str, Any]] = {}
    for gap in gaps:
        if not isinstance(gap, dict):
            raise LedgerError("usage-gap entry must be an object")
        invocation_id = nonempty_string(gap.get("invocation_id"), "usage-gap invocation_id")
        if invocation_id in receipts:
            raise LedgerError("duplicate usage-gap invocation_id")
        digest = gap.get("transcript_sha256")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise LedgerError("usage-gap transcript_sha256 is invalid")
        for field in ("epoch_id", "lane_id", "lane_run_id"):
            valid_id(gap.get(field), f"usage-gap {field}")
        nonempty_string(gap.get("epoch_closed_at"), "usage-gap epoch_closed_at")
        nonempty_string(gap.get("reason"), "usage-gap reason")
        nonempty_string(gap.get("operator_observed_absent_at_utc"), "usage-gap absence time")
        receipts[invocation_id] = gap
    return receipts, path


def validate_usage_gap(
    task: dict[str, Any], row: dict[str, Any], evidence: dict[str, Any], gap: dict[str, Any]
) -> None:
    """A receipt seals lifecycle, never fabricates a token amount."""
    if row["role"] != "worker" or row["completed"] or evidence["status"] != "in_flight":
        raise LedgerError("usage-gap receipt is not for an unmetered worker")
    if evidence.get("terminal_records") or evidence.get("comparison_total_tokens") is not None:
        raise LedgerError("usage-gap worker has terminal usage; count it normally")
    for field in ("invocation_id", "epoch_id", "lane_id", "lane_run_id"):
        if gap[field] != row.get(field):
            raise LedgerError(f"usage-gap {field} differs from worker evidence")
    if gap["transcript_sha256"] != evidence["transcript_sha256"]:
        raise LedgerError("usage-gap transcript hash differs from worker evidence")
    epoch_dir = Path(task["harness_runtime"]) / "epochs" / row["epoch_id"]
    epoch = read_object(epoch_dir / "epoch-state.json")
    lane = read_object(epoch_dir / "lanes" / row["lane_id"] / "lane.json")
    if (epoch.get("lifecycle") != "closed" or
            epoch.get("closed_at") != gap["epoch_closed_at"] or
            lane.get("lifecycle") != "retired" or
            lane.get("run_id") != row["lane_run_id"]):
        raise LedgerError("usage-gap worker lacks closed epoch and retired lane proof")


def build_snapshot(task_path: Path) -> dict[str, Any]:
    task = load_task(task_path)
    rows = discover_invocations(task)
    gaps, gap_path = usage_gap_receipts(task)
    used_gaps: set[str] = set()
    root_rows = [row for row in rows if row["role"] != "worker"]
    # Attempts within a Codex thread must be processed in chronological order.
    rows.sort(key=lambda row: (str(row.get("started_at_utc") or ""), row["invocation_id"]))
    previous: dict[str, dict[str, Any]] = {}
    fingerprints: dict[tuple[str | None, str], str] = {}
    errors: list[str] = []
    pause_seconds = 0.0
    for prior, following in zip(root_rows, root_rows[1:]):
        if prior["ended_at_utc"] is None:
            errors.append("ROOT resume began before prior segment ended")
            continue
        try:
            ended = dt.datetime.fromisoformat(prior["ended_at_utc"].replace("Z", "+00:00"))
            started = dt.datetime.fromisoformat(following["started_at_utc"].replace("Z", "+00:00"))
            gap = (started - ended).total_seconds()
        except (AttributeError, ValueError, TypeError):
            errors.append("ROOT segment timestamps are invalid")
            continue
        if gap < 0:
            errors.append("ROOT resume overlaps prior segment")
        else:
            pause_seconds += gap
    active_parts = []
    for row in root_rows:
        directory = Path(row["transcript_path"]).parent
        manifest = directory / "manifest.json"
        if not manifest.is_file():
            active_parts = []
            break
        elapsed = read_object(manifest).get("elapsed_seconds")
        if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or elapsed < 0:
            active_parts = []
            break
        active_parts.append(float(elapsed))
    known_total = 0
    in_flight = 0
    unknown_usage = 0
    for row in rows:
        if row["completed"]:
            if isinstance(row.get("exit_code"), bool) or not isinstance(row.get("exit_code"), int):
                errors.append(f"{row['invocation_id']}: missing exit status")
            if not row.get("started_at_utc") or not row.get("ended_at_utc"):
                errors.append(f"{row['invocation_id']}: missing process start/end time")
            if row["role"] == "worker":
                try:
                    effective = launch_preferences(row.get("launch_argv"))
                except LedgerError as exc:
                    errors.append(f"{row['invocation_id']}: {exc}")
                else:
                    declared = {key: row.get(key) for key in ("model", "reasoning_effort", "service_tier")}
                    if effective != declared:
                        errors.append(f"{row['invocation_id']}: launch argv differs from configured model/effort/tier")
                attempt_provider = row.get("attempt_provider")
                if isinstance(attempt_provider, dict):
                    if attempt_provider.get("model") != row.get("model") or (
                        attempt_provider.get("launch_config") or {}).get("reasoning_effort") != row.get("reasoning_effort") or (
                        attempt_provider.get("launch_config") or {}).get("service_tier") != row.get("service_tier"):
                        errors.append(f"{row['invocation_id']}: attempt provider record differs from invocation")
        evidence = extract_usage(
            Path(row["transcript_path"]), completed=row["completed"],
            segment_index=row.get("transcript_segment_index"),
        )
        row.update(evidence)
        gap = gaps.get(row["invocation_id"])
        if gap is not None:
            validate_usage_gap(task, row, evidence, gap)
            used_gaps.add(row["invocation_id"])
            row["status"] = "terminated_usage_unknown"
            row["usage_gap"] = gap
            row["source_manifest_paths"].append(str(gap_path))
            unknown_usage += 1
        if row["role"] != "worker" and row["resume"] and evidence.get("thread_id") != row.get("resume_session_id"):
            errors.append(f"{row['invocation_id']}: resumed ROOT transcript changed native session")
        if not row["completed"] and gap is None:
            in_flight += 1
        if evidence["status"] not in ("ok", "in_flight"):
            errors.append(f"{row['invocation_id']}: {evidence['status']}")
        total = evidence.get("comparison_total_tokens")
        row["charged_tokens"] = None
        row["charge_source"] = None
        if total is None:
            if row["completed"]:
                errors.append(f"{row['invocation_id']}: no countable terminal usage")
            continue
        thread_id = evidence.get("thread_id")
        fingerprint = (thread_id, evidence["transcript_segment_sha256"])
        if fingerprint in fingerprints:
            row["duplicate_of"] = fingerprints[fingerprint]
            row["charged_tokens"] = 0
            row["charge_source"] = "exact_transcript_replay"
            continue
        fingerprints[fingerprint] = row["invocation_id"]
        if row["resume"] and (not thread_id or thread_id not in previous):
            errors.append(f"{row['invocation_id']}: resumed thread has no prior baseline")
            continue
        prior = previous.get(thread_id) if thread_id else None
        if prior is not None:
            prior_total = prior["comparison_total_tokens"]
            charged = total - prior_total
            if charged < 0:
                errors.append(f"{row['invocation_id']}: cumulative session total decreased")
                continue
            row["charge_source"] = "session_cumulative_delta"
            row["previous_cumulative_total_tokens"] = prior_total
            for field in TOKEN_FIELDS:
                current_value = row.get(field)
                prior_value = prior.get(field)
                if current_value is not None and prior_value is not None:
                    difference = current_value - prior_value
                    if difference < 0:
                        errors.append(f"{row['invocation_id']}: cumulative {field} decreased")
                    row[f"charged_{field}"] = difference if difference >= 0 else None
        else:
            charged = total
            row["charge_source"] = evidence.get("comparison_total_source")
            for field in TOKEN_FIELDS:
                row[f"charged_{field}"] = row.get(field)
        row["charged_tokens"] = charged
        known_total += charged
        if thread_id:
            previous[thread_id] = row
    if used_gaps != set(gaps):
        raise LedgerError("usage-gap receipt names an unknown worker invocation")
    budget = task["budget_tokens"]
    counted_rows = [
        row for row in rows
        if row.get("charged_tokens") is not None and row.get("charge_source") != "exact_transcript_replay"
    ]
    aggregate_components: dict[str, int | None] = {}
    for field in TOKEN_FIELDS:
        pieces = [row.get(f"charged_{field}") for row in counted_rows]
        aggregate_components[field] = sum(pieces) if pieces and all(piece is not None for piece in pieces) else None
    return {
        "schema": LEDGER_SCHEMA,
        "generated_at_utc": utc_now(),
        "task_file_sha256": hashlib.sha256(task_path.read_bytes()).hexdigest(),
        "task": task["task"],
        "arm": task["arm"],
        "run_id": task["run_id"],
        "interruption_policy": task.get("interruption_policy"),
        "resume_count": len(root_rows) - 1,
        "paused_wall_seconds": round(pause_seconds, 3),
        "active_elapsed_seconds": round(sum(active_parts), 3) if len(active_parts) == len(root_rows) else None,
        "budget_tokens": budget,
        "known_total_tokens": known_total,
        "aggregate_components": aggregate_components,
        "remaining_tokens": max(0, budget - known_total) if budget is not None else None,
        "overshoot_tokens": max(0, known_total - budget) if budget is not None else None,
        "in_flight_invocations": in_flight,
        "unknown_usage_invocations": unknown_usage,
        "token_total_is_exact": unknown_usage == 0 and in_flight == 0 and not errors,
        "errors": sorted(set(errors)),
        "gate_allows_new_work": not errors and (budget is None or known_total < budget),
        "rows": rows,
    }


def finalize(task_path: Path, output_dir: Path) -> dict[str, Any]:
    """Create a write-once evidence archive after all provider processes exit."""
    snapshot = build_snapshot(task_path)
    if snapshot["errors"]:
        raise LedgerError("cannot finalize invalid usage: " + "; ".join(snapshot["errors"]))
    if snapshot["in_flight_invocations"]:
        raise LedgerError("cannot finalize while provider invocations are in flight")
    output_dir = output_dir.resolve()
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite archive: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".token-ledger-", dir=output_dir.parent))
    try:
        task_copy = temporary / "task.json"
        shutil.copyfile(task_path, task_copy)
        if hashlib.sha256(task_copy.read_bytes()).hexdigest() != snapshot["task_file_sha256"]:
            raise LedgerError("task file changed during archive")
        copied_sources: dict[str, str] = {}
        for number, row in enumerate(snapshot["rows"], 1):
            folder = temporary / "invocations" / f"{number:04d}"
            folder.mkdir(parents=True)
            transcript = Path(row["transcript_path"])
            destination = folder / "codex-events.jsonl"
            shutil.copyfile(transcript, destination)
            digest = hashlib.sha256(destination.read_bytes()).hexdigest()
            if digest != row["transcript_sha256"]:
                raise LedgerError(f"transcript changed during archive: {transcript}")
            row["archived_transcript"] = str(destination.relative_to(temporary))
            row["source_manifest_sha256"] = {}
            for source_name in row["source_manifest_paths"]:
                source = Path(source_name)
                if not source.is_file():
                    raise LedgerError(f"source manifest disappeared: {source}")
                source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
                row["source_manifest_sha256"][str(source)] = source_digest
                if str(source) not in copied_sources:
                    manifest_dest = temporary / "source-records" / f"{len(copied_sources) + 1:04d}{source.suffix}"
                    manifest_dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, manifest_dest)
                    if hashlib.sha256(manifest_dest.read_bytes()).hexdigest() != source_digest:
                        raise LedgerError(f"source manifest changed during archive: {source}")
                    copied_sources[str(source)] = str(manifest_dest.relative_to(temporary))
            row["archived_source_manifests"] = {
                name: copied_sources[name] for name in row["source_manifest_paths"]
            }
        (temporary / "ledger.json").write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        summary = {key: snapshot[key] for key in (
            "schema", "task", "arm", "run_id", "budget_tokens", "known_total_tokens",
            "aggregate_components", "remaining_tokens", "overshoot_tokens",
            "in_flight_invocations", "errors", "interruption_policy", "resume_count",
            "unknown_usage_invocations", "token_total_is_exact",
            "paused_wall_seconds", "active_elapsed_seconds",
        )}
        (temporary / "usage.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, output_dir)
        return summary
    except BaseException:
        shutil.rmtree(temporary)
        raise
