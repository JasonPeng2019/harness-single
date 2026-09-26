from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROLES = {"orchestrator", "harness", "subagent", "supervisor"}
SCHEMA_PATH = Path(__file__).resolve().with_name("verdict.schema.json")


@dataclass(frozen=True)
class ObservedSource:
    path: Path
    role: str
    source_id: str


@dataclass(frozen=True)
class WatcherConfig:
    repository_root: Path
    runtime_root: Path
    observed_log_roots: tuple[Path, ...]
    poll_interval_seconds: int = 300
    no_progress_seconds: int = 900
    max_tail_bytes: int = 65536
    evaluator_command: tuple[str, ...] = ()
    observed_sources: tuple[ObservedSource, ...] = ()
    attention_logging_enabled: bool = False
    attention_producers: tuple[tuple[str, str], ...] = ()
    attention_lock_timeout_seconds: float = 5.0
    primary_owner_identity_path: Path | None = None
    attention_epoch_id: str | None = None
    evaluator_enabled: bool = False


def _command(raw: Any) -> tuple[str, ...]:
    default = (
        "codex",
        "exec",
        "--model",
        "gpt-5.6-terra",
        "-c",
        'model_reasoning_effort="high"',
        "--output-schema",
        str(SCHEMA_PATH),
        "-",
    )
    command = raw if raw is not None else list(default)
    if not isinstance(command, list) or not all(
        isinstance(x, str) and x for x in command
    ):
        raise ValueError("evaluator_command must be command strings")
    return tuple(str(SCHEMA_PATH) if x == "REPLACED_BY_LOADER" else x for x in command)


def load_config(path: str | Path | None = None) -> WatcherConfig:
    repo = Path(__file__).resolve().parents[1]
    raw: dict[str, Any] = {}
    if path:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("watcher config must be an object")
    interval = raw.get("poll_interval_seconds", 300)
    if not isinstance(interval, int) or interval < 1:
        raise ValueError("poll_interval_seconds must be positive integer")
    runtime = (repo / raw.get("runtime_root", "harness_watcher")).resolve()
    try:
        runtime.relative_to(repo)
    except ValueError as exc:
        raise ValueError("runtime_root must stay in repository") from exc
    declared = raw.get("observed_sources")
    sources: list[ObservedSource] = []
    if declared is not None:
        if not isinstance(declared, list) or not declared:
            raise ValueError("observed_sources must be non-empty list")
        for item in declared:
            if (
                not isinstance(item, dict)
                or set(item) != {"path", "role", "source_id"}
                or not all(
                    isinstance(item.get(k), str) and item[k]
                    for k in ("path", "role", "source_id")
                )
            ):
                raise ValueError("invalid observed source declaration")
            if item["role"] not in ROLES:
                raise ValueError("invalid observed source role")
            value = Path(item["path"])
            resolved = (
                (repo / value).resolve() if not value.is_absolute() else value.resolve()
            )
            sources.append(ObservedSource(resolved, item["role"], item["source_id"]))
    else:
        roots = raw.get(
            "observed_log_roots",
            ["multi-agent-logs/orchestrator-harness/current/events.jsonl"],
        )
        if not isinstance(roots, list) or not all(isinstance(x, str) for x in roots):
            raise ValueError("observed_log_roots must be strings")
        sources = [
            ObservedSource(
                (repo / Path(x)).resolve()
                if not Path(x).is_absolute()
                else Path(x).resolve(),
                "",
                "",
            )
            for x in roots
        ]
    threshold = raw.get("no_progress_seconds", 900)
    tail = raw.get("max_tail_bytes", 65536)
    if (
        not isinstance(threshold, int)
        or threshold < 1
        or not isinstance(tail, int)
        or tail < 1024
    ):
        raise ValueError("invalid watcher bounds")
    attention_enabled = raw.get("attention_logging_enabled", False)
    if not isinstance(attention_enabled, bool):
        raise ValueError("attention_logging_enabled must be boolean")
    declared_producers = raw.get("attention_producers", [])
    if not isinstance(declared_producers, list):
        raise ValueError("attention_producers must be a list")
    producers = []
    for item in declared_producers:
        if (
            not isinstance(item, dict)
            or set(item) != {"role", "source_id"}
            or item.get("role") not in {"orchestrator", "subagent"}
            or not isinstance(item.get("source_id"), str)
            or not item["source_id"]
        ):
            raise ValueError("invalid attention producer declaration")
        producers.append((item["role"], item["source_id"]))
    lock_timeout = raw.get("attention_lock_timeout_seconds", 5.0)
    if (
        not isinstance(lock_timeout, (int, float))
        or isinstance(lock_timeout, bool)
        or lock_timeout <= 0
    ):
        raise ValueError("invalid attention lock timeout")
    identity = raw.get("primary_owner_identity_path")
    if identity is not None and (not isinstance(identity, str) or not identity):
        raise ValueError("primary_owner_identity_path must be a non-empty string")
    identity_path = (
        (
            (repo / identity).resolve()
            if isinstance(identity, str) and not Path(identity).is_absolute()
            else Path(identity).resolve()
        )
        if identity
        else None
    )
    if identity_path is not None:
        try:
            identity_path.relative_to(repo)
        except ValueError as exc:
            raise ValueError(
                "primary_owner_identity_path must stay in repository"
            ) from exc
    enabled = raw.get("evaluator_enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("evaluator_enabled must be boolean")
    epoch = raw.get("attention_epoch_id")
    if epoch is not None and (not isinstance(epoch, str) or not epoch):
        raise ValueError("attention_epoch_id must be a non-empty string")
    return WatcherConfig(
        repo,
        runtime,
        tuple(x.path for x in sources),
        interval,
        threshold,
        tail,
        _command(raw.get("evaluator_command")),
        tuple(sources),
        attention_enabled,
        tuple(producers),
        float(lock_timeout),
        identity_path,
        epoch,
        enabled,
    )
