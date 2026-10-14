from __future__ import annotations
import base64, hashlib, json, os, stat, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from . import settings
from .config import WatcherConfig
from .evaluator import Evaluator, OWNERSHIP_PHASE_RULES, STALE_STATUS_QUARANTINE_RULE
from .logging import log
from .state import save_alert
from .attention import ingest_attention
from .io import atomic_json, read_json, lock


def _cursor_path(root: Path) -> Path:
    return root / "watcher" / "cursor.json"


def _load_cursor(root: Path) -> dict[str, Any]:
    value = read_json(root, _cursor_path(root))
    if value is None:
        return {"schema": "harness-watcher-cursor/v2", "sources": {}}
    if value.get("schema") != "harness-watcher-cursor/v2" or not isinstance(value.get("sources"), dict):
        raise ValueError("invalid watcher cursor; preserve it for reconciliation")
    return value


def _save_cursor(root: Path, v: dict[str, Any]) -> None:
    atomic_json(root, _cursor_path(root), v)


def initialize_service_cursor(config: WatcherConfig) -> bool:
    with lock(config.runtime_root, _cursor_path(config.runtime_root)):
        return _initialize_service_cursor(config)


def _initialize_service_cursor(config: WatcherConfig) -> bool:
    """Set a new background service epoch at the current EOF of existing sources."""
    if _cursor_path(config.runtime_root).exists():
        return False
    now = time.time()
    sources = {}
    for path in config.observed_log_roots:
        item = _read(path, config.max_tail_bytes, {})
        if "error" in item:
            continue
        sources[str(path)] = {
            "file_id": item["file_id"],
            "offset": item["next_offset"],
            "generation": None,
            "context": {},
            "last_changed_epoch": now,
            "last_changed_utc": None,
            "no_progress_evaluated_generation": None,
        }
    _save_cursor(
        config.runtime_root,
        {
            "schema": "harness-watcher-cursor/v2",
            "sources": sources,
            "stale_quarantine": {},
        },
    )
    return True


def _read(path: Path, limit: int, prior: dict[str, Any]) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            st = os.fstat(handle.fileno())
            if not stat.S_ISREG(st.st_mode):
                raise OSError("observed source is not a regular file")
            fid = [st.st_dev, st.st_ino]
            prior_offset = prior.get("offset", 0)
            if type(prior_offset) is not int or prior_offset < 0:
                raise ValueError("invalid watcher source offset")
            old = prior_offset if prior.get("file_id") == fid and prior_offset <= st.st_size else 0
            offset = max(old, st.st_size - limit)
            handle.seek(offset)
            # Capture exactly the sampled interval. Appends after fstat are
            # deliberately left for the next poll rather than read twice.
            sampled = handle.read(st.st_size - offset)
            after = os.fstat(handle.fileno())
            if len(sampled) != st.st_size - offset or after.st_size < st.st_size:
                raise OSError("observed source truncated during the sampled read")
            if [path.stat().st_dev, path.stat().st_ino] != fid:
                raise OSError("observed source rotated during the sampled read")
        complete = sampled.rfind(b"\n") + 1
        data = sampled[:complete]
        next_offset = offset + complete
        changed = bool(data)
        generation = (
            hashlib.sha256(
                (
                    str(fid)
                    + ":"
                    + str(st.st_size)
                    + ":"
                    + hashlib.sha256(data).hexdigest()
                ).encode()
            ).hexdigest()
            if changed
            else prior.get("generation")
        )
        context = (
            {
                "path": str(path),
                "sha256": hashlib.sha256(data).hexdigest(),
                "offset": offset,
                "text": data.decode("utf-8", "replace"),
                "raw_b64": base64.b64encode(data).decode("ascii"),
            }
            if changed
            else prior.get("context", {})
        )
        return {
            "path": str(path),
            **context,
            "file_id": fid,
            "size": st.st_size,
            "next_offset": next_offset,
            "sample_start": offset,
            "partial_bytes": len(sampled) - complete,
            "dropped_bytes": offset - old,
            "changed": changed,
            "generation": generation,
        }
    except OSError as e:
        return {"path": str(path), "error": str(e), "changed": False}


def _role(cfg: WatcherConfig, path: str) -> tuple[str, str]:
    for source in cfg.observed_sources:
        if str(source.path) == path and source.role:
            return source.role, source.source_id
    name = Path(path).name.lower()
    if "manager" in name or "orchestr" in name:
        return "orchestrator", "derived"
    if "harness" in name or "event" in name:
        return "harness", "derived"
    return "subagent", "derived"


def _route(cfg: WatcherConfig, item: dict[str, Any]) -> None:
    if not item.get("changed") or "text" not in item:
        return
    role, source = _role(cfg, item["path"])
    records = []
    byte_cursor = item["offset"]
    data = base64.b64decode(item["raw_b64"]) if "raw_b64" in item else item["text"].encode("utf-8")
    raw_lines = data.split(b"\n")
    if raw_lines and raw_lines[-1] == b"":
        raw_lines.pop()
    for line_number, raw in enumerate(raw_lines):
        raw_bytes = raw + b"\n"
        byte_start = byte_cursor
        byte_cursor += len(raw_bytes)
        byte_end = byte_cursor
        line = raw_bytes.rstrip(b"\r\n")
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            log(
                cfg.runtime_root,
                "watcher",
                "ROUTE_REJECTED",
                {
                    "source_path": item["path"],
                    "offset": item["offset"],
                    "line": line_number,
                    "byte_start": byte_start,
                    "byte_end": byte_end,
                    "reason": "malformed json record",
                },
                event_id=_route_id(item, byte_start, byte_end, raw_bytes),
            )
            continue
        records.append((record, byte_start, byte_end, raw_bytes))
    for record, byte_start, byte_end, raw_bytes in records:
        if not isinstance(record, dict):
            log(
                cfg.runtime_root,
                "watcher",
                "ROUTE_REJECTED",
                {"source_path": item["path"], "reason": "json record is not an object"},
                event_id=_route_id(item, byte_start, byte_end, raw_bytes),
            )
            continue
        try:
            if role == "supervisor":
                log(
                    cfg.runtime_root,
                    "orchestrator",
                    "SUPERVISOR_ACTIVITY_INGESTED",
                    {
                        "source_path": item["path"],
                        "offset": item["offset"],
                        "byte_start": byte_start, "byte_end": byte_end,
                        "records": [record],
                    },
                    source,
                    event_id=_route_id(item, byte_start, byte_end, raw_bytes),
                )
            elif role == "subagent":
                log(
                    cfg.runtime_root,
                    "subagents",
                    "PROGRESS_INGESTED",
                    {
                        "source_path": item["path"],
                        "offset": item["offset"],
                        "byte_start": byte_start, "byte_end": byte_end,
                        "record": record,
                    },
                    str(
                        record.get("lane_id")
                        or record.get("subagent_id")
                        or record.get("doer")
                        or source
                    ),
                    event_id=_route_id(item, byte_start, byte_end, raw_bytes),
                )
            else:
                log(
                    cfg.runtime_root,
                    role,
                    "ACTIVITY_INGESTED",
                    {
                        "source_path": item["path"],
                        "offset": item["offset"],
                        "byte_start": byte_start, "byte_end": byte_end,
                        "records": [record],
                    },
                    source,
                    event_id=_route_id(item, byte_start, byte_end, raw_bytes),
                )
        except ValueError:
            digest = hashlib.sha256(raw_bytes).hexdigest()
            log(
                cfg.runtime_root,
                "watcher",
                "ROUTE_REJECTED",
                {
                    "source_path": item["path"],
                    "offset": item["offset"],
                    "byte_start": byte_start,
                    "byte_end": byte_end,
                    "encoded_size": len(raw_bytes),
                    "digest_scope": "raw_line_utf8",
                    "reason": "record exceeds route limit",
                    "sha256": digest,
                },
                event_id=_route_id(item, byte_start, byte_end, raw_bytes),
            )


def _route_id(item: dict[str, Any], begin: int, end: int, raw: bytes) -> str:
    identity = [item["path"], item.get("file_id"), begin, end, hashlib.sha256(raw).hexdigest()]
    return "hwr-" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def _attention_only_source(cfg: WatcherConfig, path: str) -> bool:
    """Attention timelines are ingested separately and must not drive evaluator reviews."""
    return any(
        str(source.path) == path
        and source.role == "harness"
        and source.path.name == "attention-events.jsonl"
        for source in cfg.observed_sources
    )


def _watcher_derived(record: object) -> bool:
    return isinstance(record, dict) and (
        record.get("type") == "HARNESS_WATCHER_ALERT"
        or (
            isinstance(record.get("identity"), str)
            and record["identity"].startswith("harness-watcher:")
        )
    )


def _is_stale_recovery(record: object) -> bool:
    if not isinstance(record, dict):
        return False
    if record.get("type") in {"CONTROLLER_ACTIVE", "CONTROLLER_EXITED"}:
        return isinstance(record.get("identity"), str)
    return (
        record.get("type") == "CONDITION_CLEARED"
        and isinstance(record.get("identity"), str)
        and isinstance(record.get("data"), dict)
        and record["data"].get("cleared_type") == "STALE_STATUS"
    )


def _quarantine_entry(
    item: dict[str, Any], record: dict[str, Any], now: float
) -> dict[str, Any]:
    """Keep the original evidence tuple: a later empty poll must still be citeable."""
    return {
        "identity": record["identity"],
        "first_seen_epoch": now,
        "record": record,
        "evidence": {
            "path": item["path"],
            "sha256": item["sha256"],
            "offset": item["offset"],
        },
        "released": False,
    }


def _evaluator_item(
    item: dict[str, Any], quarantine: dict[str, Any], now: float
) -> dict[str, Any]:
    """Hide watcher-derived records and quarantine new stale facts without rewriting raw state."""
    visible = dict(item)
    if "text" not in item:
        return visible
    lines = []
    for line in item["text"].splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            record = None
        if not isinstance(record, dict):
            continue
        if _watcher_derived(record):
            continue
        identity = record.get("identity")
        if record.get("type") == "STALE_STATUS" and isinstance(identity, str):
            if identity not in quarantine:
                quarantine[identity] = _quarantine_entry(item, record, now)
            continue
        if (
            _is_stale_recovery(record)
            and isinstance(identity, str)
            and identity in quarantine
            and not quarantine[identity].get("released")
        ):
            del quarantine[identity]
            continue
        lines.append(line)
    visible["text"] = "\n".join(lines) + ("\n" if lines else "")
    visible["changed"] = bool(item.get("changed")) and bool(lines)
    return visible


def _matured_stale_observations(
    quarantine: dict[str, Any], now: float, poll_interval_seconds: int
) -> list[dict[str, Any]]:
    """Release each unrecovered exact identity once, after one complete watcher interval."""
    released = []
    for identity, entry in quarantine.items():
        if (
            entry.get("released")
            or now - float(entry.get("first_seen_epoch", now)) < poll_interval_seconds
        ):
            continue
        evidence = entry.get("evidence", {})
        if not all(
            isinstance(evidence.get(key), expected)
            for key, expected in (("path", str), ("sha256", str), ("offset", int))
        ):
            continue
        record = entry.get("record")
        if not isinstance(record, dict):
            continue
        entry["released"] = True
        released.append(
            {
                "path": evidence["path"],
                "sha256": evidence["sha256"],
                "offset": evidence["offset"],
                "text": json.dumps(record, sort_keys=True) + "\n",
                "changed": True,
                "stale_status_quarantine_released": True,
                "quarantine_identity": identity,
                "quarantine_first_seen_epoch": entry["first_seen_epoch"],
            }
        )
    return released


def _identity_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        if value.startswith("windows-filetime:"):
            return datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(
                microseconds=int(value.split(":", 1)[1]) / 10
            )
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except (ValueError, OverflowError):
        return None


def _primary_loss(
    config: WatcherConfig, prior: dict[str, Any]
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    path = getattr(config, "primary_owner_identity_path", None)
    if path is None:
        return None, None
    try:
        identity = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"reason": "identity-unreadable", "identity_path": str(path)}, None
    if not isinstance(identity, dict):
        return {"reason": "identity-invalid", "identity_path": str(path)}, None
    if identity.get("exit_reason"):
        return {
            "reason": "primary-terminal",
            "identity_path": str(path),
            "exit_reason": identity.get("exit_reason"),
        }, identity
    expected = {
        key: identity.get(key) for key in ("owner", "managed_owner", "managed_watcher")
    }
    if not all(
        isinstance(v, dict)
        and isinstance(v.get("pid"), int)
        and _identity_time(v.get("created_utc")) is not None
        for v in expected.values()
    ):
        return {
            "reason": "identity-incomplete",
            "identity_path": str(path),
            "expected": expected,
        }, identity
    from orchestrator_harness.processes import process_snapshot

    snapshot = process_snapshot()
    if not snapshot.complete:
        return {
            "reason": "process-coverage-unknown",
            "identity_path": str(path),
            "expected": expected,
        }, identity
    live = {p.pid: p.created_utc for p in snapshot.processes}
    valid = {key: value for key, value in expected.items() if isinstance(value, dict)}
    mismatched = {
        key: value
        for key, value in valid.items()
        if live.get(value["pid"]) is None
        or _identity_time(value["created_utc"]) != live[value["pid"]]
    }
    return (
        (
            {
                "reason": "identity-mismatch",
                "identity_path": str(path),
                "expected": expected,
                "observed": {
                    str(pid): str(created)
                    for pid, created in live.items()
                    if pid in {v["pid"] for v in valid.values()}
                },
                "mismatched": mismatched,
            },
            identity,
        )
        if mismatched
        else (None, identity)
    )


def poll(config: WatcherConfig, evaluator: Evaluator | None = None) -> dict[str, Any]:
    if not settings.harness_watcher_active:
        return {"disabled": True, "alert": None}
    with lock(config.runtime_root, _cursor_path(config.runtime_root)):
        return _poll(config, evaluator)


def _poll(config: WatcherConfig, evaluator: Evaluator | None = None) -> dict[str, Any]:
    now = time.time()
    attention = ingest_attention(config)
    prior = _load_cursor(config.runtime_root)
    previous = prior.get("sources", {})
    quarantine = prior.get("stale_quarantine", {})
    quarantine = quarantine if isinstance(quarantine, dict) else {}
    items = []
    loss, _identity = _primary_loss(config, prior)
    primary_state = prior.get("primary_harness_loss")
    if loss is not None:
        fingerprint = hashlib.sha256(
            json.dumps(loss, sort_keys=True, default=str).encode()
        ).hexdigest()
        if (
            not isinstance(primary_state, dict)
            or primary_state.get("fingerprint") != fingerprint
        ):
            log(
                config.runtime_root,
                "watcher",
                "PRIMARY_HARNESS_LOST",
                {**loss, "detection_utc": datetime.now(timezone.utc).isoformat()},
            )
        primary_state = {"fingerprint": fingerprint, "loss": loss}
    else:
        primary_state = None
    for path in config.observed_log_roots:
        item = _read(path, config.max_tail_bytes, previous.get(str(path), {}))
        old = previous.get(str(path), {})
        last = now if item.get("changed") else old.get("last_changed_epoch", now)
        item["elapsed_since_change_seconds"] = max(0, now - last)
        items.append(item)
        if item.get("dropped_bytes"):
            log(config.runtime_root, "watcher", "SOURCE_TAIL_TRUNCATED", {
                "source_path": str(path), "byte_start": item["sample_start"] - item["dropped_bytes"],
                "byte_end": item["sample_start"], "dropped_bytes": item["dropped_bytes"],
            })
        _route(config, item)
    evaluator_items = []
    for item in items:
        if _attention_only_source(config, item["path"]):
            item["ordinary_evaluator_excluded"] = True
            visible = dict(item)
            visible["changed"] = False
            visible["text"] = ""
        else:
            visible = _evaluator_item(item, quarantine, now)
        evaluator_items.append(visible)
    evaluator_items.extend(
        _matured_stale_observations(quarantine, now, config.poll_interval_seconds)
    )
    evaluate = any(x.get("changed") for x in evaluator_items)
    for x, visible in zip(items, evaluator_items):
        old = previous.get(x.get("path"), {})
        x["no_progress_candidate"] = (
            not x.get("ordinary_evaluator_excluded")
            and not x.get("changed")
            and x["elapsed_since_change_seconds"] >= config.no_progress_seconds
            and old.get("no_progress_evaluated_generation") != x.get("generation")
        )
        visible["no_progress_candidate"] = x["no_progress_candidate"] and bool(
            visible.get("text", "")
        )
        evaluate = evaluate or visible["no_progress_candidate"]
    packet = {
        "schema": "harness-watcher-review-packet/v1",
        "packet_id": hashlib.sha256(
            json.dumps(evaluator_items, sort_keys=True).encode()
        ).hexdigest(),
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_since_change_seconds": max(
            (x["elapsed_since_change_seconds"] for x in items), default=0
        ),
        "observations": evaluator_items,
        "attention_findings": attention.get("findings", []),
        "constraints": {
            "no_progress_seconds": config.no_progress_seconds,
            "exclude": [
                "hardware/provider wait",
                "single retry",
                "firmware/server/test failure",
            ],
            "ownership_phase_rules": [dict(rule) for rule in OWNERSHIP_PHASE_RULES],
            "stale_status_quarantine_rule": dict(STALE_STATUS_QUARANTINE_RULE),
        },
    }
    saved = {}
    for x in items:
        old = previous.get(x.get("path"), {})
        if "error" in x:
            saved[x["path"]] = old
            continue
        saved[x["path"]] = {
            "file_id": x.get("file_id"),
            "offset": x.get("next_offset", old.get("offset", 0)),
            "partial_bytes": x.get("partial_bytes", 0),
            "generation": x.get("generation"),
            "context": {
                k: x[k] for k in ("path", "sha256", "offset", "text", "raw_b64") if k in x
            },
            "last_changed_epoch": now
            if x.get("changed")
            else old.get("last_changed_epoch", now),
            "last_changed_utc": packet["created_utc"]
            if x.get("changed")
            else old.get("last_changed_utc"),
            "no_progress_evaluated_generation": x.get("generation")
            if x.get("no_progress_candidate")
            else (
                None
                if x.get("changed")
                else old.get("no_progress_evaluated_generation")
            ),
        }
    _save_cursor(
        config.runtime_root,
        {
            "schema": "harness-watcher-cursor/v2",
            "sources": saved,
            "stale_quarantine": quarantine,
            "primary_harness_loss": primary_state,
        },
    )
    log(
        config.runtime_root,
        "watcher",
        "POLL",
        {
            "packet_id": packet["packet_id"],
            "sources": len(items),
            "evaluated": evaluate,
            "evaluator_enabled": config.evaluator_enabled,
        },
    )
    if not config.evaluator_enabled:
        log(
            config.runtime_root,
            "watcher",
            "EVALUATOR_SKIPPED",
            {"packet_id": packet["packet_id"], "mode": "diagnostic-only"},
        )
        return {
            "packet": packet,
            "alert": None,
            "skipped": True,
            "mode": "diagnostic-only",
            "primary_harness_loss": loss,
        }
    if not evaluate:
        return {
            "packet": packet,
            "alert": None,
            "skipped": True,
            "primary_harness_loss": loss,
        }
    try:
        if evaluator is None:
            raise ValueError("evaluator is required when enabled")
        verdict = evaluator.evaluate(packet)
    except ValueError as exc:
        # A malformed model verdict is evidence of an untrustworthy observation, not
        # a reason for this optional service to die.  The cursor was durably advanced
        # above, and a later ordinary change can be evaluated normally.
        log(
            config.runtime_root,
            "watcher",
            "EVALUATOR_REJECTED",
            {"packet_id": packet["packet_id"], "error": str(exc)},
        )
        return {"packet": packet, "alert": None, "evaluation_error": str(exc)}
    alert = save_alert(
        config.runtime_root,
        verdict,
        packet,
        dict(config.evaluator_identity),
    )
    log(
        config.runtime_root,
        "watcher",
        "EVALUATOR_OUTCOME",
        {
            "defect": verdict.defect,
            "kind": verdict.kind,
            "alert_id": alert and alert["alert_id"],
        },
    )
    return {"packet": packet, "verdict": verdict, "alert": alert}
