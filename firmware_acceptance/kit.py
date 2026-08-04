"""Small deterministic validators used by the synthetic S2 acceptance tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class AdmissionError(ValueError):
    """A call is unsafe or incomplete and must not be dispatched."""


FORBIDDEN = {"bootloader", "unlock", "mass_erase", "protection", "erase_all", "try_last"}
ALLOWED_METHODS = {
    "setup_overview/v1": {"mutating": False, "maximum": 30},
    "flash_application/v1": {"mutating": True, "maximum": 180},
    "reset/v1": {"mutating": True, "maximum": 30},
    "debug_snapshot/v1": {"mutating": False, "maximum": 60},
    "uart_capture/v1": {"mutating": False, "maximum": 60},
    "uart_write/v1": {"mutating": True, "maximum": 30},
}


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_seed_manifest(seed: Path) -> None:
    manifest = json.loads((seed / "TARGET_SEED_MANIFEST.json").read_text(encoding="utf-8"))
    expected = {"TARGET_CHARTER.md", "PINNED_INPUTS.json", "TEST_CONTRACT.json", "EVIDENCE_SCHEMA.json"}
    entries = manifest.get("files")
    if not isinstance(entries, list) or {item.get("path") for item in entries if isinstance(item, dict)} != expected:
        raise AdmissionError("seed manifest must enumerate exactly the four locked seed files")
    extras = {path.name for path in seed.iterdir() if path.is_file()} - expected - {"TARGET_SEED_MANIFEST.json"}
    if extras:
        raise AdmissionError(f"seed contains unmanifested files: {sorted(extras)}")
    for item in entries:
        payload = (seed / item["path"]).read_bytes()
        if hashlib.sha256(payload).hexdigest() != item.get("sha256"):
            raise AdmissionError(f"seed hash mismatch: {item['path']}")


def worker_environment() -> dict[str, str]:
    """The only environment passed to O/target workers: deliberately capability-free."""
    return {"FIRMWARE_ACCEPTANCE_ROLE": "target-worker", "MCP_ENDPOINT": "", "MCP_COMMAND": ""}


def evaluate_call(call: dict[str, Any], *, now_monotonic: float) -> dict[str, Any]:
    """Validate a fully correlated broker request without performing a physical action."""
    required = {"call_id", "lane_id", "board", "probe_uid", "target", "profile", "method", "arguments", "proposal_sha256", "decision_sha256", "authorization_sha256", "deadline_monotonic", "plan", "permission"}
    missing = sorted(required - call.keys())
    if missing:
        raise AdmissionError(f"missing required call fields: {', '.join(missing)}")
    if call["method"] not in ALLOWED_METHODS:
        raise AdmissionError("default deny: unmapped MCP method")
    if any(token in json.dumps(call["arguments"], sort_keys=True).lower() for token in FORBIDDEN):
        raise AdmissionError("prohibited destructive or try-last parameter")
    if call["deadline_monotonic"] <= now_monotonic:
        raise AdmissionError("expired monotonic deadline")
    maximum = ALLOWED_METHODS[call["method"]]["maximum"]
    duration = call["plan"].get("max_operation_duration_seconds") if isinstance(call["plan"], dict) else None
    if not isinstance(duration, int) or duration <= 0 or duration > maximum:
        raise AdmissionError("plan duration is absent, invalid, or exceeds policy")
    if call["deadline_monotonic"] - now_monotonic < duration + 60:
        raise AdmissionError("deadline cannot cover operation plus cleanup margin")
    if not isinstance(call["permission"], dict) or not call["permission"].get("granted"):
        raise AdmissionError("missing live permission")
    if call["method"] == "uart_write/v1" and len(call["arguments"].get("bytes", [])) > 256:
        raise AdmissionError("UART write exceeds 256-byte limit")
    return {"policy": "ALLOW", "evaluation_sha256": canonical_sha256(call), "maximum_duration_seconds": maximum}
