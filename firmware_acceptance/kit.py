"""Small deterministic validators used by the synthetic S2 acceptance tests."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


class AdmissionError(ValueError):
    """A call is unsafe or incomplete and must not be dispatched."""


FORBIDDEN = {"bootloader", "unlock", "mass_erase", "protection", "erase_all", "try_last"}
_CAPABILITY_KEYS = ("MCP_ENDPOINT", "MCP_COMMAND", "PYOCD_PROBE_UID", "PYOCD_TARGET", "BYO_MCP_ARTIFACT_ROOT", "MCP_CREDENTIAL", "MCP_TOKEN")


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
    return {"FIRMWARE_ACCEPTANCE_ROLE": "target-worker", **{key: "" for key in _CAPABILITY_KEYS}}


def _load_policy(policy_path: Path) -> dict[str, Any]:
    value = json.loads(policy_path.read_text(encoding="utf-8"))
    if value.get("default") != "deny" or not isinstance(value.get("methods"), dict):
        raise AdmissionError("invalid default-deny policy")
    return value


def _safe_child(root: Path, *parts: str) -> Path:
    root = root.resolve()
    candidate = root.joinpath(*parts).resolve()
    if candidate == root or root not in candidate.parents:
        raise AdmissionError("artifact path escapes confined root")
    if any(part in {"", ".", ".."} for part in parts):
        raise AdmissionError("invalid artifact path")
    return candidate


def _write_new(path: Path, value: dict[str, Any]) -> str:
    if path.exists() or path.is_symlink():
        raise AdmissionError("immutable artifact already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    try:
        with path.open("xb") as handle:
            handle.write(raw)
    except FileExistsError as exc:
        raise AdmissionError("immutable artifact already exists") from exc
    return hashlib.sha256(raw).hexdigest()


class AcceptanceBroker:
    """Controller-side materializer and immutable evidence chain; it never dispatches MCP itself."""

    def __init__(self, root: Path, seed: Path, policy_path: Path, templates_path: Path) -> None:
        self.root, self.seed, self.policy_path = root.resolve(), seed.resolve(), policy_path.resolve()
        self.templates = json.loads(templates_path.read_text(encoding="utf-8"))
        self.policy = _load_policy(self.policy_path)
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink():
            raise AdmissionError("broker root cannot be a symlink")
        validate_seed_manifest(self.seed)

    def materialize_seed(self, target_root: Path) -> None:
        target = target_root.resolve()
        if target.exists() and any(target.iterdir()):
            raise AdmissionError("target root must be fresh")
        target.mkdir(parents=True, exist_ok=True)
        for name in ("TARGET_SEED_MANIFEST.json", "TARGET_CHARTER.md", "PINNED_INPUTS.json", "TEST_CONTRACT.json", "EVIDENCE_SCHEMA.json"):
            destination = _safe_child(target, name)
            _write_new(destination, {"seed_copy_sha256": hashlib.sha256((self.seed / name).read_bytes()).hexdigest()}) if False else destination.write_bytes((self.seed / name).read_bytes())
        validate_seed_manifest(target)

    def controller_config(self, lane_id: str, inherited: dict[str, str] | None = None) -> dict[str, Any]:
        lane = next((item for item in self.templates["lanes"] if item["lane_id"] == lane_id), None)
        if not isinstance(lane, dict):
            raise AdmissionError("unknown lane template")
        env = dict(inherited or os.environ)
        if any(env.get(key) for key in _CAPABILITY_KEYS):
            raise AdmissionError("ambient physical capability is forbidden")
        lane_root = _safe_child(self.root, "lanes", lane_id)
        return {"lane": lane, "mcp_command": ["uv", "run", "--project", "<absolute-mcp-candidate>", "--locked", "pyocd-debug-mcp"], "environment": {"BYO_MCP_ARTIFACT_ROOT": str(_safe_child(lane_root, "artifacts")), "PYOCD_PROBE_UID": lane["probe_uid"], "PYOCD_TARGET": lane["target"]}, "worker_environment": worker_environment()}

    def record(self, stage: str, call_id: str, record: dict[str, Any], previous: tuple[str, str] | None = None) -> tuple[Path, str]:
        order = ("proposal", "policy-evaluation", "signed-decision", "authorization", "dispatch-admission", "dispatch", "raw-result", "returning-state-cleanup", "result")
        if stage not in order or not call_id or "/" in call_id or "\\" in call_id:
            raise AdmissionError("invalid immutable stage or call identity")
        index = order.index(stage)
        if index and previous is None:
            raise AdmissionError("immutable call artifacts are out of order")
        if previous is not None:
            previous_path = Path(previous[0]).resolve()
            if self.root not in previous_path.parents or not previous_path.is_file() or hashlib.sha256(previous_path.read_bytes()).hexdigest() != previous[1]:
                raise AdmissionError("prior immutable artifact is missing or tampered")
            record = {**record, "previous_path": previous[0], "previous_sha256": previous[1]}
        required = {"attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "route", "governing_hashes", "c1_reference", "identity"}
        if not required <= record.keys():
            raise AdmissionError("missing exact identity or lock bindings")
        path = _safe_child(self.root, "calls", call_id, f"{order.index(stage):02d}-{stage}.json")
        return path, _write_new(path, {"schema": "firmware-call-evidence/v1", "stage": stage, "call_id": call_id, **record})


def evaluate_call(call: dict[str, Any], *, now_monotonic: float, policy_path: Path | None = None) -> dict[str, Any]:
    """Validate a fully correlated broker request without performing a physical action."""
    required = {"call_id", "lane_id", "board", "probe_uid", "target", "profile", "method", "arguments", "proposal_sha256", "decision_sha256", "authorization_sha256", "deadline_monotonic", "plan", "permission"}
    missing = sorted(required - call.keys())
    if missing:
        raise AdmissionError(f"missing required call fields: {', '.join(missing)}")
    policy = _load_policy(policy_path or Path(__file__).with_name("MCP_METHOD_POLICY.json"))
    rule = policy["methods"].get(call["method"])
    if not isinstance(rule, dict):
        raise AdmissionError("default deny: unmapped MCP method")
    if any(token in json.dumps(call["arguments"], sort_keys=True).lower() for token in FORBIDDEN):
        raise AdmissionError("prohibited destructive or try-last parameter")
    if call["deadline_monotonic"] <= now_monotonic:
        raise AdmissionError("expired monotonic deadline")
    maximum = rule.get("maximum_duration_seconds")
    duration = call["plan"].get("max_operation_duration_seconds") if isinstance(call["plan"], dict) else None
    if not isinstance(maximum, int) or not isinstance(duration, int) or duration <= 0 or duration > maximum:
        raise AdmissionError("plan duration is absent, invalid, or exceeds policy")
    if call["deadline_monotonic"] - now_monotonic < duration + 60:
        raise AdmissionError("deadline cannot cover operation plus cleanup margin")
    if not isinstance(call["permission"], dict) or not call["permission"].get("granted"):
        raise AdmissionError("missing live permission")
    if call["method"] == "write_serial" and len(call["arguments"].get("bytes", [])) > 256:
        raise AdmissionError("UART write exceeds 256-byte limit")
    if call["method"] == "flash_firmware" and not call["arguments"].get("application_region_only"):
        raise AdmissionError("flash must prove application-region-only containment")
    if call["arguments"].get("dio2_dependent"):
        raise AdmissionError("DIO2-dependent work is denied while P.05 is unresolved")
    rf = call["arguments"].get("rf")
    if rf is not None:
        intent = policy["legal_rf_intent"]
        required_rf = {"electronic_admission", "frequency_hz", "power_dbm", "payload_bytes", "airtime_ms_per_60s", "campaign_minutes", "bandwidth_hz", "coding_rate", "spreading_factor"}
        if not isinstance(rf, dict) or not required_rf <= rf.keys() or not rf["electronic_admission"] or rf["frequency_hz"] != intent["frequency_hz"] or rf["power_dbm"] > intent["maximum_dbm"] or rf["payload_bytes"] > intent["maximum_payload_bytes"] or rf["airtime_ms_per_60s"] > intent["maximum_airtime_ms_per_60s"] or rf["campaign_minutes"] > intent["maximum_campaign_minutes"]:
            raise AdmissionError("RF call lacks authoritative bounded admission")
    return {"policy": "ALLOW", "evaluation_sha256": canonical_sha256({"call": call, "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest() if policy_path else "packaged"}), "maximum_duration_seconds": maximum}
