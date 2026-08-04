"""Small deterministic validators used by the synthetic S2 acceptance tests."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import subprocess
from pathlib import Path
from typing import Any

from orchestrator_harness.git_safety import GitSafetyError, declaration_from_status, validate_coding_result
from orchestrator_harness.lane_controller import InvocationError, load_invocation


class AdmissionError(ValueError):
    """A call is unsafe or incomplete and must not be dispatched."""


class SignatureVerifier:
    """Narrow production boundary; callers must supply a real verifier."""

    def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
        raise NotImplementedError


FORBIDDEN = {"bootloader", "unlock", "mass_erase", "protection", "erase_all", "try_last"}
_CAPABILITY_KEYS = ("MCP_ENDPOINT", "MCP_COMMAND", "PYOCD_PROBE_UID", "PYOCD_TARGET", "BYO_MCP_ARTIFACT_ROOT", "MCP_CREDENTIAL", "MCP_TOKEN")
_PINNED_SERVER_ROOT = Path("C:/Users/Jason/Documents/Jason/Orchestrator_Harness/plans/general-coding-harness/runtime/firmware-v2/worktrees/mcp-candidate")
_PINNED_SERVER_COMMIT = "f003f84a7df51cd8595a3203c62e225b21da2a22"
_SEED_FILES = ("TARGET_SEED_MANIFEST.json", "TARGET_CHARTER.md", "PINNED_INPUTS.json", "TEST_CONTRACT.json", "EVIDENCE_SCHEMA.json")


def canonical_sha256(value: Any) -> str:
    """The delegated-scope canonicalization is compact, sorted, UTF-8, and verbatim."""
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AdmissionError("value is not canonical JSON") from exc
    return hashlib.sha256(raw).hexdigest()


# This is intentionally data, not a permissive interpretation of goal.md.  C1 must
# copy this parsed object verbatim and may add only the separately validated bindings.
_USER_ISSUED_SCOPE = {
    "allowed_action_classes": ["probe_discovery_read", "connect_setup", "application_flash", "reset", "debug_halt_resume", "memory_register_read", "uart_session_io", "ble_gatt_test", "lora_ping_pong_test"],
    "expires_at_utc": None,
    "fixtures": ["STM-A", "STM-B", "NRF-A", "NRF-B"],
    "limits": {"application_flash": {"application_regions_only": True, "allow_bootloader_replace": False, "allow_mass_erase": False, "allow_protection_change": False, "allow_target_unlock": False}, "ble": {"max_tx_power_dbm": 0}, "lora": {"bandwidth_hz": 125000, "center_frequency_hz": 915000000, "coding_rate_denominator": 5, "max_campaign_minutes": 30, "max_payload_bytes": 64, "max_tx_airtime_ms_per_60s": 6000, "max_tx_power_dbm": 10, "spreading_factor_max": 10, "spreading_factor_min": 7}, "uart": {"max_write_bytes_per_call": 256}},
    "prohibited_action_classes": ["bootloader_replace", "mass_erase", "protection_change", "target_unlock", "destructive_recovery"],
    "schema_version": "user-hardware-authorization-v1",
}
_DELEGATED_KEYS = {"schema_version", "issuance_source", "canonical_user_scope_sha256", "user_issued_scope", "derived_bindings"}
_DERIVED_KEYS = {"c1_lock_id", "operative_goal_sha256", "stable_fixtures", "destructive_exclusions", "rf_limits", "mcp_server_pin", "mcp_method_policy", "governing_documents"}
_EFFECT_KEYS = {"schema", "effect_action_class", "target_operation_manifest", "electronic_admission", "limits"}


def _exact_reference(value: Any, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"path", "sha256"} or any(not isinstance(value[key], str) or not value[key] for key in value):
        raise AdmissionError(label + " must be an exact path/hash reference")
    return value


def _verify_exact_reference_file(value: Any, label: str) -> dict[str, str]:
    reference = _exact_reference(value, label)
    path = Path(reference["path"])
    reject_linked_path(path)
    if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != reference["sha256"]:
        raise AdmissionError(label + " reference drifted")
    return reference


def validate_delegated_authorization(reference: dict[str, str], *, policy_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """Load the closed C1 artifact without inventing any user authority."""
    reference = _exact_reference(reference, "delegated authorization")
    path = Path(reference["path"])
    reject_linked_path(path)
    if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != reference["sha256"]:
        raise AdmissionError("delegated authorization reference drifted")
    try: value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc: raise AdmissionError("delegated authorization is unreadable") from exc
    if not isinstance(value, dict) or set(value) != _DELEGATED_KEYS or value.get("schema_version") != "delegated-hardware-authorization-v1" or value.get("issuance_source") != "goal.md Section 11 USER_HARDWARE_AUTHORIZATION_V1":
        raise AdmissionError("delegated authorization is not the closed C1 shape")
    if value.get("user_issued_scope") != _USER_ISSUED_SCOPE or value.get("canonical_user_scope_sha256") != canonical_sha256(_USER_ISSUED_SCOPE):
        raise AdmissionError("delegated authorization changed the verbatim user scope")
    bindings = value.get("derived_bindings")
    if not isinstance(bindings, dict) or set(bindings) != _DERIVED_KEYS or not isinstance(bindings.get("c1_lock_id"), str) or not bindings["c1_lock_id"] or not isinstance(bindings.get("operative_goal_sha256"), str) or not bindings["operative_goal_sha256"]:
        raise AdmissionError("delegated derived bindings are incomplete")
    fixtures = bindings["stable_fixtures"]
    expected = manifest.get("fixtures")
    if not isinstance(fixtures, dict) or set(fixtures) != set(_USER_ISSUED_SCOPE["fixtures"]) or not isinstance(expected, dict): raise AdmissionError("delegated fixtures are not closed")
    for name, fixture in fixtures.items():
        if not isinstance(fixture, dict) or set(fixture) != {"probe_uid", "target", "profile"} or any(not isinstance(fixture[key], str) or not fixture[key] for key in fixture) or not isinstance(expected.get(name), dict) or any(fixture[key] != expected[name][key] for key in fixture):
            raise AdmissionError("delegated fixture does not match the locked fixture")
    if bindings["destructive_exclusions"] != _USER_ISSUED_SCOPE["prohibited_action_classes"] or bindings["rf_limits"] != {"ble": _USER_ISSUED_SCOPE["limits"]["ble"], "lora": _USER_ISSUED_SCOPE["limits"]["lora"]}:
        raise AdmissionError("delegated bindings expanded or changed user safety limits")
    for key in ("mcp_server_pin", "mcp_method_policy"):
        _verify_exact_reference_file(bindings[key], "delegated " + key)
    if Path(bindings["mcp_method_policy"]["path"]).resolve() != policy_path.resolve() or bindings["mcp_method_policy"]["sha256"] != hashlib.sha256(policy_path.read_bytes()).hexdigest():
        raise AdmissionError("delegated pin or policy binding drifted")
    governing = bindings["governing_documents"]
    if not isinstance(governing, dict) or set(governing) != {"goal", "generalization_spec", "implementation_roadmap", "execution_plan", "execution_readiness"}: raise AdmissionError("delegated governing bindings are incomplete")
    for key in governing: _verify_exact_reference_file(governing[key], "delegated governing " + key)
    return value


def _validate_scope_effect(effect: Any, scope: dict[str, Any]) -> None:
    if not isinstance(effect, dict) or set(effect) != _EFFECT_KEYS or effect.get("schema") != "firmware-call-effect/v1": raise AdmissionError("scope effect is malformed")
    if effect == {"schema": "firmware-call-effect/v1", "effect_action_class": None, "target_operation_manifest": None, "electronic_admission": None, "limits": None}: return
    action = effect.get("effect_action_class")
    if action not in {"ble_gatt_test", "lora_ping_pong_test"} or action not in scope["allowed_action_classes"] or action in scope["prohibited_action_classes"]: raise AdmissionError("scope effect action class is unauthorized")
    _verify_exact_reference_file(effect.get("target_operation_manifest"), "scope effect target manifest"); _verify_exact_reference_file(effect.get("electronic_admission"), "scope effect electronic admission")
    limits = effect.get("limits")
    maximum = scope["limits"]["ble" if action == "ble_gatt_test" else "lora"]
    if not isinstance(limits, dict) or set(limits) != set(maximum): raise AdmissionError("scope effect limits are not closed")
    for key, maximum_value in maximum.items():
        value = limits[key]
        if isinstance(maximum_value, bool) or not isinstance(value, type(maximum_value)) or (isinstance(maximum_value, int) and value > maximum_value) or (key in {"bandwidth_hz", "center_frequency_hz", "coding_rate_denominator"} and value != maximum_value): raise AdmissionError("scope effect exceeds delegated limits")
    if action == "lora_ping_pong_test" and limits.get("dio2_dependent") is not None: raise AdmissionError("LoRa effect cannot claim DIO2 authority")


def canonical_decision_payload(decision: dict[str, Any]) -> bytes:
    """The one O-decision signing representation: compact sorted UTF-8 JSON minus signature."""
    if not isinstance(decision, dict) or "signature" not in decision:
        raise AdmissionError("decision signature field is required")
    try:
        return json.dumps({key: value for key, value in decision.items() if key != "signature"}, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AdmissionError("decision is not canonical JSON") from exc


def canonical_raw_result_bytes(payload: Any) -> bytes:
    """The retained raw MCP result representation: UTF-8 canonical JSON, never a caller digest."""
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AdmissionError("raw result is not a canonical JSON payload") from exc


def raw_result_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_raw_result_bytes(payload)).hexdigest()


def reject_linked_path(path: Path) -> None:
    """Reject links/reparse points in the *spelled* existing path components."""
    original = Path(path)
    absolute = original if original.is_absolute() else Path.cwd() / original
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        if not current.exists() and not current.is_symlink():
            break
        try:
            info = current.lstat()
        except OSError as exc:
            raise AdmissionError("path component is unreadable") from exc
        junction = getattr(current, "is_junction", lambda: False)()
        reparse = bool(getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if current.is_symlink() or junction or reparse:
            raise AdmissionError("path contains a symlink, junction, or reparse point")


def validate_pinned_server() -> str:
    """Prove the controller is configured against the exact clean candidate tree."""
    reject_linked_path(_PINNED_SERVER_ROOT)
    if not _PINNED_SERVER_ROOT.is_dir() or _PINNED_SERVER_ROOT.is_symlink():
        raise AdmissionError("pinned MCP worktree is unavailable")
    dotenv = _PINNED_SERVER_ROOT / ".env"
    if dotenv.exists() or dotenv.is_symlink():
        raise AdmissionError("unreviewed pinned MCP .env is forbidden")
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=_PINNED_SERVER_ROOT, capture_output=True, text=True)
    status = subprocess.run(["git", "status", "--porcelain"], cwd=_PINNED_SERVER_ROOT, capture_output=True, text=True)
    if head.returncode or status.returncode or head.stdout.strip() != _PINNED_SERVER_COMMIT or status.stdout.strip():
        raise AdmissionError("pinned MCP worktree revision or cleanliness mismatch")
    return _PINNED_SERVER_COMMIT


def canonical_bound_operation(value: dict[str, Any]) -> dict[str, Any]:
    """Closed operation authority carried verbatim through every immutable stage."""
    required = {
        "server_commit", "method", "method_version", "arguments", "policy_sha256", "schema_sha256", "resource",
        "plan_sha256", "permission_sha256", "max_operation_duration_seconds", "permission_granted", "authorization_sha256", "authorization_path", "claim_sha256", "claim", "controller_owner", "call_id",
        "attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "route",
        "governing_hashes", "governing_documents", "c1_reference", "delegated_reference", "board_identity", "mcp_schema", "policy", "plan", "permission", "deadline_monotonic", "expires_monotonic", "seed_identity",
        "target_identity", "topology_key_release", "delegated_user_scope_sha256", "action_class", "scope_effect", "raw_result_sha256", "cleanup_owner",
    }
    if set(value) != required:
        raise AdmissionError("bound operation must be a closed complete authority object")
    if value["server_commit"] != _PINNED_SERVER_COMMIT or not isinstance(value["method"], str) or not isinstance(value["arguments"], dict):
        raise AdmissionError("bound operation server or method identity is invalid")
    if value["delegated_user_scope_sha256"] != canonical_sha256(_USER_ISSUED_SCOPE) or not isinstance(value["action_class"], str):
        raise AdmissionError("bound delegated scope or action class is invalid")
    _validate_scope_effect(value["scope_effect"], _USER_ISSUED_SCOPE)
    for key in ("method_version", "deadline_monotonic", "expires_monotonic"):
        if not isinstance(value[key], (int, float)) or isinstance(value[key], bool):
            raise AdmissionError("bound operation timing/version is invalid")
    if not all(__import__("math").isfinite(value[key]) for key in ("deadline_monotonic", "expires_monotonic")):
        raise AdmissionError("bound operation timing is not finite")
    if not isinstance(value["max_operation_duration_seconds"], int) or isinstance(value["max_operation_duration_seconds"], bool) or value["max_operation_duration_seconds"] <= 0 or value["permission_granted"] is not True or not isinstance(value["authorization_path"], str) or not value["authorization_path"]:
        raise AdmissionError("bound operation duplicated authority is invalid")
    for key in required - {"method", "arguments", "method_version", "deadline_monotonic", "expires_monotonic", "max_operation_duration_seconds", "permission_granted", "governing_hashes", "governing_documents", "c1_reference", "delegated_reference", "board_identity", "mcp_schema", "policy", "plan", "permission", "claim", "controller_owner", "seed_identity", "target_identity", "topology_key_release", "route", "delegated_user_scope_sha256", "action_class", "scope_effect", "raw_result_sha256", "cleanup_owner"}:
        if not isinstance(value[key], str) or not value[key]:
            raise AdmissionError("bound operation has an empty identity or hash")
    for key in ("c1_reference", "delegated_reference", "board_identity", "mcp_schema", "policy", "plan", "permission", "seed_identity", "target_identity", "topology_key_release"):
        ref = value[key]
        if not isinstance(ref, dict) or set(ref) != {"path", "sha256"} or not all(isinstance(ref[item], str) and ref[item] for item in ref):
            raise AdmissionError("bound operation reference is not exact")
    if not isinstance(value["governing_documents"], dict) or not value["governing_documents"] or any(not isinstance(ref, dict) or set(ref) != {"path", "sha256"} for ref in value["governing_documents"].values()):
        raise AdmissionError("bound governing references are not exact")
    if value["governing_hashes"] != {key: ref["sha256"] for key, ref in value["governing_documents"].items()} or value["policy_sha256"] != value["policy"]["sha256"] or value["schema_sha256"] != value["mcp_schema"]["sha256"] or value["plan_sha256"] != value["plan"]["sha256"] or value["permission_sha256"] != value["permission"]["sha256"]:
        raise AdmissionError("bound operation duplicated references drifted")
    claim = value["claim"]
    owner = claim.get("owner") if isinstance(claim, dict) else None
    if not isinstance(claim, dict) or set(claim) != {"resource", "path", "sha256", "owner"} or value["claim_sha256"] != claim["sha256"] or value["resource"] != value["board"] or claim["resource"] != value["board"] or claim["owner"] != value["controller_owner"] or value["cleanup_owner"] != value["controller_owner"] or not isinstance(owner, dict) or set(owner) != {"pid", "created_utc", "creation_identity"} or not isinstance(owner["pid"], int) or owner["pid"] <= 0 or not all(isinstance(owner[key], str) and owner[key] for key in ("created_utc", "creation_identity")):
        raise AdmissionError("bound claim/controller owner is not exact")
    if value["raw_result_sha256"] != "PENDING" and (not isinstance(value["raw_result_sha256"], str) or len(value["raw_result_sha256"]) != 64):
        raise AdmissionError("bound raw result identity is invalid")
    return json.loads(json.dumps(value, sort_keys=True))


def validate_seed_manifest(seed: Path) -> None:
    manifest = json.loads((seed / "TARGET_SEED_MANIFEST.json").read_text(encoding="utf-8"))
    expected = set(_SEED_FILES[1:])
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
    validate_campaign_contract(seed)


def validate_campaign_contract(seed: Path) -> None:
    """Consume the compact campaign contract; documentation alone is not admission evidence."""
    value = json.loads((seed / "TEST_CONTRACT.json").read_text(encoding="utf-8"))
    ids = ("H00", "H01", "H02", "H05", "S10", "S11", "S12", "S13", "A21", "A23", "A24", "D30", "D31", "D32", "D33", "D34", "D35", "D36")
    required = {"schema_version", "gating_ids", "definitions", "non_gating", "passed_registry", "selective_rerun", "review_test_findings"}
    if set(value) != required or tuple(value["gating_ids"]) != ids or not isinstance(value["definitions"], list) or len(value["definitions"]) != len(ids):
        raise AdmissionError("campaign contract is incomplete or not closed")
    definition_keys = {"id", "board", "family", "input", "action", "oracle", "failure_injection", "evidence_type", "dependency_inputs", "fingerprint_algorithm", "cleanup", "resume_contention", "failure_route"}
    if {item.get("id") for item in value["definitions"] if isinstance(item, dict)} != set(ids):
        raise AdmissionError("campaign contract IDs are not exact")
    for item in value["definitions"]:
        if not isinstance(item, dict) or set(item) != definition_keys or not isinstance(item["dependency_inputs"], list) or not item["dependency_inputs"] or item["fingerprint_algorithm"] != "sha256-canonical-json" or item["failure_route"] not in {"TARGET_LOCAL_REPAIR", "HARNESS_WATCHER_ABORT", "AUTHORIZED_SERVER_LIMITATION"}:
            raise AdmissionError("campaign definition is malformed")
    if value["passed_registry"] != {"schema": "firmware-passed-registry/v1", "key": "stable_test_id+dependency_fingerprint", "rerun_on": "fingerprint_change"}:
        raise AdmissionError("passed registry/selective rerun contract is invalid")


def worker_environment() -> dict[str, str]:
    """The only environment passed to O/target workers: deliberately capability-free."""
    return {"FIRMWARE_ACCEPTANCE_ROLE": "target-worker", **{key: "" for key in _CAPABILITY_KEYS}}


def finding_gate_fragment(role: str, workspace: Path) -> dict[str, object]:
    if role not in {"reviewer", "test_writer", "test_executor"}:
        raise AdmissionError("unsupported finding-gate role")
    root = workspace.resolve()
    if root.name != ".agent-workspace" or root.is_symlink():
        raise AdmissionError("finding gate requires a real lane .agent-workspace")
    return {"finding_gate": {"role": role, "path": str(_safe_child(root, "FINDINGS.json"))}}


def _load_policy(policy_path: Path) -> dict[str, Any]:
    value = json.loads(policy_path.read_text(encoding="utf-8"))
    methods = value.get("methods")
    if value.get("schema_version") != "firmware-mcp-method-policy/v2" or value.get("default") != "deny" or not isinstance(methods, dict) or len(methods) != 21:
        raise AdmissionError("invalid default-deny policy")
    states = {"BOOTSTRAPPED", "ROUTED", "SETUP_LOADED", "SETUP_PLAN_DISCLOSED", "SETUP_ACTION_READY", "SETUP_CONTINUATION", "SETUP_FIX_READY", "VALIDATION_LOADED", "READY", "OP_PLAN_DISCLOSED", "OP_ACTION_READY", "RETURNED", "ABORTED", "CLOSED"}
    for method, rule in methods.items():
        transitions = [rule.get("next"), *(rule.get("next_by_mode", {}) or {}).values(), *(rule.get("next_by_tool_name", {}) or {}).values(), *(rule.get("next_by_server_status", {}) or {}).values()]
        if not isinstance(method, str) or not isinstance(rule, dict) or not isinstance(rule.get("action_class"), str) or not isinstance(rule.get("parameters", {}).get("required_exact"), list) or not isinstance(rule.get("allowed_from"), list) or not set(rule["allowed_from"]) <= states or any(item is not None and item not in states for item in transitions):
            raise AdmissionError("method policy is incomplete")
    return value


def validate_manifest(manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("harness_commit") != "5d7c36e3dd38dc813c91f4462f4298c73883c59a" or manifest.get("mcp_server", {}).get("commit") != "f003f84a7df51cd8595a3203c62e225b21da2a22":
        raise AdmissionError("manifest pin mismatch")
    if manifest.get("resource_mirror", {}).get("manifest_rows") != 47 or len(manifest.get("datasheets", [])) != 4:
        raise AdmissionError("incomplete verified resource provenance")
    for item in [manifest.get("fixture_declaration"), *manifest.get("toolchain_locks", []), *manifest.get("datasheets", [])]:
        if not isinstance(item, dict) or not isinstance(item.get("sha256"), str) or not isinstance(item.get("bytes"), int):
            raise AdmissionError("malformed provenance identity")
    return manifest


def _safe_child(root: Path, *parts: str) -> Path:
    reject_linked_path(root)
    spelled = root.joinpath(*parts)
    reject_linked_path(spelled)
    root = root.resolve()
    candidate = spelled.resolve()
    if candidate == root or root not in candidate.parents:
        raise AdmissionError("artifact path escapes confined root")
    if any(part in {"", ".", ".."} for part in parts):
        raise AdmissionError("invalid artifact path")
    return candidate


def _write_new(path: Path, value: dict[str, Any]) -> str:
    reject_linked_path(path)
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

    def __init__(self, root: Path, seed: Path, policy_path: Path, templates_path: Path, manifest_path: Path | None = None) -> None:
        for item in (root, seed, policy_path, templates_path, manifest_path or Path(__file__).with_name("ACCEPTANCE_MANIFEST.json")): reject_linked_path(item)
        self.root, self.seed, self.policy_path = root.resolve(), seed.resolve(), policy_path.resolve()
        self.templates = json.loads(templates_path.read_text(encoding="utf-8"))
        self.policy = _load_policy(self.policy_path)
        self.manifest = validate_manifest(manifest_path or Path(__file__).with_name("ACCEPTANCE_MANIFEST.json"))
        self.root.mkdir(parents=True, exist_ok=True)
        self._target_identities: dict[Path, dict[str, str]] = {}
        if self.root.is_symlink():
            raise AdmissionError("broker root cannot be a symlink")
        validate_seed_manifest(self.seed)

    def materialize_seed(self, target_root: Path) -> None:
        reject_linked_path(target_root)
        target = target_root.resolve()
        target_parent = _safe_child(self.root, "targets")
        if target.parent != target_parent or target.exists() or target.is_symlink():
            raise AdmissionError("target root must be a fresh direct child of the confined targets root")
        target_parent.mkdir(parents=True, exist_ok=True)
        target.mkdir()
        for name in _SEED_FILES:
            destination = _safe_child(target, name)
            with destination.open("xb") as handle:
                handle.write((self.seed / name).read_bytes())
            destination.chmod(stat.S_IREAD)
        validate_seed_manifest(target)
        completed = subprocess.run(["git", "init", "-q"], cwd=target, check=False, capture_output=True, text=True)
        if completed.returncode:
            raise AdmissionError("disposable Git initialization failed")
        subprocess.run(["git", "add", "--", "."], cwd=target, check=True, capture_output=True, text=True)
        completed = subprocess.run(["git", "-c", "user.name=Firmware Acceptance", "-c", "user.email=firmware-acceptance@invalid", "commit", "-q", "-m", "seed"], cwd=target, check=False, capture_output=True, text=True)
        if completed.returncode:
            raise AdmissionError("disposable Git seed commit failed")
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=target, check=True, capture_output=True, text=True).stdout.strip()
        self._target_identities[target] = {"seed_sha256": canonical_sha256(json.loads((target / "TARGET_SEED_MANIFEST.json").read_text(encoding="utf-8"))), "initial_commit": head}

    def validate_target(self, target_root: Path) -> str:
        reject_linked_path(target_root)
        target = target_root.resolve()
        if target.parent != _safe_child(self.root, "targets") or target.is_symlink():
            raise AdmissionError("target escaped confined root")
        validate_seed_manifest(target)
        for name in _SEED_FILES:
            if target.joinpath(name).stat().st_mode & stat.S_IWRITE:
                raise AdmissionError("seed file is writable")
        result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=target, check=False, capture_output=True, text=True)
        if result.returncode:
            raise AdmissionError("target seed commit missing")
        identity = self._target_identities.get(target)
        if identity is None:
            raise AdmissionError("target seed identity is unknown")
        for name in _SEED_FILES:
            blob = subprocess.run(["git", "show", f"{identity['initial_commit']}:{name}"], cwd=target, capture_output=True)
            if blob.returncode or hashlib.sha256(blob.stdout).hexdigest() != hashlib.sha256((target / name).read_bytes()).hexdigest():
                raise AdmissionError("seed tree was substituted or rewritten")
        return result.stdout.strip()

    def controller_config(self, lane_id: str, inherited: dict[str, str] | None = None) -> dict[str, Any]:
        lane = next((item for item in self.templates["lanes"] if item["lane_id"] == lane_id), None)
        if not isinstance(lane, dict):
            raise AdmissionError("unknown lane template")
        env = dict(inherited or os.environ)
        if any(env.get(key) for key in _CAPABILITY_KEYS):
            raise AdmissionError("ambient physical capability is forbidden")
        lane_root = _safe_child(self.root, "lanes", lane_id)
        validate_pinned_server()
        server_root = _PINNED_SERVER_ROOT.resolve()
        firm, artifacts, logs, mcp = (_safe_child(lane_root, name) for name in (".firm", "artifacts", "logs", "mcp"))
        for path in (firm, artifacts, logs, mcp):
            path.mkdir(parents=True, exist_ok=True)
            if path.is_symlink():
                raise AdmissionError("lane root cannot be a symlink")
        return {"lane": lane, "working_directory": str(server_root), "mcp_command": ["uv", "run", "--project", str(server_root), "--locked", "pyocd-debug-mcp"], "stdio": {"stdin": "controller-only", "stdout": "controller-only-mcp-framing", "stderr_path": str(_safe_child(logs, "mcp.stderr.log"))}, "roots": {"firm": str(firm), "artifacts": str(artifacts), "logs": str(logs), "mcp": str(mcp)}, "environment": {"BYO_MCP_ARTIFACT_ROOT": str(artifacts), "PYOCD_PROBE_UID": lane["probe_uid"], "PYOCD_TARGET": lane["target"], "PYTHONPYCACHEPREFIX": str(_safe_child(self.root, "pycache", lane_id))}, "lifetime": {"owner": "C3-HARNESS", "requires_exact_process_identity": True, "cleanup_requires_reap": True}, "worker_environment": worker_environment()}

    def validate_lane_call(self, call: dict[str, Any]) -> dict[str, Any]:
        """Bind declared call identity to one locked, reviewed physical lane."""
        lanes = self.templates.get("lanes") if isinstance(self.templates, dict) else None
        fixtures = self.manifest.get("fixtures") if isinstance(self.manifest, dict) else None
        if not isinstance(lanes, list) or not isinstance(fixtures, dict): raise AdmissionError("lane template or manifest is malformed")
        matches = [row for row in lanes if isinstance(row, dict) and row.get("lane_id") == call.get("lane_id")]
        if len(matches) != 1 or set(matches[0]) != {"lane_id", "process_id", "firm_root", "artifact_root", "log_root", "probe_uid", "target", "profile", "serial_route", "endpoint"}:
            raise AdmissionError("lane template is unknown or ambiguous")
        lane = matches[0]; fixture = fixtures.get(lane["lane_id"])
        if not isinstance(fixture, dict) or set(fixture) != {"probe_uid", "target", "profile", "serial_route"} or any(lane[key] != fixture[key] for key in fixture):
            raise AdmissionError("lane template disagrees with locked manifest fixture")
        if any(call.get(key) != lane[key] for key in ("lane_id", "probe_uid", "target", "profile")) or call.get("board") != lane["lane_id"] or call.get("resource") != lane["lane_id"] or (call.get("route") is not None and call.get("route") != lane["serial_route"]):
            raise AdmissionError("call identity does not match selected physical lane")
        return lane

    def record_server_limitation(self, o_decision_path: Path, limitation_id: str, verifier: SignatureVerifier) -> dict[str, Any]:
        """Derive, once, an immutable server-limitation record; this never authorizes a bypass."""
        if not isinstance(limitation_id, str) or not limitation_id or any(char in limitation_id for char in "/\\"):
            raise AdmissionError("limitation identity is invalid")
        decision = self._load_limitation_decision(o_decision_path)
        required = {"schema", "limitation_id", "attempt_id", "lane_id", "session_id", "original_test", "classification", "call_chain", "session_terminal", "process_evidence", "pinned_source", "attribution", "attribution_evidence", "alternatives", "substitute", "physical_certification", "o_decision", "created_utc", "signature"}
        if set(decision) != required or decision["schema"] != "firmware-server-limitation-decision/v1" or decision["limitation_id"] != limitation_id or decision["classification"] != "AUTHORIZED_SERVER_LIMITATION" or decision["physical_certification"] != {"status":"NOT_CERTIFIED"} or not isinstance(decision["signature"], str) or not decision["signature"] or not isinstance(decision.get("o_decision"), dict) or set(decision["o_decision"]) != {"public_key", "protected_suite"} or not isinstance(decision["o_decision"].get("public_key"), str) or not verifier.verify(canonical_decision_payload(decision), decision["signature"], decision["o_decision"]["public_key"]):
            raise AdmissionError("server limitation decision is not closed or signed")
        if not all(isinstance(decision[key], str) and decision[key] for key in ("attempt_id", "lane_id", "session_id", "original_test", "created_utc")):
            raise AdmissionError("server limitation identities are incomplete")
        pinned = decision["pinned_source"]
        if not isinstance(pinned, dict) or set(pinned) != {"commit", "immutable"} or pinned != {"commit":_PINNED_SERVER_COMMIT, "immutable":True}:
            raise AdmissionError("server limitation cannot edit or repin the immutable server")
        if decision["attribution"] not in {"PINNED_SERVER_SOURCE"}:
            raise AdmissionError("server limitation attribution is not pinned-server source")
        chain = decision["call_chain"]
        stages = ("proposal", "policy-evaluation", "signed-decision", "authorization", "dispatch-admission", "dispatch", "raw-result")
        if not isinstance(chain, list) or len(chain) != len(stages) or any(not isinstance(item, dict) or set(item) != {"path", "sha256", "stage"} for item in chain) or tuple(item["stage"] for item in chain) != stages:
            raise AdmissionError("limitation requires a dispatched raw server failure chain")
        records: list[dict[str, Any]] = []
        for index, item in enumerate(chain):
            self._verify_limitation_reference({"path":item["path"], "sha256":item["sha256"]}, "limitation call chain")
            value = json.loads(Path(item["path"]).read_text(encoding="utf-8"))
            if value.get("stage") != stages[index] or value.get("attempt_id") != decision["attempt_id"] or value.get("lane_id") != decision["lane_id"] or value.get("call_id") is None or (index and (value.get("previous_path") != chain[index - 1]["path"] or value.get("previous_sha256") != chain[index - 1]["sha256"])):
                raise AdmissionError("limitation call chain identity or predecessor drifted")
            records.append(value)
        if len({value["call_id"] for value in records}) != 1 or any(value.get("bound_operation", {}).get("server_commit") != _PINNED_SERVER_COMMIT for value in records):
            raise AdmissionError("limitation chain is not one pinned-server call")
        raw_value = records[-1]
        raw_payload = raw_value.get("raw_result")
        if raw_value.get("outcome") != "FAIL" or not isinstance(raw_payload, dict) or "transport_failure" in raw_payload or not isinstance(raw_payload.get("result"), dict):
            raise AdmissionError("limitation requires raw pinned-server failure, not pre-dispatch rejection")
        self._verify_pinned_source_attribution(decision, records, chain[-1])
        terminal = decision["session_terminal"]
        if not isinstance(terminal, dict) or set(terminal) != {"path", "sha256"}: raise AdmissionError("terminal session evidence is incomplete")
        self._verify_limitation_reference(terminal, "terminal session")
        terminal_value = json.loads(Path(terminal["path"]).read_text(encoding="utf-8"))
        if not isinstance(terminal_value, dict) or set(terminal_value) != {"schema", "session_id", "session_open_path", "session_open_sha256", "terminal_state", "reason", "natural_eof", "exact_reaped", "transport_cleanup", "helpers_stopped", "claim_released"} or terminal_value.get("schema") != "firmware-session-terminal/v1" or terminal_value.get("session_id") != decision["session_id"] or terminal_value.get("terminal_state") not in {"ABORTED", "CLOSED"} or not terminal_value.get("exact_reaped") or not terminal_value.get("helpers_stopped") or not terminal_value.get("claim_released"):
            raise AdmissionError("terminal session evidence does not close this session")
        process = decision["process_evidence"]
        if not isinstance(process, dict) or set(process) != {"path", "sha256"}: raise AdmissionError("terminal process cleanup is incomplete")
        self._verify_limitation_reference(process, "terminal process")
        process_value = json.loads(Path(process["path"]).read_text(encoding="utf-8"))
        if process_value.get("session_id") != decision["session_id"] or process_value.get("exact_reaped") is not True or process_value.get("helpers_stopped") is not True or process_value.get("claim_released") is not True:
            raise AdmissionError("terminal process evidence is not exact cleanup evidence")
        alternatives = decision["alternatives"]
        order = ["PARTIAL_MCP", "PINNED_COMPONENT_INTEGRATION", "CANDIDATE_BOUNDARY_UNIT"]
        if not isinstance(alternatives, list) or [item.get("kind") for item in alternatives if isinstance(item, dict)] != order or any(not isinstance(item, dict) or set(item) != {"kind", "available", "reason", "evidence"} or not isinstance(item["available"], bool) or not isinstance(item["reason"], str) or not item["reason"] for item in alternatives): raise AdmissionError("limitation alternatives are not closed and ordered")
        for item in alternatives: self._verify_limitation_reference(item["evidence"], "alternative evidence")
        available = next((item for item in alternatives if item["available"]), None)
        substitute = decision["substitute"]
        if available is None or not isinstance(substitute, dict) or set(substitute) != {"kind", "stable_id", "assignment", "result"} or substitute["kind"] != available["kind"] or not isinstance(substitute.get("stable_id"), str) or not substitute["stable_id"]:
            raise AdmissionError("limitation substitute is not the first safe available alternative")
        self._verify_limitation_reference(substitute["assignment"], "substitute assignment"); self._verify_limitation_reference(substitute["result"], "substitute result")
        self._verify_broker_reference(substitute["assignment"], "substitute assignment"); self._verify_broker_reference(substitute["result"], "substitute result")
        self._verify_substitute_identity(substitute, decision)
        protected = decision["o_decision"].get("protected_suite") if isinstance(decision["o_decision"], dict) else None
        self._verify_limitation_reference(protected, "protected suite")
        protected_value = json.loads(Path(protected["path"]).read_text(encoding="utf-8"))
        protected_ids = protected_value.get("protected_ids") if isinstance(protected_value, dict) else None
        if not isinstance(protected_value, dict) or protected_value.get("schema") != "c1-protected-test-ids/v1" or protected_value.get("c1_reference") != records[0].get("c1_reference") or not isinstance(protected_ids, list) or not protected_ids or len(set(protected_ids)) != len(protected_ids) or any(not isinstance(value, str) or not value for value in protected_ids): raise AdmissionError("protected suite artifact is not closed")
        # Limitation admission is entirely structural.  Free-text rationale may
        # explain a decision, but it is not an authority or a denial heuristic.
        if decision["original_test"] == substitute["stable_id"] or decision["original_test"] in protected_ids or substitute["stable_id"] in protected_ids:
            raise AdmissionError("limitation cannot substitute protected tests or bypass physical authority")
        result = {key: decision[key] for key in required - {"signature"}}
        result["schema"] = "firmware-server-limitation/v1"
        result["o_decision"] = {"path":str(o_decision_path.resolve()),"sha256":hashlib.sha256(o_decision_path.read_bytes()).hexdigest()}
        path = _safe_child(self.root, "hil", decision["lane_id"], "server-limitations", decision["attempt_id"], limitation_id + ".json")
        digest = _write_new(path, result)
        return {**result, "path":str(path), "raw_sha256":digest}

    def _load_limitation_decision(self, path: Path) -> dict[str, Any]:
        reject_linked_path(path)
        if path.is_symlink() or not path.is_file(): raise AdmissionError("O limitation decision is unsafe")
        try: value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc: raise AdmissionError("O limitation decision is unreadable") from exc
        if not isinstance(value, dict): raise AdmissionError("O limitation decision is not an object")
        return value

    @staticmethod
    def _verify_limitation_reference(reference: dict[str, Any], label: str) -> None:
        if not isinstance(reference, dict) or set(reference) != {"path", "sha256"} or not all(isinstance(reference.get(key), str) and reference[key] for key in ("path", "sha256")):
            raise AdmissionError(label + " reference is not closed")
        path = Path(reference["path"]); reject_linked_path(path)
        if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != reference["sha256"]: raise AdmissionError(label + " reference drifted")

    def _verify_broker_reference(self, reference: dict[str, Any], label: str) -> None:
        if self.root not in Path(reference["path"]).resolve().parents:
            raise AdmissionError(label + " escapes the authorized broker evidence root")

    def _limitation_c3_path(self, decision: dict[str, Any], name: str) -> Path:
        return _safe_child(self.root, "hil", decision["lane_id"], "server-limitations", decision["attempt_id"], decision["limitation_id"], "c3-harness", name)

    def _verify_c3_completion(self, invocation_ref: Any, status_ref: Any, result_ref: Any, lane_id: str, worker_id: str, expected_status: str, expected_result: str, credit_token: str) -> dict[str, Any]:
        self._verify_limitation_reference(invocation_ref, "C3 candidate invocation"); self._verify_limitation_reference(status_ref, "C3 controller status"); self._verify_limitation_reference(result_ref, "C3 worker result")
        try:
            reject_linked_path(Path(expected_status)); reject_linked_path(Path(expected_result))
            expected_status_path, expected_result_path = Path(expected_status).resolve(), Path(expected_result).resolve()
            invocation = load_invocation(Path(invocation_ref["path"]))
            status = json.loads(Path(status_ref["path"]).read_text(encoding="utf-8")); result = json.loads(Path(result_ref["path"]).read_text(encoding="utf-8"))
            validation = status.get("result_validation") if isinstance(status, dict) else None
            creation_identity = (status.get("controller_created_utc") or status.get("controller_started_utc")) if isinstance(status, dict) else None
            if not isinstance(credit_token, str) or not credit_token or invocation.invocation_schema != "orchestrator-coding-invocation/v1" or invocation.worker_invocation_id != worker_id or invocation.lane_id != lane_id or invocation.repository is None or invocation.run_root != invocation.repository.worktree_root or invocation.status_path.resolve() != expected_status_path or (invocation.repository.worktree_root / ".agent-workspace" / "RESULT.json").resolve() != expected_result_path or Path(status_ref["path"]).resolve() != expected_status_path or Path(result_ref["path"]).resolve() != expected_result_path or not isinstance(status, dict) or status.get("schema") != "orchestrator-lane-controller/v1" or status.get("invocation_schema") != "orchestrator-coding-invocation/v1" or status.get("state") != "CODEX_EXITED" or status.get("exit_code") != 0 or status.get("declared_lane_id") != lane_id or status.get("worker_invocation_id") != worker_id or status.get("held_resource_claims") != [] or status.get("result_valid") is not True or not isinstance(validation, dict) or validation.get("state") != "VALID" or validation.get("path") != result_ref["path"] or validation.get("sha256") != result_ref["sha256"] or not isinstance(status.get("controller_pid"), int) or status["controller_pid"] <= 0 or not isinstance(creation_identity, str) or not creation_identity:
                raise AdmissionError("C3 terminal status is not exact")
            declaration = declaration_from_status(status, Path(status["worktree_root"]))
            workspace = declaration.worktree_root / ".agent-workspace"
            if declaration != invocation.repository or Path(result_ref["path"]).resolve() != (workspace / "RESULT.json").resolve():
                raise AdmissionError("C3 references are not the controller's real status and result")
            identity = validate_coding_result(result, lane_id=lane_id, worker_invocation_id=worker_id, declaration=declaration)
            if result.get("outcome") != "PASS" or validation.get("commit") != identity.head_commit:
                raise AdmissionError("C3 worker result is not current PASS")
            credit = {"name":"firmware-limitation-credit", "command":credit_token, "outcome":"PASS"}
            if sum(check == credit for check in result.get("checks", [])) != 1:
                raise AdmissionError("C3 worker result lacks the exact credited PASS check")
            return {"pid":status["controller_pid"], "creation_identity":creation_identity}
        except (OSError, ValueError, KeyError, json.JSONDecodeError, GitSafetyError, InvocationError) as exc:
            raise AdmissionError("C3 controller/result validation failed") from exc

    def _verify_pinned_source_attribution(self, decision: dict[str, Any], records: list[dict[str, Any]], raw_reference: dict[str, Any]) -> None:
        reference = decision["attribution_evidence"]
        self._verify_limitation_reference(reference, "pinned-source attribution")
        value = json.loads(Path(reference["path"]).read_text(encoding="utf-8"))
        keys = {"schema", "attempt_id", "lane_id", "session_id", "call_id", "raw_result", "source_path", "source_sha256", "input_signature", "failure_signature", "predicate", "diagnostic_assignment", "diagnostic_execution"}
        if not isinstance(value, dict) or set(value) != keys or value.get("schema") != "firmware-pinned-server-attribution/v1":
            raise AdmissionError("pinned-source attribution is not closed")
        if any(value.get(key) != decision[key] for key in ("attempt_id", "lane_id", "session_id")) or value.get("call_id") != records[-1].get("call_id") or value.get("raw_result") != {"path":raw_reference["path"], "sha256":raw_reference["sha256"]}:
            raise AdmissionError("pinned-source attribution does not bind the failed call")
        source_path = value.get("source_path")
        if not isinstance(source_path, str) or not source_path or Path(source_path).is_absolute() or ".." in Path(source_path).parts:
            raise AdmissionError("pinned-source attribution path is unsafe")
        shown = subprocess.run(["git", "show", _PINNED_SERVER_COMMIT + ":" + source_path], cwd=_PINNED_SERVER_ROOT, capture_output=True)
        if shown.returncode or value.get("source_sha256") != hashlib.sha256(shown.stdout).hexdigest():
            raise AdmissionError("pinned-source attribution source bytes drifted")
        raw_signature = raw_result_sha256(records[-1]["raw_result"])
        input_signature = canonical_sha256(records[-1]["bound_operation"])
        if value.get("failure_signature") != raw_signature or value.get("input_signature") != input_signature or value.get("predicate") != "PINNED_COMPONENT_REPRODUCTION":
            raise AdmissionError("pinned-source attribution signatures do not bind retained bytes")
        diagnostic_assignment_ref = value.get("diagnostic_assignment")
        self._verify_limitation_reference(diagnostic_assignment_ref, "pinned component diagnostic assignment")
        diagnostic_assignment_path = Path(diagnostic_assignment_ref["path"]).resolve()
        if diagnostic_assignment_path != self._limitation_c3_path(decision, "DIAGNOSTIC_ASSIGNMENT.json"):
            raise AdmissionError("pinned component diagnostic assignment is outside the exact attempt subtree")
        diagnostic_assignment = json.loads(diagnostic_assignment_path.read_text(encoding="utf-8"))
        assignment_keys = {"schema", "attempt_id", "lane_id", "session_id", "call_id", "server_commit", "source_path", "source_sha256", "input_signature", "failure_signature", "predicate", "worker_invocation_id", "expected_status_path", "expected_result_path"}
        if not isinstance(diagnostic_assignment, dict) or set(diagnostic_assignment) != assignment_keys or diagnostic_assignment.get("schema") != "firmware-pinned-component-diagnostic-assignment/v1" or diagnostic_assignment.get("server_commit") != _PINNED_SERVER_COMMIT or any(diagnostic_assignment.get(key) != value.get(key) for key in ("attempt_id", "lane_id", "session_id", "call_id", "source_path", "source_sha256", "input_signature", "failure_signature", "predicate")) or not all(isinstance(diagnostic_assignment.get(key), str) and diagnostic_assignment[key] for key in ("worker_invocation_id", "expected_status_path", "expected_result_path")):
            raise AdmissionError("pinned component diagnostic assignment is not closed")
        diagnostic_ref = value.get("diagnostic_execution")
        self._verify_limitation_reference(diagnostic_ref, "pinned component diagnostic")
        diagnostic_path = Path(diagnostic_ref["path"]).resolve()
        if diagnostic_path != self._limitation_c3_path(decision, "DIAGNOSTIC.json"):
            raise AdmissionError("pinned component diagnostic is outside the exact attempt subtree")
        diagnostic = json.loads(diagnostic_path.read_text(encoding="utf-8"))
        diagnostic_keys = {"schema", "attempt_id", "lane_id", "session_id", "call_id", "controller_owner", "server_commit", "source_path", "source_sha256", "input_signature", "failure_signature", "predicate", "target_independent", "locked_environment", "assignment_path", "assignment_sha256", "candidate_invocation", "controller_status", "worker_result", "worker_invocation_id", "outcome"}
        if not isinstance(diagnostic, dict) or set(diagnostic) != diagnostic_keys or diagnostic.get("schema") != "firmware-pinned-component-diagnostic/v1" or diagnostic.get("outcome") != "PASS" or diagnostic.get("target_independent") is not True or any(diagnostic.get(key) != value.get(key) for key in ("attempt_id", "lane_id", "session_id", "call_id", "source_path", "source_sha256", "input_signature", "failure_signature", "predicate")) or diagnostic.get("server_commit") != _PINNED_SERVER_COMMIT or diagnostic.get("assignment_path") != diagnostic_assignment_ref["path"] or diagnostic.get("assignment_sha256") != diagnostic_assignment_ref["sha256"] or diagnostic.get("worker_invocation_id") != diagnostic_assignment["worker_invocation_id"]:
            raise AdmissionError("pinned component diagnostic is not a causal witness")
        environment = diagnostic.get("locked_environment")
        if not isinstance(environment, dict) or set(environment) != {"server_commit", "policy_sha256", "schema_sha256"} or environment != {"server_commit":_PINNED_SERVER_COMMIT, "policy_sha256":records[-1]["bound_operation"].get("policy_sha256"), "schema_sha256":records[-1]["bound_operation"].get("schema_sha256")}:
            raise AdmissionError("pinned component diagnostic environment drifted")
        controller_identity = self._verify_c3_completion(diagnostic["candidate_invocation"], diagnostic["controller_status"], diagnostic["worker_result"], diagnostic["lane_id"], diagnostic["worker_invocation_id"], diagnostic_assignment["expected_status_path"], diagnostic_assignment["expected_result_path"], diagnostic_assignment_ref["sha256"])
        if diagnostic.get("controller_owner") != controller_identity:
            raise AdmissionError("pinned component diagnostic controller identity drifted")

    def _verify_substitute_identity(self, substitute: dict[str, Any], decision: dict[str, Any]) -> None:
        """Bind the C3-HARNESS substitute artifacts without granting launch authority."""
        assignment_ref, result_ref = substitute["assignment"], substitute["result"]
        try:
            assignment = json.loads(Path(assignment_ref["path"]).read_text(encoding="utf-8"))
            result = json.loads(Path(result_ref["path"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AdmissionError("substitute assignment or result is unreadable") from exc
        assignment_keys = {"schema", "limitation_id", "attempt_id", "lane_id", "session_id", "kind", "stable_id", "assignment_id", "worker_invocation_id", "expected_status_path", "expected_result_path"}
        result_keys = {"schema", "limitation_id", "attempt_id", "lane_id", "session_id", "kind", "stable_id", "assignment_id", "result_id", "assignment_path", "assignment_sha256", "candidate_invocation", "controller_identity", "controller_status", "candidate_result", "execution", "worker_result", "outcome"}
        if not isinstance(assignment, dict) or set(assignment) != assignment_keys or assignment.get("schema") != "firmware-limitation-substitute-assignment/v1":
            raise AdmissionError("substitute assignment is not a closed identity artifact")
        if not isinstance(result, dict) or set(result) != result_keys or result.get("schema") != "firmware-limitation-substitute-result/v1":
            raise AdmissionError("substitute result is not a closed identity artifact")
        if Path(assignment_ref["path"]).resolve() != self._limitation_c3_path(decision, "ASSIGNMENT.json") or Path(result_ref["path"]).resolve() != self._limitation_c3_path(decision, "RESULT.json"):
            raise AdmissionError("substitute artifacts are outside their exact C3 attempt subtree")
        identity = {"limitation_id":decision["limitation_id"], "attempt_id":decision["attempt_id"], "lane_id":decision["lane_id"], "session_id":decision["session_id"], "kind":substitute["kind"], "stable_id":substitute["stable_id"]}
        if any(assignment.get(key) != value or result.get(key) != value for key, value in identity.items()):
            raise AdmissionError("substitute artifacts do not bind this limitation attempt")
        if not all(isinstance(item.get(key), str) and item[key] for item in (assignment, result) for key in ("assignment_id",)) or not all(isinstance(assignment.get(key), str) and assignment[key] for key in ("worker_invocation_id", "expected_status_path", "expected_result_path")) or not isinstance(result.get("result_id"), str) or not result["result_id"] or result.get("assignment_id") != assignment["assignment_id"]:
            raise AdmissionError("substitute artifact identities are incomplete")
        if result.get("assignment_path") != assignment_ref["path"] or result.get("assignment_sha256") != assignment_ref["sha256"]:
            raise AdmissionError("substitute result does not reference its exact assignment")
        if result.get("outcome") != "PASS":
            raise AdmissionError("substitute result did not pass")
        execution_ref = result.get("execution")
        self._verify_limitation_reference(execution_ref, "C3 substitute execution")
        execution_path = Path(execution_ref["path"]).resolve()
        if execution_path != self._limitation_c3_path(decision, "EXECUTION.json"):
            raise AdmissionError("C3 substitute execution is outside the exact attempt subtree")
        execution = json.loads(execution_path.read_text(encoding="utf-8"))
        execution_keys = {"schema", "attempt_id", "lane_id", "session_id", "limitation_id", "assignment_path", "assignment_sha256", "launch", "candidate_invocation", "controller_identity", "controller_status", "worker_result", "worker_invocation_id", "execution_id", "outcome"}
        if not isinstance(execution, dict) or set(execution) != execution_keys or execution.get("schema") != "firmware-limitation-substitute-execution/v1" or execution.get("outcome") != "PASS" or any(execution.get(key) != decision[key] for key in ("attempt_id", "lane_id", "session_id", "limitation_id")) or execution.get("assignment_path") != assignment_ref["path"] or execution.get("assignment_sha256") != assignment_ref["sha256"] or execution.get("worker_invocation_id") != assignment["worker_invocation_id"] or not isinstance(execution.get("execution_id"), str) or not execution["execution_id"]:
            raise AdmissionError("substitute execution does not bind this assignment")
        launch_ref = execution.get("launch")
        self._verify_limitation_reference(launch_ref, "C3 substitute launch")
        if Path(launch_ref["path"]).resolve() != self._limitation_c3_path(decision, "LAUNCH.json"):
            raise AdmissionError("C3 substitute launch is outside the exact attempt subtree")
        launch = json.loads(Path(launch_ref["path"]).read_text(encoding="utf-8"))
        launch_keys = {"schema", "attempt_id", "lane_id", "session_id", "limitation_id", "assignment_path", "assignment_sha256", "candidate_invocation", "controller_identity", "controller_status", "worker_result", "worker_invocation_id"}
        if not isinstance(launch, dict) or set(launch) != launch_keys or launch.get("schema") != "firmware-limitation-c3-launch/v1" or any(launch.get(key) != assignment.get(key) for key in ("attempt_id", "lane_id", "session_id", "limitation_id", "worker_invocation_id")) or launch.get("assignment_path") != assignment_ref["path"] or launch.get("assignment_sha256") != assignment_ref["sha256"]:
            raise AdmissionError("C3 substitute launch identity is not exact")
        controller_identity = self._verify_c3_completion(launch["candidate_invocation"], launch["controller_status"], launch["worker_result"], launch["lane_id"], launch["worker_invocation_id"], assignment["expected_status_path"], assignment["expected_result_path"], assignment_ref["sha256"])
        if launch.get("controller_identity") != controller_identity or execution.get("candidate_invocation") != launch["candidate_invocation"] or execution.get("controller_identity") != controller_identity or execution.get("controller_status") != launch["controller_status"] or execution.get("worker_result") != launch["worker_result"]:
            raise AdmissionError("C3 substitute controller is not the validated controller")
        worker_ref = result.get("worker_result")
        self._verify_limitation_reference(worker_ref, "C3 substitute worker result")
        if Path(worker_ref["path"]).resolve() != self._limitation_c3_path(decision, "WORKER_RESULT.json"):
            raise AdmissionError("C3 substitute worker result is outside the exact attempt subtree")
        worker = json.loads(Path(worker_ref["path"]).read_text(encoding="utf-8"))
        worker_keys = {"schema", "attempt_id", "lane_id", "session_id", "limitation_id", "assignment_path", "assignment_sha256", "launch", "execution", "candidate_invocation", "controller_identity", "controller_status", "candidate_result", "worker_invocation_id", "outcome"}
        if not isinstance(worker, dict) or set(worker) != worker_keys or worker.get("schema") != "firmware-limitation-c3-worker-result/v1" or worker.get("outcome") != "PASS" or any(worker.get(key) != decision[key] for key in ("attempt_id", "lane_id", "session_id", "limitation_id")) or worker.get("assignment_path") != assignment_ref["path"] or worker.get("assignment_sha256") != assignment_ref["sha256"] or worker.get("launch") != launch_ref or worker.get("execution") != execution_ref or worker.get("candidate_invocation") != launch["candidate_invocation"] or worker.get("controller_identity") != controller_identity or worker.get("worker_invocation_id") != assignment["worker_invocation_id"] or worker.get("controller_status") != launch["controller_status"] or worker.get("candidate_result") != launch["worker_result"]:
            raise AdmissionError("C3 substitute worker completion is not exact PASS")
        if result.get("candidate_invocation") != launch["candidate_invocation"] or result.get("controller_identity") != controller_identity or result.get("controller_status") != launch["controller_status"] or result.get("candidate_result") != launch["worker_result"]:
            raise AdmissionError("substitute result does not bind the validated C3 completion")
        if self._verify_c3_completion(launch["candidate_invocation"], worker["controller_status"], worker["candidate_result"], worker["lane_id"], worker["worker_invocation_id"], assignment["expected_status_path"], assignment["expected_result_path"], assignment_ref["sha256"]) != controller_identity:
            raise AdmissionError("C3 substitute worker controller identity drifted")

    def record(self, stage: str, call_id: str, record: dict[str, Any], previous: tuple[str, str] | None = None) -> tuple[Path, str]:
        order = ("proposal", "policy-evaluation", "signed-decision", "authorization", "dispatch-admission", "dispatch", "raw-result", "returning-state-cleanup", "result")
        if stage not in order or not call_id or "/" in call_id or "\\" in call_id:
            raise AdmissionError("invalid immutable stage or call identity")
        index = order.index(stage)
        if index and previous is None:
            raise AdmissionError("immutable call artifacts are out of order")
        if previous is not None:
            reject_linked_path(Path(previous[0])); previous_path = Path(previous[0]).resolve()
            if self.root not in previous_path.parents or not previous_path.is_file() or hashlib.sha256(previous_path.read_bytes()).hexdigest() != previous[1]:
                raise AdmissionError("prior immutable artifact is missing or tampered")
            expected = _safe_child(self.root, "calls", call_id, f"{index - 1:02d}-{order[index - 1]}.json")
            if previous_path != expected:
                raise AdmissionError("prior artifact must be the immediately preceding same-call stage")
            record = {**record, "previous_path": previous[0], "previous_sha256": previous[1]}
        required = {"attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "route", "governing_hashes", "c1_reference", "identity", "bound_operation", "bound_operation_sha256"}
        if not required <= record.keys():
            raise AdmissionError("missing exact identity or lock bindings")
        bound = canonical_bound_operation(record["bound_operation"])
        if record["bound_operation_sha256"] != canonical_sha256(bound) or bound["call_id"] != call_id:
            raise AdmissionError("bound operation hash or call identity mismatch")
        if stage == "raw-result":
            if "raw_result" not in record:
                raise AdmissionError("raw result is required")
            actual = raw_result_sha256(record["raw_result"])
            if bound["raw_result_sha256"] == "PENDING":
                final_bound = {**bound, "raw_result_sha256": actual}
                record = {**record, "bound_operation": final_bound, "bound_operation_sha256": canonical_sha256(final_bound), "pre_dispatch_operation_sha256": canonical_sha256(bound)}
            elif actual != bound["raw_result_sha256"]:
                raise AdmissionError("retained raw result does not match the bound result identity")
        path = _safe_child(self.root, "calls", call_id, f"{order.index(stage):02d}-{stage}.json")
        return path, _write_new(path, {"schema": "firmware-call-evidence/v1", "stage": stage, "call_id": call_id, **record})

    def admit(self, call_id: str, stages: list[tuple[Path, str]], now_monotonic: float, verifier: SignatureVerifier | None) -> str:
        """Verify the complete immutable chain and produce a terminal classification only after cleanup."""
        names = ("proposal", "policy-evaluation", "signed-decision", "authorization", "dispatch-admission", "dispatch", "raw-result", "returning-state-cleanup", "result")
        validate_pinned_server()
        if len(stages) != len(names):
            raise AdmissionError("incomplete immutable evidence chain")
        records: list[dict[str, Any]] = []
        digests: list[str] = []
        for index, (path, expected_hash) in enumerate(stages):
            reject_linked_path(path); resolved = path.resolve()
            expected = _safe_child(self.root, "calls", call_id, f"{index:02d}-{names[index]}.json")
            if resolved != expected or resolved.is_symlink() or hashlib.sha256(resolved.read_bytes()).hexdigest() != expected_hash:
                raise AdmissionError("artifact path, hash, or stage mismatch")
            item = json.loads(resolved.read_text(encoding="utf-8"))
            if item.get("call_id") != call_id or item.get("stage") != names[index]:
                raise AdmissionError("cross-call or stage identity mismatch")
            if index and (item.get("previous_path") != str(stages[index - 1][0]) or item.get("previous_sha256") != digests[-1]):
                raise AdmissionError("broken immediate evidence chain")
            records.append(item); digests.append(expected_hash)
        baseline = {key: records[0][key] for key in ("attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "route", "governing_hashes", "c1_reference", "identity")}
        if any(any(item.get(key) != value for key, value in baseline.items()) for item in records[1:]):
            raise AdmissionError("bound call identity changed")
        intent = canonical_bound_operation(records[0]["bound_operation"])
        bound = canonical_bound_operation(records[-1]["bound_operation"])
        if records[0]["bound_operation_sha256"] != canonical_sha256(intent) or intent["raw_result_sha256"] != "PENDING" or any(canonical_bound_operation(item["bound_operation"]) != intent for item in records[1:6]) or any(canonical_bound_operation(item["bound_operation"]) != bound for item in records[6:]):
            raise AdmissionError("canonical bound operation hash mismatch")
        if {key: value for key, value in intent.items() if key != "raw_result_sha256"} != {key: value for key, value in bound.items() if key != "raw_result_sha256"} or bound["raw_result_sha256"] == "PENDING":
            raise AdmissionError("raw result binding drifted from immutable intent")
        decision = records[2]
        decision_keys = {"schema", "proposal_path", "proposal_sha256", "call", "claim", "decision", "rationale", "issued_utc", "issued_monotonic", "expires_monotonic", "topology_key_release", "orchestrator_identity", "public_key", "signature"}
        signed_decision = {key: decision[key] for key in decision_keys if key in decision}
        if set(signed_decision) != decision_keys or verifier is None or not verifier.verify(canonical_decision_payload(signed_decision), str(decision.get("signature", "")), str(decision.get("public_key", ""))):
            raise AdmissionError("signed decision is absent or invalid")
        authorization = records[3]
        expiry = authorization.get("expires_monotonic")
        if not isinstance(expiry, (int, float)) or now_monotonic >= expiry:
            return "INDETERMINATE_EXPIRED"
        admission = records[4]
        deadline = admission.get("deadline_monotonic")
        if not isinstance(deadline, (int, float)):
            raise AdmissionError("admission deadline missing")
        raw = records[6]
        if "raw_result" not in raw or raw_result_sha256(raw["raw_result"]) != bound["raw_result_sha256"]:
            raise AdmissionError("retained raw result does not match the bound result identity")
        cleanup = records[7]
        if not cleanup.get("exact_reaped") or cleanup.get("call_id") != call_id:
            raise AdmissionError("exact returning-state cleanup is required")
        return "INDETERMINATE_TIMEOUT" if now_monotonic > deadline else str(raw.get("outcome", "FAIL"))


def _finite_uart_seconds(value: Any, *, positive: bool) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(float(value)) and (float(value) > 0 if positive else float(value) >= 0) and float(value) <= 30


def _validate_uart_parameters(method: str, arguments: dict[str, Any]) -> None:
    def baud_and_port() -> bool:
        baudrate, port = arguments["baudrate"], arguments["port"]
        return (baudrate is None or isinstance(baudrate, int) and not isinstance(baudrate, bool) and baudrate > 0) and (port is None or isinstance(port, str) and bool(port.strip()))

    if method == "read_serial":
        if (arguments["expected_text"] is not None and (not isinstance(arguments["expected_text"], str) or not arguments["expected_text"])) or not _finite_uart_seconds(arguments["read_seconds"], positive=True) or not baud_and_port() or arguments["reset_on_open"] is not False or arguments["on_exit"] is not None:
            raise AdmissionError("UART read parameters do not match the locked safe surface")
        return
    if method == "write_serial":
        text = arguments["text"]
        if not isinstance(text, str) or not 1 <= len(text.encode("utf-8")) <= 256 or not _finite_uart_seconds(arguments["timeout_seconds"], positive=True) or not baud_and_port() or not isinstance(arguments["append_newline"], bool) or arguments["on_exit"] is not None:
            raise AdmissionError("UART write parameters do not match the locked safe surface")
        return
    steps = arguments["steps"]
    if not isinstance(steps, list) or not steps or not _finite_uart_seconds(arguments["read_seconds"], positive=True) or not baud_and_port() or not isinstance(arguments["clear_input"], bool):
        raise AdmissionError("UART exchange parameters do not match the locked safe surface")
    for row in steps:
        if not isinstance(row, dict) or set(row) != {"text", "expected_text", "line_ending"} or not isinstance(row["text"], str) or not row["text"] or not 1 <= len(row["text"].encode("utf-8")) <= 256 or not isinstance(row["expected_text"], str) or not row["expected_text"] or not isinstance(row["line_ending"], str) or row["line_ending"] not in {"none", "lf", "cr", "crlf"}:
            raise AdmissionError("UART exchange step is not an exact bounded command/response row")
    ready_text, ready_seconds = arguments["ready_text"], arguments["ready_seconds"]
    probe_text, probe_ending, probe_delay = arguments["ready_probe_text"], arguments["ready_probe_line_ending"], arguments["ready_probe_delay_seconds"]
    if not _finite_uart_seconds(ready_seconds, positive=False) or not _finite_uart_seconds(probe_delay, positive=False) or not isinstance(probe_ending, str) or probe_ending not in {"none", "lf", "cr", "crlf"}:
        raise AdmissionError("UART exchange readiness timing or line ending is invalid")
    if ready_text is None:
        if ready_seconds != 0 or probe_text is not None or probe_delay != 0 or probe_ending != "none":
            raise AdmissionError("UART exchange readiness values require ready_text")
    elif not isinstance(ready_text, str) or not ready_text or ready_seconds <= 0:
        raise AdmissionError("UART exchange ready_text requires a positive readiness window")
    elif probe_text is None:
        if probe_delay != 0 or probe_ending != "none":
            raise AdmissionError("UART exchange probe values require ready_probe_text")
    elif not isinstance(probe_text, str) or not probe_text and probe_ending == "none" or probe_delay > ready_seconds:
        raise AdmissionError("UART exchange readiness probe is invalid")


def evaluate_call(call: dict[str, Any], *, now_monotonic: float, policy_path: Path | None = None) -> dict[str, Any]:
    """Validate a fully correlated broker request without performing a physical action."""
    required = {"call_id", "lane_id", "board", "probe_uid", "target", "profile", "method", "method_version", "arguments", "proposal_sha256", "decision_sha256", "authorization_sha256", "deadline_monotonic", "plan", "permission", "delegated_user_scope_sha256", "action_class", "scope_effect"}
    missing = sorted(required - call.keys())
    if missing:
        raise AdmissionError(f"missing required call fields: {', '.join(missing)}")
    policy = _load_policy(policy_path or Path(__file__).with_name("MCP_METHOD_POLICY.json"))
    rule = policy["methods"].get(call["method"])
    if not isinstance(rule, dict):
        raise AdmissionError("default deny: unmapped MCP method")
    if not isinstance(call["method_version"], int) or isinstance(call["method_version"], bool) or call["method_version"] <= 0 or not isinstance(rule.get("version"), int) or isinstance(rule["version"], bool) or rule["version"] <= 0 or call["method_version"] != rule["version"]:
        raise AdmissionError("method version does not exactly match locked policy rule")
    if any(token in json.dumps(call["arguments"], sort_keys=True).lower() for token in FORBIDDEN):
        raise AdmissionError("prohibited destructive or try-last parameter")
    if call["delegated_user_scope_sha256"] != canonical_sha256(_USER_ISSUED_SCOPE) or call["action_class"] != rule["action_class"] or call["action_class"] not in _USER_ISSUED_SCOPE["allowed_action_classes"] or call["action_class"] in _USER_ISSUED_SCOPE["prohibited_action_classes"]:
        raise AdmissionError("method action class is not exactly delegated")
    _validate_scope_effect(call["scope_effect"], _USER_ISSUED_SCOPE)
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
    parameters = rule.get("parameters")
    if not isinstance(parameters, dict):
        raise AdmissionError("method parameters do not match pinned guarded surface")
    action_parameters = parameters
    if "plan_action" in rule:
        null_keys = parameters.get("null_required_exact")
        populated_keys = parameters.get("populated_required_exact")
        if not isinstance(null_keys, list) or not isinstance(populated_keys, list) or len(null_keys) != 11 or len(populated_keys) != 10 or set(populated_keys) != set(null_keys) - {"user_permission"}:
            raise AdmissionError("plan policy does not define the two exact pinned shapes")
        all_null = set(call["arguments"]) == set(null_keys) and all(value is None for value in call["arguments"].values())
        if all_null:
            return {"policy": "ALLOW", "evaluation_sha256": canonical_sha256({"call": call, "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest() if policy_path else "packaged"}), "maximum_duration_seconds": maximum}
        plan = call["arguments"]
        # The MCP board_id is a server-generated route value, not the lane's
        # logical fixture name.  The retained-session controller binds it to
        # setup_overview before dispatch.
        if set(plan) != set(populated_keys) or (not isinstance(plan["board_id"], str) or not plan["board_id"] or not all(isinstance(plan[key], str) and plan[key] for key in ("hypothesis", "strategy", "expected_fail_return", "expected_success_return")) or plan["hypothesis_made"] is not True or plan["strategy_evaluated"] is not True or any(not isinstance(plan[key], int) or isinstance(plan[key], bool) or plan[key] <= 0 for key in ("max_calls", "max_calls_buffer")) or not isinstance(plan["action_parameters"], dict)):
            raise AdmissionError("populated plan does not have the exact guarded envelope")
        action_parameters = rule.get("action_parameters")
        if not isinstance(action_parameters, dict) or set(plan["action_parameters"]) != set(action_parameters.get("required_exact", ())):
            raise AdmissionError("plan action parameters do not match the closed action schema")
        # The nested action must itself be suitable for the paired handler.
        parameters = action_parameters
    elif set(call["arguments"]) != set(parameters.get("required_exact", ())):
        raise AdmissionError("method parameters do not match pinned guarded surface")
    action_arguments = call["arguments"].get("action_parameters", call["arguments"])
    uart_method = call["method"].removesuffix("-plan")
    if uart_method in {"read_serial", "write_serial", "serial_exchange"}:
        _validate_uart_parameters(uart_method, action_arguments)
    if call["method"] == "read_memory_symbol":
        args = call["arguments"]
        if not isinstance(args["symbol"], str) or not args["symbol"] or args["width"] not in (8, 16, 32) or args["elf_artifact"] is not None and not isinstance(args["elf_artifact"], str):
            raise AdmissionError("memory diagnostic arguments do not match pinned handler bounds")
    if call["method"] == "flash_application" and not parameters.get("safe_flags", {}).get("application_region_only"):
        raise AdmissionError("flash method lacks reviewed application containment")
    if call["method"] in {"flash_application", "flash_application-plan"}:
        artifact = action_arguments.get("artifact")
        if not isinstance(artifact, str) or not artifact or any(token in artifact.lower() for token in ("bootloader", "mass_erase", "unlock", "protection")):
            raise AdmissionError("flash artifact is not an application-only input")
    return {"policy": "ALLOW", "evaluation_sha256": canonical_sha256({"call": call, "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest() if policy_path else "packaged"}), "maximum_duration_seconds": maximum}
