"""Small deterministic validators used by the synthetic S2 acceptance tests."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path
from typing import Any


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
        required = {"schema", "limitation_id", "attempt_id", "lane_id", "session_id", "original_test", "classification", "call_chain", "session_terminal", "process_evidence", "pinned_source", "attribution", "alternatives", "substitute", "physical_certification", "o_decision", "created_utc", "signature"}
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
            self._verify_limitation_reference(item, "limitation call chain")
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
        terminal = decision["session_terminal"]
        if not isinstance(terminal, dict) or set(terminal) != {"path", "sha256"}: raise AdmissionError("terminal session evidence is incomplete")
        self._verify_limitation_reference(terminal, "terminal session")
        terminal_value = json.loads(Path(terminal["path"]).read_text(encoding="utf-8"))
        if terminal_value.get("session_id") != decision["session_id"] or terminal_value.get("state") not in {"ABORTED", "CLOSED"}:
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
        protected = decision["o_decision"].get("protected_suite") if isinstance(decision["o_decision"], dict) else None
        self._verify_limitation_reference(protected, "protected suite")
        protected_value = json.loads(Path(protected["path"]).read_text(encoding="utf-8"))
        protected_ids = protected_value.get("protected_ids") if isinstance(protected_value, dict) else None
        if not isinstance(protected_value, dict) or protected_value.get("schema") != "c1-protected-test-ids/v1" or protected_value.get("c1_reference") != records[0].get("c1_reference") or not isinstance(protected_ids, list) or not protected_ids or len(set(protected_ids)) != len(protected_ids) or any(not isinstance(value, str) or not value for value in protected_ids): raise AdmissionError("protected suite artifact is not closed")
        if decision["original_test"] == substitute["stable_id"] or decision["original_test"] in protected_ids or substitute["stable_id"] in protected_ids or any(token in json.dumps(decision, sort_keys=True).lower() for token in ("pyocd", "direct serial", "direct mcp", "hardware absent", "operator error", "fixture error", "environment error", "unsafe call", "xfail", "skip", "weaken")):
            raise AdmissionError("limitation cannot substitute protected tests or bypass physical authority")
        result = {key: decision[key] for key in required - {"signature"}}
        result["schema"] = "firmware-server-limitation/v1"
        result["o_decision"] = {"path":str(o_decision_path.resolve()),"sha256":hashlib.sha256(o_decision_path.read_bytes()).hexdigest()}
        path = _safe_child(self.root, "hil", decision["lane_id"], "server-limitations", limitation_id + ".json")
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
    if not isinstance(parameters, dict) or set(call["arguments"]) != set(parameters.get("required_exact", ())):
        raise AdmissionError("method parameters do not match pinned guarded surface")
    action_parameters = parameters
    if "plan_action" in rule:
        all_null = all(value is None for value in call["arguments"].values())
        if all_null:
            return {"policy": "ALLOW", "evaluation_sha256": canonical_sha256({"call": call, "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest() if policy_path else "packaged"}), "maximum_duration_seconds": maximum}
        plan = call["arguments"]
        if (plan["board_id"] != call["board"] or not all(isinstance(plan[key], str) and plan[key] for key in ("hypothesis", "strategy", "expected_fail_return", "expected_success_return")) or plan["hypothesis_made"] is not True or plan["strategy_evaluated"] is not True or any(not isinstance(plan[key], int) or isinstance(plan[key], bool) or plan[key] <= 0 for key in ("max_calls", "max_calls_buffer")) or not isinstance(plan["action_parameters"], dict) or (plan["user_permission"] is not None and (not isinstance(plan["user_permission"], dict) or not plan["user_permission"]))):
            raise AdmissionError("populated plan does not have the exact guarded envelope")
        action_parameters = rule.get("action_parameters")
        if not isinstance(action_parameters, dict) or set(plan["action_parameters"]) != set(action_parameters.get("required_exact", ())):
            raise AdmissionError("plan action parameters do not match the closed action schema")
        # The nested action must itself be suitable for the paired handler.
        parameters = action_parameters
    action_arguments = call["arguments"].get("action_parameters", call["arguments"])
    if call["method"] in {"write_serial", "write_serial-plan"}:
        text = action_arguments.get("text")
        if not isinstance(text, str) or not 1 <= len(text.encode("utf-8")) <= 256:
            raise AdmissionError("UART write exceeds locked UTF-8 byte limit")
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
    if call["method"] in {"read_serial", "write_serial"} and call["arguments"].get("on_exit") is not None:
        raise AdmissionError("serial action cannot request a hidden exit effect")
    return {"policy": "ALLOW", "evaluation_sha256": canonical_sha256({"call": call, "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest() if policy_path else "packaged"}), "maximum_duration_seconds": maximum}
