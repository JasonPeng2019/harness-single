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
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def canonical_raw_result_bytes(payload: Any) -> bytes:
    """The retained raw MCP result representation: UTF-8 canonical JSON, never a caller digest."""
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AdmissionError("raw result is not a canonical JSON payload") from exc


def raw_result_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_raw_result_bytes(payload)).hexdigest()


def validate_pinned_server() -> str:
    """Prove the controller is configured against the exact clean candidate tree."""
    if not _PINNED_SERVER_ROOT.is_dir() or _PINNED_SERVER_ROOT.is_symlink():
        raise AdmissionError("pinned MCP worktree is unavailable")
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=_PINNED_SERVER_ROOT, capture_output=True, text=True)
    status = subprocess.run(["git", "status", "--porcelain"], cwd=_PINNED_SERVER_ROOT, capture_output=True, text=True)
    if head.returncode or status.returncode or head.stdout.strip() != _PINNED_SERVER_COMMIT or status.stdout.strip():
        raise AdmissionError("pinned MCP worktree revision or cleanliness mismatch")
    return _PINNED_SERVER_COMMIT


def canonical_bound_operation(value: dict[str, Any]) -> dict[str, Any]:
    """Closed operation authority carried verbatim through every immutable stage."""
    required = {
        "server_commit", "method", "method_version", "arguments", "policy_sha256", "schema_sha256",
        "plan_sha256", "permission_sha256", "authorization_sha256", "claim_sha256", "call_id",
        "attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "route",
        "governing_hashes", "c1_reference", "deadline_monotonic", "expires_monotonic", "seed_identity",
        "target_identity", "raw_result_sha256", "cleanup_owner",
    }
    if set(value) != required:
        raise AdmissionError("bound operation must be a closed complete authority object")
    if value["server_commit"] != _PINNED_SERVER_COMMIT or not isinstance(value["method"], str) or not isinstance(value["arguments"], dict):
        raise AdmissionError("bound operation server or method identity is invalid")
    for key in ("method_version", "deadline_monotonic", "expires_monotonic"):
        if not isinstance(value[key], (int, float)) or isinstance(value[key], bool):
            raise AdmissionError("bound operation timing/version is invalid")
    for key in required - {"method", "arguments", "method_version", "deadline_monotonic", "expires_monotonic", "governing_hashes", "c1_reference", "seed_identity", "target_identity", "route", "raw_result_sha256"}:
        if not isinstance(value[key], str) or not value[key]:
            raise AdmissionError("bound operation has an empty identity or hash")
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
        if not isinstance(item, dict) or set(item) != definition_keys or not isinstance(item["dependency_inputs"], list) or not item["dependency_inputs"] or item["fingerprint_algorithm"] != "sha256-canonical-json" or item["failure_route"] not in {"TARGET_LOCAL_REPAIR", "HARNESS_WATCHER_ABORT", "PINNED_SERVER_REPAIR"}:
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
    if value.get("default") != "deny" or not isinstance(value.get("methods"), dict):
        raise AdmissionError("invalid default-deny policy")
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

    def __init__(self, root: Path, seed: Path, policy_path: Path, templates_path: Path, manifest_path: Path | None = None) -> None:
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
            resolved = path.resolve()
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
        if verifier is None or not verifier.verify(canonical_sha256(records[0]).encode(), str(decision.get("signature", "")), str(decision.get("public_key", ""))):
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
    allowed_parameters = rule.get("parameters")
    if not isinstance(allowed_parameters, list) or set(call["arguments"]) - set(allowed_parameters) - {"rf", "dio2_dependent"}:
        raise AdmissionError("method parameters do not match pinned guarded surface")
    if call["method"] == "read_memory_address":
        args = call["arguments"]
        if set(args) != {"board_id", "address", "width", "length"} or args["width"] not in rule["widths"] or not isinstance(args["length"], int) or isinstance(args["length"], bool) or not 1 <= args["length"] <= rule["maximum_length_bytes"]:
            raise AdmissionError("memory diagnostic arguments do not match pinned handler bounds")
    if call["method"] == "flash_application" and not rule.get("application_region_only"):
        raise AdmissionError("flash method lacks reviewed application containment")
    if call["arguments"].get("dio2_dependent"):
        raise AdmissionError("DIO2-dependent work is denied while P.05 is unresolved")
    rf = call["arguments"].get("rf")
    if rf is not None:
        intent = policy["legal_rf_intent"]
        required_rf = {"electronic_admission", "frequency_hz", "power_dbm", "payload_bytes", "airtime_ms_per_60s", "campaign_minutes", "bandwidth_hz", "coding_rate", "spreading_factor"}
        if not isinstance(rf, dict) or not required_rf <= rf.keys() or not isinstance(rf["electronic_admission"], dict) or not {"antenna", "supply_current", "module_identity"} <= rf["electronic_admission"].keys() or rf["frequency_hz"] != intent["frequency_hz"] or rf["power_dbm"] > intent["maximum_dbm"] or rf["payload_bytes"] > intent["maximum_payload_bytes"] or rf["airtime_ms_per_60s"] > intent["maximum_airtime_ms_per_60s"] or rf["campaign_minutes"] > intent["maximum_campaign_minutes"] or rf["bandwidth_hz"] != intent["bandwidth_hz"] or rf["coding_rate"] != intent["coding_rate"] or rf["spreading_factor"] not in intent["spreading_factor_range"]:
            raise AdmissionError("RF call lacks authoritative bounded admission")
    return {"policy": "ALLOW", "evaluation_sha256": canonical_sha256({"call": call, "policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest() if policy_path else "packaged"}), "maximum_duration_seconds": maximum}
