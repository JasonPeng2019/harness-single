from __future__ import annotations

"""One owning registry and selector for portable release checks.

The registry is deliberately small and declarative.  It describes the checks
that consume the public release surface; it does not run them.  A credit keeps
its exact origin identity, but may travel only along the same repository,
common directory, branch, and ancestor lineage when its declared inputs and
check contract remain unchanged.
"""

import argparse
import fnmatch
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

REGISTRY_SCHEMA = "orchestrator-release-check-registry/v1"
SELECTION_SCHEMA = "orchestrator-check-selection/v1"
CREDIT_SCHEMA = "orchestrator-check-credit/v1"
CHECKPOINT_SCHEMA = "orchestrator-checkpoint/v1"
DISPOSITION_SCHEMA = "orchestrator-check-disposition/v1"
DISPOSITION_STATUSES = frozenset({"FAIL", "UNRESOLVED", "SKIP"})
_DISPOSITION_REASONS = {
    "FAIL": "prior-failed",
    "UNRESOLVED": "prior-unresolved",
    "SKIP": "prior-skipped",
}
REGISTRY_VERSION = 1
RELEASE_AGGREGATE_ID = "S6.RELEASE.ACCUMULATED-SAFEGUARD"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_TIERS = frozenset({"fast", "affected", "full", "release"})
_INTENTS = frozenset({"fast", "affected", "full", "release"})
_WINDOWLESS_CREATION_FLAGS = 0x08000000 if os.name == "nt" else 0

# This is the audited, intentionally small route manifest.  It follows the
# actual imports used by public_launch -> operator_launch -> lane_controller,
# including the primitives that establish invocation, provider, Git/result,
# process, claim, lifecycle, and cleanup truth.  It is shared by the local
# public journey and the optional real-agent journey; it is not a repository
# walk or a caller-supplied domain shortcut.
PUBLIC_ROUTE_DEPENDENCIES = (
    "orchestrator_harness/public_launch.py",
    "orchestrator_harness/operator_launch.py",
    "orchestrator_harness/lane_controller.py",
    "orchestrator_harness/invocation.py",
    "orchestrator_harness/provider.py",
    "orchestrator_harness/process_supervisor.py",
    "orchestrator_harness/processes.py",
    "orchestrator_harness/git_safety.py",
    "orchestrator_harness/resource_locks.py",
    "orchestrator_harness/lane_lifecycle.py",
    "orchestrator_harness/models.py",
    "orchestrator_harness/mutation.py",
    "orchestrator_harness/stable_io.py",
    "orchestrator_harness/profile.py",
    "orchestrator_harness/prompt_bundle.py",
    "orchestrator_harness/resume.py",
    "orchestrator_harness/task.py",
    "orchestrator_harness/cli.py",
    "orchestrator_harness/config.py",
    "orchestrator_harness/discovery.py",
    "harness_common/process_identity.py",
)

REAL_AGENT_ROUTE_DEPENDENCIES = PUBLIC_ROUTE_DEPENDENCIES + (
    "orchestrator_harness/tests/real_agent_test.py",
    "orchestrator_harness/tests/wsl_identity.py",
    "orchestrator_harness/tests/support/wsl_real_agent_driver.py",
    "orchestrator_harness/tests/support/wsl_codex_provider.py",
    "orchestrator_harness/tests/support/wsl_public_route_entry.py",
    "orchestrator_harness/tests/support/wsl_guarded_entry.py",
    "orchestrator_harness/tests/support/cgroup_exec.py",
    "orchestrator_harness/tests/support/allowlist_connect_proxy.py",
    "orchestrator_harness/install_wsl_codex.ps1",
)


class SelectionError(ValueError):
    """Raised when a source root, registry, or credit cannot be trusted."""


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _nonempty(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SelectionError(f"{name} must be a non-empty string")
    return value.strip()


def _string_tuple(value: Iterable[str], name: str) -> tuple[str, ...]:
    result = tuple(value)
    if any(not isinstance(item, str) or not item.strip() for item in result):
        raise SelectionError(f"{name} must contain non-empty strings")
    if len(set(result)) != len(result):
        raise SelectionError(f"{name} must not contain duplicates")
    return result


def _relative_path(value: str, name: str) -> str:
    candidate = value.replace("\\", "/")
    path = Path(candidate)
    if path.is_absolute() or ".." in path.parts:
        raise SelectionError(f"{name} must be a relative path")
    normalized = "/".join(part for part in path.parts if part not in ("", "."))
    if not normalized:
        raise SelectionError(f"{name} must not be empty")
    return normalized


@dataclass(frozen=True)
class InputScope:
    """A validated root-relative input scope for a broad release command."""

    kind: str
    path: str
    required: bool = True

    def __post_init__(self) -> None:
        if self.kind not in {"file", "tree", "glob"}:
            raise SelectionError(f"unsupported input scope kind: {self.kind}")
        normalized = _relative_path(self.path, "input_scope.path")
        if normalized != self.path.replace("\\", "/"):
            raise SelectionError("input scope path must use normalized separators")
        if not isinstance(self.required, bool):
            raise SelectionError("input_scope.required must be boolean")
        if self.kind == "glob" and not any(token in self.path for token in "*?["):
            raise SelectionError("glob input scope must contain a wildcard")
        if self.kind != "glob" and any(token in self.path for token in "*?["):
            raise SelectionError("file/tree input scope cannot contain wildcards")

    def to_record(self) -> dict[str, Any]:
        return {"kind": self.kind, "path": self.path, "required": self.required}


@dataclass(frozen=True)
class CheckSpec:
    """A stable, fingerprinted check declaration."""

    stable_id: str
    name: str
    tier: str
    command: tuple[str, ...]
    dependency_domains: tuple[str, ...]
    dependency_paths: tuple[str, ...]
    external_requirements: tuple[str, ...] = ()
    platform_requirements: tuple[str, ...] = ()
    estimated_duration_seconds: float = 1.0
    decisive: bool = False
    output_contract: str = CREDIT_SCHEMA
    producer: str = "S6.P"
    input_scopes: tuple[InputScope, ...] = ()

    def __post_init__(self) -> None:
        _nonempty(self.stable_id, "stable_id")
        _nonempty(self.name, "name")
        if self.tier not in _TIERS:
            raise SelectionError(f"unsupported check tier: {self.tier}")
        if not self.command or any(
            not isinstance(item, str) or not item for item in self.command
        ):
            raise SelectionError(f"{self.stable_id} command must be non-empty")
        _string_tuple(self.dependency_domains, f"{self.stable_id}.dependency_domains")
        for path in self.dependency_paths:
            _relative_path(path, f"{self.stable_id}.dependency_paths")
        for scope in self.input_scopes:
            if not isinstance(scope, InputScope):
                raise SelectionError(
                    f"{self.stable_id}.input_scopes must contain InputScope values"
                )
        if len(
            {(scope.kind, scope.path.casefold()) for scope in self.input_scopes}
        ) != len(self.input_scopes):
            raise SelectionError(f"{self.stable_id}.input_scopes must not be ambiguous")
        _string_tuple(
            self.external_requirements, f"{self.stable_id}.external_requirements"
        )
        _string_tuple(
            self.platform_requirements, f"{self.stable_id}.platform_requirements"
        )
        if self.estimated_duration_seconds <= 0:
            raise SelectionError(f"{self.stable_id} duration must be positive")
        _nonempty(self.output_contract, f"{self.stable_id}.output_contract")
        _nonempty(self.producer, f"{self.stable_id}.producer")

    def to_record(self) -> dict[str, Any]:
        record = {
            "stable_id": self.stable_id,
            "name": self.name,
            "tier": self.tier,
            "command": list(self.command),
            "dependency_domains": list(self.dependency_domains),
            "dependency_paths": list(self.dependency_paths),
            "external_requirements": list(self.external_requirements),
            "platform_requirements": list(self.platform_requirements),
            "estimated_duration_seconds": self.estimated_duration_seconds,
            "decisive": self.decisive,
            "output_contract": self.output_contract,
            "producer": self.producer,
        }
        if self.input_scopes:
            record["input_scopes"] = [scope.to_record() for scope in self.input_scopes]
        return record

    def consumes(self, changed_paths: set[str], changed_domains: set[str]) -> bool:
        if changed_domains.intersection(self.dependency_domains):
            return True
        declared = {
            _relative_path(path, "dependency_path").casefold()
            for path in self.dependency_paths
        }
        for changed in changed_paths:
            normalized = changed.casefold().rstrip("/")
            if normalized in declared:
                return True
            if any(normalized.startswith(path + "/") for path in declared):
                return True
            if any(_scope_matches(scope, normalized) for scope in self.input_scopes):
                return True
        return False


@dataclass(frozen=True)
class SourceIdentity:
    source_root: str
    git_common_dir: str
    branch: str
    tip: str

    def to_record(self) -> dict[str, str]:
        return {
            "source_root": self.source_root,
            "git_common_dir": self.git_common_dir,
            "branch": self.branch,
            "tip": self.tip,
        }


@dataclass(frozen=True)
class SelectedCheck:
    spec: CheckSpec
    dependency_fingerprint: str
    reason: str

    def to_record(self) -> dict[str, Any]:
        record = self.spec.to_record()
        record.update(
            {
                "dependency_fingerprint": self.dependency_fingerprint,
                "selection_reason": self.reason,
            }
        )
        return record


@dataclass(frozen=True)
class SelectionDecision:
    intent: str
    source: SourceIdentity
    selected: tuple[SelectedCheck, ...]
    preserved_credit_ids: tuple[str, ...]
    preserved_credit_records: tuple[dict[str, Any], ...]
    invalidated_credit_ids: tuple[str, ...]
    rejected_credit_ids: tuple[str, ...]
    registry_fingerprint: str
    changed_paths: tuple[str, ...]
    changed_domains: tuple[str, ...]

    @property
    def selected_ids(self) -> tuple[str, ...]:
        return tuple(item.spec.stable_id for item in self.selected)

    def to_record(self) -> dict[str, Any]:
        return {
            "schema": SELECTION_SCHEMA,
            "intent": self.intent,
            "selector_module": str(Path(__file__).resolve()),
            "source": self.source.to_record(),
            "registry_fingerprint": self.registry_fingerprint,
            "changed_paths": list(self.changed_paths),
            "changed_domains": list(self.changed_domains),
            "selected": [item.to_record() for item in self.selected],
            "selected_ids": list(self.selected_ids),
            "preserved_credit_ids": list(self.preserved_credit_ids),
            "preserved_credit_records": [
                dict(item) for item in self.preserved_credit_records
            ],
            "invalidated_credit_ids": list(self.invalidated_credit_ids),
            "rejected_credit_ids": list(self.rejected_credit_ids),
            "producer_must_not_run": [RELEASE_AGGREGATE_ID],
        }


def _python_test(module: str, test: str) -> tuple[str, ...]:
    return ("python", "-m", "unittest", "-v", f"{module}.{test}")


_RUFF_INPUT_SCOPES = (
    InputScope("glob", "**/*.py"),
    InputScope("glob", "**/*.pyi", required=False),
    InputScope("glob", "**/*.ipynb", required=False),
    InputScope("file", "pyproject.toml", required=False),
    InputScope("file", "ruff.toml", required=False),
    InputScope("file", ".ruff.toml", required=False),
    InputScope("file", "setup.cfg", required=False),
    InputScope("file", "tox.ini", required=False),
)
_PYRIGHT_INPUT_SCOPES = (
    InputScope("glob", "**/*.py"),
    InputScope("glob", "**/*.pyi", required=False),
    InputScope("file", "pyrightconfig.json", required=False),
    InputScope("file", "pyproject.toml", required=False),
    InputScope("file", ".codex/dev/basedpyright-baseline.json", required=False),
)
_COMPILE_INPUT_SCOPES = (
    InputScope("glob", "orchestrator_harness/**/*.py"),
    InputScope("glob", "harness_watcher_implementation/**/*.py"),
)
_SYNTHETIC_CLEANUP_INPUT_SCOPES = (
    InputScope("glob", "orchestrator_harness/**/*.py"),
    InputScope("glob", "harness_common/**/*.py"),
)
_PACKAGE_INPUT_SCOPES = (
    # ``pip wheel orchestrator_harness`` consumes the complete package tree,
    # including its package data, and the declared data-file trees below.
    InputScope("tree", "orchestrator_harness"),
    InputScope("tree", "harness_common"),
    InputScope("tree", "examples"),
    InputScope("tree", "release_evidence_templates"),
)
_ORCHESTRATOR_UNIT_INPUT_SCOPES = (
    # Discovery imports every harness test and its package dependencies and
    # the discovered tests read these bounded release-facing inputs.
    InputScope("tree", "orchestrator_harness"),
    InputScope("tree", "harness_common"),
    InputScope("tree", "examples"),
    InputScope("tree", "release_evidence_templates"),
    InputScope("tree", "tools"),
    InputScope("tree", "docs"),
    InputScope("file", "README.md"),
    InputScope("file", "QUICK_START.md"),
    InputScope("file", "QUICK_RULES.md"),
    InputScope("file", "AGENTS.md"),
    InputScope("file", "PORTABLE_CONTENTS.md"),
    InputScope("file", "pyrightconfig.json", required=False),
    InputScope("file", ".codex/dev/basedpyright-baseline.json", required=False),
)
_WATCHER_UNIT_INPUT_SCOPES = (
    InputScope("tree", "harness_watcher_implementation"),
    InputScope("tree", "harness_common"),
    InputScope("file", "README.md"),
    InputScope("file", "QUICK_START.md"),
    InputScope("file", "QUICK_RULES.md"),
)
_ATTENTION_INPUT_SCOPES = _WATCHER_UNIT_INPUT_SCOPES
_RELEASE_AGGREGATE_INPUT_SCOPES = (
    InputScope("tree", "orchestrator_harness"),
    InputScope("tree", "harness_common"),
    InputScope("tree", "harness_watcher_implementation"),
    InputScope("tree", "examples"),
    InputScope("tree", "release_evidence_templates"),
    InputScope("tree", "tools"),
    InputScope("tree", "docs"),
    InputScope("file", "README.md"),
    InputScope("file", "QUICK_START.md"),
    InputScope("file", "QUICK_RULES.md"),
    InputScope("file", "AGENTS.md"),
    InputScope("file", "PORTABLE_CONTENTS.md"),
    InputScope("file", ".gitignore"),
    InputScope("file", "pyrightconfig.json", required=False),
    InputScope("file", ".codex/dev/basedpyright-baseline.json", required=False),
)


def _scope_matches(scope: InputScope, relative: str) -> bool:
    normalized = _relative_path(relative, "scope_relative_path").casefold()
    pattern = scope.path.casefold()
    if scope.kind == "file":
        return normalized == pattern
    if scope.kind == "tree":
        return normalized == pattern or normalized.startswith(pattern + "/")
    if PurePosixPath(normalized).match(pattern):
        return True
    if pattern.startswith("**/"):
        short = pattern[3:]
        if PurePosixPath(normalized).match(short) or fnmatch.fnmatchcase(
            normalized, short
        ):
            return True
    if "/**/" in pattern:
        prefix, suffix = pattern.split("/**/", 1)
        if normalized.startswith(prefix + "/") and fnmatch.fnmatchcase(
            normalized[len(prefix) + 1 :], suffix
        ):
            return True
    return fnmatch.fnmatchcase(normalized, pattern)


def _tracked_paths(root: Path) -> tuple[str, ...]:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--cached"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=10,
            creationflags=_WINDOWLESS_CREATION_FLAGS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SelectionError(f"cannot enumerate tracked input paths: {exc}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()
        raise SelectionError(f"cannot enumerate tracked input paths: {detail}")
    try:
        values = completed.stdout.decode("utf-8").split("\0")
    except UnicodeDecodeError as exc:
        raise SelectionError(f"tracked input paths are not UTF-8: {exc}") from exc
    paths = tuple(
        sorted({_relative_path(value, "tracked_path") for value in values if value})
    )
    return paths


def _validated_input_path(root: Path, relative: str, *, scope: InputScope) -> Path:
    candidate = root / Path(relative)
    try:
        resolved = candidate.resolve(strict=False)
        resolved.relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise SelectionError(
            f"{scope.kind} input scope escapes source root: {scope.path} -> {relative}"
        ) from exc
    if candidate.is_symlink() or not candidate.is_file():
        raise SelectionError(
            f"{scope.kind} input scope matched a non-regular file: {relative}"
        )
    return candidate


def resolve_input_scope(spec: CheckSpec, root: str | Path) -> tuple[str, ...]:
    """Resolve declared scopes to tracked, regular, root-relative files."""

    root_path = Path(root).expanduser().resolve(strict=True)
    tracked = _tracked_paths(root_path)
    resolved: list[str] = []
    for scope in spec.input_scopes:
        if scope.kind == "tree":
            directory = root_path / Path(scope.path)
            if directory.is_symlink() or not directory.is_dir():
                if scope.required:
                    raise SelectionError(
                        f"required input tree is unavailable: {scope.path}"
                    )
                continue
        matches = tuple(
            relative for relative in tracked if _scope_matches(scope, relative)
        )
        if scope.required and not matches:
            raise SelectionError(
                f"required input scope matched no tracked files: {scope.path}"
            )
        for relative in matches:
            _validated_input_path(root_path, relative, scope=scope)
            resolved.append(relative)
    if len(set(resolved)) != len(resolved):
        # Overlapping scopes are allowed only when they describe the same
        # input once; duplicate declarations are rejected in CheckSpec.
        resolved = list(dict.fromkeys(resolved))
    return tuple(sorted(set(resolved)))


def _registry() -> tuple[CheckSpec, ...]:
    return (
        CheckSpec(
            "S6.FAST.SELECTOR",
            "stable selector and credit contract",
            "fast",
            _python_test(
                "orchestrator_harness.tests.test_s6_public_release", "S6SelectorTests"
            ),
            ("selector",),
            (
                ".gitignore",
                "orchestrator_harness/release_checks.py",
                "orchestrator_harness/tests/test_s6_public_release.py",
                "tools/Invoke-CandidateSafeguard.ps1",
                "tools/CandidateSafeguard.Core.psm1",
            ),
            estimated_duration_seconds=1.0,
            decisive=True,
        ),
        CheckSpec(
            "S6.FAST.DOCS",
            "public release documentation contract",
            "fast",
            _python_test(
                "orchestrator_harness.tests.test_s6_public_release",
                "S6DocumentationTests",
            ),
            ("docs",),
            (
                "README.md",
                "QUICK_START.md",
                "QUICK_RULES.md",
                "orchestrator_harness/README.md",
                "examples/public-coding-launch.example.md",
                "examples/release-selection.example.json",
                "orchestrator_harness/tests/test_s6_public_release.py",
            ),
            estimated_duration_seconds=1.0,
        ),
        CheckSpec(
            "S6.FAST.PACKAGE",
            "package resource and metadata contract",
            "fast",
            _python_test(
                "orchestrator_harness.tests.test_s6_public_release", "S6PackageTests"
            ),
            ("package",),
            (
                "orchestrator_harness/pyproject.toml",
                "orchestrator_harness/release_assets.py",
                "orchestrator_harness/assets/release/manifest.json",
                "orchestrator_harness/tests/test_s6_public_release.py",
            ),
            estimated_duration_seconds=1.0,
            input_scopes=_PACKAGE_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.FAST.LOCAL-ISOLATION",
            "deterministic isolation helper contract",
            "fast",
            (
                "python",
                "-m",
                "unittest",
                "-v",
                "orchestrator_harness.tests.test_s6_public_release.S6LocalIsolationTests",
            ),
            ("isolation-local",),
            REAL_AGENT_ROUTE_DEPENDENCIES
            + ("orchestrator_harness/tests/test_s6_public_release.py",),
            estimated_duration_seconds=2.0,
        ),
        CheckSpec(
            "S6.AFFECTED.PUBLIC-E2E",
            "public operator/controller disposable journey",
            "affected",
            _python_test(
                "orchestrator_harness.tests.test_s6_public_release",
                "S6PublicJourneyTests",
            ),
            ("public-launch", "controller-lifecycle"),
            PUBLIC_ROUTE_DEPENDENCIES
            + (
                "examples/disposable_coding_fixture.py",
                "orchestrator_harness/tests/test_s6_public_release.py",
            ),
            estimated_duration_seconds=8.0,
            decisive=True,
        ),
        CheckSpec(
            "S6.AFFECTED.SAFEGUARD",
            "portable candidate safeguard contract",
            "affected",
            _python_test(
                "orchestrator_harness.tests.test_s6_public_release", "S6SafeguardTests"
            ),
            ("safeguard", "selector"),
            (
                "tools/Invoke-CandidateSafeguard.ps1",
                "tools/CandidateSafeguard.Core.psm1",
                "orchestrator_harness/release_checks.py",
                "orchestrator_harness/tests/test_s6_public_release.py",
            ),
            platform_requirements=("powershell",),
            estimated_duration_seconds=2.0,
        ),
        CheckSpec(
            "S6.AFFECTED.LEGACY",
            "schema-less legacy and coding fixture compatibility",
            "affected",
            _python_test(
                "orchestrator_harness.tests.test_general_coding_docs",
                "GeneralCodingDocumentationTests",
            ),
            ("legacy-compat", "controller-lifecycle"),
            (
                "orchestrator_harness/invocation.py",
                "orchestrator_harness/lane_controller.py",
                "examples/legacy-firmware.invocation.example.json",
                "examples/coding.invocation.example.json",
                "orchestrator_harness/tests/test_general_coding_docs.py",
            ),
            estimated_duration_seconds=3.0,
        ),
        CheckSpec(
            "S6.AFFECTED.REAL-AGENT",
            "optional real-agent isolation journey",
            "affected",
            ("python", "-m", "orchestrator_harness.tests.real_agent_test"),
            ("isolation-real-agent",),
            REAL_AGENT_ROUTE_DEPENDENCIES,
            external_requirements=("WSL2", "Codex provider", "ephemeral auth"),
            platform_requirements=("windows", "wsl2"),
            estimated_duration_seconds=120.0,
        ),
        CheckSpec(
            "S6.RELEASE.RUFF",
            "accumulated Ruff check",
            "release",
            ("python", "-m", "ruff", "check", "--select", "E9,F63,F7,F82", "."),
            ("release-ruff",),
            (
                "orchestrator_harness/pyproject.toml",
                "tools/Invoke-CandidateSafeguard.ps1",
            ),
            estimated_duration_seconds=15.0,
            input_scopes=_RUFF_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.RELEASE.FORMAT",
            "accumulated Ruff format check",
            "release",
            ("python", "-m", "ruff", "format", "--check", "."),
            ("release-format",),
            (
                "orchestrator_harness/pyproject.toml",
                "tools/Invoke-CandidateSafeguard.ps1",
            ),
            estimated_duration_seconds=15.0,
            input_scopes=_RUFF_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.RELEASE.BASEDPYRIGHT",
            "retained BasedPyright baseline check",
            "release",
            ("python", "-m", "basedpyright", "--project", "pyrightconfig.json"),
            ("release-basedpyright",),
            (
                "orchestrator_harness/pyproject.toml",
                "tools/Invoke-CandidateSafeguard.ps1",
            ),
            estimated_duration_seconds=30.0,
            input_scopes=_PYRIGHT_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.RELEASE.COMPILE",
            "accumulated Python compilation",
            "release",
            (
                "python",
                "-m",
                "compileall",
                "-q",
                "orchestrator_harness",
                "harness_watcher_implementation",
            ),
            ("release-compile",),
            (
                "orchestrator_harness/pyproject.toml",
                "harness_watcher_implementation/__init__.py",
            ),
            estimated_duration_seconds=10.0,
            input_scopes=_COMPILE_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.RELEASE.ORCHESTRATOR-UNIT",
            "accumulated orchestrator unit discovery",
            "release",
            (
                "python",
                "-m",
                "unittest",
                "discover",
                "-s",
                "orchestrator_harness/tests",
                "-t",
                ".",
                "-v",
            ),
            ("release-orchestrator-unit",),
            (
                "orchestrator_harness/tests/test_s6_public_release.py",
                "orchestrator_harness/tests/test_coding_lane_controller.py",
            ),
            estimated_duration_seconds=1800.0,
            input_scopes=_ORCHESTRATOR_UNIT_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.RELEASE.WATCHER-UNIT",
            "accumulated watcher unit discovery",
            "release",
            (
                "python",
                "-m",
                "unittest",
                "discover",
                "-s",
                "harness_watcher_implementation/tests",
                "-t",
                ".",
                "-v",
            ),
            ("release-watcher-unit",),
            (
                "harness_watcher_implementation/__init__.py",
                "harness_watcher_implementation/tests/test_attention.py",
            ),
            estimated_duration_seconds=120.0,
            input_scopes=_WATCHER_UNIT_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.RELEASE.ATTENTION",
            "attention-retention practical",
            "release",
            (
                "python",
                "harness_watcher_implementation/tests/run_attention_practical.py",
            ),
            ("release-attention",),
            (
                "harness_watcher_implementation/tests/run_attention_practical.py",
                "harness_watcher_implementation/tests/test_attention_practical_retention.py",
            ),
            estimated_duration_seconds=30.0,
            input_scopes=_ATTENTION_INPUT_SCOPES,
        ),
        CheckSpec(
            "S6.RELEASE.SYNTHETIC-CLEANUP",
            "synthetic cleanup guard",
            "release",
            ("python", "orchestrator_harness/tests/wsl_cleanup_guard_test.py"),
            ("release-synthetic-cleanup",),
            (
                "orchestrator_harness/tests/wsl_cleanup_guard_test.py",
                "orchestrator_harness/tests/support/wsl_cleanup_fixture_driver.py",
            ),
            estimated_duration_seconds=20.0,
            input_scopes=_SYNTHETIC_CLEANUP_INPUT_SCOPES,
        ),
        CheckSpec(
            RELEASE_AGGREGATE_ID,
            "accumulated candidate safeguard (ROOT release assurance)",
            "release",
            (
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                "tools/Invoke-CandidateSafeguard.ps1",
                "-Run",
            ),
            ("release-assurance", "safeguard"),
            (
                "tools/Invoke-CandidateSafeguard.ps1",
                "orchestrator_harness/release_checks.py",
                "orchestrator_harness/pyproject.toml",
            ),
            external_requirements=("ROOT release assurance",),
            platform_requirements=("windows",),
            estimated_duration_seconds=2400.0,
            producer="ROOT-IM",
            input_scopes=_RELEASE_AGGREGATE_INPUT_SCOPES,
        ),
    )


CHECK_REGISTRY = _registry()


def registry() -> tuple[CheckSpec, ...]:
    """Return the immutable public registry in stable order."""

    return CHECK_REGISTRY


def registry_record() -> dict[str, Any]:
    records = [spec.to_record() for spec in CHECK_REGISTRY]
    return {
        "schema": REGISTRY_SCHEMA,
        "version": REGISTRY_VERSION,
        "registry_fingerprint": _sha256(_canonical_json(records)),
        "checks": records,
    }


def _canonical_root(path: Path) -> str:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise SelectionError(f"source root cannot be resolved: {path}: {exc}") from exc
    if not resolved.is_dir():
        raise SelectionError(f"source root is not a directory: {resolved}")
    return os.path.normcase(os.path.normpath(str(resolved)))


def _git(root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *arguments],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=10,
            creationflags=_WINDOWLESS_CREATION_FLAGS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SelectionError(f"Git identity read failed: {exc}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise SelectionError(
            f"Git identity read failed for {' '.join(arguments)}: {detail}"
        )
    return completed.stdout.strip()


def read_source_identity(
    root: str | Path,
    *,
    expected_branch: str | None = None,
    expected_tip: str | None = None,
) -> SourceIdentity:
    """Read and validate the exact repository identity used by selection."""

    root_path = Path(root).expanduser().resolve(strict=True)
    canonical = _canonical_root(root_path)
    top_level = Path(_git(root_path, "rev-parse", "--show-toplevel")).resolve(
        strict=True
    )
    if _canonical_root(top_level) != canonical:
        raise SelectionError(
            f"Git top level does not match requested source root: {top_level}"
        )
    branch = _git(root_path, "branch", "--show-current")
    if not branch:
        raise SelectionError("detached HEAD cannot satisfy exact branch binding")
    tip = _git(root_path, "rev-parse", "HEAD").lower()
    if not _COMMIT.fullmatch(tip):
        raise SelectionError("Git HEAD is not a full commit identity")
    common_value = Path(_git(root_path, "rev-parse", "--git-common-dir"))
    common = (
        root_path / common_value if not common_value.is_absolute() else common_value
    ).resolve(strict=True)
    identity = SourceIdentity(canonical, _canonical_root(common), branch, tip)
    if expected_branch is not None and branch != _nonempty(
        expected_branch, "expected_branch"
    ):
        raise SelectionError(
            f"branch mismatch: expected {expected_branch}, observed {branch}"
        )
    if expected_tip is not None:
        requested_tip = _nonempty(expected_tip, "expected_tip").lower()
        if not _COMMIT.fullmatch(requested_tip) or requested_tip != tip:
            raise SelectionError(
                f"tip mismatch: expected {expected_tip}, observed {tip}"
            )
    return identity


def _changed_path(root: Path, value: str) -> str:
    text = _nonempty(value, "changed_path").replace("\\", "/")
    candidate = Path(text)
    if candidate.is_absolute():
        try:
            text = (
                candidate.resolve(strict=False).relative_to(root.resolve()).as_posix()
            )
        except ValueError as exc:
            raise SelectionError(f"changed_path escapes source root: {value}") from exc
    return _relative_path(text, "changed_path")


def dependency_input_paths(spec: CheckSpec, root: str | Path) -> tuple[str, ...]:
    """Return the exact direct plus scoped inputs used by a check fingerprint."""

    root_path = Path(root).expanduser().resolve(strict=True)
    direct: list[str] = []
    for raw_path in spec.dependency_paths:
        relative = _relative_path(raw_path, f"{spec.stable_id}.dependency_path")
        path = (root_path / Path(relative)).resolve(strict=False)
        try:
            path.relative_to(root_path)
        except ValueError as exc:
            raise SelectionError(
                f"declared dependency escapes source root: {relative}"
            ) from exc
        if not path.is_file() or path.is_symlink():
            raise SelectionError(
                f"declared dependency is not a regular file: {relative}"
            )
        direct.append(relative)
    if not spec.input_scopes:
        return tuple(direct)
    return tuple(dict.fromkeys((*direct, *resolve_input_scope(spec, root_path))))


def dependency_fingerprint(spec: CheckSpec, root: str | Path) -> str:
    """Hash only declared direct files and validated scoped tracked inputs."""

    root_path = Path(root).expanduser().resolve(strict=True)
    inputs: list[dict[str, Any]] = []
    for relative in dependency_input_paths(spec, root_path):
        path = root_path / Path(relative)
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise SelectionError(
                f"cannot read declared dependency {relative}: {exc}"
            ) from exc
        inputs.append({"path": relative, "sha256": _sha256(data), "bytes": len(data)})
    return _sha256(_canonical_json({"check": spec.to_record(), "inputs": inputs}))


def runner_coordinate() -> dict[str, Any]:
    """Compact actual runner coordinate for the executing environment.

    PASS credit is reusable only while this coordinate is unchanged, so a
    Python interpreter or version change conservatively invalidates prior
    PASS instead of trusting it.
    """

    version = ".".join(str(part) for part in sys.version_info[:3])
    return {
        "python": {
            "executable": os.path.normcase(os.path.normpath(sys.executable)),
            "version": version,
        }
    }


def _runner_matches(value: object) -> bool:
    """True only when a credit's runner coordinate equals the current one."""

    if not isinstance(value, Mapping):
        return False
    python = value.get("python")
    if not isinstance(python, Mapping):
        return False
    current = runner_coordinate()["python"]
    return (
        python.get("executable") == current["executable"]
        and python.get("version") == current["version"]
    )


def credit_record(
    check: str | CheckSpec,
    root: str | Path,
    *,
    outcome: str = "PASS",
    observed_utc: str | None = None,
) -> dict[str, Any]:
    """Create a strict credit record after a check has actually passed."""

    spec = get_check(check) if isinstance(check, str) else check
    if outcome != "PASS":
        raise SelectionError("only PASS checks may publish credit")
    identity = read_source_identity(root)
    return {
        "schema": CREDIT_SCHEMA,
        "stable_id": spec.stable_id,
        "status": "PASS",
        "outcome": outcome,
        "tier": spec.tier,
        "command": list(spec.command),
        "output_contract": spec.output_contract,
        "dependency_fingerprint": dependency_fingerprint(spec, root),
        "source": identity.to_record(),
        "runner": runner_coordinate(),
        "observed_utc": observed_utc
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def get_check(stable_id: str) -> CheckSpec:
    for spec in CHECK_REGISTRY:
        if spec.stable_id == stable_id:
            return spec
    raise SelectionError(f"unknown stable check ID: {stable_id}")


def disposition_record(
    check: str | CheckSpec,
    root: str | Path,
    *,
    status: str,
    reason: str,
    observed_utc: str | None = None,
) -> dict[str, Any]:
    """Create a truthful non-PASS disposition record for one coarse unit.

    FAIL, UNRESOLVED, and SKIP are the only accepted statuses; PASS is owned
    by ``credit_record``.  A disposition keeps the same exact source identity
    and declared-input fingerprint as credit so a later invocation can hold
    or re-run the unit conservatively.
    """

    spec = get_check(check) if isinstance(check, str) else check
    normalized_status = _nonempty(status, "status").upper()
    if normalized_status not in DISPOSITION_STATUSES:
        raise SelectionError(
            f"disposition status must be one of {sorted(DISPOSITION_STATUSES)}"
        )
    normalized_reason = _nonempty(reason, "reason")
    identity = read_source_identity(root)
    return {
        "schema": DISPOSITION_SCHEMA,
        "stable_id": spec.stable_id,
        "status": normalized_status,
        "tier": spec.tier,
        "command": list(spec.command),
        "output_contract": spec.output_contract,
        "dependency_fingerprint": dependency_fingerprint(spec, root),
        "source": identity.to_record(),
        "reason": normalized_reason,
        "observed_utc": observed_utc
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def checkpoint_record(
    *,
    credits: Iterable[Mapping[str, Any]],
    dispositions: Iterable[Mapping[str, Any]],
    first_unresolved_unit: str | None = None,
) -> dict[str, Any]:
    """Build one validated checkpoint artifact record.

    The checkpoint is the existing ``-CreditFile`` interface: it holds only
    selector-validated PASS credit plus truthful non-PASS dispositions.  A
    unit can never hold both a PASS credit and a disposition.
    """

    credit_items = [dict(item) for item in credits]
    disposition_items = [dict(item) for item in dispositions]
    for item in credit_items:
        if item.get("schema") != CREDIT_SCHEMA or item.get("status") != "PASS":
            raise SelectionError("checkpoint credits must be PASS credit records")
        if not isinstance(item.get("stable_id"), str) or not item["stable_id"]:
            raise SelectionError("checkpoint credit has no stable ID")
    for item in disposition_items:
        if (
            item.get("schema") != DISPOSITION_SCHEMA
            or item.get("status") not in DISPOSITION_STATUSES
        ):
            raise SelectionError("checkpoint dispositions must carry a non-PASS status")
        if not isinstance(item.get("stable_id"), str) or not item["stable_id"]:
            raise SelectionError("checkpoint disposition has no stable ID")
        if not isinstance(item.get("reason"), str) or not item["reason"].strip():
            raise SelectionError(
                f"checkpoint disposition must carry a reason: {item.get('stable_id')}"
            )
    credit_ids = [item["stable_id"] for item in credit_items]
    disposition_ids = [item["stable_id"] for item in disposition_items]
    if len(set(credit_ids)) != len(credit_ids):
        raise SelectionError("checkpoint credits contain duplicate stable IDs")
    if len(set(disposition_ids)) != len(disposition_ids):
        raise SelectionError("checkpoint dispositions contain duplicate stable IDs")
    overlap = set(credit_ids).intersection(disposition_ids)
    if overlap:
        raise SelectionError(
            f"checkpoint must not hold both credit and disposition for: {sorted(overlap)}"
        )
    if first_unresolved_unit is not None:
        normalized_first = _nonempty(
            first_unresolved_unit, "first_unresolved_unit"
        )
        if normalized_first not in disposition_ids:
            raise SelectionError(
                "first_unresolved_unit must name a recorded disposition unit"
            )
    else:
        normalized_first = None
    return {
        "schema": CHECKPOINT_SCHEMA,
        "credits": credit_items,
        "dispositions": disposition_items,
        "first_unresolved_unit": normalized_first,
    }


def write_checkpoint(
    path: str | Path,
    *,
    credits: Iterable[Mapping[str, Any]],
    dispositions: Iterable[Mapping[str, Any]],
    first_unresolved_unit: str | None = None,
) -> dict[str, Any]:
    """Atomically persist a checkpoint artifact next to the caller's path."""

    target = Path(path).expanduser()
    if target.is_dir():
        raise SelectionError("checkpoint path must be a file, not a directory")
    target.parent.mkdir(parents=True, exist_ok=True)
    record = checkpoint_record(
        credits=credits,
        dispositions=dispositions,
        first_unresolved_unit=first_unresolved_unit,
    )
    payload = (json.dumps(record, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=target.parent,
            prefix=target.name + ".",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
        os.replace(temporary, target)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)
    return record


def merge_checkpoint_results(
    selection: Mapping[str, Any],
    results: Sequence[Mapping[str, Any]],
    input_checkpoint: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge one complete run's truthful unit results into a checkpoint.

    The selection is the exact registry-reconciled decision that produced the
    run; PASS credit is preserved only from that decision, and every non-PASS
    unit is recorded as a disposition with a reason.  Identity, source,
    registry, or environment uncertainty never becomes PASS here and never
    preserves an earlier PASS as trusted.
    """

    if selection.get("schema") != SELECTION_SCHEMA:
        raise SelectionError(
            "checkpoint merge requires an orchestrator-check-selection/v1 decision"
        )
    source = selection.get("source")
    if not isinstance(source, Mapping):
        raise SelectionError("checkpoint selection is missing its source identity")
    selected_by_id: dict[str, Mapping[str, Any]] = {}
    for item in selection.get("selected", ()):
        if not isinstance(item, Mapping):
            raise SelectionError("checkpoint selection has a malformed selected record")
        stable_id = item.get("stable_id")
        if not isinstance(stable_id, str) or not stable_id:
            raise SelectionError(
                "checkpoint selection has a selected record without a stable ID"
            )
        if stable_id in selected_by_id:
            raise SelectionError(
                f"checkpoint selection has duplicate selected IDs: {stable_id}"
            )
        selected_by_id[stable_id] = item

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in results:
        if not isinstance(item, Mapping):
            raise SelectionError("checkpoint results must be objects")
        stable_id = item.get("stable_id")
        status = item.get("status")
        if not isinstance(stable_id, str) or stable_id not in selected_by_id:
            raise SelectionError(f"checkpoint result is not a selected unit: {stable_id}")
        if stable_id in seen:
            raise SelectionError(
                f"checkpoint results contain a duplicate stable ID: {stable_id}"
            )
        seen.add(stable_id)
        if status not in {"PASS", "FAIL", "UNRESOLVED", "SKIP"}:
            raise SelectionError(
                f"checkpoint result has an invalid status: {stable_id}: {status}"
            )
        reason = item.get("reason")
        if status != "PASS" and (not isinstance(reason, str) or not reason.strip()):
            raise SelectionError(
                f"non-PASS checkpoint result must carry a reason: {stable_id}"
            )
        selected = selected_by_id[stable_id]
        command = selected.get("command")
        fingerprint = selected.get("dependency_fingerprint")
        if (
            not isinstance(command, list)
            or not command
            or not isinstance(fingerprint, str)
            or not _SHA256.fullmatch(fingerprint)
        ):
            raise SelectionError(
                f"checkpoint selection is missing command or fingerprint for {stable_id}"
            )
        record: dict[str, Any] = {
            "stable_id": stable_id,
            "tier": selected.get("tier"),
            "command": [str(part) for part in command],
            "output_contract": selected.get("output_contract"),
            "dependency_fingerprint": fingerprint,
            "source": dict(source),
            "observed_utc": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
        }
        if status == "PASS":
            record.update(
                {
                    "schema": CREDIT_SCHEMA,
                    "status": "PASS",
                    "outcome": "PASS",
                    "runner": runner_coordinate(),
                }
            )
        else:
            record.update(
                {
                    "schema": DISPOSITION_SCHEMA,
                    "status": status,
                    "reason": reason.strip(),
                }
            )
        normalized.append(record)

    preserved: list[dict[str, Any]] = []
    preserved_ids: set[str] = set()
    for item in selection.get("preserved_credit_records", ()):
        if not isinstance(item, Mapping):
            raise SelectionError(
                "checkpoint selection has a malformed preserved credit"
            )
        stable_id = item.get("stable_id")
        if not isinstance(stable_id, str) or not stable_id:
            raise SelectionError(
                "checkpoint selection has a preserved credit without a stable ID"
            )
        if stable_id in preserved_ids:
            raise SelectionError(
                f"checkpoint selection has duplicate preserved credits: {stable_id}"
            )
        if stable_id in selected_by_id:
            raise SelectionError(
                f"preserved PASS credit was also selected for execution: {stable_id}"
            )
        preserved_ids.add(stable_id)
        preserved.append(dict(item))

    # Identity, source, registry, or environment uncertainty never
    # preserves an earlier PASS as trusted: any UNRESOLVED or SKIP unit
    # means the run ended in uncertainty, so no preserved credit is
    # carried forward.
    identity_uncertain = any(
        item.get("status") in {"UNRESOLVED", "SKIP"} for item in results
    )
    if identity_uncertain:
        preserved = []
    result_ids = set(seen)
    kept_dispositions: list[dict[str, Any]] = []
    for item in _disposition_items(input_checkpoint):
        stable_id = item.get("stable_id")
        if not isinstance(stable_id, str):
            continue
        if stable_id in result_ids or stable_id in preserved_ids:
            continue
        kept_dispositions.append(dict(item))

    first_unresolved: str | None = next(
        (
            str(item.get("stable_id"))
            for item in normalized
            if item.get("status") == "UNRESOLVED"
        ),
        None,
    )
    if first_unresolved is None and isinstance(input_checkpoint, Mapping):
        prior_first = input_checkpoint.get("first_unresolved_unit")
        if isinstance(prior_first, str) and any(
            item.get("stable_id") == prior_first
            and item.get("status") == "UNRESOLVED"
            for item in kept_dispositions
        ):
            first_unresolved = prior_first

    credits = preserved + [
        dict(item) for item in normalized if item.get("status") == "PASS"
    ]
    dispositions = kept_dispositions + [
        dict(item) for item in normalized if item.get("status") != "PASS"
    ]
    return checkpoint_record(
        credits=credits,
        dispositions=dispositions,
        first_unresolved_unit=first_unresolved,
    )


def _credit_items(value: object) -> list[Mapping[str, Any]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        if "credits" in value:
            value = value["credits"]
        elif isinstance(value.get("green_test_ids"), list):
            value = [
                {"stable_id": item}
                for item in value["green_test_ids"]
                if isinstance(item, str)
            ]
        else:
            expanded: list[Mapping[str, Any]] = []
            for stable_id, item in value.items():
                if isinstance(item, Mapping):
                    expanded.append({"stable_id": stable_id, **dict(item)})
            value = expanded
    if not isinstance(value, list):
        raise SelectionError("credits must be a list or an object containing credits")
    result: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise SelectionError("credit records must be objects")
        result.append(item)
    return result


def _disposition_items(value: object) -> list[Mapping[str, Any]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        if "dispositions" in value:
            value = value["dispositions"]
        else:
            return []
    if not isinstance(value, list):
        raise SelectionError(
            "dispositions must be a list or an object containing dispositions"
        )
    result: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise SelectionError("disposition records must be objects")
        result.append(item)
    return result


def _valid_credit_shape(credit: Mapping[str, Any]) -> bool:
    stable_id = credit.get("stable_id")
    source = credit.get("source")
    return (
        isinstance(stable_id, str)
        and isinstance(source, Mapping)
        and credit.get("schema") == CREDIT_SCHEMA
        and credit.get("status") == "PASS"
        and credit.get("outcome") == "PASS"
        and isinstance(credit.get("tier"), str)
        and isinstance(credit.get("command"), list)
        and isinstance(credit.get("output_contract"), str)
        and isinstance(credit.get("dependency_fingerprint"), str)
        and bool(_SHA256.fullmatch(credit["dependency_fingerprint"]))
        and isinstance(credit.get("runner"), Mapping)
        and isinstance(credit["runner"].get("python"), Mapping)
        and isinstance(credit["runner"]["python"].get("executable"), str)
        and isinstance(credit["runner"]["python"].get("version"), str)
    )


def _is_ancestor(root: Path, origin_tip: str, current_tip: str) -> bool:
    if not _COMMIT.fullmatch(origin_tip) or not _COMMIT.fullmatch(current_tip):
        return False
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "merge-base",
                "--is-ancestor",
                origin_tip,
                current_tip,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=10,
            creationflags=_WINDOWLESS_CREATION_FLAGS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return completed.returncode == 0


def _source_matches(value: object, expected: SourceIdentity, root: Path) -> bool:
    """Validate origin identity without requiring the current tip to be unchanged."""

    if not isinstance(value, Mapping):
        return False
    origin_root = value.get("source_root")
    origin_common = value.get("git_common_dir")
    origin_branch = value.get("branch")
    origin_tip = value.get("tip")
    if (
        origin_root != expected.source_root
        or origin_common != expected.git_common_dir
        or origin_branch != expected.branch
        or not isinstance(origin_tip, str)
    ):
        return False
    return _is_ancestor(root, origin_tip.lower(), expected.tip)


def _eligible(
    spec: CheckSpec,
    intent: str,
    changed_paths: set[str],
    changed_domains: set[str],
    unknown_changes: bool,
) -> bool:
    if intent == "fast":
        return spec.tier == "fast"
    if intent == "affected":
        if spec.tier == "fast":
            return True
        if spec.tier == "affected":
            return unknown_changes or spec.consumes(changed_paths, changed_domains)
        return False
    if intent == "full":
        return spec.tier in {"fast", "affected", "full", "release"}
    return True


def select_checks(
    intent: str,
    root: str | Path,
    *,
    credits: object = None,
    dispositions: object = None,
    changed_paths: Sequence[str] | None = None,
    changed_domains: Sequence[str] = (),
    expected_branch: str | None = None,
    expected_tip: str | None = None,
    exclude_ids: Sequence[str] = (),
) -> SelectionDecision:
    """Select checks, preserve only exact still-valid PASS credit, and resume.

    The returned units are the failed/unresolved/affected/uncertain union in
    exact registry order: every eligible unit without selector-validated
    unchanged PASS credit is selected, so a validated PASS prefix is never
    replayed and changed or unknown inputs conservatively re-run.
    """

    if intent not in _INTENTS:
        raise SelectionError(f"intent must be one of {sorted(_INTENTS)}")
    source = read_source_identity(
        root, expected_branch=expected_branch, expected_tip=expected_tip
    )
    normalized_paths = tuple(
        sorted(
            {
                _changed_path(Path(source.source_root), item)
                for item in (changed_paths or ())
            }
        )
    )
    normalized_domains = tuple(
        sorted({_nonempty(item, "changed_domain") for item in changed_domains})
    )
    path_set = set(normalized_paths)
    domain_set = set(normalized_domains)
    unknown_changes = changed_paths is None and not normalized_domains
    excluded = set(exclude_ids)
    known = {spec.stable_id: spec for spec in CHECK_REGISTRY}
    raw_credits = _credit_items(credits)
    by_id: dict[str, Mapping[str, Any]] = {}
    rejected: set[str] = set()
    ambiguous: set[str] = set()
    for credit in raw_credits:
        stable_id = credit.get("stable_id")
        if (
            not isinstance(stable_id, str)
            or stable_id not in known
            or stable_id in by_id
        ):
            rejected.add(str(stable_id) if stable_id is not None else "<missing>")
            if isinstance(stable_id, str) and stable_id in known:
                ambiguous.add(stable_id)
                by_id.pop(stable_id, None)
            continue
        by_id[stable_id] = credit
        if not _valid_credit_shape(credit):
            rejected.add(stable_id)

    dispositions_by_id: dict[str, Mapping[str, Any]] = {}
    for disposition in _disposition_items(dispositions):
        stable_id = disposition.get("stable_id")
        status = disposition.get("status")
        if (
            not isinstance(stable_id, str)
            or stable_id not in known
            or stable_id in dispositions_by_id
            or not isinstance(status, str)
            or status.upper() not in DISPOSITION_STATUSES
        ):
            # A malformed or unknown disposition is conservative: the unit
            # holds no valid PASS credit and is simply re-selected.
            continue
        dispositions_by_id[stable_id] = disposition
    for stable_id in sorted(set(by_id).intersection(dispositions_by_id)):
        # A unit can never hold both PASS credit and a non-PASS disposition;
        # the contradiction fails closed and re-runs the unit.
        ambiguous.add(stable_id)
        by_id.pop(stable_id, None)
        dispositions_by_id.pop(stable_id, None)
        rejected.add(stable_id)

    fingerprints = {
        spec.stable_id: dependency_fingerprint(spec, source.source_root)
        for spec in CHECK_REGISTRY
    }
    preserved: list[str] = []
    preserved_records: list[dict[str, Any]] = []
    invalidated: list[str] = []
    selected: list[SelectedCheck] = []
    for spec in CHECK_REGISTRY:
        credit = by_id.get(spec.stable_id)
        disposition = dispositions_by_id.get(spec.stable_id)
        changed_dependency = bool(
            (changed_paths is not None or normalized_domains)
            and spec.consumes(path_set, domain_set)
        )
        is_valid = bool(
            credit is not None
            and spec.stable_id not in ambiguous
            and disposition is None
            and _valid_credit_shape(credit)
            and _source_matches(credit.get("source"), source, Path(source.source_root))
            and _runner_matches(credit.get("runner"))
            and not spec.external_requirements
            and credit.get("tier") == spec.tier
            and credit.get("command") == list(spec.command)
            and credit.get("output_contract") == spec.output_contract
            and credit.get("dependency_fingerprint") == fingerprints[spec.stable_id]
            and not changed_dependency
        )
        if is_valid:
            preserved.append(spec.stable_id)
            preserved_records.append(dict(credit))
        elif credit is not None:
            invalidated.append(spec.stable_id)
        if spec.stable_id in excluded or not _eligible(
            spec, intent, path_set, domain_set, unknown_changes
        ):
            continue
        if is_valid:
            continue
        if disposition is not None:
            reason = _DISPOSITION_REASONS[str(disposition.get("status")).upper()]
        elif credit is None:
            reason = "missing-credit"
        else:
            reason = "stale-or-invalid-credit"
        # Selected units keep the immutable registry order; the union begins
        # at its earliest registry member and never replays a preserved PASS
        # prefix.
        selected.append(SelectedCheck(spec, fingerprints[spec.stable_id], reason))

    registry_hash = registry_record()["registry_fingerprint"]
    return SelectionDecision(
        intent=intent,
        source=source,
        selected=tuple(selected),
        preserved_credit_ids=tuple(sorted(preserved)),
        preserved_credit_records=tuple(preserved_records),
        invalidated_credit_ids=tuple(sorted(set(invalidated).union(rejected))),
        rejected_credit_ids=tuple(sorted(rejected)),
        registry_fingerprint=registry_hash,
        changed_paths=normalized_paths,
        changed_domains=normalized_domains,
    )


def _load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SelectionError(f"cannot read JSON file {path}: {exc}") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Portable stable release-check registry and selector"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("registry", help="print the versioned check registry")
    select = sub.add_parser("select", help="select checks for one exact source root")
    select.add_argument("--intent", choices=sorted(_INTENTS), required=True)
    select.add_argument("--root", required=True, type=Path)
    select.add_argument("--credit-file", type=Path)
    select.add_argument("--changed-path", action="append", default=[])
    select.add_argument("--changed-domain", action="append", default=[])
    select.add_argument("--expected-branch")
    select.add_argument("--expected-tip")
    select.add_argument("--exclude-id", action="append", default=[])
    checkpoint = sub.add_parser(
        "checkpoint", help="merge a run's unit results into the checkpoint file"
    )
    checkpoint.add_argument("--input", type=Path)
    checkpoint.add_argument("--selection", type=Path, required=True)
    checkpoint.add_argument("--results", type=Path, required=True)
    checkpoint.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "registry":
            print(json.dumps(registry_record(), indent=2, sort_keys=True))
            return 0
        if args.command == "checkpoint":
            selection = _load_json(args.selection)
            if not isinstance(selection, Mapping):
                raise SelectionError("checkpoint --selection must be a JSON object")
            results = _load_json(args.results)
            if not isinstance(results, list):
                raise SelectionError("checkpoint --results must be a JSON array")
            input_checkpoint = None
            if args.input is not None and Path(args.input).is_file():
                input_checkpoint = _load_json(args.input)
                if not isinstance(input_checkpoint, Mapping):
                    raise SelectionError("checkpoint --input must be a JSON object")
            merged = merge_checkpoint_results(selection, results, input_checkpoint)
            write_checkpoint(
                args.output,
                credits=merged["credits"],
                dispositions=merged["dispositions"],
                first_unresolved_unit=merged["first_unresolved_unit"],
            )
            return 0
        checkpoint_value = None
        if args.credit_file is not None:
            credit_path = Path(args.credit_file)
            if credit_path.exists() and not credit_path.is_file():
                raise SelectionError(
                    f"credit file path exists but is not a file: {credit_path}"
                )
            if credit_path.is_file():
                checkpoint_value = _load_json(credit_path)
            # A nonexistent -CreditFile path is an empty cold input; the
            # checkpoint becomes the output on the first truthful persist.
        decision = select_checks(
            args.intent,
            args.root,
            credits=checkpoint_value,
            dispositions=checkpoint_value,
            changed_paths=args.changed_path if args.changed_path else None,
            changed_domains=args.changed_domain,
            expected_branch=args.expected_branch,
            expected_tip=args.expected_tip,
            exclude_ids=args.exclude_id,
        )
        print(json.dumps(decision.to_record(), indent=2, sort_keys=True))
        return 0
    except SelectionError as exc:
        print(f"release-check selector error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CHECKPOINT_SCHEMA",
    "CHECK_REGISTRY",
    "CREDIT_SCHEMA",
    "DISPOSITION_SCHEMA",
    "DISPOSITION_STATUSES",
    "PUBLIC_ROUTE_DEPENDENCIES",
    "REAL_AGENT_ROUTE_DEPENDENCIES",
    "RELEASE_AGGREGATE_ID",
    "SELECTION_SCHEMA",
    "CheckSpec",
    "InputScope",
    "SelectionDecision",
    "SelectionError",
    "SourceIdentity",
    "checkpoint_record",
    "credit_record",
    "disposition_record",
    "merge_checkpoint_results",
    "write_checkpoint",
    "dependency_fingerprint",
    "dependency_input_paths",
    "get_check",
    "main",
    "read_source_identity",
    "registry",
    "runner_coordinate",
    "registry_record",
    "resolve_input_scope",
    "select_checks",
]
