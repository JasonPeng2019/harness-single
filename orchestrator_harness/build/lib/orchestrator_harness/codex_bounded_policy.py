"""Provider-owned bounded-execution launcher/exclusion policy for the Codex adapter.

REQ-O43 product counterpart of SRC-010: an editable launcher JSON plus one
Git-ignore exclusion file, host-shell-correct chain segmentation, Git-backed
path evaluation with policy-source provenance, fail-closed actionable
handling, stateless concurrent evaluation, and worktree hook packaging.

Generic workflow/session/MCP core never references these provider filenames.
The provider adapter owns every policy file, launcher name, and hook.  The
approved stable-runner script and the provider supervisor entrypoint are the
only initial exclusions, so lane-managed orchestration sessions receive no
bounded-test deadline while every non-excluded covered nested command launched
inside an agent worktree still requires the one supervisor.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

LAUNCHERS_SCHEMA = "bounded-launchers/v1"
LAUNCHERS_RELATIVE = Path(".codex") / "policies" / "bounded-launchers.json"
EXCLUSIONS_RELATIVE = Path(".codex") / "policies" / "bounded-exclusions.gitignore"
SUPERVISOR_RELATIVE = Path(".codex") / "scripts" / "Invoke-BoundedTest.ps1"
POLICY_READBACK_SCHEMA = "orchestrator-codex-bounded-policy/v1"
EXPECTED_CATEGORIES = frozenset({"python_script", "powershell_file", "posix_shell"})


class BoundedPolicyError(ValueError):
    """The provider bounded policy is missing, invalid, or failed closed."""


@dataclass(frozen=True)
class CoveredInvocation:
    """One covered script operand resolved from a command segment."""

    script: str | None


def _deny(reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def load_launchers(policy_root: str | Path) -> dict[str, frozenset[str]]:
    """Strictly load and validate the editable launcher JSON for one root."""
    root = Path(policy_root)
    path = root / LAUNCHERS_RELATIVE
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BoundedPolicyError(
            f"cannot read valid bounded launcher policy {path}: {exc}"
        ) from exc
    if not isinstance(value, dict) or set(value) != {"schema", "launcher_categories"}:
        raise BoundedPolicyError(
            "bounded launcher policy must contain only schema and launcher_categories"
        )
    if value.get("schema") != LAUNCHERS_SCHEMA:
        raise BoundedPolicyError("bounded launcher policy has the wrong schema")
    categories = value.get("launcher_categories")
    if not isinstance(categories, dict) or set(categories) != EXPECTED_CATEGORIES:
        raise BoundedPolicyError(
            "bounded launcher policy must declare the three supported categories"
        )
    result: dict[str, frozenset[str]] = {}
    for category, raw_names in categories.items():
        if not isinstance(raw_names, list) or any(
            not isinstance(name, str) or not name.strip() for name in raw_names
        ):
            raise BoundedPolicyError(
                f"bounded launcher category {category} must be a string list"
            )
        result[category] = frozenset(_launcher_name(name) for name in raw_names)
    return result


def validate_exclusions(policy_root: str | Path) -> Path:
    """Validate that the exclusion file is readable UTF-8 and return its path."""
    root = Path(policy_root)
    path = root / EXCLUSIONS_RELATIVE
    try:
        path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise BoundedPolicyError(
            f"cannot read valid bounded exclusion policy {path}: {exc}"
        ) from exc
    return path


def _launcher_name(token: str) -> str:
    name = token.strip("\"'").replace("\\", "/").rsplit("/", 1)[-1].casefold()
    return name[:-4] if name.endswith(".exe") else name


def _tokens(segment: str) -> list[str]:
    try:
        values = shlex.split(segment, posix=False)
    except ValueError:
        return []
    return [value.strip("\"'") for value in values]


def _python_script(arguments: list[str]) -> str | None:
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        lowered = argument.casefold()
        if lowered in {"-c", "-m"} or lowered.startswith(("-c", "-m")):
            return None
        if lowered in {"-w", "-x"}:
            index += 2
            continue
        if lowered == "--":
            return arguments[index + 1] if index + 1 < len(arguments) else None
        if lowered.startswith("-"):
            index += 1
            continue
        return argument
    return None


def _shell_script(arguments: list[str]) -> str | None:
    for argument in arguments:
        lowered = argument.casefold()
        if lowered == "--":
            continue
        if lowered.startswith("-"):
            if "c" in lowered[1:]:
                return None
            continue
        return argument
    return None


def _covered_segment(
    segment: str, launchers: Mapping[str, frozenset[str]]
) -> CoveredInvocation | None:
    values = _tokens(segment)
    while values and values[0] in {"&", "."}:
        values.pop(0)
    if not values:
        return None

    first_name = _launcher_name(values[0])
    if first_name == "uv" and any(value.casefold() == "run" for value in values[1:]):
        run_index = next(
            index for index, value in enumerate(values) if value.casefold() == "run"
        )
        for index in range(run_index + 1, len(values)):
            if _launcher_name(values[index]) in launchers["python_script"]:
                return CoveredInvocation(_python_script(values[index + 1 :]))

    if first_name in launchers["python_script"]:
        return CoveredInvocation(_python_script(values[1:]))

    if first_name in launchers["powershell_file"]:
        for index, value in enumerate(values[1:], start=1):
            if value.casefold() in {"-file", "-f"}:
                script = values[index + 1] if index + 1 < len(values) else None
                return CoveredInvocation(script)
        return None

    if values[0].casefold().endswith(".ps1"):
        return CoveredInvocation(values[0])

    shell_index = 0
    if first_name == "wsl" and len(values) > 1:
        shell_index = 1
    if _launcher_name(values[shell_index]) in launchers["posix_shell"]:
        return CoveredInvocation(_shell_script(values[shell_index + 1 :]))
    return None


def command_segments(command: str) -> list[str]:
    """Split one PowerShell host-shell command chain at unquoted separators.

    Semicolon, pipe, double-pipe, double-ampersand, CR, and LF separate only
    outside quotes.  Backtick escapes the next character outside quotes and
    inside double quotes; backslash is never an escape inside PowerShell
    single-quoted text.
    """
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(command):
        character = command[index]
        if quote is not None:
            current.append(character)
            if quote == '"' and character == "`" and index + 1 < len(command):
                index += 1
                current.append(command[index])
            elif character == quote:
                quote = None
            index += 1
            continue
        if character == "`" and index + 1 < len(command):
            current.append(character)
            index += 1
            current.append(command[index])
            if (
                command[index] == "\r"
                and index + 1 < len(command)
                and command[index + 1] == "\n"
            ):
                index += 1
                current.append(command[index])
            index += 1
            continue
        if character in {"'", '"'}:
            quote = character
            current.append(character)
            index += 1
            continue
        separator_length = 0
        if character in {";", "|", "\r", "\n"}:
            separator_length = (
                2 if character == "|" and command[index : index + 2] == "||" else 1
            )
        elif command[index : index + 2] == "&&":
            separator_length = 2
        if separator_length:
            if part := "".join(current).strip():
                parts.append(part)
            current = []
            index += separator_length
            continue
        current.append(character)
        index += 1
    if part := "".join(current).strip():
        parts.append(part)
    return parts


def _working_directory(payload: Mapping[str, Any], policy_root: Path) -> Path:
    tool_input = payload.get("tool_input")
    raw = tool_input.get("workdir") if isinstance(tool_input, Mapping) else None
    if not isinstance(raw, str) or not raw:
        raw = payload.get("cwd")
    return (
        Path(raw).resolve(strict=False) if isinstance(raw, str) and raw else policy_root
    )


def is_excluded(
    script: str,
    *,
    cwd: Path,
    policy_root: Path,
    exclusions: Path,
) -> bool:
    """Evaluate one resolved script path with Git's own ignore engine.

    The exclusion is accepted only when Git's winning verbose match came from
    the provider's declared exclusion file and is not negated.  An ordinary
    repository ignore rule is never bounded-policy authorization.  Git startup,
    evaluation, and unreadable-output failures fail closed.
    """
    candidate = Path(script)
    if not candidate.is_absolute():
        candidate = cwd / candidate
    candidate = candidate.resolve(strict=False)
    root = policy_root.resolve(strict=False)
    try:
        relative = candidate.relative_to(root).as_posix()
    except ValueError:
        return False
    try:
        completed = subprocess.run(
            [
                "git",
                "-c",
                f"core.excludesFile={exclusions.as_posix()}",
                "-c",
                "core.quotePath=false",
                "check-ignore",
                "--no-index",
                "--verbose",
                relative,
            ],
            cwd=root,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise BoundedPolicyError(
            f"git could not start to evaluate bounded exclusions: {exc}"
        ) from exc
    if completed.returncode not in {0, 1}:
        raise BoundedPolicyError(
            f"git could not evaluate bounded exclusions: {completed.stderr.strip()}"
        )
    if completed.returncode == 1:
        return False
    match = re.match(r"^(.*):(\d+):(.*)\t", completed.stdout.strip())
    if match is None:
        raise BoundedPolicyError("git returned an unreadable bounded-exclusion match")
    source = Path(match.group(1).strip("\"'")).resolve(strict=False)
    return source == exclusions.resolve(strict=False) and not match.group(3).startswith(
        "!"
    )


def guard_pre_tool_use(
    payload: Mapping[str, Any],
    *,
    policy_root: str | Path,
) -> dict[str, Any]:
    """Assess every actual host-shell segment without shared mutable state.

    Returns an empty object when nothing covered is present, an allow decision
    when every covered segment is a policy-file exclusion, or a deny decision
    with an actionable reason.  Configuration and Git failures fail closed with
    a deny rather than a hook crash.
    """
    tool_input = payload.get("tool_input")
    command = tool_input.get("command") if isinstance(tool_input, Mapping) else None
    if not isinstance(command, str):
        return {}
    root = Path(policy_root).resolve(strict=False)
    try:
        launchers = load_launchers(root)
        exclusions = validate_exclusions(root)
        cwd = _working_directory(payload, root)
        covered = [
            invocation
            for segment in command_segments(command)
            if (invocation := _covered_segment(segment, launchers)) is not None
        ]
        if not covered:
            return {}
        if all(
            invocation.script is not None
            and is_excluded(
                invocation.script,
                cwd=cwd,
                policy_root=root,
                exclusions=exclusions,
            )
            for invocation in covered
        ):
            return {}
    except BoundedPolicyError as exc:
        return _deny(
            "BOUNDED-TEST-v1 configuration error: "
            f"{exc}. Fix the provider policy before retrying."
        )

    return _deny(
        "Direct configured Python-script, executed .ps1, and POSIX-shell "
        "execution is blocked by the provider bounded-execution policy unless "
        "its resolved script path matches the provider exclusion file. Run "
        "covered work through the bounded supervisor "
        f"({SUPERVISOR_RELATIVE.as_posix()}) with a justified expected upper "
        "bound, bounded cleanup allowance, computed maximum lifetime, "
        "heartbeat interval, timeout basis, and unique result path."
    )


def bounded_policy_status(policy_root: str | Path) -> dict[str, Any]:
    """Readback of the editable provider policy without any Git side effects."""
    root = Path(policy_root).resolve(strict=False)
    launchers_path = root / LAUNCHERS_RELATIVE
    exclusions_path = root / EXCLUSIONS_RELATIVE
    record: dict[str, Any] = {
        "schema": POLICY_READBACK_SCHEMA,
        "policy_root": str(root),
        "launchers_path": str(launchers_path),
        "exclusions_path": str(exclusions_path),
        "launchers_valid": False,
        "exclusions_valid": False,
        "launchers": {},
        "exclusion_lines": [],
        "error": None,
    }
    try:
        launchers = load_launchers(root)
        record["launchers_valid"] = True
        record["launchers"] = {
            category: sorted(names) for category, names in launchers.items()
        }
    except BoundedPolicyError as exc:
        record["error"] = str(exc)
    try:
        validate_exclusions(root)
        record["exclusions_valid"] = True
        record["exclusion_lines"] = [
            line.strip()
            for line in exclusions_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    except (BoundedPolicyError, OSError, UnicodeError) as exc:
        if record["error"] is None:
            record["error"] = str(exc)
    return record


__all__ = [
    "BoundedPolicyError",
    "CoveredInvocation",
    "EXCLUSIONS_RELATIVE",
    "LAUNCHERS_RELATIVE",
    "LAUNCHERS_SCHEMA",
    "POLICY_READBACK_SCHEMA",
    "SUPERVISOR_RELATIVE",
    "bounded_policy_status",
    "command_segments",
    "guard_pre_tool_use",
    "is_excluded",
    "load_launchers",
    "validate_exclusions",
]
