from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
POLICY_CONFIG = ROOT / ".agent" / "bounded-script-policy.json"
LAUNCHERS = ROOT / ".agent" / "bounded-launchers.json"
EXCLUSIONS = ROOT / ".agent" / "bounded-exclusions.gitignore"
EXPECTED_CATEGORIES = frozenset({"python_script", "powershell_file", "posix_shell"})


class PolicyError(ValueError):
    pass


@dataclass(frozen=True)
class CoveredInvocation:
    script: str | None


def deny(reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def load_enabled() -> bool:
    try:
        value = json.loads(POLICY_CONFIG.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyError(f"cannot read valid optional bounded script policy {POLICY_CONFIG}: {exc}") from exc
    if not isinstance(value, dict) or set(value) != {"schema", "enabled"}:
        raise PolicyError("optional bounded script policy must contain only schema and enabled")
    if value["schema"] != "portable-bounded-script-policy/v1" or not isinstance(value["enabled"], bool):
        raise PolicyError("optional bounded script policy has an invalid schema or enabled value")
    return value["enabled"]


def launcher_name(token: str) -> str:
    name = token.strip("\"'").replace("\\", "/").rsplit("/", 1)[-1].casefold()
    return name[:-4] if name.endswith(".exe") else name


def load_launchers() -> dict[str, frozenset[str]]:
    try:
        value = json.loads(LAUNCHERS.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyError(f"cannot read valid bounded launcher policy {LAUNCHERS}: {exc}") from exc
    if not isinstance(value, dict) or set(value) != {"schema", "launcher_categories"}:
        raise PolicyError("bounded launcher policy must contain only schema and launcher_categories")
    if value["schema"] != "portable-bounded-launchers/v1":
        raise PolicyError("bounded launcher policy has the wrong schema")
    categories = value["launcher_categories"]
    if not isinstance(categories, dict) or set(categories) != EXPECTED_CATEGORIES:
        raise PolicyError("bounded launcher policy must declare the three supported categories")
    result: dict[str, frozenset[str]] = {}
    for category, names in categories.items():
        if not isinstance(names, list) or any(not isinstance(name, str) or not name.strip() for name in names):
            raise PolicyError(f"bounded launcher category {category} must be a non-empty string list")
        result[category] = frozenset(launcher_name(name) for name in names)
    return result


def tokens(segment: str) -> list[str]:
    try:
        values = shlex.split(segment, posix=False)
    except ValueError:
        return []
    return [value.strip("\"'") for value in values]


def python_script(arguments: list[str]) -> str | None:
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        lowered = argument.casefold()
        if lowered == "--":
            return arguments[index + 1] if index + 1 < len(arguments) else None
        if lowered in {"-c", "-m"} or lowered.startswith(("-c", "-m")):
            return None
        if lowered in {"-w", "-x"}:
            index += 2
            continue
        if lowered.startswith("-"):
            index += 1
            continue
        return argument
    return None


def shell_script(arguments: list[str]) -> str | None:
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


def covered_segment(segment: str, launchers: dict[str, frozenset[str]]) -> CoveredInvocation | None:
    values = tokens(segment)
    while values and values[0] in {"&", "."}:
        values.pop(0)
    if not values:
        return None
    first_name = launcher_name(values[0])
    if first_name == "uv" and any(value.casefold() == "run" for value in values[1:]):
        run_index = next(index for index, value in enumerate(values) if value.casefold() == "run")
        for index in range(run_index + 1, len(values)):
            if launcher_name(values[index]) in launchers["python_script"]:
                return CoveredInvocation(python_script(values[index + 1 :]))
    if first_name in launchers["python_script"]:
        return CoveredInvocation(python_script(values[1:]))
    if first_name in launchers["powershell_file"]:
        for index, value in enumerate(values[1:], start=1):
            if value.casefold() in {"-file", "-f"}:
                return CoveredInvocation(values[index + 1] if index + 1 < len(values) else None)
        return None
    if values[0].casefold().endswith(".ps1"):
        return CoveredInvocation(values[0])
    shell_index = 1 if first_name == "wsl" and len(values) > 1 else 0
    if launcher_name(values[shell_index]) in launchers["posix_shell"]:
        return CoveredInvocation(shell_script(values[shell_index + 1 :]))
    return None


def command_segments(command: str) -> list[str]:
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
            current.extend((character, command[index + 1]))
            index += 2
            continue
        if character in {"'", '"'}:
            quote = character
            current.append(character)
            index += 1
            continue
        if character in {";", "|", "\r", "\n"}:
            separator_length = 2 if character == "|" and command[index : index + 2] == "||" else 1
        elif command[index : index + 2] == "&&":
            separator_length = 2
        else:
            separator_length = 0
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


def working_directory(payload: dict[str, Any]) -> Path:
    tool_input = payload.get("tool_input")
    raw = tool_input.get("workdir") if isinstance(tool_input, dict) else None
    if not isinstance(raw, str) or not raw:
        raw = payload.get("cwd")
    return Path(raw).resolve(strict=False) if isinstance(raw, str) and raw else ROOT


def is_excluded(script: str, *, cwd: Path) -> bool:
    candidate = Path(script)
    if not candidate.is_absolute():
        candidate = cwd / candidate
    candidate = candidate.resolve(strict=False)
    try:
        relative = candidate.relative_to(ROOT.resolve(strict=False)).as_posix()
    except ValueError:
        return False
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(ROOT),
                "-c",
                f"core.excludesFile={EXCLUSIONS.as_posix()}",
                "-c",
                "core.quotePath=false",
                "check-ignore",
                "--no-index",
                "--verbose",
                relative,
            ],
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise PolicyError(f"Git could not start to evaluate bounded exclusions: {exc}") from exc
    if completed.returncode not in {0, 1}:
        raise PolicyError(f"Git could not evaluate bounded exclusions: {completed.stderr.strip()}")
    if completed.returncode == 1:
        return False
    match = re.match(r"^(.*):(\d+):(.*)\t", completed.stdout.strip())
    if match is None or match.group(3).startswith("!"):
        raise PolicyError("Git returned an unreadable bounded-exclusion match")
    return Path(match.group(1).strip("\"'")).resolve(strict=False) == EXCLUSIONS.resolve(strict=False)


def guard(payload: dict[str, Any]) -> dict[str, Any]:
    if not POLICY_CONFIG.is_file():
        return {}
    try:
        if not load_enabled():
            return {}
        if not EXCLUSIONS.is_file():
            raise PolicyError(f"cannot read bounded exclusion policy {EXCLUSIONS}")
        tool_input = payload.get("tool_input")
        command = tool_input.get("command") if isinstance(tool_input, dict) else None
        if not isinstance(command, str):
            return {}
        launchers = load_launchers()
        covered = [
            invocation
            for segment in command_segments(command)
            if (invocation := covered_segment(segment, launchers)) is not None
        ]
        if not covered:
            return {}
        cwd = working_directory(payload)
        if all(invocation.script is not None and is_excluded(invocation.script, cwd=cwd) for invocation in covered):
            return {}
    except PolicyError as exc:
        return deny(f"Optional bounded script policy configuration error: {exc}. Fix it or disable the policy.")
    return deny(
        "Direct Python, executed PowerShell, or POSIX-shell execution is blocked by the enabled "
        "optional bounded script policy. Run it through .agent/run-bounded with a justified expected runtime, "
        "cleanup allowance, timeout basis, and unique result path, or add a narrow truthful exclusion."
    )


def payload() -> dict[str, Any]:
    try:
        value = json.loads(sys.stdin.read() or "{}")
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def main(argv: list[str] | None = None) -> int:
    if (argv if argv is not None else sys.argv[1:]) != ["pre-tool-use"]:
        print("bounded-script-adapter: expected pre-tool-use", file=sys.stderr)
        return 2
    print(json.dumps(guard(payload())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
