"""Direct v2 launcher binding for Codex."""

from __future__ import annotations

import json
import os
import platform
import shutil
import tomllib
from pathlib import Path
from typing import Any

PROVIDER_ID = "codex"
ADAPTER_VERSION = "codex-v2"

_LAUNCH_CONFIG_KEYS = frozenset({"reasoning_effort", "service_tier"})
_OPTIONAL_LAUNCH_CONFIG_KEYS = frozenset({"launcher", "permission_mode"})


def _direct_codex_executable() -> str:
    """Use the packaged native executable for a Windows suspended launch."""
    if os.name != "nt":
        return "codex"
    shim = shutil.which("codex.cmd")
    if shim:
        arch = platform.machine().lower()
        suffix = "arm64" if arch in ("arm64", "aarch64") else "x64"
        triple = (
            "aarch64-pc-windows-msvc"
            if suffix == "arm64"
            else "x86_64-pc-windows-msvc"
        )
        package = Path(shim).parent / "node_modules" / "@openai" / "codex"
        candidates = (
            package
            / "node_modules"
            / "@openai"
            / f"codex-win32-{suffix}"
            / "vendor"
            / triple
            / "bin"
            / "codex.exe",
            package / "vendor" / triple / "bin" / "codex.exe",
        )
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate.resolve())
    standalone = shutil.which("codex.exe")
    if standalone:
        return standalone
    raise FileNotFoundError(
        "native codex.exe not found for suspended Windows worker launch"
    )


def _toml_inline(value: str | dict[str, Any]) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, dict):
        return "{" + ", ".join(
            f"{json.dumps(str(key))} = {_toml_inline(child)}"
            for key, child in value.items()
        ) + "}"
    raise ValueError("unsupported Codex worker permission value")


def _ollama_catalog_path() -> Path:
    return Path(__file__).resolve().parents[3] / "local-config" / "ollama-model-catalog.json"


def _ollama_model_catalog(model: str, reasoning_effort: str) -> Path:
    """Use explicitly prepared metadata, never an OpenAI/fallback catalog."""
    path = _ollama_catalog_path()
    try:
        catalog = json.loads(path.read_text(encoding="utf-8"))
        entries = catalog["models"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"prepare the operator-owned Ollama model catalog: {path}") from exc
    if not isinstance(entries, list):
        raise ValueError(f"invalid Ollama model catalog: {path}")
    matching = [item for item in entries if isinstance(item, dict) and item.get("slug") == model]
    if len(matching) != 1:
        raise ValueError(f"Ollama model catalog must contain exactly one entry for {model}")
    levels = matching[0].get("supported_reasoning_levels", [])
    if not isinstance(levels, list) or reasoning_effort not in {
        item.get("effort") for item in levels if isinstance(item, dict)
    }:
        raise ValueError(f"Ollama model {model} does not advertise reasoning effort {reasoning_effort}")
    return path.resolve()


def _isolation_overrides(worktree: str, isolation_config_path: str | None = None) -> list[str]:
    config_path = Path(isolation_config_path) if isolation_config_path else Path(worktree) / ".codex" / "config.toml"
    if not config_path.is_file():
        if Path(worktree).exists():
            raise ValueError(f"Codex worker isolation profile missing: {config_path}")
        return []
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    profile = config.get("permissions", {}).get("worker-isolated")
    if not isinstance(profile, dict) or profile.get("extends") != ":workspace":
        raise ValueError(f"Codex worker isolation profile invalid: {config_path}")
    filesystem = profile.get("filesystem")
    if not isinstance(filesystem, dict) or not filesystem:
        raise ValueError(f"Codex worker filesystem isolation missing: {config_path}")
    codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")).resolve()
    if filesystem.get(str(codex_home)) != "deny":
        raise ValueError(f"Codex worker auth directory is not denied: {config_path}")
    return [
        'permissions.worker-isolated.extends=":workspace"',
        "permissions.worker-isolated.filesystem=" + _toml_inline(filesystem),
    ]


def validate_launch_config(*, model: str, launch_config: dict[str, Any]) -> dict[str, str]:
    """Validate every Codex model preference before a provider can start."""
    if not isinstance(model, str) or not model.strip():
        raise ValueError("Codex model must be configured")
    if not isinstance(launch_config, dict):
        raise ValueError("Codex launch_config must be an object")
    missing = sorted(_LAUNCH_CONFIG_KEYS - set(launch_config))
    if missing:
        raise ValueError(f"Codex launch_config is missing: {', '.join(missing)}")
    unsupported = sorted(set(launch_config) - _LAUNCH_CONFIG_KEYS - _OPTIONAL_LAUNCH_CONFIG_KEYS)
    if unsupported:
        raise ValueError(f"Codex launch_config has unsupported options: {', '.join(unsupported)}")
    normalized: dict[str, str] = {}
    for key in sorted(_LAUNCH_CONFIG_KEYS):
        value = launch_config[key]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Codex launch_config {key} must be a non-empty string")
        normalized[key] = value.strip()
    if "launcher" in launch_config:
        if launch_config["launcher"] not in ("codex", "ollama"):
            raise ValueError("Codex launch_config launcher must be codex or ollama")
        normalized["launcher"] = launch_config["launcher"]
    if "permission_mode" in launch_config:
        if launch_config["permission_mode"] not in ("worker-isolated", "yolo"):
            raise ValueError("Codex launch_config permission_mode must be worker-isolated or yolo")
        normalized["permission_mode"] = launch_config["permission_mode"]
    return normalized


def build_argv(
    *,
    model: str,
    launch_config: dict[str, Any],
    worktree: str,
    prompt_path: str,
    session_id: str | None = None,
    resume: bool = False,
    isolation_config_path: str | None = None,
) -> list[str]:
    """Build the provider-owned, stdin-prompted Codex launch vector."""
    del prompt_path
    configured = validate_launch_config(model=model, launch_config=launch_config)
    if configured.get("permission_mode") == "yolo":
        permission_args = ["--yolo"]
    else:
        isolation_overrides = _isolation_overrides(worktree, isolation_config_path)
        permission_args = [
            "-c", 'default_permissions="worker-isolated"',
            *[part for override in isolation_overrides for part in ("-c", override)],
            *(["-c", 'windows.sandbox="elevated"'] if os.name == "nt" else []),
        ]
    argv = ["codex", "exec", "--ignore-user-config"]
    if resume:
        if not session_id:
            raise ValueError("Codex resume requires a session ID")
        argv.extend(["resume", session_id])
    argv.extend(
        [
            *permission_args,
            "--skip-git-repo-check",
            "-c",
            'approval_policy="never"',
            "-m",
            model,
            "-c",
            f"model_reasoning_effort={json.dumps(configured['reasoning_effort'])}",
            "-c",
            f"service_tier={json.dumps(configured['service_tier'])}",
            "--dangerously-bypass-hook-trust",
            "-c",
            "features.hooks=true",
            "-c",
            f'projects={{ {json.dumps(worktree, ensure_ascii=False)} = {{ trust_level="trusted" }} }}',
            "--json",
            "--output-last-message",
            str(Path(worktree) / ".agent-workspace" / "last-message.txt"),
        ]
    )
    if not resume:
        argv.extend(["--cd", worktree])
    argv.append("-")
    if configured.get("launcher") == "ollama":
        # Select transport inside exec: outer `ollama launch` profile settings
        # do not survive --ignore-user-config and can route requests to OpenAI.
        catalog = _ollama_model_catalog(model, configured["reasoning_effort"])
        # OSS selection belongs to exec; resume has no --oss option of its own.
        argv[2:2] = ["--oss", "--local-provider", "ollama"]
        argv[-1:-1] = ["-c", f"model_catalog_json={json.dumps(str(catalog))}"]
    argv[0] = _direct_codex_executable()
    return argv


def parse_line(line: str) -> dict[str, Any] | None:
    """Parse one Codex JSON transcript line into controller facts."""
    try:
        value = json.loads(line)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    raw_type = value.get("type")
    if raw_type not in {"thread.started", "item.completed", "turn.completed", "turn.failed", "turn.cancelled"}:
        return None
    session_id = value.get("thread_id") or value.get("threadId")
    parsed: dict[str, Any] = {}
    if isinstance(session_id, str) and session_id:
        parsed["session_id"] = session_id
    if raw_type == "item.completed":
        item = value.get("item")
        if not isinstance(item, dict) or item.get("type") != "agent_message":
            return None
        text = item.get("text")
        if isinstance(text, str) and text:
            parsed["message"] = text
    # Successful completion carries usage, not assistant text. Returning a
    # terminal label here overwrites Codex's --output-last-message contents.
    if raw_type in {"turn.failed", "turn.cancelled"}:
        parsed["message"] = raw_type
        parsed["non_retryable_failure"] = True
    return parsed
