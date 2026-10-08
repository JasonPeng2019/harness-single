"""Provider-native web controls inspected immediately before process spawn.

The controller supplies the requested mode from the durable captured
preparation. This module does not resolve permissions, block shell egress, or
prove Atlas service-boundary behavior.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from .setup import _is_reparse


_PROFILES = frozenset({"soft_guardrail_network", "atlas_memory_only", "restricted_local"})
_QWEN_TOOLS = frozenset({"web_search", "web_fetch"})


def _qwen_settings_path(worktree: Path) -> Path:
    """Reject redirected installed settings and confirm physical containment."""
    root = Path(worktree)
    path = root / ".qwen" / "settings.json"
    for component in (root, path.parent, path):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if _is_reparse(info):
            raise ValueError(f"Qwen settings path is a link or reparse point: {component}")
    physical_root = root.resolve(strict=True)
    if not path.resolve(strict=False).is_relative_to(physical_root):
        raise ValueError(f"Qwen settings path escapes worktree: {path}")
    return path


def _string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) and bool(item) for item in value)


def _qwen_cli_supports_deny(argv: list[str], worktree: Path) -> bool:
    """Limit the argv control to the installed CLI version inspected here."""
    if not argv:
        return False
    try:
        result = subprocess.run(
            [argv[0], "--version"], cwd=worktree, capture_output=True,
            text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and result.stdout.strip() == "0.21.10"


def install_soft_controls(provider_id: str, worktree: Path) -> None:
    """Compose Qwen's worktree settings before launch; other providers use argv.

    Existing unrelated Qwen settings are retained. A malformed settings file is
    a collision, not permission to replace the user's configuration.
    """
    if provider_id != "qwen-code":
        return
    path = _qwen_settings_path(worktree)
    if path.exists():
        settings = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(settings, dict):
            raise ValueError(f"Qwen settings are not an object: {path}")
    else:
        settings = {}
    tools = settings.setdefault("tools", {})
    permissions = settings.setdefault("permissions", {})
    if not isinstance(tools, dict) or not isinstance(permissions, dict):
        raise ValueError(f"Qwen settings have invalid tools or permissions: {path}")
    disabled = tools.setdefault("disabled", [])
    denied = permissions.setdefault("deny", [])
    if not _string_list(disabled) or not _string_list(denied):
        raise ValueError(f"Qwen settings have invalid deny lists: {path}")
    web_search = tools.setdefault("webSearch", {})
    if not isinstance(web_search, dict):
        raise ValueError(f"Qwen settings have invalid webSearch: {path}")
    web_search["enabled"] = False
    for name in sorted(_QWEN_TOOLS):
        if name not in disabled:
            disabled.append(name)
        if name not in denied:
            denied.append(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path = _qwen_settings_path(worktree)
    descriptor, temporary = tempfile.mkstemp(prefix=".network-settings-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(settings, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
        _qwen_settings_path(worktree)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _qwen_controls_verified(worktree: Path) -> bool:
    try:
        settings = json.loads(_qwen_settings_path(worktree).read_text(encoding="utf-8"))
        if not isinstance(settings, dict):
            return False
        tools = settings["tools"]
        permissions = settings["permissions"]
        if not isinstance(tools, dict) or not isinstance(permissions, dict):
            return False
        web_search = tools["webSearch"]
        if not isinstance(web_search, dict):
            return False
        disabled = tools["disabled"]
        denied = permissions["deny"]
        return (
            web_search["enabled"] is False
            and _string_list(disabled)
            and _string_list(denied)
            and _QWEN_TOOLS <= set(disabled)
            and _QWEN_TOOLS <= set(denied)
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False


def _codex_launch_argv(argv: list[str]) -> bool:
    if len(argv) >= 2 and Path(argv[0]).name.casefold() in {"codex", "codex.exe"}:
        return argv[1] == "exec"
    return (
        len(argv) >= 3
        and Path(argv[0]).name.casefold() in {"ollama", "ollama.exe"}
        and argv[1:3] == ["launch", "codex"]
    )


def resolve_launch(
    provider_id: str,
    worktree: Path,
    argv: list[str],
    requested_profile: str | None,
) -> tuple[list[str], dict[str, Any] | None]:
    """Inspect the actual worktree settings and return argv plus truthful facts.

    ``None`` is the legacy path. The stronger profiles are deliberately capped
    at soft until an independent egress and service-boundary proof is joined.
    """
    if requested_profile is None:
        return argv, None
    if not isinstance(requested_profile, str) or requested_profile not in _PROFILES:
        raise ValueError(f"unsupported requested network profile: {requested_profile}")
    launched = list(argv)
    sources: list[str] = []
    suppressed: list[str] = []
    if provider_id == "codex" and _codex_launch_argv(argv):
        # Codex 0.156.1 takes top-level web_search as an exec config override.
        # The prompt marker must remain last, including through ollama launch.
        launched[-1:-1] = ["-c", 'web_search="disabled"']
        sources = ["codex argv: web_search=disabled"]
        suppressed = ["web_search"]
    elif provider_id == "claude-code" and argv and argv[0] == "claude":
        launched.extend(["--disallowedTools", "WebSearch", "WebFetch"])
        sources = ["claude argv: --disallowedTools WebSearch WebFetch"]
        suppressed = ["WebSearch", "WebFetch"]
    elif (provider_id == "qwen-code" and _qwen_controls_verified(worktree)
          and _qwen_cli_supports_deny(argv, worktree)):
        # Project settings can be ignored in an untrusted folder, and
        # ENABLE_WEB_SEARCH can override webSearch.enabled. The CLI's whole-
        # tool deny is merged after settings load and blocks registration.
        launched.extend(["--exclude-tools", "web_search,web_fetch"])
        sources = ["qwen argv: --exclude-tools web_search,web_fetch"]
        suppressed = ["web_search", "web_fetch"]
    verified = bool(sources)
    if not verified:
        reason = "provider-native controls are unsupported, missing, or changed at launch"
    elif requested_profile == "atlas_memory_only":
        reason = "independent unrelated-destination blocking has not been measured"
    elif requested_profile == "restricted_local":
        reason = "Atlas task-path service-boundary proof is pending"
    else:
        reason = "supported provider-native web tools are suppressed; shell egress remains possible"
    return launched, {
        "requested_profile": requested_profile,
        "effective_profile": "soft_guardrail_network" if verified else "uncontrolled_network",
        "reason": reason,
        "enforcement_sources": sources,
        "suppressed_native_tools": suppressed,
        "shell_egress_possible": True,
        "hardened_sandbox": False,
        "atlas_task_path_reenabled_by_network_controls": False,
        "uncontrolled_surfaces": [
            "shell commands can still reach the network",
            "user-configured MCP, plugin, extension, and other provider tools are not covered",
        ],
    }
