"""Provider-child entry used by the WSL route after native controller launch.

The lane controller owns the only provider ``Popen``.  This entry only
translates host paths to the already-mounted sandbox paths and replaces its
own process with the pinned Codex command, preserving the controller's exact
PID/creation identity and process boundary.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

try:
    from .wsl_real_agent_driver import bwrap_base
except ImportError:  # Direct support-script execution.
    from wsl_real_agent_driver import bwrap_base


def _sandbox_arguments(arguments: list[str]) -> list[str]:
    rewritten: list[str] = []
    index = 0
    while index < len(arguments):
        value = arguments[index]
        if value == "--cd" and index + 1 < len(arguments):
            rewritten.extend((value, "/workspace"))
            index += 2
            continue
        if value == "--output-last-message" and index + 1 < len(arguments):
            rewritten.extend((value, "/workspace/.agent-workspace/real_agent_last_message.txt"))
            index += 2
            continue
        rewritten.append(value)
        index += 1
    return rewritten


def _provider_argv(command: list[str], base: list[str]) -> list[str]:
    """Build the pinned provider argv without changing its adapter action."""

    if not command or command[0] != "exec":
        raise RuntimeError("native Codex provider adapter must supply an exec action")
    return [*base, "/opt/codex/bin/codex", *_sandbox_arguments(command)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Exec one controller-owned Codex provider child")
    parser.add_argument("--bwrap", required=True, type=Path)
    parser.add_argument("--release", required=True, type=Path)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--codex-home", required=True, type=Path)
    parser.add_argument("--proxy-url", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = list(args.command)
    if command[:1] == ["--"]:
        command = command[1:]
    base = bwrap_base(
        args.bwrap,
        args.release,
        args.workspace,
        args.codex_home,
        args.proxy_url,
    )
    os.execvp(base[0], _provider_argv(command, base))
    raise AssertionError("provider exec returned")


if __name__ == "__main__":
    raise SystemExit(main())
