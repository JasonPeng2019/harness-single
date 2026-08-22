"""Disposable, provider-free fixture for the Qwen project adapter surface.

This fixture creates a temporary project, installs the runner-owned Qwen
assets into ``.qwen/``, and reads back the canonical project settings.  It
does not launch Qwen and does not claim that a live Qwen hook session was
proved.  Use the canonical invocation example for a real caller-owned lane.

Run from the repository root:
    python examples/disposable_qwen_coding_fixture.py [--keep DIRECTORY]
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Sequence

HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(HARNESS_ROOT))

from orchestrator_harness.qwen_installer import install_qwen_adapter


class FixtureError(RuntimeError):
    pass


def run_fixture(root: Path) -> dict[str, object]:
    project = root / "project"
    project.mkdir(parents=True)
    settings = project / ".qwen" / "settings.json"
    settings.parent.mkdir()
    settings.write_text(
        json.dumps({"hooks": {"SessionStart": [{"matcher": "*", "hooks": []}]}}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    result = install_qwen_adapter(project)
    installed = json.loads(settings.read_text(encoding="utf-8"))
    hooks = installed.get("hooks")
    if not isinstance(hooks, dict) or set(("PostToolUse", "Notification", "Stop")) - set(hooks):
        raise FixtureError("Qwen project settings did not receive the owned hook groups")
    if "SessionStart" not in hooks:
        raise FixtureError("unrelated Qwen project settings were not preserved")
    return {
        "schema": "orchestrator-disposable-qwen-fixture/v1",
        "provider_id": "qwen-code",
        "project_root": str(project),
        "install_operation": result.get("operation"),
        "installed_current": result.get("current"),
        "project_settings": ".qwen/settings.json",
        "live_provider_session": False,
        "live_hook_claim": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    keep_root: Path | None = None
    if args:
        if len(args) != 2 or args[0] != "--keep":
            print("usage: disposable_qwen_coding_fixture.py [--keep DIRECTORY]", file=sys.stderr)
            return 2
        keep_root = Path(args[1]).resolve()
        if keep_root.exists():
            print("--keep DIRECTORY must not already exist", file=sys.stderr)
            return 2
        keep_root.mkdir(parents=True)
    try:
        if keep_root is not None:
            result = run_fixture(keep_root)
            result["cleanup"] = "retained by --keep"
        else:
            with tempfile.TemporaryDirectory(prefix="orchestrator-qwen-fixture-") as temporary:
                result = run_fixture(Path(temporary))
            result["cleanup"] = "temporary project removed"
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (FixtureError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"fixture failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
