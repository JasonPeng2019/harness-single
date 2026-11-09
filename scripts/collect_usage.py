"""Provider-aware ROOT capture and usage accounting.

``run`` and ``resume`` delegate to the protected provider capture lifecycle.
Reports and transcript summaries read provider identity from ROOT launch and
protected worker evidence; reporting never modifies native session history.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

from token_ledger import LedgerError, build_snapshot, extract_usage, finalize


def main() -> int:
    argv = sys.argv[1:]
    if argv and argv[0] in ("run", "resume"):
        # Preserve the mature process-boundary/watchdog implementation and its
        # established Codex options while exposing a provider-aware entrypoint.
        collector = Path(__file__).with_name("collect_codex_usage.py")
        return subprocess.call([sys.executable, "-B", str(collector), *argv])
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("snapshot", "gate"):
        command = commands.add_parser(name)
        command.add_argument("--task-file", required=True)
    archive = commands.add_parser("finalize")
    archive.add_argument("--task-file", required=True)
    archive.add_argument("--output", required=True)
    single = commands.add_parser("summarize")
    single.add_argument("--events", required=True)
    single.add_argument("--provider", choices=("codex", "claude-code", "ollama"), required=True)
    single.add_argument("--in-flight", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "summarize":
            result = extract_usage(Path(args.events).resolve(), provider_id=args.provider,
                                  completed=not args.in_flight)
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0 if result["status"] == "ok" else 2
        if args.command == "finalize":
            result = finalize(Path(args.task_file).resolve(), Path(args.output).resolve())
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        report = build_snapshot(Path(args.task_file).resolve())
        if args.command == "snapshot":
            print(json.dumps(report, indent=2, sort_keys=True))
            return 2 if report["errors"] else 0
        summary = {key: report[key] for key in (
            "run_id", "budget_tokens", "known_total_tokens", "aggregate_components",
            "provider_metric_totals", "aggregate_metric_scope",
            "remaining_tokens", "overshoot_tokens", "in_flight_invocations",
            "unknown_usage_invocations", "token_total_is_exact", "errors", "gate_allows_new_work")}
        print(json.dumps(summary, sort_keys=True))
        if report["errors"]:
            return 2
        return 0 if report["gate_allows_new_work"] else 3
    except (LedgerError, FileExistsError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
