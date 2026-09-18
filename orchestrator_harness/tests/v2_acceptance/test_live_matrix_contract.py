from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import PLATFORM_CLAIMS
from examples import v2_live_matrix as matrix
from examples.v2_live_matrix import AUTHORIZATION_ENV, CHECKS, MANAGED_ONLY, NATIVE_GAPS, execute, expected_cells


ROOT = Path(__file__).resolve().parents[3]
MATRIX = ROOT / "examples" / "v2_live_matrix.py"
ENTRYPOINT = ROOT / ".agent-workspace" / "execute-matrix.py"


class LiveMatrixContractTests(unittest.TestCase):

    def _entrypoint(self, manifests, checkpoints, *, checks=(), total_budget=3):
        command = [sys.executable, str(ENTRYPOINT)]
        for manifest in manifests:
            command.extend(("--manifest", str(manifest)))
        for checkpoint in checkpoints:
            command.extend(("--checkpoint", str(checkpoint)))
        for check in checks:
            command.extend(("--check", check))
        command.extend(("--total-budget-seconds", str(total_budget)))
        return subprocess.run(
            command, cwd=ROOT, text=True, capture_output=True,
            env={**os.environ, AUTHORIZATION_ENV: "M09"},
        )

    @staticmethod
    def _outer_manifest(root, provider, attempts, *, total_budget=3):
        return {
            "schema": "harness-v2-live-matrix/v2",
            "candidate": "fake-candidate",
            "native_runner_identity": {"platform": "Windows", "provider": provider, "profile": "managed", "candidate": "fake-candidate"},
            "coverage_cells": expected_cells(),
            "maximum_qualified_coordinate_processes": 2,
            "total_budget_seconds": total_budget,
            "attempts": attempts,
        }

    @staticmethod
    def _outer_attempt(root, name, provider, command, cleanup, *, depends_on=(), resource=None, budget=2):
        evidence = {kind: str(root / provider / name / f"{kind}.txt") for kind in ("transcript", "hook", "state", "cleanup")}
        return {
            "name": name, "coordinate_key": f"Windows/{provider}/managed/{name}",
            "target": f"fake-{provider}-{name}", "provider": provider, "profile": "managed", "platform": "Windows",
            "command": command, "cleanup_command": cleanup, "evidence": evidence,
            "evidence_oracles": {"transcript": [f"{name}-transcript", "provider-session"], "hook": [f"{name}-hook", "manager-notify"], "state": [f"{name}-state", "controller-status"], "cleanup": [f"{name}-cleanup", "process-absent"]},
            "agent_expectations": [{"classification": "Observed"}], "depends_on": list(depends_on),
            "exclusive_resources": [resource] if resource else [], "budget_seconds": budget,
        }

    @staticmethod
    def _fake_cell_script(path):
        path.write_text(
            "import argparse, pathlib, subprocess, sys, time\n"
            "p=argparse.ArgumentParser(); p.add_argument('--root'); p.add_argument('--name'); p.add_argument('--sleep',type=float,default=0); p.add_argument('--log'); p.add_argument('--spawn-child',action='store_true'); p.add_argument('--signal'); p.add_argument('--wait-for'); p.add_argument('--wait-seconds',type=float,default=5); a=p.parse_args()\n"
            "root=pathlib.Path(a.root); root.mkdir(parents=True,exist_ok=True)\n"
            "if a.log: pathlib.Path(a.log).open('a',encoding='utf8').write(f'start {a.name} {time.monotonic()}\\n')\n"
            "if a.wait_for:\n deadline=time.monotonic()+a.wait_seconds\n while not pathlib.Path(a.wait_for).exists() and time.monotonic()<deadline: time.sleep(.02)\n if not pathlib.Path(a.wait_for).exists(): sys.exit(23)\n"
            "if a.spawn_child: (root/'child.pid').write_text(str(subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']).pid))\n"
            "time.sleep(a.sleep)\n"
            "if a.signal: pathlib.Path(a.signal).write_text(a.name,encoding='utf8')\n"
            "for kind, tokens in {'transcript':[f'{a.name}-transcript','provider-session'],'hook':[f'{a.name}-hook','manager-notify'],'state':[f'{a.name}-state','controller-status'],'cleanup':[f'{a.name}-cleanup','process-absent']}.items(): (root/f'{kind}.txt').write_text(' '.join(tokens),encoding='utf8')\n"
            "if a.log: pathlib.Path(a.log).open('a',encoding='utf8').write(f'end {a.name} {time.monotonic()}\\n')\n",
            encoding="utf-8",
        )

    def test_outer_entrypoint_refills_capacity_and_persists_terminal_pool(self) -> None:
        """A causal B/C handshake proves completion-driven refill, not executor waves."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cell, log = root / "cell.py", root / "starts.log"
            self._fake_cell_script(cell)
            c_started = root / "c-started"
            def command(provider, name, delay, *extra):
                return [sys.executable, str(cell), "--root", str(root / provider / name), "--name", name, "--sleep", str(delay), "--log", str(log), *extra]
            cleanup = [sys.executable, "-c", "pass"]
            attempts = [
                self._outer_attempt(root, "CHECK-LIVE-1", "codex", command("codex", "CHECK-LIVE-1", .06), cleanup),
                self._outer_attempt(root, "CHECK-LIVE-2", "qwen-code", command("qwen-code", "CHECK-LIVE-2", 0, "--wait-for", str(c_started), "--wait-seconds", "5"), cleanup, budget=6),
                self._outer_attempt(root, "CHECK-LIVE-3", "codex", command("codex", "CHECK-LIVE-3", 0, "--signal", str(c_started)), cleanup),
            ]
            manifest, checkpoint = root / "manifest.json", root / "checkpoint.json"
            manifest.write_text(json.dumps(self._outer_manifest(root, "codex", attempts, total_budget=10)), encoding="utf-8")
            completed = self._entrypoint([manifest], [checkpoint], total_budget=10)
            self.assertEqual(0, completed.returncode, completed.stderr)
            rows = json.loads(completed.stdout)["checks"]
            self.assertTrue(all(row["outcome"] == "PASS" for row in rows))
            persisted = json.loads(checkpoint.read_text(encoding="utf-8"))["results"]
            self.assertEqual(3, len(persisted))
            events = [(line.split()[0], line.split()[1]) for line in log.read_text(encoding="utf-8").splitlines()]
            self.assertTrue(c_started.is_file(), "C must signal actual start before B may finish")
            self.assertLess(events.index(("end", "CHECK-LIVE-1")), events.index(("start", "CHECK-LIVE-3")), "same provider home excludes C until A exits")
            self.assertLess(events.index(("start", "CHECK-LIVE-3")), events.index(("end", "CHECK-LIVE-2")), "B completion is causally gated on C start")

    def test_outer_entrypoint_runs_check16_after_failed_terminal_evidence(self) -> None:
        """A failed prerequisite remains retained evidence; it is not a reason to skip CHECK16."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cell, marker = root / "cell.py", root / "check16-ran.txt"
            self._fake_cell_script(cell)
            def good(name):
                return [sys.executable, str(cell), "--root", str(root / "codex" / name), "--name", name]
            bad = [str(root / "missing-command")]
            mark = [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')"]
            cleanup = [sys.executable, "-c", "pass"]
            attempts = [
                self._outer_attempt(root, "CHECK-LIVE-8", "codex", bad, cleanup),
                self._outer_attempt(root, "CHECK-LIVE-11", "codex", good("CHECK-LIVE-11"), [str(root / "missing-cleanup")]),
                self._outer_attempt(root, "CHECK-LIVE-16", "codex", mark, cleanup, depends_on=("CHECK-LIVE-8", "CHECK-LIVE-11")),
            ]
            # CHECK16's command independently writes its required evidence before succeeding.
            attempts[2]["command"] = [sys.executable, "-c", (
                f"from pathlib import Path; r=Path({str(root / 'codex' / 'CHECK-LIVE-16')!r}); r.mkdir(parents=True,exist_ok=True); "
                f"Path({str(marker)!r}).write_text('ran'); "
                "[(r/f'{k}.txt').write_text(v) for k,v in {'transcript':'CHECK-LIVE-16-transcript provider-session','hook':'CHECK-LIVE-16-hook manager-notify','state':'CHECK-LIVE-16-state controller-status','cleanup':'CHECK-LIVE-16-cleanup process-absent'}.items()]"
            )]
            manifest, checkpoint = root / "manifest.json", root / "checkpoint.json"
            manifest.write_text(json.dumps(self._outer_manifest(root, "codex", attempts)), encoding="utf-8")
            completed = self._entrypoint([manifest], [checkpoint])
            self.assertEqual(1, completed.returncode, completed.stderr)
            rows = {row["name"]: row for row in json.loads(completed.stdout)["checks"]}
            self.assertTrue(marker.is_file(), "CHECK16 command must run after terminal failed upstream evidence")
            self.assertEqual("FAIL", rows["CHECK-LIVE-8"]["outcome"])
            self.assertEqual("FAIL", rows["CHECK-LIVE-11"]["outcome"], "cleanup launch failure is terminal evidence, not an outer abort")
            self.assertEqual("DEPENDENCY_EVIDENCE_FAILED", rows["CHECK-LIVE-16"]["acceptance_outcome"])

    def test_outer_entrypoint_total_budget_times_out_owned_tree_without_touching_sentinel(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cell, sentinel = root / "cell.py", root / "sentinel.txt"
            self._fake_cell_script(cell)
            sentinel.write_text("untouched", encoding="utf-8")
            command = [sys.executable, str(cell), "--root", str(root / "codex" / "CHECK-LIVE-1"), "--name", "CHECK-LIVE-1", "--sleep", "2", "--spawn-child"]
            attempt = self._outer_attempt(root, "CHECK-LIVE-1", "codex", command, [sys.executable, "-c", "pass"], budget=2)
            manifest, checkpoint = root / "manifest.json", root / "checkpoint.json"
            manifest.write_text(json.dumps(self._outer_manifest(root, "codex", [attempt], total_budget=.3)), encoding="utf-8")
            began = time.monotonic(); completed = self._entrypoint([manifest], [checkpoint], total_budget=.3); elapsed = time.monotonic() - began
            self.assertEqual(1, completed.returncode, completed.stderr)
            self.assertLess(elapsed, 1.5, "outer deadline must bound the selected process tree")
            self.assertEqual("untouched", sentinel.read_text(encoding="utf-8"))
            child = int((root / "codex" / "CHECK-LIVE-1" / "child.pid").read_text(encoding="utf-8"))
            child_status = subprocess.run(["powershell", "-NoProfile", "-Command", f"Get-Process -Id {child} -ErrorAction SilentlyContinue"], text=True, capture_output=True)
            self.assertFalse(child_status.stdout.strip(), "only the owned command tree is stopped; the unrelated sentinel remains untouched")
            row = json.loads(completed.stdout)["checks"][0]
            self.assertIn(row["outcome"], {"FAIL", "BUDGET_EXHAUSTED"})

    def test_timeout_records_exact_owned_boundary_and_preserves_live_unrelated_process(self) -> None:
        """A real child/grandchild tree is bounded without touching a live sentinel process."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tree = root / "tree.py"
            tree.write_text(
                "from pathlib import Path\nimport subprocess,sys,time\nr=Path(sys.argv[1]); r.mkdir(parents=True,exist_ok=True)\n"
                "child=subprocess.Popen([sys.executable,'-c',\"from pathlib import Path; import subprocess,sys,time; r=Path(sys.argv[1]); grand=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); (r/'grand.pid').write_text(str(grand.pid)); time.sleep(30)\",str(r)])\n"
                "(r/'child.pid').write_text(str(child.pid)); time.sleep(30)\n",
                encoding="utf-8",
            )
            sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
            try:
                command_root = root / "codex" / "CHECK-LIVE-1"
                attempt = self._outer_attempt(root, "CHECK-LIVE-1", "codex", [sys.executable, str(tree), str(command_root)], [sys.executable, "-c", "pass"], budget=2)
                manifest, checkpoint = root / "manifest.json", root / "checkpoint.json"
                manifest.write_text(json.dumps(self._outer_manifest(root, "codex", [attempt], total_budget=.4)), encoding="utf-8")
                completed = self._entrypoint([manifest], [checkpoint], total_budget=.4)
                self.assertEqual(1, completed.returncode, completed.stderr)
                row = json.loads(completed.stdout)["checks"][0]
                self.assertIn("owned_boundary", row["command"])
                self.assertTrue(row["command"]["owned_boundary"]["processes"], "root/children/grandchildren require recorded creation identities")
                recorded = {
                    item["pid"]: item["creation_time"]
                    for item in row["command"]["owned_boundary"]["processes"]
                }
                for pid_path in (command_root / "child.pid", command_root / "grand.pid"):
                    pid = int(pid_path.read_text(encoding="utf-8"))
                    self.assertTrue(recorded.get(pid), f"{pid_path.name} must carry its own creation identity")
                    gone = subprocess.run(["powershell", "-NoProfile", "-Command", f"Get-Process -Id {pid} -ErrorAction SilentlyContinue"], text=True, capture_output=True)
                    self.assertFalse(gone.stdout.strip(), f"owned {pid_path.name} must be absent")
                self.assertIsNone(sentinel.poll(), "live unrelated sentinel process must remain alive")
            finally:
                sentinel.terminate()
                sentinel.wait(timeout=5)

    def test_cleanup_failure_is_finite_nonpass_and_does_not_abort_independent_coordinate(self) -> None:
        """Deterministic boundary-cleanup failure is terminal evidence, not a hung pool."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cell = root / "cell.py"; self._fake_cell_script(cell)
            def command(name):
                return [sys.executable, str(cell), "--root", str(root / "qwen-code" / name), "--name", name]
            bad = self._outer_attempt(root, "CHECK-LIVE-1", "codex", [sys.executable, "-c", "import time; time.sleep(2)"], [sys.executable, "-c", "pass"], budget=1)
            good = self._outer_attempt(root, "CHECK-LIVE-2", "qwen-code", command("CHECK-LIVE-2"), [sys.executable, "-c", "pass"], budget=1)
            manifest, checkpoint = root / "manifest.json", root / "checkpoint.json"
            manifest.write_text(json.dumps(self._outer_manifest(root, "codex", [bad, good], total_budget=1.2)), encoding="utf-8")
            began = time.monotonic(); completed = self._entrypoint([manifest], [checkpoint], total_budget=1.2); elapsed = time.monotonic() - began
            self.assertLess(elapsed, 2.5)
            rows = {row["name"]: row for row in json.loads(completed.stdout)["checks"]}
            self.assertEqual("FAIL", rows["CHECK-LIVE-1"]["outcome"])
            self.assertEqual("PASS", rows["CHECK-LIVE-2"]["outcome"])
        child = Mock(pid=9137)
        child.wait.side_effect = subprocess.TimeoutExpired(["fake"], .01)
        boundary = Mock(root_creation_time="created-9137")
        boundary.observe.return_value = True
        boundary.cleanup.return_value = False
        boundary.record.return_value = {"root": {"pid": 9137, "creation_time": "created-9137"}, "processes": [{"pid": 9137, "creation_time": "created-9137"}]}
        with (
            patch.object(matrix.processes, "spawn_provider", return_value=child),
            patch.object(matrix.processes, "process_identity", return_value={"pid": 9137, "creation_time": "created-9137"}),
            patch.object(matrix.processes, "ProcessBoundary", return_value=boundary),
        ):
            began = time.monotonic(); failed = matrix._run(["fake"], timeout_seconds=.03); elapsed = time.monotonic() - began
        self.assertLess(elapsed, .5, "deterministic cleanup refusal cannot make pipe/wait collection unbounded")
        self.assertEqual(125, failed["returncode"])
        self.assertEqual("CLEANUP_UNRESOLVED", failed["terminal"])

    def test_outer_checkpoint_schema_gate_and_stale_unselected_quarantine(self) -> None:
        """Only validated checkpoint rows can reuse; stale unselected evidence is retained, never credited."""
        with tempfile.TemporaryDirectory() as temporary:
            root, marker = Path(temporary), Path(temporary) / "calls.txt"
            cell = root / "cell.py"; self._fake_cell_script(cell)
            def command(name):
                return [sys.executable, "-c", (
                    f"from pathlib import Path; m=Path({str(marker)!r}); m.write_text(str(int(m.read_text())+1) if m.exists() else '1'); "
                    f"r=Path({str(root / 'codex' / name)!r}); r.mkdir(parents=True,exist_ok=True); "
                    f"[(r/f'{{k}}.txt').write_text(v) for k,v in {{'transcript':'{name}-transcript provider-session','hook':'{name}-hook manager-notify','state':'{name}-state controller-status','cleanup':'{name}-cleanup process-absent'}}.items()]"
                )]
            cleanup = [sys.executable, "-c", "pass"]
            first = self._outer_attempt(root, "CHECK-LIVE-1", "codex", command("CHECK-LIVE-1"), cleanup)
            unselected = self._outer_attempt(root, "CHECK-LIVE-2", "codex", command("CHECK-LIVE-2"), cleanup)
            manifest, checkpoint = root / "manifest.json", root / "checkpoint.json"
            document = self._outer_manifest(root, "codex", [first, unselected]); manifest.write_text(json.dumps(document), encoding="utf-8")
            digests = matrix._coordinates(document)[1]
            runner = document["native_runner_identity"]
            wrong_schema = {"schema": "wrong/v1", "results": [{"name": "CHECK-LIVE-1", "coordinate_key": "Windows/codex/managed/CHECK-LIVE-1", "outcome": "PASS", "input_digest": digests["CHECK-LIVE-1"], "candidate": "fake-candidate", "native_runner_identity": runner}]}
            checkpoint.write_text(json.dumps(wrong_schema), encoding="utf-8")
            first_run = self._entrypoint([manifest], [checkpoint], checks=("CHECK-LIVE-1",))
            self.assertEqual(0, first_run.returncode, first_run.stderr)
            self.assertEqual("1", marker.read_text(encoding="utf-8"), "wrong-schema row must not be reused")
            stale = {"name": "CHECK-LIVE-2", "coordinate_key": "Windows/codex/managed/CHECK-LIVE-2", "outcome": "PASS", "input_digest": "obsolete", "candidate": "fake-candidate", "native_runner_identity": runner}
            current = json.loads(checkpoint.read_text(encoding="utf-8")); current["results"].append(stale); checkpoint.write_text(json.dumps(current), encoding="utf-8")
            resumed = self._entrypoint([manifest], [checkpoint], checks=("CHECK-LIVE-1",))
            self.assertEqual(0, resumed.returncode, resumed.stderr)
            self.assertEqual("1", marker.read_text(encoding="utf-8"), "unchanged validated selected row remains reusable")
            rows = {row["name"]: row for row in json.loads(checkpoint.read_text(encoding="utf-8"))["results"]}
            self.assertEqual("STALE_INPUT", rows["CHECK-LIVE-2"]["reuse_state"])
            self.assertFalse(rows["CHECK-LIVE-2"]["reusable"])

    def test_builder_validator_entrypoint_restart_and_consumed_file_invalidation(self) -> None:
        """The real producer, validator and outer entrypoint share one durable contract."""
        builder = ROOT / ".agent-workspace" / "live-matrix" / "driver" / "build_manifests.py"
        validator = ROOT / ".agent-workspace" / "live-matrix" / "driver" / "validate_manifests.py"
        candidate = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=True).stdout.strip()
        with tempfile.TemporaryDirectory() as temporary:
            root, epoch, retained, matrix_root = Path(temporary), Path(temporary) / "epoch", Path(temporary) / "retained", Path(temporary) / "matrix"
            (matrix_root / "driver").mkdir(parents=True)
            run_cell = matrix_root / "driver" / "run-cell.py"
            run_cell.write_text(
                "import argparse, time\nfrom pathlib import Path\np=argparse.ArgumentParser(); p.add_argument('--check'); p.add_argument('--evidence-root'); p.add_argument('--target'); p.add_argument('--provider'); p.add_argument('--profile'); p.add_argument('--manifest'); p.add_argument('--attempt-root'); a=p.parse_args(); r=Path(a.evidence_root); r.mkdir(parents=True,exist_ok=True)\nfor k,v in {'transcript':f'{a.check}-transcript provider-session','hook':f'{a.check}-hook manager-notify','state':f'{a.check}-state controller-status','cleanup':f'{a.check}-cleanup process-absent'}.items(): (r/f'{k}.jsonl' if k in {'transcript','hook'} else r/('state.json' if k=='state' else 'cleanup.log')).write_text(v + str(time.time_ns()))\n",
                encoding="utf-8",
            )
            (matrix_root / "driver" / "cleanup-cell.py").write_text("raise SystemExit(0)\n", encoding="utf-8")
            for provider in ("codex", "claude-code", "qwen-code"):
                for profile in ("managed", "plain"):
                    target, attempt = f"{provider}-{profile}-accepted", f"{provider}-{profile}-attempt"
                    target_root = retained / target
                    evidence = target_root / "evidence"; evidence.mkdir(parents=True)
                    review, acceptance = target_root / "COMPLETION_REVIEW.json", target_root / "ORCHESTRATOR_ACCEPTANCE.json"
                    review.write_text(json.dumps({"review_outcome": "PASS"}), encoding="utf-8")
                    acceptance.write_text(json.dumps({"approval": "ACCEPTED"}), encoding="utf-8")
                    (evidence / "RESULT.json").write_text(json.dumps({"schema": "result/v1", "outcome": "PASS", "content_hash": "fixture"}), encoding="utf-8")
                    (target_root / "TARGET-STATE.json").write_text(json.dumps({"schema": "addendum3-live-target-state/v1", "target": target, "attempt_id": attempt, "candidate": candidate, "model": "fixture", "root_runtime_state": {"state": "CLOSED"}, "review": {"code": "COMPLETION_REVIEW_OK", "evidence_paths": [str(review), str(acceptance)]}}), encoding="utf-8")
                    classifications = [{"check": check, "classification": "Observed", "credit": check == "CHECK-LIVE-7", "outcome": "PASS", "reason": "fixture"} for check in CHECKS]
                    record = {"schema": "tier4-addendum3-windows-feature-classification/v1", "candidate": candidate, "provider": provider, "profile": profile, "target": target, "attempt_id": attempt, "credited_cell_count": 1, "credited_cells": [f"Windows/{provider}/{profile}/CHECK-LIVE-7"], "classifications": classifications}
                    epoch.mkdir(parents=True, exist_ok=True)
                    (epoch / f"WINDOWS-CELL-{provider.upper()}-{profile.upper()}-NORMAL-001.json").write_text(json.dumps(record), encoding="utf-8")
            build = subprocess.run([sys.executable, str(builder), "--candidate", candidate, "--runtime-root", str(root), "--epoch-root", str(epoch), "--retained-root", str(retained), "--matrix-root", str(matrix_root)], cwd=ROOT, text=True, capture_output=True)
            self.assertEqual(0, build.returncode, build.stderr)
            valid = subprocess.run([sys.executable, str(validator), "--candidate", candidate, "--runtime-root", str(root), "--epoch-root", str(epoch), "--retained-root", str(retained), "--matrix-root", str(matrix_root)], cwd=ROOT, text=True, capture_output=True)
            self.assertEqual(0, valid.returncode, valid.stderr)
            manifests = [matrix_root / "manifests" / f"windows-{provider}-managed-normal-001.json" for provider in ("codex", "qwen-code")]
            checkpoints = [matrix_root / "checkpoints" / f"checkpoint-windows-{provider}-managed-normal-001.json" for provider in ("codex", "qwen-code")]
            first = self._entrypoint(manifests, checkpoints, checks=("CHECK-LIVE-1",), total_budget=3)
            self.assertEqual(0, first.returncode, first.stderr + first.stdout)
            before = [json.loads(path.read_text(encoding="utf-8")) for path in checkpoints]
            before_digests = [{row["name"]: row["input_digest"] for row in checkpoint["results"]} for checkpoint in before]
            second = self._entrypoint(manifests, checkpoints, checks=("CHECK-LIVE-1",), total_budget=3)
            self.assertEqual(0, second.returncode, second.stderr + second.stdout)
            self.assertEqual(before, [json.loads(path.read_text(encoding="utf-8")) for path in checkpoints], "unchanged restart must reuse every terminal row")
            run_cell.write_text(run_cell.read_text(encoding="utf-8") + "# changed consumed runner\n", encoding="utf-8")
            third = self._entrypoint(manifests, checkpoints, checks=("CHECK-LIVE-1",), total_budget=3)
            self.assertEqual(0, third.returncode, third.stderr + third.stdout)
            for checkpoint in checkpoints:
                rows = json.loads(checkpoint.read_text(encoding="utf-8"))["results"]
                self.assertEqual("PASS", next(row["outcome"] for row in rows if row["name"] == "CHECK-LIVE-7"), "accepted seed remains reusable")
            after_digests = [{row["name"]: row["input_digest"] for row in json.loads(path.read_text(encoding="utf-8"))["results"]} for path in checkpoints]
            self.assertTrue(all(after["CHECK-LIVE-1"] != before["CHECK-LIVE-1"] and after["CHECK-LIVE-7"] == before["CHECK-LIVE-7"] for before, after in zip(before_digests, after_digests)))

    @staticmethod
    def _coordinate(name, provider, *, depends_on=(), input_hashes=None):
        """A deliberately disposable coordinate; the outer runner never receives a live argv here."""
        root = Path(tempfile.gettempdir()) / "a3-live-matrix-contract"
        evidence = {kind: str(root / name / f"{kind}.txt") for kind in ("transcript", "hook", "state", "cleanup")}
        return {
            "name": name, "coordinate_id": name, "target": name, "provider": provider,
            "profile": "managed", "platform": "Windows", "command": [sys.executable, "-c", "pass"],
            "cleanup_command": [sys.executable, "-c", "pass"], "evidence": evidence,
            "evidence_oracles": {kind: ["required token"] for kind in evidence},
            "agent_expectations": [{"classification": "Observed"}], "depends_on": list(depends_on),
            "input_hashes": input_hashes or {"fixture": name}, "budget_seconds": 1,
        }

    def test_coordinate_scheduler_overlaps_only_different_provider_homes(self) -> None:
        """Two providers may overlap; a shared provider home remains exclusive."""
        manifest = {"native_runner_identity": {"platform": "Windows"}, "total_budget_seconds": 3, "attempts": [
            self._coordinate("CHECK-LIVE-1", "codex"),
            self._coordinate("CHECK-LIVE-2", "qwen-code"),
            self._coordinate("CHECK-LIVE-3", "codex"),
        ]}
        calls = []

        def fake_run(argv, *, timeout_seconds=None):
            calls.append((argv[-1], time.monotonic()))
            time.sleep(.12)
            return {"argv": argv, "returncode": 1, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}), patch.object(sys.modules[execute.__module__], "_run", side_effect=fake_run):
            started = time.monotonic()
            result = execute(manifest, Path(temporary) / "checkpoint.json")
            elapsed = time.monotonic() - started
        self.assertEqual(6, len(calls))
        self.assertLess(elapsed, .58, "different providers should overlap under the two-coordinate cap")
        self.assertGreaterEqual(calls[4][1] - calls[0][1], .20, "same-provider coordinates must not overlap")
        self.assertEqual(["CHECK-LIVE-1", "CHECK-LIVE-2", "CHECK-LIVE-3"], [row["name"] for row in result["checks"]])

    def test_dependency_failure_blocks_only_its_dependent_and_keeps_collecting(self) -> None:
        manifest = {"native_runner_identity": {"platform": "Windows"}, "total_budget_seconds": 3, "attempts": [
            self._coordinate("CHECK-LIVE-1", "codex"),
            self._coordinate("CHECK-LIVE-2", "qwen-code", depends_on=("CHECK-LIVE-1",)),
            self._coordinate("CHECK-LIVE-3", "claude-code"),
        ]}
        calls = []

        def fake_run(argv, *, timeout_seconds=None):
            calls.append(argv)
            return {"argv": argv, "returncode": 1, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}), patch.object(sys.modules[execute.__module__], "_run", side_effect=fake_run):
            result = execute(manifest, Path(temporary) / "checkpoint.json")
        rows = {row["name"]: row for row in result["checks"]}
        self.assertEqual("FAIL", rows["CHECK-LIVE-2"]["outcome"])
        self.assertEqual("DEPENDENCY_EVIDENCE_FAILED", rows["CHECK-LIVE-2"]["acceptance_outcome"])
        self.assertEqual(6, len(calls), "terminal failed evidence still permits CHECK16-style dependent collection")

    def test_resume_reuses_matching_coordinate_and_invalidates_changed_consumed_input(self) -> None:
        first = {"native_runner_identity": {"platform": "Windows"}, "total_budget_seconds": 3, "attempts": [
            self._coordinate("CHECK-LIVE-1", "codex", input_hashes={"fixture": "a"}),
            self._coordinate("CHECK-LIVE-2", "qwen-code", input_hashes={"fixture": "b"}),
        ]}
        changed = {**first, "attempts": [first["attempts"][0], {**first["attempts"][1], "input_hashes": {"fixture": "changed"}}]}
        calls = []

        def fake_run(argv, *, timeout_seconds=None):
            calls.append(argv)
            return {"argv": argv, "returncode": 1, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}), patch.object(sys.modules[execute.__module__], "_run", side_effect=fake_run):
            checkpoint = Path(temporary) / "checkpoint.json"
            execute(first, checkpoint)
            calls.clear()
            resumed = execute(changed, checkpoint)
        self.assertEqual(2, len(calls), "only the changed coordinate receives command plus cleanup")
        self.assertEqual(["CHECK-LIVE-1", "CHECK-LIVE-2"], [row["name"] for row in resumed["checks"]])

    def test_parameterized_entrypoint_collects_same_check_across_two_provider_manifests(self) -> None:
        """Use the real outer entrypoint with controlled commands, never a historical wrapper."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifests, checkpoints = [], []
            for provider in ("codex", "qwen-code"):
                evidence = {kind: str(root / provider / f"{kind}.txt") for kind in ("transcript", "hook", "state", "cleanup")}
                manifest = {
                    "schema": "harness-v2-live-matrix/v2", "native_runner_identity": {"platform": "Windows", "provider": provider}, "total_budget_seconds": 3,
                    "coverage_cells": expected_cells(), "attempts": [{
                        "name": "CHECK-LIVE-1", "coordinate_key": f"Windows/{provider}/managed/CHECK-LIVE-1",
                        "target": "controlled-fake", "provider": provider, "profile": "managed", "platform": "Windows",
                        "command": [sys.executable, "-c", "pass"], "cleanup_command": [sys.executable, "-c", "pass"],
                        "evidence": evidence, "evidence_oracles": {kind: ["required token"] for kind in evidence},
                        "agent_expectations": [{"classification": "Observed"}], "depends_on": [], "exclusive_resources": [], "budget_seconds": 1,
                    }],
                }
                manifest_path, checkpoint_path = root / f"{provider}.json", root / f"{provider}.checkpoint.json"
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                checkpoint_path.write_text(json.dumps({}), encoding="utf-8")
                manifests.append(manifest_path)
                checkpoints.append(checkpoint_path)
            command = [sys.executable, str(ENTRYPOINT), *sum((["--manifest", str(path)] for path in manifests), []), *sum((["--checkpoint", str(path)] for path in checkpoints), []), "--total-budget-seconds", "3"]
            completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, env={**os.environ, AUTHORIZATION_ENV: "M09"})
        self.assertEqual(1, completed.returncode, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual({"Windows/codex/managed/CHECK-LIVE-1", "Windows/qwen-code/managed/CHECK-LIVE-1"}, {row["coordinate_key"] for row in result["checks"]})
    """Controls prove that M08 rehearsal cannot impersonate M09 native proof."""

    def test_platform_matrix_has_three_explicit_native_only_claims(self) -> None:
        self.assertEqual(
            {"CHECK-PLATFORM-WINDOWS", "CHECK-PLATFORM-MACOS", "CHECK-PLATFORM-LINUX"},
            {item.name for item in PLATFORM_CLAIMS},
        )
        for item in PLATFORM_CLAIMS:
            self.assertTrue(all((item.scenario, item.trigger, item.expected, item.oracle, item.cleanup)))

    def test_live_checks_remain_reserved_without_m09_authorization(self) -> None:
        completed = subprocess.run([sys.executable, str(MATRIX)], cwd=ROOT, text=True, capture_output=True, check=True)
        result = json.loads(completed.stdout)
        self.assertEqual("RESERVED_FOR_M09", result["outcome"])
        self.assertEqual([f"CHECK-LIVE-{number}" for number in range(1, 17)], [item["name"] for item in result["checks"]])
        self.assertEqual(set(CHECKS), {item["name"] for item in result["checks"]})

    def test_exhaustive_matrix_keeps_native_gaps_and_profile_reasons(self) -> None:
        cells = expected_cells()
        self.assertGreater(len(cells), 100, "matrix must not retain the obsolete four-attempt ceiling")
        self.assertTrue(all(cell["applicability_reason"] for cell in cells))
        completed = subprocess.run([sys.executable, str(MATRIX)], cwd=ROOT, text=True, capture_output=True, check=True)
        rows = json.loads(completed.stdout)["cells"]
        for platform, gap in NATIVE_GAPS.items():
            with self.subTest(platform=platform):
                native_rows = [row for row in rows if row["platform"] == platform]
                self.assertTrue(native_rows)
                for row in native_rows:
                    expected = "NOT_APPLICABLE" if row["applicable"] == "false" else gap
                    self.assertEqual(expected, row["outcome"])

        for platform in ("Windows", "macOS", "Linux"):
            for command in MANAGED_ONLY:
                with self.subTest(platform=platform, command=command):
                    managed_only_plain = [
                        row for row in rows
                        if row["platform"] == platform and row["command"] == command and row["profile"] == "plain"
                    ]
                    self.assertTrue(managed_only_plain)
                    self.assertTrue(all(row["applicable"] == "false" for row in managed_only_plain))
                    self.assertTrue(all(row["outcome"] == "NOT_APPLICABLE" for row in managed_only_plain))

    def test_same_input_failed_attempt_is_terminal_without_duplicate_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            counter = root / "counter.txt"
            evidence = {kind: str(root / f"{kind}.txt") for kind in ("transcript", "hook", "state", "cleanup")}
            increment = (
                "from pathlib import Path; "
                f"counter = Path({str(counter)!r}); "
                "counter.write_text(str(int(counter.read_text()) + 1) if counter.exists() else '1')"
            )
            manifest = {
                "native_runner_identity": {"platform": "Windows"}, "total_budget_seconds": 3,
                "attempts": [{
                    "name": "CHECK-LIVE-1",
                    "target": "counter regression",
                    "provider": "codex",
                    "profile": "managed",
                    "platform": "Windows",
                    "command": [sys.executable, "-c", increment],
                    "cleanup_command": [sys.executable, "-c", "pass"],
                    "evidence": evidence,
                    "evidence_oracles": {kind: ["required token"] for kind in evidence},
                    "agent_expectations": [{"classification": "Observed"}],
                }],
            }
            checkpoint = root / "checkpoint.json"
            with patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}):
                first = execute(manifest, checkpoint)
                resumed = execute(manifest, checkpoint)

            self.assertEqual("1", counter.read_text(encoding="utf-8"))
            self.assertEqual("FAIL", first["outcome"])
            self.assertEqual("FAIL", resumed["outcome"])
            self.assertEqual(["CHECK-LIVE-1"], [row["name"] for row in resumed["checks"]])
            self.assertEqual("FAIL", resumed["checks"][0]["outcome"])
