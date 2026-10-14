"""Offline failure evidence for WSL cleanup; these are not live Linux proof."""
from pathlib import Path
import json
from contextlib import ExitStack
from types import SimpleNamespace
import tempfile
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent / "support"))
try:
    import wsl_real_agent_driver as driver
finally:
    sys.path.pop(0)


class WslCleanupInventoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.group = Path(temporary.name) / "group"
        self.group.mkdir()
        (self.group / "cgroup.procs").write_text("", encoding="ascii")
        metadata = self.group.lstat()
        self.identity = (metadata.st_dev, metadata.st_ino)

    def test_complete_recursive_inventory_includes_descendants(self):
        (self.group / "cgroup.procs").write_text("23\n", encoding="ascii")
        child = self.group / "child"
        child.mkdir()
        (child / "cgroup.procs").write_text("41\n", encoding="ascii")
        self.assertEqual({23, 41}, driver.cgroup_processes(self.group, expected_identity=self.identity))

    def test_unreadable_or_malformed_inventory_is_unknown(self):
        with patch.object(Path, "read_text", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                driver.cgroup_processes(self.group)
        for raw in ("not-a-pid\n", "0\n", "-1\n"):
            with self.subTest(raw=raw):
                (self.group / "cgroup.procs").write_text(raw, encoding="ascii")
                with self.assertRaises(ValueError):
                    driver.cgroup_processes(self.group)

    def test_missing_child_inventory_is_not_empty(self):
        (self.group / "child").mkdir()
        with self.assertRaises(FileNotFoundError):
            driver.cgroup_processes(self.group)

    def test_empty_inventory_still_requires_owned_cgroup_kill(self):
        driver.kill_cgroup(self.group, expected_identity=self.identity)
        self.assertEqual("1\n", (self.group / "cgroup.kill").read_text(encoding="ascii"))

    def test_inventory_failure_cannot_skip_kill_or_prove_cleanup(self):
        with patch.object(driver, "cgroup_processes", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                driver.kill_cgroup(self.group, expected_identity=self.identity)
        self.assertEqual("1\n", (self.group / "cgroup.kill").read_text(encoding="ascii"))

    def test_missing_or_changed_ownership_refuses_kill(self):
        for identity in (None, (self.identity[0], self.identity[1] + 1)):
            with self.subTest(identity=identity):
                with self.assertRaises(RuntimeError):
                    driver.kill_cgroup(self.group, expected_identity=identity)
                self.assertFalse((self.group / "cgroup.kill").exists())

    def test_confirmed_absent_group_is_distinct_from_unreadable(self):
        absent = self.group / "absent"
        self.assertEqual(set(), driver.cgroup_processes(absent))
        driver.kill_cgroup(absent, expected_identity=self.identity)

    def test_preparation_cleanup_preserves_all_failures_and_owned_evidence(self):
        # Ordinary owned files model cgroup controls for offline Windows tests;
        # no namespace, credential, account or kernel cgroup is operated.
        for mode in ("complete", "inventory-denied", "namespace-delete-failed", "credential-delete-denied", "preexisting-unowned"):
            with self.subTest(mode=mode):
                base = self.group.parent / uuid4().hex
                base.mkdir()
                codex_root = base / "release"
                (codex_root / "bin").mkdir(parents=True)
                (codex_root / "bin/codex").write_text("inert fixture", encoding="utf-8")
                (codex_root / "codex-resources").mkdir()
                (codex_root / "codex-resources/bwrap").write_text("inert fixture", encoding="utf-8")
                workspace = base / "workspace"
                (workspace / ".git").mkdir(parents=True)
                temporary_root = base / "temporary"
                temporary_root.mkdir()
                mount = base / "cgroup-mount"
                mount.mkdir()
                run_id = uuid4().hex
                group = mount / ("orchestrator-harness-" + run_id)
                if mode == "preexisting-unowned":
                    group.mkdir()
                    (group / "cgroup.procs").write_text("", encoding="ascii")
                    (group / "marker").write_text("unowned", encoding="ascii")
                helper = base / "helper.py"
                helper.write_text("# inert fixture", encoding="utf-8")
                (base / "auth.json").write_text("{}", encoding="utf-8")
                release_signal = base / "release.json"
                release_signal.write_text("{}", encoding="utf-8")
                args = SimpleNamespace(run_id=run_id, nonce="a" * 64, invocation_id="fixture-invocation",
                    codex_root=str(codex_root), workspace=str(workspace), state=str(base / "state.json"),
                    release_signal=str(release_signal), evidence=str(base / "evidence"), auth_json=str(base / "auth.json"),
                    system_mounts=str(base / "mounts.json"), cgroup_launcher=str(helper), provider_entry=str(helper), wait_seconds=1)
                def auth(_source, destination):
                    destination.parent.mkdir(parents=True)
                    destination.write_text("offline-fixture-credential", encoding="utf-8")
                    return ["offline-fixture-credential"], "fixture-expiry", "fixture-expiry"
                def preflight(*_args, **_kwargs):
                    (group / "cgroup.procs").write_text("", encoding="ascii")
                    return {"fixture": True}
                real_rmdir = Path.rmdir
                def model_kernel_rmdir(path):
                    if path == group:
                        self.assertTrue(path.resolve().is_relative_to(base.resolve()))
                        for control in path.iterdir():
                            if control.is_file():
                                control.unlink()  # Model disappearance of virtual kernel controls.
                    return real_rmdir(path)
                proxy = SimpleNamespace(close=lambda: None)
                with ExitStack() as stack:
                    stack.enter_context(patch.object(driver.os, "geteuid", return_value=0, create=True))
                    stack.enter_context(patch.object(driver.os, "chown", create=True))
                    stack.enter_context(patch.object(driver, "chown_tree"))
                    stack.enter_context(patch.object(driver.tempfile, "gettempdir", return_value=str(temporary_root)))
                    stack.enter_context(patch.object(driver, "cgroup_root", return_value=mount))
                    stack.enter_context(patch.object(driver, "minimal_auth", side_effect=auth))
                    stack.enter_context(patch.object(driver, "isolation_layout", return_value={"fixture": True}))
                    stack.enter_context(patch.object(driver, "bwrap_base", return_value=[]))
                    stack.enter_context(patch.object(driver, "preflight", side_effect=preflight))
                    stack.enter_context(patch.object(driver, "make_network_namespace", return_value={
                        "name": "oh-" + run_id[:8], "host_ip": "127.0.0.1", "proxy_port": 42, "proxy": proxy}))
                    stack.enter_context(patch.object(driver, "run", return_value=SimpleNamespace(
                        returncode=1 if mode == "namespace-delete-failed" else 0)))
                    stack.enter_context(patch.object(Path, "rmdir", new=model_kernel_rmdir))
                    if mode == "inventory-denied":
                        stack.enter_context(patch.object(driver, "cgroup_processes", side_effect=PermissionError("denied")))
                    if mode == "credential-delete-denied":
                        stack.enter_context(patch.object(driver.shutil, "rmtree", side_effect=PermissionError("denied")))
                    if mode == "complete":
                        self.assertEqual(0, driver._prepare(args))
                    else:
                        with self.assertRaises((RuntimeError, FileExistsError)):
                            driver._prepare(args)
                state = json.loads(Path(args.state).read_text(encoding="utf-8"))
                self.assertEqual(mode == "complete", state["cleanup_complete"], state)
                self.assertEqual("CLEANED" if mode == "complete" else "CLEANUP_UNPROVEN", state["status"])
                self.assertEqual(run_id, state["run_id"])
                self.assertEqual(args.nonce, state["nonce"])
                evidence = json.loads((Path(args.evidence) / "prepared-state.json").read_text(encoding="utf-8"))
                self.assertEqual(state, evidence)
                if mode == "inventory-denied":
                    self.assertIsNone(state["cgroup_inventory"]["remaining_pids"])
                    self.assertFalse(state["cgroup_inventory"]["inventory_complete"])
                    self.assertEqual("1\n", (group / "cgroup.kill").read_text(encoding="ascii"))
                    self.assertTrue(Path(state["codex_home"]).is_dir(), "unknown cleanup must retain owned credential evidence")
                elif mode == "preexisting-unowned":
                    self.assertEqual("unowned", (group / "marker").read_text(encoding="ascii"))
                    self.assertFalse((group / "cgroup.kill").exists(), "failed creation cannot adopt an existing group")
                else:
                    self.assertEqual("64", state["cgroup_inventory"]["limits"]["pids.max"])
                    self.assertEqual([], state["cgroup_inventory"]["remaining_pids"])

    def test_prepared_state_refuses_changed_or_foreign_cgroup_before_launch(self):
        run_id = uuid4().hex
        group = self.group.parent / ("orchestrator-harness-" + run_id)
        group.mkdir()
        metadata = group.lstat()
        state = {"schema": driver.PREPARED_STATE_SCHEMA, "status": "READY", "consumed": False,
                 "nonce": "a" * 64, "invocation_id": "fixture", "run_id": run_id,
                 "network_namespace": "oh-" + run_id[:8], "isolation_layout": {"fixture": True},
                 "cgroup": str(group), "cgroup_identity": [metadata.st_dev, metadata.st_ino]}
        for key in ("proxy_url", "workspace", "codex_home", "release", "pinned_codex", "bwrap",
                    "cgroup_launcher", "release_signal"):
            state[key] = "explicit-fixture-input"
        with patch.object(driver, "cgroup_root", return_value=group.parent):
            self.assertEqual(state, driver.validate_prepared_state(state, nonce=state["nonce"], invocation_id="fixture"))
            for changed in ({"cgroup_identity": [metadata.st_dev, metadata.st_ino + 1]},
                            {"cgroup": str(self.group)}, {"cgroup_identity": None}):
                with self.subTest(changed=changed), self.assertRaises(RuntimeError):
                    driver.validate_prepared_state({**state, **changed}, nonce=state["nonce"], invocation_id="fixture")


if __name__ == "__main__":
    unittest.main()
