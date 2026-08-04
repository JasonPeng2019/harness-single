from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import firmware_acceptance.kit as kit
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, SignatureVerifier, canonical_bound_operation, canonical_sha256, raw_result_sha256


class _Verifier(SignatureVerifier):
    def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
        return bool(payload) and signature == "sig" and public_key == "key"


class FirmwareAcceptanceHostTests(unittest.TestCase):
    def _broker(self, root: Path) -> AcceptanceBroker:
        seed = root / "seed"
        if not seed.exists():
            shutil.copytree("firmware_acceptance/seed", seed)
            manifest = json.loads((seed / "TARGET_SEED_MANIFEST.json").read_text(encoding="utf-8"))
            for item in manifest["files"]:
                item["sha256"] = hashlib.sha256((seed / item["path"]).read_bytes()).hexdigest()
            (seed / "TARGET_SEED_MANIFEST.json").write_text(json.dumps(manifest), encoding="utf-8")
        return AcceptanceBroker(
            root / "broker",
            seed,
            Path("firmware_acceptance/MCP_METHOD_POLICY.json"),
            Path("firmware_acceptance/LANE_TEMPLATES.json"),
        )

    def _complete_chain(self, broker: AcceptanceBroker) -> list[tuple[Path, str]]:
        bound = {
            "resource":"STM-A","server_commit": kit._PINNED_SERVER_COMMIT, "method": "reset_and_halt", "method_version": 1,
            "arguments": {"board_id": "STM-A"}, "policy_sha256": "p", "schema_sha256": "s",
            "plan_sha256": "pl", "permission_sha256": "pe", "authorization_sha256": "a",
            "claim_sha256": "c", "call_id": "host-chain", "attempt_id": "attempt-host",
            "lane_id": "STM-A", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG",
            "profile": "stm", "route": "rediscover", "governing_hashes": {"goal": "g"},
            "c1_reference": {"path": "c1", "sha256": "h"}, "deadline_monotonic": 100,
            "expires_monotonic": 99, "seed_identity": {"manifest": "x"}, "target_identity": {"commit": "y"},
            "raw_result_sha256": raw_result_sha256({"mcp": "ok"}), "cleanup_owner": "C3-HARNESS",
        }
        bound |= {"authorization_path":"authorization","claim":{"resource":"STM-A","path":"claim","sha256":"c","owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}},"controller_owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"},"governing_documents":{"goal":{"path":"goal","sha256":"g"}},"delegated_reference":{"path":"delegated","sha256":"d"},"board_identity":{"path":"board","sha256":"b"},"mcp_schema":{"path":"schema","sha256":"s"},"policy":{"path":"policy","sha256":"p"},"plan":{"path":"plan","sha256":"pl"},"permission":{"path":"permission","sha256":"pe"},"seed_identity":{"path":"seed","sha256":"x"},"target_identity":{"path":"target","sha256":"y"},"topology_key_release":{"path":"release","sha256":"r"},"cleanup_owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}}
        common = {
            "attempt_id": "attempt-host", "lane_id": "STM-A", "board": "STM-A", "probe_uid": "uid",
            "target": "STM32L476RG", "profile": "stm", "route": "rediscover", "governing_hashes": {"goal": "g"},
            "c1_reference": {"path": "c1", "sha256": "h"}, "identity": {"controller": "pid:1"},
            "bound_operation": bound, "bound_operation_sha256": canonical_sha256(canonical_bound_operation(bound)),
        }
        stages: list[tuple[Path, str]] = []
        for stage in ("proposal", "policy-evaluation", "signed-decision", "authorization", "dispatch-admission", "dispatch", "raw-result", "returning-state-cleanup", "result"):
            extra = {"signature": "sig", "public_key": "key"} if stage == "signed-decision" else {}
            extra |= {"expires_monotonic": 99} if stage == "authorization" else {}
            extra |= {"deadline_monotonic": 100} if stage == "dispatch-admission" else {}
            extra |= {"raw_result": {"mcp": "ok"}, "outcome": "PASS"} if stage == "raw-result" else {}
            extra |= {"exact_reaped": True} if stage == "returning-state-cleanup" else {}
            stages.append(broker.record(stage, "host-chain", {**common, **extra}, (str(stages[-1][0]), stages[-1][1]) if stages else None))
        return stages

    def test_S2_A1_001_clean_pinned_server_launch_is_confined_to_controller_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            server = root / "clean-server"
            server.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=server, check=True)
            (server / "server.txt").write_text("pinned", encoding="utf-8")
            subprocess.run(["git", "add", "server.txt"], cwd=server, check=True)
            subprocess.run(["git", "-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "pin"], cwd=server, check=True)
            commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=server, check=True, capture_output=True, text=True).stdout.strip()
            with patch.object(kit, "_PINNED_SERVER_ROOT", server), patch.object(kit, "_PINNED_SERVER_COMMIT", commit):
                config = self._broker(root).controller_config("STM-A", {})
                self.assertEqual(["uv", "run", "--project", str(server.resolve()), "--locked", "pyocd-debug-mcp"], config["mcp_command"])
                self.assertEqual("controller-only-mcp-framing", config["stdio"]["stdout"])
                self.assertTrue(config["stdio"]["stderr_path"].endswith("lanes\\STM-A\\logs\\mcp.stderr.log"))
                self.assertEqual(str((root / "broker" / "lanes" / "STM-A" / "artifacts").resolve()), config["environment"]["BYO_MCP_ARTIFACT_ROOT"])
                self.assertEqual({"MCP_ENDPOINT": "", "MCP_COMMAND": "", "PYOCD_PROBE_UID": "", "PYOCD_TARGET": "", "BYO_MCP_ARTIFACT_ROOT": "", "MCP_CREDENTIAL": "", "MCP_TOKEN": "", "FIRMWARE_ACCEPTANCE_ROLE": "target-worker"}, config["worker_environment"])
                (server / "dirty.txt").write_text("reject", encoding="utf-8")
                with self.assertRaises(AdmissionError):
                    self._broker(root).controller_config("STM-A", {})

    def test_S2_A1_002_admission_rejects_immutable_operation_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            broker = self._broker(Path(temporary))
            stages = self._complete_chain(broker)
            revised: list[tuple[Path, str]] = stages[:5]
            for index, (path, _) in enumerate(stages[5:], start=5):
                payload = json.loads(path.read_text(encoding="utf-8"))
                if index == 5:
                    payload["bound_operation"]["method"] = "reset_and_run"
                    payload["bound_operation_sha256"] = canonical_sha256(canonical_bound_operation(payload["bound_operation"]))
                else:
                    payload["previous_sha256"] = revised[-1][1]
                raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
                path.write_bytes(raw)
                revised.append((path, hashlib.sha256(raw).hexdigest()))
            with patch.object(kit, "validate_pinned_server", return_value=kit._PINNED_SERVER_COMMIT):
                with self.assertRaises(AdmissionError):
                    broker.admit("host-chain", revised, 50, _Verifier())


if __name__ == "__main__":
    unittest.main()
