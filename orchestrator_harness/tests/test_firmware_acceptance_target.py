from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
import unittest

from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, validate_campaign_contract


class FirmwareAcceptanceTargetTests(unittest.TestCase):
    """Target-materialization coverage that deliberately uses a disposable Git repository."""

    def _broker(self, root: Path) -> AcceptanceBroker:
        return AcceptanceBroker(
            root / "broker",
            Path("firmware_acceptance/seed"),
            Path("firmware_acceptance/MCP_METHOD_POLICY.json"),
            Path("firmware_acceptance/LANE_TEMPLATES.json"),
        )

    def test_five_file_seed_remains_immutable_while_target_files_can_be_committed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = self._broker(root)
            target = root / "broker" / "targets" / "attempt-0001"
            broker.materialize_seed(target)

            seed_files = {
                "TARGET_SEED_MANIFEST.json",
                "TARGET_CHARTER.md",
                "PINNED_INPUTS.json",
                "TEST_CONTRACT.json",
                "EVIDENCE_SCHEMA.json",
            }
            self.assertEqual(seed_files, {path.name for path in target.iterdir() if path.is_file()})
            self.assertTrue(all(not (target / name).stat().st_mode & 0o222 for name in seed_files))

            additions = {
                ".gitignore": "*.local\n",
                "src/main.c": "int main(void) { return 0; }\n",
                "tests/test_target.py": "def test_target():\n    assert True\n",
                "build/firmware.bin": "synthetic artifact\n",
                "config/target.json": "{}\n",
            }
            for relative, content in additions.items():
                path = target / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            subprocess.run(["git", "add", "--", "."], cwd=target, check=True, capture_output=True)
            subprocess.run(
                ["git", "-c", "user.name=Target", "-c", "user.email=target@invalid", "commit", "-q", "-m", "target files"],
                cwd=target,
                check=True,
                capture_output=True,
            )

            head = broker.validate_target(target)
            self.assertEqual(
                head,
                subprocess.run(["git", "rev-parse", "HEAD"], cwd=target, check=True, capture_output=True, text=True).stdout.strip(),
            )

    def test_closed_contract_keeps_all_eighteen_ids_and_failure_routes(self) -> None:
        seed = Path("firmware_acceptance/seed")
        validate_campaign_contract(seed)
        contract = json.loads((seed / "TEST_CONTRACT.json").read_text(encoding="utf-8"))
        definitions = contract["definitions"]
        self.assertEqual(tuple(contract["gating_ids"]), tuple(item["id"] for item in definitions))
        self.assertEqual(18, len(definitions))
        self.assertEqual(
            {"TARGET_LOCAL_REPAIR", "HARNESS_WATCHER_ABORT", "AUTHORIZED_SERVER_LIMITATION"},
            {item["failure_route"] for item in definitions},
        )
        self.assertTrue(all(item["dependency_inputs"] for item in definitions))

    def test_seed_rewrite_is_rejected_after_target_files_exist(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = self._broker(root)
            target = root / "broker" / "targets" / "attempt-0002"
            broker.materialize_seed(target)
            (target / "src").mkdir()
            (target / "src" / "main.c").write_text("int main(void) { return 0; }\n", encoding="utf-8")
            charter = target / "TARGET_CHARTER.md"
            charter.chmod(0o600)
            charter.write_text("substituted", encoding="utf-8")

            with self.assertRaises(AdmissionError):
                broker.validate_target(target)
