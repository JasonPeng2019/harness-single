from __future__ import annotations

"""Cross-route lifecycle isolation for retained firmware and coding lanes."""

import hashlib
import json
import unittest
from datetime import timedelta

from orchestrator_harness.discovery import discover_suite
from orchestrator_harness.reconcile import reconcile
from orchestrator_harness.tests.support import NOW, SuiteFixture, write_json


class FirmwareLifecycleCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def observe(self, snapshot):
        return reconcile(
            discover_suite(self.fixture.config), snapshot, self.fixture.config, now=NOW
        )

    def _two_live_processes(self):
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        return type(base)(
            True,
            tuple(base.processes)
            + (
                info(201, 1, "python", "controller", NOW),
                info(202, 201, "codex", "codex", NOW),
            ),
            (),
            "fake",
        )

    def test_S1_LIFE_001_cross_route_contention_is_live_only(self) -> None:
        coding = self.fixture.status(
            run="coding",
            label="coding",
            doer="Coding",
            task="C01",
            declared_lane_id="coding:worker-1",
        )
        coding_value = json.loads(coding.read_text(encoding="utf-8"))
        coding_value.update(
            {
                "invocation_schema": "orchestrator-coding-invocation/v1",
                "resources": ["board:stm-a"],
                "exclusive_resources": ["board:stm-a"],
            }
        )
        write_json(coding, coding_value)
        firmware = self.fixture.status(
            run="firmware",
            label="firmware",
            doer="Firmware",
            task="F01",
            controller_pid=201,
            codex_pid=202,
            board_tokens=["STM-A"],
            declared_lane_id="firmware:relay-1",
        )

        live = self.observe(self._two_live_processes())
        self.assertEqual(
            [
                {
                    "resource": "board:stm-a",
                    "owners": ["coding:worker-1", "firmware:relay-1"],
                }
            ],
            live["resource_conflicts"],
        )

        for path in (coding, firmware):
            value = json.loads(path.read_text(encoding="utf-8"))
            value["state"] = "exited"
            write_json(path, value)
        exited = self.observe(
            type(self.fixture.process_snapshot())(True, (), (), "fake")
        )
        lanes = {lane["lane_id"]: lane for lane in exited["lanes"]}
        self.assertEqual([], lanes["coding:worker-1"]["resources"])
        self.assertEqual([], lanes["firmware:relay-1"]["resources"])
        self.assertTrue(lanes["coding:worker-1"]["resource_release_possible"])
        self.assertTrue(lanes["firmware:relay-1"]["resource_release_possible"])

    def test_S1_LIFE_002_relay_uses_explicit_lane_over_shared_thread_and_expires(
        self,
    ) -> None:
        coding = self.fixture.status(
            run="coding",
            label="coding",
            doer="Coding",
            task="C01",
            declared_lane_id="coding:worker-1",
        )
        coding_value = json.loads(coding.read_text(encoding="utf-8"))
        coding_value.update(
            {
                "invocation_schema": "orchestrator-coding-invocation/v1",
                "resources": ["workspace:coding"],
            }
        )
        write_json(coding, coding_value)
        firmware = self.fixture.status(
            run="firmware",
            label="firmware",
            doer="Firmware",
            task="F01",
            controller_pid=201,
            codex_pid=202,
            declared_lane_id="firmware:relay-1",
        )
        firmware_value = json.loads(firmware.read_text(encoding="utf-8"))
        firmware_value["thread_id"] = coding_value["thread_id"]
        write_json(firmware, firmware_value)

        request = {
            "schema": "suite-manager-request/v1",
            "request_id": "firmware-relay-1",
            "created_utc": NOW.isoformat().replace("+00:00", "Z"),
            "deadline_utc": (NOW + timedelta(minutes=10))
            .isoformat()
            .replace("+00:00", "Z"),
            "declared_lane_id": "firmware:relay-1",
            "session_id": coding_value["thread_id"],
            "live_lifetime": {
                "run_id": "firmware-run-1",
                "process": {
                    "pid": 201,
                    "creation_time_utc": NOW.isoformat().replace("+00:00", "Z"),
                },
            },
            "board": {
                "board_token": "STM-A",
                "board_id": "stm_a",
                "probe_uid": "probe-a",
                "vcom": "COM1",
            },
            "roots": {"project": ".agent-workspace/runtime/project"},
            "server_snapshot": {"head": "snapshot"},
            "tool": "board_setup-plan",
            "arguments": {"board_id": "stm_a"},
            "tool_argument_sha256": "a" * 64,
            "relay": {"path": ".agent-workspace/manager-relays/firmware-relay-1.json"},
        }
        request_path = (
            self.fixture.workspace("firmware")
            / "manager-requests"
            / "firmware-relay-1.json"
        )
        write_json(request_path, request)
        relay_path = (
            self.fixture.workspace("firmware")
            / "manager-relays"
            / "firmware-relay-1.json"
        )
        write_json(
            relay_path,
            {
                "schema": "suite-manager-relay/v1",
                "decision": "approved",
                "request_id": request["request_id"],
                "request_sha256": hashlib.sha256(request_path.read_bytes()).hexdigest(),
                "declared_lane_id": request["declared_lane_id"],
                "tool_argument_sha256": request["tool_argument_sha256"],
                "server_snapshot": request["server_snapshot"],
                "exact_approved_call": {
                    "tool": request["tool"],
                    "arguments": request["arguments"],
                },
                "expires_utc": (NOW + timedelta(minutes=5))
                .isoformat()
                .replace("+00:00", "Z"),
            },
        )

        bound = self.observe(self._two_live_processes())
        lanes = {lane["lane_id"]: lane for lane in bound["lanes"]}
        self.assertEqual("RELAYED", bound["requests"][0]["operational_state"])
        self.assertEqual([], lanes["coding:worker-1"]["resource_ambiguity"])
        self.assertIn("board:stm-a", bound["requests"][0]["resources"])
        self.assertNotIn("board:stm-a", lanes["coding:worker-1"]["resources"])

        relay = json.loads(relay_path.read_text(encoding="utf-8"))
        relay["expires_utc"] = (
            (NOW - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        )
        write_json(relay_path, relay)
        expired = self.observe(self._two_live_processes())
        self.assertEqual("BOUND_EXPIRED", expired["requests"][0]["relay_state"])
        self.assertFalse(expired["requests"][0]["manager_actionable"])


if __name__ == "__main__":
    unittest.main()
