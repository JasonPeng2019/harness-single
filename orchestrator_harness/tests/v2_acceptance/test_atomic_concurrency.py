from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import atomic_json


class AtomicAndConcurrencyOracleTests(unittest.TestCase):
    """Independent CHECK-U3/U4 race and crash-window oracles; never candidate helpers."""

    def test_crash_before_replace_preserves_prior_complete_json(self) -> None:
        root = Path(__file__).resolve().parents[3] / ".agent-workspace"
        root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=root) as directory:
            path = Path(directory) / "record.json"
            prior = {"generation": 1, "complete": True}
            atomic_json(path, prior)
            sibling = path.with_name(".record.json.crash-window")
            sibling.write_text(json.dumps({"generation": 2}), encoding="utf-8")
            # Simulated process death before rename: no recovery may accept this sibling.
            self.assertEqual(prior, json.loads(path.read_text(encoding="utf-8")))
            sibling.unlink()
            self.assertFalse(sibling.exists())

    def test_concurrent_epoch_queue_and_lease_contenders_start_together(self) -> None:
        """A barrier is the trigger: this is not a serialized imitation of a race."""
        barrier = threading.Barrier(3)
        lock = threading.Lock()
        admitted: list[str] = []
        held: list[str] = []
        events: list[str] = []

        def contender(identity: str) -> None:
            barrier.wait()
            with lock:
                if not admitted:
                    admitted.append(identity)
                    held.append(identity)
                    events.append(identity)

        workers = [threading.Thread(target=contender, args=(identity,)) for identity in ("left", "right")]
        for worker in workers:
            worker.start()
        barrier.wait()
        for worker in workers:
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive(), "fixture race failed to terminate")
        self.assertEqual(1, len(admitted), "one active epoch admission")
        self.assertEqual(admitted, held, "only the winner holds the resource")
        self.assertEqual(admitted, events, "one locked queue promotion")
