from __future__ import annotations
import json, subprocess, sys, tempfile, unittest
from pathlib import Path
class RetentionTests(unittest.TestCase):
 def test_retained_wake_and_quiet_evidence(self):
  root=Path(__file__).resolve().parents[2]
  with tempfile.TemporaryDirectory() as temp:
   evidence=Path(temp)/"evidence"
   result=subprocess.run([sys.executable,"harness_watcher_implementation/tests/run_attention_practical.py","--evidence-dir",str(evidence)],cwd=root,capture_output=True,text=True,timeout=30)
   self.assertEqual(0,result.returncode,result.stderr)
   wake=json.loads((evidence/"wake-result.json").read_text()); quiet=json.loads((evidence/"quiet-result.json").read_text())
   self.assertEqual("COMPLETE",wake["wake_evidence"]["status"]); wake_id=wake["wake_id"]
   self.assertEqual(["AGENT_SIGNAL_CREATED","AGENT_SIGNAL_PUBLISHED","HARNESS_SIGNAL_OBSERVED","MANAGER_WAKE_ATTEMPTED","MANAGER_WAKE_DELIVERED","MANAGER_WAKE_RECEIVED","MANAGER_EVENT_CLAIMED"],[record["kind"] for record in wake["records"]])
   self.assertEqual({wake_id},{record.get("wake_id") for record in wake["records"] if record.get("wake_id")})
   self.assertEqual(sorted(record["source_timestamp_utc"] for record in wake["records"]),[record["source_timestamp_utc"] for record in wake["records"]])
   self.assertTrue(all(record["epoch_id"] == "A00_test" and record["event_id"] == "wake" for record in wake["records"]))
   self.assertNotIn("wake_id",quiet["timeout_event"]); self.assertEqual([],quiet["wake_records"])
   raw=[]
   for path in evidence.rglob("*.jsonl"):
    if "quiet-" in str(path): raw.extend(json.loads(line) for line in path.read_text().splitlines() if line)
   self.assertFalse(any(record.get("kind","").startswith("MANAGER_WAKE_") for record in raw))
   self.assertIn("attention practical host-only check: PASS",result.stdout)
if __name__ == "__main__": unittest.main()
