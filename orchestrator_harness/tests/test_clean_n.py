from __future__ import annotations
import unittest
from datetime import timedelta
from unittest.mock import patch
from orchestrator_harness.notifications import coalesce_mutable_handoffs, admit_deferred_handoffs
from orchestrator_harness.tests.support import NOW
from orchestrator_harness.cli import observe
from orchestrator_harness.models import ProcessSnapshot
from orchestrator_harness.models import ProcessInfo
from orchestrator_harness.tests.support import SuiteFixture
from orchestrator_harness.cli import watch_managed, stop_command, EXIT_OK
import io

class CleanNTests(unittest.TestCase):
 def test_newest_checkpoint_replaces_pending_and_deferred_versions(self):
  def event(event_id, stamp): return {"event_id":event_id,"identity":"lane:b:checkpoint","type":"CHECKPOINT_UPDATED","data":{"checkpoint_sha256":event_id},"observed_utc":stamp.isoformat()}
  old=event("old",NOW); mid=event("mid",NOW+timedelta(seconds=1)); new=event("new",NOW+timedelta(seconds=2))
  pending, deferred=coalesce_mutable_handoffs(old,[{"event":mid,"priority":5,"deadline_order":"z","identity":mid["identity"],"admitted_utc":mid["observed_utc"]},{"event":new,"priority":4.75,"deadline_order":"z","identity":new["identity"],"admitted_utc":new["observed_utc"]}],observed_at=NOW)
  self.assertEqual("new",pending["event_id"]); self.assertEqual(4.75,pending["admitted_priority"]); self.assertEqual(["mid","old"],pending["data"]["superseded_event_ids"]); self.assertEqual([],deferred)
 def test_pending_coalesce_winner_preserves_existing_admitted_priority(self):
  old={"event_id":"old","identity":"lane:b:checkpoint","type":"CHECKPOINT_UPDATED","data":{},"observed_utc":NOW.isoformat()}
  pending={"event_id":"new","identity":old["identity"],"type":"CHECKPOINT_UPDATED","data":{},"observed_utc":(NOW+timedelta(seconds=1)).isoformat(),"admitted_utc":(NOW+timedelta(seconds=1)).isoformat(),"admitted_priority":4.5}
  selected,deferred=coalesce_mutable_handoffs(pending,[{"event":old,"priority":5,"deadline_order":"z","identity":old["identity"],"admitted_utc":old["observed_utc"]}],observed_at=NOW+timedelta(seconds=2))
  self.assertEqual("new",selected["event_id"]); self.assertEqual(4.5,selected["admitted_priority"]); self.assertEqual([],deferred)
 def test_admission_versions_coalesce_and_old_ack_cannot_clear_replacement(self):
  def condition(event_id, stamp): return {"event_id":event_id,"identity":"lane:x:checkpoint","type":"CHECKPOINT_UPDATED","data":{"checkpoint_sha256":event_id,"deadline_utc":None},"observed_utc":stamp.isoformat()}
  old=condition("old",NOW); new=condition("new",NOW+timedelta(seconds=1)); base={"lanes":[],"requests":[],"helpers":[],"mcps":[]}
  deferred=admit_deferred_handoffs({old["identity"]:old},base,observed_at=NOW,acknowledged_event_ids=set(),newly_observed_event_ids={"old"},existing=[])
  deferred=admit_deferred_handoffs({new["identity"]:new},base,observed_at=NOW+timedelta(seconds=1),acknowledged_event_ids=set(),newly_observed_event_ids={"new"},existing=deferred)
  pending, deferred=coalesce_mutable_handoffs(old,deferred,observed_at=NOW+timedelta(seconds=2))
  self.assertEqual("new",pending["event_id"]); self.assertNotEqual("old",pending["event_id"]); self.assertEqual([],deferred)
 def test_promoted_old_keeps_admission_so_newer_replaces_it(self):
  old={"event_id":"old","identity":"lane:x:checkpoint","type":"CHECKPOINT_UPDATED","data":{}}
  newer={"event_id":"new","identity":old["identity"],"type":"CHECKPOINT_UPDATED","data":{}}
  base={"lanes":[],"requests":[],"helpers":[],"mcps":[]}
  from orchestrator_harness.notifications import select_actionable_with_deferred
  selected,_=select_actionable_with_deferred({},base,observed_at=NOW+timedelta(seconds=10),acknowledged_event_ids=set(),newly_observed_event_ids=set(),deferred=[{"event":old,"priority":5,"deadline_order":"z","identity":old["identity"],"admitted_utc":NOW.isoformat()}])
  self.assertEqual(NOW.isoformat(),selected["admitted_utc"])
  pending,_=coalesce_mutable_handoffs(selected,[{"event":newer,"priority":5,"deadline_order":"z","identity":newer["identity"],"admitted_utc":(NOW+timedelta(seconds=2)).isoformat()}],observed_at=NOW+timedelta(seconds=11))
  self.assertEqual("new",pending["event_id"]); self.assertIn("old",pending["data"]["superseded_event_ids"])
 def test_managed_checkpoint_promotion_restart_and_exact_ack(self):
  from orchestrator_harness.cli import ack_command, stop_command
  from orchestrator_harness.stable_io import SafeOutput
  def snapshot(version):
   lane={"lane_id":"boreal","process_state":"EXITED","operational_state":"CHECKPOINTED","run_root":"run","doer":"B","task":"D","phase":"p","checkpoint_path":"checkpoint.md","checkpoint_sha256":version}
   return {"process_snapshot_complete":True,"lanes":[lane],"requests":[],"helpers":[],"mcps":[],"manager_signals":[],"resource_conflicts":[{"resource":"high"}],"observation_errors":[]}
  fixture=SuiteFixture.create(); self.addCleanup(fixture.close); owner=ProcessInfo(8000,1,"python","owner",NOW)
  def run(wid, snap):
   watcher=ProcessInfo(wid,8000,"python","watcher",NOW); ps=ProcessSnapshot(True,(watcher,owner),(),"fake")
   def stop(_): stop_command(fixture.config,clock=lambda:NOW,stream=io.StringIO())
   with patch("orchestrator_harness.cli.reconcile",return_value=snap): self.assertEqual(EXIT_OK,watch_managed(fixture.config,process_provider=lambda:ps,clock=lambda:NOW,sleeper=stop,stream=io.StringIO(),watcher=watcher,owner=owner))
  store=SafeOutput(harness_root=fixture.harness_root,output_root=fixture.config.output_dir,forbidden_roots=(fixture.suite_root,)); store.prepare()
  run(801,snapshot("old")); high=store.load_notification_state()["pending"]; self.assertEqual("RESOURCE_CONFLICT",high["type"])
  from orchestrator_harness.events import conditions_from_snapshot
  old_event=next(item for item in conditions_from_snapshot(snapshot("old")).values() if item["type"]=="CHECKPOINT_UPDATED")
  store.save_notification_state(pending=high,acknowledged_event_ids=[],deferred=[{"event":old_event,"priority":5,"deadline_order":"9999-12-31T23:59:59Z","identity":old_event["identity"],"admitted_utc":NOW.isoformat()}])
  ack_command(fixture.config,event_id=high["event_id"],clock=lambda:NOW,stream=io.StringIO())
  run(802,snapshot("old")); old_id=store.load_notification_state()["pending"]["event_id"]
  run(803,snapshot("new")); current=store.load_notification_state()["pending"]; self.assertNotEqual(old_id,current["event_id"])
  with self.assertRaises(ValueError): ack_command(fixture.config,event_id=old_id,clock=lambda:NOW,stream=io.StringIO())
  self.assertEqual(current["event_id"],store.load_notification_state()["pending"]["event_id"])
  ack_command(fixture.config,event_id=current["event_id"],clock=lambda:NOW,stream=io.StringIO())
  run(804,snapshot("new")); self.assertIsNone(store.load_notification_state()["pending"])
 def test_manager_signals_do_not_coalesce(self):
  signals=[{"event":{"event_id":x,"identity":"manager-signal:same","type":"MANAGER_SIGNAL","data":{}},"priority":3,"deadline_order":"z","identity":"manager-signal:same","admitted_utc":NOW.isoformat()} for x in ("one","two")]
  pending,deferred=coalesce_mutable_handoffs(None,signals,observed_at=NOW)
  self.assertIsNone(pending); self.assertEqual(2,len(deferred))
 def test_observe_orders_discovery_process_then_post_sample_clock(self):
  trace=[]
  class Config: pass
  def discover(config): trace.append("discover"); return []
  def provider(): trace.append("process"); return ProcessSnapshot(True,(),(),"fake")
  def clock(): trace.append("clock"); return NOW
  with patch("orchestrator_harness.cli.discover_suite",side_effect=discover), patch("orchestrator_harness.cli.reconcile",return_value={"lanes":[]}):
   observe(Config(),process_provider=provider,clock=clock)
  self.assertEqual(["discover","process","clock"],trace)
 def test_slow_sample_live_identity_is_not_stale_in_observe_or_managed_watch(self):
  fixture=SuiteFixture.create(); self.addCleanup(fixture.close)
  started=NOW+timedelta(seconds=10); fixture.status(started=started)
  controller=ProcessInfo(101,1,"python","controller",started); codex=ProcessInfo(102,101,"codex","codex",started)
  sample=ProcessSnapshot(True,(controller,codex),(),"fake")
  snap, observed=observe(fixture.config,process_provider=lambda:sample,clock=lambda:NOW+timedelta(seconds=20))
  self.assertEqual("RUNNING_CODEX",snap["lanes"][0]["operational_state"]); self.assertEqual(NOW+timedelta(seconds=20),observed)
  watcher=ProcessInfo(9001,9000,"python","watcher",NOW); owner=ProcessInfo(9000,1,"python","owner",NOW)
  processes=ProcessSnapshot(True,(watcher,owner,controller,codex),(),"fake")
  calls=iter([NOW,NOW+timedelta(seconds=20),NOW+timedelta(seconds=20)])
  def stop(_): stop_command(fixture.config,clock=lambda:NOW+timedelta(seconds=20),stream=io.StringIO())
  self.assertEqual(EXIT_OK,watch_managed(fixture.config,process_provider=lambda:processes,clock=lambda:next(calls),sleeper=stop,stream=io.StringIO(),watcher=watcher,owner=owner))
if __name__=='__main__': unittest.main()
