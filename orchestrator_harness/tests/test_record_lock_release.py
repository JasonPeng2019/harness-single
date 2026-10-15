"""Real kernel locks and deterministic acquisition/unlock/close failure fixtures."""
from __future__ import annotations

import importlib.util
import errno
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import json
import unittest
from unittest.mock import patch

from orchestrator_harness import records
from orchestrator_harness.stable_io import PathKeyedAppendLock, _PATH_LOCK_REGISTRY


class RecordLockReleaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="l")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        source = Path(__file__).resolve().parents[2] / "super-cache/workspace/.agent-workspace/lane-queue.py"
        spec = importlib.util.spec_from_file_location("standalone_lock_release_fixture", source)
        self.worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.worker)

    def locks(self):
        return (records.RecordLock(self.root / "record"),
                records.RecordLock(self.root / "anchored", root=self.root),
                PathKeyedAppendLock(self.root / "append"),
                self.worker._RecordLock(self.root / "standalone"))

    @staticmethod
    def native_api():
        if os.name == "nt":
            import msvcrt
            return msvcrt, "locking", msvcrt.LK_UNLCK
        import fcntl
        return fcntl, "flock", fcntl.LOCK_UN

    @staticmethod
    def local_lock(lock):
        if isinstance(lock, PathKeyedAppendLock):
            return lock._local_entry.lock
        return lock._local

    def assert_released(self, lock, local):
        self.assertIsNone(lock._handle)
        self.assertTrue(local.acquire(blocking=False), "the local lock must be released on every failure path")
        local.release()
        if isinstance(lock, PathKeyedAppendLock):
            self.assertIsNone(lock._anchored)
            self.assertIsNone(lock._local_entry)
            self.assertNotIn(lock.key, _PATH_LOCK_REGISTRY._entries)
        else:
            self.assertIsNone(lock._local)
            if isinstance(lock, records.RecordLock):
                self.assertIsNone(lock._anchor)
                self.assertNotIn(lock.key, records.RecordLock._registry)

    def test_kernel_acquisition_failure_closes_file_and_allows_a_fresh_lock(self):
        module, name, _ = self.native_api()
        for error in (OSError("controlled acquisition failure"), KeyboardInterrupt()):
            for lock in self.locks():
                with self.subTest(kind=type(lock).__name__, error=type(error).__name__):
                    with patch.object(module, name, side_effect=error):
                        with self.assertRaises(type(error)):
                            lock.__enter__()
                    self.assertIsNone(lock._handle)
                    if isinstance(lock, PathKeyedAppendLock):
                        self.assertIsNone(lock._anchored)
                        self.assertIsNone(lock._local_entry)
                        self.assertNotIn(lock.key, _PATH_LOCK_REGISTRY._entries)
                    else:
                        self.assertIsNone(lock._local)
                    lock.lock_path.unlink()  # Native Windows no-delete handles expose a leak here.
                    with lock:
                        pass

    def test_unlock_failure_closes_handle_and_releases_local_lock(self):
        module, name, unlock = self.native_api()
        native = getattr(module, name)
        def fail_unlock(descriptor, mode, *args):
            if mode == unlock:
                raise OSError("controlled unlock failure")
            return native(descriptor, mode, *args)
        for lock in self.locks():
            with self.subTest(kind=type(lock).__name__):
                lock.__enter__()
                local = self.local_lock(lock)
                handle = lock._handle
                with patch.object(module, name, side_effect=fail_unlock):
                    with self.assertRaises(OSError):
                        lock.__exit__(None, None, None)
                self.assertTrue(handle.closed)
                self.assert_released(lock, local)
                lock.lock_path.unlink()
                with lock:
                    pass

    def test_close_failure_before_or_after_close_still_releases_local_lock(self):
        for close_first in (False, True):
            for lock in self.locks():
                with self.subTest(kind=type(lock).__name__, close_first=close_first):
                    lock.__enter__()
                    local = self.local_lock(lock)
                    handle = lock._handle
                    native_close = handle.close
                    def fail_close():
                        if close_first:
                            native_close()
                        raise OSError("controlled close failure")
                    try:
                        with patch.object(handle, "close", side_effect=fail_close):
                            with self.assertRaises(OSError):
                                lock.__exit__(None, None, None)
                        self.assert_released(lock, local)
                    finally:
                        # A failed native close is uncertainty, not proof of kernel release.
                        native_close()
                    lock.lock_path.unlink()
                    with lock:
                        pass

    def test_unlock_and_close_failure_together_do_not_retain_the_local_lock(self):
        module, name, _ = self.native_api()
        for lock in self.locks():
            with self.subTest(kind=type(lock).__name__):
                lock.__enter__()
                local = self.local_lock(lock)
                handle = lock._handle
                native_close = handle.close
                try:
                    with patch.object(module, name, side_effect=OSError("controlled unlock failure")), patch.object(
                        handle, "close", side_effect=OSError("controlled close failure")):
                        with self.assertRaises(OSError):
                            lock.__exit__(None, None, None)
                    self.assert_released(lock, local)
                finally:
                    native_close()
                lock.lock_path.unlink()

    def test_native_anchor_close_failure_still_releases_the_local_lock(self):
        from orchestrator_harness import mutation
        for lock in self.locks()[:3]:
            with self.subTest(kind=type(lock).__name__):
                lock.__enter__()
                local = self.local_lock(lock)
                anchor = lock._anchored.anchor if isinstance(lock, PathKeyedAppendLock) else lock._anchor.anchor
                module, name = (mutation, "_CloseHandle") if os.name == "nt" else (mutation.os, "close")
                try:
                    with patch.object(module, name, side_effect=OSError("controlled native anchor close failure")):
                        with self.assertRaises(OSError):
                            lock.__exit__(None, None, None)
                    self.assert_released(lock, local)
                finally:
                    anchor.close()
                lock.lock_path.unlink()
                with lock:
                    pass

    def test_acquisition_and_close_failure_together_release_local_lock(self):
        module, name, _ = self.native_api()
        for lock in self.locks():
            with self.subTest(kind=type(lock).__name__):
                opened = []
                original_open = Path.open
                from orchestrator_harness import mutation
                anchored_open = mutation.open_append_file
                def wrap(handle):
                    original_close = handle.close
                    handle.close = lambda: (_ for _ in ()).throw(OSError("controlled close failure"))
                    opened.append((handle, original_close))
                    return handle
                def open_file(path, *args, **kwargs):
                    return wrap(original_open(path, *args, **kwargs))
                def open_anchor(*args, **kwargs):
                    anchor = anchored_open(*args, **kwargs)
                    wrap(anchor.handle)
                    return anchor
                try:
                    with patch.object(Path, "open", open_file), patch.object(mutation, "open_append_file", open_anchor), patch(
                        "orchestrator_harness.stable_io.open_append_file", open_anchor), patch.object(
                        module, name, side_effect=OSError("controlled acquisition failure")):
                        with self.assertRaises(OSError):
                            lock.__enter__()
                    self.assertIsNone(lock._handle)
                    if isinstance(lock, PathKeyedAppendLock):
                        self.assertIsNone(lock._local_entry)
                        self.assertNotIn(lock.key, _PATH_LOCK_REGISTRY._entries)
                    else:
                        self.assertIsNone(lock._local)
                        registry = lock._registry
                        if lock.key in registry:
                            self.assertTrue(registry[lock.key].acquire(blocking=False))
                            registry[lock.key].release()
                finally:
                    for handle, close in opened:
                        handle.close = close
                        close()
                lock.lock_path.unlink()

    def test_record_and_append_lock_reject_linked_or_hardlinked_lock_files(self):
        protected = self.root / "protected"
        protected.write_bytes(b"retain")
        for lock in self.locks()[:3]:
            with self.subTest(kind=type(lock).__name__):
                os.link(protected, lock.lock_path)
                with self.assertRaises(OSError):
                    lock.__enter__()
                self.assertEqual(b"retain", protected.read_bytes())
                lock.lock_path.unlink()
                with lock:
                    pass

    def test_runtime_admission_is_reentrant_in_thread_and_releases_after_error(self):
        with self.assertRaisesRegex(RuntimeError, "controlled body failure"):
            with records.runtime_admission_lock(self.root):
                with records.runtime_admission_lock(self.root):
                    raise RuntimeError("controlled body failure")
        with records.runtime_admission_lock(self.root):
            pass

    def test_threads_serialize_read_modify_replace_without_losing_updates(self):
        path = self.root / "counter"
        records.write_record(path, "counter/v1", {"value": 0})
        errors = []
        barrier = threading.Barrier(3)
        def writer():
            try:
                barrier.wait(timeout=5)
                for _ in range(8):
                    records.update_record(path, "counter/v1", lambda old: {"value": old["value"] + 1})
            except BaseException as exc:
                errors.append(exc)
        threads = [threading.Thread(target=writer, daemon=True) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait(timeout=5)
        for thread in threads:
            thread.join(timeout=15)
            self.assertFalse(thread.is_alive())
        self.assertEqual([], errors)
        self.assertEqual(16, records.read_record(path, "counter/v1")["value"])

    def test_kernel_lock_serializes_an_independent_native_process(self):
        repository = Path(__file__).resolve().parents[2]
        code = ("import sys; from pathlib import Path; from orchestrator_harness.records import RecordLock; "
                "lock=RecordLock(Path(sys.argv[1])); lock.__enter__(); print('held',flush=True); "
                "sys.stdin.readline(); lock.__exit__(None,None,None)")
        path = self.root / "cross-process"
        child = subprocess.Popen([sys.executable, "-B", "-c", code, str(path)], cwd=repository,
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        entered = threading.Event()
        errors = []
        def contender():
            try:
                with records.RecordLock(path):
                    entered.set()
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=contender, daemon=True)
        try:
            self.assertEqual("held", child.stdout.readline().strip())
            thread.start()
            self.assertFalse(entered.wait(0.15))
        finally:
            child.stdin.write("release\n")
            child.stdin.flush()
            stdout, stderr = child.communicate(timeout=15)
        thread.join(timeout=15)
        self.assertFalse(thread.is_alive())
        self.assertEqual(0, child.returncode, stderr)
        self.assertEqual([], errors)
        self.assertTrue(entered.is_set())

    @unittest.skipUnless(os.name == "nt", "Windows LK_LOCK finite wait")
    def test_windows_retries_acquisition_only_and_preserves_body_error(self):
        import msvcrt
        native = msvcrt.locking
        calls = []
        def locking(fd, mode, size):
            calls.append(mode)
            if mode == msvcrt.LK_LOCK and calls.count(mode) == 1:
                raise OSError(errno.EDEADLK, "native contention")
            return native(fd, mode, size)
        bodies = []
        with patch.object(msvcrt, "locking", side_effect=locking):
            with self.assertRaisesRegex(OSError, "body failure"):
                with records.RecordLock(self.root / "retry"):
                    bodies.append(True)
                    raise OSError(errno.EDEADLK, "body failure")
        self.assertEqual([True], bodies)
        self.assertEqual([msvcrt.LK_LOCK, msvcrt.LK_LOCK, msvcrt.LK_UNLCK], calls)
        with patch.object(msvcrt, "locking", side_effect=PermissionError(errno.EACCES, "denied")) as lock:
            with self.assertRaises(PermissionError):
                with records.RecordLock(self.root / "retry"):
                    self.fail("denied acquisition entered body")
        lock.assert_called_once()

    @unittest.skipUnless(os.name == "nt", "Windows LK_LOCK native contention")
    def test_windows_admission_waits_past_native_window_for_exact_child(self):
        from orchestrator_harness import processes
        code = ("import json,os,sys,time; from pathlib import Path; "
                "from orchestrator_harness import processes; "
                "from orchestrator_harness.records import runtime_admission_lock; "
                "lock=runtime_admission_lock(Path(sys.argv[1])); lock.__enter__(); "
                "print(json.dumps(processes.process_identity(os.getpid())),flush=True); "
                "time.sleep(12); lock.__exit__(None,None,None)")
        child = subprocess.Popen([sys.executable, "-B", "-c", code, str(self.root)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        identity = json.loads(child.stdout.readline())
        self.assertTrue(processes.identity_matches(identity["pid"], identity["creation_time"]))
        start = time.monotonic()
        try:
            with records.runtime_admission_lock(self.root):
                self.assertGreater(time.monotonic() - start, 10)
        finally:
            stdout, stderr = child.communicate(timeout=30)
        self.assertEqual(0, child.returncode, stdout + stderr)
        self.assertTrue(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))


if __name__ == "__main__":
    unittest.main()
