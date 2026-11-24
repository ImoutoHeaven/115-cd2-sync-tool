"""Unit tests for cd2_pipeline and cd2_stream."""

from __future__ import annotations

import os
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import clouddrive_pb2 as pb
import cd2_stream
from cd2_client import AuthExpiredError, CD2Client
from cd2_core import MANIFEST_FIELDS, ThrottleGate, parse_args
from cd2_manifest import Manifest, ManifestLockError
from cd2_pipeline import Pipeline, run_clean


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str = ""):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details

    def __str__(self):
        return f"{self._code.name}: {self._details}"


def make_cfg(state: str, *extra: str, cmd: str = "run"):
    argv = [cmd, "--dest", "/115", "--state", state]
    if cmd in ("scan", "run"):
        argv.extend(["--src", "/123"])
    argv.extend(extra)
    return parse_args(argv)


def make_file(full_path: str, size: int = 100, is_dir: bool = False, can_perm: bool = True):
    name = full_path.rstrip("/").rsplit("/", 1)[-1]
    return pb.CloudDriveFile(
        name=name,
        fullPathName=full_path,
        size=size,
        isDirectory=is_dir,
        canDeletePermanently=can_perm,
    )


def make_task(
    src: str,
    dest: str = "/115",
    status: int = pb.CopyTask.Completed,
    uploaded_files: int = 1,
    total_bytes: int = 100,
    uploaded_bytes: int = 100,
    failed_files: int = 0,
    paused: bool = False,
    start_sec: int = 1000,
):
    return pb.CopyTask(
        sourcePath=src,
        destPath=dest,
        status=status,
        uploadedFiles=uploaded_files,
        totalBytes=total_bytes,
        uploadedBytes=uploaded_bytes,
        failedFiles=failed_files,
        paused=paused,
        startTime=Timestamp(seconds=start_sec),
    )


def make_cloud(name: str = "115", user: str = "acc1"):
    return pb.CloudAPI(name=name, userName=user)


def make_fake_ctl():
    ctl = MagicMock(spec=CD2Client)
    ctl.creds = None
    ctl.token = "tok"
    ctl.tasks.return_value = []
    ctl.cloud_of.return_value = (make_cloud(), None, False)
    ctl.check_dest.return_value = None
    ctl.ensure_dir.return_value = (True, "")
    ctl.remove_tasks.return_value = (True, set())
    ctl.submit_one.return_value = (True, "")
    ctl.delete_batch.return_value = pb.FileOperationResult(success=True)
    ctl.delete_one.return_value = pb.FileOperationResult(success=True)
    ctl.find.return_value = None
    return ctl


class PipelineTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_path = os.path.join(self.tmp.name, "manifest.jsonl")

    def make_man(self, src: str = "/123", dest: str = "/115") -> Manifest:
        man = Manifest(self.state_path, src, dest)
        self.addCleanup(man.close)
        return man


class BatchDeleteAndThrottleTest(PipelineTestBase):
    def test_batch_delete_single_call_and_no_dir_cache_purge(self):
        man = self.make_man()
        files = [make_file(f"/123/f{i}.mkv", size=100 + i) for i in range(4)]
        recs = man.add_batch(files)
        man.set_batch([(r, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100}) for r in recs])

        ctl = make_fake_ctl()
        ctl.find.side_effect = lambda p: make_file(
            p, size=100 + int(p.rsplit("f", 1)[1].split(".")[0])
        )
        cfg = make_cfg(self.state_path, "--delete-batch", "10")
        gate = ThrottleGate(base=5.0, max_backoff=60.0)
        pipe = Pipeline(ctl, man, cfg, gate=gate)
        pipe.cloud = "115|acc1"

        pipe.clear_landed()

        self.assertEqual(ctl.delete_batch.call_count, 1)
        deleted_paths = ctl.delete_batch.call_args.args[0]
        self.assertEqual(len(deleted_paths), 4)
        self.assertEqual(ctl.delete_one.call_count, 0)
        self.assertEqual(pipe.dropped, 4)
        self.assertEqual(pipe.stranded, 0)
        self.assertEqual(len(man.of("dropped")), 4)
        # Never call ForceExpireDirCache or forceRefresh list_dir
        ctl.list_dir.assert_not_called()

    def test_bisect_fallback_isolates_bad_file(self):
        man = self.make_man()
        files = [make_file(f"/123/f{i}.mkv", size=100) for i in range(4)]
        recs = man.add_batch(files)
        man.set_batch([(r, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100}) for r in recs])

        bad_path = "/115/f2.mkv"
        ctl = make_fake_ctl()
        ctl.find.side_effect = lambda p: make_file(p, size=100)

        def fake_delete_batch(paths, permanent=False, timeout=45.0):
            if bad_path in paths:
                return pb.FileOperationResult(success=False, errorMessage="file locked")
            return pb.FileOperationResult(success=True, resultFilePaths=list(paths))

        def fake_delete_one(path, permanent=False, timeout=45.0):
            if path == bad_path:
                return pb.FileOperationResult(success=False, errorMessage="file locked")
            return pb.FileOperationResult(success=True, resultFilePaths=[path])

        ctl.delete_batch.side_effect = fake_delete_batch
        ctl.delete_one.side_effect = fake_delete_one

        cfg = make_cfg(self.state_path, "--delete-batch", "10", "--stall", "300")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        pipe.clear_landed()

        self.assertEqual(pipe.dropped, 3)
        self.assertEqual(pipe.stranded, 0)
        self.assertEqual(man.recs["f0.mkv"]["state"], "dropped")
        self.assertEqual(man.recs["f1.mkv"]["state"], "dropped")
        self.assertEqual(man.recs["f3.mkv"]["state"], "dropped")
        self.assertEqual(man.recs["f2.mkv"]["state"], "landed")
        self.assertGreaterEqual(ctl.delete_one.call_count, 1)
        # Runtime retry state must not pollute Manifest record
        self.assertNotIn("retry", man.recs["f2.mkv"])
        self.assertNotIn("clear_since", man.recs["f2.mkv"])
        self.assertEqual(pipe._rt["f2.mkv"]["retry"], 1)

    def test_throttle_aborts_without_bisect_and_freezes_stall_timer(self):
        man = self.make_man()
        files = [make_file(f"/123/f{i}.mkv", size=100) for i in range(3)]
        recs = man.add_batch(files)
        man.set_batch([(r, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100}) for r in recs])

        now = [1000.0]
        gate = ThrottleGate(base=20.0, max_backoff=120.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
        ctl = make_fake_ctl()
        ctl.find.side_effect = lambda p: make_file(p, size=100)

        # First: non-throttle failure starts clear_since at t=1000
        ctl.delete_batch.return_value = pb.FileOperationResult(success=False, errorMessage="transient io")
        ctl.delete_one.return_value = pb.FileOperationResult(success=False, errorMessage="transient io")
        cfg = make_cfg(self.state_path, "--delete-batch", "10", "--stall", "30")
        pipe = Pipeline(ctl, man, cfg, gate=gate)
        pipe.cloud = "115|acc1"

        with patch("cd2_pipeline.time.time", side_effect=lambda: now[0]):
            pipe.clear_landed()
            self.assertEqual(pipe._rt["f0.mkv"]["clear_since"], 1000.0)

            # Next at t=1010: hit 115open throttle -> must abort immediately without bisecting!
            now[0] = 1010.0
            ctl.delete_one.reset_mock()
            ctl.delete_batch.reset_mock()
            ctl.delete_batch.side_effect = FakeRpcError(
                grpc.StatusCode.INTERNAL, "115open throttle (code 405)"
            )
            pipe.clear_landed()
            self.assertEqual(ctl.delete_batch.call_count, 1)
            self.assertEqual(ctl.delete_one.call_count, 0)
            self.assertTrue(gate.in_cooldown())

            # During cooldown (t=1025): clear_landed skips remote calls
            now[0] = 1025.0
            ctl.delete_batch.reset_mock()
            pipe.clear_landed()
            ctl.delete_batch.assert_not_called()

            # At t=1035: wall time elapsed is 35s (> stall=30s), but 20s was cooldown -> effective 15s < 30s!
            now[0] = 1035.0
            ctl.delete_batch.side_effect = None
            ctl.delete_batch.return_value = pb.FileOperationResult(success=False, errorMessage="transient io")
            pipe.clear_landed()
            self.assertEqual(man.recs["f0.mkv"]["state"], "landed")
            self.assertEqual(pipe.stranded, 0)
            self.assertAlmostEqual(pipe._rt["f0.mkv"]["clear_since"], 1020.0)

    def test_permanent_unsupported_and_size_mismatch_mark_stranded_separately(self):
        man = self.make_man()
        r_perm = man.add(make_file("/123/no_perm.mkv", size=100))
        r_size = man.add(make_file("/123/wrong_size.mkv", size=200))
        r_dir = man.add(make_file("/123/is_dir.mkv", size=300))
        man.set_batch([
            (r_perm, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100}),
            (r_size, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100}),
            (r_dir, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100}),
        ])

        ctl = make_fake_ctl()
        def fake_find(p):
            if p.endswith("no_perm.mkv"):
                return make_file(p, size=100, can_perm=False)
            if p.endswith("wrong_size.mkv"):
                return make_file(p, size=999, can_perm=True)
            return make_file(p, size=300, is_dir=True, can_perm=True)

        ctl.find.side_effect = fake_find
        cfg = make_cfg(self.state_path, "--delete", "permanent", "--stall", "300")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        pipe.clear_landed()

        self.assertEqual(pipe.stranded, 3)
        self.assertEqual(pipe.dropped, 0)
        for name in ("no_perm.mkv", "wrong_size.mkv", "is_dir.mkv"):
            self.assertEqual(man.recs[name]["state"], "stranded")
        ctl.delete_batch.assert_not_called()

    def test_fast_step_zero_remote_calls_when_no_landed(self):
        man = self.make_man()
        r = man.add(make_file("/123/a.mkv", size=100))
        man.set(r, "submitted", dest_root="/115", cloud="115|acc1", submitted_at=100)

        ctl = make_fake_ctl()
        ctl.tasks.return_value = [make_task("/123/a.mkv", status=pb.CopyTask.Scanning, uploaded_files=0)]
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)

        pipe.fast_step()

        ctl.tasks.assert_called_once()
        ctl.find.assert_not_called()
        ctl.delete_batch.assert_not_called()
        ctl.cloud_of.assert_not_called()

        # Now task finishes -> fast_step harvests locally -> landed_at set -> deletes in batch
        ctl.tasks.return_value = [make_task("/123/a.mkv", status=pb.CopyTask.Completed, total_bytes=100, uploaded_bytes=100)]
        ctl.find.return_value = make_file("/115/a.mkv", size=100)
        pipe.fast_step()
        self.assertEqual(man.recs["a.mkv"]["state"], "dropped")
        self.assertGreater(man.recs["a.mkv"]["landed_at"], 0)
        ctl.delete_batch.assert_called_once_with(["/115/a.mkv"], permanent=False, timeout=cfg.delete_timeout)

    def test_delete_chunk_non_bisect_codes_abort_without_bisect(self):
        for code in (
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.UNAVAILABLE,
        ):
            with self.subTest(code=code.name):
                state = os.path.join(self.tmp.name, f"m_{code.name}.jsonl")
                man = Manifest(state, "/123", "/115")
                self.addCleanup(man.close)
                recs = man.add_batch([make_file(f"/123/f{i}.mkv", size=100) for i in range(4)])
                man.set_batch([
                    (r, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100})
                    for r in recs
                ])

                ctl = make_fake_ctl()
                ctl.find.side_effect = lambda p: make_file(p, size=100)
                ctl.delete_batch.side_effect = FakeRpcError(code, "session broken")
                cfg = make_cfg(state, "--delete-batch", "4", "--stall", "300")
                pipe = Pipeline(ctl, man, cfg)
                pipe.cloud = "115|acc1"
                pipe._dest_checked_at = 12345.0

                pipe.clear_landed()

                self.assertEqual(ctl.delete_batch.call_count, 1)
                self.assertEqual(ctl.delete_one.call_count, 0)
                self.assertEqual(pipe._dest_checked_at, 0.0)
                for i in range(4):
                    self.assertEqual(pipe._rt[f"f{i}.mkv"]["retry"], 1)
                    self.assertEqual(man.recs[f"f{i}.mkv"]["state"], "landed")

    def test_bisect_delete_non_bisect_codes_abort_without_further_bisect(self):
        man = self.make_man()
        recs = man.add_batch([make_file(f"/123/f{i}.mkv", size=100) for i in range(4)])
        man.set_batch([
            (r, "landed", {"dest_root": "/115", "cloud": "115|acc1", "landed_at": 100})
            for r in recs
        ])
        chunk = [(r, f"/115/f{i}.mkv") for i, r in enumerate(recs)]

        ctl = make_fake_ctl()
        ctl.delete_batch.side_effect = FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "timeout")
        cfg = make_cfg(self.state_path, "--stall", "300")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"
        pipe._dest_checked_at = 999.0

        ok = pipe._bisect_delete(chunk, now=1000.0)

        self.assertFalse(ok)
        # Only the first half was attempted; must not recurse or attempt the second half
        self.assertEqual(ctl.delete_batch.call_count, 1)
        self.assertEqual(ctl.delete_one.call_count, 0)
        self.assertEqual(pipe._dest_checked_at, 0.0)
        self.assertEqual(pipe._rt["f0.mkv"]["retry"], 1)
        self.assertEqual(pipe._rt["f1.mkv"]["retry"], 1)
        self.assertNotIn("f2.mkv", pipe._rt)

    def test_clear_landed_empty_dest_root_not_dropped_as_mismatch(self):
        man = self.make_man()
        r = man.add(make_file("/123/a.mkv", size=100))
        man.set(r, "landed", dest_root="", cloud="115|acc1", landed_at=100)

        ctl = make_fake_ctl()
        ctl.find.return_value = make_file("/115/a.mkv", size=100)
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        pipe.clear_landed()

        ctl.find.assert_called_once_with("/115/a.mkv")
        ctl.delete_batch.assert_called_once_with(
            ["/115/a.mkv"], permanent=False, timeout=cfg.delete_timeout
        )
        self.assertEqual(man.recs["a.mkv"]["state"], "dropped")


class MissingGraceTest(PipelineTestBase):
    def test_missing_grace_recovers_on_second_check(self):
        man = self.make_man()
        r = man.add(make_file("/123/a.mkv", size=100))
        man.set(r, "landed", dest_root="/115", cloud="115|acc1", landed_at=1000)

        ctl = make_fake_ctl()
        ctl.find.side_effect = [None, make_file("/115/a.mkv", size=100)]
        cfg = make_cfg(self.state_path, "--missing-grace", "10")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        with patch("cd2_pipeline.time.time", return_value=1002.0):
            pipe.clear_landed()
            self.assertEqual(man.recs["a.mkv"]["state"], "landed")
            self.assertEqual(pipe._rt["a.mkv"]["miss_count"], 1)

        with patch("cd2_pipeline.time.time", return_value=1005.0):
            pipe.clear_landed()
            self.assertEqual(man.recs["a.mkv"]["state"], "dropped")
            ctl.delete_batch.assert_called_once_with(["/115/a.mkv"], permanent=False, timeout=cfg.delete_timeout)
        ctl.list_dir.assert_not_called()

    def test_missing_grace_marks_dropped_only_after_grace_and_two_misses(self):
        man = self.make_man()
        r = man.add(make_file("/123/gone.mkv", size=100))
        man.set(r, "landed", dest_root="/115", cloud="115|acc1", landed_at=1000)

        ctl = make_fake_ctl()
        ctl.find.return_value = None
        cfg = make_cfg(self.state_path, "--missing-grace", "10")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        # Miss 1 after grace still requires miss_count >= 2
        with patch("cd2_pipeline.time.time", return_value=1015.0):
            pipe.clear_landed()
            self.assertEqual(man.recs["gone.mkv"]["state"], "landed")
            self.assertEqual(pipe._rt["gone.mkv"]["miss_count"], 1)

        # Miss 2 before grace would not drop; after grace drops
        with patch("cd2_pipeline.time.time", return_value=1018.0):
            pipe.clear_landed()
            self.assertEqual(man.recs["gone.mkv"]["state"], "dropped")
            self.assertEqual(pipe.dropped, 1)
        ctl.delete_batch.assert_not_called()
        ctl.list_dir.assert_not_called()


class PushTaskChangeListenerTest(PipelineTestBase):
    def test_should_wake_filters_precisely(self):
        man = self.make_man()
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)

        # Initial snapshot with copyTaskCount=3, no finish -> initialize only, no wake
        self.assertFalse(pipe._should_wake(pb.GetAllTasksCountResult(copyTaskCount=3)))
        # Same copyTaskCount=3 (unrelated download tick) -> no wake
        self.assertFalse(pipe._should_wake(pb.GetAllTasksCountResult(downloadCount=5, copyTaskCount=3)))
        # Transfer+Copy -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.GetAllTasksCountResult(
                    copyTaskCount=3,
                    uploadFileStatusChanges=[
                        pb.UploadFileInfo(
                            statusEnum=pb.UploadFileInfo.Transfer,
                            operatorType=pb.UploadFileInfo.Copy,
                        )
                    ],
                )
            )
        )
        # Finish+Mount -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.GetAllTasksCountResult(
                    copyTaskCount=3,
                    uploadFileStatusChanges=[
                        pb.UploadFileInfo(
                            statusEnum=pb.UploadFileInfo.Finish,
                            operatorType=pb.UploadFileInfo.Mount,
                        )
                    ],
                )
            )
        )
        # Finish+Copy -> wake!
        self.assertTrue(
            pipe._should_wake(
                pb.GetAllTasksCountResult(
                    copyTaskCount=3,
                    uploadFileStatusChanges=[
                        pb.UploadFileInfo(
                            statusEnum=pb.UploadFileInfo.Finish,
                            operatorType=pb.UploadFileInfo.Copy,
                        )
                    ],
                )
            )
        )
        # copyTaskCount delta (3 -> 2) -> wake!
        self.assertTrue(pipe._should_wake(pb.GetAllTasksCountResult(copyTaskCount=2)))

    def test_listen_cancel_on_stop_and_auth_expired_exits(self):
        man = self.make_man()
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)
        pipe.DEBOUNCE_SECS = 0.02

        cancelled = threading.Event()
        entered = threading.Event()

        class FakeStream:
            def __iter__(self):
                entered.set()
                yield pb.GetAllTasksCountResult(
                    copyTaskCount=1,
                    uploadFileStatusChanges=[
                        pb.UploadFileInfo(
                            statusEnum=pb.UploadFileInfo.Finish,
                            operatorType=pb.UploadFileInfo.Copy,
                        )
                    ],
                )
                cancelled.wait(timeout=2.0)

            def cancel(self):
                cancelled.set()

        ctl.push_task_changes.return_value = FakeStream()
        t = threading.Thread(target=pipe.listen, daemon=True)
        t.start()
        self.assertTrue(entered.wait(timeout=2.0))
        self.assertTrue(pipe.trigger.wait(timeout=1.0))

        pipe.stop.set()
        pipe._cancel_stream()
        t.join(timeout=2.0)
        self.assertTrue(cancelled.is_set())
        self.assertFalse(t.is_alive())

        # AuthExpiredError stops listener and sets stop + _auth_error
        pipe2 = Pipeline(ctl, man, cfg)
        ctl.push_task_changes.side_effect = AuthExpiredError("expired")
        pipe2.listen()
        self.assertTrue(pipe2.stop.is_set())
        self.assertIsInstance(pipe2._auth_error, AuthExpiredError)


class AccountRotationTest(PipelineTestBase):
    def test_rotated_persists_resets_and_drops_old_landed_without_deleting_new_account(self):
        man = self.make_man()
        r_fail = man.add(make_file("/123/fail.mkv", size=100))
        r_pend = man.add(make_file("/123/pend.mkv", size=100))
        r_sub = man.add(make_file("/123/sub.mkv", size=100))
        r_land = man.add(make_file("/123/same.mkv", size=100))

        man.set_batch([
            (r_fail, "failed", {"attempts": 5, "submit_fails": 5, "cloud": "115|acc1", "dest_root": "/115"}),
            (r_pend, "pending", {"attempts": 2, "submit_fails": 3, "cloud": "115|acc1", "dest_root": "/115"}),
            (r_sub, "submitted", {"attempts": 4, "submit_fails": 2, "cloud": "115|acc1", "dest_root": "/115", "submitted_at": 100}),
            (r_land, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100}),
        ])

        ctl = make_fake_ctl()
        # New account happens to have a same-named, same-size file!
        ctl.find.return_value = make_file("/115/same.mkv", size=100)
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        pipe.rotated("115|acc2")
        pipe.cloud = "115|acc2"

        self.assertEqual(man.recs["fail.mkv"]["state"], "pending")
        self.assertEqual(man.recs["fail.mkv"]["attempts"], 0)
        self.assertEqual(man.recs["fail.mkv"]["submit_fails"], 0)
        self.assertEqual(man.recs["pend.mkv"]["attempts"], 0)
        self.assertEqual(man.recs["pend.mkv"]["submit_fails"], 0)
        self.assertEqual(man.recs["sub.mkv"]["attempts"], 0)
        self.assertEqual(man.recs["sub.mkv"]["submit_fails"], 0)
        self.assertEqual(man.recs["same.mkv"]["state"], "dropped")
        ctl.delete_batch.assert_not_called()
        ctl.delete_one.assert_not_called()

        # Verify persisted to disk by reloading manifest
        man.close()
        reloaded = Manifest(self.state_path, "/123", "/115")
        self.addCleanup(reloaded.close)
        self.assertEqual(reloaded.recs["fail.mkv"]["state"], "pending")
        self.assertEqual(reloaded.recs["fail.mkv"]["attempts"], 0)
        self.assertEqual(reloaded.recs["sub.mkv"]["attempts"], 0)
        self.assertEqual(reloaded.recs["same.mkv"]["state"], "dropped")

    def test_clear_landed_cross_account_guard_never_deletes_new_account_file(self):
        man = self.make_man()
        r = man.add(make_file("/123/dup.mkv", size=500))
        man.set(r, "landed", dest_root="/115", cloud="115|acc1", landed_at=100)

        ctl = make_fake_ctl()
        ctl.find.return_value = make_file("/115/dup.mkv", size=500)
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc2"

        pipe.clear_landed()

        self.assertEqual(man.recs["dup.mkv"]["state"], "dropped")
        ctl.find.assert_not_called()
        ctl.delete_batch.assert_not_called()

    def test_retry_updates_cloud_so_reconcile_does_not_requeue_infinitely(self):
        man = self.make_man()
        r = man.add(make_file("/123/a.mkv", size=100))
        man.set(r, "submitted", dest_root="/115", cloud="115|acc1", submitted_at=100, attempts=0)

        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--stall", "300")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc2"
        pipe.rotated_at = 200.0

        with patch("cd2_pipeline.time.time", return_value=250.0):
            pipe._retry(r, "disk full")
            self.assertEqual(r["cloud"], "115|acc2")
            # Next reconcile with empty seen: last=250 >= rotated_at=200 and cloud matches -> stays submitted
            pipe.reconcile(set())
            self.assertEqual(r["state"], "submitted")

    def test_slow_step_refreshes_tasks_after_rotation_and_dest_ready_ttl_cache(self):
        man = self.make_man()
        r = man.add(make_file("/123/a.mkv", size=100))
        man.set(r, "failed", dest_root="/115", cloud="115|acc1", attempts=5)

        ctl = make_fake_ctl()
        old_failed = make_task("/123/a.mkv", status=pb.CopyTask.Failed, uploaded_files=0)
        ctl.cloud_of.side_effect = [
            (make_cloud("115", "acc1"), None, False),
            (make_cloud("115", "acc2"), None, False),
        ]
        # slow_step 1: acc1; slow_step 2 (after TTL): acc2 -> rotation triggers second ctl.tasks()!
        ctl.tasks.side_effect = [[old_failed], [old_failed], []]

        cfg = make_cfg(self.state_path, "--sweep", "20")
        pipe = Pipeline(ctl, man, cfg)

        mono = [100.0]
        with patch("cd2_pipeline.time.monotonic", side_effect=lambda: mono[0]):
            pipe.slow_step()
            self.assertEqual(ctl.cloud_of.call_count, 1)

            # Within TTL (< 20s): dest_ready uses cache and does NOT call cloud_of again
            mono[0] = 105.0
            self.assertIsNone(pipe.dest_ready())
            self.assertEqual(ctl.cloud_of.call_count, 1)

            # Past TTL (>= 20s): dest_ready re-queries cloud_of -> detects rotation to acc2
            mono[0] = 125.0
            pipe.slow_step()
            self.assertEqual(ctl.cloud_of.call_count, 2)
            # ctl.tasks called once in step1 + twice in step2 (before and after rotation)
            self.assertEqual(ctl.tasks.call_count, 3)
            # Fresh submit succeeded on acc2 instead of adopting old failed task
            ctl.submit_one.assert_called_once_with("/123/a.mkv", "/115")

    def test_submit_pending_does_not_adopt_old_account_working_or_paused_task(self):
        man = self.make_man()
        r_work = man.add(make_file("/123/work.mkv", size=100))
        r_pause = man.add(make_file("/123/pause.mkv", size=200))
        man.set_batch([
            (r_work, "pending", {"dest_root": "/115", "cloud": "115|acc1"}),
            (r_pause, "pending", {"dest_root": "/115", "cloud": "115|acc1"}),
        ])

        ctl = make_fake_ctl()
        ctl.tasks.return_value = []  # after remove_tasks, old tasks are gone
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc2"

        existing_tasks = [
            make_task("/123/work.mkv", status=pb.CopyTask.Scanning, uploaded_files=0, paused=False),
            make_task("/123/pause.mkv", status=pb.CopyTask.Scanning, uploaded_files=0, paused=True),
        ]

        pipe.submit_pending(existing_tasks)

        self.assertEqual(ctl.remove_tasks.call_count, 2)
        self.assertEqual(ctl.submit_one.call_count, 2)
        submitted_srcs = [c.args[0] for c in ctl.submit_one.call_args_list]
        self.assertEqual(submitted_srcs, ["/123/work.mkv", "/123/pause.mkv"])
        self.assertEqual(r_work["cloud"], "115|acc2")
        self.assertEqual(r_pause["cloud"], "115|acc2")


class StateMachineHardeningTest(PipelineTestBase):
    def test_paused_and_cancelled_tasks_restart_after_retry_after(self):
        man = self.make_man()
        r_paused = man.add(make_file("/123/p.mkv", size=100))
        r_cancel = man.add(make_file("/123/c.mkv", size=100))
        man.set_batch([
            (r_paused, "submitted", {"dest_root": "/115", "cloud": "115|acc1", "submitted_at": 100}),
            (r_cancel, "submitted", {"dest_root": "/115", "cloud": "115|acc1", "submitted_at": 100}),
        ])

        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--retry-after", "60")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        tasks = [
            make_task("/123/p.mkv", status=pb.CopyTask.Scanning, uploaded_files=0, paused=True),
            make_task("/123/c.mkv", status=5, uploaded_files=0, paused=False),  # Cancelled = 5
        ]

        with patch("cd2_pipeline.time.time", return_value=1000.0):
            pipe.poll_tasks(tasks, ready=True)
            self.assertEqual(pipe.due, [])
            self.assertEqual(pipe._rt["p.mkv"]["failed_at"], 1000.0)
            self.assertEqual(pipe._rt["c.mkv"]["failed_at"], 1000.0)

        with patch("cd2_pipeline.time.time", return_value=1065.0):
            pipe.poll_tasks(tasks, ready=True)
            self.assertEqual(len(pipe.due), 2)
            pipe.restart_due()
            self.assertEqual(ctl.restart_task.call_count, 2)
            self.assertEqual(man.recs["p.mkv"]["attempts"], 1)
            self.assertEqual(man.recs["c.mkv"]["attempts"], 1)

    def test_failed_lane_mutex_and_cleanup_and_cross_subdir_serialization(self):
        man = self.make_man()
        r1 = man.add(make_file("/123/dir1/dup.mkv", size=100))
        r2 = man.add(make_file("/123/dir2/dup.mkv", size=100))
        man.set(r1, "submitted", dest_root="/115", cloud="115|acc1", attempts=5)

        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--max-attempts", "5")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        # Parking r1 as failed must proactively call remove_tasks
        pipe._retry(r1, "still failing")
        self.assertEqual(r1["state"], "failed")
        ctl.remove_tasks.assert_called_once_with(["/123/dir1/dup.mkv:/115"])

        # While residual engine task for failed r1 still exists, dir2/dup.mkv MUST NOT be submitted!
        residual = [make_task("/123/dir1/dup.mkv", status=pb.CopyTask.Failed, uploaded_files=0)]
        pipe.submit_pending(residual)
        ctl.submit_one.assert_not_called()
        self.assertEqual(r2["state"], "pending")

        # Once residual task is gone, dir2/dup.mkv can submit, while dir3/dup.mkv waits in series
        r3 = man.add(make_file("/123/dir3/dup.mkv", size=100))
        pipe.submit_pending([])
        ctl.submit_one.assert_called_once_with("/123/dir2/dup.mkv", "/115")
        self.assertEqual(r2["state"], "submitted")
        self.assertEqual(r3["state"], "pending")

    def test_submit_pending_batch_predelete_and_skip_dir_conflict(self):
        man = self.make_man()
        r1 = man.add(make_file("/123/a.mkv", size=100))
        r2 = man.add(make_file("/123/b.mkv", size=100))
        r3 = man.add(make_file("/123/dir_conflict.mkv", size=100))

        ctl = make_fake_ctl()
        def fake_find(p):
            if p.endswith("dir_conflict.mkv"):
                return make_file(p, size=0, is_dir=True)
            return make_file(p, size=50)

        ctl.find.side_effect = fake_find
        cfg = make_cfg(self.state_path, "--max-attempts", "5")
        pipe = Pipeline(ctl, man, cfg)
        pipe.cloud = "115|acc1"

        with patch.object(man, "set", wraps=man.set) as spy_set:
            pipe.submit_pending([])
            # Single batch pre-delete for both stale files (skipping directory conflict)
            ctl.delete_batch.assert_called_once_with(
                ["/115/a.mkv", "/115/b.mkv"], permanent=False, timeout=cfg.delete_timeout
            )
            self.assertEqual(ctl.submit_one.call_count, 2)
            self.assertEqual(r1["state"], "submitted")
            self.assertEqual(r2["state"], "submitted")
            self.assertEqual(r3["state"], "pending")
            self.assertEqual(r3["submit_fails"], 1)
            # Only 1 manifest write per clean submission (no redundant submit_fails=0 fsync)
            self.assertEqual(spy_set.call_count, 2)

    def test_prune_done_keeps_failed_keys_and_preserves_trailing_slash(self):
        man = self.make_man()
        r = man.add(make_file("/123/a.mkv", size=100))
        man.set(r, "submitted", dest_root="/115", cloud="115|acc1")

        ctl = make_fake_ctl()
        # Engine reports destPath with trailing slash!
        t = make_task("/123/a.mkv", dest="/115/", status=pb.CopyTask.Completed, total_bytes=100, uploaded_bytes=100)
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)

        pipe.poll_tasks([t], ready=True)
        self.assertEqual(r["state"], "landed")
        self.assertEqual(pipe.prune, {"/123/a.mkv:/115/"})

        # First remove_tasks fails -> key stays in self.prune
        ctl.remove_tasks.return_value = (False, {"/123/a.mkv:/115/"})
        pipe.prune_done()
        self.assertEqual(pipe.prune, {"/123/a.mkv:/115/"})

        # Second remove_tasks succeeds -> key removed
        ctl.remove_tasks.return_value = (True, set())
        pipe.prune_done()
        self.assertEqual(pipe.prune, set())

    def test_uploaded_size_check_and_duplicate_task_priority(self):
        man = self.make_man()
        r_bad = man.add(make_file("/123/wrong_bytes.mkv", size=1000))
        r_dup = man.add(make_file("/123/dup.mkv", size=500))
        man.set_batch([
            (r_bad, "submitted", {"dest_root": "/115", "cloud": "115|acc1"}),
            (r_dup, "submitted", {"dest_root": "/115", "cloud": "115|acc1"}),
        ])

        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--retry-after", "60")
        pipe = Pipeline(ctl, man, cfg)

        tasks = [
            # totalBytes=999 != expected_size=1000 -> must NOT be treated as uploaded!
            make_task("/123/wrong_bytes.mkv", status=pb.CopyTask.Completed, total_bytes=999, uploaded_bytes=999),
            # Duplicate tasks for dup.mkv: Failed (newer), WORKING (middle), Completed (older) -> Completed wins!
            make_task("/123/dup.mkv", dest="/115", status=pb.CopyTask.Failed, uploaded_files=0, start_sec=3000),
            make_task("/123/dup.mkv", dest="/115", status=pb.CopyTask.Scanning, uploaded_files=0, start_sec=2000),
            make_task("/123/dup.mkv", dest="/115", status=pb.CopyTask.Completed, total_bytes=500, uploaded_bytes=500, start_sec=1000),
        ]

        pipe.poll_tasks(tasks, ready=True)
        self.assertEqual(r_bad["state"], "submitted")
        self.assertEqual(r_dup["state"], "landed")
        # Volatile state never leaks into persisted manifest fields
        for rec in man.recs.values():
            self.assertTrue(set(rec.keys()).issubset(set(MANIFEST_FIELDS)))

    def test_submit_pending_predelete_throttle_does_not_bump_and_cooldown_skips_predelete(self):
        # 1) find(stale) hits throttle -> records throttle, no bump, stops further find/pre-delete
        state1 = os.path.join(self.tmp.name, "m_t1.jsonl")
        man1 = Manifest(state1, "/123", "/115")
        self.addCleanup(man1.close)
        r1 = man1.add(make_file("/123/a.mkv", size=100))
        r2 = man1.add(make_file("/123/b.mkv", size=100))
        ctl1 = make_fake_ctl()
        ctl1.find.side_effect = [
            make_file("/115/a.mkv", size=50),
            FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "115open throttle"),
        ]
        now = [1000.0]
        gate1 = ThrottleGate(base=10.0, max_backoff=60.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
        pipe1 = Pipeline(ctl1, man1, make_cfg(state1), gate=gate1)
        pipe1.cloud = "115|acc1"

        pipe1.submit_pending([])
        self.assertTrue(gate1.in_cooldown())
        ctl1.delete_batch.assert_not_called()
        ctl1.submit_one.assert_not_called()
        self.assertEqual(r1["submit_fails"], 0)
        self.assertEqual(r2["submit_fails"], 0)

        # 2) While in cooldown: stale file is deferred without bump, non-stale file still submits
        state2 = os.path.join(self.tmp.name, "m_t2.jsonl")
        man2 = Manifest(state2, "/123", "/115")
        self.addCleanup(man2.close)
        r_clean = man2.add(make_file("/123/clean.mkv", size=100))
        r_stale = man2.add(make_file("/123/stale.mkv", size=100))
        ctl2 = make_fake_ctl()
        ctl2.find.side_effect = lambda p: make_file(p, size=50) if p.endswith("stale.mkv") else None
        gate2 = ThrottleGate(base=10.0, max_backoff=60.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
        gate2.record_throttle(5)
        pipe2 = Pipeline(ctl2, man2, make_cfg(state2), gate=gate2)
        pipe2.cloud = "115|acc1"

        pipe2.submit_pending([])
        ctl2.delete_batch.assert_not_called()
        ctl2.submit_one.assert_called_once_with("/123/clean.mkv", "/115")
        self.assertEqual(r_clean["state"], "submitted")
        self.assertEqual(r_stale["state"], "pending")
        self.assertEqual(r_stale["submit_fails"], 0)

        # 3) delete_batch(stale_paths) hits throttle via RpcError AND via res.errorMessage -> no bump
        for mode in ("rpc_error", "error_message"):
            with self.subTest(mode=mode):
                state3 = os.path.join(self.tmp.name, f"m_t3_{mode}.jsonl")
                man3 = Manifest(state3, "/123", "/115")
                self.addCleanup(man3.close)
                rs = man3.add(make_file("/123/s.mkv", size=100))
                ctl3 = make_fake_ctl()
                ctl3.find.return_value = make_file("/115/s.mkv", size=50)
                if mode == "rpc_error":
                    ctl3.delete_batch.side_effect = FakeRpcError(
                        grpc.StatusCode.INTERNAL, "115open throttle (code 405)"
                    )
                else:
                    ctl3.delete_batch.return_value = pb.FileOperationResult(
                        success=False, errorMessage="115open throttle"
                    )
                gate3 = ThrottleGate(base=10.0, max_backoff=60.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
                pipe3 = Pipeline(ctl3, man3, make_cfg(state3), gate=gate3)
                pipe3.cloud = "115|acc1"

                pipe3.submit_pending([])
                self.assertTrue(gate3.in_cooldown())
                ctl3.submit_one.assert_not_called()
                self.assertEqual(rs["state"], "pending")
                self.assertEqual(rs["submit_fails"], 0)

    def test_submit_pending_find_non_bisect_code_aborts_delete_batch_and_submit_one(self):
        man = self.make_man()
        r1 = man.add(make_file("/123/a.mkv", size=100))
        r2 = man.add(make_file("/123/b.mkv", size=100))
        r3 = man.add(make_file("/123/c.mkv", size=100))

        ctl = make_fake_ctl()
        ctl.find.side_effect = [
            make_file("/115/a.mkv", size=50),
            None,
            FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "session expired"),
        ]
        pipe = Pipeline(ctl, man, make_cfg(self.state_path))
        pipe.cloud = "115|acc1"
        pipe._dest_checked_at = 500.0

        pipe.submit_pending([])

        self.assertEqual(pipe._dest_checked_at, 0.0)
        ctl.delete_batch.assert_not_called()
        ctl.submit_one.assert_not_called()
        for r in (r1, r2, r3):
            self.assertEqual(r["state"], "pending")

    def test_run_clears_trigger_before_step_so_mid_step_wake_is_not_lost(self):
        man = self.make_man()
        r = man.add(make_file("/123/a.mkv", size=100))
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--sweep", "60", "--delete-interval", "60")
        pipe = Pipeline(ctl, man, cfg)

        step_calls = 0

        def fake_slow_step():
            nonlocal step_calls
            step_calls += 1
            if step_calls == 1:
                # Wake arrives during step; trigger must remain set for trigger.wait()
                pipe.trigger.set()
            else:
                man.set(r, "dropped")

        def fake_fast_step():
            man.set(r, "dropped")

        with patch.object(pipe, "listen"), patch.object(pipe, "slow_step", side_effect=fake_slow_step), patch.object(
            pipe, "fast_step", side_effect=fake_fast_step
        ):
            t0 = time.monotonic()
            rc = pipe.run()
            elapsed = time.monotonic() - t0

        self.assertEqual(rc, 0)
        # If trigger.clear() were after wait() or lost, wait(60) would block for 60s
        self.assertLess(elapsed, 2.0)


class RunCleanAndCLITest(PipelineTestBase):
    def test_run_clean_dry_run(self):
        ctl = make_fake_ctl()
        ctl.list_dir.return_value = [
            make_file("/115/a.mkv", size=100),
            make_file("/115/sub", size=0, is_dir=True),
        ]
        cfg = make_cfg(self.state_path, cmd="clean")
        rc = run_clean(ctl, cfg)
        self.assertEqual(rc, 0)
        ctl.list_dir.assert_called_once_with("/115", force_refresh=False)
        ctl.delete_batch.assert_not_called()

    def test_run_clean_yes_batch_and_permanent_filter(self):
        ctl = make_fake_ctl()
        ctl.list_dir.return_value = [
            make_file("/115/a.mkv", size=100, can_perm=True),
            make_file("/115/b.mkv", size=200, can_perm=False),
            make_file("/115/c.mkv", size=300, can_perm=True),
            make_file("/115/dir", size=0, is_dir=True, can_perm=True),
        ]
        cfg = make_cfg(self.state_path, "--yes", "--delete", "permanent", "--delete-batch", "10", cmd="clean")
        rc = run_clean(ctl, cfg)
        self.assertEqual(rc, 0)
        ctl.delete_batch.assert_called_once_with(
            ["/115/a.mkv", "/115/c.mkv"], permanent=True, timeout=cfg.delete_timeout
        )

    def test_run_clean_throttle_retry(self):
        ctl = make_fake_ctl()
        ctl.list_dir.return_value = [make_file(f"/115/f{i}.mkv", size=100) for i in range(6)]
        ctl.delete_batch.side_effect = [
            FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "115open throttle"),
            pb.FileOperationResult(success=True),
            pb.FileOperationResult(success=True),
        ]
        now = [100.0]
        gate = ThrottleGate(base=1.0, max_backoff=10.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
        cfg = make_cfg(self.state_path, "--yes", "--delete-batch", "6", cmd="clean")

        def fake_sleep(secs):
            now[0] += secs

        with patch("cd2_pipeline.time.sleep", side_effect=fake_sleep):
            rc = run_clean(ctl, cfg, gate=gate)
        self.assertEqual(rc, 0)
        # First call tried 6 -> throttled -> halved to 5 (floor) -> 5 + 1 = 2 more calls
        self.assertEqual(ctl.delete_batch.call_count, 3)

    def test_run_clean_auth_expired_returns_1(self):
        ctl = make_fake_ctl()
        ctl.list_dir.side_effect = AuthExpiredError("token expired")
        cfg = make_cfg(self.state_path, "--yes", cmd="clean")
        rc = run_clean(ctl, cfg)
        self.assertEqual(rc, 1)

    def test_cli_scan_batches_add_and_warns_on_both_token_and_user(self):
        fake_ctl = make_fake_ctl()
        fake_ctl.walk.return_value = iter([make_file(f"/123/f{i}.mkv", size=10) for i in range(3)])

        with patch("cd2_stream.CD2Client", return_value=fake_ctl), patch("cd2_stream.log") as mock_log:
            rc = cd2_stream.main([
                "scan",
                "--src", "/123",
                "--dest", "/115",
                "--state", self.state_path,
                "--token", "my-tok",
                "--user", "alice",
                "--password", "secret",
            ])

        self.assertEqual(rc, 0)
        fake_ctl.close.assert_called_once()
        self.assertTrue(
            any("--token" in str(c.args[0]) and "--user/--password" in str(c.args[0]) for c in mock_log.call_args_list)
        )
        man = Manifest(self.state_path, "/123", "/115")
        self.addCleanup(man.close)
        self.assertEqual(len(man.recs), 3)

    def test_run_clean_bisects_on_non_throttle_refusal_and_rpc_error(self):
        for mode in ("refusal", "rpc_error"):
            with self.subTest(mode=mode):
                ctl = make_fake_ctl()
                ctl.list_dir.return_value = [make_file(f"/115/f{i}.mkv", size=100) for i in range(4)]
                bad_path = "/115/f2.mkv"

                def fake_delete_batch(paths, permanent=False, timeout=45.0):
                    if bad_path in paths:
                        if mode == "rpc_error":
                            raise FakeRpcError(grpc.StatusCode.INTERNAL, "bad file")
                        return pb.FileOperationResult(success=False, errorMessage="file locked")
                    return pb.FileOperationResult(success=True, resultFilePaths=list(paths))

                def fake_delete_one(path, permanent=False, timeout=45.0):
                    if path == bad_path:
                        if mode == "rpc_error":
                            raise FakeRpcError(grpc.StatusCode.INTERNAL, "bad file")
                        return pb.FileOperationResult(success=False, errorMessage="file locked")
                    return pb.FileOperationResult(success=True, resultFilePaths=[path])

                ctl.delete_batch.side_effect = fake_delete_batch
                ctl.delete_one.side_effect = fake_delete_one
                cfg = make_cfg(self.state_path, "--yes", "--delete-batch", "4", cmd="clean")

                with patch("cd2_pipeline.log") as mock_log:
                    rc = run_clean(ctl, cfg)

                self.assertEqual(rc, 1)
                self.assertGreaterEqual(ctl.delete_one.call_count, 1)
                summary_logs = [str(c.args[0]) for c in mock_log.call_args_list]
                self.assertTrue(any("3 files, 1 failed" in line for line in summary_logs))

        # Session/connection error in _NON_BISECT_CODES must NOT bisect
        ctl_nb = make_fake_ctl()
        ctl_nb.list_dir.return_value = [make_file(f"/115/f{i}.mkv", size=100) for i in range(4)]
        ctl_nb.delete_batch.side_effect = FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "timeout")
        cfg_nb = make_cfg(self.state_path, "--yes", "--delete-batch", "4", cmd="clean")
        rc_nb = run_clean(ctl_nb, cfg_nb)
        self.assertEqual(rc_nb, 1)
        ctl_nb.delete_one.assert_not_called()

    def test_cli_main_converts_manifest_lock_error_to_system_exit(self):
        fake_ctl = make_fake_ctl()
        with patch("cd2_stream.CD2Client", return_value=fake_ctl), patch(
            "cd2_stream.Manifest", side_effect=ManifestLockError("manifest is locked by another instance")
        ):
            with self.assertRaises(SystemExit) as ctx:
                cd2_stream.main([
                    "scan",
                    "--src", "/123",
                    "--dest", "/115",
                    "--state", self.state_path,
                    "--token", "my-tok",
                ])

        self.assertEqual(str(ctx.exception), "manifest is locked by another instance")
        fake_ctl.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
