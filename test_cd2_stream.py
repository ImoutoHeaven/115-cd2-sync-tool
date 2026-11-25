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


class PushMessageListenerTest(PipelineTestBase):
    def test_should_wake_filters_precisely(self):
        man = self.make_man()
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path)
        pipe = Pipeline(ctl, man, cfg)

        # Initial COPY_TASK_COUNT snapshot with copyTaskCount=3, no finish -> initialize only, no wake
        self.assertFalse(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.COPY_TASK_COUNT,
                    transferTaskStatus=pb.TransferTaskStatus(copyTaskCount=3),
                )
            )
        )
        # Same copyTaskCount=3 (unrelated download tick) -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.DOWNLOADER_COUNT,
                    transferTaskStatus=pb.TransferTaskStatus(downloadCount=5, copyTaskCount=3),
                )
            )
        )
        # Transfer+Copy -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.UPLOADER_COUNT,
                    transferTaskStatus=pb.TransferTaskStatus(
                        copyTaskCount=3,
                        uploadFileStatusChanges=[
                            pb.UploadFileInfo(
                                statusEnum=pb.UploadFileInfo.Transfer,
                                operatorType=pb.UploadFileInfo.Copy,
                            )
                        ],
                    ),
                )
            )
        )
        # Finish+Mount -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.UPLOADER_COUNT,
                    transferTaskStatus=pb.TransferTaskStatus(
                        copyTaskCount=3,
                        uploadFileStatusChanges=[
                            pb.UploadFileInfo(
                                statusEnum=pb.UploadFileInfo.Finish,
                                operatorType=pb.UploadFileInfo.Mount,
                            )
                        ],
                    ),
                )
            )
        )
        # Finish+Copy on UPLOADER_COUNT -> wake!
        self.assertTrue(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.UPLOADER_COUNT,
                    transferTaskStatus=pb.TransferTaskStatus(
                        copyTaskCount=3,
                        uploadFileStatusChanges=[
                            pb.UploadFileInfo(
                                statusEnum=pb.UploadFileInfo.Finish,
                                operatorType=pb.UploadFileInfo.Copy,
                            )
                        ],
                    ),
                )
            )
        )
        # copyTaskCount delta (3 -> 2) on COPY_TASK_COUNT -> wake!
        self.assertTrue(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.COPY_TASK_COUNT,
                    transferTaskStatus=pb.TransferTaskStatus(copyTaskCount=2),
                )
            )
        )

        # FILE_SYSTEM_CHANGE(CREATE) regular file under dest_root -> wake!
        self.assertTrue(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.FILE_SYSTEM_CHANGE,
                    fileSystemChange=pb.FileSystemChange(
                        changeType=pb.FileSystemChange.CREATE,
                        isDirectory=False,
                        path="/115/landed.mkv",
                    ),
                )
            )
        )
        # FILE_SYSTEM_CHANGE(CREATE) directory under dest_root -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.FILE_SYSTEM_CHANGE,
                    fileSystemChange=pb.FileSystemChange(
                        changeType=pb.FileSystemChange.CREATE,
                        isDirectory=True,
                        path="/115/subdir",
                    ),
                )
            )
        )
        # FILE_SYSTEM_CHANGE(DELETE) under dest_root -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.FILE_SYSTEM_CHANGE,
                    fileSystemChange=pb.FileSystemChange(
                        changeType=pb.FileSystemChange.DELETE,
                        isDirectory=False,
                        path="/115/landed.mkv",
                    ),
                )
            )
        )
        # FILE_SYSTEM_CHANGE(CREATE) outside dest_root (or prefix collision /1150/...) -> no wake
        for other_path in ("/123/a.mkv", "/1150/a.mkv"):
            self.assertFalse(
                pipe._should_wake(
                    pb.CloudDrivePushMessage(
                        messageType=pb.CloudDrivePushMessage.FILE_SYSTEM_CHANGE,
                        fileSystemChange=pb.FileSystemChange(
                            changeType=pb.FileSystemChange.CREATE,
                            isDirectory=False,
                            path=other_path,
                        ),
                    )
                )
            )

        # MOUNT_POINT_CHANGE and CLOUD_API_CHANGE invalidate dest_ready cache and wake
        for mtype in (
            pb.CloudDrivePushMessage.MOUNT_POINT_CHANGE,
            pb.CloudDrivePushMessage.CLOUD_API_CHANGE,
        ):
            pipe._dest_checked_at = 1234.5
            self.assertTrue(pipe._should_wake(pb.CloudDrivePushMessage(messageType=mtype)))
            self.assertEqual(pipe._dest_checked_at, 0.0)

        # FORCE_EXIT logs message and does not wake
        with patch("cd2_pipeline.log") as mock_log:
            self.assertFalse(
                pipe._should_wake(
                    pb.CloudDrivePushMessage(
                        messageType=pb.CloudDrivePushMessage.FORCE_EXIT,
                        exitedMessage=pb.ExitedMessage(
                            exitReason=pb.ExitedMessage.SHUTDOWN,
                            message="server shutting down",
                        ),
                    )
                )
            )
            self.assertTrue(
                any("FORCE_EXIT" in str(c.args[0]) and "server shutting down" in str(c.args[0]) for c in mock_log.call_args_list)
            )

        # Unrelated message types -> no wake
        self.assertFalse(
            pipe._should_wake(
                pb.CloudDrivePushMessage(messageType=pb.CloudDrivePushMessage.LOG_MESSAGE)
            )
        )

    def test_listen_cancel_on_stop_auth_expired_and_unimplemented_exits(self):
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
                yield pb.CloudDrivePushMessage(
                    messageType=pb.CloudDrivePushMessage.COPY_TASK_COUNT,
                    transferTaskStatus=pb.TransferTaskStatus(
                        copyTaskCount=1,
                        uploadFileStatusChanges=[
                            pb.UploadFileInfo(
                                statusEnum=pb.UploadFileInfo.Finish,
                                operatorType=pb.UploadFileInfo.Copy,
                            )
                        ],
                    ),
                )
                cancelled.wait(timeout=2.0)

            def cancel(self):
                cancelled.set()

        ctl.push_messages.return_value = FakeStream()
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
        ctl.push_messages.side_effect = AuthExpiredError("expired")
        pipe2.listen()
        self.assertTrue(pipe2.stop.is_set())
        self.assertIsInstance(pipe2._auth_error, AuthExpiredError)

        # UNIMPLEMENTED logs once and exits listener immediately without retrying or stopping pipeline
        ctl3 = make_fake_ctl()
        ctl3.push_messages.side_effect = FakeRpcError(
            grpc.StatusCode.UNIMPLEMENTED, "not implemented"
        )
        pipe3 = Pipeline(ctl3, man, cfg)
        with patch("cd2_pipeline.log") as mock_log:
            pipe3.listen()
        self.assertEqual(ctl3.push_messages.call_count, 1)
        self.assertFalse(pipe3.stop.is_set())
        self.assertIsNone(pipe3._auth_error)
        self.assertTrue(
            any("unimplemented" in str(c.args[0]).lower() for c in mock_log.call_args_list)
        )

    def test_mount_or_cloud_api_change_forces_slow_step_within_sweep_window(self):
        for mtype in (
            pb.CloudDrivePushMessage.MOUNT_POINT_CHANGE,
            pb.CloudDrivePushMessage.CLOUD_API_CHANGE,
        ):
            with self.subTest(message_type=mtype):
                state = os.path.join(self.tmp.name, f"m_force_slow_{mtype}.jsonl")
                man = Manifest(state, "/123", "/115")
                self.addCleanup(man.close)
                r = man.add(make_file("/123/a.mkv", size=100))
                ctl = make_fake_ctl()
                cfg = make_cfg(state, "--sweep", "60", "--delete-interval", "60")
                pipe = Pipeline(ctl, man, cfg)
                pipe.DEBOUNCE_SECS = 0.01

                first_step_done = threading.Event()
                cancelled = threading.Event()

                class FakeStream:
                    def __iter__(self):
                        first_step_done.wait(timeout=2.0)
                        yield pb.CloudDrivePushMessage(messageType=mtype)
                        cancelled.wait(timeout=2.0)

                    def cancel(self):
                        cancelled.set()

                ctl.push_messages.return_value = FakeStream()

                orig_slow = pipe.slow_step
                orig_dest_ready = pipe.dest_ready
                slow_calls: list[bool] = []
                dest_ready_forces: list[bool] = []

                def tracked_dest_ready(force: bool = False):
                    dest_ready_forces.append(force)
                    return orig_dest_ready(force=force)

                def tracked_slow_step(force_dest: bool = False):
                    slow_calls.append(force_dest)
                    orig_slow(force_dest=force_dest)
                    if len(slow_calls) == 1:
                        first_step_done.set()
                    else:
                        man.set(r, "dropped")

                with patch.object(pipe, "dest_ready", side_effect=tracked_dest_ready), patch.object(
                    pipe, "slow_step", side_effect=tracked_slow_step
                ), patch.object(pipe, "fast_step") as mock_fast:
                    t0 = time.monotonic()
                    rc = pipe.run()
                    elapsed = time.monotonic() - t0

                self.assertEqual(rc, 0)
                self.assertLess(elapsed, 2.0)
                mock_fast.assert_not_called()
                self.assertEqual(slow_calls, [False, True])
                self.assertEqual(dest_ready_forces, [False, True])
                self.assertFalse(pipe._slow_requested.is_set())


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

        def fake_slow_step(force_dest: bool = False):
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


class QuotaAndRapidUploadTest(PipelineTestBase):
    @staticmethod
    def _push_upload(dest_path: str, status_enum: int, transfered: int = 0, size: int = 100):
        return pb.CloudDrivePushMessage(
            messageType=pb.CloudDrivePushMessage.UPLOADER_COUNT,
            transferTaskStatus=pb.TransferTaskStatus(
                copyTaskCount=1,
                uploadFileStatusChanges=[
                    pb.UploadFileInfo(
                        destPath=dest_path,
                        size=size,
                        transferedBytes=transfered,
                        operatorType=pb.UploadFileInfo.Copy,
                        statusEnum=status_enum,
                    )
                ],
            ),
        )

    def test_reserve_settle_hard_cap_and_oversized_file_parked(self):
        man = self.make_man()
        f_huge = man.add(make_file("/123/huge.mkv", size=1200))
        f1 = man.add(make_file("/123/a.mkv", size=600))
        f2 = man.add(make_file("/123/b.mkv", size=500))
        f3 = man.add(make_file("/123/c.mkv", size=400))

        ctl = make_fake_ctl()
        cfg = make_cfg(
            self.state_path,
            "--daily-limit", "1000",
            "--max-inflight-bytes", "0",
            "--quota-fit", "first",
        )
        t_now = 1_700_000_000.0
        pipe = Pipeline(ctl, man, cfg, clock=lambda: t_now)
        pipe.cloud = "115|acc1"

        pipe.submit_pending([])
        # huge.mkv > 1000 parked immediately; a (600) + c (400) = 1000 <= 1000 (First-Fit skips b=500)
        self.assertEqual(f_huge["state"], "failed")
        self.assertEqual(f_huge["quota_bytes"], 0)
        self.assertEqual(f1["state"], "submitted")
        self.assertEqual(f2["state"], "pending")
        self.assertEqual(f3["state"], "submitted")
        qs = pipe.quota_status("115|acc1", t_now)
        self.assertEqual(qs.confirmed_real, 0)
        self.assertEqual(qs.reserved_inflight, 1000)

    def test_quota_fit_strict_blocks_behind_large_file(self):
        man = self.make_man()
        f1 = man.add(make_file("/123/a.mkv", size=600))
        f2 = man.add(make_file("/123/b.mkv", size=500))
        f3 = man.add(make_file("/123/c.mkv", size=400))

        ctl = make_fake_ctl()
        cfg = make_cfg(
            self.state_path,
            "--daily-limit", "1000",
            "--max-inflight-bytes", "0",
            "--quota-fit", "strict",
        )
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 1_700_000_000.0)
        pipe.cloud = "115|acc1"

        pipe.submit_pending([])
        self.assertEqual(f1["state"], "submitted")
        self.assertEqual(f2["state"], "pending")
        self.assertEqual(f3["state"], "pending")

    def test_rapid_free_vs_real_vs_unknown_fail_closed_and_submitted_mode(self):
        man = self.make_man()
        r_rapid = man.add(make_file("/123/rapid.mkv", size=500))
        r_real = man.add(make_file("/123/real.mkv", size=500))
        r_unk = man.add(make_file("/123/unk.mkv", size=500))
        r_next = man.add(make_file("/123/next.mkv", size=500))

        ctl = make_fake_ctl()
        cfg = make_cfg(
            self.state_path,
            "--daily-limit", "1000",
            "--max-inflight-bytes", "0",
        )
        t_now = 1_700_000_000.0
        pipe = Pipeline(ctl, man, cfg, clock=lambda: t_now)
        pipe.cloud = "115|acc1"

        # Submit first 2 files (1000 B reserved = full cap)
        pipe.submit_pending([])
        self.assertEqual(r_rapid["state"], "submitted")
        self.assertEqual(r_real["state"], "submitted")
        self.assertEqual(r_unk["state"], "pending")

        # rapid.mkv finishes without Transfer(3) and transferedBytes == 0
        pipe._should_wake(self._push_upload("/115/rapid.mkv", pb.UploadFileInfo.Finish, transfered=0, size=500))
        # real.mkv enters Transfer(3) then Finish(5)
        pipe._should_wake(self._push_upload("/115/real.mkv", pb.UploadFileInfo.Transfer, transfered=200, size=500))
        pipe._should_wake(self._push_upload("/115/real.mkv", pb.UploadFileInfo.Finish, transfered=500, size=500))

        tasks = [
            make_task("/123/rapid.mkv", total_bytes=500, uploaded_bytes=500),
            make_task("/123/real.mkv", total_bytes=500, uploaded_bytes=500),
        ]
        pipe.harvest_completed(tasks)
        # Before dir_cache verification in clear_landed, both are landed & unsettled (still reserved)
        self.assertEqual(r_rapid["state"], "landed")
        self.assertEqual(r_rapid["upload_kind"], "rapid")
        self.assertIsNone(r_rapid["quota_bytes"])
        self.assertEqual(pipe.quota_status("115|acc1", t_now).reserved_inflight, 1000)

        # Verify local dir_cache hit before DeleteFiles
        ctl.find.side_effect = lambda p: make_file(p, size=500)
        pipe.clear_landed()
        self.assertEqual(r_rapid["state"], "dropped")
        self.assertEqual(r_rapid["quota_bytes"], 0)
        self.assertEqual(r_real["state"], "dropped")
        self.assertEqual(r_real["quota_bytes"], 500)
        qs = pipe.quota_status("115|acc1", t_now)
        self.assertEqual(qs.confirmed_real, 500)
        self.assertEqual(qs.reserved_inflight, 0)
        self.assertEqual(qs.rapid_saved, 500)

        # Now 500 B is free: unk.mkv submits without any PushMessage -> Fail-Closed counts 500 B
        ctl.find.side_effect = None
        ctl.find.return_value = None
        pipe.submit_pending([])
        self.assertEqual(r_unk["state"], "submitted")
        self.assertEqual(r_next["state"], "pending")

        pipe.harvest_completed([make_task("/123/unk.mkv", total_bytes=500, uploaded_bytes=500)])
        ctl.find.return_value = make_file("/115/unk.mkv", size=500)
        pipe.clear_landed()
        self.assertEqual(r_unk["upload_kind"], "unknown")
        self.assertEqual(r_unk["quota_bytes"], 500)
        self.assertEqual(pipe.quota_status("115|acc1", t_now).confirmed_real, 1000)

        # Quota now full: r_next cannot submit
        ctl.find.return_value = None
        pipe.submit_pending([])
        self.assertEqual(r_next["state"], "pending")

        # Submitted mode charges even rapid uploads
        cfg_sub = make_cfg(self.state_path, "--daily-limit", "1000", "--quota-mode", "submitted")
        pipe_sub = Pipeline(ctl, man, cfg_sub, clock=lambda: t_now)
        self.assertEqual(pipe_sub._calc_quota_bytes(r_rapid, "rapid"), 500)

    def test_failed_vanished_rejected_release_reservation_zero_quota(self):
        man = self.make_man()
        r1 = man.add(make_file("/123/rej.mkv", size=600))
        r2 = man.add(make_file("/123/van.mkv", size=600))

        ctl = make_fake_ctl()
        ctl.submit_one.side_effect = [(False, "rejected"), (True, "")]
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--max-inflight-bytes", "0", "--stall", "10")
        clock = [1000.0]
        pipe = Pipeline(ctl, man, cfg, clock=lambda: clock[0])
        pipe.cloud = "115|acc1"

        # r1 rejected -> back to pending with quota_bytes=0
        pipe.submit_pending([])
        self.assertEqual(r1["state"], "pending")
        self.assertEqual(r1["quota_bytes"], 0)
        self.assertEqual(pipe.quota_status("115|acc1", clock[0]).reserved_inflight, 0)

        # Submit r2, then vanish past stall -> requeued with quota_bytes=0
        ctl.submit_one.side_effect = None
        ctl.submit_one.return_value = (True, "")
        man.set(r1, "dropped", quota_bytes=0, cloud="115|acc1", landed_at=900)
        pipe.submit_pending([])
        self.assertEqual(r2["state"], "submitted")
        self.assertEqual(pipe.quota_status("115|acc1", clock[0]).reserved_inflight, 600)

        clock[0] = 1020.0
        pipe.reconcile(set())
        self.assertEqual(r2["state"], "pending")
        self.assertEqual(r2["quota_bytes"], 0)
        self.assertEqual(pipe.quota_status("115|acc1", clock[0]).reserved_inflight, 0)

    def test_pre_gc_dir_cache_four_branches_and_delete_none(self):
        # Branch 1 (--delete none): must call FindFileByPath before marking kept & settling
        man = self.make_man()
        r_kept_rapid = man.add(make_file("/123/kept_rapid.mkv", size=300))
        man.set(r_kept_rapid, "landed", cloud="115|acc1", dest_root="/115", landed_at=100, upload_kind="rapid", quota_bytes=None)
        ctl = make_fake_ctl()
        ctl.find.return_value = make_file("/115/kept_rapid.mkv", size=300)
        cfg_none = make_cfg(self.state_path, "--delete", "none")
        pipe_none = Pipeline(ctl, man, cfg_none, clock=lambda: 105.0)
        pipe_none.cloud = "115|acc1"
        pipe_none.clear_landed()
        ctl.find.assert_called_once_with("/115/kept_rapid.mkv")
        ctl.delete_batch.assert_not_called()
        self.assertEqual(r_kept_rapid["state"], "kept")
        self.assertEqual(r_kept_rapid["quota_bytes"], 0)

        # Branch 2 (stranded mismatch): rapid -> 0, real -> size
        r_str_rapid = man.add(make_file("/123/str_rapid.mkv", size=300))
        r_str_real = man.add(make_file("/123/str_real.mkv", size=400))
        man.set_batch([
            (r_str_rapid, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "rapid", "quota_bytes": None}),
            (r_str_real, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "real", "quota_bytes": None}),
        ])
        ctl2 = make_fake_ctl()
        ctl2.find.side_effect = lambda p: make_file(p, size=999)  # size mismatch
        pipe = Pipeline(ctl2, man, make_cfg(self.state_path, "--missing-grace", "10"), clock=lambda: 120.0)
        pipe.cloud = "115|acc1"
        pipe.clear_landed()
        self.assertEqual(r_str_rapid["state"], "stranded")
        self.assertEqual(r_str_rapid["quota_bytes"], 0)
        self.assertEqual(r_str_real["state"], "stranded")
        self.assertEqual(r_str_real["quota_bytes"], 400)

        # Branch 3 (NOT_FOUND past missing-grace -> gone already): real -> size, unknown/rapid -> 0
        r_gone_unk = man.add(make_file("/123/gone_unk.mkv", size=300))
        r_gone_real = man.add(make_file("/123/gone_real.mkv", size=400))
        man.set_batch([
            (r_gone_unk, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "unknown", "quota_bytes": None}),
            (r_gone_real, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "real", "quota_bytes": None}),
        ])
        ctl2.find.side_effect = None
        ctl2.find.return_value = None
        pipe.clear_landed()  # miss 1
        self.assertEqual(r_gone_unk["state"], "landed")
        pipe.clear_landed()  # miss 2, elapsed 20 >= 10
        self.assertEqual(r_gone_unk["state"], "dropped")
        self.assertEqual(r_gone_unk["quota_bytes"], 0)
        self.assertEqual(r_gone_real["state"], "dropped")
        self.assertEqual(r_gone_real["quota_bytes"], 400)

        # Branch 4 (cross-account landed guard): dropped without delete, settles quota on original account
        r_old = man.add(make_file("/123/old_acc.mkv", size=500))
        man.set(r_old, "landed", cloud="115|acc0", dest_root="/115", landed_at=100, upload_kind="real", quota_bytes=None)
        pipe.clear_landed()
        self.assertEqual(r_old["state"], "dropped")
        self.assertEqual(r_old["quota_bytes"], 500)
        self.assertEqual(pipe.quota_status("115|acc0", 120.0).confirmed_real, 500)
        self.assertEqual(pipe.quota_status("115|acc1", 120.0).confirmed_real, 800)

    def test_utc8_midnight_reset_and_rolling_24h_window(self):
        import datetime
        from cd2_core import UTC8

        t_before = datetime.datetime(2026, 6, 1, 23, 59, 50, tzinfo=UTC8).timestamp()
        t_after = datetime.datetime(2026, 6, 2, 0, 0, 5, tzinfo=UTC8).timestamp()
        clock = [t_before]

        man = self.make_man()
        r1 = man.add(make_file("/123/d1.mkv", size=1000))
        r2 = man.add(make_file("/123/d2.mkv", size=500))
        man.set(r1, "dropped", cloud="115|acc1", dest_root="/115", landed_at=int(t_before - 10), upload_kind="real", quota_bytes=1000)

        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--quota-window", "daily-utc8")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: clock[0])
        pipe.cloud = "115|acc1"

        pipe.submit_pending([])
        self.assertEqual(r2["state"], "pending")
        expected_midnight = datetime.datetime(2026, 6, 2, 0, 0, 0, tzinfo=UTC8).timestamp()
        self.assertEqual(pipe.next_quota_wakeup("115|acc1", clock[0]), expected_midnight)

        # Cross UTC+8 midnight -> quota resets -> r2 submits
        clock[0] = t_after
        pipe.submit_pending([])
        self.assertEqual(r2["state"], "submitted")

        # Rolling 24h window test
        man2 = Manifest(os.path.join(self.tmp.name, "m2.jsonl"), "/123", "/115")
        self.addCleanup(man2.close)
        a = man2.add(make_file("/123/a.mkv", size=600))
        b = man2.add(make_file("/123/b.mkv", size=400))
        c = man2.add(make_file("/123/c.mkv", size=500))
        man2.set_batch([
            (a, "dropped", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 1000, "upload_kind": "real", "quota_bytes": 600}),
            (b, "dropped", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 2000, "upload_kind": "real", "quota_bytes": 400}),
        ])
        rclock = [5000.0]
        cfg_r = make_cfg(man2.path, "--daily-limit", "1000", "--quota-window", "rolling-24h")
        pipe_r = Pipeline(ctl, man2, cfg_r, clock=lambda: rclock[0])
        pipe_r.cloud = "115|acc1"
        self.assertEqual(pipe_r.next_quota_wakeup("115|acc1", rclock[0]), 1000.0 + 86400.0)
        pipe_r.submit_pending([])
        self.assertEqual(c["state"], "pending")
        rclock[0] = 1000.0 + 86400.0 + 1.0
        pipe_r.submit_pending([])
        self.assertEqual(c["state"], "submitted")

    def test_account_swap_independent_quota_and_no_reupload(self):
        man = self.make_man()
        r_done = man.add(make_file("/123/done.mkv", size=1000))
        r_pend = man.add(make_file("/123/pend.mkv", size=500))
        man.set(r_done, "dropped", cloud="115|accA", dest_root="/115", landed_at=1000, upload_kind="real", quota_bytes=1000)

        ctl = make_fake_ctl()
        ctl.cloud_of.return_value = (make_cloud("115", "accA"), None, False)
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--max-inflight-bytes", "0")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 1010.0)

        pipe.slow_step(force_dest=True)
        self.assertEqual(pipe.cloud, "115|accA")
        self.assertEqual(r_pend["state"], "pending")  # accA is full

        # Swap to accB -> independent quota -> r_pend submits immediately, r_done never re-uploaded
        ctl.cloud_of.return_value = (make_cloud("115", "accB"), None, False)
        pipe.slow_step(force_dest=True)
        self.assertEqual(pipe.cloud, "115|accB")
        self.assertEqual(r_done["state"], "dropped")
        self.assertEqual(r_pend["state"], "submitted")
        self.assertEqual(r_pend["cloud"], "115|accB")
        ctl.submit_one.assert_called_once_with("/123/pend.mkv", "/115")

    def test_max_inflight_bytes_cap_and_solo_large_file(self):
        man = self.make_man()
        f1 = man.add(make_file("/123/f1.mkv", size=100))
        f2 = man.add(make_file("/123/f2.mkv", size=100))
        f3 = man.add(make_file("/123/f3.mkv", size=50))

        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--max-inflight-bytes", "150", "--quota-fit", "first")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 1000.0)
        pipe.cloud = "115|acc1"

        pipe.submit_pending([])
        # f1 (100) + f3 (50) = 150 <= 150; f2 (100) skipped because 100+100 > 150
        self.assertEqual(f1["state"], "submitted")
        self.assertEqual(f2["state"], "pending")
        self.assertEqual(f3["state"], "submitted")

        # Solo file > max_inflight_bytes (but <= daily_limit) still runs when reserved == 0
        man2 = Manifest(os.path.join(self.tmp.name, "solo.jsonl"), "/123", "/115")
        self.addCleanup(man2.close)
        f_solo = man2.add(make_file("/123/solo.mkv", size=300))
        pipe2 = Pipeline(ctl, man2, make_cfg(man2.path, "--daily-limit", "1000", "--max-inflight-bytes", "150"), clock=lambda: 1000.0)
        pipe2.cloud = "115|acc1"
        pipe2.submit_pending([])
        self.assertEqual(f_solo["state"], "submitted")

    def test_crash_restart_restores_quota_from_manifest(self):
        t_now = 1_700_000_000.0
        with Manifest(self.state_path, "/123", "/115") as man:
            r1 = man.add(make_file("/123/a.mkv", size=600))
            r2 = man.add(make_file("/123/b.mkv", size=300))
            r3 = man.add(make_file("/123/c.mkv", size=200))
            man.set_batch([
                (r1, "dropped", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": int(t_now - 60), "upload_kind": "real", "quota_bytes": 600}),
                (r2, "submitted", {"cloud": "115|acc1", "dest_root": "/115", "submitted_at": int(t_now - 30), "upload_kind": "", "quota_bytes": None}),
            ])

        # Reopen manifest in a fresh Pipeline (simulating crash recovery)
        man2 = self.make_man()
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--max-inflight-bytes", "0")
        pipe = Pipeline(ctl, man2, cfg, clock=lambda: t_now)
        pipe.cloud = "115|acc1"

        qs = pipe.quota_status("115|acc1", t_now)
        self.assertEqual(qs.confirmed_real, 600)
        self.assertEqual(qs.reserved_inflight, 300)

        # Remaining is 100 B, so c.mkv (200 B) must NOT be submitted after restart
        pipe.submit_pending([])
        self.assertEqual(man2.recs["c.mkv"]["state"], "pending")

    def test_account_swap_and_dest_change_settle_unsettled_landed_before_switchback(self):
        man = self.make_man()
        r_real = man.add(make_file("/123/real.mkv", size=600))
        r_unk = man.add(make_file("/123/unk.mkv", size=400))
        r_rapid = man.add(make_file("/123/rapid.mkv", size=500))
        r_next = man.add(make_file("/123/next.mkv", size=200))
        man.set_batch([
            (r_real, "landed", {"cloud": "115|accA", "dest_root": "/115", "landed_at": 1000, "upload_kind": "real", "quota_bytes": None}),
            (r_unk, "landed", {"cloud": "115|accA", "dest_root": "/115", "landed_at": 1000, "upload_kind": "unknown", "quota_bytes": None}),
            (r_rapid, "landed", {"cloud": "115|accA", "dest_root": "/115", "landed_at": 1000, "upload_kind": "rapid", "quota_bytes": None}),
        ])
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--max-inflight-bytes", "0")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 1010.0)
        pipe.cloud = "115|accA"
        pipe._should_wake(self._push_upload("/115/real.mkv", pb.UploadFileInfo.Transfer, transfered=100, size=600))

        # Swap to accB: real(600) + unknown(400) settled on accA, rapid(500) -> 0, _upload_obs cleared
        pipe.rotated("115|accB")
        pipe.cloud = "115|accB"
        self.assertEqual(r_real["quota_bytes"], 600)
        self.assertEqual(r_unk["quota_bytes"], 400)
        self.assertEqual(r_rapid["quota_bytes"], 0)
        self.assertEqual(pipe._upload_obs, {})
        self.assertEqual(pipe.quota_status("115|accA", 1010.0).confirmed_real, 1000)
        self.assertEqual(pipe.quota_status("115|accB", 1010.0).confirmed_real, 0)

        # Switch back to accA: accA is still at 1000/1000 so r_next cannot bypass daily_limit
        pipe.cloud = "115|accA"
        pipe.submit_pending([])
        self.assertEqual(r_next["state"], "pending")

        # Also verify dest_root change in clear_landed settles quota on original account
        man2 = Manifest(os.path.join(self.tmp.name, "dest_change.jsonl"), "/123", "/115_new")
        self.addCleanup(man2.close)
        r_old_dest = man2.add(make_file("/123/old_dest.mkv", size=700))
        man2.set(r_old_dest, "landed", cloud="115|accA", dest_root="/115_old", landed_at=1000, upload_kind="real", quota_bytes=None)
        pipe2 = Pipeline(ctl, man2, make_cfg(man2.path, "--dest", "/115_new", "--daily-limit", "1000"), clock=lambda: 1010.0)
        pipe2.cloud = "115|accA"
        pipe2.clear_landed()
        self.assertEqual(r_old_dest["state"], "dropped")
        self.assertEqual(r_old_dest["quota_bytes"], 700)
        self.assertEqual(pipe2.quota_status("115|accA", 1010.0).confirmed_real, 700)

    def test_observe_upload_changes_filters_scope_operator_and_non_transfer_states(self):
        man = self.make_man()
        ctl = make_fake_ctl()
        pipe = Pipeline(ctl, man, make_cfg(self.state_path), clock=lambda: 1000.0)
        changes = [
            pb.UploadFileInfo(destPath="/other/a.mkv", size=100, transferedBytes=50, operatorType=pb.UploadFileInfo.Copy, statusEnum=pb.UploadFileInfo.Transfer),
            pb.UploadFileInfo(destPath="/115/sub/b.mkv", size=100, transferedBytes=0, operatorType=pb.UploadFileInfo.Copy, statusEnum=pb.UploadFileInfo.Finish),
            pb.UploadFileInfo(destPath="/1152/c.mkv", size=100, transferedBytes=0, operatorType=pb.UploadFileInfo.Copy, statusEnum=pb.UploadFileInfo.Finish),
            pb.UploadFileInfo(destPath="/115/d.mkv", size=100, transferedBytes=100, operatorType=pb.UploadFileInfo.Mount, statusEnum=pb.UploadFileInfo.Transfer),
            pb.UploadFileInfo(destPath="/115/e.mkv", size=100, transferedBytes=0, operatorType=pb.UploadFileInfo.Copy, statusEnum=pb.UploadFileInfo.WaitforPreprocessing),
            pb.UploadFileInfo(destPath="/115/f.mkv", size=100, transferedBytes=0, operatorType=pb.UploadFileInfo.Copy, statusEnum=pb.UploadFileInfo.Inqueue),
            pb.UploadFileInfo(destPath="/115/g.mkv", size=100, transferedBytes=0, operatorType=pb.UploadFileInfo.Copy, statusEnum=pb.UploadFileInfo.Cancelled),
            pb.UploadFileInfo(destPath="/115/h.mkv", size=100, transferedBytes=0, operatorType=pb.UploadFileInfo.Copy, statusEnum=pb.UploadFileInfo.Error),
        ]
        pipe._observe_upload_changes(changes)
        self.assertEqual(pipe._upload_obs, {})

    def test_windows_backslash_destpath_recognizes_rapid_upload(self):
        man = self.make_man()
        r1 = man.add(make_file("/123/win1.mkv", size=500))
        r2 = man.add(make_file("/123/win2.mkv", size=300))
        ctl = make_fake_ctl()
        pipe = Pipeline(ctl, man, make_cfg(self.state_path, "--daily-limit", "1000"), clock=lambda: 1000.0)
        pipe.cloud = "115|acc1"
        pipe.submit_pending([])

        pipe._should_wake(self._push_upload("\\115\\win1.mkv", pb.UploadFileInfo.Finish, transfered=0, size=500))
        pipe._should_wake(self._push_upload("115\\win2.mkv", pb.UploadFileInfo.Finish, transfered=0, size=300))
        pipe.harvest_completed([
            make_task("/123/win1.mkv", total_bytes=500, uploaded_bytes=500),
            make_task("/123/win2.mkv", total_bytes=300, uploaded_bytes=300),
        ])
        ctl.find.side_effect = lambda p: make_file(p, size=500 if p.endswith("win1.mkv") else 300)
        pipe.clear_landed()
        self.assertEqual(r1["upload_kind"], "rapid")
        self.assertEqual(r1["quota_bytes"], 0)
        self.assertEqual(r2["upload_kind"], "rapid")
        self.assertEqual(r2["quota_bytes"], 0)

    def test_same_basename_consecutive_rounds_delayed_finish_size_mismatch_fails_closed(self):
        man = self.make_man()
        r1 = man.add(make_file("/123/d1/dup.mkv", size=100))
        r2 = man.add(make_file("/123/d2/dup.mkv", size=500))
        ctl = make_fake_ctl()
        pipe = Pipeline(ctl, man, make_cfg(self.state_path, "--daily-limit", "1000"), clock=lambda: 1000.0)
        pipe.cloud = "115|acc1"

        # Round 1 completes and clears
        pipe.submit_pending([])
        self.assertEqual(r1["state"], "submitted")
        self.assertEqual(r2["state"], "pending")
        pipe._should_wake(self._push_upload("/115/dup.mkv", pb.UploadFileInfo.Finish, transfered=0, size=100))
        pipe.harvest_completed([make_task("/123/d1/dup.mkv", total_bytes=100, uploaded_bytes=100)])
        ctl.find.return_value = make_file("/115/dup.mkv", size=100)
        pipe.clear_landed()
        self.assertEqual(r1["quota_bytes"], 0)

        # Round 2 submits; delayed Finish(0, size=100) from Round 1 arrives
        ctl.find.return_value = None
        pipe.submit_pending([])
        self.assertEqual(r2["state"], "submitted")
        pipe._should_wake(self._push_upload("/115/dup.mkv", pb.UploadFileInfo.Finish, transfered=0, size=100))
        pipe.harvest_completed([make_task("/123/d2/dup.mkv", total_bytes=500, uploaded_bytes=500)])
        ctl.find.return_value = make_file("/115/dup.mkv", size=500)
        pipe.clear_landed()
        # Size mismatch (100 != 500) must reject rapid and Fail-Closed charge 500
        self.assertEqual(r2["upload_kind"], "unknown")
        self.assertEqual(r2["quota_bytes"], 500)

    def test_rolling_24h_exact_boundary_releases_without_spin(self):
        man = self.make_man()
        a = man.add(make_file("/123/a.mkv", size=600))
        b = man.add(make_file("/123/b.mkv", size=400))
        c = man.add(make_file("/123/c.mkv", size=500))
        man.set_batch([
            (a, "dropped", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 1000, "upload_kind": "real", "quota_bytes": 600}),
            (b, "dropped", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 2000, "upload_kind": "real", "quota_bytes": 400}),
        ])
        ctl = make_fake_ctl()
        rclock = [1000.0 + 86400.0 - 0.5]
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--quota-window", "rolling-24h")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: rclock[0])
        pipe.cloud = "115|acc1"

        self.assertEqual(pipe.next_quota_wakeup("115|acc1", rclock[0]), 1000.0 + 86400.0)
        # At exact wakeup boundary ev_ts + 86400.0, a slides out immediately (no spin)
        rclock[0] = 1000.0 + 86400.0
        self.assertEqual(pipe.quota_status("115|acc1", rclock[0]).confirmed_real, 400)
        self.assertIsNone(pipe.next_quota_wakeup("115|acc1", rclock[0]))
        pipe.submit_pending([])
        self.assertEqual(c["state"], "submitted")

    def test_restart_due_parks_oversized_enforces_max_inflight_and_computes_wakeup(self):
        man = self.make_man()
        r_huge = man.add(make_file("/123/huge.mkv", size=1200))
        r_cross = man.add(make_file("/123/cross.mkv", size=300))
        r_active = man.add(make_file("/123/active.mkv", size=200))
        r_done = man.add(make_file("/123/done.mkv", size=800))
        man.set_batch([
            (r_huge, "submitted", {"cloud": "115|acc1", "dest_root": "/115", "quota_bytes": None}),
            (r_cross, "submitted", {"cloud": "115|acc0", "dest_root": "/115", "quota_bytes": None}),
            (r_active, "submitted", {"cloud": "115|acc1", "dest_root": "/115", "quota_bytes": None}),
            (r_done, "dropped", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 1000, "upload_kind": "real", "quota_bytes": 800}),
        ])
        ctl = make_fake_ctl()
        cfg = make_cfg(
            self.state_path,
            "--daily-limit", "1000",
            "--max-inflight-bytes", "350",
            "--quota-window", "rolling-24h",
        )
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 2000.0)
        pipe.cloud = "115|acc1"

        # Case 1: oversized parked & cross-account blocked by daily_limit -> next_quota_wakeup works even with no pending
        pipe.due = [(r_huge, "err", "/115"), (r_cross, "err", "/115")]
        pipe.restart_due()
        self.assertEqual(r_huge["state"], "failed")
        self.assertEqual(r_huge["quota_bytes"], 0)
        ctl.remove_tasks.assert_called_once_with(["/123/huge.mkv:/115"])
        ctl.restart_task.assert_not_called()
        self.assertEqual(pipe.next_quota_wakeup("115|acc1", 2000.0), 1000.0 + 86400.0)

        # Case 2: daily_limit has room (move r_done out of window), but cross-account extra=300 exceeds max_inflight_bytes (200+300 > 350)
        man.set(r_done, "dropped", landed_at=100, quota_bytes=0)
        pipe.due = [(r_cross, "err", "/115")]
        pipe.restart_due()
        ctl.restart_task.assert_not_called()

    def test_gone_already_submitted_mode_charges_size(self):
        man = self.make_man()
        r_unk = man.add(make_file("/123/gone_unk.mkv", size=300))
        r_rapid = man.add(make_file("/123/gone_rapid.mkv", size=200))
        man.set_batch([
            (r_unk, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "unknown", "quota_bytes": None}),
            (r_rapid, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "rapid", "quota_bytes": None}),
        ])
        ctl = make_fake_ctl()
        ctl.find.return_value = None
        cfg = make_cfg(self.state_path, "--missing-grace", "10", "--quota-mode", "submitted")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 120.0)
        pipe.cloud = "115|acc1"
        pipe.clear_landed()
        pipe.clear_landed()
        self.assertEqual(r_unk["state"], "dropped")
        self.assertEqual(r_unk["quota_bytes"], 300)
        self.assertEqual(r_rapid["state"], "dropped")
        self.assertEqual(r_rapid["quota_bytes"], 200)

    def test_record_clear_failure_stall_stranded_clears_obs_and_persists_quota(self):
        man = self.make_man()
        r_unk = man.add(make_file("/123/fail_unk.mkv", size=400))
        r_rapid = man.add(make_file("/123/fail_rapid.mkv", size=300))
        man.set_batch([
            (r_unk, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "unknown", "quota_bytes": None}),
            (r_rapid, "landed", {"cloud": "115|acc1", "dest_root": "/115", "landed_at": 100, "upload_kind": "rapid", "quota_bytes": None}),
        ])
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--stall", "10")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 100.0)
        pipe.cloud = "115|acc1"
        pipe._should_wake(self._push_upload("/115/fail_unk.mkv", pb.UploadFileInfo.Transfer, transfered=50, size=400))
        pipe._should_wake(self._push_upload("/115/fail_rapid.mkv", pb.UploadFileInfo.Finish, transfered=0, size=300))

        pipe._record_clear_failure(r_unk, 100.0, "io err")
        pipe._record_clear_failure(r_rapid, 100.0, "io err")
        pipe._record_clear_failure(r_unk, 115.0, "io err")
        pipe._record_clear_failure(r_rapid, 115.0, "io err")

        self.assertEqual(r_unk["state"], "stranded")
        self.assertEqual(r_unk["quota_bytes"], 400)
        self.assertEqual(r_rapid["state"], "stranded")
        self.assertEqual(r_rapid["quota_bytes"], 0)
        self.assertEqual(pipe._upload_obs, {})

        # Verify persisted in manifest on disk
        man.close()
        with Manifest(self.state_path, "/123", "/115") as reloaded:
            self.assertEqual(reloaded.recs["fail_unk.mkv"]["quota_bytes"], 400)
            self.assertEqual(reloaded.recs["fail_rapid.mkv"]["quota_bytes"], 0)

    def test_legacy_done_records_without_quota_bytes_fail_closed(self):
        import json

        legacy = {
            "rel": "legacy.mkv",
            "root": "/123",
            "src": "/123/legacy.mkv",
            "size": 700,
            "state": "dropped",
            "attempts": 0,
            "submit_fails": 0,
            "cloud": "115|acc1",
            "dest_root": "/115",
            "submitted_at": 900,
            "landed_at": 950,
            "ts": 960,
        }
        with open(self.state_path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(legacy) + "\n")

        man = self.make_man()
        r_new = man.add(make_file("/123/new.mkv", size=400))
        ctl = make_fake_ctl()
        cfg = make_cfg(self.state_path, "--daily-limit", "1000", "--quota-window", "rolling-24h")
        pipe = Pipeline(ctl, man, cfg, clock=lambda: 1000.0)
        pipe.cloud = "115|acc1"

        qs = pipe.quota_status("115|acc1", 1000.0)
        self.assertEqual(qs.confirmed_real, 700)
        self.assertEqual(qs.window_events, ((950.0, 700),))
        pipe.submit_pending([])
        self.assertEqual(r_new["state"], "pending")


if __name__ == "__main__":
    unittest.main()
