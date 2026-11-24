"""Unit tests for cd2_client.CD2Client."""

import re
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import grpc
from google.protobuf import timestamp_pb2

import clouddrive_pb2 as pb
from cd2_client import AuthExpiredError, CD2Client


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str = ""):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


def make_client(token: str | None = "tok-init") -> tuple[CD2Client, MagicMock, MagicMock]:
    channel = MagicMock()
    stub = MagicMock()
    client = CD2Client("127.0.0.1:19798", tls=False, token=token, channel=channel, stub=stub)
    return client, stub, channel


class CD2ClientAuthTest(unittest.TestCase):
    def test_instance_state_is_isolated(self):
        c1, _, _ = make_client()
        c2, _, _ = make_client()
        c1.creds = ("u", "p")
        c1.expires = 100.0
        c1._last_renew_fail = 50.0
        c1._renew_backoff = 10.0
        self.assertIsNone(c2.creds)
        self.assertEqual(c2.expires, float("inf"))
        self.assertEqual(c2._last_renew_fail, 0.0)
        self.assertEqual(c2._renew_backoff, 0.0)

    def test_login_password_saves_creds_and_expiration(self):
        client, stub, _ = make_client(token=None)
        stub.GetToken.return_value = pb.JWTToken(
            success=True,
            token="jwt-1",
            expiration=timestamp_pb2.Timestamp(seconds=9999),
        )
        tok = client.login("alice", "secret")
        self.assertEqual(tok.token, "jwt-1")
        self.assertEqual(client.token, "jwt-1")
        self.assertEqual(client.creds, ("alice", "secret"))
        self.assertEqual(client.expires, 9999)

    def test_login_with_totp_does_not_save_creds(self):
        client, stub, _ = make_client(token=None)
        stub.GetToken.return_value = pb.JWTToken(success=True, token="jwt-totp")
        client.login("alice", "secret", totp="123456")
        self.assertIsNone(client.creds)
        req = stub.GetToken.call_args.args[0]
        self.assertEqual(req.totpCode, "123456")

    def test_login_rpc_error_raises_system_exit(self):
        client, stub, _ = make_client(token=None)
        stub.GetToken.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "dial refused")
        with self.assertRaises(SystemExit) as ctx:
            client.login("alice", "secret")
        self.assertIn("UNAVAILABLE", str(ctx.exception))
        self.assertIn("dial refused", str(ctx.exception))

    def test_login_rejected_raises_system_exit(self):
        client, stub, _ = make_client(token=None)
        stub.GetToken.return_value = pb.JWTToken(success=False, errorMessage="bad password")
        with self.assertRaises(SystemExit) as ctx:
            client.login("alice", "wrong")
        self.assertIn("bad password", str(ctx.exception))

    def test_keep_login_exponential_backoff_and_reset(self):
        client, stub, _ = make_client()
        client.creds = ("alice", "secret")
        client.expires = 1500.0
        stub.GetToken.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down")

        now = 1000.0  # expires - 600 == 900 -> due for renewal
        expected_backoffs = [5.0, 10.0, 20.0, 40.0, 80.0, 120.0, 120.0]

        with patch("cd2_client.time.time", side_effect=lambda: now):
            for idx, want_backoff in enumerate(expected_backoffs, start=1):
                client.keep_login()
                self.assertEqual(stub.GetToken.call_count, idx)
                self.assertEqual(client._renew_backoff, want_backoff)
                # Calling again before backoff elapses must not hit GetToken.
                now += want_backoff - 0.5
                client.keep_login()
                self.assertEqual(stub.GetToken.call_count, idx)
                # Advance past backoff for the next attempt.
                now += 1.0

            # Subsequent success resets backoff state.
            stub.GetToken.side_effect = None
            stub.GetToken.return_value = pb.JWTToken(
                success=True,
                token="jwt-new",
                expiration=timestamp_pb2.Timestamp(seconds=int(now) + 3600),
            )
            client.keep_login()
            self.assertEqual(client.token, "jwt-new")
            self.assertEqual(client._renew_backoff, 0.0)
            self.assertEqual(client._last_renew_fail, 0.0)

    def test_local_rpc_unauthenticated_with_creds_renews_and_retries_once(self):
        client, stub, _ = make_client(token="old-tok")
        client.creds = ("alice", "secret")
        stub.GetCopyTasks.side_effect = [
            FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "expired"),
            pb.GetCopyTaskResult(copyTasks=[pb.CopyTask(sourcePath="/a", destPath="/115")]),
        ]
        stub.GetToken.return_value = pb.JWTToken(success=True, token="fresh-tok")

        tasks = client.tasks()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(stub.GetToken.call_count, 1)
        self.assertEqual(stub.GetCopyTasks.call_count, 2)
        first_md = stub.GetCopyTasks.call_args_list[0].kwargs["metadata"]
        second_md = stub.GetCopyTasks.call_args_list[1].kwargs["metadata"]
        self.assertEqual(first_md, [("authorization", "Bearer old-tok")])
        self.assertEqual(second_md, [("authorization", "Bearer fresh-tok")])

    def test_local_rpc_unauthenticated_without_creds_raises_auth_expired(self):
        client, stub, _ = make_client(token="static-tok")
        stub.GetCopyTasks.side_effect = FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "expired")

        with self.assertRaises(AuthExpiredError):
            client.tasks()
        stub.GetToken.assert_not_called()

    def test_local_rpc_unauthenticated_when_renew_fails_raises_auth_expired(self):
        client, stub, _ = make_client(token="old-tok")
        client.creds = ("alice", "secret")
        stub.GetCopyTasks.side_effect = FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "expired")
        stub.GetToken.return_value = pb.JWTToken(success=False, errorMessage="revoked")

        with self.assertRaises(AuthExpiredError):
            client.tasks()

    def test_remote_rpc_unauthenticated_propagates_raw_rpc_error(self):
        client, stub, _ = make_client(token="static-tok")
        err = FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "115 credential expired")
        stub.FindFileByPath.side_effect = err

        with self.assertRaises(grpc.RpcError) as ctx:
            client.find("/115/file.mkv")
        self.assertIs(ctx.exception, err)
        stub.GetToken.assert_not_called()

    def test_login_does_not_send_stale_metadata(self):
        client, stub, _ = make_client(token="stale-expired-tok")
        stub.GetToken.return_value = pb.JWTToken(success=True, token="jwt-fresh")

        client.login("alice", "secret")
        self.assertEqual(client.token, "jwt-fresh")
        self.assertEqual(stub.GetToken.call_args.kwargs, {"timeout": 45.0})
        self.assertNotIn("metadata", stub.GetToken.call_args.kwargs)

    def test_local_rpc_unauthenticated_twice_raises_auth_expired(self):
        client, stub, _ = make_client(token="old-tok")
        client.creds = ("alice", "secret")
        stub.GetCopyTasks.side_effect = [
            FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "expired"),
            FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "still rejected"),
        ]
        stub.GetToken.return_value = pb.JWTToken(success=True, token="fresh-tok")

        with self.assertRaises(AuthExpiredError):
            client.tasks()
        self.assertEqual(stub.GetToken.call_count, 1)
        self.assertEqual(stub.GetCopyTasks.call_count, 2)

    def test_concurrent_local_rpc_unauthenticated_renews_only_once(self):
        client, stub, _ = make_client(token="old-tok")
        client.creds = ("alice", "secret")
        barrier = threading.Barrier(2)

        def fake_get_copy_tasks(req, timeout=None, metadata=None):
            if metadata == [("authorization", "Bearer old-tok")]:
                barrier.wait(timeout=5.0)
                raise FakeRpcError(grpc.StatusCode.UNAUTHENTICATED, "expired")
            if metadata == [("authorization", "Bearer fresh-tok")]:
                return pb.GetCopyTaskResult(
                    copyTasks=[pb.CopyTask(sourcePath="/a", destPath="/115")]
                )
            raise AssertionError(f"unexpected metadata: {metadata}")

        def fake_get_token(req, timeout=None):
            time.sleep(0.05)
            return pb.JWTToken(success=True, token="fresh-tok")

        stub.GetCopyTasks.side_effect = fake_get_copy_tasks
        stub.GetToken.side_effect = fake_get_token

        results = []
        errors = []

        def worker():
            try:
                results.append(client.tasks())
            except Exception as exc:
                errors.append(exc)

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(stub.GetToken.call_count, 1)
        self.assertEqual(stub.GetCopyTasks.call_count, 4)


class CD2ClientOperationsTest(unittest.TestCase):
    def test_find_returns_none_only_on_not_found(self):
        client, stub, _ = make_client()
        stub.FindFileByPath.side_effect = FakeRpcError(grpc.StatusCode.NOT_FOUND, "missing")
        self.assertIsNone(client.find("/115/missing"))

    def test_find_does_not_swallow_internal_error_with_not_found_text(self):
        client, stub, _ = make_client()
        stub.FindFileByPath.side_effect = FakeRpcError(
            grpc.StatusCode.INTERNAL, "upstream error: file not found"
        )
        with self.assertRaises(grpc.RpcError) as ctx:
            client.find("/115/maybe")
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.INTERNAL)

    def test_find_returns_file_or_none_on_empty_message(self):
        client, stub, _ = make_client()
        stub.FindFileByPath.return_value = pb.CloudDriveFile(name="a.mkv", fullPathName="/115/a.mkv")
        self.assertEqual(client.find("/115/a.mkv").name, "a.mkv")

        stub.FindFileByPath.return_value = pb.CloudDriveFile()
        self.assertIsNone(client.find("/115/empty"))

    def test_delete_batch_empty_short_circuits(self):
        client, stub, _ = make_client()
        res = client.delete_batch([])
        self.assertTrue(res.success)
        stub.DeleteFiles.assert_not_called()
        stub.DeleteFilesPermanently.assert_not_called()

    def test_delete_batch_recycle_and_permanent(self):
        client, stub, _ = make_client()
        stub.DeleteFiles.return_value = pb.FileOperationResult(success=True)
        stub.DeleteFilesPermanently.return_value = pb.FileOperationResult(success=True)

        res1 = client.delete_batch(["/115/a", "/115/b"], permanent=False, timeout=60.0)
        self.assertTrue(res1.success)
        req1 = stub.DeleteFiles.call_args.args[0]
        self.assertEqual(list(req1.path), ["/115/a", "/115/b"])
        self.assertEqual(stub.DeleteFiles.call_args.kwargs["timeout"], 60.0)

        res2 = client.delete_batch(["/115/c"], permanent=True)
        self.assertTrue(res2.success)
        req2 = stub.DeleteFilesPermanently.call_args.args[0]
        self.assertEqual(list(req2.path), ["/115/c"])

    def test_delete_one_recycle_and_permanent(self):
        client, stub, _ = make_client()
        stub.DeleteFile.return_value = pb.FileOperationResult(success=True)
        stub.DeleteFilePermanently.return_value = pb.FileOperationResult(success=True)

        self.assertTrue(client.delete_one("/115/a").success)
        self.assertEqual(stub.DeleteFile.call_args.args[0].path, "/115/a")

        self.assertTrue(client.delete_one("/115/b", permanent=True, timeout=15.0).success)
        self.assertEqual(stub.DeleteFilePermanently.call_args.args[0].path, "/115/b")
        self.assertEqual(stub.DeleteFilePermanently.call_args.kwargs["timeout"], 15.0)

    def test_ensure_dir_creates_missing_segments_and_propagates_error(self):
        client, stub, _ = make_client()
        # /115 exists, /115/a and /115/a/b do not.
        stub.FindFileByPath.side_effect = [
            pb.CloudDriveFile(name="115", fullPathName="/115", isDirectory=True),
            FakeRpcError(grpc.StatusCode.NOT_FOUND),
            FakeRpcError(grpc.StatusCode.NOT_FOUND),
        ]
        stub.CreateFolder.return_value = pb.CreateFolderResult(
            result=pb.FileOperationResult(success=True)
        )
        ok, err = client.ensure_dir("/115/a/b")
        self.assertEqual((ok, err), (True, ""))
        created = [(c.args[0].parentPath, c.args[0].folderName) for c in stub.CreateFolder.call_args_list]
        self.assertEqual(created, [("/115", "a"), ("/115/a", "b")])

        # Failure returns exact errorMessage.
        stub.FindFileByPath.side_effect = [
            pb.CloudDriveFile(name="115", fullPathName="/115", isDirectory=True),
            FakeRpcError(grpc.StatusCode.NOT_FOUND),
        ]
        stub.CreateFolder.return_value = pb.CreateFolderResult(
            result=pb.FileOperationResult(success=False, errorMessage="quota exceeded")
        )
        ok, err = client.ensure_dir("/115/faildir")
        self.assertEqual((ok, err), (False, "quota exceeded"))

    def test_ensure_dir_fails_when_segment_is_regular_file(self):
        client, stub, _ = make_client()
        stub.FindFileByPath.side_effect = [
            pb.CloudDriveFile(name="115", fullPathName="/115", isDirectory=True),
            pb.CloudDriveFile(name="a", fullPathName="/115/a", isDirectory=False),
        ]
        ok, err = client.ensure_dir("/115/a/b")
        self.assertEqual((ok, err), (False, "/115/a exists and is not a directory"))
        stub.CreateFolder.assert_not_called()

    def test_walk_retries_transient_errors_and_raises_after_exhaustion(self):
        client, stub, _ = make_client()
        sub_dir = pb.CloudDriveFile(name="sub", fullPathName="/src/sub", isDirectory=True)
        file_a = pb.CloudDriveFile(name="a.bin", fullPathName="/src/sub/a.bin", isDirectory=False)

        stub.GetSubFiles.side_effect = [
            FakeRpcError(grpc.StatusCode.UNAVAILABLE, "temp"),
            [pb.SubFilesReply(subFiles=[sub_dir])],
            [pb.SubFilesReply(subFiles=[file_a])],
        ]
        with patch("cd2_client.time.sleep") as mock_sleep:
            files = list(client.walk("/src", tries=3))
        self.assertEqual([f.fullPathName for f in files], ["/src/sub/a.bin"])
        mock_sleep.assert_called_once_with(2)
        for call in stub.GetSubFiles.call_args_list:
            self.assertFalse(call.args[0].forceRefresh)

        # Exhausting retries raises the final RpcError.
        stub.GetSubFiles.side_effect = FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "timeout")
        with patch("cd2_client.time.sleep") as mock_sleep:
            with self.assertRaises(grpc.RpcError) as ctx:
                list(client.walk("/src", tries=3))
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_walk_does_not_retry_non_transient_errors(self):
        client, stub, _ = make_client()
        stub.GetSubFiles.side_effect = FakeRpcError(grpc.StatusCode.NOT_FOUND, "missing dir")
        with patch("cd2_client.time.sleep") as mock_sleep:
            with self.assertRaises(grpc.RpcError) as ctx:
                list(client.walk("/missing", tries=3))
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.NOT_FOUND)
        self.assertEqual(stub.GetSubFiles.call_count, 1)
        mock_sleep.assert_not_called()

    def test_list_dir_passes_force_refresh(self):
        client, stub, _ = make_client()
        stub.GetSubFiles.return_value = [
            pb.SubFilesReply(subFiles=[pb.CloudDriveFile(name="x")]),
            pb.SubFilesReply(subFiles=[pb.CloudDriveFile(name="y")]),
        ]
        items = client.list_dir("/115", force_refresh=True)
        self.assertEqual([f.name for f in items], ["x", "y"])
        req = stub.GetSubFiles.call_args.args[0]
        self.assertEqual(req.path, "/115")
        self.assertTrue(req.forceRefresh)

    def test_cloud_of_and_check_dest(self):
        client, stub, _ = make_client()
        stub.FindFileByPath.return_value = pb.CloudDriveFile(
            name="115",
            fullPathName="/115",
            CloudAPI=pb.CloudAPI(name="115open", userName="u1"),
        )
        api, why, absent = client.cloud_of("/115/nested/dir")
        self.assertEqual(api.name, "115open")
        self.assertIsNone(why)
        self.assertFalse(absent)
        self.assertEqual(stub.FindFileByPath.call_args.args[0].path, "/115")
        self.assertIsNone(client.check_dest("/115/nested", "115"))
        self.assertIn("expected /aliyun/", client.check_dest("/115/nested", "aliyun"))

        # Missing mount -> absent=True.
        stub.FindFileByPath.side_effect = FakeRpcError(grpc.StatusCode.NOT_FOUND)
        api, why, absent = client.cloud_of("/115/nested")
        self.assertIsNone(api)
        self.assertTrue(absent)
        self.assertIn("not present", why)

        # Transient RPC error -> absent=False.
        stub.FindFileByPath.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
        api, why, absent = client.cloud_of("/115/nested")
        self.assertIsNone(api)
        self.assertFalse(absent)
        self.assertIn("UNAVAILABLE", why)

    def test_check_dest_accepts_compiled_pattern(self):
        client, stub, _ = make_client()
        stub.FindFileByPath.return_value = pb.CloudDriveFile(
            name="115open",
            fullPathName="/115",
            CloudAPI=pb.CloudAPI(name="115open", userName="u1"),
        )
        self.assertIsNone(client.check_dest("/115/nested", re.compile(r"115open")))
        self.assertIsNone(client.check_dest("/115/nested", re.compile(r"115OPEN", re.I)))
        err = client.check_dest("/115/nested", re.compile(r"aliyun"))
        self.assertEqual(err, "cloud is '115open', expected /aliyun/")
        case_err = client.check_dest("/115/nested", re.compile(r"115OPEN"))
        self.assertEqual(case_err, "cloud is '115open', expected /115OPEN/")

    def test_submit_one_and_restart_task(self):
        client, stub, _ = make_client()
        stub.CopyFile.return_value = pb.FileOperationResult(success=True, errorMessage="")
        ok, err = client.submit_one("/src/a.mkv", "/115")
        self.assertEqual((ok, err), (True, ""))
        req = stub.CopyFile.call_args.args[0]
        self.assertEqual(list(req.theFilePaths), ["/src/a.mkv"])
        self.assertEqual(req.destPath, "/115")
        self.assertEqual(req.conflictPolicy, pb.CopyFileRequest.Rename)
        self.assertFalse(req.handleConflictRecursively)

        client.restart_task("/src/a.mkv", "/115")
        rreq = stub.RestartCopyTask.call_args.args[0]
        self.assertEqual((rreq.sourcePath, rreq.destPath), ("/src/a.mkv", "/115"))

    def test_remove_tasks_success_rejected_and_rpc_error(self):
        client, stub, _ = make_client()
        self.assertEqual(client.remove_tasks([]), (True, set()))
        stub.RemoveCopyTasks.assert_not_called()

        stub.RemoveCopyTasks.return_value = pb.BatchOperationResult(success=True, affectedCount=2)
        self.assertEqual(client.remove_tasks(["k1", "k2"]), (True, set()))

        stub.RemoveCopyTasks.return_value = pb.BatchOperationResult(
            success=False, affectedCount=0, errorMessage="db locked"
        )
        ok, failed = client.remove_tasks(["k1", "k2"])
        self.assertFalse(ok)
        self.assertEqual(failed, {"k1", "k2"})

        stub.RemoveCopyTasks.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "gone")
        ok, failed = client.remove_tasks(["k3"])
        self.assertFalse(ok)
        self.assertEqual(failed, {"k3"})

    def test_push_task_changes_and_close(self):
        client, stub, channel = make_client(token="tok-stream")
        stream_obj = MagicMock()
        stub.PushTaskChange.return_value = stream_obj

        res = client.push_task_changes()
        self.assertIs(res, stream_obj)
        self.assertEqual(
            stub.PushTaskChange.call_args.kwargs["metadata"],
            [("authorization", "Bearer tok-stream")],
        )

        client.close()
        channel.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
