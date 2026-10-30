"""Behaviour of cd2_stream.Pipeline against a scripted CloudDrive2.

    python -m unittest test_cd2_stream
"""

import tempfile
import unittest
from types import SimpleNamespace

import grpc

import clouddrive_pb2 as pb
from cd2_stream import Manifest, Pipeline, channel_target

DEST = "/115"


class Expired(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAUTHENTICATED

    def details(self):
        return "115 credential expired"


class FakeCD2:
    """One 115 mount at /115 whose account can be swapped or removed."""

    def __init__(self):
        self.account = "userA"      # None = mount removed
        self.dead = False           # the 115 credential behind the mount expired
        self.files = {}             # path -> size on the current account
        self.task_rows = {}         # sourcePath -> CopyTask
        self.copies, self.restarts, self.deleted, self.events = [], [], [], []
        self.delete_permanent = []
        self.can_delete_permanently = False

    def keep_login(self):
        pass

    def tasks(self):
        return list(self.task_rows.values())

    def cloud_of(self, path):
        if self.account is None:
            return None, "mount /115 not present", True
        return pb.CloudAPI(name="115open", userName=self.account), None, False

    def ensure_dir(self, path):
        return False

    def find(self, path):
        if self.dead:
            raise Expired()
        size = self.files.get(path)
        return None if size is None else pb.CloudDriveFile(
            name=path.rsplit("/", 1)[1], size=size,
            canDeletePermanently=self.can_delete_permanently,
        )

    def delete(self, path, permanent=False):
        if self.dead:
            raise Expired()
        self.deleted.append(path)
        self.delete_permanent.append(permanent)
        self.events.append("delete")
        self.files.pop(path, None)
        return pb.FileOperationResult(success=True)

    def submit_one(self, src, dest):
        self.copies.append(src)
        self.task_rows[src] = pb.CopyTask(sourcePath=src, destPath=dest, status=pb.CopyTask.Pending,
                                          totalFiles=1, totalBytes=100)
        return True, ""

    def restart_task(self, src, dest):
        self.restarts.append(src)
        self.events.append("restart")

    def remove_tasks(self, keys):
        for k in keys:
            self.task_rows.pop(k.rsplit(":", 1)[0], None)

    # engine outcomes
    def finish(self, src, **counts):
        t = self.task_rows[src]
        t.status = pb.CopyTask.Completed
        for k, v in {"uploadedFiles": 1, "uploadedBytes": 100, **counts}.items():
            setattr(t, k, v)
        if t.uploadedFiles:
            self.files[f"{DEST}/{src.rsplit('/', 1)[1]}"] = 100

    def fail(self, src):
        self.task_rows[src].status = pb.CopyTask.Failed


def args(**kw):
    a = dict(batch=0, workers=1, stall=300, retry_after=0, max_attempts=2,
             delete="recycle", expect_cloud="115")
    a.update(kw)
    return SimpleNamespace(**a)


class PipelineTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state = f"{tmp.name}/manifest.jsonl"
        self.cd2 = FakeCD2()

    def pipeline(self, names, **kw):
        man = Manifest(self.state, "/src", DEST)
        for n in names:
            man.add(pb.CloudDriveFile(fullPathName=f"/src/d/{n}", name=n, size=100))
        return Pipeline(self.cd2, man, args(**kw))

    def state_of(self, p, name):
        return p.man.by_src()[f"/src/d/{name}"]["state"]

    def test_account_swap_resubmits_only_what_did_not_land(self):
        cd2 = self.cd2
        p = self.pipeline(["ok", "lost", "queued"])
        p.step()
        self.assertEqual(sorted(cd2.copies), ["/src/d/lost", "/src/d/ok", "/src/d/queued"])

        cd2.finish("/src/d/ok")
        cd2.dead = True                          # account kicked before the landed copy is cleared
        cd2.fail("/src/d/lost")
        p.step()
        self.assertEqual(self.state_of(p, "ok"), "landed")

        cd2.task_rows.clear()                    # operator purges every task
        cd2.account = None                       # removes the mount
        p.step()
        cd2.account, cd2.dead, cd2.files = "userB", False, {}   # same mount name, empty account
        cd2.copies.clear()
        p.step()
        self.assertEqual(sorted(cd2.copies), ["/src/d/lost", "/src/d/queued"])
        self.assertEqual(self.state_of(p, "ok"), "dropped")

        cd2.task_rows.clear()                    # the new tasks are not visible yet
        p.step()
        self.assertEqual(sorted(cd2.copies), ["/src/d/lost", "/src/d/queued"])

    def test_upload_stands_when_its_copy_cannot_be_cleared(self):
        cd2 = self.cd2
        p = self.pipeline(["a"], stall=0)
        p.step()
        cd2.finish("/src/d/a")
        cd2.dead = True
        p.step()
        self.assertEqual(self.state_of(p, "a"), "stranded")

        reloaded = Manifest(self.state, "/src", DEST)
        r = reloaded.add(pb.CloudDriveFile(fullPathName="/src/d/a", size=100))
        self.assertEqual(r["state"], "stranded")

    def test_parked_file_whose_task_later_completes_counts_as_uploaded(self):
        cd2 = self.cd2
        p = self.pipeline(["a"], max_attempts=1)
        p.step()
        cd2.fail("/src/d/a")
        p.step()
        p.step()
        self.assertEqual(self.state_of(p, "a"), "failed")
        cd2.finish("/src/d/a")
        p.step()
        self.assertEqual(self.state_of(p, "a"), "dropped")

    def test_parked_files_get_a_fresh_budget_on_another_account(self):
        cd2 = self.cd2
        p = self.pipeline(["a"], max_attempts=1)
        p.step()
        cd2.fail("/src/d/a")
        p.step()                                 # one restart allowed
        p.step()
        self.assertEqual(self.state_of(p, "a"), "failed")

        cd2.task_rows.clear()
        cd2.account = "userB"
        cd2.copies.clear()
        p.step()
        self.assertEqual(cd2.copies, ["/src/d/a"])

    def test_queued_task_is_left_to_the_engine(self):
        cd2 = self.cd2
        p = self.pipeline(["a"], stall=0)
        p.step()
        for _ in range(3):
            p.step()
        self.assertEqual(cd2.restarts, [])
        self.assertEqual(self.state_of(p, "a"), "submitted")

    def test_completed_without_a_copied_file_is_not_an_upload(self):
        cd2 = self.cd2
        p = self.pipeline(["a"], retry_after=3600)
        p.step()
        cd2.finish("/src/d/a", uploadedFiles=0, skippedFiles=1)
        p.step()
        self.assertEqual(self.state_of(p, "a"), "submitted")

    def test_same_named_file_is_cleared_before_submitting(self):
        cd2 = self.cd2
        cd2.files[f"{DEST}/a"] = 7
        p = self.pipeline(["a"])
        p.step()
        self.assertEqual(cd2.deleted, [f"{DEST}/a"])
        self.assertEqual(cd2.delete_permanent, [False])
        self.assertEqual(cd2.copies, ["/src/d/a"])

    def test_same_named_file_blocks_when_permanent_is_unsupported(self):
        cd2 = self.cd2
        cd2.files[f"{DEST}/a"] = 7
        p = self.pipeline(["a"], delete="permanent", max_attempts=1)
        p.step()
        self.assertEqual(cd2.deleted, [])
        self.assertEqual(cd2.copies, [])
        self.assertEqual(self.state_of(p, "a"), "failed")

    def test_foreign_completed_task_does_not_count_for_a_parked_file(self):
        cd2 = self.cd2
        cd2.task_rows["/src/d/a"] = pb.CopyTask(sourcePath="/src/d/a", destPath=DEST, status=pb.CopyTask.Completed,
                                                 uploadedFiles=1, totalFiles=1, totalBytes=200)
        cd2.remove_tasks = lambda keys: None     # the stale row cannot be removed
        p = self.pipeline(["a"], max_attempts=1)
        p.step()
        p.step()
        self.assertEqual(self.state_of(p, "a"), "failed")

    def test_landed_copies_are_cleared_before_failed_tasks_restart(self):
        cd2 = self.cd2
        p = self.pipeline(["ok", "bad"])
        p.step()
        cd2.finish("/src/d/ok")
        cd2.fail("/src/d/bad")
        cd2.events.clear()
        p.step()
        self.assertEqual(cd2.events, ["delete", "restart"])
        self.assertEqual(cd2.delete_permanent, [False])

    def test_permanent_delete_is_refused_without_engine_flag(self):
        cd2 = self.cd2
        p = self.pipeline(["a"], delete="permanent", stall=0)
        p.step()
        cd2.finish("/src/d/a")
        p.step()
        self.assertEqual(cd2.deleted, [])
        self.assertEqual(self.state_of(p, "a"), "stranded")

    def test_permanent_delete_when_engine_allows_it(self):
        cd2 = self.cd2
        cd2.can_delete_permanently = True
        p = self.pipeline(["a"], delete="permanent")
        p.step()
        cd2.finish("/src/d/a")
        p.step()
        self.assertEqual(cd2.deleted, [f"{DEST}/a"])
        self.assertEqual(cd2.delete_permanent, [True])
        self.assertEqual(self.state_of(p, "a"), "dropped")

    def test_subfolder_dest_that_cannot_be_created_is_not_ready(self):
        man = Manifest(self.state, "/src", "/115/inbox")
        p = Pipeline(self.cd2, man, args())
        self.assertEqual(p.dest_ready(), "cannot create the destination folder")


class EndpointTest(unittest.TestCase):
    def test_endpoint_forms(self):
        cases = {
            "127.0.0.1:19798": ("127.0.0.1:19798", False),
            "http://nas.lan:8080": ("nas.lan:8080", False),
            "https://cd2.example.com": ("cd2.example.com:443", True),
            "https://cd2.example.com:9443/": ("cd2.example.com:9443", True),
            "http://[::1]:19798": ("[::1]:19798", False),
        }
        for endpoint, want in cases.items():
            self.assertEqual(channel_target(endpoint), want, endpoint)
        for bad in ("ftp://x:1", "https://x:99999", "https://x/sub"):
            with self.assertRaises(SystemExit, msg=bad):
                channel_target(bad)


if __name__ == "__main__":
    unittest.main()
