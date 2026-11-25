"""Unit tests for cd2_manifest.Manifest.

    python -m unittest -v test_cd2_manifest
"""

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import cd2_manifest
from cd2_manifest import DONE, PERSIST_FIELDS, RETRY, Manifest, ManifestLockError


def fake_file(path: str, size: int = 100):
    return SimpleNamespace(fullPathName=path, size=size)


def make_rec(rel: str, root: str = "/src", state: str = "pending", **extra) -> dict:
    rec = {
        "rel": rel,
        "root": root,
        "src": f"{root}/{rel}",
        "size": 100,
        "state": state,
        "attempts": 0,
        "submit_fails": 0,
        "cloud": "",
        "dest_root": "/115",
        "submitted_at": 0,
        "landed_at": 0,
        "ts": 1700000000,
    }
    rec.update(extra)
    return rec


class ManifestTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.path = os.path.join(self.dir, "manifest.jsonl")

    def _capture_open(self, src_root="/src", dest_root="/115"):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            man = Manifest(self.path, src_root, dest_root)
        self.addCleanup(man.close)
        return man, buf.getvalue()

    def test_repair_small_torn_tail_and_fsync(self):
        good = json.dumps(make_rec("a.mkv")) + "\n"
        with open(self.path, "wb") as fh:
            fh.write(good.encode("utf-8") + b'{"rel": "b.mkv", "broken')

        with mock.patch("cd2_manifest.os.fsync", wraps=os.fsync) as m_fsync:
            man, out = self._capture_open()
            self.assertEqual(m_fsync.call_count, 1)

        self.assertIn("dropped torn tail", out)
        self.assertEqual(list(man.recs.keys()), ["a.mkv"])
        with open(self.path, "rb") as fh:
            self.assertEqual(fh.read(), good.encode("utf-8"))

    def test_repair_multi_block_torn_tail_without_full_read(self):
        good = json.dumps(make_rec("keep.mkv")) + "\n"
        torn = b"x" * 10000  # > 2 * 4096B chunks
        with open(self.path, "wb") as fh:
            fh.write(good.encode("utf-8") + torn)

        man, out = self._capture_open()
        self.assertIn("dropped torn tail (10000 bytes)", out)
        self.assertEqual(list(man.recs.keys()), ["keep.mkv"])
        with open(self.path, "rb") as fh:
            self.assertEqual(fh.read(), good.encode("utf-8"))

    def test_repair_single_torn_line_without_newline_truncates_to_empty(self):
        with open(self.path, "wb") as fh:
            fh.write(b'{"rel": "only_torn"')

        man, out = self._capture_open()
        self.assertIn("dropped torn tail", out)
        self.assertEqual(man.recs, {})
        self.assertEqual(os.path.getsize(self.path), 0)

    def test_repair_clean_file_does_not_fsync(self):
        with open(self.path, "wb") as fh:
            fh.write((json.dumps(make_rec("ok.mkv")) + "\n").encode("utf-8"))

        with mock.patch("cd2_manifest.os.fsync", wraps=os.fsync) as m_fsync:
            man, _ = self._capture_open()
            self.assertEqual(m_fsync.call_count, 0)
        self.assertIn("ok.mkv", man.recs)

    def test_load_corrupt_middle_lines_warn_with_lineno_and_log_dedup_count(self):
        lines = [
            json.dumps(make_rec("a.mkv", state="pending")),
            "{not valid json",
            json.dumps(make_rec("a.mkv", state="submitted")),
            json.dumps([1, 2, 3]),
            json.dumps(make_rec("b.mkv", state="dropped")),
            json.dumps(make_rec("a.mkv", state="landed")),
        ]
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")

        man, out = self._capture_open()
        self.assertIn("line 2", out)
        self.assertIn("line 4", out)
        self.assertIn("manifest: 2 known files", out)
        self.assertEqual(len(man.recs), 2)
        self.assertEqual(man.recs["a.mkv"]["state"], "landed")
        self.assertEqual(man.recs["b.mkv"]["state"], "dropped")

    def test_load_rejects_cross_source_and_releases_lock(self):
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(make_rec("a.mkv", root="/srcA")) + "\n")

        with self.assertRaises(SystemExit):
            Manifest(self.path, "/srcB", "/115")

        # Lock must have been released on failed __init__
        with Manifest(self.path, "/srcA", "/115") as man:
            self.assertIn("a.mkv", man.recs)

    def test_add_prefix_validation(self):
        with Manifest(self.path, "/src", "/115") as man:
            for bad in ("/src2/a.mkv", "/other/a.mkv", "/src", "/src/"):
                with self.assertRaises(ValueError, msg=bad):
                    man.add(fake_file(bad))
            self.assertEqual(man.recs, {})

            # Atomic validation in add_batch: bad item prevents partial add
            with self.assertRaises(ValueError):
                man.add_batch([fake_file("/src/good.mkv"), fake_file("/src2/bad.mkv")])
            self.assertEqual(man.recs, {})
            self.assertFalse(os.path.exists(self.path))

    def test_done_does_not_regress_and_failed_revives(self):
        with Manifest(self.path, "/src", "/115") as man:
            r_done = man.add(fake_file("/src/done.mkv", 10))
            r_fail = man.add(fake_file("/src/fail.mkv", 20))
            man.set_batch([
                (r_done, "dropped", {}),
                (r_fail, "failed", {"attempts": 3, "submit_fails": 2}),
            ])

            with mock.patch("cd2_manifest.os.fsync", wraps=os.fsync) as m_fsync:
                out_done = man.add(fake_file("/src/done.mkv", 999))
                self.assertEqual(m_fsync.call_count, 0)
            self.assertEqual(out_done["state"], "dropped")
            self.assertEqual(out_done["size"], 10)

            out_revived = man.add(fake_file("/src/fail.mkv", 25))
            self.assertEqual(out_revived["state"], "pending")
            self.assertEqual(out_revived["attempts"], 0)
            self.assertEqual(out_revived["submit_fails"], 0)
            self.assertEqual(out_revived["size"], 25)

        with Manifest(self.path, "/src", "/115") as reloaded:
            self.assertEqual(reloaded.recs["done.mkv"]["state"], "dropped")
            self.assertEqual(reloaded.recs["fail.mkv"]["state"], "pending")
            self.assertEqual(reloaded.recs["fail.mkv"]["attempts"], 0)
            self.assertEqual(reloaded.recs["fail.mkv"]["submit_fails"], 0)

    def test_add_batch_and_set_batch_single_fsync_and_reload(self):
        files = [fake_file(f"/src/sub/f{i}.mkv", 100 + i) for i in range(5)]
        with Manifest(self.path, "/src", "/115") as man:
            with mock.patch("cd2_manifest.os.fsync", wraps=os.fsync) as m_fsync:
                recs = man.add_batch(files)
                self.assertEqual(m_fsync.call_count, 1)
            self.assertEqual(len(recs), 5)

            updates = [
                (recs[0], "submitted", {"cloud": "115|u1", "dest_root": "/115", "submitted_at": 111}),
                (recs[1], "landed", {"landed_at": 222}),
                (recs[2], "kept", {}),
            ]
            with mock.patch("cd2_manifest.os.fsync", wraps=os.fsync) as m_fsync:
                man.set_batch(updates)
                self.assertEqual(m_fsync.call_count, 1)

            # Empty batches do not fsync
            with mock.patch("cd2_manifest.os.fsync", wraps=os.fsync) as m_fsync:
                man.add_batch([])
                man.set_batch([])
                self.assertEqual(m_fsync.call_count, 0)

            self.assertEqual(len(man.of("pending")), 2)
            self.assertEqual(len(man.of("submitted", "landed")), 2)
            self.assertEqual(man.by_src()["/src/sub/f1.mkv"]["landed_at"], 222)

        with Manifest(self.path, "/src", "/115") as reloaded:
            self.assertEqual(len(reloaded.recs), 5)
            self.assertEqual(reloaded.recs["sub/f0.mkv"]["cloud"], "115|u1")
            self.assertEqual(reloaded.recs["sub/f0.mkv"]["submitted_at"], 111)
            self.assertEqual(reloaded.recs["sub/f1.mkv"]["state"], "landed")
            self.assertEqual(reloaded.recs["sub/f1.mkv"]["landed_at"], 222)
            self.assertEqual(reloaded.recs["sub/f2.mkv"]["state"], "kept")

    def test_volatile_fields_are_never_persisted(self):
        volatile = {
            "seen": True,
            "failed_at": 12345,
            "clear_since": 999.5,
            "retry": 4,
            "miss_since": 888.0,
            "miss_count": 3,
        }
        with Manifest(self.path, "/src", "/115") as man:
            r = man.add(fake_file("/src/v.mkv"))
            r.update(volatile)
            man.set(r, "submitted", submitted_at=555, failed_at=99999, retry=7)
            # Volatile keys stay in memory
            self.assertEqual(r["retry"], 7)

        with open(self.path, encoding="utf-8") as fh:
            for line in fh:
                obj = json.loads(line)
                self.assertTrue(set(obj.keys()).issubset(PERSIST_FIELDS))
                for k in volatile:
                    self.assertNotIn(k, obj)

        with Manifest(self.path, "/src", "/115") as reloaded:
            rec = reloaded.recs["v.mkv"]
            for k in volatile:
                self.assertNotIn(k, rec)
            self.assertEqual(rec["submitted_at"], 555)

    def test_same_process_duplicate_lock_and_release(self):
        man = Manifest(self.path, "/src", "/115")
        try:
            with self.assertRaises(ManifestLockError):
                Manifest(self.path, "/src", "/115")
        finally:
            man.close()
            man.close()  # idempotent

        with Manifest(self.path, "/src", "/115") as man2:
            self.assertEqual(man2.recs, {})

    def test_cross_process_lock_conflict_and_release(self):
        helper = (
            "import sys; "
            f"sys.path.insert(0, {os.path.dirname(os.path.abspath(cd2_manifest.__file__))!r}); "
            "from cd2_manifest import Manifest; "
            f"m = Manifest({self.path!r}, '/src', '/115'); "
            "print('LOCKED', flush=True); "
            "sys.stdin.readline(); "
            "m.close()"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", helper],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            ready = proc.stdout.readline().strip()
            if ready != "LOCKED":
                self.fail(f"child failed to lock: {ready!r} stderr={proc.stderr.read()!r}")
            with self.assertRaises(ManifestLockError):
                Manifest(self.path, "/src", "/115")
            proc.stdin.write("release\n")
            proc.stdin.flush()
            proc.stdin.close()
            proc.wait(timeout=10)
            self.assertEqual(proc.returncode, 0, proc.stderr.read())
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream:
                    stream.close()

        with Manifest(self.path, "/src", "/115") as man:
            self.assertEqual(man.recs, {})

    def test_load_skips_missing_or_invalid_fields_without_cross_source_exit(self):
        no_root = make_rec("no_root.mkv")
        del no_root["root"]
        empty_root = make_rec("empty_root.mkv", root="")
        no_rel = make_rec("no_rel.mkv")
        del no_rel["rel"]
        no_src = make_rec("no_src.mkv")
        del no_src["src"]
        no_state = make_rec("no_state.mkv")
        del no_state["state"]
        bad_state = make_rec("bad_state.mkv", state="unknown")
        non_str_state = make_rec("non_str_state.mkv", state=123)

        lines = [
            json.dumps(make_rec("a.mkv", state="pending")),
            json.dumps(no_root),
            json.dumps(empty_root),
            json.dumps(no_rel),
            json.dumps(no_src),
            json.dumps(no_state),
            json.dumps(bad_state),
            json.dumps(non_str_state),
            json.dumps(make_rec("b.mkv", state="landed")),
        ]
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")

        man, out = self._capture_open()
        for lineno in range(2, 9):
            self.assertIn(f"line {lineno}", out)
        self.assertEqual(set(man.recs.keys()), {"a.mkv", "b.mkv"})
        self.assertEqual(man.recs["a.mkv"]["state"], "pending")
        self.assertEqual(man.recs["b.mkv"]["state"], "landed")

    def test_write_after_close_raises_runtime_error(self):
        man = Manifest(self.path, "/src", "/115")
        r = man.add(fake_file("/src/a.mkv", 10))
        man.close()

        with self.assertRaisesRegex(RuntimeError, "manifest is closed"):
            man.add(fake_file("/src/new.mkv", 20))
        self.assertNotIn("new.mkv", man.recs)

        with self.assertRaisesRegex(RuntimeError, "manifest is closed"):
            man.add(fake_file("/src/a.mkv", 999))
        self.assertEqual(r["size"], 10)

        with self.assertRaisesRegex(RuntimeError, "manifest is closed"):
            man.set(r, "submitted", cloud="c1")
        self.assertEqual(r["state"], "pending")
        self.assertEqual(r["cloud"], "")

    def test_write_failure_does_not_pollute_memory(self):
        with Manifest(self.path, "/src", "/115") as man:
            r_pend = man.add(fake_file("/src/pend.mkv", 10))
            r_fail = man.add(fake_file("/src/fail.mkv", 20))
            man.set(r_fail, "failed", attempts=3, submit_fails=2)
            orig_fail = dict(r_fail)
            orig_pend = dict(r_pend)

            with mock.patch("cd2_manifest.os.fsync", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    man.add_batch([
                        fake_file("/src/new.mkv", 30),
                        fake_file("/src/fail.mkv", 25),
                        fake_file("/src/pend.mkv", 99),
                    ])
            self.assertNotIn("new.mkv", man.recs)
            self.assertEqual(r_fail, orig_fail)
            self.assertEqual(r_pend, orig_pend)

            with mock.patch("cd2_manifest.os.fsync", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    man.set_batch([
                        (r_pend, "submitted", {"cloud": "115|u1", "submitted_at": 123}),
                    ])
            self.assertEqual(r_pend, orig_pend)

    def test_root_and_path_normalization(self):
        with Manifest(self.path, "/", "//") as man:
            self.assertEqual(man.src_root, "/")
            self.assertEqual(man.dest_root, "/")
            r1 = man.add(fake_file("/a.mkv", 10))
            r2 = man.add(fake_file("//sub//b.mkv", 20))
            self.assertEqual(r1["rel"], "a.mkv")
            self.assertEqual(r1["src"], "/a.mkv")
            self.assertEqual(r1["root"], "/")
            self.assertEqual(r2["rel"], "sub/b.mkv")
            self.assertEqual(r2["src"], "/sub/b.mkv")
            for bad in ("/", "//", "relative/c.mkv"):
                with self.assertRaises(ValueError, msg=bad):
                    man.add(fake_file(bad))

        with Manifest(self.path, "/", "/") as reloaded:
            self.assertEqual(set(reloaded.recs.keys()), {"a.mkv", "sub/b.mkv"})

        path2 = os.path.join(self.dir, "manifest2.jsonl")
        with Manifest(path2, "//src/", "//115/") as man:
            self.assertEqual(man.src_root, "/src")
            self.assertEqual(man.dest_root, "/115")
            r = man.add(fake_file("//src//dir//x.mkv", 30))
            self.assertEqual(r["rel"], "dir/x.mkv")
            self.assertEqual(r["src"], "/src/dir/x.mkv")
            self.assertEqual(r["root"], "/src")
            with self.assertRaises(ValueError):
                man.add(fake_file("/src/../other/y.mkv"))

        with Manifest(path2, "/src", "/115") as reloaded:
            self.assertIn("dir/x.mkv", reloaded.recs)

    def test_constants(self):
        self.assertEqual(DONE, ("dropped", "kept", "stranded"))
        self.assertEqual(RETRY, ("failed",))
        self.assertEqual(
            PERSIST_FIELDS,
            (
                "rel", "root", "src", "size", "state", "attempts", "submit_fails",
                "cloud", "dest_root", "submitted_at", "landed_at", "upload_kind",
                "quota_bytes", "ts",
            ),
        )

    def test_quota_fields_persistence_and_legacy_compat(self):
        legacy = {
            "rel": "old.mkv",
            "root": "/src",
            "src": "/src/old.mkv",
            "size": 500,
            "state": "dropped",
            "attempts": 0,
            "submit_fails": 0,
            "cloud": "115|acc1",
            "dest_root": "/115",
            "submitted_at": 100,
            "landed_at": 120,
            "ts": 125,
        }
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(legacy) + "\n")

        with Manifest(self.path, "/src", "/115") as man:
            self.assertIn("old.mkv", man.recs)
            self.assertIsNone(man.recs["old.mkv"].get("quota_bytes"))
            r = man.add(fake_file("/src/new.mkv", 1000))
            self.assertEqual(r["upload_kind"], "")
            self.assertIsNone(r["quota_bytes"])
            man.set(r, "dropped", upload_kind="rapid", quota_bytes=0, landed_at=200)

        with Manifest(self.path, "/src", "/115") as reloaded:
            self.assertEqual(reloaded.recs["new.mkv"]["upload_kind"], "rapid")
            self.assertEqual(reloaded.recs["new.mkv"]["quota_bytes"], 0)
            self.assertEqual(reloaded.recs["new.mkv"]["landed_at"], 200)
            self.assertIsNone(reloaded.recs["old.mkv"].get("quota_bytes"))

    def test_add_revives_failed_without_modifying_done(self):
        with Manifest(self.path, "/src", "/115") as man:
            r_fail = man.add(fake_file("/src/fail.mkv", 200))
            r_done = man.add(fake_file("/src/done.mkv", 300))
            man.set_batch([
                (r_fail, "failed", {"attempts": 3, "submit_fails": 2, "upload_kind": "real", "quota_bytes": 200}),
                (r_done, "dropped", {"upload_kind": "real", "quota_bytes": 300, "landed_at": 500}),
            ])

            revived = man.add(fake_file("/src/fail.mkv", 250))
            untouched = man.add(fake_file("/src/done.mkv", 999))

            self.assertEqual(revived["state"], "pending")
            self.assertEqual(revived["attempts"], 0)
            self.assertEqual(revived["submit_fails"], 0)
            self.assertEqual(revived["upload_kind"], "")
            self.assertEqual(revived["quota_bytes"], 0)
            self.assertEqual(revived["size"], 250)

            self.assertEqual(untouched["state"], "dropped")
            self.assertEqual(untouched["size"], 300)
            self.assertEqual(untouched["upload_kind"], "real")
            self.assertEqual(untouched["quota_bytes"], 300)
            self.assertEqual(untouched["landed_at"], 500)


if __name__ == "__main__":
    unittest.main()
