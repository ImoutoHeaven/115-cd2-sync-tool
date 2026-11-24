"""Unit tests for cd2_core."""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import clouddrive_pb2 as pb
from cd2_core import (
    DONE,
    LIVE,
    MANIFEST_FIELDS,
    RETRY,
    VALID_STATES,
    WORKING,
    Config,
    ThrottleGate,
    base_of,
    channel_target,
    is_throttle_error,
    mount_of,
    normalize_path,
    parse_args,
    paths_overlap,
    ts_of,
    uploaded,
)


class FakeRpcError(grpc.RpcError):
    def __init__(self, code, details=""):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details

    def __str__(self):
        return f"{self._code.name}: {self._details}"


class ConstantsAndPathTest(unittest.TestCase):
    def test_constants(self):
        self.assertEqual(
            VALID_STATES,
            ("pending", "submitted", "landed", "dropped", "kept", "stranded", "failed"),
        )
        self.assertEqual(DONE, ("dropped", "kept", "stranded"))
        self.assertEqual(RETRY, ("failed",))
        self.assertEqual(LIVE, ("submitted", "landed"))
        self.assertEqual(WORKING, (pb.CopyTask.Pending, pb.CopyTask.Scanning, pb.CopyTask.Scanned))
        self.assertEqual(
            MANIFEST_FIELDS,
            (
                "rel", "root", "src", "size", "state", "attempts", "submit_fails",
                "cloud", "dest_root", "submitted_at", "landed_at", "ts",
            ),
        )

    def test_normalize_path_valid(self):
        cases = {
            "/": "/",
            "//": "/",
            "///": "/",
            "/115/": "/115",
            "//115//sub/./a/../b/": "/115/sub/b",
            "/a/b/..": "/a",
            "/a/..": "/",
            "/../115": "/115",
        }
        for raw, want in cases.items():
            self.assertEqual(normalize_path(raw), want, raw)

    def test_normalize_path_invalid(self):
        for bad in ("", "115/a", "./a", "../a", " /115", "/115\x00a"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                normalize_path(bad)

    def test_paths_overlap(self):
        self.assertTrue(paths_overlap("/115", "/115"))
        self.assertTrue(paths_overlap("/115/", "//115/."))
        self.assertTrue(paths_overlap("/115", "/115/sub"))
        self.assertTrue(paths_overlap("/115/sub/a", "/115"))
        self.assertTrue(paths_overlap("/", "/115"))
        self.assertTrue(paths_overlap("/115", "/"))
        self.assertFalse(paths_overlap("/115", "/1150"))
        self.assertFalse(paths_overlap("/123pan/movies", "/115"))

    def test_base_mount_ts(self):
        self.assertEqual(base_of("/123/a/b.mkv"), "b.mkv")
        self.assertEqual(base_of("/123/a/"), "a")
        self.assertEqual(base_of("/"), "")
        self.assertEqual(mount_of("/115/inbox/a"), "/115")
        self.assertEqual(mount_of("//115/"), "/115")
        self.assertEqual(mount_of("/"), "/")
        self.assertEqual(ts_of(None), 0)
        self.assertEqual(ts_of(Timestamp(seconds=0)), 0)
        self.assertEqual(ts_of(Timestamp(seconds=1700000000)), 1700000000)


class EndpointTest(unittest.TestCase):
    def test_valid_endpoints(self):
        cases = {
            "127.0.0.1:19798": ("127.0.0.1:19798", False),
            "[::1]:19798": ("[::1]:19798", False),
            "nas.lan:1": ("nas.lan:1", False),
            "nas.lan:65535": ("nas.lan:65535", False),
            "http://nas.lan": ("nas.lan:80", False),
            "http://nas.lan:8080": ("nas.lan:8080", False),
            "https://cd2.example.com": ("cd2.example.com:443", True),
            "https://cd2.example.com:9443/": ("cd2.example.com:9443", True),
            "http://[::1]:19798": ("[::1]:19798", False),
            "https://[::1]/": ("[::1]:443", True),
        }
        for ep, want in cases.items():
            self.assertEqual(channel_target(ep), want, ep)

    def test_invalid_endpoints(self):
        bad_cases = (
            "",
            "   ",
            " 127.0.0.1:19798",
            "127.0.0.1",
            "127.0.0.1:",
            ":19798",
            "127.0.0.1:0",
            "127.0.0.1:65536",
            "127.0.0.1:-1",
            "127.0.0.1:abc",
            "127.0.0.1:19798/path",
            "127.0.0.1:19798?a=1",
            "127.0.0.1:19798#frag",
            "user@127.0.0.1:19798",
            "ftp://host:21",
            "https://",
            "https://host:",
            "https://host:0",
            "https://host:99999",
            "https://host/sub",
            "https://user:pass@host:443",
            "https://@host:443",
            "https://host:443?x=1",
            "https://host:443?",
            "https://host:443#top",
            "https://host:443#",
        )
        for bad in bad_cases:
            with self.assertRaises(SystemExit, msg=repr(bad)):
                channel_target(bad)


class UploadedAndThrottleErrorTest(unittest.TestCase):
    def _task(self, **kw):
        defaults = dict(
            status=pb.CopyTask.Completed,
            uploadedFiles=1,
            failedFiles=0,
            cancelledFiles=0,
            skippedFiles=0,
            failedFolders=0,
            totalBytes=100,
            uploadedBytes=100,
        )
        defaults.update(kw)
        return pb.CopyTask(**defaults)

    def test_uploaded_checks(self):
        self.assertTrue(uploaded(self._task()))
        self.assertTrue(uploaded(self._task(), expected_size=100))
        self.assertTrue(uploaded(self._task(uploadedBytes=120), expected_size=100))
        self.assertTrue(uploaded(self._task(totalBytes=0, uploadedBytes=0), expected_size=100))
        self.assertFalse(uploaded(self._task(status=pb.CopyTask.Failed)))
        self.assertFalse(uploaded(self._task(uploadedFiles=0)))
        self.assertFalse(uploaded(self._task(failedFiles=1)))
        self.assertFalse(uploaded(self._task(cancelledFiles=1)))
        self.assertFalse(uploaded(self._task(skippedFiles=1)))
        self.assertFalse(uploaded(self._task(failedFolders=1)))
        self.assertFalse(uploaded(self._task(uploadedBytes=99)))
        self.assertFalse(uploaded(self._task(totalBytes=200, uploadedBytes=200), expected_size=100))

    def test_is_throttle_error(self):
        self.assertTrue(is_throttle_error(FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "quota")))
        self.assertTrue(is_throttle_error(FakeRpcError(grpc.StatusCode.INTERNAL, "115open throttle (code 405)")))
        self.assertTrue(is_throttle_error("HTTP 429"))
        self.assertTrue(is_throttle_error("HTTP 429 Too Many Requests"))
        self.assertTrue(is_throttle_error("status=429"))
        self.assertTrue(is_throttle_error("status: 429"))
        self.assertTrue(is_throttle_error("code 429"))
        self.assertTrue(is_throttle_error("429 Too Many Requests"))
        self.assertTrue(is_throttle_error("Rate Limit exceeded"))
        self.assertTrue(is_throttle_error("操作频率过快"))
        self.assertTrue(is_throttle_error(RuntimeError("upstream throttle")))
        self.assertFalse(is_throttle_error(FakeRpcError(grpc.StatusCode.NOT_FOUND, "not found")))
        self.assertFalse(is_throttle_error("permission denied"))
        self.assertFalse(is_throttle_error("/115/ep429.mkv"))
        self.assertFalse(is_throttle_error("/115/429/video.mkv"))
        self.assertFalse(is_throttle_error("/115/429.mkv"))
        self.assertFalse(is_throttle_error("file_id=14290"))
        self.assertFalse(is_throttle_error("offset 4290"))
        self.assertFalse(is_throttle_error(None))


class ThrottleGateTest(unittest.TestCase):
    def test_backoff_jitter_and_cap(self):
        now = [100.0]
        gate = ThrottleGate(base=5.0, max_backoff=20.0, clock=lambda: now[0], rng=lambda a, b: 1.1)

        d1 = gate.record_throttle(50)
        self.assertAlmostEqual(d1, 5.5)
        self.assertTrue(gate.in_cooldown())
        self.assertAlmostEqual(gate.remaining(), 5.5)
        self.assertEqual(gate.effective_batch(50), 25)

        now[0] = 106.0
        self.assertFalse(gate.in_cooldown())
        d2 = gate.record_throttle(25)
        self.assertAlmostEqual(d2, 11.0)
        self.assertEqual(gate.effective_batch(50), 12)

        now[0] = 120.0
        d3 = gate.record_throttle(12)
        self.assertAlmostEqual(d3, 22.0)  # min(20, 5*4) * 1.1
        self.assertEqual(gate.effective_batch(50), 6)

        now[0] = 150.0
        d4 = gate.record_throttle(6)
        self.assertAlmostEqual(d4, 22.0)  # capped at max_backoff * 1.1
        self.assertEqual(gate.effective_batch(50), 5)  # floor = 5

        now[0] = 180.0
        gate.record_throttle(5)
        self.assertEqual(gate.effective_batch(50), 5)

    def test_long_throttle_streak_no_overflow(self):
        now = [0.0]
        gate = ThrottleGate(base=5.0, max_backoff=120.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
        delay = 0.0
        for i in range(1150):
            now[0] = float(i * 200)
            delay = gate.record_throttle(50)
        self.assertAlmostEqual(delay, 120.0)
        self.assertEqual(gate.effective_batch(50), 5)

    def test_aimd_recovery(self):
        now = [0.0]
        gate = ThrottleGate(base=5.0, max_backoff=120.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
        self.assertEqual(gate.effective_batch(50), 50)
        self.assertEqual(gate.effective_batch(0), 0)

        gate.record_throttle(50)
        self.assertEqual(gate.effective_batch(50), 25)

        # 1st success does not raise batch yet
        gate.record_success(50)
        self.assertEqual(gate.effective_batch(50), 25)

        # 2nd consecutive success starts linear recovery
        gate.record_success(50)
        b1 = gate.effective_batch(50)
        self.assertGreater(b1, 25)
        self.assertLess(b1, 50)

        # Continued successes linearly recover all the way back to target_batch
        for _ in range(10):
            gate.record_success(50)
        self.assertEqual(gate.effective_batch(50), 50)

        # After recovery, backoff streak was reset by successes
        d = gate.record_throttle(50)
        self.assertAlmostEqual(d, 5.0)

    def test_consume_paused_secs_exact_accounting(self):
        now = [10.0]
        gate = ThrottleGate(base=5.0, max_backoff=120.0, clock=lambda: now[0], rng=lambda a, b: 1.0)
        self.assertEqual(gate.consume_paused_secs(), 0.0)

        gate.record_throttle(50)  # cooldown [10, 15]
        now[0] = 12.0
        self.assertAlmostEqual(gate.consume_paused_secs(), 2.0)
        self.assertAlmostEqual(gate.consume_paused_secs(), 0.0)

        # Advance past cooldown end (15.0) to 20.0; only [12, 15] = 3.0s should count
        now[0] = 20.0
        self.assertAlmostEqual(gate.consume_paused_secs(), 3.0)
        self.assertAlmostEqual(gate.consume_paused_secs(), 0.0)

        # Overlapping throttles without intermediate consume
        gate.record_throttle(25)  # cooldown [20, 30] (streak=2 => 10s)
        now[0] = 24.0
        gate.record_throttle(12)  # cooldown extended to [24, 44] (streak=3 => 20s)
        now[0] = 50.0
        # Total continuous cooldown was [20, 44] = 24.0s
        self.assertAlmostEqual(gate.consume_paused_secs(), 24.0)
        self.assertAlmostEqual(gate.consume_paused_secs(), 0.0)


class ParseArgsTest(unittest.TestCase):
    def _clean_env(self, **overrides):
        base = {k: v for k, v in os.environ.items() if not k.startswith("CD2_")}
        base.update(overrides)
        return patch.dict(os.environ, base, clear=True)

    def test_defaults_and_env_precedence(self):
        with self._clean_env(
            CD2_ENDPOINT="http://nas.lan:8080",
            CD2_TOKEN="env-tok",
            CD2_DELETE="permanent",
            CD2_DELETE_INTERVAL="3.5",
            CD2_DELETE_BATCH="20",
            CD2_DELETE_TIMEOUT="60",
            CD2_MISSING_GRACE="15",
            CD2_THROTTLE_BASE="8",
            CD2_THROTTLE_MAX="64",
            CD2_SWEEP="12",
            CD2_BATCH="16",
            CD2_STALL="200",
            CD2_RETRY_AFTER="90",
            CD2_MAX_ATTEMPTS="3",
            CD2_EXPECT_CLOUD="115open",
        ):
            cfg = parse_args(["run", "--src", "//123pan/movies/./a/", "--dest", "/115//inbox/"])
            self.assertIsInstance(cfg, Config)
            self.assertEqual(cfg.cmd, "run")
            self.assertEqual(cfg.endpoint, "http://nas.lan:8080")
            self.assertEqual(cfg.target, "nas.lan:8080")
            self.assertFalse(cfg.use_tls)
            self.assertEqual(cfg.token, "env-tok")
            self.assertEqual(cfg.src, "/123pan/movies/a")
            self.assertEqual(cfg.dest, "/115/inbox")
            self.assertEqual(cfg.delete, "permanent")
            self.assertEqual(cfg.delete_interval, 3.5)
            self.assertEqual(cfg.delete_batch, 20)
            self.assertEqual(cfg.delete_timeout, 60.0)
            self.assertEqual(cfg.missing_grace, 15.0)
            self.assertEqual(cfg.throttle_base, 8.0)
            self.assertEqual(cfg.throttle_max, 64.0)
            self.assertEqual(cfg.sweep, 12.0)
            self.assertEqual(cfg.batch, 16)
            self.assertEqual(cfg.stall, 200.0)
            self.assertEqual(cfg.retry_after, 90.0)
            self.assertEqual(cfg.max_attempts, 3)
            self.assertTrue(cfg.expect_cloud_re.search("115OPEN"))

            # CLI overrides env
            cfg2 = parse_args([
                "scan", "--src", "/123/x", "--endpoint", "https://cd2.example.com",
                "--token", "cli-tok", "--delete", "none", "--delete-batch", "10",
                "--batch", "0", "--yes",
            ])
            self.assertEqual(cfg2.target, "cd2.example.com:443")
            self.assertTrue(cfg2.use_tls)
            self.assertEqual(cfg2.token, "cli-tok")
            self.assertEqual(cfg2.delete, "none")
            self.assertEqual(cfg2.delete_batch, 10)
            self.assertEqual(cfg2.batch, 0)
            self.assertTrue(cfg2.yes)

    def test_ca_cert_validation(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            cert_path = tf.name
        self.addCleanup(lambda: os.path.exists(cert_path) and os.unlink(cert_path))

        with self._clean_env():
            # Valid https + existing cert
            cfg = parse_args(["clean", "--endpoint", "https://cd2.local:9443", "--ca-cert", cert_path])
            self.assertEqual(cfg.ca_cert, cert_path)
            self.assertTrue(cfg.use_tls)

            # Plaintext endpoint + --ca-cert must fail
            with self.assertRaises(SystemExit):
                parse_args(["clean", "--endpoint", "127.0.0.1:19798", "--ca-cert", cert_path])
            with self.assertRaises(SystemExit):
                parse_args(["clean", "--endpoint", "http://cd2.local:19798", "--ca-cert", cert_path])

            # Missing cert file must fail
            with self.assertRaises(SystemExit):
                parse_args(["clean", "--endpoint", "https://cd2.local", "--ca-cert", cert_path + ".missing"])

    def test_numeric_and_semantic_validations(self):
        base = ["run", "--src", "/src/a", "--dest", "/115"]
        bad_arg_lists = [
            ["run"],  # missing --src
            ["scan"],  # missing --src
            ["run", "--src", "relative/path", "--dest", "/115"],
            ["run", "--src", "/src/a", "--dest", "115"],
            ["run", "--src", "/115", "--dest", "/115"],
            ["run", "--src", "/115/sub", "--dest", "/115"],
            ["run", "--src", "/115", "--dest", "/115/sub"],
            ["clean", "--delete", "none"],
            base + ["--delete", "bogus"],
            base + ["--delete-interval", "0"],
            base + ["--delete-interval", "-1"],
            base + ["--delete-interval", "nan"],
            base + ["--delete-batch", "0"],
            base + ["--delete-timeout", "0"],
            base + ["--missing-grace", "-0.1"],
            base + ["--throttle-base", "0"],
            base + ["--throttle-base", "20", "--throttle-max", "10"],
            base + ["--sweep", "0"],
            base + ["--batch", "-1"],
            base + ["--stall", "-1"],
            base + ["--retry-after", "-1"],
            base + ["--max-attempts", "0"],
            base + ["--expect-cloud", ""],
            base + ["--expect-cloud", "   "],
            base + ["--expect-cloud", "[unclosed"],
            base + ["--state", ""],
            base + ["--state", "   "],
            ["clean", "--dest", "/"],
            ["clean", "--dest", "//"],
            ["clean", "--dest", "/115/.."],
            ["run", "--src", "/src/a", "--dest", "/"],
            base + ["--endpoint", "https://bad:99999"],
        ]
        with self._clean_env():
            for argv in bad_arg_lists:
                with self.assertRaises(SystemExit, msg=repr(argv)):
                    parse_args(argv)

    def test_empty_env_endpoint_and_blank_env_expect_cloud(self):
        with self._clean_env(CD2_ENDPOINT=""):
            cfg = parse_args(["clean"])
            self.assertEqual(cfg.endpoint, "127.0.0.1:19798")
            self.assertEqual(cfg.target, "127.0.0.1:19798")

        with self._clean_env(CD2_EXPECT_CLOUD="   "):
            with self.assertRaises(SystemExit):
                parse_args(["clean"])

    def test_zero_boundaries_allowed(self):
        with self._clean_env():
            cfg = parse_args([
                "run", "--src", "/src/a", "--dest", "/115",
                "--missing-grace", "0", "--batch", "0", "--stall", "0", "--retry-after", "0",
                "--throttle-base", "5", "--throttle-max", "5", "--delete-batch", "1", "--max-attempts", "1",
            ])
            self.assertEqual(cfg.missing_grace, 0.0)
            self.assertEqual(cfg.batch, 0)
            self.assertEqual(cfg.stall, 0.0)
            self.assertEqual(cfg.retry_after, 0.0)
            self.assertEqual(cfg.throttle_base, 5.0)
            self.assertEqual(cfg.throttle_max, 5.0)
            self.assertEqual(cfg.delete_batch, 1)
            self.assertEqual(cfg.max_attempts, 1)


if __name__ == "__main__":
    unittest.main()
