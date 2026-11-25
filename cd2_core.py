"""Core constants, path/endpoint/error helpers, throttle gate, and CLI config."""

from __future__ import annotations

import argparse
import datetime
from decimal import Decimal, InvalidOperation
import math
import os
import posixpath
import random
import re
import threading
import time
from dataclasses import dataclass
from typing import Callable, Sequence
from urllib.parse import urlsplit

import grpc

import clouddrive_pb2 as pb

VALID_STATES = ("pending", "submitted", "landed", "dropped", "kept", "stranded", "failed")
DONE = ("dropped", "kept", "stranded")
RETRY = ("failed",)
LIVE = ("submitted", "landed")
WORKING = (pb.CopyTask.Pending, pb.CopyTask.Scanning, pb.CopyTask.Scanned)
MANIFEST_FIELDS = (
    "rel", "root", "src", "size", "state", "attempts", "submit_fails",
    "cloud", "dest_root", "submitted_at", "landed_at", "upload_kind",
    "quota_bytes", "ts",
)

UTC8 = datetime.timezone(datetime.timedelta(hours=8))
QUOTA_WINDOWS = ("daily-utc8", "rolling-24h")
QUOTA_MODES = ("real", "submitted")
QUOTA_FITS = ("first", "strict")

_BYTE_UNITS = {
    "": 1,
    "b": 1,
    "k": 1000,
    "kb": 1000,
    "m": 1000**2,
    "mb": 1000**2,
    "g": 1000**3,
    "gb": 1000**3,
    "t": 1000**4,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}
_BYTE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)\s*$")

_THROTTLE_KEYWORDS = (
    "115open throttle",
    "throttle",
    "rate limit",
    "too many",
    "频率",
)
_THROTTLE_429_RE = re.compile(
    r"(?<![/\w])(?:http\s*|status\s*[:=]?\s*|code\s*[:=]?\s*)?429(?![/\w])"
)


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def normalize_path(path: str) -> str:
    """Normalize a CloudDrive2 POSIX path; must start with '/'."""
    if not isinstance(path, str) or not path.startswith("/") or "\x00" in path:
        raise ValueError(f"invalid CloudDrive2 path: {path!r}")
    norm = posixpath.normpath(path)
    # POSIX preserves exactly two leading slashes; collapse them to a single '/'.
    if norm.startswith("//"):
        norm = "/" + norm.lstrip("/")
    return norm


def paths_overlap(src: str, dest: str) -> bool:
    """Return True if normalized src and dest are equal or parent/child."""
    s = normalize_path(src)
    d = normalize_path(dest)
    if s == d or s == "/" or d == "/":
        return True
    return d.startswith(s + "/") or s.startswith(d + "/")


def base_of(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def mount_of(path: str) -> str:
    return "/" + path.strip("/").split("/")[0]


def ts_of(ts) -> int:
    return int(ts.seconds) if ts and getattr(ts, "seconds", 0) else 0


def parse_bytes(raw: str | int) -> int:
    """Parse byte size: decimal by default (K/M/G/T, KB/MB/GB/TB) plus explicit KiB/MiB/GiB/TiB."""
    if isinstance(raw, bool):
        raise ValueError(f"invalid byte size: {raw!r}")
    if isinstance(raw, int):
        if raw < 0:
            raise ValueError(f"byte size must be >= 0, got {raw!r}")
        return raw
    if not isinstance(raw, str):
        raise ValueError(f"invalid byte size: {raw!r}")
    m = _BYTE_RE.match(raw)
    if not m:
        raise ValueError(f"invalid byte size: {raw!r}")
    num_s, unit_s = m.group(1), m.group(2).lower()
    mult = _BYTE_UNITS.get(unit_s)
    if mult is None:
        raise ValueError(f"unsupported byte unit {unit_s!r} in {raw!r}")
    try:
        val = Decimal(num_s) * Decimal(mult)
    except InvalidOperation:
        raise ValueError(f"invalid byte size: {raw!r}") from None
    if not val.is_finite() or val < 0:
        raise ValueError(f"invalid byte size: {raw!r}")
    out = int(val)
    if val > 0 and out == 0:
        raise ValueError(f"byte size {raw!r} truncates to 0")
    return out


def fmt_bytes(n: int) -> str:
    """Format byte count using decimal units (1000-based) to match quota defaults."""
    sign = "-" if n < 0 else ""
    v = float(abs(n))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if v < 1000.0 or unit == "TB":
            return f"{sign}{int(v)}B" if unit == "B" else f"{sign}{v:.2f}{unit}"
        v /= 1000.0
    return f"{n}B"


def quota_window_start(now: float, window: str) -> float:
    """Return inclusive start timestamp for the active quota window."""
    if window == "daily-utc8":
        dt = datetime.datetime.fromtimestamp(now, tz=UTC8)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    if window == "rolling-24h":
        return now - 86400.0
    raise ValueError(f"unsupported quota window: {window!r}")


def next_utc8_midnight(now: float) -> float:
    """Return the next UTC+8 00:00:00 timestamp strictly after `now`."""
    dt = datetime.datetime.fromtimestamp(now, tz=UTC8)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return (day_start + datetime.timedelta(days=1)).timestamp()


def utc8_date_str(now: float) -> str:
    return datetime.datetime.fromtimestamp(now, tz=UTC8).strftime("%Y-%m-%d")


def utc8_time_str(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts, tz=UTC8).strftime("%Y-%m-%d %H:%M:%S UTC+8")


def channel_target(endpoint: str) -> tuple[str, bool]:
    """Validate 'host:port', 'http://host[:port]', or 'https://host[:port]'
    and return (gRPC target, use_tls)."""
    if not isinstance(endpoint, str) or not endpoint or any(c.isspace() for c in endpoint):
        raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}")

    if "://" not in endpoint:
        if any(ch in endpoint for ch in ("/", "?", "#", "@")):
            raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}")
        u = urlsplit(f"http://{endpoint}")
        if u.netloc.endswith(":"):
            raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}")
        try:
            port = u.port
        except ValueError:
            raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}") from None
        if not u.hostname or port is None or not (1 <= port <= 65535):
            raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}")
        host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname
        return f"{host}:{port}", False

    u = urlsplit(endpoint)
    scheme = u.scheme.lower()
    if (
        scheme not in ("http", "https")
        or not u.hostname
        or u.username is not None
        or u.password is not None
        or "@" in u.netloc
        or u.path not in ("", "/")
        or u.query
        or u.fragment
        or "?" in endpoint
        or "#" in endpoint
        or u.netloc.endswith(":")
    ):
        raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}")
    try:
        port = u.port
    except ValueError:
        raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}") from None
    if port is not None and not (1 <= port <= 65535):
        raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}")
    tls = scheme == "https"
    host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname
    return f"{host}:{port or (443 if tls else 80)}", tls


def uploaded(t, expected_size: int | None = None) -> bool:
    """Return True only when a CopyTask cleanly uploaded its file."""
    if (
        t.status != pb.CopyTask.Completed
        or t.uploadedFiles < 1
        or t.failedFiles != 0
        or t.cancelledFiles != 0
        or t.skippedFiles != 0
        or t.failedFolders != 0
    ):
        return False
    if t.totalBytes > 0:
        if t.uploadedBytes < t.totalBytes:
            return False
        if expected_size is not None and t.totalBytes != expected_size:
            return False
    return True


def is_throttle_error(err_or_msg) -> bool:
    """Detect gRPC RESOURCE_EXHAUSTED or 115open/HTTP rate-limit error text."""
    if err_or_msg is None:
        return False
    code_fn = getattr(err_or_msg, "code", None)
    if callable(code_fn):
        try:
            if code_fn() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                return True
        except Exception:
            pass
    parts: list[str] = []
    details_fn = getattr(err_or_msg, "details", None)
    if callable(details_fn):
        try:
            d = details_fn()
            if d:
                parts.append(str(d))
        except Exception:
            pass
    parts.append(str(err_or_msg))
    text = " ".join(parts).lower()
    return any(kw in text for kw in _THROTTLE_KEYWORDS) or bool(_THROTTLE_429_RE.search(text))


class ThrottleGate:
    """Exponential backoff with jitter + AIMD batch-size recovery."""

    MIN_BATCH = 5

    def __init__(
        self,
        base: float = 5.0,
        max_backoff: float = 120.0,
        clock: Callable[[], float] = time.monotonic,
        rng: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if not math.isfinite(base) or base <= 0:
            raise ValueError(f"base must be > 0, got {base!r}")
        if not math.isfinite(max_backoff) or max_backoff < base:
            raise ValueError(f"max_backoff must be >= base, got {max_backoff!r}")
        self.base = float(base)
        self.max_backoff = float(max_backoff)
        self._clock = clock
        self._rng = rng
        self._lock = threading.Lock()
        self._streak = 0
        self._success_streak = 0
        self._cap: int | None = None
        self._cooldown_until = 0.0
        self._pause_cursor: float | None = None
        self._paused_secs = 0.0

    def _account(self, now: float) -> None:
        if self._pause_cursor is None:
            return
        end = min(now, self._cooldown_until)
        if end > self._pause_cursor:
            self._paused_secs += end - self._pause_cursor
            self._pause_cursor = end
        if now >= self._cooldown_until:
            self._pause_cursor = None

    def record_throttle(self, cur_batch: int) -> float:
        with self._lock:
            now = self._clock()
            self._account(now)
            self._streak += 1
            self._success_streak = 0
            exp = min(self._streak - 1, 60)
            raw = min(self.max_backoff, self.base * (2 ** exp))
            delay = max(0.0, raw * self._rng(0.8, 1.2))
            if self._pause_cursor is None:
                self._pause_cursor = now
            self._cooldown_until = max(self._cooldown_until, now + delay)
            cur = max(1, int(cur_batch))
            if self._cap is not None:
                cur = min(cur, self._cap)
            self._cap = max(self.MIN_BATCH, cur // 2)
            return delay

    def record_success(self, target_batch: int) -> None:
        with self._lock:
            self._streak = 0
            self._success_streak += 1
            if self._cap is None:
                return
            if self._success_streak >= 2:
                self._success_streak = 0
                step = max(1, target_batch // 5)
                nxt = self._cap + step
                if nxt >= target_batch:
                    self._cap = None
                else:
                    self._cap = nxt

    def remaining(self) -> float:
        with self._lock:
            now = self._clock()
            self._account(now)
            return max(0.0, self._cooldown_until - now)

    def in_cooldown(self) -> bool:
        return self.remaining() > 0.0

    def effective_batch(self, target_batch: int) -> int:
        with self._lock:
            if target_batch <= 0:
                return 0
            if self._cap is None:
                return target_batch
            return min(target_batch, self._cap)

    def consume_paused_secs(self) -> float:
        with self._lock:
            now = self._clock()
            self._account(now)
            secs = self._paused_secs
            self._paused_secs = 0.0
            return secs


@dataclass
class Config:
    cmd: str
    endpoint: str
    target: str
    use_tls: bool
    ca_cert: str | None
    token: str | None
    user: str | None
    password: str | None
    totp: str | None
    src: str
    dest: str
    state: str
    delete: str
    delete_interval: float
    delete_batch: int
    delete_timeout: float
    missing_grace: float
    throttle_base: float
    throttle_max: float
    sweep: float
    batch: int
    stall: float
    retry_after: float
    max_attempts: int
    expect_cloud: str
    expect_cloud_re: re.Pattern[str]
    yes: bool
    daily_limit: int = 800 * 1000**3
    quota_window: str = "daily-utc8"
    quota_mode: str = "real"
    max_inflight_bytes: int = 128 * 1000**3
    quota_fit: str = "first"


def _resolve_num(cli_val, env_key: str, default, parser, flag: str):
    if cli_val is not None:
        return cli_val
    raw = os.environ.get(env_key)
    if raw is None or raw == "":
        return default
    try:
        return parser(raw)
    except ValueError:
        raise SystemExit(f"invalid {env_key}={raw!r} for {flag}") from None


def _check_finite(val: float, flag: str) -> None:
    if not math.isfinite(val):
        raise SystemExit(f"{flag} must be a finite number, got {val!r}")


def parse_args(argv: Sequence[str] | None = None) -> Config:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["scan", "run", "clean"])
    ap.add_argument("--endpoint", default=None)
    ap.add_argument("--ca-cert", default=None)
    ap.add_argument("--token", default=None)
    ap.add_argument("--user", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--totp", default=None)
    ap.add_argument("--src", default="")
    ap.add_argument("--dest", default="/115")
    ap.add_argument("--state", default="manifest.jsonl")
    ap.add_argument("--delete", default=None)
    ap.add_argument("--delete-interval", type=float, default=None)
    ap.add_argument("--delete-batch", type=int, default=None)
    ap.add_argument("--delete-timeout", type=float, default=None)
    ap.add_argument("--missing-grace", type=float, default=None)
    ap.add_argument("--throttle-base", type=float, default=None)
    ap.add_argument("--throttle-max", type=float, default=None)
    ap.add_argument("--sweep", type=float, default=None)
    ap.add_argument("--batch", type=int, default=None)
    ap.add_argument("--stall", type=float, default=None)
    ap.add_argument("--retry-after", type=float, default=None)
    ap.add_argument("--max-attempts", type=int, default=None)
    ap.add_argument("--expect-cloud", default=None)
    ap.add_argument("--daily-limit", default=None)
    ap.add_argument("--quota-window", default=None)
    ap.add_argument("--quota-mode", default=None)
    ap.add_argument("--max-inflight-bytes", default=None)
    ap.add_argument("--quota-fit", default=None)
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args(argv)

    endpoint = a.endpoint if a.endpoint is not None else (os.environ.get("CD2_ENDPOINT") or "127.0.0.1:19798")
    ca_cert = a.ca_cert if a.ca_cert is not None else (os.environ.get("CD2_CA_CERT") or None)
    token = a.token if a.token is not None else (os.environ.get("CD2_TOKEN") or None)
    user = a.user if a.user is not None else (os.environ.get("CD2_USER") or None)
    password = a.password if a.password is not None else (os.environ.get("CD2_PASSWORD") or None)
    totp = a.totp if a.totp is not None else (os.environ.get("CD2_TOTP") or None)
    delete = a.delete if a.delete is not None else (os.environ.get("CD2_DELETE") or "recycle")
    expect_cloud = a.expect_cloud if a.expect_cloud is not None else (os.environ.get("CD2_EXPECT_CLOUD") or "115")
    quota_window = a.quota_window if a.quota_window is not None else (os.environ.get("CD2_QUOTA_WINDOW") or "daily-utc8")
    quota_mode = a.quota_mode if a.quota_mode is not None else (os.environ.get("CD2_QUOTA_MODE") or "real")
    quota_fit = a.quota_fit if a.quota_fit is not None else (os.environ.get("CD2_QUOTA_FIT") or "first")

    if delete not in ("recycle", "permanent", "none"):
        raise SystemExit(f"invalid --delete mode {delete!r} (expected recycle, permanent, or none)")
    if quota_window not in QUOTA_WINDOWS:
        raise SystemExit(f"invalid --quota-window {quota_window!r} (expected {', '.join(QUOTA_WINDOWS)})")
    if quota_mode not in QUOTA_MODES:
        raise SystemExit(f"invalid --quota-mode {quota_mode!r} (expected {', '.join(QUOTA_MODES)})")
    if quota_fit not in QUOTA_FITS:
        raise SystemExit(f"invalid --quota-fit {quota_fit!r} (expected {', '.join(QUOTA_FITS)})")

    daily_limit_raw = a.daily_limit if a.daily_limit is not None else (os.environ.get("CD2_DAILY_LIMIT") or "800G")
    try:
        daily_limit = parse_bytes(daily_limit_raw)
    except ValueError as e:
        raise SystemExit(f"invalid --daily-limit {daily_limit_raw!r}: {e}") from None

    max_inflight_raw = (
        a.max_inflight_bytes
        if a.max_inflight_bytes is not None
        else (os.environ.get("CD2_MAX_INFLIGHT_BYTES") or "128G")
    )
    try:
        max_inflight_bytes = parse_bytes(max_inflight_raw)
    except ValueError as e:
        raise SystemExit(f"invalid --max-inflight-bytes {max_inflight_raw!r}: {e}") from None

    delete_interval = _resolve_num(a.delete_interval, "CD2_DELETE_INTERVAL", 2.5, float, "--delete-interval")
    delete_batch = _resolve_num(a.delete_batch, "CD2_DELETE_BATCH", 50, int, "--delete-batch")
    delete_timeout = _resolve_num(a.delete_timeout, "CD2_DELETE_TIMEOUT", 45.0, float, "--delete-timeout")
    missing_grace = _resolve_num(a.missing_grace, "CD2_MISSING_GRACE", 10.0, float, "--missing-grace")
    throttle_base = _resolve_num(a.throttle_base, "CD2_THROTTLE_BASE", 5.0, float, "--throttle-base")
    throttle_max = _resolve_num(a.throttle_max, "CD2_THROTTLE_MAX", 120.0, float, "--throttle-max")
    sweep = _resolve_num(a.sweep, "CD2_SWEEP", 20.0, float, "--sweep")
    batch = _resolve_num(a.batch, "CD2_BATCH", 32, int, "--batch")
    stall = _resolve_num(a.stall, "CD2_STALL", 300.0, float, "--stall")
    retry_after = _resolve_num(a.retry_after, "CD2_RETRY_AFTER", 180.0, float, "--retry-after")
    max_attempts = _resolve_num(a.max_attempts, "CD2_MAX_ATTEMPTS", 5, int, "--max-attempts")

    for val, flag in (
        (delete_interval, "--delete-interval"),
        (delete_timeout, "--delete-timeout"),
        (missing_grace, "--missing-grace"),
        (throttle_base, "--throttle-base"),
        (throttle_max, "--throttle-max"),
        (sweep, "--sweep"),
        (stall, "--stall"),
        (retry_after, "--retry-after"),
    ):
        _check_finite(val, flag)

    if delete_interval <= 0:
        raise SystemExit("--delete-interval must be > 0")
    if delete_batch < 1:
        raise SystemExit("--delete-batch must be >= 1")
    if delete_timeout <= 0:
        raise SystemExit("--delete-timeout must be > 0")
    if missing_grace < 0:
        raise SystemExit("--missing-grace must be >= 0")
    if throttle_base <= 0:
        raise SystemExit("--throttle-base must be > 0")
    if throttle_max < throttle_base:
        raise SystemExit("--throttle-max must be >= --throttle-base")
    if sweep <= 0:
        raise SystemExit("--sweep must be > 0")
    if batch < 0:
        raise SystemExit("--batch must be >= 0")
    if stall < 0:
        raise SystemExit("--stall must be >= 0")
    if retry_after < 0:
        raise SystemExit("--retry-after must be >= 0")
    if max_attempts < 1:
        raise SystemExit("--max-attempts must be >= 1")

    if not a.state or not a.state.strip():
        raise SystemExit("--state must not be empty")

    if not expect_cloud.strip():
        raise SystemExit("--expect-cloud must not be empty")
    try:
        expect_cloud_re = re.compile(expect_cloud, re.I)
    except re.error as e:
        raise SystemExit(f"invalid --expect-cloud regex {expect_cloud!r}: {e}") from None

    target, use_tls = channel_target(endpoint)
    if ca_cert:
        if not use_tls:
            raise SystemExit("--ca-cert requires an https:// endpoint")
        if not os.path.isfile(ca_cert):
            raise SystemExit(f"--ca-cert file not found: {ca_cert!r}")

    if a.cmd == "clean" and delete == "none":
        raise SystemExit("clean cannot be combined with --delete none")

    try:
        dest = normalize_path(a.dest)
    except ValueError as e:
        raise SystemExit(f"invalid --dest: {e}") from None
    if dest == "/":
        raise SystemExit("--dest must be a cloud mount path or subdirectory, not '/'")

    src = ""
    if a.cmd in ("scan", "run"):
        if not a.src:
            raise SystemExit("--src is required")
        try:
            src = normalize_path(a.src)
        except ValueError as e:
            raise SystemExit(f"invalid --src: {e}") from None
        if paths_overlap(src, dest):
            raise SystemExit(f"--src {src} and --dest {dest} overlap; refusing to delete inside the source")
    elif a.src:
        try:
            src = normalize_path(a.src)
        except ValueError as e:
            raise SystemExit(f"invalid --src: {e}") from None

    return Config(
        cmd=a.cmd,
        endpoint=endpoint,
        target=target,
        use_tls=use_tls,
        ca_cert=ca_cert,
        token=token,
        user=user,
        password=password,
        totp=totp,
        src=src,
        dest=dest,
        state=a.state,
        delete=delete,
        delete_interval=delete_interval,
        delete_batch=delete_batch,
        delete_timeout=delete_timeout,
        missing_grace=missing_grace,
        throttle_base=throttle_base,
        throttle_max=throttle_max,
        sweep=sweep,
        batch=batch,
        stall=stall,
        retry_after=retry_after,
        max_attempts=max_attempts,
        expect_cloud=expect_cloud,
        expect_cloud_re=expect_cloud_re,
        yes=a.yes,
        daily_limit=daily_limit,
        quota_window=quota_window,
        quota_mode=quota_mode,
        max_inflight_bytes=max_inflight_bytes,
        quota_fit=quota_fit,
    )
