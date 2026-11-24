"""Transfer pipeline and destination cleanup for CloudDrive2."""

from __future__ import annotations

import threading
import time

import grpc

import clouddrive_pb2 as pb
from cd2_client import AuthExpiredError, CD2Client
from cd2_core import (
    DONE,
    LIVE,
    WORKING,
    Config,
    ThrottleGate,
    base_of,
    is_throttle_error,
    log,
    mount_of,
    ts_of,
    uploaded,
)
from cd2_manifest import Manifest

_NON_BISECT_CODES = (
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.DEADLINE_EXCEEDED,
)


def _task_rank(t, expected_size: int | None = None) -> tuple[int, int]:
    """Higher tuple wins when multiple CopyTasks exist for the same sourcePath."""
    if uploaded(t, expected_size=expected_size):
        tier = 3
    elif t.status in WORKING and not t.paused:
        tier = 2
    elif t.status in WORKING:
        tier = 1
    else:
        tier = 0
    started = ts_of(t.startTime) if t.HasField("startTime") else 0
    return (tier, started)


def _group_tasks(tasks: list, dest_root: str) -> dict[str, list]:
    grouped: dict[str, list] = {}
    for t in tasks:
        if t.destPath.rstrip("/") == dest_root:
            grouped.setdefault(t.sourcePath, []).append(t)
    return grouped


def _pick_task(cands: list, expected_size: int | None = None):
    return max(cands, key=lambda t: _task_rank(t, expected_size=expected_size))


class Pipeline:
    """Fast/slow dual-cadence pipeline: submit -> poll -> batch-recycle -> prune."""

    DEBOUNCE_SECS = 0.3

    def __init__(
        self,
        ctl: CD2Client,
        man: Manifest,
        cfg: Config,
        gate: ThrottleGate | None = None,
    ) -> None:
        self.ctl = ctl
        self.man = man
        self.cfg = cfg
        self.gate = (
            gate
            if gate is not None
            else ThrottleGate(base=cfg.throttle_base, max_backoff=cfg.throttle_max)
        )
        self.trigger = threading.Event()
        self.stop = threading.Event()
        self.dropped = 0
        self.kept = 0
        self.stranded = 0
        self.prune: set[str] = set()
        self.cloud = ""
        self.gap = False
        self.rotated_at = 0.0
        self.more = False
        self.why: str | None = None
        self.due: list[tuple[dict, str, str]] = []

        # Volatile runtime state isolated from Manifest records (keyed by rel).
        self._rt: dict[str, dict] = {}
        self._dest_checked_at = 0.0
        self._dest_cached_why: str | None = None
        self._rotated_this_step = False
        self._auth_error: AuthExpiredError | None = None

        self._stream_lock = threading.Lock()
        self._stream = None
        self._wake_lock = threading.Lock()
        self._debounce_timer: threading.Timer | None = None
        self._last_copy_count: int | None = None
        self._slow_requested = threading.Event()

    def _rt_of(self, rel: str) -> dict:
        return self._rt.setdefault(rel, {})

    def invalidate_dest_cache(self) -> None:
        self._dest_checked_at = 0.0

    # --- PushMessage listener ----------------------------------------------

    def _should_wake(self, msg: pb.CloudDrivePushMessage) -> bool:
        mtype = msg.messageType
        if mtype in (
            pb.CloudDrivePushMessage.COPY_TASK_COUNT,
            pb.CloudDrivePushMessage.UPLOADER_COUNT,
            pb.CloudDrivePushMessage.DOWNLOADER_COUNT,
        ) and msg.HasField("transferTaskStatus"):
            status = msg.transferTaskStatus
            finish_copy = any(
                u.statusEnum == pb.UploadFileInfo.Finish
                and u.operatorType == pb.UploadFileInfo.Copy
                for u in status.uploadFileStatusChanges
            )
            count = status.copyTaskCount
            count_changed = (
                self._last_copy_count is not None and count != self._last_copy_count
            )
            self._last_copy_count = count
            return finish_copy or count_changed

        if (
            mtype == pb.CloudDrivePushMessage.FILE_SYSTEM_CHANGE
            and msg.HasField("fileSystemChange")
        ):
            fsc = msg.fileSystemChange
            dest_prefix = f"{self.man.dest_root.rstrip('/')}/"
            return (
                fsc.changeType == pb.FileSystemChange.CREATE
                and not fsc.isDirectory
                and fsc.path.startswith(dest_prefix)
            )

        if mtype in (
            pb.CloudDrivePushMessage.MOUNT_POINT_CHANGE,
            pb.CloudDrivePushMessage.CLOUD_API_CHANGE,
        ):
            self.invalidate_dest_cache()
            self._slow_requested.set()
            return True

        if mtype == pb.CloudDrivePushMessage.FORCE_EXIT:
            detail = msg.exitedMessage.message if msg.HasField("exitedMessage") else ""
            log(f"push listener received FORCE_EXIT{f': {detail}' if detail else ''}")
            return False

        return False

    def _fire_wake(self) -> None:
        if not self.stop.is_set():
            self.trigger.set()

    def _schedule_wake(self) -> None:
        delay = self.DEBOUNCE_SECS
        if delay <= 0:
            self._fire_wake()
            return
        with self._wake_lock:
            if self._debounce_timer is not None and self._debounce_timer.is_alive():
                return
            t = threading.Timer(delay, self._fire_wake)
            t.daemon = True
            self._debounce_timer = t
            t.start()

    def _cancel_stream(self) -> None:
        with self._wake_lock:
            timer = self._debounce_timer
            self._debounce_timer = None
        if timer is not None:
            timer.cancel()
        with self._stream_lock:
            stream = self._stream
            self._stream = None
        if stream is not None:
            cancel = getattr(stream, "cancel", None)
            if callable(cancel):
                try:
                    cancel()
                except Exception:
                    pass

    def listen(self) -> None:
        backoff = 1.0
        while not self.stop.is_set():
            stream = None
            try:
                stream = self.ctl.push_messages()
                with self._stream_lock:
                    if self.stop.is_set():
                        cancel = getattr(stream, "cancel", None)
                        if callable(cancel):
                            cancel()
                        return
                    self._stream = stream
                for msg in stream:
                    if self.stop.is_set():
                        break
                    backoff = 1.0
                    if self._should_wake(msg):
                        self._schedule_wake()
            except AuthExpiredError as e:
                log(f"push listener stopped: {e}")
                self._auth_error = e
                self.stop.set()
                self.trigger.set()
                return
            except grpc.RpcError as e:
                if self.stop.is_set():
                    return
                if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                    log(
                        f"PushMessage is unimplemented ({e.details()}); "
                        f"disabling push listener and falling back to timers"
                    )
                    return
                if e.code() == grpc.StatusCode.UNAUTHENTICATED and not self.ctl.creds:
                    err = AuthExpiredError(
                        f"PushMessage unauthorized without stored credentials: {e.details()}"
                    )
                    log(f"push listener stopped: {err}")
                    self._auth_error = err
                    self.stop.set()
                    self.trigger.set()
                    return
                log(f"push listener disconnected ({e.code().name} {e.details()}); retry in {backoff:.0f}s")
                self.stop.wait(backoff)
                backoff = min(backoff * 2.0, 30.0)
            except Exception as e:
                if self.stop.is_set():
                    return
                log(f"push listener error ({e}); retry in {backoff:.0f}s")
                self.stop.wait(backoff)
                backoff = min(backoff * 2.0, 30.0)
            finally:
                with self._stream_lock:
                    if self._stream is stream:
                        self._stream = None

    # --- Destination & account rotation ------------------------------------

    def dest_ready(self, force: bool = False) -> str | None:
        """Return None when dest is ready; cached for one sweep interval."""
        now_mono = time.monotonic()
        if (
            not force
            and self._dest_checked_at > 0.0
            and self._dest_cached_why is None
            and (now_mono - self._dest_checked_at) < self.cfg.sweep
        ):
            return None

        dest = self.man.dest_root
        api, why, absent = self.ctl.cloud_of(dest)
        if why:
            self.gap = self.gap or absent
            self._dest_cached_why = why
            self._dest_checked_at = 0.0
            return why
        if not self.cfg.expect_cloud_re.search(api.name):
            why = f"cloud is {api.name!r}, expected /{self.cfg.expect_cloud}/"
            self._dest_cached_why = why
            self._dest_checked_at = 0.0
            return why
        if dest != mount_of(dest):
            try:
                ok, err = self.ctl.ensure_dir(dest)
                if not ok:
                    why = err or "cannot create the destination folder"
                    self._dest_cached_why = why
                    self._dest_checked_at = 0.0
                    return why
            except grpc.RpcError as e:
                why = f"create failed: {e.code().name}"
                self._dest_cached_why = why
                self._dest_checked_at = 0.0
                return why

        account = f"{api.name}|{api.userName}"
        if self.cloud and (account != self.cloud or self.gap):
            self.rotated(account)
        elif not self.cloud:
            log(f"staging on {account}")
        self.cloud = account
        self.gap = False
        self._dest_cached_why = None
        self._dest_checked_at = now_mono
        return None

    def rotated(self, account: str) -> None:
        """Persist fresh retry budgets and drop old-account landed entries."""
        log(f"destination account is now {account} (was {self.cloud}); resuming")
        self.rotated_at = time.time()
        self._rotated_this_step = True
        self._dest_checked_at = 0.0
        updates: list[tuple[dict, str, dict]] = []
        for r in self.man.of("failed"):
            updates.append((r, "pending", {"attempts": 0, "submit_fails": 0}))
            self._rt.pop(r["rel"], None)
        for r in self.man.of("pending"):
            updates.append((r, "pending", {"attempts": 0, "submit_fails": 0}))
        for r in self.man.of("submitted"):
            updates.append((r, "submitted", {"attempts": 0, "submit_fails": 0}))
            rt = self._rt.get(r["rel"])
            if rt and rt.get("failed_at"):
                rt["failed_at"] = 1
        for r in self.man.of("landed"):
            if not r.get("cloud") or r["cloud"] != account:
                updates.append((r, "dropped", {}))
                self.dropped += 1
                self._rt.pop(r["rel"], None)
                log(f"gone with previous account/dest {r['rel']}")
        self.man.set_batch(updates)

    # --- Landed cleanup (batch DeleteFiles + bisect + ThrottleGate) --------

    def _adjust_stall_for_cooldown(self) -> None:
        paused = self.gate.consume_paused_secs()
        if paused <= 0.0:
            return
        for rt in self._rt.values():
            if "clear_since" in rt:
                rt["clear_since"] += paused

    def _record_clear_failure(self, r: dict, now: float, why: str) -> None:
        rt = self._rt_of(r["rel"])
        rt.setdefault("clear_since", now)
        rt["retry"] = rt.get("retry", 0) + 1
        if rt["retry"] in (1, 5, 50):
            log(f"wait {r['rel']}: {why} (x{rt['retry']})")
        if now - rt["clear_since"] >= self.cfg.stall:
            self.man.set(r, "stranded")
            self.stranded += 1
            self._rt.pop(r["rel"], None)
            log(f"stranded {r['rel']}: could not clear the landed copy ({why})")

    def _mark_dropped_batch(self, items: list[tuple[dict, str]]) -> None:
        if not items:
            return
        self.man.set_batch([(r, "dropped", {}) for r, _ in items])
        for r, _ in items:
            self.dropped += 1
            self._rt.pop(r["rel"], None)
            log(f"[{self.dropped}] {r['rel']} ({r['size']} B), cleared from 115")

    def _bisect_delete(self, chunk: list[tuple[dict, str]], now: float) -> bool:
        """Recursively split a failing batch down to delete_one; False on throttle/session error."""
        permanent = self.cfg.delete == "permanent"
        if len(chunk) == 1:
            r, path = chunk[0]
            try:
                res = self.ctl.delete_one(path, permanent=permanent, timeout=self.cfg.delete_timeout)
            except grpc.RpcError as e:
                if is_throttle_error(e):
                    delay = self.gate.record_throttle(1)
                    log(f"delete throttled on {r['rel']} ({e.code().name}); cooldown {delay:.1f}s")
                    return False
                if e.code() in _NON_BISECT_CODES:
                    self.invalidate_dest_cache()
                    self._record_clear_failure(r, now, f"{e.code().name} {e.details()}")
                    return False
                self._record_clear_failure(r, now, f"{e.code().name} {e.details()}")
                return True
            if res.success:
                self._mark_dropped_batch([(r, path)])
                return True
            if is_throttle_error(res.errorMessage):
                delay = self.gate.record_throttle(1)
                log(f"delete throttled on {r['rel']} ({res.errorMessage}); cooldown {delay:.1f}s")
                return False
            self._record_clear_failure(r, now, f"delete refused: {res.errorMessage}")
            return True

        mid = len(chunk) // 2
        for half in (chunk[:mid], chunk[mid:]):
            if len(half) == 1:
                if not self._bisect_delete(half, now):
                    return False
                continue
            paths = [p for _, p in half]
            try:
                res = self.ctl.delete_batch(
                    paths, permanent=permanent, timeout=self.cfg.delete_timeout
                )
            except grpc.RpcError as e:
                if is_throttle_error(e):
                    delay = self.gate.record_throttle(len(paths))
                    log(f"delete_batch throttled ({e.code().name}); cooldown {delay:.1f}s")
                    return False
                if e.code() in _NON_BISECT_CODES:
                    self.invalidate_dest_cache()
                    for r, _ in half:
                        self._record_clear_failure(r, now, f"{e.code().name} {e.details()}")
                    return False
                if not self._bisect_delete(half, now):
                    return False
                continue
            if res.success:
                self._apply_batch_result(half, res, now, record_gate_success=False)
                continue
            if is_throttle_error(res.errorMessage):
                delay = self.gate.record_throttle(len(paths))
                log(f"delete_batch throttled ({res.errorMessage}); cooldown {delay:.1f}s")
                return False
            if not self._bisect_delete(half, now):
                return False
        return True

    def _apply_batch_result(
        self,
        chunk: list[tuple[dict, str]],
        res: pb.FileOperationResult,
        now: float,
        record_gate_success: bool = True,
    ) -> None:
        if res.resultFilePaths:
            ok_paths = set(res.resultFilePaths)
            succeeded = [(r, p) for r, p in chunk if p in ok_paths]
            failed = [(r, p) for r, p in chunk if p not in ok_paths]
            self._mark_dropped_batch(succeeded)
            if succeeded and record_gate_success:
                self.gate.record_success(self.cfg.delete_batch)
            for r, _ in failed:
                self._record_clear_failure(
                    r, now, res.errorMessage or "path missing from DeleteFiles result"
                )
        else:
            self._mark_dropped_batch(chunk)
            if record_gate_success:
                self.gate.record_success(self.cfg.delete_batch)

    def _delete_chunk(self, chunk: list[tuple[dict, str]], now: float) -> bool:
        """Delete one batch via DeleteFiles; False means throttled so abort sweep."""
        paths = [p for _, p in chunk]
        permanent = self.cfg.delete == "permanent"
        try:
            res = self.ctl.delete_batch(
                paths, permanent=permanent, timeout=self.cfg.delete_timeout
            )
        except grpc.RpcError as e:
            if is_throttle_error(e):
                delay = self.gate.record_throttle(len(paths))
                log(f"delete_batch throttled ({e.code().name}); cooldown {delay:.1f}s")
                return False
            if e.code() in _NON_BISECT_CODES:
                self.invalidate_dest_cache()
                for r, _ in chunk:
                    self._record_clear_failure(r, now, f"{e.code().name} {e.details()}")
                return False
            return self._bisect_delete(chunk, now)

        if res.success:
            self._apply_batch_result(chunk, res, now, record_gate_success=True)
            return True
        if is_throttle_error(res.errorMessage):
            delay = self.gate.record_throttle(len(paths))
            log(f"delete_batch throttled ({res.errorMessage}); cooldown {delay:.1f}s")
            return False
        return self._bisect_delete(chunk, now)

    def clear_landed(self) -> None:
        self._adjust_stall_for_cooldown()
        if self.gate.in_cooldown():
            return
        landed = self.man.of("landed")
        if not landed:
            return

        now = time.time()
        instant_updates: list[tuple[dict, str, dict]] = []
        to_delete: list[tuple[dict, str]] = []
        throttled = False

        for r in landed:
            if (bool(r.get("dest_root")) and r["dest_root"] != self.man.dest_root) or (
                r.get("cloud") and self.cloud and r["cloud"] != self.cloud
            ):
                instant_updates.append((r, "dropped", {}))
                self.dropped += 1
                self._rt.pop(r["rel"], None)
                log(f"gone with previous account/dest {r['rel']}")
                continue

            if self.cfg.delete == "none":
                instant_updates.append((r, "kept", {}))
                self.kept += 1
                self._rt.pop(r["rel"], None)
                log(f"ok {r['rel']} ({r['size']} B, left in place)")
                continue

            path = f"{self.man.dest_root}/{base_of(r['src'])}"
            try:
                f = self.ctl.find(path)
            except grpc.RpcError as e:
                if is_throttle_error(e):
                    delay = self.gate.record_throttle(self.gate.effective_batch(self.cfg.delete_batch))
                    log(f"find throttled ({e.code().name}); cooldown {delay:.1f}s")
                    throttled = True
                    break
                if e.code() in _NON_BISECT_CODES:
                    self.invalidate_dest_cache()
                self._record_clear_failure(r, now, f"{e.code().name} {e.details()}")
                continue

            if f is None:
                rt = self._rt_of(r["rel"])
                rt["miss_count"] = rt.get("miss_count", 0) + 1
                landed_at = r.get("landed_at") or r.get("ts") or 0
                if (now - landed_at) >= self.cfg.missing_grace and rt["miss_count"] >= 2:
                    instant_updates.append((r, "dropped", {}))
                    self.dropped += 1
                    self._rt.pop(r["rel"], None)
                    log(f"gone already {r['rel']}")
                continue

            if f.isDirectory or f.size != r["size"]:
                instant_updates.append((r, "stranded", {}))
                self.stranded += 1
                self._rt.pop(r["rel"], None)
                log(f"{r['rel']} landed under another name/size; left on 115")
                continue

            if self.cfg.delete == "permanent" and not f.canDeletePermanently:
                instant_updates.append((r, "stranded", {}))
                self.stranded += 1
                self._rt.pop(r["rel"], None)
                log(f"stranded {r['rel']}: permanent delete not supported on destination")
                continue

            rt = self._rt.get(r["rel"])
            if rt is not None:
                rt.pop("miss_count", None)
            to_delete.append((r, path))

        if instant_updates:
            self.man.set_batch(instant_updates)
        if throttled or not to_delete:
            return

        idx = 0
        while idx < len(to_delete):
            if self.gate.in_cooldown():
                break
            bs = max(1, self.gate.effective_batch(self.cfg.delete_batch))
            chunk = to_delete[idx : idx + bs]
            idx += len(chunk)
            if not self._delete_chunk(chunk, now):
                break

    # --- Task harvesting, polling, submission, retry, reconcile, prune -----

    def ours(self, r: dict, t) -> bool:
        return r.get("dest_root") == self.man.dest_root and (
            not t.totalBytes or t.totalBytes == r["size"]
        )

    def _park(self, r: dict, why: str, task_keys: list[str] | None = None) -> None:
        self.man.set(r, "failed")
        self._rt.pop(r["rel"], None)
        keys = task_keys if task_keys else [f"{r['src']}:{self.man.dest_root}"]
        self.ctl.remove_tasks(keys)
        log(f"parked {r['rel']}: {why}")

    def bump(self, r: dict, why: str, task_keys: list[str] | None = None) -> None:
        fails = r.get("submit_fails", 0) + 1
        r["submit_fails"] = fails
        if fails >= self.cfg.max_attempts:
            self._park(r, why, task_keys=task_keys)
        else:
            log(f"blocked {r['rel']}: {why} ({fails}/{self.cfg.max_attempts})")

    def harvest_completed(self, tasks: list) -> int:
        """Pure-local harvest of newly completed CopyTasks -> landed."""
        now = time.time()
        by_src = self.man.by_src()
        grouped = _group_tasks(tasks, self.man.dest_root)
        updates: list[tuple[dict, str, dict]] = []
        for src, cands in grouped.items():
            r = by_src.get(src)
            if r is None or r["state"] not in ("submitted", "failed"):
                continue
            best = _pick_task(cands, expected_size=r["size"])
            if not uploaded(best, expected_size=r["size"]):
                continue
            if r["state"] == "failed" and not self.ours(r, best):
                continue
            updates.append((r, "landed", {"landed_at": int(now)}))
            self._rt.pop(r["rel"], None)
            for t in cands:
                self.prune.add(f"{t.sourcePath}:{t.destPath}")
        if updates:
            self.man.set_batch(updates)
        return len(updates)

    def poll_tasks(self, tasks: list, ready: bool = True) -> set[str]:
        now = time.time()
        by_src = self.man.by_src()
        grouped = _group_tasks(tasks, self.man.dest_root)
        seen: set[str] = set()
        self.due = []
        landed_updates: list[tuple[dict, str, dict]] = []

        for src, cands in grouped.items():
            r = by_src.get(src)
            if r is None or r["state"] not in ("submitted", "failed"):
                continue
            best = _pick_task(cands, expected_size=r["size"])
            if r["state"] == "failed" and not (
                uploaded(best, expected_size=r["size"]) and self.ours(r, best)
            ):
                continue
            seen.add(src)
            rt = self._rt_of(r["rel"])
            rt["seen"] = now

            if uploaded(best, expected_size=r["size"]):
                landed_updates.append((r, "landed", {"landed_at": int(now)}))
                self._rt.pop(r["rel"], None)
                for t in cands:
                    self.prune.add(f"{t.sourcePath}:{t.destPath}")
                continue

            if best.paused:
                if not rt.get("failed_at"):
                    log(f"paused {r['rel']}: task is paused (status {best.status})")
                    rt["failed_at"] = now
                if ready and (now - rt["failed_at"]) >= self.cfg.retry_after:
                    self.due.append((r, "task is paused", best.destPath))
                continue

            if best.status in WORKING:
                rt["failed_at"] = 0
                continue

            msg = "; ".join(e.message for e in best.errors[-2:]) or f"task status {best.status}"
            rt["failed_at"] = rt.get("failed_at") or now
            if ready and (now - rt["failed_at"]) >= self.cfg.retry_after:
                self.due.append((r, msg, best.destPath))

        if landed_updates:
            self.man.set_batch(landed_updates)
        return seen

    def _retry(self, r: dict, why: str, dest_path: str | None = None) -> None:
        target_dest = dest_path or self.man.dest_root
        if r["attempts"] >= self.cfg.max_attempts:
            self._park(r, why, task_keys=[f"{r['src']}:{target_dest}"])
            return
        log(f"retry {r['rel']} (attempt {r['attempts'] + 1}): {why}")
        self.man.set(r, "submitted", attempts=r["attempts"] + 1, cloud=self.cloud)
        rt = self._rt_of(r["rel"])
        rt["failed_at"] = 0
        rt["seen"] = time.time()
        try:
            self.ctl.restart_task(r["src"], target_dest)
        except grpc.RpcError as e:
            log(f"  restart failed: {e.code().name}")

    def restart_due(self) -> None:
        due, self.due = self.due, []
        cap = self.cfg.batch if self.cfg.batch > 0 else len(due)
        for r, msg, dest_path in due[:cap]:
            if r["state"] == "submitted":
                self._retry(r, msg, dest_path=dest_path)
        self.more = self.more or len(due) > cap

    def reconcile(self, seen: set[str]) -> None:
        now = time.time()
        for r in self.man.of("submitted"):
            if r["src"] in seen:
                continue
            rt = self._rt.get(r["rel"], {})
            last = rt.get("seen") or r.get("submitted_at") or r.get("ts", 0)
            moved = bool(self.cloud and r.get("cloud") and r["cloud"] != self.cloud)
            if last < self.rotated_at or moved:
                self.man.set(r, "pending")
                self._rt.pop(r["rel"], None)
                log(f"task lost with the previous account, requeue {r['rel']}")
            elif now - last < self.cfg.stall:
                continue
            elif r["attempts"] >= self.cfg.max_attempts:
                self._park(r, f"task vanished {int(now - last)}s ago")
            else:
                self.man.set(r, "pending", attempts=r["attempts"] + 1)
                self._rt.pop(r["rel"], None)
                log(f"task vanished, requeue {r['rel']}")

    def submit_pending(self, tasks: list) -> None:
        grouped = _group_tasks(tasks, self.man.dest_root)
        lanes = {base_of(r["src"]) for r in self.man.of(*LIVE)}
        # Keep failed records with uncleaned engine tasks out of active basename lanes.
        for r in self.man.of("failed"):
            if r["src"] in grouped:
                lanes.add(base_of(r["src"]))

        pending = self.man.of("pending")
        cap = self.cfg.batch if self.cfg.batch > 0 else len(pending)
        batch: list[dict] = []
        for r in pending:
            if len(batch) >= cap:
                break
            b = base_of(r["src"])
            if b in lanes:
                continue
            lanes.add(b)
            batch.append(r)
        if not batch:
            return

        ok_n = 0
        to_submit: list[dict] = []
        now = time.time()

        for r in batch:
            cands = grouped.get(r["src"])
            if cands:
                best = _pick_task(cands, expected_size=r["size"])
                old_account_uncompleted = bool(
                    r.get("cloud")
                    and self.cloud
                    and r["cloud"] != self.cloud
                    and not uploaded(best, expected_size=r["size"])
                )
                if self.ours(r, best) and not old_account_uncompleted:
                    started = ts_of(best.startTime) if best.HasField("startTime") else 0
                    log(f"task already exists for {r['rel']} (status {best.status}), adopting")
                    self.man.set(
                        r,
                        "submitted",
                        dest_root=self.man.dest_root,
                        submitted_at=int(started or now),
                        cloud=self.cloud,
                        submit_fails=0,
                    )
                    rt = self._rt_of(r["rel"])
                    rt["seen"] = now
                    rt["failed_at"] = 0
                    ok_n += 1
                    continue

                log(f"existing task for {r['rel']} is not reusable, invalidating it first")
                keys = [f"{t.sourcePath}:{t.destPath}" for t in cands]
                self.ctl.remove_tasks(keys)
                try:
                    still = [
                        t
                        for t in self.ctl.tasks()
                        if t.sourcePath == r["src"] and t.destPath.rstrip("/") == self.man.dest_root
                    ]
                except grpc.RpcError as e:
                    still = [e.code().name]
                if still:
                    self.bump(r, "a foreign task could not be invalidated", task_keys=keys)
                    continue
            to_submit.append(r)

        # Batch pre-delete same-named stale files in dest_root before submitting.
        stale_paths: list[str] = []
        stale_owner: dict[str, dict] = {}
        ready_to_submit: list[dict] = []
        aborted = False
        for r in to_submit:
            stale = f"{self.man.dest_root}/{base_of(r['src'])}"
            try:
                f = self.ctl.find(stale)
            except grpc.RpcError as e:
                if is_throttle_error(e):
                    delay = self.gate.record_throttle(
                        self.gate.effective_batch(self.cfg.delete_batch)
                    )
                    log(f"staging check throttled ({e.code().name}); cooldown {delay:.1f}s")
                    break
                if e.code() in _NON_BISECT_CODES:
                    log(f"staging check aborted ({e.code().name}); invalidating dest cache")
                    self.invalidate_dest_cache()
                    aborted = True
                    break
                self.bump(r, f"staging check failed: {e.code().name}")
                continue
            if f is not None and f.isDirectory:
                self.bump(r, f"a directory owns {base_of(stale)} in {self.man.dest_root}")
                continue
            if f is not None and self.cfg.delete != "none":
                if self.cfg.delete == "permanent" and not f.canDeletePermanently:
                    self.bump(r, f"cannot permanently clear {base_of(stale)}")
                    continue
                stale_paths.append(stale)
                stale_owner[stale] = r
            ready_to_submit.append(r)

        if aborted:
            stale_paths = []
            ready_to_submit = []
        elif stale_paths and self.gate.in_cooldown():
            deferred_rels = {stale_owner[p]["rel"] for p in stale_paths}
            ready_to_submit = [r for r in ready_to_submit if r["rel"] not in deferred_rels]
            stale_paths = []

        if stale_paths:
            permanent = self.cfg.delete == "permanent"
            blocked_rels: set[str] = set()
            try:
                res = self.ctl.delete_batch(
                    stale_paths, permanent=permanent, timeout=self.cfg.delete_timeout
                )
            except grpc.RpcError as e:
                if is_throttle_error(e):
                    delay = self.gate.record_throttle(len(stale_paths))
                    log(f"pre-delete throttled ({e.code().name}); cooldown {delay:.1f}s")
                    for stale in stale_paths:
                        blocked_rels.add(stale_owner[stale]["rel"])
                elif e.code() in _NON_BISECT_CODES:
                    log(f"pre-delete aborted ({e.code().name}); invalidating dest cache")
                    self.invalidate_dest_cache()
                    aborted = True
                else:
                    for stale in stale_paths:
                        r = stale_owner[stale]
                        self.bump(r, f"cannot clear {base_of(stale)}: {e.code().name}")
                        blocked_rels.add(r["rel"])
            else:
                if res.success:
                    if res.resultFilePaths:
                        ok_paths = set(res.resultFilePaths)
                        for stale in stale_paths:
                            if stale not in ok_paths:
                                r = stale_owner[stale]
                                self.bump(r, f"cannot clear {base_of(stale)}: not deleted")
                                blocked_rels.add(r["rel"])
                    cleared = len(stale_paths) - len(blocked_rels)
                    if cleared:
                        log(f"cleared {cleared} same-named file(s) before submitting")
                elif is_throttle_error(res.errorMessage):
                    delay = self.gate.record_throttle(len(stale_paths))
                    log(f"pre-delete throttled ({res.errorMessage}); cooldown {delay:.1f}s")
                    for stale in stale_paths:
                        blocked_rels.add(stale_owner[stale]["rel"])
                else:
                    for stale in stale_paths:
                        r = stale_owner[stale]
                        self.bump(r, f"cannot clear {base_of(stale)}: {res.errorMessage}")
                        blocked_rels.add(r["rel"])
            if aborted:
                ready_to_submit = []
            elif blocked_rels:
                ready_to_submit = [r for r in ready_to_submit if r["rel"] not in blocked_rels]

        for r in ready_to_submit:
            now = time.time()
            had_fails = r.get("submit_fails", 0) != 0
            self.man.set(
                r,
                "submitted",
                dest_root=self.man.dest_root,
                submitted_at=int(now),
                cloud=self.cloud,
            )
            rt = self._rt_of(r["rel"])
            rt["seen"] = now
            rt["failed_at"] = 0
            try:
                ok, err = self.ctl.submit_one(r["src"], self.man.dest_root)
            except grpc.RpcError as e:
                log(f"submit {r['rel']} outcome unknown ({e.code().name}); left submitted")
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    break
                continue
            if ok:
                if had_fails:
                    self.man.set(r, "submitted", submit_fails=0)
                ok_n += 1
            else:
                self.man.set(r, "pending", submitted_at=0)
                self.bump(r, f"submit rejected: {err}")

        left = len(self.man.of("pending"))
        self.more = self.more or (self.cfg.batch > 0 and ok_n == cap and left > 0)
        log(f"submitted {ok_n}/{len(batch)}{f', {left} queued' if left else ''}")

    def prune_done(self) -> None:
        if not self.prune:
            return
        keys = list(self.prune)
        for i in range(0, len(keys), 200):
            chunk = keys[i : i + 200]
            ok, failed = self.ctl.remove_tasks(chunk)
            if ok:
                succeeded = set(chunk) - set(failed)
                self.prune.difference_update(succeeded)

    # --- Fast/slow cadence & main loop -------------------------------------

    def fast_step(self) -> None:
        self.ctl.keep_login()
        try:
            tasks = self.ctl.tasks()
        except grpc.RpcError as e:
            log(f"GetCopyTasks failed: {e.code().name} {e.details()}")
            return
        self.harvest_completed(tasks)
        self._adjust_stall_for_cooldown()
        if self.why is None and self.man.of("landed") and not self.gate.in_cooldown():
            self.clear_landed()
        self.prune_done()

    def slow_step(self, force_dest: bool = False) -> None:
        self.more = False
        self.due = []
        self._rotated_this_step = False
        self.ctl.keep_login()
        why = self.dest_ready(force=force_dest)
        if why != self.why:
            log(f"dest {self.man.dest_root}: {why or 'ready'}")
            self.why = why
        try:
            tasks = self.ctl.tasks()
        except grpc.RpcError as e:
            log(f"GetCopyTasks failed: {e.code().name} {e.details()}")
            return
        seen = self.poll_tasks(tasks, ready=why is None)
        self.reconcile(seen)
        if why is None:
            self.clear_landed()
            self.restart_due()
            if self._rotated_this_step:
                try:
                    tasks = self.ctl.tasks()
                except grpc.RpcError as e:
                    log(f"GetCopyTasks refresh after rotation failed: {e.code().name} {e.details()}")
                    tasks = []
            self.submit_pending(tasks)
        self.prune_done()

    def run(self) -> int:
        threading.Thread(target=self.listen, daemon=True).start()
        log(
            f"{self.man.src_root} (read-only) -> {self.man.dest_root} | "
            f"after-landing={self.cfg.delete} delete-interval={self.cfg.delete_interval}s "
            f"sweep={self.cfg.sweep}s"
        )
        next_slow = 0.0
        next_fast = 0.0
        try:
            while not self.stop.is_set():
                self.trigger.clear()
                force_slow = self._slow_requested.is_set()
                if force_slow:
                    self._slow_requested.clear()
                now = time.monotonic()
                if self.more or force_slow or now >= next_slow:
                    self.slow_step(force_dest=force_slow)
                    after = time.monotonic()
                    next_slow = after + self.cfg.sweep
                    next_fast = after + self.cfg.delete_interval
                else:
                    self.fast_step()
                    next_fast = time.monotonic() + self.cfg.delete_interval

                if self._auth_error is not None:
                    raise self._auth_error

                total = len(self.man.recs)
                done = len(self.man.of(*DONE))
                outstanding = len(self.man.of("pending", *LIVE))
                parked = len(self.man.of("failed"))
                if outstanding == 0 and parked == 0:
                    log(
                        f"finished: {total} files, {self.dropped} cleared from 115, "
                        f"{self.kept} kept, {self.stranded} stranded"
                    )
                    return 0
                log(
                    f"progress {done}/{total} | pending {len(self.man.of('pending'))} "
                    f"submitted {len(self.man.of('submitted'))} landed {len(self.man.of('landed'))} "
                    f"parked {parked}{' (waiting for another 115 account)' if outstanding == 0 else ''}"
                )
                if self.more:
                    continue
                wait = max(0.0, min(next_slow, next_fast) - time.monotonic())
                if self.gate.in_cooldown() and self.man.of("landed"):
                    rem = self.gate.remaining()
                    if rem > 0:
                        wait = min(wait, rem)
                self.trigger.wait(wait)
            if self._auth_error is not None:
                raise self._auth_error
            return 0
        except KeyboardInterrupt:
            log("interrupted; shutting down cleanly")
            return 130
        except AuthExpiredError as e:
            log(f"authentication expired: {e}")
            return 1
        finally:
            self.stop.set()
            self._cancel_stream()
            self.man.close()
            self.ctl.close()


def _bisect_clean(
    ctl: CD2Client,
    paths: list[str],
    permanent: bool,
    timeout: float,
    gate: ThrottleGate,
) -> tuple[int, int, bool]:
    """Recursively bisect a failing clean batch down to delete_one.

    Returns (deleted_count, failed_count, aborted_by_throttle_or_session_error).
    """
    if not paths:
        return (0, 0, False)
    if len(paths) == 1:
        path = paths[0]
        try:
            res = ctl.delete_one(path, permanent=permanent, timeout=timeout)
        except grpc.RpcError as e:
            if is_throttle_error(e):
                gate.record_throttle(1)
                log(f"clean throttled on {path} ({e.code().name})")
                return (0, 1, True)
            if e.code() in _NON_BISECT_CODES:
                log(f"clean failed on {path}: {e.code().name} {e.details()}")
                return (0, 1, True)
            log(f"clean failed on {path}: {e.code().name} {e.details()}")
            return (0, 1, False)
        if res.success:
            return (1, 0, False)
        if is_throttle_error(res.errorMessage):
            gate.record_throttle(1)
            log(f"clean throttled on {path} ({res.errorMessage})")
            return (0, 1, True)
        log(f"clean refused on {path}: {res.errorMessage}")
        return (0, 1, False)

    mid = len(paths) // 2
    deleted = 0
    failed = 0
    halves = (paths[:mid], paths[mid:])
    for i, half in enumerate(halves):
        if len(half) == 1:
            d, f, stop = _bisect_clean(ctl, half, permanent, timeout, gate)
            deleted += d
            failed += f
            if stop:
                for rem in halves[i + 1 :]:
                    failed += len(rem)
                return (deleted, failed, True)
            continue
        try:
            res = ctl.delete_batch(half, permanent=permanent, timeout=timeout)
        except grpc.RpcError as e:
            if is_throttle_error(e):
                gate.record_throttle(len(half))
                log(f"clean bisect throttled ({e.code().name})")
                for rem in halves[i:]:
                    failed += len(rem)
                return (deleted, failed, True)
            if e.code() in _NON_BISECT_CODES:
                log(f"clean bisect aborted: {e.code().name} {e.details()}")
                for rem in halves[i:]:
                    failed += len(rem)
                return (deleted, failed, True)
            d, f, stop = _bisect_clean(ctl, half, permanent, timeout, gate)
            deleted += d
            failed += f
            if stop:
                for rem in halves[i + 1 :]:
                    failed += len(rem)
                return (deleted, failed, True)
            continue

        if res.success:
            if res.resultFilePaths:
                ok_n = len(set(res.resultFilePaths) & set(half))
                deleted += ok_n
                failed += len(half) - ok_n
            else:
                deleted += len(half)
        elif is_throttle_error(res.errorMessage):
            gate.record_throttle(len(half))
            log(f"clean bisect throttled ({res.errorMessage})")
            for rem in halves[i:]:
                failed += len(rem)
            return (deleted, failed, True)
        else:
            d, f, stop = _bisect_clean(ctl, half, permanent, timeout, gate)
            deleted += d
            failed += f
            if stop:
                for rem in halves[i + 1 :]:
                    failed += len(rem)
                return (deleted, failed, True)
    return (deleted, failed, False)


def run_clean(
    ctl: CD2Client,
    cfg: Config,
    gate: ThrottleGate | None = None,
    max_throttle_retries: int = 5,
) -> int:
    """Clean top-level non-directory files in cfg.dest using batched DeleteFiles."""
    gate = (
        gate
        if gate is not None
        else ThrottleGate(base=cfg.throttle_base, max_backoff=cfg.throttle_max)
    )
    try:
        why = ctl.check_dest(cfg.dest, cfg.expect_cloud_re)
    except AuthExpiredError as e:
        log(f"authentication expired: {e}")
        return 1
    if why is not None:
        raise SystemExit(f"refusing to touch {cfg.dest}: {why}")

    try:
        files = [f for f in ctl.list_dir(cfg.dest, force_refresh=False) if not f.isDirectory]
    except AuthExpiredError as e:
        log(f"authentication expired: {e}")
        return 1
    except grpc.RpcError as e:
        raise SystemExit(f"listing {cfg.dest} failed: {e.code().name} {e.details()}") from e

    log(f"{len(files)} files in {cfg.dest}")
    for f in files[:20]:
        print(f"{f.size:>12}  {f.name}")
    if not cfg.yes:
        log("dry run, pass --yes to delete them")
        return 0

    permanent = cfg.delete == "permanent"
    skipped = 0
    if permanent:
        targets = [f for f in files if f.canDeletePermanently]
        skipped = len(files) - len(targets)
        if skipped:
            log(f"skipping {skipped} file(s) without permanent-delete capability")
    else:
        targets = files

    deleted = 0
    failed = 0
    idx = 0
    retries = 0
    while idx < len(targets):
        if gate.in_cooldown():
            rem = gate.remaining()
            if rem > 0:
                time.sleep(rem)
        bs = max(1, gate.effective_batch(cfg.delete_batch))
        chunk = targets[idx : idx + bs]
        paths = [f.fullPathName for f in chunk]
        try:
            res = ctl.delete_batch(paths, permanent=permanent, timeout=cfg.delete_timeout)
        except AuthExpiredError as e:
            log(f"authentication expired: {e}")
            return 1
        except grpc.RpcError as e:
            if is_throttle_error(e):
                delay = gate.record_throttle(len(paths))
                retries += 1
                if retries <= max_throttle_retries:
                    log(
                        f"clean throttled ({e.code().name}); retry {retries}/{max_throttle_retries} "
                        f"in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue
                log(f"clean throttled ({e.code().name}); retries exhausted for {len(paths)} file(s)")
                failed += len(paths)
            elif e.code() in _NON_BISECT_CODES:
                log(f"clean batch failed: {e.code().name} {e.details()}")
                failed += len(paths)
            else:
                log(f"clean batch failed ({e.code().name} {e.details()}); bisecting")
                try:
                    d, f, _ = _bisect_clean(ctl, paths, permanent, cfg.delete_timeout, gate)
                except AuthExpiredError as auth_err:
                    log(f"authentication expired: {auth_err}")
                    return 1
                deleted += d
                failed += f
            idx += len(paths)
            retries = 0
            continue

        if res.success:
            if res.resultFilePaths:
                ok_n = len(set(res.resultFilePaths) & set(paths))
                deleted += ok_n
                failed += len(paths) - ok_n
            else:
                deleted += len(paths)
            gate.record_success(cfg.delete_batch)
            idx += len(paths)
            retries = 0
        elif is_throttle_error(res.errorMessage):
            delay = gate.record_throttle(len(paths))
            retries += 1
            if retries <= max_throttle_retries:
                log(
                    f"clean throttled ({res.errorMessage}); retry {retries}/{max_throttle_retries} "
                    f"in {delay:.1f}s"
                )
                time.sleep(delay)
                continue
            log(f"clean throttled ({res.errorMessage}); retries exhausted for {len(paths)} file(s)")
            failed += len(paths)
            idx += len(paths)
            retries = 0
        else:
            log(f"clean batch refused ({res.errorMessage}); bisecting")
            try:
                d, f, _ = _bisect_clean(ctl, paths, permanent, cfg.delete_timeout, gate)
            except AuthExpiredError as auth_err:
                log(f"authentication expired: {auth_err}")
                return 1
            deleted += d
            failed += f
            idx += len(paths)
            retries = 0

    verb = "deleted permanently" if permanent else "moved to recycle bin"
    extra = f", {skipped} skipped" if skipped else ""
    log(f"{verb}: {deleted} files, {failed} failed{extra}")
    return 0 if failed == 0 else 1
