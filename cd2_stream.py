#!/usr/bin/env python3
"""Drive CloudDrive2 as a transient <any mount> -> 115 transfer pump.

Source is read-only and never modified. Every file is submitted as its own copy
task into one flat folder on 115 (normally the mount root); once the engine
reports the copy completed, the landed file is deleted from 115 (into its
recycle bin) and the content is later fetched back by ed2k.

CloudDrive2 owns naming, hashing, uploading, concurrency, rate and flow limits.
This script only:
  1. submits one CopyFile per file      (conflictPolicy = Rename)
  2. watches CopyTask per source path   (identity = sourcePath)
  3. deletes the landed file            (dest/<source basename>)

The manifest is keyed by the source file. A file the engine reported as
uploaded is never submitted again, whichever 115 account took it and whether or
not its landed copy could be deleted. The script keeps running across a 115
account swap on the same mount and resubmits only what had not landed.

    python cd2_stream.py run   --src "/123pan/movies/title" --dest /115
    python cd2_stream.py run   --src "/123pan/movies/title" --dest /115 --delete none
    python cd2_stream.py clean --dest /115
"""

import argparse
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import grpc
from google.protobuf import empty_pb2

import clouddrive_pb2 as pb
import clouddrive_pb2_grpc as pbg

DONE = ("dropped", "kept", "stranded")  # uploaded; never submitted again
RETRY = ("failed",)                     # not uploaded; revived by a new run or account
LIVE = ("submitted", "landed")
WORKING = (pb.CopyTask.Pending, pb.CopyTask.Scanning, pb.CopyTask.Scanned)


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def base_of(path):
    return path.rstrip("/").rsplit("/", 1)[-1]


def mount_of(path):
    return "/" + path.strip("/").split("/")[0]


def ts_of(ts):
    return ts.seconds if ts and ts.seconds else 0


def uploaded(t):
    """Completed is not enough: the task must count its one file as copied."""
    return (t.status == pb.CopyTask.Completed and t.uploadedFiles >= 1
            and t.failedFiles == 0 and t.cancelledFiles == 0 and t.skippedFiles == 0)


def channel_target(endpoint):
    """'host:port' (plaintext), 'http://host[:port]' or 'https://host[:port]'
    -> (gRPC target, use TLS)."""
    if "://" not in endpoint:
        return endpoint, False
    u = urlsplit(endpoint)
    try:
        port = u.port
    except ValueError:
        port = -1
    if u.scheme not in ("http", "https") or not u.hostname or u.path not in ("", "/") or port == -1:
        raise SystemExit(f"unsupported CloudDrive2 endpoint {endpoint!r}")
    tls = u.scheme == "https"
    host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname
    return f"{host}:{port or (443 if tls else 80)}", tls


class CD2:
    """The CloudDrive2 gRPC calls this script needs."""
    creds = None
    expires = float("inf")

    def __init__(self, endpoint, token=None, ca_cert=None):
        target, tls = channel_target(endpoint)
        options = [
            ("grpc.max_receive_message_length", -1),
            ("grpc.max_send_message_length", -1),
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 10000),
            ("grpc.keepalive_permit_without_calls", 1),
        ]
        if tls:
            roots = None
            if ca_cert:
                with open(ca_cert, "rb") as fh:
                    roots = fh.read()
            self.channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(roots), options=options)
        else:
            self.channel = grpc.insecure_channel(target, options=options)
        self.stub = pbg.CloudDriveFileSrvStub(self.channel)
        self.token = token

    def _md(self):
        return [("authorization", f"Bearer {self.token}")] if self.token else None

    def _call(self, method, req, timeout=30):
        return method(req, timeout=timeout, metadata=self._md())

    def login(self, user, password, totp=None):
        req = pb.GetTokenRequest(userName=user, password=password)
        if totp:
            req.totpCode = totp
        tok = self._call(self.stub.GetToken, req, timeout=45)
        if not tok.success:
            raise SystemExit(f"login failed: {tok.errorMessage}")
        self.token = tok.token
        # a TOTP code is single-use, so only a password login can be renewed
        self.creds = None if totp else (user, password)
        self.expires = ts_of(tok.expiration) or time.time() + 3600
        return tok

    def keep_login(self):
        """Renew the CloudDrive2 API token before it expires (not the 115 one)."""
        if not self.creds or time.time() < self.expires - 600:
            return
        try:
            self.login(*self.creds)
            log("CloudDrive2 token renewed")
        except (grpc.RpcError, SystemExit) as e:
            log(f"CloudDrive2 token renewal failed: {e}")

    def find(self, path):
        """Return CloudDriveFile or None. Server raises for missing paths."""
        try:
            f = self._call(self.stub.FindFileByPath, pb.FindFileByPathRequest(parentPath="", path=path))
            return f if f.fullPathName or f.name else None
        except grpc.RpcError as e:
            detail = (e.details() or "").lower()
            if e.code() == grpc.StatusCode.NOT_FOUND or any(
                s in detail for s in ("not found", "not exist", "no such", "不存在")
            ):
                return None
            raise

    def ensure_dir(self, path):
        """Create every missing segment of `path`. Returns True if it exists."""
        cur = ""
        for name in [p for p in path.split("/") if p]:
            if not self.find(f"{cur}/{name}"):
                self._call(self.stub.CreateFolder, pb.CreateFolderRequest(parentPath=cur or "/", folderName=name))
            cur = f"{cur}/{name}"
        return self.find(cur) is not None

    def walk(self, root, tries=3):
        """Depth-first walk. A directory that cannot be listed aborts the scan:
        silently skipping it would report a subtree as successfully moved."""
        stack, seen = [root.rstrip("/")], set()
        while stack:
            d = stack.pop()
            if d in seen:
                continue
            seen.add(d)
            items = None
            for attempt in range(tries):
                try:
                    items = []
                    call = self.stub.GetSubFiles(
                        pb.ListSubFileRequest(path=d, forceRefresh=False), timeout=600, metadata=self._md()
                    )
                    for reply in call:
                        items.extend(reply.subFiles)
                    break
                except grpc.RpcError as e:
                    if attempt == tries - 1:
                        raise
                    log(f"  list {d} failed ({e.code().name}), retry {attempt + 2}/{tries}")
                    time.sleep(2)
            for f in items:
                if f.isDirectory:
                    stack.append(f.fullPathName)
                else:
                    yield f

    def list_dir(self, path):
        out = []
        call = self.stub.GetSubFiles(
            pb.ListSubFileRequest(path=path, forceRefresh=True), timeout=600, metadata=self._md()
        )
        for reply in call:
            out.extend(reply.subFiles)
        return out

    def cloud_of(self, path):
        """(CloudAPI, why, absent) of the mount holding `path`, without writing.
        `absent` separates a removed mount from a failed lookup."""
        top = mount_of(path)
        try:
            f = self.find(top)
        except grpc.RpcError as e:
            return None, f"mount lookup failed: {e.code().name}", False
        if f is None:
            return None, f"mount {top} not present", True
        if not (f.HasField("CloudAPI") and f.CloudAPI.name):
            return None, f"{top} is not a cloud mount", True
        return f.CloudAPI, None, False

    def check_dest(self, dest, expect):
        api, why, _ = self.cloud_of(dest)
        if why:
            return why
        if not re.search(expect, api.name, re.I):
            return f"cloud is {api.name!r}, expected /{expect}/"
        return None

    def submit_one(self, src, dest):
        """Rename keeps both files if names ever collide; Skip would drop one."""
        res = self._call(
            self.stub.CopyFile,
            pb.CopyFileRequest(
                theFilePaths=[src], destPath=dest,
                conflictPolicy=pb.CopyFileRequest.Rename, handleConflictRecursively=False,
            ),
        )
        return res.success, res.errorMessage

    def restart_task(self, src, dest):
        self._call(self.stub.RestartCopyTask, pb.CopyTaskRequest(sourcePath=src, destPath=dest))

    def remove_tasks(self, keys):
        try:
            return self._call(self.stub.RemoveCopyTasks, pb.CopyTaskBatchRequest(taskKeys=list(keys)), timeout=120)
        except grpc.RpcError as e:
            log(f"  remove tasks failed: {e.code().name}")
            return None

    def tasks(self):
        return list(self._call(self.stub.GetCopyTasks, empty_pb2.Empty(), timeout=180).copyTasks)

    def delete(self, path, permanent=False):
        m = self.stub.DeleteFilePermanently if permanent else self.stub.DeleteFile
        return self._call(m, pb.FileRequest(path=path), timeout=300)


class Manifest:
    """Append-only JSONL, last line per `rel` wins.

    pending -> submitted -> landed -> dropped | kept | stranded
    pending/submitted -> failed (bounded retries; revived by a new run or account)

    `landed` and later mean the engine reported the upload; those records never
    go back to pending. Bound to one source root: mixing roots in one file would
    make two different files with the same relative path look like one record.
    """

    def __init__(self, path, src_root, dest_root):
        self.path, self.lock = path, threading.RLock()
        self.recs = {}
        self.src_root = src_root.rstrip("/")
        self.dest_root = dest_root.rstrip("/")
        self._repair()
        self._load()

    def _repair(self):
        """Drop a torn last line, so the next append cannot corrupt the file."""
        if not os.path.exists(self.path):
            return
        with open(self.path, "rb+") as fh:
            data = fh.read()
            if data and not data.endswith(b"\n"):
                cut = data.rfind(b"\n")
                fh.seek(cut + 1 if cut >= 0 else 0)
                fh.truncate()
                log(f"manifest: dropped torn tail ({len(data) - (cut + 1)} bytes)")

    def _load(self):
        if not os.path.exists(self.path):
            return
        n = 0
        with open(self.path, encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("root") != self.src_root:
                raise SystemExit(
                    f"manifest {self.path} belongs to source {r.get('root')!r}, refusing to mix with "
                    f"{self.src_root!r}. Use a different --state file."
                )
            self.recs[r["rel"]] = r
            n += 1
        if n:
            log(f"manifest: {n} known files ({self.path})")

    def add(self, f):
        """Register a source file; returns a record that is done, live or pending."""
        rel = f.fullPathName[len(self.src_root):].lstrip("/")
        with self.lock:
            r = self.recs.get(rel)
            if r is None:
                r = {"rel": rel, "root": self.src_root, "src": f.fullPathName,
                     "size": f.size, "state": "pending", "attempts": 0,
                     "cloud": "", "dest_root": "", "submitted_at": 0, "ts": int(time.time())}
                self.recs[rel] = r
                self._append(r)
                return r
            if r["state"] in DONE:
                return r
            if r["state"] in RETRY:
                self.set(r, "pending", attempts=0, submit_fails=0)
            r["src"] = f.fullPathName
            r["size"] = f.size
            return r

    def _append(self, r):
        line = json.dumps(r, ensure_ascii=False)
        with self.lock:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())

    def set(self, r, state, **kw):
        with self.lock:
            r.update(kw)
            r["state"] = state
            r["ts"] = int(time.time())
            self._append(r)

    def of(self, *states):
        with self.lock:
            return [r for r in self.recs.values() if r["state"] in states]

    def by_src(self):
        with self.lock:
            return {r["src"]: r for r in self.recs.values()}


class Pipeline:
    def __init__(self, ctl, man, args):
        self.ctl, self.man, self.a = ctl, man, args
        self.trigger = threading.Event()
        self.stop = threading.Event()
        self.dropped = 0
        self.kept = 0
        self.lock = threading.Lock()
        self.prune = []
        self.cloud = ""        # "<cloud name>|<user name>" of the account behind dest
        self.gap = False       # dest mount was seen missing since the last ready check
        self.rotated_at = 0.0  # when dest last came back as a different account
        self.more = False      # pending work was held back by --batch
        self.why = None

    def listen(self):
        while not self.stop.is_set():
            try:
                for msg in self.ctl.stub.PushTaskChange(empty_pb2.Empty(), metadata=self.ctl._md()):
                    if msg.uploadFileStatusChanges or msg.copyTaskCount:
                        self.trigger.set()
            except grpc.RpcError:
                if not self.stop.is_set():
                    time.sleep(3)

    def dest_ready(self):
        """None when dest can take writes now; never blocks."""
        dest = self.man.dest_root
        api, why, absent = self.ctl.cloud_of(dest)
        if why:
            self.gap = self.gap or absent
            return why
        if not re.search(self.a.expect_cloud, api.name, re.I):
            return f"cloud is {api.name!r}, expected /{self.a.expect_cloud}/"
        if dest != mount_of(dest):
            try:
                if not self.ctl.ensure_dir(dest):
                    return "cannot create the destination folder"
            except grpc.RpcError as e:
                return f"create failed: {e.code().name}"
        account = f"{api.name}|{api.userName}"
        if self.cloud and (account != self.cloud or self.gap):
            self.rotated(account)
        elif not self.cloud:
            log(f"staging on {account}")
        self.cloud, self.gap = account, False
        return None

    def rotated(self, account):
        """Dest came back (as another account): what failed before gets a fresh
        budget, and tasks lost with the old account are resubmitted at once."""
        log(f"destination account is now {account} (was {self.cloud}); resuming")
        self.rotated_at = time.time()
        for r in self.man.of("failed"):
            self.man.set(r, "pending", attempts=0, submit_fails=0)
        for r in self.man.of("pending", "submitted"):
            r["attempts"] = r["submit_fails"] = 0
            if r.get("failed_at"):
                r["failed_at"] = 1   # a visible Failed task restarts on the next poll

    # --- 3. clear the landing spot ---------------------------------------
    def settle(self, r):
        """Delete the landed copy. The upload already counts; this only cleans."""
        if r.get("dest_root") and r["dest_root"] != self.man.dest_root:
            return self._done(r, "dropped", f"done elsewhere {r['rel']} (was on {r['dest_root']})")
        if self.a.delete == "none":
            return self._done(r, "kept", f"ok {r['rel']} ({r['size']} B, left in place)")
        path = f"{self.man.dest_root}/{base_of(r['src'])}"
        try:
            f = self.ctl.find(path)
            if f is None:
                # already deleted (crash before the manifest write) or gone
                # with the previous account: nothing left to clear
                return self._done(r, "dropped", f"gone already {r['rel']}")
            if f.isDirectory or f.size != r["size"]:
                return self._done(r, "stranded", f"{r['rel']} landed under another name; left on 115")
            res = self.ctl.delete(path, permanent=(self.a.delete == "permanent"))
        except grpc.RpcError as e:
            return r, False, f"{e.code().name} {e.details()}"
        if not res.success:
            return r, False, f"delete refused: {res.errorMessage}"
        with self.lock:
            n = self.dropped + 1
        return self._done(r, "dropped", f"[{n}] {r['rel']} ({r['size']} B), cleared from 115")

    def _done(self, r, state, msg):
        self.man.set(r, state)
        with self.lock:
            if state == "kept":
                self.kept += 1
            else:
                self.dropped += 1
        log(msg)
        return r, True, "ok"

    def clear_landed(self):
        landed = self.man.of("landed")
        if not landed:
            return
        now = time.time()
        with ThreadPoolExecutor(max_workers=self.a.workers) as pool:
            for r, ok, why in pool.map(self.settle, landed):
                if ok:
                    continue
                r.setdefault("clear_since", now)
                r["retry"] = r.get("retry", 0) + 1
                if r["retry"] in (1, 5, 50):
                    log(f"wait {r['rel']}: {why} (x{r['retry']})")
                if now - r["clear_since"] >= self.a.stall:
                    # the upload stands; only the 115 copy is left behind
                    self.man.set(r, "stranded")
                    log(f"stranded {r['rel']}: could not clear the landed copy ({why})")

    def ours(self, r, t):
        """A task this manifest submitted for `r`, with the same byte size."""
        return (r.get("dest_root") == self.man.dest_root
                and (not t.totalBytes or t.totalBytes == r["size"]))

    def bump(self, r, why):
        """Count a blocker towards the retry budget so a record can never sit in
        `pending` forever."""
        r["submit_fails"] = r.get("submit_fails", 0) + 1
        if r["submit_fails"] >= self.a.max_attempts:
            self.man.set(r, "failed")
            log(f"parked {r['rel']}: {why}")
        else:
            log(f"blocked {r['rel']}: {why} ({r['submit_fails']}/{self.a.max_attempts})")

    # --- 1. submit, one file per basename at a time -----------------------
    def submit_pending(self, tasks):
        known = {}
        for t in tasks:
            if t.destPath.rstrip("/") == self.man.dest_root:
                known.setdefault(t.sourcePath, t)
        lanes = {base_of(r["src"]) for r in self.man.of(*LIVE)}
        pending = self.man.of("pending")
        cap = self.a.batch if self.a.batch > 0 else len(pending)
        batch = []
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
        for r in batch:
            now = time.time()
            # a task for this source may already exist: an earlier submit whose
            # reply never arrived, or a crash between the engine accepting it
            # and us recording it. Adopt it; the API layer does not dedupe, so
            # submitting again would start a second task for the same target.
            t = known.get(r["src"])
            if t is not None:
                # Adopt only a task this manifest asked for itself, with the
                # same byte size; a task left by the GUI or another state file
                # must not make a file count as done without being copied.
                mine = self.ours(r, t)
                if mine:
                    started = ts_of(t.startTime) if t.HasField("startTime") else 0
                    log(f"task already exists for {r['rel']} (status {t.status}), adopting")
                    self.man.set(r, "submitted", dest_root=self.man.dest_root,
                                 submitted_at=int(started or now), seen=now, cloud=self.cloud,
                                 submit_fails=0, failed_at=0)
                    ok_n += 1
                    continue
                # a foreign task for our source/dest pair: it must be gone
                # before we submit, otherwise two tasks target one name
                log(f"existing task for {r['rel']} is not ours, invalidating it first")
                self.ctl.remove_tasks([f"{r['src']}:{self.man.dest_root}"])
                try:
                    still = [t for t in self.ctl.tasks()
                             if t.sourcePath == r["src"] and t.destPath.rstrip("/") == self.man.dest_root]
                except grpc.RpcError as e:
                    still = [e.code().name]
                if still:
                    self.bump(r, "a foreign task could not be invalidated")
                    continue
            # a same-named file would make the engine append " (N)" and the
            # delete after landing would miss ours. Whatever owns the name is
            # recycled first: dest is ours to write and clear.
            stale = f"{self.man.dest_root}/{base_of(r['src'])}"
            try:
                f = self.ctl.find(stale)
                if f is not None and f.isDirectory:
                    self.bump(r, f"a directory owns {base_of(stale)} in {self.man.dest_root}")
                    continue
                if f is not None and self.a.delete != "none":
                    res = self.ctl.delete(stale, permanent=(self.a.delete == "permanent"))
                    if not res.success:
                        self.bump(r, f"cannot clear {base_of(stale)}: {res.errorMessage}")
                        continue
                    log(f"cleared same-named {base_of(stale)} before submitting")
            except grpc.RpcError as e:
                # dest itself is failing (e.g. its 115 login died): stop this batch
                self.bump(r, f"staging check failed: {e.code().name}")
                break
            # Persist the intent before the RPC. If we die between the engine
            # accepting the task and us recording it, the record is already
            # `submitted`, so the next sweep adopts that task instead of
            # submitting a second one (the API layer does not dedupe).
            self.man.set(r, "submitted", dest_root=self.man.dest_root, submitted_at=int(now),
                         seen=now, cloud=self.cloud, failed_at=0)
            try:
                ok, err = self.ctl.submit_one(r["src"], self.man.dest_root)
            except grpc.RpcError as e:
                # outcome unknown: leave it submitted and let poll_tasks adopt
                # the task, or reconcile requeue it after --stall
                log(f"submit {r['rel']} outcome unknown ({e.code().name}); left submitted")
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    break
                continue
            if ok:
                self.man.set(r, "submitted", submit_fails=0)
                ok_n += 1
            else:
                self.man.set(r, "pending", submitted_at=0)
                self.bump(r, f"submit rejected: {err}")
        left = len(self.man.of("pending"))
        # keep going without the sweep pause only while whole batches succeed
        self.more = self.more or (self.a.batch > 0 and ok_n == cap and left > 0)
        log(f"submitted {ok_n}/{len(batch)}{f', {left} queued' if left else ''}")

    def _retry(self, r, why):
        """Restart the task in place, bounded. Never creates a second one."""
        if r["attempts"] >= self.a.max_attempts:
            self.man.set(r, "failed")
            log(f"parked {r['rel']}: {why}")
            return
        log(f"retry {r['rel']} (attempt {r['attempts'] + 1}): {why}")
        self.man.set(r, "submitted", attempts=r["attempts"] + 1, failed_at=0, seen=time.time())
        try:
            self.ctl.restart_task(r["src"], self.man.dest_root)
        except grpc.RpcError as e:
            log(f"  restart failed: {e.code().name}")

    # --- 2. follow CopyTask ----------------------------------------------
    def poll_tasks(self, tasks, ready=True):
        now = time.time()
        by_src = self.man.by_src()
        seen = set()
        self.due = []
        for t in tasks:
            if t.destPath.rstrip("/") != self.man.dest_root:
                continue
            r = by_src.get(t.sourcePath)
            if r is None or r["state"] not in ("submitted", "failed"):
                continue
            if r["state"] == "failed" and not (uploaded(t) and self.ours(r, t)):
                continue    # parked; only a late success of our own task still counts
            seen.add(r["src"])
            r["seen"] = now
            if uploaded(t):
                self.man.set(r, "landed", failed_at=0)
                # the manifest now holds the success: the task row is spent
                self.prune.append(f"{t.sourcePath}:{t.destPath}")
                continue
            if t.status in WORKING:
                continue    # queued or moving inside CloudDrive2; its scheduler owns it
            # Failed, Cancelled (the engine reports 5, which the proto enum
            # does not name), Completed without a copied file, or anything the
            # engine adds later: restart it once dest can take writes.
            msg = "; ".join(e.message for e in t.errors[-2:]) or f"task status {t.status}"
            r["failed_at"] = r.get("failed_at") or now
            if ready and now - r["failed_at"] >= self.a.retry_after:
                self.due.append((r, msg))   # restarted after cleanup, see step
        return seen

    def restart_due(self):
        due, self.due = self.due, []
        cap = self.a.batch if self.a.batch > 0 else len(due)
        for r, msg in due[:cap]:
            if r["state"] == "submitted":
                self._retry(r, msg)
        self.more = self.more or len(due) > cap

    def reconcile(self, seen):
        """Submitted records whose task is gone go back to pending: at once when
        the task was last seen before an account swap, else after --stall."""
        now = time.time()
        for r in self.man.of("submitted"):
            if r["src"] in seen:
                continue
            last = r.get("seen") or r.get("submitted_at") or r.get("ts", 0)
            moved = self.cloud and r.get("cloud") and r["cloud"] != self.cloud
            if last < self.rotated_at or moved:
                self.man.set(r, "pending")
                log(f"task lost with the previous account, requeue {r['rel']}")
            elif now - last < self.a.stall:
                continue
            elif r["attempts"] >= self.a.max_attempts:
                self.man.set(r, "failed")
                log(f"parked {r['rel']}: task vanished {int(now - last)}s ago")
            else:
                self.man.set(r, "pending", attempts=r["attempts"] + 1)
                log(f"task vanished, requeue {r['rel']}")

    def prune_done(self):
        if not self.prune:
            return
        keys, self.prune = self.prune, []
        for i in range(0, len(keys), 200):
            self.ctl.remove_tasks(keys[i:i + 200])

    def step(self):
        self.more = False
        self.due = []
        self.ctl.keep_login()
        try:
            tasks = self.ctl.tasks()
        except grpc.RpcError as e:
            log(f"GetCopyTasks failed: {e.code().name} {e.details()}")
            if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                self.ctl.expires = 0
            return
        why = self.dest_ready()
        if why != self.why:
            log(f"dest {self.man.dest_root}: {why or 'ready'}")
            self.why = why
        seen = self.poll_tasks(tasks, ready=why is None)
        self.reconcile(seen)
        if why is None:
            self.clear_landed()
            self.restart_due()
            self.submit_pending(tasks)
        self.prune_done()

    def run(self):
        threading.Thread(target=self.listen, daemon=True).start()
        log(f"{self.man.src_root} (read-only) -> {self.man.dest_root} | "
            f"after-landing={self.a.delete} sweep={self.a.sweep}s")

        while True:
            self.step()
            total = len(self.man.recs)
            done = len(self.man.of(*DONE))
            outstanding = len(self.man.of("pending", *LIVE))
            parked = len(self.man.of("failed"))
            if outstanding == 0 and parked == 0:
                log(f"finished: {total} files, {self.dropped} cleared from 115, {self.kept} kept")
                self.stop.set()
                return 0
            log(f"progress {done}/{total} | pending {len(self.man.of('pending'))} "
                f"submitted {len(self.man.of('submitted'))} landed {len(self.man.of('landed'))} "
                f"parked {parked}{' (waiting for another 115 account)' if outstanding == 0 else ''}")
            if self.more:
                continue
            self.trigger.wait(self.a.sweep)
            self.trigger.clear()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["scan", "run", "clean"])
    ap.add_argument("--endpoint", default=os.environ.get("CD2_ENDPOINT", "127.0.0.1:19798"),
                    help="host:port, http://host[:port] or https://host[:port]")
    ap.add_argument("--ca-cert", default=os.environ.get("CD2_CA_CERT"),
                    help="PEM certificate to trust for an https endpoint (e.g. self-signed)")
    ap.add_argument("--token", default=os.environ.get("CD2_TOKEN"))
    ap.add_argument("--user", default=os.environ.get("CD2_USER"))
    ap.add_argument("--password", default=os.environ.get("CD2_PASSWORD"))
    ap.add_argument("--totp", default=os.environ.get("CD2_TOTP"))
    ap.add_argument("--src", default="")
    ap.add_argument("--dest", default="/115")
    ap.add_argument("--state", default="manifest.jsonl")
    ap.add_argument("--batch", type=int, default=32, help="submissions per sweep, 0 = all free lanes")
    ap.add_argument("--workers", type=int, default=8, help="clear threads")
    ap.add_argument("--sweep", type=float, default=20.0)
    ap.add_argument("--stall", type=float, default=300.0,
                    help="a submitted file whose task vanished, or a landed copy that cannot be cleared, waits this long")
    ap.add_argument("--retry-after", type=float, default=180.0, help="wait this long before restarting a Failed task")
    ap.add_argument("--max-attempts", type=int, default=5)
    ap.add_argument("--delete", default="recycle", choices=["recycle", "permanent", "none"],
                    help="what to do with the file after it lands on 115")
    ap.add_argument("--yes", action="store_true", help="clean: actually delete")
    ap.add_argument("--expect-cloud", default="115",
                    help="refuse to run unless --dest sits on a cloud whose name matches this regex")
    a = ap.parse_args()

    ctl = CD2(a.endpoint, a.token, a.ca_cert)
    if not ctl.token:
        if not (a.user and a.password):
            raise SystemExit("need --token or --user/--password")
        ctl.login(a.user, a.password, a.totp)

    if a.cmd == "clean":
        why = ctl.check_dest(a.dest, a.expect_cloud)
        if why is not None:
            raise SystemExit(f"refusing to touch {a.dest}: {why}")
        files = [f for f in ctl.list_dir(a.dest) if not f.isDirectory]
        log(f"{len(files)} files in {a.dest}")
        for f in files[:20]:
            print(f"{f.size:>12}  {f.name}")
        if not a.yes:
            log("dry run, pass --yes to delete them")
            return 0
        bad = 0
        for f in files:
            res = ctl.delete(f.fullPathName, permanent=(a.delete == "permanent"))
            if not res.success:
                bad += 1
                log(f"  refused {f.name}: {res.errorMessage}")
        verb = "deleted permanently" if a.delete == "permanent" else "moved to recycle bin"
        log(f"{verb}: {len(files) - bad} files, {bad} failed")
        return 0

    if not a.src:
        raise SystemExit("--src is required")
    src, dest = a.src.rstrip("/"), a.dest.rstrip("/")
    if src == dest or dest.startswith(src + "/") or src.startswith(dest + "/"):
        raise SystemExit(f"--src {src} and --dest {dest} overlap; refusing to delete inside the source")

    man = Manifest(a.state, src, dest)

    log(f"scanning {src}")
    n = 0
    try:
        for f in ctl.walk(src):
            man.add(f)
            n += 1
            if n % 1000 == 0:
                log(f"  {n} files")
    except grpc.RpcError as e:
        raise SystemExit(
            f"listing {src} failed: {e.code().name} {e.details()}; nothing was submitted this run"
        )
    log(f"scan: {n} files seen, {len(man.of(*DONE))} already done, "
        f"{len(man.of('pending'))} to move, manifest holds {len(man.recs)}")

    if a.cmd == "scan":
        for r in list(man.recs.values())[:10]:
            print(f"{r['state']:<10} {r['size']:>12}  {r['rel']}")
        return 0

    return Pipeline(ctl, man, a).run()


if __name__ == "__main__":
    raise SystemExit(main())
