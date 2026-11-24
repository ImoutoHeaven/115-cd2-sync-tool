"""Append-only JSONL manifest with cross-process locking and crash repair."""

import json
import os
import threading
import time

if os.name == "nt":
    import msvcrt
else:
    import fcntl

from cd2_core import (
    DONE,
    MANIFEST_FIELDS as PERSIST_FIELDS,
    RETRY,
    VALID_STATES as STATES,
    log,
    normalize_path as _norm_path,
)
_REPAIR_CHUNK = 4096




class ManifestLockError(RuntimeError):
    """Raised when another process or instance holds the manifest lock."""




class Manifest:
    """Append-only JSONL manifest; last line per `rel` wins.

    pending -> submitted -> landed -> dropped | kept | stranded
    pending/submitted -> failed (revived to pending on add)
    """

    def __init__(self, path: str, src_root: str, dest_root: str):
        self.path = path
        self.lock = threading.RLock()
        self.recs: dict[str, dict] = {}
        self.src_root = _norm_path(src_root)
        self.dest_root = _norm_path(dest_root)
        self._lock_fh = None
        self._acquire_lock()
        try:
            self._repair()
            self._load()
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _acquire_lock(self) -> None:
        fh = open(self.path + ".lock", "a+b")
        try:
            if os.name == "nt":
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            fh.close()
            raise ManifestLockError(f"manifest lock busy: {self.path}.lock") from e
        self._lock_fh = fh

    def close(self) -> None:
        with self.lock:
            fh = self._lock_fh
            if fh is None:
                return
            self._lock_fh = None
            try:
                if os.name == "nt":
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            finally:
                fh.close()

    def _repair(self) -> None:
        """Truncate an incomplete trailing line via reverse chunked seek."""
        if not os.path.exists(self.path):
            return
        with open(self.path, "rb+") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            if size == 0:
                return
            fh.seek(size - 1)
            if fh.read(1) == b"\n":
                return
            pos = size
            cut = 0
            while pos > 0:
                step = min(_REPAIR_CHUNK, pos)
                pos -= step
                fh.seek(pos)
                idx = fh.read(step).rfind(b"\n")
                if idx >= 0:
                    cut = pos + idx + 1
                    break
            fh.seek(cut)
            fh.truncate()
            fh.flush()
            os.fsync(fh.fileno())
            log(f"manifest: dropped torn tail ({size - cut} bytes)")

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8", errors="replace") as fh:
            for lineno, raw in enumerate(fh, 1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError as e:
                    log(f"manifest: WARNING corrupt JSON at line {lineno} in {self.path}: {e}")
                    continue
                if not isinstance(r, dict):
                    log(f"manifest: WARNING non-object JSON at line {lineno} in {self.path}")
                    continue
                root = r.get("root")
                if not isinstance(root, str) or not root:
                    log(f"manifest: WARNING missing/invalid root at line {lineno} in {self.path}")
                    continue
                if root != self.src_root:
                    raise SystemExit(
                        f"manifest {self.path} belongs to source {root!r}, refusing to mix with "
                        f"{self.src_root!r}. Use a different --state file."
                    )
                rel = r.get("rel")
                if not isinstance(rel, str) or not rel:
                    log(f"manifest: WARNING missing/invalid rel at line {lineno} in {self.path}")
                    continue
                src = r.get("src")
                if not isinstance(src, str) or not src:
                    log(f"manifest: WARNING missing/invalid src at line {lineno} in {self.path}")
                    continue
                state = r.get("state")
                if not isinstance(state, str) or state not in STATES:
                    log(f"manifest: WARNING invalid state {state!r} at line {lineno} in {self.path}")
                    continue
                self.recs[rel] = r
        if self.recs:
            log(f"manifest: {len(self.recs)} known files ({self.path})")

    @staticmethod
    def _serialize(r: dict) -> str:
        return json.dumps({k: r[k] for k in PERSIST_FIELDS if k in r}, ensure_ascii=False)

    def _append_many(self, recs: list[dict]) -> None:
        if self._lock_fh is None:
            raise RuntimeError("manifest is closed")
        if not recs:
            return
        payload = "".join(self._serialize(r) + "\n" for r in recs)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())

    def add_batch(self, files) -> list[dict]:
        prefix = "/" if self.src_root == "/" else self.src_root + "/"
        items = list(files)
        parsed: list[tuple[object, str, str]] = []
        for f in items:
            src = _norm_path(f.fullPathName)
            if not src.startswith(prefix):
                raise ValueError(f"path {f.fullPathName!r} does not match src_root {self.src_root!r}")
            rel = src[len(prefix):]
            if not rel:
                raise ValueError(f"path {f.fullPathName!r} has empty relative path under {self.src_root!r}")
            parsed.append((f, src, rel))

        now = int(time.time())
        result: list[dict] = []
        to_write: list[dict] = []
        staged_new: dict[str, dict] = {}
        staged_updates: list[tuple[dict, dict]] = []
        with self.lock:
            for f, src, rel in parsed:
                r = self.recs.get(rel)
                if r is None:
                    r = staged_new.get(rel)
                if r is None:
                    r = {
                        "rel": rel,
                        "root": self.src_root,
                        "src": src,
                        "size": f.size,
                        "state": "pending",
                        "attempts": 0,
                        "submit_fails": 0,
                        "cloud": "",
                        "dest_root": "",
                        "submitted_at": 0,
                        "landed_at": 0,
                        "ts": now,
                    }
                    staged_new[rel] = r
                    to_write.append(r)
                elif r["state"] in DONE:
                    pass
                elif r["state"] in RETRY:
                    patch = {
                        "state": "pending",
                        "attempts": 0,
                        "submit_fails": 0,
                        "src": src,
                        "size": f.size,
                        "ts": now,
                    }
                    to_write.append({**r, **patch})
                    staged_updates.append((r, patch))
                else:
                    if rel in staged_new:
                        r["src"] = src
                        r["size"] = f.size
                    else:
                        staged_updates.append((r, {"src": src, "size": f.size}))
                result.append(r)
            self._append_many(to_write)
            if staged_new:
                self.recs.update(staged_new)
            for r, patch in staged_updates:
                r.update(patch)
        return result

    def add(self, f) -> dict:
        return self.add_batch([f])[0]

    def set_batch(self, updates: list[tuple[dict, str, dict]]) -> None:
        if not updates:
            return
        now = int(time.time())
        with self.lock:
            snaps: list[dict] = []
            patches: list[tuple[dict, dict]] = []
            latest: dict[int, dict] = {}
            for r, state, kw in updates:
                patch = dict(kw) if kw else {}
                patch["state"] = state
                patch["ts"] = now
                snap = dict(latest.get(id(r), r))
                snap.update(patch)
                latest[id(r)] = snap
                snaps.append(snap)
                patches.append((r, patch))
            self._append_many(snaps)
            for r, patch in patches:
                r.update(patch)

    def set(self, r: dict, state: str, **kw) -> None:
        self.set_batch([(r, state, kw)])

    def of(self, *states: str) -> list[dict]:
        with self.lock:
            return [r for r in self.recs.values() if r["state"] in states]

    def by_src(self) -> dict[str, dict]:
        with self.lock:
            return {r["src"]: r for r in self.recs.values()}
