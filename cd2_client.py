"""Thin CloudDrive2 gRPC client with explicit auth-renewal and error boundaries."""

import logging
import re
import threading
import time

import grpc
from google.protobuf import empty_pb2

import clouddrive_pb2 as pb
import clouddrive_pb2_grpc as pbg

logger = logging.getLogger(__name__)

_TRANSIENT_WALK_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
    }
)


class AuthExpiredError(RuntimeError):
    """Raised when the local CloudDrive2 session token expires and cannot be renewed."""


class CD2Client:
    """CloudDrive2 gRPC client."""

    def __init__(
        self,
        target: str,
        tls: bool,
        token: str | None = None,
        ca_cert: str | None = None,
        channel=None,
        stub=None,
    ):
        self._auth_lock = threading.RLock()
        self.token: str | None = token
        self.creds: tuple[str, str] | None = None
        self.expires: float = float("inf")
        self._last_renew_fail: float = 0.0
        self._renew_backoff: float = 0.0

        if stub is not None:
            self.channel = channel
            self.stub = stub
            return

        if channel is not None:
            self.channel = channel
        else:
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
                self.channel = grpc.secure_channel(
                    target, grpc.ssl_channel_credentials(roots), options=options
                )
            else:
                self.channel = grpc.insecure_channel(target, options=options)
        self.stub = pbg.CloudDriveFileSrvStub(self.channel)

    def _md(self) -> list[tuple[str, str]] | None:
        with self._auth_lock:
            token = self.token
        return [("authorization", f"Bearer {token}")] if token else None

    def _call(
        self,
        method,
        req,
        timeout: float = 30.0,
        is_local_cd2_rpc: bool = False,
    ):
        with self._auth_lock:
            used_token = self.token
        md = [("authorization", f"Bearer {used_token}")] if used_token else None
        try:
            return method(req, timeout=timeout, metadata=md)
        except grpc.RpcError as e:
            if not (is_local_cd2_rpc and e.code() == grpc.StatusCode.UNAUTHENTICATED):
                raise
            with self._auth_lock:
                if self.token == used_token:
                    if not self.creds:
                        raise AuthExpiredError(
                            "CloudDrive2 token expired and cannot be renewed without stored credentials"
                        ) from e
                    try:
                        self.login(*self.creds)
                    except (grpc.RpcError, SystemExit) as renew_err:
                        raise AuthExpiredError(
                            f"CloudDrive2 token renewal failed: {renew_err}"
                        ) from renew_err
                retry_token = self.token
            retry_md = [("authorization", f"Bearer {retry_token}")] if retry_token else None
            try:
                return method(req, timeout=timeout, metadata=retry_md)
            except grpc.RpcError as retry_err:
                if retry_err.code() == grpc.StatusCode.UNAUTHENTICATED:
                    raise AuthExpiredError(
                        "CloudDrive2 token remained unauthorized after renewal"
                    ) from retry_err
                raise

    def login(self, user: str, password: str, totp: str | None = None):
        req = pb.GetTokenRequest(userName=user, password=password)
        if totp:
            req.totpCode = totp
        try:
            tok = self.stub.GetToken(req, timeout=45.0)
        except grpc.RpcError as e:
            detail = e.details() or ""
            suffix = f" ({detail})" if detail else ""
            raise SystemExit(f"login failed: {e.code().name}{suffix}") from e
        if not tok.success:
            raise SystemExit(f"login failed: {tok.errorMessage}")
        with self._auth_lock:
            self.token = tok.token
            # TOTP codes are single-use, so only password logins can be renewed automatically.
            self.creds = None if totp else (user, password)
            exp = tok.expiration.seconds if tok.expiration and tok.expiration.seconds else 0
            self.expires = exp or (time.time() + 3600)
            self._last_renew_fail = 0.0
            self._renew_backoff = 0.0
        return tok

    def keep_login(self) -> None:
        """Renew the CloudDrive2 token before expiry with exponential backoff on failure."""
        with self._auth_lock:
            now = time.time()
            if not self.creds or now < self.expires - 600:
                return
            if self._renew_backoff > 0 and (now - self._last_renew_fail) < self._renew_backoff:
                return
            try:
                self.login(*self.creds)
                logger.info("CloudDrive2 token renewed")
            except (grpc.RpcError, SystemExit) as e:
                self._last_renew_fail = now
                self._renew_backoff = (
                    5.0 if self._renew_backoff <= 0 else min(self._renew_backoff * 2, 120.0)
                )
                logger.warning(
                    "CloudDrive2 token renewal failed (%s); next retry in %.0fs",
                    e,
                    self._renew_backoff,
                )

    def find(self, path: str):
        """Return CloudDriveFile or None strictly when gRPC status is NOT_FOUND."""
        try:
            f = self._call(
                self.stub.FindFileByPath,
                pb.FindFileByPathRequest(parentPath="", path=path),
            )
            return f if (f.fullPathName or f.name) else None
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise

    def delete_batch(
        self,
        paths: list[str],
        permanent: bool = False,
        timeout: float = 45.0,
    ) -> pb.FileOperationResult:
        if not paths:
            return pb.FileOperationResult(success=True)
        method = self.stub.DeleteFilesPermanently if permanent else self.stub.DeleteFiles
        return self._call(method, pb.MultiFileRequest(path=list(paths)), timeout=timeout)

    def delete_one(
        self,
        path: str,
        permanent: bool = False,
        timeout: float = 45.0,
    ) -> pb.FileOperationResult:
        method = self.stub.DeleteFilePermanently if permanent else self.stub.DeleteFile
        return self._call(method, pb.FileRequest(path=path), timeout=timeout)

    def ensure_dir(self, path: str) -> tuple[bool, str]:
        """Create every missing segment of `path`, returning (ok, error_message)."""
        parts = [p for p in path.split("/") if p]
        if not parts:
            return True, ""
        cur = ""
        for name in parts:
            nxt = f"{cur}/{name}"
            f = self.find(nxt)
            if f is None:
                res = self._call(
                    self.stub.CreateFolder,
                    pb.CreateFolderRequest(parentPath=cur or "/", folderName=name),
                )
                if not res.result.success:
                    return False, res.result.errorMessage or f"CreateFolder failed for {nxt}"
            elif not f.isDirectory:
                return False, f"{nxt} exists and is not a directory"
            cur = nxt
        return True, ""

    def walk(self, root: str, tries: int = 3):
        """Depth-first file walk using cached GetSubFiles; aborts if any dir fails after retries."""
        attempts = max(1, tries)
        stack, seen = [root.rstrip("/") or "/"], set()
        while stack:
            d = stack.pop()
            if d in seen:
                continue
            seen.add(d)
            items = []
            for attempt in range(attempts):
                try:
                    items = []
                    call = self._call(
                        self.stub.GetSubFiles,
                        pb.ListSubFileRequest(path=d, forceRefresh=False),
                        timeout=600.0,
                    )
                    for reply in call:
                        items.extend(reply.subFiles)
                    break
                except grpc.RpcError as e:
                    if e.code() not in _TRANSIENT_WALK_CODES or attempt == attempts - 1:
                        raise
                    logger.warning(
                        "list %s failed (%s), retry %d/%d",
                        d,
                        e.code().name,
                        attempt + 2,
                        attempts,
                    )
                    time.sleep(2)
            for f in items:
                if f.isDirectory:
                    stack.append(f.fullPathName)
                else:
                    yield f

    def list_dir(self, path: str, force_refresh: bool = False) -> list:
        out = []
        call = self._call(
            self.stub.GetSubFiles,
            pb.ListSubFileRequest(path=path, forceRefresh=force_refresh),
            timeout=600.0,
        )
        for reply in call:
            out.extend(reply.subFiles)
        return out

    def cloud_of(self, path: str):
        """Return (CloudAPI | None, why | None, absent: bool) for the top-level mount of `path`."""
        stripped = path.strip("/")
        if not stripped:
            return None, "empty mount path", True
        mount = "/" + stripped.split("/")[0]
        try:
            f = self.find(mount)
        except grpc.RpcError as e:
            return None, f"mount lookup failed: {e.code().name}", False
        if f is None:
            return None, f"mount {mount} not present", True
        if not (f.HasField("CloudAPI") and f.CloudAPI.name):
            return None, f"{mount} is not a cloud mount", True
        return f.CloudAPI, None, False

    def check_dest(self, dest: str, expect_regex: str | re.Pattern[str]) -> str | None:
        api, why, _ = self.cloud_of(dest)
        if why:
            return why
        if isinstance(expect_regex, re.Pattern):
            matched = expect_regex.search(api.name)
            pattern = expect_regex.pattern
        else:
            matched = re.search(expect_regex, api.name, re.I)
            pattern = expect_regex
        if not matched:
            return f"cloud is {api.name!r}, expected /{pattern}/"
        return None

    def submit_one(self, src: str, dest: str) -> tuple[bool, str]:
        res = self._call(
            self.stub.CopyFile,
            pb.CopyFileRequest(
                theFilePaths=[src],
                destPath=dest,
                conflictPolicy=pb.CopyFileRequest.Rename,
                handleConflictRecursively=False,
            ),
        )
        return res.success, res.errorMessage

    def restart_task(self, src: str, dest: str) -> None:
        self._call(
            self.stub.RestartCopyTask,
            pb.CopyTaskRequest(sourcePath=src, destPath=dest),
            is_local_cd2_rpc=True,
        )

    def remove_tasks(self, keys: list[str]) -> tuple[bool, set[str]]:
        if not keys:
            return True, set()
        key_list = list(keys)
        try:
            res = self._call(
                self.stub.RemoveCopyTasks,
                pb.CopyTaskBatchRequest(taskKeys=key_list),
                timeout=120.0,
                is_local_cd2_rpc=True,
            )
        except grpc.RpcError as e:
            logger.warning("remove_tasks failed: %s %s", e.code().name, e.details())
            return False, set(key_list)
        if not res.success:
            logger.warning(
                "remove_tasks rejected: %s (affected=%d/%d)",
                res.errorMessage,
                res.affectedCount,
                len(key_list),
            )
            return False, set(key_list)
        return True, set()

    def tasks(self) -> list:
        return list(
            self._call(
                self.stub.GetCopyTasks,
                empty_pb2.Empty(),
                timeout=180.0,
                is_local_cd2_rpc=True,
            ).copyTasks
        )

    def push_messages(self):
        return self.stub.PushMessage(empty_pb2.Empty(), metadata=self._md())

    def close(self) -> None:
        if self.channel is not None:
            try:
                self.channel.close()
            except Exception:
                pass
