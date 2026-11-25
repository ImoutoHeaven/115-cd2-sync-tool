# CloudDrive2 Copy Task API and Pipeline Internals

Target version: CloudDrive2 1.1.3-3 (windows-x64). Reference artifacts in this repository: `clouddrive.proto`, `clouddrive_pb2.py`, and `clouddrive_pb2_grpc.py`.

## Installer Structure

`CloudDrive2-1.1.3-3-windows-x64.exe` is a WiX Burn bootstrapper package containing an attached CAB archive with two MSI payloads:

| Payload | Content |
| --- | --- |
| `a0` | WinFsp 2026 Beta1 (filesystem driver for mounting cloud storage) |
| `a1` | CloudDrive application files |
| `u4` | Bootstrapper user interface |

The `a1` payload extracts to `PFiles64/CloudDrive` containing:

- `CloudDrive.dll` / `CloudDrive.exe`: WinUI 3 desktop client (.NET 10, including debugging symbols).
- `cloudapi.dll`: 33 MB Rust cdylib containing the core service engine: tonic 0.14.5 gRPC server, Web UI, WebDAV server, transfer scheduler, directory cache (`dir_cache.rs`), and storage providers (115, 123pan, and others). Stripped binary protected by `client_integrity` validation.
- `CloudDrive.ProtoTypes.dll`: Compiled C# protobuf contract types. Defines the `clouddrive.CloudDriveFileSrv` service comprising 258 RPCs and 346 message types. `clouddrive.proto` is reconstructed from this assembly.

## gRPC Endpoint and Authentication

The client and engine communicate over local gRPC (default: `127.0.0.1:19798`, HTTP/2 plaintext, or optional HTTPS with a self-signed certificate).

Endpoints accept `host:port` (plaintext), `http://host[:port]`, or `https://host[:port]`. If omitted, ports default to 80 for HTTP and 443 for HTTPS. TLS endpoints validate against system root certificates; self-signed certificates are specified via `--ca-cert <PEM>`.

Authentication requires an authorization metadata header on every call:

```
authorization: Bearer <token>
```

Tokens are obtained via `GetToken(GetTokenRequest)` which returns `JWTToken`:

```proto
message JWTToken {
  bool success = 1;
  string errorMessage = 2;
  string token = 3;
  google.protobuf.Timestamp expiration = 4;
}
```

The engine does not renew tokens automatically; renewal logic belongs to the calling client.

## Copy Task API

File copying is asynchronous. Calling `CopyFile` validates arguments, records a persistent task in SQLite, and returns immediately.

```proto
message CopyFileRequest {
  repeated string theFilePaths = 1;                  // Source paths (files or directories)
  string destPath = 2;                               // Destination directory
  optional ConflictPolicy conflictPolicy = 3;        // Overwrite | Rename | Skip
  optional bool handleConflictRecursively = 5;       // Recursive conflict handling for directories
}
```

Task tracking and execution use `CopyTask`:

```proto
message CopyTask {
  TaskMode taskMode = 2;                             // Copy | Move
  string sourcePath = 3;
  string destPath = 4;
  TaskStatus status = 5;                             // Pending=0 | Scanning=1 | Scanned=2 | Completed=3 | Failed=4 (+ unnamed 5=Cancelled)
  uint64 totalFolders = 6;
  uint64 totalFiles = 7;
  uint64 failedFolders = 8;
  uint64 failedFiles = 9;
  uint64 uploadedFiles = 10;
  uint64 cancelledFiles = 11;
  uint64 skippedFiles = 16;
  uint64 totalBytes = 12;
  uint64 uploadedBytes = 13;
  bool paused = 14;                                  // Pause flag tracked independently of status
  repeated TaskError errors = 15;                    // Timestamp time, string message
  Timestamp startTime = 17;
  optional Timestamp endTime = 18;
}
```

> **Note on `TaskStatus` and `paused`**: `clouddrive.proto` defines `Pending=0`, `Scanning=1`, `Scanned=2`, `Completed=3`, and `Failed=4`, while the Rust engine also emits an unnamed numeric status `5` (`Cancelled`). Task pause state is carried on `bool paused = 14` rather than a separate `TaskStatus` enum value.

### Key Engine Behaviors

1. **Composite Identity**: Tasks do not have independent numeric identifiers. The primary key is `sourcePath + ":" + destPath` (matching `CopyTask.Key`). Pause, cancel, restart, and removal RPCs all address tasks using this path pair.
2. **SQLite Persistence & Source Watcher**: The engine stores tasks and upload states in a local SQLite database under `transfer_tasks`:
   `key, task_order, task_type, entry_source, dest_path, filename, size, upload_filename, upload_string, shadow_file_id, local_data_range, operator, crypt_header`.
   `upload_string` holds the 115 upload session; `local_data_range` tracks uploaded byte ranges, enabling interrupted transfers to resume without restarting. The engine also attaches a filesystem watcher on active copy sources and automatically re-queues a transfer if the source file is modified mid-upload.
3. **Destination Preparation**: The engine verifies source and destination paths prior to copying. Missing destination directories are created automatically by the engine; existing directories with conflicting names trigger an error.
4. **Restart Deduplication**: Files completed during an earlier run are not re-uploaded when a task restarts. The engine rescans and applies `conflictPolicy` (`Skip` ignores already transferred files; `Overwrite` replaces them).
5. **Error State Handling**: The engine does not automatically retry failed tasks. Errors such as destination unavailability append to `errors` and leave the task in `Failed` status. Resuming requires an explicit `RestartCopyTask` call.

### Task & File Management RPCs

| RPC | Request | Response | Purpose |
| --- | --- | --- | --- |
| `GetToken` | `GetTokenRequest` | `JWTToken` | Authenticates and returns a bearer JWT + expiration |
| `GetCopyTasks` | `google.protobuf.Empty` | `GetCopyTaskResult` | Purely local engine query listing active and completed tasks (no cloud I/O) |
| `RestartCopyTask` | `CopyTaskRequest` | `google.protobuf.Empty` | Rescans and resumes a task |
| `PauseCopyTask` | `PauseCopyTaskRequest` | `google.protobuf.Empty` | Pauses or resumes a task |
| `CancelCopyTask` | `CopyTaskRequest` | `google.protobuf.Empty` | Cancels a task |
| `RemoveCopyTasks` | `CopyTaskBatchRequest` | `BatchOperationResult` | Removes specified task records |
| `CreateFolder` | `CreateFolderRequest` | `CreateFolderResult` | Creates a directory |
| `FindFileByPath` | `FindFileByPathRequest` | `CloudDriveFile` | Locates a file or directory from cache (no `forceRefresh` flag; returns `NOT_FOUND` if absent) |
| `GetSubFiles` | `ListSubFileRequest` | `stream SubFilesReply` | Lists directory contents (`forceRefresh=false` reads cache; `true` refreshes remotely) |
| `ForceExpireDirCache` | `FileRequest` | `google.protobuf.Empty` | Explicitly expires cached directory entries for a path |
| `DeleteFile` / `DeleteFilePermanently` | `FileRequest` | `FileOperationResult` | Single-file delete (recycle bin or permanent) |
| `DeleteFiles` / `DeleteFilesPermanently` | `MultiFileRequest` | `FileOperationResult` | Batch multi-file delete (recycle bin or permanent) |
| `PushMessage` | `google.protobuf.Empty` | `stream CloudDrivePushMessage` | Official server-streaming notifications (`transferTaskStatus`, `fileSystemChange`, `mountPointChange`, `cloudApiChange`, `exitedMessage`); note: `PushTaskChange` returns `UNIMPLEMENTED` in CloudDrive2 1.1.3-3 |

## 115open Deletion, Throttling, and Directory Cache Internals (`cloudapi.dll`)

Decompilation of `cloudapi.dll` (1.1.3-3) and `CloudDrive.dll` reveals three mechanisms that directly shape high-throughput 115 deletion:

### 1. Batch Delete via Comma-Joined `/open/ufile/delete`

`DeleteFiles` and `DeleteFilesPermanently` accept `MultiFileRequest { repeated string path = 1; }` and return `FileOperationResult { bool success = 1; string errorMessage = 2; repeated string resultFilePaths = 3; }`.

In `cloudapi.dll`, the 115open delete implementation targets `https://proapi.115.com/open/ufile/delete` with form field `file_ids`:
- Single-file `DeleteFile` allocates a 1-element slice and sends one HTTP request per file.
- Batch `DeleteFiles` routes to the provider's `delete_files` handler, which converts all `N` target file IDs to strings and joins them with `','` into a single comma-separated `file_ids` form parameter sent in **one** `/open/ufile/delete` HTTP request.
- The official WinUI 3 client (`CloudDrive.ViewModels/FilesViewModel.cs`) likewise calls `DeleteFilesAsync` / `DeleteFilesPermanentlyAsync` for multi-file deletion rather than looping `DeleteFile`.

### 2. 115open Rate Limiting and Exponential Backoff

115open API requests pass through an internal retry/throttle executor in `cloudapi.dll`:
- On retryable throttle responses, the executor starts with a `1000ms` backoff and doubles it (`*= 2`) on each retry until exceeding `max_retry_time`.
- Once retries are exhausted, it surfaces an error formatted as `115open throttle (code ...): ...`.
- Per-cloud QPS limits are enforced by the engine via `CloudAPIConfig.maxQueriesPerSecond` (field `6`) and `CloudAPIConfig.maxQueriesPerSecondLimit` (field `20`). Batching deletes into `DeleteFiles` avoids per-file RPC storms that would otherwise trip 115open throttling.

### 3. Local `dir_cache.rs` Synchronization (No `ForceExpireDirCache` Needed)

`cloudapi\src\dir_cache.rs` maintains an in-memory DashMap plus optional SQLite (`dir_cache.sqlite`) directory cache governed by `SystemSettings.dirCacheTimeToLiveSecs` (and per-folder overrides via `SetDirCacheTimeSecs`):
- **On upload completion**: Upload workers update `dir_cache` directly (`DirCache::get_dir` -> `DirCache::remove_entry` -> `DirCache::add_entry`), inserting the newly landed file into both in-memory cache and `dir_cache.sqlite` (creating the in-memory directory entry if not yet loaded).
- **On file deletion**: Delete paths invoke `DirCache::remove_entry` to evict deleted entries locally.

Consequently, `FindFileByPath` + `DeleteFiles` can operate directly against the locally synchronized `dir_cache` with **zero** extra 115 list API calls. Calling `ForceExpireDirCache` or `GetSubFiles(forceRefresh=True)` during aggressive 115 recycle deletion is unnecessary and only wastes 115open QPS.

### 4. Rapid Upload (秒传) vs Real Upload in `UploadFileInfo` (`cloudapi.dll`)

Decompilation of `cloudapi.dll` confirms that CloudDrive2 separates rapid-upload preprocessing from real data transfer:
- **Rapid-upload worker (`0x1801d4db2`)**: Invokes 115 `rapid_upload` (`0x180861d91`, `/open/upload/init` with `file_name`, `file_size`, `fileid`, `preid`, `sign_key`, `sign_val`, `pick_code`; 0-byte files skip rapid upload). On a rapid-upload hit (tag `0x8000000000000048`), it updates local `dir_cache` (`sub_1801873BA`) and immediately sets `UploadFileInfo.statusEnum` to `Finish (5)` (`sub_180C34BF3`), completely bypassing `0x1801d2b51`, never entering `Transfer (3)`, and leaving `transferedBytes` (`task + 512`) at `0`.
- **Real-upload worker (`0x1801d2b51`)**: Immediately sets `UploadFileInfo.statusEnum` to `Transfer (3)`, increments `transferedBytes` during upload, then updates `dir_cache` and sets `Finish (5)` upon completion.
- **`UploadFileInfo` proto (`sub_180C3FB1B`)**: Serializes `size` (`sub_180C36890`) and `transferedBytes` (`sub_180C35367`), allowing `PushMessage` (`uploadFileStatusChanges`) to distinguish rapid uploads (`Finish (5)` with `transferedBytes == 0` and no prior `Transfer (3)`) from real uploads (`Transfer (3)` or `transferedBytes > 0`).

### Quota Ledger & Pre-GC Settlement

The pipeline enforces per-account daily real-upload limits (`--daily-limit`, default `800G` decimal; `--quota-window`, default `daily-utc8`; `--quota-mode`, default `real`; `--max-inflight-bytes`, default `128G`; `--quota-fit`, default `first`) directly on `manifest.jsonl` via `landed_at`, `upload_kind`, and `quota_bytes`:
- Submissions require `confirmed_real_bytes(account, window) + reserved_inflight_bytes(account) + size <= daily_limit` (where `reserved_inflight_bytes` sums `submitted` records and unsettled `landed` records with `quota_bytes is None`).
- `clear_landed` verifies local `dir_cache` via `FindFileByPath` before `DeleteFiles` (and before `--delete none` transitions to `kept`) and settles `quota_bytes`: rotated account/destination records settle immediately before `FindFileByPath` via `_calc_quota_bytes` (`0` for `rapid` in `real` mode, or `size` otherwise); verified matches and `stranded` mismatches settle via `_calc_quota_bytes` the same way; `NOT_FOUND` records expiring `--missing-grace` settle `size` if `--quota-mode submitted` or `upload_kind == "real"`, or `0` otherwise.
- When an account hits its limit, `submit_pending` and `restart_due` pause while `fast_step()`, `keep_login()`, and `dest_ready()` remain active in the foreground, resuming automatically on an account swap or when the next UTC+8 day / `rolling-24h` slot opens.

## Pipeline Engine Architecture

The transfer pump is split into five modules (`cd2_core.py`, `cd2_client.py`, `cd2_manifest.py`, `cd2_pipeline.py`, `cd2_stream.py`) driving CloudDrive2 without modifying the source tree.

### Dual-Cadence Loop and Filtered `PushMessage`

- **Fast Delete Cadence (`--delete-interval`, default `2.5`s)**: Drains `landed` records frequently so 115 space is reclaimed continuously via batch `DeleteFiles` calls without waiting for the slower sweep timer.
- **Slow Sweep Cadence (`--sweep`, default `20.0`s)**: Reconciles `GetCopyTasks` state, prunes completed tasks, submits new `CopyFile` tasks, and restarts eligible failed tasks.
- **Precise `PushMessage` Filtering**: A background daemon thread listens on `PushMessage` (`stream CloudDrivePushMessage`; note that `PushTaskChange` returns `UNIMPLEMENTED` in CloudDrive2 1.1.3-3). It wakes the pipeline (with 0.3s debounce) only on copy-task count changes or `Finish`+`Copy` `uploadFileStatusChanges` (`COPY_TASK_COUNT` / `UPLOADER_COUNT` / `DOWNLOADER_COUNT`), non-directory `CREATE` events under `dest_root` (`FILE_SYSTEM_CHANGE`), or mount/account changes (`MOUNT_POINT_CHANGE` / `CLOUD_API_CHANGE`, which also invalidate the `dest_ready` cache). If `PushMessage` returns `UNIMPLEMENTED`, the listener exits once and falls back cleanly to the `--delete-interval` / `--sweep` timers.

### Transfer, Completion, and Restart Semantics

- **Individual File Submission**: Every file is submitted via `CopyFile` with `conflictPolicy = Rename`.
- **Per-Basename Serialization**: Deletion on 115 targets `dest/<basename>`. Only one file of a given basename may be in flight (`submitted` or `landed`) at a time, and any pre-existing file at `dest/<basename>` is cleared using the configured `--delete` mode prior to submission.
- **Strict Completion Verification (`uploaded`)**: A task transitions to `landed` only when `status == CopyTask.Completed`, `uploadedFiles >= 1`, `failedFiles == cancelledFiles == skippedFiles == failedFolders == 0`, and (when `totalBytes > 0`) `uploadedBytes >= totalBytes` matching the expected file size. Recorded tasks are pruned via `RemoveCopyTasks([f"{source}:{dest}"])`.
- **Engine Scheduler Independence**: Tasks in `Pending`, `Scanning`, or `Scanned` states are owned by CloudDrive2's internal scheduler and are never restarted by the pipeline.
- **Bounded Submissions & Restarts (`--batch`)**: Both new task submissions and failed-task restarts (`RestartCopyTask` after `--retry-after` seconds) are capped at `--batch` per sweep round (`0` = unlimited). Reaching `--max-attempts` parks a record in `failed` state. If a parked `failed` record's own task later reports a clean `Completed` upload, it transitions to `landed`.
- **Vanished Task Reconciliation**: A `submitted` record absent from `GetCopyTasks` requeues to `pending` after `--stall` seconds, or immediately on an account swap or mount remount `gap`.

### Batch Deletion, Missing Grace, and `ThrottleGate`

- **Batch `DeleteFiles` Cleanup**: Landed files are verified via `FindFileByPath` (reading local `dir_cache` with no `ForceExpireDirCache` or `forceRefresh=True`) and deleted in chunks of up to `--delete-batch` (default `50`) via `DeleteFiles` (`recycle`) or `DeleteFilesPermanently` (`permanent`, gated on `CloudDriveFile.canDeletePermanently`), or left in place (`none` -> `kept`). Each delete RPC uses `--delete-timeout` (default `45.0`s).
- **Missing Grace (`--missing-grace`, default `10.0`s)**: If `FindFileByPath` returns `NOT_FOUND` for a landed file, the pipeline marks it `dropped` only after **both** exceeding `--missing-grace` seconds and observing at least 2 consecutive `NOT_FOUND` responses (`miss_count >= 2`), avoiding premature terminal transitions on transient cache races.
- **`ThrottleGate` Adaptive Backoff**: On gRPC `RESOURCE_EXHAUSTED`, `115open throttle`, HTTP `429`, or rate-limit messages, `ThrottleGate` applies exponential backoff with jitter (`--throttle-base` default `5.0`s to `--throttle-max` default `120.0`s) and AIMD batch-size halving (floor `MIN_BATCH = 5`, recovering gradually after consecutive clean batches). Cooldown pause time is accounted for so rate-limit pauses do not falsely expire `--stall`.
- **Stranded Handling**: Landed files that remain un-clearable past `--stall` (excluding throttle cooldown time) or collide with a directory / different-size file transition to `stranded`. Upload status remains confirmed; stranded files are never resubmitted.

### Persistence, Auth Boundaries, and Account Swapping

- **Cross-Process Locked `Manifest`**: `cd2_manifest.py` maintains an append-only JSONL ledger guarded by an exclusive non-blocking `<state>.lock` (`msvcrt.locking` on Windows, `fcntl.flock` on POSIX; raises `ManifestLockError` if busy). On startup, reverse-chunked seek truncates any torn trailing line and `fsync`s before loading; state transitions flush via batched `fsync` appends.
- **Explicit Auth Renewal (`CD2Client`)**: Local CloudDrive2 RPCs (`GetCopyTasks`, `RestartCopyTask`, `RemoveCopyTasks`) automatically renew the JWT token and retry once on `UNAUTHENTICATED` (raising `AuthExpiredError` if renewal is unavailable or still unauthorized), isolating local token expiry from upstream 115 authentication errors. Password logins without TOTP also renew proactively 10 minutes before expiry with exponential backoff (`5s` to `120s`) on failure.
- **Account Swap & Mount Remount Detection**: The pipeline inspects the destination mount root via `FindFileByPath` (`CloudAPI.name|CloudAPI.userName`). Either an account identity change **or** a mount remount `gap` (mount disappears and returns) resets `failed` records to `pending` with fresh retry budgets and immediately requeues vanished `submitted` tasks while preserving all confirmed uploads (`landed`, `dropped`, `kept`, `stranded`).

## Concurrency and Rate Limits

Transfer concurrency, directory caching, and QPS limits are governed by CloudDrive2's native settings:

| Scope | Field | Purpose |
| --- | --- | --- |
| `SystemSettings` | `maxProcessTasks` | Concurrent active upload tasks |
| `SystemSettings` | `maxPreProcessTasks` | Concurrent pre-processing tasks (hashing, fast upload check) |
| `SystemSettings` | `tempFileLocation` | Disk cache path for cross-cloud transfers |
| `SystemSettings` | `useTempFileForCrossCloudCopy` | Enables local disk spooling during cross-cloud copying |
| `SystemSettings` | `maxUploadSpeedKBytesPerSecond` | Global upload bandwidth limit |
| `SystemSettings` | `maxDownloadSpeedKBytesPerSecond` | Global download bandwidth limit |
| `SystemSettings` | `dirCacheTimeToLiveSecs` | Default TTL in seconds for local `dir_cache` entries |
| `CloudAPIConfig` | `maxUploadThreads` | Upload thread pool size for a specific cloud driver |
| `CloudAPIConfig` | `maxQueriesPerSecond` | Target QPS limit enforced per cloud driver |
| `CloudAPIConfig` | `maxQueriesPerSecondLimit` | Upper ceiling for per-cloud QPS configuration |

Settings are read and modified via `GetSystemSettings` / `SetSystemSettings` and `GetCloudAPIConfig` / `SetCloudAPIConfig`.

Cross-cloud copies spool through local temporary files in `copy_spool` only when `useTempFileForCrossCloudCopy` is enabled. When enabled, the effective concurrency limit is bounded by available disk space at `tempFileLocation` (`maxProcessTasks * file_size`).

## Reverse-Engineering Reproduction Steps

Generating Python gRPC stubs (step 6) requires `grpcio-tools` (`pip install grpcio-tools`) in addition to the runtime dependencies in `requirements.txt`.

```bash
# 1. Extract bootstrapper bundle
cabextract -d out/cab CloudDrive2-1.1.3-3-windows-x64.exe

# 2. Extract MSI package
msiextract -C out/msi out/cab/a1

# 3. Decompile .NET 10 client and contract assemblies
dotnet tool install -g ilspycmd
ilspycmd -p -o out/ilspy/cd out/msi/x/PFiles64/CloudDrive/CloudDrive.dll
ilspycmd -p -o out/ilspy/pt out/msi/x/PFiles64/CloudDrive/CloudDrive.ProtoTypes.dll

# 4. Dump protobuf FileDescriptorSet to binary descriptor (/out/clouddrive.desc)
# tools/dumper/dumper.csproj references assemblies at /cd/CloudDrive.ProtoTypes.dll and /cd/Google.Protobuf.dll,
# and Program.cs writes to /out/clouddrive.desc. Run inside a container with the extracted CloudDrive
# application directory mounted at /cd and an output directory mounted at /out:
dotnet run --project tools/dumper/dumper.csproj

# 5. Reconstruct .proto schema from descriptor
python tools/desc2proto.py /out/clouddrive.desc clouddrive.proto

# 6. Generate Python gRPC client stubs (requires: pip install grpcio-tools)
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```
