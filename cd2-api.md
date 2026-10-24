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
- `cloudapi.dll`: 33 MB Rust cdylib containing the core service engine: tonic 0.14.5 gRPC server, Web UI, WebDAV server, transfer scheduler, and storage providers (115, 123pan, and others). Stripped binary protected by `client_integrity` validation.
- `CloudDrive.ProtoTypes.dll`: Compiled C# protobuf contract types. Defines the `clouddrive.CloudDriveFileSrv` service comprising 258 RPCs and 346 message types. `clouddrive.proto` is reconstructed from this assembly.

## gRPC Endpoint and Authentication

The client and engine communicate over local gRPC (default: `127.0.0.1:19798`, HTTP/2 plaintext, or optional HTTPS with a self-signed certificate).

Endpoints accept `host:port` (plaintext), `http://host[:port]`, or `https://host[:port]`. If omitted, ports default to 80 for HTTP and 443 for HTTPS. TLS endpoints validate against system root certificates; self-signed certificates are specified via `--ca-cert <PEM>`.

Authentication requires an authorization metadata header on every call:

```
authorization: Bearer <token>
```

Tokens are obtained via `GetToken(userName, password, totpCode)`, returning a JWT and an expiration timestamp. The engine does not renew tokens automatically; renewal logic belongs to the calling client.

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
  TaskStatus status = 5;                             // Pending | Scanning | Scanned | Completed | Failed
  uint64 totalFolders = 6;
  uint64 totalFiles = 7;
  uint64 failedFolders = 8;
  uint64 failedFiles = 9;
  uint64 uploadedFiles = 10;
  uint64 cancelledFiles = 11;
  uint64 skippedFiles = 16;
  uint64 totalBytes = 12;
  uint64 uploadedBytes = 13;
  bool paused = 14;
  repeated TaskError errors = 15;                    // Timestamp time, string message
  Timestamp startTime = 17;
  optional Timestamp endTime = 18;
}
```

### Key Engine Behaviors

1. Composite Identity: Tasks do not have independent numeric identifiers. The primary key is `sourcePath + ":" + destPath` (matching `CopyTask.Key`). Pause, cancel, restart, and removal RPCs all address tasks using this path pair.
2. SQLite Persistence: The engine stores tasks and upload states in a local SQLite database under `transfer_tasks`:
   `key, task_order, task_type, entry_source, dest_path, filename, size, upload_filename, upload_string, shadow_file_id, local_data_range, operator, crypt_header`.
   `upload_string` holds the 115 upload session; `local_data_range` tracks uploaded byte ranges, enabling interrupted transfers to resume without restarting.
3. Destination Preparation: The engine verifies source and destination paths prior to copying. Missing destination directories are created automatically by the engine; existing directories with conflicting names trigger an error.
4. Restart Deduplication: Files completed during an earlier run are not re-uploaded when a task restarts. The engine rescans and applies `conflictPolicy` (`Skip` ignores already transferred files; `Overwrite` replaces them).
5. Error State Handling: The engine does not automatically retry failed tasks. Errors such as destination unavailability append to `errors` and leave the task in `Failed` status. Resuming requires an explicit `RestartCopyTask` call.

### Task Management RPCs

| RPC | Request | Purpose |
| --- | --- | --- |
| `GetCopyTasks` | `google.protobuf.Empty` | Lists all active and completed tasks |
| `RestartCopyTask` | `CopyTaskRequest` | Rescans and resumes a task |
| `PauseCopyTask` | `PauseCopyTaskRequest` | Pauses or resumes a task |
| `CancelCopyTask` | `CopyTaskRequest` | Cancels a task |
| `RemoveCopyTasks` | `CopyTaskBatchRequest` | Removes specified task records |
| `CreateFolder` | `CreateFolderRequest` | Creates directory hierarchy |
| `FindFileByPath` | `FindFileByPathRequest` | Locates file or directory (returns `NOT_FOUND` if absent) |
| `GetSubFiles` | `ListSubFileRequest` | Lists directory contents via server streaming |
| `PushTaskChange` | `google.protobuf.Empty` | Server-streaming notifications of task and upload state changes |

## Pipeline Engine (`cd2_stream.py`)

`cd2_stream.py` acts as an external transfer pump driving CloudDrive2 without modifying the source tree.

### Transfer and Submission Mechanics

- Individual File Submission: Every file is submitted via `CopyFile` with `conflictPolicy = Rename`.
- Per-Basename Serialization: Deletion on 115 relies on `dest/<basename>`. To avoid ambiguous landed files or engine renaming (`filename (1)`), only one file of a given basename can be in flight (`submitted` or `landed`) at any time. Any pre-existing file at `dest/<basename>` is recycled before submitting.
- Completion Verification: A transfer is marked `landed` only when `status == CopyTask.Completed`, `uploadedFiles >= 1`, and `failedFiles == cancelledFiles == skippedFiles == 0`. Once recorded, the task row is pruned via `RemoveCopyTasks([f"{source}:{dest}"])`.
- Engine Scheduler Independence: Tasks in `Pending`, `Scanning`, or `Scanned` states are managed by CloudDrive2's internal scheduler and are not restarted by the pipeline.
- Bounded Restarts: Tasks in `Failed` status (or unfulfilled completions) wait `--retry-after` seconds before restarting in place with `RestartCopyTask`. Restarts run after landed copies are cleared each sweep round, capped at `--batch` per round. Reaching `--max-attempts` transitions the record to `failed`. If a parked `failed` record's own task (same destination path and matching byte size) subsequently reports `Completed`, it counts as uploaded and transitions to `landed`.
- Vanished Task Reconciliation: A `submitted` record whose task is absent from `GetCopyTasks` is requeued to `pending` if missing for `--stall` seconds, or immediately upon an account change.
- Landed File Cleanup: Landed files are deleted into the recycle bin (`DeleteFile`), removed permanently (`DeleteFilePermanently`), or kept in place (`--delete none`). If cleanup fails or is delayed past `--stall`, the file transitions to `stranded`. The `stranded` state also covers files that landed under another name (where the same-name destination slot is occupied by a directory or a file of different size). Upload status remains confirmed; stranded files are never resubmitted.
- Server Push Notifications: The pipeline listens on `PushTaskChange` in a background daemon thread, waking the sweep loop immediately whenever task progress occurs.

### Persistence and Account Swapping

- State Ledger: Manifest records are persisted in an append-only JSONL file with `fsync` on every state write. Damaged trailing lines from crashes are truncated during startup.
- Mount Account Detection: The pipeline polls the destination mount root via `FindFileByPath` and inspects `CloudAPI.name` and `CloudAPI.userName`. If the mount unmounts or the account changes:
  - Records in `failed` state are reset to `pending` with cleared retry budgets.
  - Tasks lost during the account transition are requeued to `pending` immediately.
  - Records already marked `landed`, `dropped`, `kept`, or `stranded` remain complete.
- Token Renewal: When authenticated via username and password without TOTP, `cd2_stream.py` automatically renews the CloudDrive2 JWT token 10 minutes prior to expiration.

## Concurrency and Rate Limits

Transfer concurrency and bandwidth are governed by CloudDrive2's native settings:

| Scope | Field | Purpose |
| --- | --- | --- |
| `SystemSettings` | `maxProcessTasks` | Concurrent active upload tasks |
| `SystemSettings` | `maxPreProcessTasks` | Concurrent pre-processing tasks (hashing, fast upload check) |
| `SystemSettings` | `tempFileLocation` | Disk cache path for cross-cloud transfers |
| `SystemSettings` | `useTempFileForCrossCloudCopy` | Enables local disk spooling during cross-cloud copying |
| `SystemSettings` | `maxUploadSpeedKBytesPerSecond` | Global upload bandwidth limit |
| `SystemSettings` | `maxDownloadSpeedKBytesPerSecond` | Global download bandwidth limit |
| `CloudAPIConfig` | `maxUploadThreads` | Upload thread pool size for a specific cloud driver |

Settings are read and modified via `GetSystemSettings` / `SetSystemSettings` and `GetCloudAPIConfig` / `SetCloudAPIConfig`.

Cross-cloud copies spool through local temporary files in `copy_spool` only when `useTempFileForCrossCloudCopy` is enabled. When enabled, the effective concurrency limit is bounded by available disk space at `tempFileLocation` (`maxProcessTasks * file_size`).

## Reverse-Engineering Reproduction Steps

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

# 6. Generate Python gRPC client stubs
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```
