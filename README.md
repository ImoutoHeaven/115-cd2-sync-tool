# cd2Analyse

Automated transient file pump driving CloudDrive2 to transfer files from any read-only mount to 115 Cloud storage.

Each file in the source directory tree is submitted as an individual copy task to a flat folder on 115 (typically the mount root). Once CloudDrive2 reports the transfer completed, the landed file is batch-deleted into the 115 recycle bin. The original file content enters 115's global deduplication pool and can later be retrieved through 115 offline download using an ed2k link. Target filenames, folder structures, and 115 destination accounts do not affect retrieval.

The pipeline operates continuously across 115 account rotations and mount remounts on the same mount point, retrying only files that have not yet landed. Source files are strictly read-only and never modified.

## Architecture

The project is organized into five focused modules:

- `cd2_core.py`: Core constants, path/endpoint validation, completion verification (`uploaded`), throttle detection (`is_throttle_error`), `ThrottleGate` (exponential backoff with jitter and AIMD batch-size recovery), and CLI/environment `Config` parsing.
- `cd2_client.py`: Thin CloudDrive2 gRPC client (`CD2Client`) with explicit local-RPC auth renewal (`AuthExpiredError`), strict `NOT_FOUND` lookups, cached directory walks, and batch `DeleteFiles` / `DeleteFilesPermanently` calls.
- `cd2_manifest.py`: Append-only JSONL ledger (`Manifest`) with cross-process exclusive locking (`<state>.lock`, `ManifestLockError`), reverse-seek torn-tail crash repair, and batched `fsync` writes.
- `cd2_pipeline.py`: Dual-cadence transfer engine (`Pipeline`), filtered `PushMessage` listener (`PushTaskChange` returns `UNIMPLEMENTED` in CloudDrive2 1.1.3-3), fast batch-delete cycle with `ThrottleGate` and `--missing-grace`, slow reconciliation/submission/restart sweep, and `clean` runner.
- `cd2_stream.py`: Thin CLI entrypoint dispatching `scan`, `run`, and `clean`.

## Requirements

- Python 3 (tested with 3.13)
- Running CloudDrive2 instance with gRPC enabled
- Python dependencies (`grpcio>=1.70.0`, `protobuf>=7.35.1,<8`):

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Scan source tree and build or update manifest without transferring
python cd2_stream.py scan --src /123pan/movies --dest /115

# Run continuous transfer pump
python cd2_stream.py run --src /123pan/movies --dest /115

# Run transfer pump without deleting landed files from 115
python cd2_stream.py run --src /123pan/movies --dest /115 --delete none

# Inspect non-directory files directly under destination (dry run)
python cd2_stream.py clean --dest /115

# Batch-delete all direct non-directory files under destination into 115 recycle bin
python cd2_stream.py clean --dest /115 --yes
```

### Commands

- `scan`: Walks the source tree using cached `GetSubFiles`, updates the local manifest, and prints a status preview of pending and completed files.
- `run`: Executes the dual-cadence pipeline loop. Scans source files, submits tasks (holding back any file whose basename is already in flight), monitors task progression and destination changes via `PushMessage` (`PushTaskChange` returns `UNIMPLEMENTED` in CloudDrive2 1.1.3-3) and periodic sweeps, batch-cleans landed files from 115, and handles retries until all files are transferred.
- `clean`: Lists or deletes every non-directory file directly under `--dest`, regardless of the manifest or origin. On a mount root (e.g. `/115`), this targets every root file. Supports `--delete recycle` (default) and `--delete permanent`; `--delete none` is rejected. Defaults to dry run; `--yes` confirms deletion, which executes via chunked `DeleteFiles` / `DeleteFilesPermanently` batches with `ThrottleGate` rate-limit backoff.

### Options

All numeric and string options can also be configured via their corresponding environment variables (CLI flags take precedence):

| CLI Flag | Environment Variable | Default | Description |
| --- | --- | --- | --- |
| `--endpoint` | `CD2_ENDPOINT` | `127.0.0.1:19798` | CloudDrive2 gRPC endpoint (`host:port`, `http://host[:port]`, or `https://host[:port]`). |
| `--ca-cert` | `CD2_CA_CERT` | — | Path to a PEM certificate file to trust for `https://` TLS connections. |
| `--token` | `CD2_TOKEN` | — | CloudDrive2 API JWT token. |
| `--user` | `CD2_USER` | — | CloudDrive2 username. |
| `--password` | `CD2_PASSWORD` | — | CloudDrive2 password. Prefer environment variables or `--token` over CLI args. |
| `--totp` | `CD2_TOTP` | — | CloudDrive2 TOTP 2FA code (single-use; cannot auto-renew; prefer `--token` for long runs). |
| `--src` | — | — | Source mount path. Required for `scan` and `run`. Must not overlap with `--dest`. |
| `--dest` | — | `/115` | Destination mount path on 115. Must be a cloud mount or subdirectory, not `/`. |
| `--state` | — | `manifest.jsonl` | Local manifest file path (uses `<state>.lock` for cross-process locking). |
| `--delete` | `CD2_DELETE` | `recycle` | Cleanup mode: `recycle` (`DeleteFiles`), `permanent` (`DeleteFilesPermanently`, gated on `canDeletePermanently`), or `none` (keep file, record `kept`; disallowed for `clean`). |
| `--delete-interval` | `CD2_DELETE_INTERVAL` | `2.5` | Fast-cadence batch-delete loop interval in seconds (`> 0`). |
| `--delete-batch` | `CD2_DELETE_BATCH` | `50` | Maximum landed files deleted per `DeleteFiles` / `DeleteFilesPermanently` batch call (`>= 1`). |
| `--delete-timeout` | `CD2_DELETE_TIMEOUT` | `45.0` | Per-RPC timeout in seconds for delete requests (`> 0`). |
| `--missing-grace` | `CD2_MISSING_GRACE` | `10.0` | Grace window in seconds (`>= 0`); a missing landed file is marked `dropped` only after both exceeding `--missing-grace` and seeing at least 2 consecutive `NOT_FOUND` lookups (`miss_count >= 2`). |
| `--throttle-base` | `CD2_THROTTLE_BASE` | `5.0` | Base exponential backoff delay in seconds on 115open rate-limit errors (`> 0`). |
| `--throttle-max` | `CD2_THROTTLE_MAX` | `120.0` | Maximum exponential backoff delay in seconds (`>= --throttle-base`). |
| `--sweep` | `CD2_SWEEP` | `20.0` | Slow-cadence sweep interval in seconds for task reconciliation, submissions, and restarts (`> 0`). |
| `--batch` | `CD2_BATCH` | `32` | Maximum new task submissions **and** maximum failed-task restarts per sweep cycle (`0` = unlimited for eligible non-conflicting basenames). |
| `--stall` | `CD2_STALL` | `300.0` | Seconds before requeuing a vanished `submitted` task or marking an un-clearable `landed` file as `stranded` (`>= 0`). |
| `--retry-after` | `CD2_RETRY_AFTER` | `180.0` | Seconds to wait before restarting a failed task (`>= 0`). |
| `--max-attempts` | `CD2_MAX_ATTEMPTS` | `5` | Maximum submission or restart attempts before parking a record in `failed` state (`>= 1`). |
| `--expect-cloud` | `CD2_EXPECT_CLOUD` | `115` | Case-insensitive regex required to match the destination cloud driver name. |
| `--yes` | — | `false` | Confirms deletion for the `clean` subcommand. |

## Aggressive 115 Recycle Deletion

CloudDrive2 maintains its local directory cache (`dir_cache.rs`, in-memory plus `dir_cache.sqlite`) synchronously on both upload completion and file deletion:
- When an upload finishes, CloudDrive2 inserts the newly landed file directly into local `dir_cache`.
- When a file is deleted, CloudDrive2 evicts the entry directly from local `dir_cache`.

Because local `dir_cache` stays synchronized without remote directory refreshes, the pipeline **never** calls `ForceExpireDirCache` or `GetSubFiles(forceRefresh=True)` during transfer cleanup. Instead, the fast-cadence cleanup loop (`--delete-interval`, default `2.5`s) verifies landed files against local cache and sends up to `--delete-batch` (default `50`) paths in a single `DeleteFiles` RPC. CloudDrive2 translates `DeleteFiles` into a single 115open `/open/ufile/delete` HTTP request with comma-joined `file_ids`, minimizing 115open QPS consumption.

To guard against transient cache races and API rate limits:
- If `FindFileByPath` returns `NOT_FOUND` for a newly landed file, the pipeline marks it `dropped` only after **both** exceeding `--missing-grace` (default `10.0`s) and observing at least 2 consecutive `NOT_FOUND` responses (`miss_count >= 2`).
- If CloudDrive2 or 115open returns a throttle error (`RESOURCE_EXHAUSTED`, `115open throttle`, HTTP `429`, etc.), `ThrottleGate` applies exponential backoff with jitter (`--throttle-base` to `--throttle-max`) and halves the active delete batch size (down to a floor of `5`, gradually recovering after consecutive successes).

## Manifest State Machine

Transfer progress is persisted in an append-only JSONL manifest file specified by `--state`. The manifest acquires an exclusive non-blocking lock on `<state>.lock` (preventing concurrent instances from corrupting state), locks itself to a single source root, truncates any incomplete trailing line left by a crash on startup, and flushes batched writes with `fsync`.

| State | Description | Terminal Upload |
| --- | --- | --- |
| `pending` | Discovered in source directory; waiting for submission. | No |
| `submitted` | Task submitted to CloudDrive2 or adopted from an existing engine task. | No |
| `landed` | Engine reported clean copy completion (`uploadedFiles >= 1`, zero failures/skips/cancellations, full byte count). Awaiting post-transfer cleanup. | Yes |
| `dropped` | Landed file cleared from 115 (moved to recycle bin or permanently deleted). | Yes |
| `kept` | Landed file retained in place on 115 (`--delete none`). | Yes |
| `stranded` | Landed file could not be cleared within `--stall` seconds or landed under a conflicting name. | Yes |
| `failed` | Reached `--max-attempts` failures. Parked until an account swap, mount remount, or a fresh run resets the budget. | No |

Records in `landed`, `dropped`, `kept`, or `stranded` represent confirmed uploads to 115 and are never resubmitted.

## 115 Account Swapping Procedure

When the 115 account behind the mount is replaced (for example after its login expires), swap the account while `cd2_stream.py run` remains running:

1. Allow `cd2_stream.py` to process active `Completed` tasks. Completed tasks are automatically pruned from CloudDrive2 once recorded as `landed`.
2. Clear failed, paused, or stalled tasks in the CloudDrive2 interface.
3. Remove or unmount the old 115 account.
4. Mount the new 115 account under the identical mount path (e.g. `/115`).

Account-swap recovery triggers whenever the pipeline detects **either**:
- A `CloudAPI` identity change (`name|userName`), **or**
- A mount remount `gap` (the destination mount becomes absent/unmounted and later reappears).

When triggered:
- Records in `failed` state reset to `pending` with fresh retry budgets.
- `submitted` tasks lost with the previous account requeue immediately to `pending`.
- Successfully landed records (`landed`, `dropped`, `kept`, `stranded`) remain completed and are not re-uploaded.

`run` never exits while `failed` records exist; it waits for another account or remount, and exits only when every file has been uploaded.

### At-Least-Once Delivery Limit

CloudDrive2 tasks must not be manually removed while in `Completed` status until the script records them as `landed`. If an operator deletes a `Completed` task before the script records it, the script cannot distinguish it from an interrupted task; the file will be resubmitted and uploaded again to the next account.

## Testing

Run the full unit test suite with Python's unittest discovery:

```bash
python -m unittest discover -p "test_*.py"
```

## Reverse Engineering and API Reference

See [cd2-api.md](cd2-api.md) for CloudDrive2 internals, gRPC definitions, SQLite schema, 115open reverse-engineering evidence, and reproduction steps.
