# cd2Analyse

Automated transient file pump driving CloudDrive2 to transfer files from any read-only mount to 115 Cloud storage.

Each file in the source directory tree is submitted as an individual copy task to a flat folder on 115 (typically the mount root). Once CloudDrive2 reports the transfer completed, the landed file is deleted into the 115 recycle bin. The original file content enters 115's global deduplication pool and can later be retrieved through 115 offline download using an ed2k link. Target filenames, folder structures, and 115 destination accounts do not affect retrieval.

The script operates continuously across 115 account rotations on the same mount point, retrying only files that have not yet landed. Source files are strictly read-only and never modified.

## Requirements

- Python 3 (tested with 3.13)
- Running CloudDrive2 instance with gRPC enabled
- Python dependencies:

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

# Delete all direct non-directory files under destination into 115 recycle bin
python cd2_stream.py clean --dest /115 --yes
```

### Commands

- `scan`: Walks the source tree, updates the local manifest, and prints a status preview of pending and completed files.
- `run`: Executes the main pipeline loop. Scans source files, submits tasks (holding back any file whose basename is already in flight), monitors task progression, cleans landed files from 115, and handles retries until all files are transferred.
- `clean`: Lists or deletes every non-directory file directly under `--dest`, regardless of the manifest or origin. On a mount root (e.g. `/115`), this recycles every root file. Defaults to dry run; `--yes` confirms deletion.

### Options

- `--endpoint`: CloudDrive2 gRPC service endpoint. Accepts `host:port` (plaintext), `http://host[:port]`, or `https://host[:port]`. Default: `127.0.0.1:19798` or `CD2_ENDPOINT`.
- `--ca-cert`: Path to a PEM certificate file to trust for TLS connections, such as self-signed certificates. Default: `CD2_CA_CERT`.
- `--token`: CloudDrive2 API JWT token. Default: `CD2_TOKEN`.
- `--user`: CloudDrive2 username. Default: `CD2_USER`.
- `--password`: CloudDrive2 password. Default: `CD2_PASSWORD`. Use environment variables or `--token` rather than command-line arguments to prevent credential exposure in process listings.
- `--totp`: CloudDrive2 TOTP two-factor authentication code. Default: `CD2_TOTP`. TOTP tokens cannot be automatically renewed; use `--token` for long-running processes when 2FA is active.
- `--src`: Source mount path. Required for `scan` and `run`. Must not overlap with `--dest`.
- `--dest`: Destination mount path on 115. Default: `/115`.
- `--state`: Local manifest file path. Default: `manifest.jsonl`.
- `--batch`: Maximum task submissions per sweep cycle. Default: `32` (`0` submits all eligible files whose basename is not in flight).
- `--workers`: Number of concurrent worker threads for clearing landed files from 115. Default: `8`.
- `--sweep`: Main loop poll interval in seconds. Default: `20.0`.
- `--stall`: Seconds before requeuing a vanished submitted task, or marking an un-clearable landed file as stranded. Default: `300.0`.
- `--retry-after`: Seconds to wait before restarting a failed task. Default: `180.0`.
- `--max-attempts`: Maximum submission or restart attempts before parking a record in `failed` state. Default: `5`.
- `--delete`: Post-landing cleanup mode: `recycle` (default, `DeleteFile` into the cloud recycle bin), `permanent` (`DeleteFilePermanently`, only when that file has `canDeletePermanently`), or `none` (leave the file, record `kept`).
- `--expect-cloud`: Case-insensitive regular expression required to match the destination cloud driver name. Default: `115`. Prevents writes if `--dest` does not point to the expected cloud provider.
- `--yes`: Confirms deletion for the `clean` subcommand.

## Manifest State Machine

Transfer progress is persisted in an append-only JSONL manifest file specified by `--state`. The manifest is locked to a single source root and syncs to disk with `fsync`.

| State | Description | Terminal Upload |
| --- | --- | --- |
| `pending` | Discovered in source directory; waiting for submission. | No |
| `submitted` | Task submitted to CloudDrive2 or adopted from an existing engine task. | No |
| `landed` | Engine reported copy completion (`uploadedFiles >= 1`). Awaiting post-transfer cleanup. | Yes |
| `dropped` | Landed file cleared from 115 (moved to recycle bin or permanently deleted). | Yes |
| `kept` | Landed file retained in place on 115 (`--delete none`). | Yes |
| `stranded` | Landed file could not be cleared within `--stall` seconds or landed under a conflicting name. | Yes |
| `failed` | Reached `--max-attempts` failures. Parked until account swap or a fresh run resets the budget. | No |

Records in `landed`, `dropped`, `kept`, or `stranded` represent confirmed uploads to 115 and are never resubmitted.

## 115 Account Swapping Procedure

When the 115 account behind the mount is replaced (for example after its login expires), swap the account using the following steps while `cd2_stream.py run` remains running:

1. Allow `cd2_stream.py` to process active `Completed` tasks. Completed tasks are automatically pruned from CloudDrive2 once recorded as `landed`.
2. Clear failed, paused, or stalled tasks in the CloudDrive2 interface.
3. Remove or unmount the old 115 account.
4. Mount the new 115 account under the identical mount path (e.g. `/115`).

Once the destination mount becomes available under the new account:
- The pipeline detects the account change via `CloudAPI` identity (`name|userName`).
- Records in `failed` state reset to `pending` with fresh retry budgets.
- Tasks lost with the previous account requeue immediately to `pending`.
- Successfully landed records remain completed and are not re-uploaded.

`run` never exits while `failed` records exist; it waits for another account, and exits only when every file has been uploaded.

### At-Least-Once Delivery Limit

CloudDrive2 tasks must not be manually removed while in `Completed` status until the script records them as `landed`. If an operator deletes a `Completed` task before the script records it, the script cannot distinguish it from an interrupted task; the file will be resubmitted and uploaded again to the next account.

## Testing

Run unit tests with the standard Python unittest runner:

```bash
python -m unittest test_cd2_stream
```

## Reverse Engineering and API Reference

See [cd2-api.md](cd2-api.md) for CloudDrive2 internals, gRPC definitions, SQLite schema, and reverse-engineering reproduction steps.
