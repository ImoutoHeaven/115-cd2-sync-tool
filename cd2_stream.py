#!/usr/bin/env python3
"""Minimal CLI entry point for CloudDrive2 streaming transfer & cleanup."""

from __future__ import annotations

from typing import Sequence

import grpc

from cd2_client import AuthExpiredError, CD2Client
from cd2_core import DONE, log, parse_args
from cd2_manifest import Manifest, ManifestLockError
from cd2_pipeline import Pipeline, run_clean

_SCAN_BATCH_SIZE = 500


def main(argv: Sequence[str] | None = None) -> int:
    cfg = parse_args(argv)
    if cfg.token and (cfg.user or cfg.password):
        log("note: both --token and --user/--password provided; using --token")

    ctl = CD2Client(cfg.target, cfg.use_tls, token=cfg.token, ca_cert=cfg.ca_cert)
    try:
        if not ctl.token:
            if not (cfg.user and cfg.password):
                raise SystemExit("need --token or --user/--password")
            ctl.login(cfg.user, cfg.password, cfg.totp)

        if cfg.cmd == "clean":
            return run_clean(ctl, cfg)

        try:
            man_ctx = Manifest(cfg.state, cfg.src, cfg.dest)
        except ManifestLockError as e:
            raise SystemExit(str(e)) from None

        with man_ctx as man:
            log(f"scanning {cfg.src}")
            n = 0
            buf = []
            try:
                for f in ctl.walk(cfg.src):
                    buf.append(f)
                    n += 1
                    if len(buf) >= _SCAN_BATCH_SIZE:
                        man.add_batch(buf)
                        buf.clear()
                        if n % 1000 == 0:
                            log(f"  {n} files")
                if buf:
                    man.add_batch(buf)
            except AuthExpiredError as e:
                log(f"authentication expired while scanning {cfg.src}: {e}")
                return 1
            except grpc.RpcError as e:
                raise SystemExit(
                    f"listing {cfg.src} failed: {e.code().name} {e.details()}; "
                    f"nothing was submitted this run"
                ) from e

            log(
                f"scan: {n} files seen, {len(man.of(*DONE))} already done, "
                f"{len(man.of('pending'))} to move, manifest holds {len(man.recs)}"
            )
            if cfg.cmd == "scan":
                for r in list(man.recs.values())[:10]:
                    print(f"{r['state']:<10} {r['size']:>12}  {r['rel']}")
                return 0

            return Pipeline(ctl, man, cfg).run()
    finally:
        ctl.close()


if __name__ == "__main__":
    raise SystemExit(main())
