#!/usr/bin/env python3
"""Watchdog for the Cloudflare cache-purge credential.

Purges are best-effort by design: ``purge_urls`` logs a warning and returns, so
a broken credential never fails a publish that already wrote its data. That is
correct for the publish and blind for observability. When the purge token
lapsed on 2026-08-17 every rollup across the repo kept reporting ``success``
for twelve days while no invalidation happened anywhere — the only trace was a
warning line buried in each run's log.

This is the check that looks. It runs in the nightly freshness workflow and
fails it, so a dead or expiring credential surfaces the next morning through
the notification path the repo already has, instead of waiting for someone to
read a log.

Missing credentials are a failure, not a skip: an unset token is exactly the
state where ``purge_urls`` silently no-ops, which is the gap being closed. Pass
``--allow-unconfigured`` for local runs with no Cloudflare setup.
"""

from __future__ import annotations

import argparse
import sys

from spicy_regs.sources.cloudflare import verify_token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-unconfigured",
        action="store_true",
        help="treat absent credentials as a skip (local runs), not a failure",
    )
    args = parser.parse_args()

    problems = verify_token()

    if problems is None:
        if args.allow_unconfigured:
            print("SKIP: CLOUDFLARE_API_TOKEN / CLOUDFLARE_ZONE_ID not set")
            return 0
        print("FAIL: CLOUDFLARE_API_TOKEN / CLOUDFLARE_ZONE_ID not set — purges are silently no-oping")
        return 1

    if problems:
        print("Cloudflare purge credential problems:")
        for problem in problems:
            print(f"- {problem}")
        print("\nRotate at https://dash.cloudflare.com/profile/api-tokens (template: Purge Cache).")
        return 1

    print("OK: Cloudflare purge credential is active")
    return 0


if __name__ == "__main__":
    sys.exit(main())
