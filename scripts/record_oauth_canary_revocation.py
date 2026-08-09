#!/usr/bin/env python3
"""Append one exact canary token JTI to the OAuth revocation journal."""

from __future__ import annotations

import argparse
import json
import os
import re

SAFE_CLAIM = re.compile(r"^[A-Za-z0-9._:/@+-]{1,512}$")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url", default=os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", "")
    )
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--jti", required=True)
    parser.add_argument("--expires-at", required=True)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL or --database-url is required")
    if not SAFE_CLAIM.fullmatch(args.issuer) or not SAFE_CLAIM.fullmatch(args.jti):
        parser.error("issuer and JTI must be bounded safe claim strings")

    import psycopg

    with psycopg.connect(args.database_url, connect_timeout=3) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '2000ms'")
            cursor.execute(
                """
                INSERT INTO auth.oauth_revocation (
                    issuer, token_jti, reason, expires_at, created_by
                ) VALUES (%s, %s, 'post-deploy-canary', %s, 'devstand-deploy')
                RETURNING revocation_id, revoked_at
                """,
                (args.issuer, args.jti, args.expires_at),
            )
            revocation_id, revoked_at = cursor.fetchone()
        connection.commit()
    print(
        json.dumps(
            {
                "ok": True,
                "revocation_id": str(revocation_id),
                "issuer": args.issuer,
                "jti": args.jti,
                "revoked_at": revoked_at.isoformat(),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
