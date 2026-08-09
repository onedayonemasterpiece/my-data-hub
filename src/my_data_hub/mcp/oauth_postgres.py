from __future__ import annotations

from dataclasses import dataclass

from my_data_hub.mcp.oauth import RevocationCheckError, RevocationKey


@dataclass(frozen=True, slots=True)
class PostgresRevocationStore:
    """Fail-closed bounded lookup over the append-only OAuth revocation journal."""

    database_url: str
    statement_timeout_ms: int = 1000

    def is_revoked(self, key: RevocationKey) -> bool:
        try:
            import psycopg

            with psycopg.connect(self.database_url) as connection, connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(
                    f"SET LOCAL statement_timeout = '{int(self.statement_timeout_ms)}ms'"
                )
                cursor.execute("SET LOCAL lock_timeout = '250ms'")
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM auth.oauth_revocation
                        WHERE issuer = %s
                          AND revoked_at <= now()
                          AND (expires_at IS NULL OR expires_at > now())
                          AND (
                              token_jti = %s
                              OR client_id = %s
                              OR subject = %s
                          )
                    )
                    """,
                    (key.issuer, key.token_id, key.client_id, key.subject),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RevocationCheckError("revocation query returned no result")
                return bool(row[0])
        except RevocationCheckError:
            raise
        except Exception as exc:
            raise RevocationCheckError("revocation authority is unavailable") from exc
