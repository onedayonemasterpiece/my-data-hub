from __future__ import annotations

from dataclasses import dataclass

from my_data_hub.mcp.oauth import RevocationCheckError, RevocationKey


@dataclass(frozen=True, slots=True)
class PostgresRevocationStore:
    """Fail-closed bounded lookup over the append-only OAuth revocation journal."""

    database_url: str
    statement_timeout_ms: int = 1000
    connect_timeout_seconds: int = 3

    def __post_init__(self) -> None:
        if not self.database_url:
            raise ValueError("revocation database URL is required")
        if not 1 <= self.statement_timeout_ms <= 30_000:
            raise ValueError("statement timeout must be between 1 and 30000 ms")
        if not 1 <= self.connect_timeout_seconds <= 30:
            raise ValueError("connect timeout must be between 1 and 30 seconds")

    def is_revoked(self, key: RevocationKey) -> bool:
        try:
            import psycopg

            with psycopg.connect(
                self.database_url, connect_timeout=self.connect_timeout_seconds
            ) as connection, connection.cursor() as cursor:
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
