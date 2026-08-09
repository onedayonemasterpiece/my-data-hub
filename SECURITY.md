# Security policy

`my-data-hub` is a public code repository intended to operate on private data.

## Never commit

- `.env`, database URLs or passwords;
- YDB service-account material or export payloads;
- Kaggle, GitHub, Telegram, Joplin or model-provider tokens;
- decrypted PostgreSQL checkpoints or dumps;
- personal Joplin notes;
- Telegram sessions, chat exports or reviewer identity dumps;
- production logs containing payload text, authorization headers or signed URLs.

## Runtime boundaries

- PostgreSQL binds to localhost or a private network only.
- Streamable HTTP MCP binds to localhost by default.
- Remote MCP requires TLS, an explicit Host/Origin allowlist, an authorization layer and network restriction; scope checks remain enabled inside the application.
- Migration credentials are read-only, time-bounded and available only in a migration environment.
- Kaggle receives no YDB credentials.
- Backups/checkpoints are encrypted before leaving the host.
- Public health endpoints contain no build secrets, DB URL or migration details.

## Reporting

Do not open a public issue containing credentials or production data. Revoke exposed credentials first and preserve only redacted incident evidence.
