CREATE TABLE oauth_clients (
    issuer TEXT NOT NULL,
    client_id TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    allowed_scopes_json TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    profile_kind TEXT NOT NULL CHECK (profile_kind IN ('reader', 'owner_operator')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (issuer, client_id)
);

CREATE TRIGGER oauth_clients_no_delete
BEFORE DELETE ON oauth_clients BEGIN SELECT RAISE(ABORT, 'oauth_clients cannot be deleted; disable instead'); END;
