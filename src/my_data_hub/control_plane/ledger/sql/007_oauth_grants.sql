CREATE TABLE oauth_authorization_grants (
    code_digest TEXT PRIMARY KEY CHECK (length(code_digest) = 64),
    code_challenge TEXT NOT NULL,
    client_id TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    resource TEXT NOT NULL,
    scopes_json TEXT NOT NULL,
    subject TEXT NOT NULL,
    nonce TEXT,
    authenticated_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX oauth_authorization_grants_expiry
ON oauth_authorization_grants(expires_at);

CREATE TABLE oauth_refresh_grants (
    credential_digest TEXT PRIMARY KEY CHECK (length(credential_digest) = 64),
    family_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    resource TEXT NOT NULL,
    scopes_json TEXT NOT NULL,
    subject TEXT NOT NULL,
    authenticated_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    consumed_at INTEGER,
    revoked_at INTEGER,
    created_at TEXT NOT NULL
);

CREATE INDEX oauth_refresh_grants_family
ON oauth_refresh_grants(family_id);
CREATE INDEX oauth_refresh_grants_expiry
ON oauth_refresh_grants(expires_at);
