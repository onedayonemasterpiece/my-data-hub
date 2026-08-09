-- Semantic command boundary, disconnected changesets, receipts and external side-effect outbox.
CREATE TABLE sync.session (
    session_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id            text NOT NULL,
    client_id           text NOT NULL,
    client_kind         text NOT NULL,
    base_revision       bigint NOT NULL CHECK (base_revision >= 0),
    status              text NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'flushed', 'closed', 'abandoned')),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    opened_at           timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz NOT NULL DEFAULT now(),
    closed_at           timestamptz
);

CREATE TABLE sync.command (
    command_id          uuid PRIMARY KEY,
    session_id          uuid REFERENCES sync.session(session_id) ON DELETE SET NULL,
    client_id           text NOT NULL,
    actor_id            text NOT NULL,
    idempotency_key     text NOT NULL,
    command_type        text NOT NULL,
    schema_version      text NOT NULL,
    base_revision       bigint NOT NULL CHECK (base_revision >= 0),
    expected_revision   bigint CHECK (expected_revision IS NULL OR expected_revision >= 0),
    target_type         text,
    target_id           uuid,
    depends_on          uuid[] NOT NULL DEFAULT '{}'::uuid[],
    input_fingerprint   text NOT NULL,
    payload             jsonb NOT NULL,
    reason              text,
    status              text NOT NULL DEFAULT 'accepted'
                        CHECK (status IN ('accepted', 'dry_run', 'applied', 'rejected', 'conflicted', 'quarantined')),
    dry_run             boolean NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now(),
    applied_at          timestamptz,
    UNIQUE (client_id, idempotency_key)
);
CREATE INDEX sync_command_status_idx ON sync.command (status, created_at);

CREATE TABLE sync.command_receipt (
    command_id          uuid PRIMARY KEY REFERENCES sync.command(command_id) ON DELETE RESTRICT,
    accepted_revision   bigint CHECK (accepted_revision IS NULL OR accepted_revision >= 0),
    result              jsonb NOT NULL DEFAULT '{}'::jsonb,
    output_fingerprint  text NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER command_receipt_append_only
BEFORE UPDATE OR DELETE ON sync.command_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE sync.changeset_header (
    changeset_id        uuid PRIMARY KEY,
    session_id          uuid NOT NULL REFERENCES sync.session(session_id) ON DELETE RESTRICT,
    client_id           text NOT NULL,
    actor_id            text NOT NULL,
    idempotency_key     text NOT NULL,
    base_revision       bigint NOT NULL CHECK (base_revision >= 0),
    schema_version      text NOT NULL,
    depends_on          uuid[] NOT NULL DEFAULT '{}'::uuid[],
    input_fingerprint   text NOT NULL,
    changeset_sha256    text NOT NULL UNIQUE,
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'applied', 'merged', 'deduplicated', 'conflict', 'rejected', 'quarantined')),
    reason              text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    applied_at          timestamptz,
    UNIQUE (client_id, idempotency_key)
);
CREATE INDEX changeset_status_idx ON sync.changeset_header (status, created_at);

CREATE TABLE sync.changeset_operation (
    changeset_id        uuid NOT NULL REFERENCES sync.changeset_header(changeset_id) ON DELETE CASCADE,
    operation_index     integer NOT NULL CHECK (operation_index >= 0),
    operation_id        uuid NOT NULL UNIQUE,
    operation_kind      text NOT NULL,
    target_type         text,
    target_id           uuid,
    expected_revision   bigint CHECK (expected_revision IS NULL OR expected_revision >= 0),
    preconditions       jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload             jsonb NOT NULL,
    PRIMARY KEY (changeset_id, operation_index)
);

CREATE TABLE sync.applied_changeset (
    changeset_id        uuid PRIMARY KEY REFERENCES sync.changeset_header(changeset_id) ON DELETE RESTRICT,
    canonical_revision  bigint NOT NULL CHECK (canonical_revision >= 1),
    result_status       text NOT NULL
                        CHECK (result_status IN ('applied', 'merged', 'deduplicated', 'stale', 'conflict', 'rejected', 'quarantined')),
    result_sha256       text NOT NULL,
    applied_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sync.conflict (
    conflict_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    command_id          uuid REFERENCES sync.command(command_id) ON DELETE RESTRICT,
    changeset_id        uuid REFERENCES sync.changeset_header(changeset_id) ON DELETE RESTRICT,
    operation_id        uuid,
    target_type         text,
    target_id           uuid,
    conflict_kind       text NOT NULL,
    expected_state      jsonb,
    canonical_state     jsonb,
    attempted_operation jsonb NOT NULL,
    status              text NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'resolved', 'rejected', 'superseded')),
    resolution          jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    resolved_at         timestamptz,
    CHECK (command_id IS NOT NULL OR changeset_id IS NOT NULL)
);
CREATE INDEX conflict_open_idx ON sync.conflict (created_at) WHERE status = 'open';

CREATE TABLE sync.id_remap (
    provisional_id      uuid PRIMARY KEY,
    canonical_id        uuid NOT NULL,
    entity_type         text NOT NULL,
    command_id          uuid REFERENCES sync.command(command_id) ON DELETE RESTRICT,
    changeset_id        uuid REFERENCES sync.changeset_header(changeset_id) ON DELETE RESTRICT,
    reason              text NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (command_id IS NOT NULL OR changeset_id IS NOT NULL)
);

CREATE TABLE sync.external_outbox (
    outbox_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type      text NOT NULL,
    aggregate_id        uuid,
    effect_kind         text NOT NULL,
    idempotency_key     text NOT NULL UNIQUE,
    payload             jsonb NOT NULL,
    required_revision   bigint NOT NULL CHECK (required_revision >= 0),
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'leased', 'delivered', 'retry', 'terminal', 'cancelled')),
    available_at        timestamptz NOT NULL DEFAULT now(),
    attempt_count       integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner         text,
    lease_token         uuid,
    lease_expires_at    timestamptz,
    last_error          jsonb,
    receipt             jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX external_outbox_pending_idx
    ON sync.external_outbox (available_at, created_at)
    WHERE status IN ('pending', 'retry');
CREATE TRIGGER external_outbox_set_updated_at
BEFORE UPDATE ON sync.external_outbox
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE sync.checkpoint (
    checkpoint_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_revision  bigint NOT NULL UNIQUE CHECK (canonical_revision >= 0),
    checkpoint_kind     text NOT NULL CHECK (checkpoint_kind IN ('hot_physical', 'portable_logical')),
    locator             text NOT NULL,
    sha256              text NOT NULL,
    manifest_sha256     text NOT NULL,
    parent_checkpoint_id uuid REFERENCES sync.checkpoint(checkpoint_id) ON DELETE RESTRICT,
    postgres_major      integer NOT NULL,
    extension_versions  jsonb NOT NULL,
    encrypted           boolean NOT NULL DEFAULT true,
    verified_readback_at timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sync.audit_event (
    audit_event_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id            text NOT NULL,
    client_id           text NOT NULL,
    action              text NOT NULL,
    outcome             text NOT NULL,
    subject_type        text,
    subject_id          uuid,
    command_id          uuid REFERENCES sync.command(command_id) ON DELETE SET NULL,
    details             jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_event_time_idx ON sync.audit_event (occurred_at DESC);
CREATE INDEX audit_event_actor_idx ON sync.audit_event (actor_id, occurred_at DESC);
CREATE TRIGGER audit_event_append_only
BEFORE UPDATE OR DELETE ON sync.audit_event
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
