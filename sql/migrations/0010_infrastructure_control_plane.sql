-- R1 infrastructure control plane: connector intake, provider resources, recovery evidence,
-- and disposable database-operator audit. These tables retain evidence; they do not make
-- Kaggle or connector payloads a second canonical head.
CREATE SCHEMA integration;
CREATE SCHEMA recovery;
CREATE SCHEMA operator_control;
CREATE SCHEMA auth;

CREATE TABLE auth.oauth_revocation (
    revocation_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    issuer                  text NOT NULL,
    token_jti               text,
    client_id               text,
    subject                 text,
    reason                  text NOT NULL,
    revoked_at              timestamptz NOT NULL DEFAULT now(),
    expires_at              timestamptz,
    created_by              text NOT NULL,
    CHECK (num_nonnulls(token_jti, client_id, subject) >= 1),
    CHECK (expires_at IS NULL OR expires_at > revoked_at)
);
CREATE INDEX oauth_revocation_token_idx
    ON auth.oauth_revocation (issuer, token_jti) WHERE token_jti IS NOT NULL;
CREATE INDEX oauth_revocation_client_idx
    ON auth.oauth_revocation (issuer, client_id) WHERE client_id IS NOT NULL;
CREATE INDEX oauth_revocation_subject_idx
    ON auth.oauth_revocation (issuer, subject) WHERE subject IS NOT NULL;
CREATE TRIGGER oauth_revocation_append_only
BEFORE UPDATE OR DELETE ON auth.oauth_revocation
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE integration.connector (
    connector_id            text PRIMARY KEY,
    owner_principal         text NOT NULL,
    service_principal       text NOT NULL UNIQUE,
    delivery_mode           text NOT NULL CHECK (delivery_mode IN ('push', 'pull', 'artifact', 'trusted_landing')),
    status                  text NOT NULL DEFAULT 'paused' CHECK (status IN ('active', 'paused', 'revoked')),
    policy_revision         bigint NOT NULL DEFAULT 1 CHECK (policy_revision >= 1),
    expected_cadence        interval,
    policy                  jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER connector_set_updated_at
BEFORE UPDATE ON integration.connector
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE integration.data_product (
    data_product            text PRIMARY KEY,
    connector_id            text NOT NULL REFERENCES integration.connector(connector_id) ON DELETE RESTRICT,
    schema_version          text NOT NULL,
    normalizer_contract     text NOT NULL,
    sensitivity             text NOT NULL DEFAULT 'non_sensitive'
                            CHECK (sensitivity IN ('non_sensitive', 'internal', 'confidential')),
    enabled                 boolean NOT NULL DEFAULT false,
    configuration           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (connector_id, data_product, schema_version)
);
CREATE TRIGGER data_product_set_updated_at
BEFORE UPDATE ON integration.data_product
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE integration.batch (
    batch_id                uuid PRIMARY KEY,
    connector_id            text NOT NULL REFERENCES integration.connector(connector_id) ON DELETE RESTRICT,
    data_product            text NOT NULL REFERENCES integration.data_product(data_product) ON DELETE RESTRICT,
    idempotency_key         text NOT NULL,
    contract_version        text NOT NULL,
    schema_version          text NOT NULL,
    payload_sha256          text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
    envelope_sha256         text NOT NULL CHECK (envelope_sha256 ~ '^[a-f0-9]{64}$'),
    record_count            bigint NOT NULL CHECK (record_count >= 0),
    delivery_mode           text NOT NULL CHECK (delivery_mode IN ('inline', 'artifact')),
    producer_partition      text,
    period_start            timestamptz,
    period_end              timestamptz,
    source_cursor           jsonb,
    authenticated_principal text NOT NULL,
    correlation_id          text NOT NULL,
    supersedes_batch_id     uuid REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    correction_reason       text,
    status                  text NOT NULL DEFAULT 'accepted' CHECK (status IN (
                                'accepted', 'staged', 'normalized', 'canonical_committed', 'reconciled',
                                'rejected_contract', 'conflicting_replay', 'quarantined_semantic',
                                'expired_uncommitted'
                            )),
    accepted_at             timestamptz NOT NULL DEFAULT now(),
    committed_at            timestamptz,
    UNIQUE (connector_id, idempotency_key),
    CHECK (period_end IS NULL OR period_start IS NULL OR period_end > period_start),
    CHECK ((supersedes_batch_id IS NULL) = (correction_reason IS NULL))
);
CREATE INDEX integration_batch_status_idx ON integration.batch (status, accepted_at);
CREATE INDEX integration_batch_product_period_idx
    ON integration.batch (data_product, producer_partition, period_start, period_end);

CREATE OR REPLACE FUNCTION integration.reject_batch_identity_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (NEW.batch_id, NEW.connector_id, NEW.data_product, NEW.idempotency_key,
        NEW.contract_version, NEW.schema_version, NEW.payload_sha256, NEW.envelope_sha256,
        NEW.record_count, NEW.delivery_mode, NEW.authenticated_principal,
        NEW.supersedes_batch_id, NEW.correction_reason)
       IS DISTINCT FROM
       (OLD.batch_id, OLD.connector_id, OLD.data_product, OLD.idempotency_key,
        OLD.contract_version, OLD.schema_version, OLD.payload_sha256, OLD.envelope_sha256,
        OLD.record_count, OLD.delivery_mode, OLD.authenticated_principal,
        OLD.supersedes_batch_id, OLD.correction_reason) THEN
        RAISE EXCEPTION 'integration.batch accepted identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER integration_batch_identity_immutable
BEFORE UPDATE ON integration.batch
FOR EACH ROW EXECUTE FUNCTION integration.reject_batch_identity_change();

CREATE TABLE integration.batch_payload (
    batch_id                uuid PRIMARY KEY REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    exact_envelope          bytea NOT NULL,
    inline_payload          jsonb,
    artifact_reference      jsonb,
    byte_size               bigint NOT NULL CHECK (byte_size >= 0),
    created_at              timestamptz NOT NULL DEFAULT now(),
    CHECK ((inline_payload IS NOT NULL)::integer + (artifact_reference IS NOT NULL)::integer = 1)
);
CREATE TRIGGER integration_batch_payload_append_only
BEFORE UPDATE OR DELETE ON integration.batch_payload
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE integration.batch_event (
    batch_event_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                uuid NOT NULL REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    event_type              text NOT NULL,
    actor_principal         text NOT NULL,
    correlation_id          text NOT NULL,
    details                 jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX integration_batch_event_idx ON integration.batch_event (batch_id, occurred_at);
CREATE TRIGGER integration_batch_event_append_only
BEFORE UPDATE OR DELETE ON integration.batch_event
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE integration.watermark (
    data_product            text NOT NULL REFERENCES integration.data_product(data_product) ON DELETE RESTRICT,
    producer_partition      text NOT NULL DEFAULT '',
    source_cursor           jsonb,
    period_end              timestamptz,
    committed_batch_id      uuid NOT NULL REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    canonical_revision      bigint NOT NULL CHECK (canonical_revision >= 0),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (data_product, producer_partition)
);

CREATE TABLE integration.daily_statistic (
    batch_id                uuid PRIMARY KEY REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    data_product            text NOT NULL REFERENCES integration.data_product(data_product) ON DELETE RESTRICT,
    reporting_date          date NOT NULL,
    timezone                text NOT NULL,
    source_revision         text NOT NULL,
    counters                jsonb NOT NULL CHECK (jsonb_typeof(counters) = 'object'),
    canonical_revision      bigint NOT NULL UNIQUE CHECK (canonical_revision >= 1),
    committed_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (data_product, reporting_date, timezone)
);
CREATE TRIGGER integration_daily_statistic_append_only
BEFORE UPDATE OR DELETE ON integration.daily_statistic
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE integration.quarantine (
    quarantine_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                uuid REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    connector_id            text NOT NULL,
    idempotency_key         text NOT NULL,
    reason_code             text NOT NULL,
    expected_sha256         text CHECK (expected_sha256 IS NULL OR expected_sha256 ~ '^[a-f0-9]{64}$'),
    observed_sha256         text CHECK (observed_sha256 IS NULL OR observed_sha256 ~ '^[a-f0-9]{64}$'),
    exact_envelope          bytea,
    status                  text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved', 'superseded')),
    resolution              jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    resolved_at             timestamptz
);
CREATE INDEX integration_quarantine_open_idx ON integration.quarantine (created_at) WHERE status = 'open';

CREATE TABLE integration.receipt (
    receipt_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                uuid REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    receipt_type            text NOT NULL CHECK (receipt_type IN ('accepted', 'duplicate', 'conflict', 'committed', 'rejected')),
    connector_id            text NOT NULL,
    idempotency_key         text NOT NULL,
    payload_sha256          text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
    canonical_revision      bigint CHECK (canonical_revision IS NULL OR canonical_revision >= 0),
    correlation_id          text NOT NULL,
    receipt                 jsonb NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX integration_receipt_batch_idx ON integration.receipt (batch_id, created_at DESC);
CREATE TRIGGER integration_receipt_append_only
BEFORE UPDATE OR DELETE ON integration.receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE integration.provider_resource (
    resource_id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider                text NOT NULL CHECK (provider = 'kaggle'),
    resource_kind           text NOT NULL CHECK (resource_kind IN ('notebook', 'dataset')),
    provider_owner          text NOT NULL,
    provider_slug           text NOT NULL,
    origin                  text NOT NULL CHECK (origin IN ('orchestrator', 'mcp', 'external', 'migration')),
    control_class           text NOT NULL CHECK (control_class IN (
                                'orchestrator_protected', 'mcp_managed', 'mcp_exchange', 'external_read_only'
                            )),
    project_id              uuid REFERENCES hub.project(project_id) ON DELETE RESTRICT,
    privacy_attestation     text,
    expected_fingerprint    text,
    current_fingerprint     text,
    lifecycle_state         text NOT NULL DEFAULT 'observed'
                            CHECK (lifecycle_state IN ('observed', 'active', 'unknown', 'deleted')),
    policy_revision         bigint NOT NULL DEFAULT 1 CHECK (policy_revision >= 1),
    discovered_at           timestamptz NOT NULL DEFAULT now(),
    created_at              timestamptz,
    last_observed_at        timestamptz NOT NULL DEFAULT now(),
    deleted_at              timestamptz,
    UNIQUE (provider, resource_kind, provider_owner, provider_slug)
);
CREATE INDEX provider_resource_class_idx ON integration.provider_resource (control_class, resource_kind);

CREATE OR REPLACE FUNCTION integration.reject_protected_reclassification()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.control_class = 'orchestrator_protected' AND NEW.control_class <> OLD.control_class THEN
        RAISE EXCEPTION 'orchestrator_protected resources cannot be reclassified';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER provider_resource_protected_class
BEFORE UPDATE OF control_class ON integration.provider_resource
FOR EACH ROW EXECUTE FUNCTION integration.reject_protected_reclassification();

CREATE TABLE integration.provider_operation (
    operation_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key         text NOT NULL UNIQUE,
    resource_id             uuid REFERENCES integration.provider_resource(resource_id) ON DELETE RESTRICT,
    principal               text NOT NULL,
    scope                   text NOT NULL,
    action                  text NOT NULL,
    expected_fingerprint    text,
    arguments_fingerprint   text NOT NULL,
    fencing_token           bigint NOT NULL CHECK (fencing_token >= 1),
    status                  text NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'unknown_outcome', 'cleaned')),
    unknown_outcome         boolean NOT NULL DEFAULT false,
    provider_fingerprint    text,
    receipt                 jsonb,
    correlation_id          text NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER provider_operation_set_updated_at
BEFORE UPDATE ON integration.provider_operation
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE integration.provider_event (
    provider_event_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id             uuid REFERENCES integration.provider_resource(resource_id) ON DELETE RESTRICT,
    operation_id            uuid REFERENCES integration.provider_operation(operation_id) ON DELETE RESTRICT,
    event_type              text NOT NULL,
    observed_fingerprint    text,
    details                 jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at             timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER provider_event_append_only
BEFORE UPDATE OR DELETE ON integration.provider_event
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE recovery.evidence (
    evidence_id             uuid PRIMARY KEY,
    run_id                  text NOT NULL,
    commit_sha              text NOT NULL,
    evidence_type           text NOT NULL CHECK (evidence_type IN ('encrypted_backup', 'offhost_readback', 'isolated_restore')),
    status                  text NOT NULL CHECK (status IN ('passed', 'failed', 'blocked')),
    artifact_sha256         text CHECK (artifact_sha256 IS NULL OR artifact_sha256 ~ '^[a-f0-9]{64}$'),
    readback_sha256         text CHECK (readback_sha256 IS NULL OR readback_sha256 ~ '^[a-f0-9]{64}$'),
    encrypted               boolean NOT NULL,
    private_offhost         boolean NOT NULL,
    readback_verified       boolean NOT NULL,
    restore_verified        boolean NOT NULL,
    schema_revision         integer,
    manifest                jsonb NOT NULL,
    completed_at            timestamptz NOT NULL,
    recorded_at             timestamptz NOT NULL DEFAULT now(),
    CHECK (NOT readback_verified OR artifact_sha256 = readback_sha256)
);
CREATE INDEX recovery_evidence_fresh_idx ON recovery.evidence (evidence_type, completed_at DESC) WHERE status = 'passed';
CREATE TRIGGER recovery_evidence_append_only
BEFORE UPDATE OR DELETE ON recovery.evidence
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE operator_control.preview_receipt (
    preview_id              uuid PRIMARY KEY,
    principal               text NOT NULL,
    correlation_id          text NOT NULL,
    sql_sha256              text NOT NULL CHECK (sql_sha256 ~ '^[a-f0-9]{64}$'),
    params_sha256           text NOT NULL CHECK (params_sha256 ~ '^[a-f0-9]{64}$'),
    allowed_targets         text[] NOT NULL,
    expected_revision       bigint NOT NULL CHECK (expected_revision >= 0),
    expected_min_rows       integer NOT NULL CHECK (expected_min_rows >= 0),
    expected_max_rows       integer NOT NULL CHECK (expected_max_rows >= expected_min_rows),
    backup_evidence_id      uuid NOT NULL REFERENCES recovery.evidence(evidence_id) ON DELETE RESTRICT,
    expires_at              timestamptz NOT NULL,
    receipt_sha256          text NOT NULL UNIQUE CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
    created_at              timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER operator_preview_append_only
BEFORE UPDATE OR DELETE ON operator_control.preview_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE operator_control.apply_receipt (
    apply_id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    preview_id              uuid NOT NULL REFERENCES operator_control.preview_receipt(preview_id) ON DELETE RESTRICT,
    principal               text NOT NULL,
    idempotency_key         text NOT NULL,
    status                  text NOT NULL CHECK (status IN ('committed', 'rolled_back', 'rejected')),
    affected_rows           integer CHECK (affected_rows IS NULL OR affected_rows >= 0),
    revision_before         bigint NOT NULL CHECK (revision_before >= 0),
    revision_after          bigint CHECK (revision_after IS NULL OR revision_after >= revision_before),
    sql_sha256              text NOT NULL CHECK (sql_sha256 ~ '^[a-f0-9]{64}$'),
    params_sha256           text NOT NULL CHECK (params_sha256 ~ '^[a-f0-9]{64}$'),
    audit_receipt           jsonb NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (principal, idempotency_key)
);
CREATE TRIGGER operator_apply_append_only
BEFORE UPDATE OR DELETE ON operator_control.apply_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

INSERT INTO integration.connector (
    connector_id, owner_principal, service_principal, delivery_mode, status,
    expected_cadence, policy
) VALUES
    (
        'synthetic.daily-statistics', 'my-data-hub',
        'service:synthetic.daily-statistics', 'push', 'active', interval '1 day',
        '{"non_sensitive": true, "test_only": true}'::jsonb
    ),
    (
        'events-bot.daily-statistics', 'events-bot-new',
        'service:events-bot.daily-statistics', 'push', 'paused', interval '1 day',
        '{"non_sensitive": true, "opt_in": true, "disabled_until_canary": true}'::jsonb
    );

INSERT INTO integration.data_product (
    data_product, connector_id, schema_version, normalizer_contract,
    sensitivity, enabled, configuration
) VALUES
    (
        'synthetic.daily-statistics.v1', 'synthetic.daily-statistics',
        'synthetic-daily-statistics.v1', 'synthetic_daily_statistics_v1',
        'non_sensitive', true, '{}'::jsonb
    ),
    (
        'events-bot.daily-statistics.v1', 'events-bot.daily-statistics',
        'events-bot-daily-statistics.v1', 'events_bot_daily_statistics_v1',
        'non_sensitive', false, '{"canary_required": true}'::jsonb
    );

UPDATE hub.canonical_state
SET schema_revision = 10,
    updated_at = now()
WHERE singleton = true;
