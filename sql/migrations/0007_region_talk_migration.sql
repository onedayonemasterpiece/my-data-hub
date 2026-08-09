-- Lossless landing, mapping and cutover evidence for Region Talk YDB migration.
-- The landing layer is intentionally source-shaped: unknown row kinds are retained,
-- never discarded, until a versioned mapper assigns a terminal disposition.

CREATE TABLE migration.export_batch (
    export_batch_id         uuid PRIMARY KEY,
    source_system           text NOT NULL CHECK (source_system = 'ydb'),
    source_database         text NOT NULL,
    source_tables           jsonb NOT NULL CHECK (jsonb_typeof(source_tables) = 'array'),
    source_scope            text NOT NULL,
    schema_version          text NOT NULL,
    source_revision         text,
    source_code_revision    text,
    consistency_mode        text NOT NULL,
    watermark_start         timestamptz,
    watermark_end           timestamptz,
    expected_row_count      bigint NOT NULL CHECK (expected_row_count >= 0),
    manifest_sha256         text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    logical_sha256          text CHECK (logical_sha256 IS NULL OR logical_sha256 ~ '^[a-f0-9]{64}$'),
    status                  text NOT NULL DEFAULT 'created'
                            CHECK (status IN (
                                'created', 'validating', 'landing', 'landed',
                                'mapping', 'reconciled', 'accepted', 'rejected'
                            )),
    metadata                jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    completed_at            timestamptz,
    UNIQUE (source_system, source_database, source_scope, manifest_sha256)
);

CREATE TABLE migration.export_batch_kind (
    export_batch_id         uuid NOT NULL REFERENCES migration.export_batch(export_batch_id) ON DELETE CASCADE,
    row_kind                text NOT NULL,
    expected_row_count      bigint NOT NULL CHECK (expected_row_count >= 0),
    PRIMARY KEY (export_batch_id, row_kind)
);

CREATE TABLE migration.export_file (
    export_batch_id         uuid NOT NULL REFERENCES migration.export_batch(export_batch_id) ON DELETE CASCADE,
    relative_path           text NOT NULL,
    source_table            text NOT NULL,
    sha256                  text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
    row_count               bigint NOT NULL CHECK (row_count >= 0),
    byte_size               bigint NOT NULL CHECK (byte_size >= 0),
    PRIMARY KEY (export_batch_id, relative_path)
);

CREATE TABLE migration.raw_record (
    raw_record_id           uuid PRIMARY KEY,
    export_batch_id         uuid NOT NULL REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    source_table            text NOT NULL,
    source_pk               text NOT NULL,
    row_kind                text NOT NULL,
    source_updated_at       timestamptz,
    payload                 jsonb NOT NULL,
    payload_sha256          text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
    landed_at               timestamptz NOT NULL DEFAULT now(),
    UNIQUE (export_batch_id, source_table, source_pk),
    FOREIGN KEY (export_batch_id, row_kind)
        REFERENCES migration.export_batch_kind(export_batch_id, row_kind)
        ON DELETE RESTRICT
);
CREATE INDEX raw_record_batch_kind_idx
    ON migration.raw_record(export_batch_id, row_kind, source_table);
CREATE INDEX raw_record_source_identity_idx
    ON migration.raw_record(source_table, source_pk);
CREATE INDEX raw_record_payload_gin_idx
    ON migration.raw_record USING gin(payload jsonb_path_ops);

CREATE TABLE migration.row_disposition (
    raw_record_id           uuid PRIMARY KEY REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    mapping_version         text NOT NULL,
    disposition            text NOT NULL
                            CHECK (disposition IN (
                                'normalized', 'deduplicated', 'intentionally_excluded',
                                'retained_raw', 'quarantined'
                            )),
    target_refs             jsonb NOT NULL DEFAULT '[]'::jsonb,
    reason_code             text NOT NULL,
    reason_detail           text,
    transformer_sha256      text CHECK (transformer_sha256 IS NULL OR transformer_sha256 ~ '^[a-f0-9]{64}$'),
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX row_disposition_state_idx
    ON migration.row_disposition(disposition, reason_code);
CREATE TRIGGER row_disposition_updated_at
BEFORE UPDATE ON migration.row_disposition
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

-- Generic legacy identity registry. A polymorphic FK would be dishonest; target_table
-- and target_pk are instead verified by reconciliation queries for each mapper release.
CREATE TABLE migration.legacy_identity_map (
    source_system           text NOT NULL,
    source_table            text NOT NULL,
    source_pk               text NOT NULL,
    target_table            text,
    target_pk               jsonb,
    mapping_version         text NOT NULL,
    mapping_kind            text NOT NULL
                            CHECK (mapping_kind IN (
                                'created', 'matched', 'merged', 'excluded', 'unresolved'
                            )),
    evidence                jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_system, source_table, source_pk)
);
CREATE INDEX legacy_identity_target_idx
    ON migration.legacy_identity_map(target_table)
    WHERE target_table IS NOT NULL;

CREATE TABLE migration.reconciliation_run (
    reconciliation_run_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    export_batch_id         uuid NOT NULL REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    mapping_version         text NOT NULL,
    code_revision           text NOT NULL,
    schema_revision         integer NOT NULL CHECK (schema_revision >= 1),
    status                  text NOT NULL DEFAULT 'running'
                            CHECK (status IN (
                                'running', 'passed', 'failed', 'accepted_with_exceptions'
                            )),
    report                  jsonb NOT NULL DEFAULT '{}'::jsonb,
    report_sha256           text CHECK (report_sha256 IS NULL OR report_sha256 ~ '^[a-f0-9]{64}$'),
    started_at              timestamptz NOT NULL DEFAULT now(),
    finished_at             timestamptz
);

CREATE TABLE migration.reconciliation_finding (
    finding_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    reconciliation_run_id   uuid NOT NULL REFERENCES migration.reconciliation_run(reconciliation_run_id) ON DELETE CASCADE,
    severity                text NOT NULL CHECK (severity IN ('info', 'warning', 'blocking')),
    finding_kind            text NOT NULL,
    source_ref              text,
    target_ref              text,
    expected                jsonb,
    actual                  jsonb,
    explanation             text,
    status                  text NOT NULL DEFAULT 'open'
                            CHECK (status IN ('open', 'resolved', 'accepted')),
    created_at              timestamptz NOT NULL DEFAULT now(),
    resolved_at             timestamptz
);
CREATE INDEX reconciliation_blocking_idx
    ON migration.reconciliation_finding(reconciliation_run_id, status)
    WHERE severity = 'blocking';

CREATE TABLE migration.cutover_receipt (
    cutover_receipt_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workload_key            text NOT NULL,
    export_batch_id         uuid NOT NULL REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    final_delta_batch_id    uuid REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    reconciliation_run_id   uuid NOT NULL REFERENCES migration.reconciliation_run(reconciliation_run_id) ON DELETE RESTRICT,
    source_freeze_at        timestamptz NOT NULL,
    target_revision         bigint NOT NULL CHECK (target_revision >= 0),
    source_code_revision    text NOT NULL,
    target_code_revision    text NOT NULL,
    backup_ref              text NOT NULL,
    production_publish_enabled boolean NOT NULL DEFAULT false,
    rollback_until          timestamptz NOT NULL,
    owner_approval_ref      text,
    receipt                 jsonb NOT NULL,
    receipt_sha256          text NOT NULL CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
    created_at              timestamptz NOT NULL DEFAULT now()
);

CREATE VIEW migration.row_accounting AS
SELECT
    expected.export_batch_id,
    expected.row_kind,
    expected.expected_row_count,
    count(raw.raw_record_id) AS raw_count,
    count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'normalized') AS normalized_count,
    count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'deduplicated') AS deduplicated_count,
    count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'intentionally_excluded') AS intentionally_excluded_count,
    count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'retained_raw') AS retained_raw_count,
    count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'quarantined') AS quarantined_count,
    count(raw.raw_record_id) FILTER (WHERE disp.raw_record_id IS NULL) AS undispositioned_count,
    count(raw.raw_record_id) = expected.expected_row_count AS raw_count_matches_manifest,
    count(raw.raw_record_id) = expected.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE disp.raw_record_id IS NULL) = 0
        AS fully_accounted,
    count(raw.raw_record_id) = expected.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE disp.raw_record_id IS NULL) = 0
        AND count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'quarantined') = 0
        AS cutover_ready
FROM migration.export_batch_kind expected
LEFT JOIN migration.raw_record raw
  ON raw.export_batch_id = expected.export_batch_id
 AND raw.row_kind = expected.row_kind
LEFT JOIN migration.row_disposition disp ON disp.raw_record_id = raw.raw_record_id
GROUP BY expected.export_batch_id, expected.row_kind, expected.expected_row_count;

CREATE VIEW migration.batch_accounting AS
SELECT
    batch.export_batch_id,
    batch.expected_row_count,
    count(raw.raw_record_id) AS raw_count,
    count(disp.raw_record_id) AS dispositioned_count,
    count(raw.raw_record_id) FILTER (WHERE disp.raw_record_id IS NULL) AS undispositioned_count,
    count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'quarantined') AS quarantined_count,
    count(raw.raw_record_id) = batch.expected_row_count AS raw_count_matches_manifest,
    count(raw.raw_record_id) = batch.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE disp.raw_record_id IS NULL) = 0
        AS fully_accounted,
    count(raw.raw_record_id) = batch.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE disp.raw_record_id IS NULL) = 0
        AND count(raw.raw_record_id) FILTER (WHERE disp.disposition = 'quarantined') = 0
        AS cutover_ready
FROM migration.export_batch batch
LEFT JOIN migration.raw_record raw ON raw.export_batch_id = batch.export_batch_id
LEFT JOIN migration.row_disposition disp ON disp.raw_record_id = raw.raw_record_id
GROUP BY batch.export_batch_id, batch.expected_row_count;
