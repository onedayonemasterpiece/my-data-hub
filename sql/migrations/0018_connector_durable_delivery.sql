-- Close the connector plane around one delivery-mode vocabulary and a
-- producer-visible durable checkpoint lifecycle.  All rows remain in the
-- ACTIVE master PostgreSQL; the devstand receives only bounded status metadata.

ALTER TABLE integration.connector DISABLE TRIGGER USER;
ALTER TABLE integration.connector
    DROP CONSTRAINT connector_delivery_mode_check;
UPDATE integration.connector
SET delivery_mode = CASE delivery_mode
    WHEN 'artifact' THEN 'artifact_handoff'
    WHEN 'trusted_landing' THEN 'trusted_database_landing'
    ELSE delivery_mode
END;
ALTER TABLE integration.connector
    ADD CONSTRAINT connector_delivery_mode_check CHECK (
        delivery_mode IN ('push', 'pull', 'artifact_handoff', 'trusted_database_landing')
    );
ALTER TABLE integration.connector ENABLE TRIGGER USER;

ALTER TABLE integration.batch DISABLE TRIGGER USER;
ALTER TABLE integration.batch
    DROP CONSTRAINT batch_delivery_mode_check;
UPDATE integration.batch
SET delivery_mode = CASE delivery_mode
    WHEN 'inline' THEN 'push'
    WHEN 'artifact' THEN 'artifact_handoff'
    ELSE delivery_mode
END;
ALTER TABLE integration.batch
    ADD CONSTRAINT batch_delivery_mode_check CHECK (
        delivery_mode IN ('push', 'pull', 'artifact_handoff', 'trusted_database_landing')
    );
ALTER TABLE integration.batch ENABLE TRIGGER USER;

UPDATE integration.connector
SET policy = policy || jsonb_build_object(
    'allowed_delivery_modes', jsonb_build_array(delivery_mode)
)
WHERE NOT (policy ? 'allowed_delivery_modes');

CREATE TABLE integration.connector_durability (
    batch_id                    uuid PRIMARY KEY
                                REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    acceptance_receipt_id       uuid NOT NULL UNIQUE
                                REFERENCES integration.receipt(receipt_id) ON DELETE RESTRICT,
    state                       text NOT NULL CHECK (state IN (
                                    'ACCEPTED', 'CANONICAL_COMMITTED',
                                    'CHECKPOINT_REQUESTED', 'CHECKPOINTING',
                                    'DURABLE_COMPLETE', 'FAILED'
                                )),
    canonical_revision          bigint CHECK (canonical_revision IS NULL OR canonical_revision >= 1),
    checkpoint_request_id       text UNIQUE CHECK (
                                    checkpoint_request_id IS NULL
                                    OR checkpoint_request_id ~ '^[a-f0-9]{64}$'
                                ),
    checkpoint_request_sha256   text CHECK (
                                    checkpoint_request_sha256 IS NULL
                                    OR checkpoint_request_sha256 ~ '^[a-f0-9]{64}$'
                                ),
    checkpoint_operation_id     text UNIQUE,
    checkpoint_status_receipt   jsonb,
    checkpoint_receipt_sha256   text CHECK (
                                    checkpoint_receipt_sha256 IS NULL
                                    OR checkpoint_receipt_sha256 ~ '^[a-f0-9]{64}$'
                                ),
    checkpoint_id               text,
    created_at                  timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at                  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state = 'ACCEPTED') = (canonical_revision IS NULL)),
    CHECK (
        (state IN ('CHECKPOINT_REQUESTED', 'CHECKPOINTING', 'DURABLE_COMPLETE', 'FAILED'))
        = (checkpoint_request_id IS NOT NULL
           AND checkpoint_request_sha256 IS NOT NULL
           AND checkpoint_operation_id IS NOT NULL)
    ),
    CHECK (
        (state = 'DURABLE_COMPLETE')
        = (checkpoint_receipt_sha256 IS NOT NULL AND checkpoint_id IS NOT NULL)
    )
);
CREATE INDEX connector_durability_state_idx
    ON integration.connector_durability (state, updated_at);
CREATE TRIGGER connector_durability_set_updated_at
BEFORE UPDATE ON integration.connector_durability
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

INSERT INTO integration.connector_durability (
    batch_id, acceptance_receipt_id, state, canonical_revision
)
SELECT b.batch_id,
       accepted.receipt_id,
       CASE WHEN statistic.canonical_revision IS NULL
            THEN 'ACCEPTED' ELSE 'CANONICAL_COMMITTED' END,
       statistic.canonical_revision
FROM integration.batch b
JOIN LATERAL (
    SELECT receipt_id
    FROM integration.receipt
    WHERE batch_id = b.batch_id AND receipt_type IN ('accepted', 'duplicate')
    ORDER BY created_at, receipt_id
    LIMIT 1
) accepted ON true
LEFT JOIN integration.daily_statistic statistic ON statistic.batch_id = b.batch_id;

-- This is real registry metadata, not a live-import claim.  Both connector and
-- product remain paused/disabled; the callable pull interface refuses before
-- invoking an adapter or mutating its spool.
INSERT INTO integration.connector (
    connector_id, owner_principal, service_principal, delivery_mode, status,
    expected_cadence, policy
) VALUES (
    'region-talk-ydb-bloggers-v1',
    'my-data-hub',
    'service:region-talk-ydb-bloggers-v1',
    'pull',
    'paused',
    NULL,
    '{
      "allowed_delivery_modes": ["pull"],
      "adapter_contract": "orchestrator_pull.v1",
      "no_live_import": true,
      "paused_reason": "region_talk_ordered_lifecycle_gates"
    }'::jsonb
);

INSERT INTO integration.data_product (
    data_product, connector_id, schema_version, normalizer_contract,
    sensitivity, enabled, configuration
) VALUES (
    'region-talk.ydb-bloggers.v1',
    'region-talk-ydb-bloggers-v1',
    'region-talk-ydb-export-manifest.v1',
    'region_talk_ydb_bloggers_pull_v1',
    'internal',
    false,
    '{"no_live_import": true, "migration_only": true}'::jsonb
);

UPDATE hub.canonical_state
SET schema_revision = 18,
    updated_at = clock_timestamp()
WHERE singleton = true;
