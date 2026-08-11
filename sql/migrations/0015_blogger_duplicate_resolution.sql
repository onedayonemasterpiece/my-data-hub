-- Explicit, append-only resolution evidence for quarantined blogger duplicates.
-- Raw rows and their first terminal classification are never overwritten.  A
-- successful exact replay adds an effective terminal disposition in a separate
-- ledger and atomically creates/links the explicitly selected canonical actor.

CREATE TABLE migration.blogger_replay (
    blogger_replay_id          uuid PRIMARY KEY,
    export_batch_id            uuid NOT NULL UNIQUE
                               REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    resolution_set_sha256      text NOT NULL CHECK (resolution_set_sha256 ~ '^[a-f0-9]{64}$'),
    canonical_outcome_sha256   text NOT NULL CHECK (canonical_outcome_sha256 ~ '^[a-f0-9]{64}$'),
    canonical_revision         bigint NOT NULL CHECK (canonical_revision >= 1),
    actor_count                bigint NOT NULL CHECK (actor_count >= 0),
    account_count              bigint NOT NULL CHECK (account_count >= 0),
    replayed_row_count         bigint NOT NULL CHECK (replayed_row_count >= 0),
    created_at                 timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE migration.blogger_duplicate_resolution (
    duplicate_resolution_id   uuid PRIMARY KEY,
    blogger_replay_id          uuid NOT NULL
                               REFERENCES migration.blogger_replay(blogger_replay_id) ON DELETE RESTRICT,
    duplicate_group_id         uuid NOT NULL UNIQUE
                               REFERENCES migration.duplicate_group(duplicate_group_id) ON DELETE RESTRICT,
    canonical_source_pk        text NOT NULL,
    canonical_actor_id         uuid NOT NULL REFERENCES hub.actor(actor_id) ON DELETE RESTRICT,
    member_record_id_set_sha256 text NOT NULL
                                CHECK (member_record_id_set_sha256 ~ '^[a-f0-9]{64}$'),
    resolution_sha256          text NOT NULL UNIQUE CHECK (resolution_sha256 ~ '^[a-f0-9]{64}$'),
    decision_kind              text NOT NULL CHECK (decision_kind = 'same_actor'),
    reason                     text NOT NULL CHECK (octet_length(reason) BETWEEN 1 AND 4096),
    decided_by                 text NOT NULL CHECK (octet_length(decided_by) BETWEEN 1 AND 512),
    created_at                 timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE migration.blogger_replay_disposition (
    raw_record_id              uuid PRIMARY KEY
                               REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    blogger_replay_id          uuid NOT NULL
                               REFERENCES migration.blogger_replay(blogger_replay_id) ON DELETE RESTRICT,
    disposition                text NOT NULL CHECK (disposition IN (
                                   'normalized', 'deduplicated', 'intentionally_excluded', 'retained_raw'
                               )),
    target_refs                jsonb NOT NULL CHECK (
                                   jsonb_typeof(target_refs) = 'array' AND jsonb_array_length(target_refs) > 0
                               ),
    reason_code                text NOT NULL,
    created_at                 timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TRIGGER blogger_replay_append_only
BEFORE UPDATE OR DELETE ON migration.blogger_replay
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER blogger_duplicate_resolution_append_only
BEFORE UPDATE OR DELETE ON migration.blogger_duplicate_resolution
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER blogger_replay_disposition_append_only
BEFORE UPDATE OR DELETE ON migration.blogger_replay_disposition
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard
AFTER INSERT OR UPDATE OR DELETE ON migration.blogger_replay
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION master_control.enforce_write_epoch();
CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard
AFTER INSERT OR UPDATE OR DELETE ON migration.blogger_duplicate_resolution
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION master_control.enforce_write_epoch();
CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard
AFTER INSERT OR UPDATE OR DELETE ON migration.blogger_replay_disposition
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION master_control.enforce_write_epoch();

CREATE OR REPLACE VIEW migration.row_accounting AS
SELECT
    expected.export_batch_id,
    expected.row_kind,
    expected.expected_row_count,
    count(raw.raw_record_id) AS raw_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'normalized') AS normalized_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'deduplicated') AS deduplicated_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'intentionally_excluded') AS intentionally_excluded_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'retained_raw') AS retained_raw_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'quarantined') AS quarantined_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) IS NULL) AS undispositioned_count,
    count(raw.raw_record_id) = expected.expected_row_count AS raw_count_matches_manifest,
    count(raw.raw_record_id) = expected.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) IS NULL) = 0
        AS fully_accounted,
    count(raw.raw_record_id) = expected.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) IS NULL) = 0
        AND count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'quarantined') = 0
        AS cutover_ready
FROM migration.export_batch_kind expected
LEFT JOIN migration.raw_record raw
  ON raw.export_batch_id = expected.export_batch_id
 AND raw.row_kind = expected.row_kind
LEFT JOIN migration.row_disposition disp ON disp.raw_record_id = raw.raw_record_id
LEFT JOIN migration.blogger_replay_disposition replay ON replay.raw_record_id = raw.raw_record_id
GROUP BY expected.export_batch_id, expected.row_kind, expected.expected_row_count;

CREATE OR REPLACE VIEW migration.batch_accounting AS
SELECT
    batch.export_batch_id,
    batch.expected_row_count,
    count(raw.raw_record_id) AS raw_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) IS NOT NULL) AS dispositioned_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) IS NULL) AS undispositioned_count,
    count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'quarantined') AS quarantined_count,
    count(raw.raw_record_id) = batch.expected_row_count AS raw_count_matches_manifest,
    count(raw.raw_record_id) = batch.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) IS NULL) = 0
        AS fully_accounted,
    count(raw.raw_record_id) = batch.expected_row_count
        AND count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) IS NULL) = 0
        AND count(raw.raw_record_id) FILTER (WHERE coalesce(replay.disposition, disp.disposition) = 'quarantined') = 0
        AS cutover_ready
FROM migration.export_batch batch
LEFT JOIN migration.raw_record raw ON raw.export_batch_id = batch.export_batch_id
LEFT JOIN migration.row_disposition disp ON disp.raw_record_id = raw.raw_record_id
LEFT JOIN migration.blogger_replay_disposition replay ON replay.raw_record_id = raw.raw_record_id
GROUP BY batch.export_batch_id, batch.expected_row_count;

CREATE VIEW migration.blogger_duplicate_accounting AS
SELECT
    batch.export_batch_id,
    count(groups.duplicate_group_id) AS duplicate_group_count,
    count(resolution.duplicate_group_id) AS resolved_duplicate_group_count,
    count(groups.duplicate_group_id) FILTER (WHERE resolution.duplicate_group_id IS NULL)
        AS duplicate_groups_pending
FROM migration.export_batch batch
LEFT JOIN migration.duplicate_group groups ON groups.export_batch_id = batch.export_batch_id
LEFT JOIN migration.blogger_duplicate_resolution resolution
  ON resolution.duplicate_group_id = groups.duplicate_group_id
GROUP BY batch.export_batch_id;

GRANT SELECT, INSERT ON migration.blogger_replay,
    migration.blogger_duplicate_resolution, migration.blogger_replay_disposition
    TO mdh_migration_operator;
GRANT SELECT ON migration.blogger_duplicate_accounting TO mdh_migration_operator, mdh_mcp_reader;
REVOKE UPDATE, DELETE ON migration.blogger_replay,
    migration.blogger_duplicate_resolution, migration.blogger_replay_disposition
    FROM mdh_migration_operator;

UPDATE hub.canonical_state
SET schema_revision = 15,
    updated_at = clock_timestamp()
WHERE singleton = true;
