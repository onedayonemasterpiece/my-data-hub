-- Typed MCP blogger discovery intake, immutable preview plans and one bounded
-- canonical-committer transaction. No generic SQL or migration provenance is reused.

INSERT INTO integration.connector (
    connector_id, owner_principal, service_principal, delivery_mode, status, policy
) VALUES
    (
        'mcp-blogger-discovery-inline-v1', 'datahub-owner',
        'service:mcp-blogger-discovery-inline-v1', 'push', 'active',
        '{"allowed_delivery_modes":["push"],"semantic_contract":"submit-discovery-batch.v1","maximum_records":500}'::jsonb
    ),
    (
        'mcp-blogger-discovery-artifact-v1', 'datahub-owner',
        'service:mcp-blogger-discovery-artifact-v1', 'artifact_handoff', 'active',
        '{"allowed_delivery_modes":["artifact_handoff"],"semantic_contract":"submit-discovery-batch.v1","exact_private_provider_claim_required":true,"maximum_records":500}'::jsonb
    );

INSERT INTO integration.data_product (
    data_product, connector_id, schema_version, normalizer_contract,
    sensitivity, enabled, configuration
) VALUES
    (
        'mcp.bloggers.discovery.inline.v1', 'mcp-blogger-discovery-inline-v1',
        'blogger-discovery-batch.v1', 'blogger_discovery_v1', 'internal', true,
        '{"payload":"closed_typed_rows","maximum_records":500}'::jsonb
    ),
    (
        'mcp.bloggers.discovery.artifact.v1', 'mcp-blogger-discovery-artifact-v1',
        'blogger-discovery-batch.v1', 'blogger_discovery_v1', 'internal', true,
        '{"payload":"exact_private_artifact","maximum_records":500}'::jsonb
    );

CREATE TABLE integration.blogger_discovery_artifact_landing (
    batch_id                    uuid PRIMARY KEY REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    verified_artifact_sha256    text NOT NULL CHECK (verified_artifact_sha256 ~ '^[a-f0-9]{64}$'),
    materialized_records_sha256 text NOT NULL CHECK (materialized_records_sha256 ~ '^[a-f0-9]{64}$'),
    materialized_records        jsonb NOT NULL CHECK (jsonb_typeof(materialized_records) = 'array'),
    record_count                integer NOT NULL CHECK (record_count BETWEEN 1 AND 500),
    materialized_by             text NOT NULL,
    materialized_at             timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER blogger_discovery_artifact_landing_append_only
BEFORE UPDATE OR DELETE ON integration.blogger_discovery_artifact_landing
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE integration.blogger_discovery_plan (
    batch_id              uuid PRIMARY KEY REFERENCES integration.batch(batch_id) ON DELETE RESTRICT,
    operation_id          text NOT NULL UNIQUE CHECK (operation_id ~ '^[a-f0-9]{64}$'),
    request_sha256        text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    plan_sha256           text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
    project_id            uuid NOT NULL REFERENCES hub.project(project_id) ON DELETE RESTRICT,
    expected_revision     bigint NOT NULL CHECK (expected_revision >= 0),
    master_instance_id    uuid NOT NULL,
    master_epoch          bigint NOT NULL CHECK (master_epoch >= 1),
    principal_id          text NOT NULL CHECK (length(principal_id) BETWEEN 1 AND 300),
    client_id             text NOT NULL CHECK (length(client_id) BETWEEN 1 AND 300),
    create_actor_count    integer NOT NULL CHECK (create_actor_count >= 0),
    link_existing_count   integer NOT NULL CHECK (link_existing_count >= 0),
    quarantine_count      integer NOT NULL CHECK (quarantine_count >= 0),
    account_count         integer NOT NULL CHECK (account_count >= 0),
    state                 text NOT NULL DEFAULT 'PREVIEWED' CHECK (state IN ('PREVIEWED', 'APPLIED')),
    created_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (principal_id, client_id, operation_id)
);

CREATE TABLE integration.blogger_discovery_plan_row (
    batch_id          uuid NOT NULL REFERENCES integration.blogger_discovery_plan(batch_id) ON DELETE RESTRICT,
    row_ordinal       integer NOT NULL CHECK (row_ordinal BETWEEN 0 AND 499),
    source_record_id  text NOT NULL CHECK (length(source_record_id) BETWEEN 1 AND 500),
    record_sha256     text NOT NULL CHECK (record_sha256 ~ '^[a-f0-9]{64}$'),
    disposition       text NOT NULL CHECK (disposition IN ('create_actor', 'link_existing', 'quarantined')),
    reason_code       text,
    resolved_actor_id uuid REFERENCES hub.actor(actor_id) ON DELETE RESTRICT,
    normalized_record jsonb NOT NULL CHECK (jsonb_typeof(normalized_record) = 'object'),
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id, row_ordinal),
    UNIQUE (batch_id, source_record_id),
    CHECK ((disposition = 'link_existing') = (resolved_actor_id IS NOT NULL)),
    CHECK ((disposition = 'quarantined') = (reason_code IS NOT NULL))
);

CREATE TABLE integration.blogger_discovery_apply_receipt (
    operation_id       text PRIMARY KEY CHECK (operation_id ~ '^[a-f0-9]{64}$'),
    batch_id           uuid NOT NULL UNIQUE REFERENCES integration.blogger_discovery_plan(batch_id) ON DELETE RESTRICT,
    request_sha256     text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    plan_sha256        text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
    master_instance_id uuid NOT NULL,
    master_epoch       bigint NOT NULL CHECK (master_epoch >= 1),
    revision_before    bigint NOT NULL CHECK (revision_before >= 0),
    revision_after     bigint NOT NULL UNIQUE CHECK (revision_after >= 1),
    affected_rows      integer NOT NULL CHECK (affected_rows >= 1),
    principal_id       text NOT NULL CHECK (length(principal_id) BETWEEN 1 AND 300),
    client_id          text NOT NULL CHECK (length(client_id) BETWEEN 1 AND 300),
    outbox_id          uuid NOT NULL UNIQUE REFERENCES sync.external_outbox(outbox_id) ON DELETE RESTRICT,
    committed_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (revision_after = revision_before + 1)
);
CREATE TRIGGER blogger_discovery_apply_receipt_append_only
BEFORE UPDATE OR DELETE ON integration.blogger_discovery_apply_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE integration.blogger_discovery_quarantine (
    batch_id         uuid NOT NULL REFERENCES integration.blogger_discovery_plan(batch_id) ON DELETE RESTRICT,
    row_ordinal      integer NOT NULL,
    source_record_id text NOT NULL,
    record_sha256    text NOT NULL CHECK (record_sha256 ~ '^[a-f0-9]{64}$'),
    reason_code      text NOT NULL,
    evidence         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id, row_ordinal),
    FOREIGN KEY (batch_id, row_ordinal)
        REFERENCES integration.blogger_discovery_plan_row(batch_id, row_ordinal) ON DELETE RESTRICT
);
CREATE TRIGGER blogger_discovery_quarantine_append_only
BEFORE UPDATE OR DELETE ON integration.blogger_discovery_quarantine
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

-- A sanitized cross-source read model. Historical migration-only fields remain
-- nullable instead of fabricating migration.export_batch identities for MCP discoveries.
CREATE VIEW hub.bloggers_v1 AS
SELECT
    actor.actor_id AS blogger_id,
    actor.display_name,
    actor.actor_type AS actor_kind,
    actor.summary AS public_description,
    profile.geography_signal,
    profile.geography_provenance,
    profile.legacy_record_id AS source_legacy_id,
    profile.source_updated_at,
    profile.confirmation_status,
    coalesce(profile.requires_review, false) AS requires_review,
    membership.project_id,
    coalesce(
        jsonb_agg(
            jsonb_build_object(
                'platform', account.platform,
                'handle', account.handle,
                'url', account.url
            ) ORDER BY account.platform, account.normalized_url, account.account_id
        ) FILTER (WHERE account.account_id IS NOT NULL),
        '[]'::jsonb
    ) AS public_accounts
FROM hub.project_actor membership
JOIN hub.actor actor ON actor.actor_id = membership.actor_id
LEFT JOIN region_talk.blogger_profile profile ON profile.actor_id = actor.actor_id
LEFT JOIN hub.external_account account
  ON account.actor_id = actor.actor_id AND account.status = 'active'
WHERE membership.membership_kind = 'blogger' AND membership.status = 'included'
GROUP BY actor.actor_id, actor.display_name, actor.actor_type, actor.summary,
    profile.geography_signal, profile.geography_provenance, profile.legacy_record_id,
    profile.source_updated_at, profile.confirmation_status, profile.requires_review,
    membership.project_id;

CREATE FUNCTION integration.materialize_blogger_discovery_artifact(
    requested_batch_id uuid,
    requested_verified_artifact_sha256 text,
    requested_records jsonb,
    requested_materialized_by text
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    accepted integration.batch%ROWTYPE;
    calculated_sha256 text;
    count_records integer;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_connector_intake', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'artifact materialization requires exact connector intake login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT accepted FROM integration.batch WHERE batch_id = requested_batch_id;
    IF accepted.data_product <> 'mcp.bloggers.discovery.artifact.v1'
       OR accepted.delivery_mode <> 'artifact_handoff'
       OR accepted.status <> 'accepted'
       OR requested_verified_artifact_sha256 IS DISTINCT FROM accepted.payload_sha256
       OR requested_materialized_by IS NULL
       OR length(requested_materialized_by) NOT BETWEEN 1 AND 300
       OR jsonb_typeof(requested_records) <> 'array' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'artifact materialization differs from accepted exact claim';
    END IF;
    count_records := jsonb_array_length(requested_records);
    IF count_records NOT BETWEEN 1 AND 500 OR count_records <> accepted.record_count THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'artifact materialization record count differs from manifest';
    END IF;
    calculated_sha256 := encode(sha256(convert_to(requested_records::text, 'UTF8')), 'hex');
    INSERT INTO integration.blogger_discovery_artifact_landing (
        batch_id, verified_artifact_sha256, materialized_records_sha256,
        materialized_records, record_count, materialized_by
    ) VALUES (
        requested_batch_id, requested_verified_artifact_sha256, calculated_sha256,
        requested_records, count_records, requested_materialized_by
    ) ON CONFLICT (batch_id) DO NOTHING;
    IF NOT FOUND THEN
        PERFORM 1 FROM integration.blogger_discovery_artifact_landing
        WHERE batch_id = requested_batch_id
          AND verified_artifact_sha256 = requested_verified_artifact_sha256
          AND materialized_records_sha256 = calculated_sha256
          AND record_count = count_records;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'artifact materialization replay conflicts with accepted landing';
        END IF;
    END IF;
    RETURN calculated_sha256;
END
$$;

CREATE FUNCTION integration.preview_blogger_discovery(
    requested_batch_id uuid,
    requested_operation_id text,
    requested_request_sha256 text,
    requested_expected_revision bigint,
    requested_principal_id text,
    requested_client_id text
)
RETURNS TABLE (
    batch_id uuid,
    operation_id text,
    request_sha256 text,
    plan_sha256 text,
    expected_revision bigint,
    create_actor_count integer,
    link_existing_count integer,
    quarantine_count integer,
    account_count integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    accepted integration.batch%ROWTYPE;
    epoch_state master_control.epoch_state%ROWTYPE;
    canonical_revision bigint;
    project_slug text;
    project_uuid uuid;
    records jsonb;
    record jsonb;
    account jsonb;
    ordinal integer := 0;
    matching_actors integer;
    resolved_actor uuid;
    row_disposition text;
    row_reason text;
    row_hash text;
    calculated_plan_sha256 text;
    creates integer;
    links integer;
    quarantines integer;
    accounts integer;
    existing integration.blogger_discovery_plan%ROWTYPE;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_canonical_committer', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'blogger preview requires exact canonical committer login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT epoch_state FROM master_control.epoch_state WHERE singleton;
    SELECT canonical_state.canonical_revision INTO STRICT canonical_revision
    FROM hub.canonical_state canonical_state WHERE singleton;
    IF requested_operation_id !~ '^[a-f0-9]{64}$'
       OR requested_request_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_expected_revision < 0
       OR requested_expected_revision <> canonical_revision
       OR length(requested_principal_id) NOT BETWEEN 1 AND 300
       OR length(requested_client_id) NOT BETWEEN 1 AND 300 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'blogger preview identity is invalid or stale';
    END IF;

    SELECT * INTO existing FROM integration.blogger_discovery_plan
    WHERE integration.blogger_discovery_plan.operation_id = requested_operation_id
       OR integration.blogger_discovery_plan.batch_id = requested_batch_id;
    IF FOUND THEN
        IF existing.batch_id <> requested_batch_id
           OR existing.operation_id <> requested_operation_id
           OR existing.request_sha256 <> requested_request_sha256
           OR existing.expected_revision <> requested_expected_revision
           OR existing.master_instance_id <> epoch_state.master_instance_id
           OR existing.master_epoch <> epoch_state.current_epoch
           OR existing.principal_id <> requested_principal_id
           OR existing.client_id <> requested_client_id THEN
            RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'blogger preview replay conflicts with immutable plan';
        END IF;
        RETURN QUERY SELECT existing.batch_id, existing.operation_id, existing.request_sha256,
            existing.plan_sha256, existing.expected_revision, existing.create_actor_count,
            existing.link_existing_count, existing.quarantine_count, existing.account_count;
        RETURN;
    END IF;

    SELECT * INTO STRICT accepted FROM integration.batch WHERE integration.batch.batch_id = requested_batch_id;
    IF accepted.data_product NOT IN (
           'mcp.bloggers.discovery.inline.v1', 'mcp.bloggers.discovery.artifact.v1'
       ) OR accepted.schema_version <> 'blogger-discovery-batch.v1'
       OR accepted.status <> 'accepted' OR accepted.record_count NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'accepted batch is not a previewable blogger discovery product';
    END IF;
    SELECT coalesce(payload.inline_payload, landing.materialized_records),
           convert_from(payload.exact_envelope, 'UTF8')::jsonb #>> '{trace,project_slug}'
    INTO records, project_slug
    FROM integration.batch_payload payload
    LEFT JOIN integration.blogger_discovery_artifact_landing landing USING (batch_id)
    WHERE payload.batch_id = requested_batch_id;
    IF jsonb_typeof(records) <> 'array' OR jsonb_array_length(records) <> accepted.record_count THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'artifact batch is not materialized or record count differs';
    END IF;
    SELECT project.project_id INTO STRICT project_uuid FROM hub.project project WHERE project.slug = project_slug;

    CREATE TEMP TABLE IF NOT EXISTS pg_temp.mdh_blogger_plan_rows (
        row_ordinal integer PRIMARY KEY,
        source_record_id text UNIQUE,
        record_sha256 text NOT NULL,
        disposition text NOT NULL,
        reason_code text,
        resolved_actor_id uuid,
        normalized_record jsonb NOT NULL
    ) ON COMMIT DROP;
    TRUNCATE pg_temp.mdh_blogger_plan_rows;

    FOR record IN SELECT value FROM jsonb_array_elements(records) AS item(value) LOOP
        row_reason := NULL;
        resolved_actor := NULL;
        IF jsonb_typeof(record) <> 'object'
           OR NOT (record ?& ARRAY['source_record_id','actor_kind','display_name','accounts','source_uri','observed_at'])
           OR EXISTS (SELECT 1 FROM jsonb_object_keys(record) key
                      WHERE key NOT IN ('source_record_id','actor_kind','display_name','canonical_name','summary','accounts','source_uri','observed_at','evidence'))
           OR record->>'actor_kind' NOT IN ('person','organisation','outlet','collective','unknown')
           OR length(record->>'source_record_id') NOT BETWEEN 1 AND 500
           OR length(record->>'display_name') NOT BETWEEN 1 AND 1000
           OR jsonb_typeof(record->'accounts') <> 'array'
           OR jsonb_array_length(record->'accounts') NOT BETWEEN 1 AND 25
           OR jsonb_typeof(coalesce(record->'evidence','{}'::jsonb)) <> 'object'
           OR NOT pg_input_is_valid(record->>'observed_at', 'timestamp with time zone') THEN
            row_disposition := 'quarantined';
            row_reason := 'invalid_closed_record_contract';
        ELSE
            FOR account IN SELECT value FROM jsonb_array_elements(record->'accounts') AS item(value) LOOP
                IF jsonb_typeof(account) <> 'object'
                   OR NOT (account ? 'platform')
                   OR EXISTS (SELECT 1 FROM jsonb_object_keys(account) key
                              WHERE key NOT IN ('platform','external_id','handle','url','normalized_url'))
                   OR length(account->>'platform') NOT BETWEEN 1 AND 100
                   OR num_nonnulls(account->>'external_id', account->>'handle', account->>'normalized_url') = 0 THEN
                    row_reason := 'invalid_closed_account_contract';
                    EXIT;
                END IF;
            END LOOP;
            IF row_reason IS NOT NULL THEN
                row_disposition := 'quarantined';
            ELSIF EXISTS (
                SELECT 1
                FROM pg_temp.mdh_blogger_plan_rows prior,
                     jsonb_array_elements(prior.normalized_record->'accounts') prior_account,
                     jsonb_array_elements(record->'accounts') incoming_account
                WHERE lower(prior_account->>'platform') = lower(incoming_account->>'platform')
                  AND (
                      (prior_account->>'external_id' IS NOT NULL AND prior_account->>'external_id' = incoming_account->>'external_id')
                      OR (prior_account->>'normalized_url' IS NOT NULL AND prior_account->>'normalized_url' = incoming_account->>'normalized_url')
                      OR (prior_account->>'handle' IS NOT NULL AND lower(prior_account->>'handle') = lower(incoming_account->>'handle'))
                  )
            ) THEN
                row_disposition := 'quarantined';
                row_reason := 'duplicate_account_identity_in_batch';
            ELSE
                SELECT count(DISTINCT external_account.actor_id),
                       min(external_account.actor_id::text)::uuid
                INTO matching_actors, resolved_actor
                FROM hub.external_account external_account
                JOIN jsonb_array_elements(record->'accounts') incoming_account ON
                    lower(external_account.platform) = lower(incoming_account->>'platform')
                    AND (
                        (incoming_account->>'external_id' IS NOT NULL AND external_account.external_id = incoming_account->>'external_id')
                        OR (incoming_account->>'normalized_url' IS NOT NULL AND external_account.normalized_url = incoming_account->>'normalized_url')
                        OR (incoming_account->>'handle' IS NOT NULL AND lower(external_account.handle) = lower(incoming_account->>'handle'))
                    );
                IF matching_actors > 1 THEN
                    row_disposition := 'quarantined';
                    row_reason := 'account_identities_resolve_to_multiple_actors';
                    resolved_actor := NULL;
                ELSIF matching_actors = 1 THEN
                    row_disposition := 'link_existing';
                ELSE
                    row_disposition := 'create_actor';
                    resolved_actor := NULL;
                END IF;
            END IF;
        END IF;
        row_hash := encode(sha256(convert_to(record::text, 'UTF8')), 'hex');
        BEGIN
            INSERT INTO pg_temp.mdh_blogger_plan_rows VALUES (
                ordinal, coalesce(nullif(record->>'source_record_id',''), 'invalid:' || ordinal::text),
                row_hash, row_disposition,
                row_reason, resolved_actor, record
            );
        EXCEPTION WHEN unique_violation THEN
            RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'source_record_id is duplicated within discovery batch';
        END;
        ordinal := ordinal + 1;
    END LOOP;

    SELECT encode(sha256(convert_to(string_agg(
               row_ordinal::text || ':' || record_sha256 || ':' || disposition || ':' ||
               coalesce(resolved_actor_id::text, '') || ':' || coalesce(reason_code, ''),
               E'\n' ORDER BY row_ordinal), 'UTF8')), 'hex'),
           count(*) FILTER (WHERE disposition = 'create_actor'),
           count(*) FILTER (WHERE disposition = 'link_existing'),
           count(*) FILTER (WHERE disposition = 'quarantined'),
           coalesce(sum(jsonb_array_length(normalized_record->'accounts')) FILTER (WHERE disposition <> 'quarantined'), 0)
    INTO calculated_plan_sha256, creates, links, quarantines, accounts
    FROM pg_temp.mdh_blogger_plan_rows;

    INSERT INTO integration.blogger_discovery_plan (
        batch_id, operation_id, request_sha256, plan_sha256, project_id,
        expected_revision, master_instance_id, master_epoch, principal_id, client_id,
        create_actor_count, link_existing_count, quarantine_count, account_count
    ) VALUES (
        requested_batch_id, requested_operation_id, requested_request_sha256,
        calculated_plan_sha256, project_uuid, requested_expected_revision,
        epoch_state.master_instance_id, epoch_state.current_epoch,
        requested_principal_id, requested_client_id, creates, links, quarantines, accounts
    );
    INSERT INTO integration.blogger_discovery_plan_row (
        batch_id, row_ordinal, source_record_id, record_sha256, disposition,
        reason_code, resolved_actor_id, normalized_record
    ) SELECT requested_batch_id, row_ordinal, source_record_id, record_sha256,
        disposition, reason_code, resolved_actor_id, normalized_record
      FROM pg_temp.mdh_blogger_plan_rows ORDER BY row_ordinal;
    INSERT INTO integration.blogger_discovery_quarantine (
        batch_id, row_ordinal, source_record_id, record_sha256, reason_code, evidence
    ) SELECT requested_batch_id, row_ordinal, source_record_id, record_sha256,
        reason_code, jsonb_build_object('disposition','quarantined')
      FROM pg_temp.mdh_blogger_plan_rows WHERE disposition = 'quarantined';
    INSERT INTO integration.batch_event (batch_id, event_type, actor_principal, correlation_id, details)
    VALUES (
        requested_batch_id, 'blogger_discovery_previewed', requested_principal_id,
        accepted.correlation_id,
        jsonb_build_object('operation_id',requested_operation_id,'request_sha256',requested_request_sha256,
                           'plan_sha256',calculated_plan_sha256,'create_actor_count',creates,
                           'link_existing_count',links,'quarantine_count',quarantines,'account_count',accounts)
    );
    UPDATE integration.batch SET status = 'staged' WHERE integration.batch.batch_id = requested_batch_id;
    RETURN QUERY SELECT requested_batch_id, requested_operation_id, requested_request_sha256,
        calculated_plan_sha256, requested_expected_revision, creates, links, quarantines, accounts;
END
$$;

CREATE FUNCTION integration.apply_blogger_discovery(
    requested_batch_id uuid,
    requested_operation_id text,
    requested_request_sha256 text,
    requested_plan_sha256 text,
    requested_expected_revision bigint,
    requested_principal_id text,
    requested_client_id text
)
RETURNS TABLE (
    operation_id text,
    batch_id uuid,
    plan_sha256 text,
    affected_rows integer,
    revision_after bigint,
    duplicate boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    epoch_state master_control.epoch_state%ROWTYPE;
    plan integration.blogger_discovery_plan%ROWTYPE;
    prior integration.blogger_discovery_apply_receipt%ROWTYPE;
    planned integration.blogger_discovery_plan_row%ROWTYPE;
    account jsonb;
    actor_uuid uuid;
    provenance_uuid uuid;
    conflicting_actor uuid;
    changed integer := 0;
    next_revision bigint;
    created_outbox_id uuid := gen_random_uuid();
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_canonical_committer', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'blogger apply requires exact canonical committer login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT epoch_state FROM master_control.epoch_state WHERE singleton;
    IF requested_operation_id !~ '^[a-f0-9]{64}$'
       OR requested_request_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_plan_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_expected_revision < 0
       OR length(requested_principal_id) NOT BETWEEN 1 AND 300
       OR length(requested_client_id) NOT BETWEEN 1 AND 300 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'blogger apply identity is invalid';
    END IF;
    SELECT * INTO prior FROM integration.blogger_discovery_apply_receipt
    WHERE integration.blogger_discovery_apply_receipt.operation_id = requested_operation_id
       OR integration.blogger_discovery_apply_receipt.batch_id = requested_batch_id;
    IF FOUND THEN
        IF prior.operation_id <> requested_operation_id OR prior.batch_id <> requested_batch_id
           OR prior.request_sha256 <> requested_request_sha256 OR prior.plan_sha256 <> requested_plan_sha256
           OR prior.master_instance_id <> epoch_state.master_instance_id
           OR prior.master_epoch <> epoch_state.current_epoch
           OR prior.revision_before <> requested_expected_revision
           OR prior.principal_id <> requested_principal_id OR prior.client_id <> requested_client_id THEN
            RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'blogger apply replay conflicts with immutable receipt';
        END IF;
        RETURN QUERY SELECT prior.operation_id, prior.batch_id, prior.plan_sha256,
            prior.affected_rows, prior.revision_after, true;
        RETURN;
    END IF;
    SELECT * INTO STRICT plan FROM integration.blogger_discovery_plan
    WHERE integration.blogger_discovery_plan.batch_id = requested_batch_id FOR UPDATE;
    IF plan.operation_id <> requested_operation_id OR plan.request_sha256 <> requested_request_sha256
       OR plan.plan_sha256 <> requested_plan_sha256 OR plan.expected_revision <> requested_expected_revision
       OR plan.master_instance_id <> epoch_state.master_instance_id OR plan.master_epoch <> epoch_state.current_epoch
       OR plan.principal_id <> requested_principal_id OR plan.client_id <> requested_client_id
       OR plan.state <> 'PREVIEWED' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'blogger apply differs from immutable preview plan';
    END IF;
    IF plan.create_actor_count + plan.link_existing_count < 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'blogger apply has no committable canonical rows';
    END IF;

    FOR planned IN SELECT * FROM integration.blogger_discovery_plan_row
                   WHERE integration.blogger_discovery_plan_row.batch_id = requested_batch_id
                     AND disposition <> 'quarantined' ORDER BY row_ordinal LOOP
        IF planned.disposition = 'create_actor' THEN
            actor_uuid := gen_random_uuid();
            INSERT INTO hub.actor (actor_id, actor_type, display_name, canonical_name, summary, metadata)
            VALUES (
                actor_uuid, planned.normalized_record->>'actor_kind', planned.normalized_record->>'display_name',
                planned.normalized_record->>'canonical_name', planned.normalized_record->>'summary',
                jsonb_build_object('discovery_batch_id',requested_batch_id,
                                   'source_record_id',planned.source_record_id)
            );
            changed := changed + 1;
        ELSE
            actor_uuid := planned.resolved_actor_id;
        END IF;
        FOR account IN SELECT value FROM jsonb_array_elements(planned.normalized_record->'accounts') AS item(value) LOOP
            PERFORM pg_advisory_xact_lock(hashtextextended(
                lower(account->>'platform') || chr(31) || coalesce(account->>'external_id',account->>'normalized_url',lower(account->>'handle')), 0
            ));
            SELECT external_account.actor_id INTO conflicting_actor
            FROM hub.external_account external_account
            WHERE lower(external_account.platform) = lower(account->>'platform')
              AND (
                  (account->>'external_id' IS NOT NULL AND external_account.external_id = account->>'external_id')
                  OR (account->>'normalized_url' IS NOT NULL AND external_account.normalized_url = account->>'normalized_url')
                  OR (account->>'handle' IS NOT NULL AND lower(external_account.handle) = lower(account->>'handle'))
              ) LIMIT 1;
            IF conflicting_actor IS NOT NULL AND conflicting_actor <> actor_uuid THEN
                RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'account identity changed after blogger preview';
            END IF;
            IF conflicting_actor IS NULL THEN
                INSERT INTO hub.external_account (
                    actor_id, platform, external_id, handle, url, normalized_url, status, metadata
                ) VALUES (
                    actor_uuid, lower(account->>'platform'), account->>'external_id', account->>'handle',
                    account->>'url', account->>'normalized_url', 'active',
                    jsonb_build_object('discovery_batch_id',requested_batch_id,
                                       'source_record_id',planned.source_record_id)
                );
                changed := changed + 1;
            END IF;
        END LOOP;
        INSERT INTO hub.provenance_event (
            project_id, subject_type, subject_id, event_type, actor_kind, actor_ref,
            source_uri, observed_at, evidence
        ) VALUES (
            plan.project_id, 'actor', actor_uuid, 'blogger_discovery_applied', 'mcp_owner',
            requested_principal_id, planned.normalized_record->>'source_uri',
            (planned.normalized_record->>'observed_at')::timestamptz,
            coalesce(planned.normalized_record->'evidence','{}'::jsonb) ||
                jsonb_build_object('batch_id',requested_batch_id,'source_record_id',planned.source_record_id,
                                   'record_sha256',planned.record_sha256,'plan_sha256',plan.plan_sha256)
        ) RETURNING hub.provenance_event.provenance_event_id INTO provenance_uuid;
        INSERT INTO hub.project_actor (
            project_id, actor_id, membership_kind, status, provenance_event_id, metadata
        ) VALUES (
            plan.project_id, actor_uuid, 'blogger', 'included', provenance_uuid,
            jsonb_build_object('discovery_batch_id',requested_batch_id)
        ) ON CONFLICT (project_id, actor_id, membership_kind) DO NOTHING;
        changed := changed + 1;
    END LOOP;

    next_revision := hub.advance_canonical_revision(requested_expected_revision);
    INSERT INTO sync.external_outbox (
        outbox_id, aggregate_type, aggregate_id, effect_kind, idempotency_key, payload, required_revision
    ) VALUES (
        created_outbox_id, 'blogger_discovery_batch', requested_batch_id,
        'blogger_discovery.canonical_committed', 'blogger-discovery:' || requested_operation_id,
        jsonb_build_object('contract_version','blogger-discovery-commit.v1','batch_id',requested_batch_id,
                           'operation_id',requested_operation_id,'request_sha256',requested_request_sha256,
                           'plan_sha256',requested_plan_sha256,'canonical_revision',next_revision,
                           'create_actor_count',plan.create_actor_count,
                           'link_existing_count',plan.link_existing_count,
                           'quarantine_count',plan.quarantine_count),
        next_revision
    );
    INSERT INTO sync.audit_event (actor_id, client_id, action, outcome, subject_type, subject_id, details)
    VALUES (
        requested_principal_id, requested_client_id, 'blogger_discovery_apply', 'committed',
        'integration.batch', requested_batch_id,
        jsonb_build_object('operation_id',requested_operation_id,'request_sha256',requested_request_sha256,
                           'plan_sha256',requested_plan_sha256,'revision_before',requested_expected_revision,
                           'revision_after',next_revision,'affected_rows',changed)
    );
    INSERT INTO integration.blogger_discovery_apply_receipt (
        operation_id, batch_id, request_sha256, plan_sha256, master_instance_id,
        master_epoch, revision_before, revision_after, affected_rows, principal_id,
        client_id, outbox_id
    ) VALUES (
        requested_operation_id, requested_batch_id, requested_request_sha256,
        requested_plan_sha256, epoch_state.master_instance_id, epoch_state.current_epoch,
        requested_expected_revision, next_revision, changed, requested_principal_id,
        requested_client_id, created_outbox_id
    );
    INSERT INTO integration.receipt (
        batch_id, receipt_type, connector_id, idempotency_key, payload_sha256,
        canonical_revision, correlation_id, receipt
    ) SELECT
        accepted.batch_id, 'committed', accepted.connector_id, accepted.idempotency_key,
        accepted.payload_sha256, next_revision, accepted.correlation_id,
        jsonb_build_object('contract_version','blogger-discovery-apply-receipt.v1',
                           'operation_id',requested_operation_id,'request_sha256',requested_request_sha256,
                           'plan_sha256',requested_plan_sha256,'revision_after',next_revision,
                           'affected_rows',changed)
    FROM integration.batch accepted WHERE accepted.batch_id = requested_batch_id;
    UPDATE integration.batch SET status = 'canonical_committed', committed_at = clock_timestamp()
    WHERE integration.batch.batch_id = requested_batch_id AND status = 'staged';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'blogger discovery batch status changed before apply';
    END IF;
    UPDATE integration.connector_durability
    SET state = 'CANONICAL_COMMITTED', canonical_revision = next_revision, updated_at = clock_timestamp()
    WHERE integration.connector_durability.batch_id = requested_batch_id AND state = 'ACCEPTED';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'blogger discovery durability state changed before apply';
    END IF;
    UPDATE integration.blogger_discovery_plan SET state = 'APPLIED'
    WHERE integration.blogger_discovery_plan.batch_id = requested_batch_id AND state = 'PREVIEWED';
    INSERT INTO integration.batch_event (batch_id, event_type, actor_principal, correlation_id, details)
    SELECT requested_batch_id, 'canonical_committed', requested_principal_id, accepted.correlation_id,
        jsonb_build_object('operation_id',requested_operation_id,'plan_sha256',requested_plan_sha256,
                           'canonical_revision',next_revision,'outbox_id',created_outbox_id)
    FROM integration.batch accepted WHERE accepted.batch_id = requested_batch_id;
    RETURN QUERY SELECT requested_operation_id, requested_batch_id, requested_plan_sha256,
        changed, next_revision, false;
END
$$;

CREATE FUNCTION integration.reconcile_blogger_discovery(
    requested_operation_id text,
    requested_request_sha256 text,
    requested_plan_sha256 text,
    requested_master_instance_id uuid,
    requested_master_epoch bigint,
    requested_previous_revision bigint,
    requested_principal_id text,
    requested_client_id text
)
RETURNS TABLE (
    operation_id text,
    batch_id uuid,
    plan_sha256 text,
    affected_rows integer,
    revision_after bigint,
    duplicate boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    receipt integration.blogger_discovery_apply_receipt%ROWTYPE;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_canonical_committer', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'blogger reconciliation requires exact canonical committer login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO receipt FROM integration.blogger_discovery_apply_receipt
    WHERE integration.blogger_discovery_apply_receipt.operation_id = requested_operation_id;
    IF NOT FOUND THEN RETURN; END IF;
    IF receipt.request_sha256 <> requested_request_sha256
       OR receipt.plan_sha256 <> requested_plan_sha256
       OR receipt.master_instance_id <> requested_master_instance_id
       OR receipt.master_epoch <> requested_master_epoch
       OR receipt.revision_before <> requested_previous_revision
       OR receipt.principal_id <> requested_principal_id
       OR receipt.client_id <> requested_client_id THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'blogger receipt differs from exact reconciliation request';
    END IF;
    RETURN QUERY SELECT receipt.operation_id, receipt.batch_id, receipt.plan_sha256,
        receipt.affected_rows, receipt.revision_after, true;
END
$$;

DO $$
DECLARE relation text;
BEGIN
    FOREACH relation IN ARRAY ARRAY[
        'integration.blogger_discovery_artifact_landing',
        'integration.blogger_discovery_plan',
        'integration.blogger_discovery_plan_row',
        'integration.blogger_discovery_apply_receipt',
        'integration.blogger_discovery_quarantine'
    ] LOOP
        EXECUTE format(
            'CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard AFTER INSERT OR UPDATE OR DELETE ON %s '
            'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION master_control.enforce_write_epoch()',
            relation
        );
    END LOOP;
END
$$;

REVOKE ALL ON integration.blogger_discovery_artifact_landing,
    integration.blogger_discovery_plan, integration.blogger_discovery_plan_row,
    integration.blogger_discovery_apply_receipt, integration.blogger_discovery_quarantine
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    integration.materialize_blogger_discovery_artifact(uuid,text,jsonb,text),
    integration.preview_blogger_discovery(uuid,text,text,bigint,text,text),
    integration.apply_blogger_discovery(uuid,text,text,text,bigint,text,text),
    integration.reconcile_blogger_discovery(text,text,text,uuid,bigint,bigint,text,text)
    FROM PUBLIC;
GRANT SELECT ON hub.bloggers_v1 TO mdh_mcp_reader;
GRANT EXECUTE ON FUNCTION integration.materialize_blogger_discovery_artifact(uuid,text,jsonb,text)
    TO mdh_connector_intake;
GRANT EXECUTE ON FUNCTION
    integration.preview_blogger_discovery(uuid,text,text,bigint,text,text),
    integration.apply_blogger_discovery(uuid,text,text,text,bigint,text,text),
    integration.reconcile_blogger_discovery(text,text,text,uuid,bigint,bigint,text,text)
    TO mdh_canonical_committer;

UPDATE hub.canonical_state
SET schema_revision = 20, updated_at = clock_timestamp()
WHERE singleton = true;
