-- Cluster-local R1 group roles. Login credentials are provisioned separately by the
-- deployment secret manager; this file never creates passwords or grants remote roles
-- owner/superuser/DDL/BYPASSRLS/server-file/program privileges.
DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'mdh_owner', 'mdh_migrator', 'mdh_application', 'mdh_orchestrator',
        'mdh_connector_intake', 'mdh_mcp_reader', 'mdh_mcp_editor',
        'mdh_migration_operator', 'mdh_canonical_committer', 'mdh_backup', 'mdh_monitoring',
        'mdh_authenticator', 'mdh_master_controller', 'mdh_checkpoint'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS', role_name);
        END IF;
        EXECUTE format(
            'ALTER ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
            role_name
        );
    END LOOP;
END
$$;

-- The local-only migrator may deliberately SET ROLE to the non-login owner. No remote
-- or service role is a member of either role.
GRANT mdh_owner TO mdh_migrator;

DO $$
BEGIN
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO mdh_migrator, mdh_application, mdh_orchestrator, mdh_connector_intake, mdh_mcp_reader, mdh_mcp_editor, mdh_migration_operator, mdh_canonical_committer, mdh_backup, mdh_monitoring, mdh_authenticator, mdh_master_controller, mdh_checkpoint', current_database());
    EXECUTE format('GRANT CREATE, TEMPORARY ON DATABASE %I TO mdh_migrator', current_database());
    EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO mdh_application, mdh_orchestrator', current_database());
END
$$;

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;

-- The production API is an intake/readiness process, not a generic application writer.
-- Converge installations that applied the earlier broad scaffold before re-granting only
-- the relations actually exercised by API readiness and worker-result intake.
REVOKE ALL ON ALL TABLES IN SCHEMA hub, analysis, region_talk, joplin, sync,
    integration, orchestration FROM mdh_application;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA hub, analysis, region_talk, joplin, sync,
    integration, orchestration FROM mdh_application;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA hub, analysis, region_talk, joplin, sync,
    integration, orchestration FROM mdh_application;
GRANT USAGE ON SCHEMA hub, sync, orchestration TO mdh_application;
GRANT SELECT ON hub.canonical_state, hub.project TO mdh_application;
GRANT SELECT ON orchestration.pipeline, orchestration.pipeline_stage,
    orchestration.stage_run, orchestration.worker_result_inbox TO mdh_application;
GRANT INSERT ON orchestration.worker_result_inbox TO mdh_application;
GRANT INSERT ON sync.audit_event TO mdh_application;

GRANT USAGE ON SCHEMA orchestration, hub, sync TO mdh_orchestrator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA orchestration TO mdh_orchestrator;
GRANT SELECT ON hub.project, hub.canonical_state TO mdh_orchestrator;
GRANT INSERT ON sync.audit_event, sync.external_outbox TO mdh_orchestrator;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA orchestration, sync TO mdh_orchestrator;

GRANT USAGE ON SCHEMA integration TO mdh_connector_intake;
GRANT SELECT ON integration.connector, integration.data_product, integration.batch,
    integration.receipt TO mdh_connector_intake;
GRANT INSERT ON integration.batch, integration.batch_payload, integration.batch_event,
    integration.quarantine, integration.receipt TO mdh_connector_intake;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA integration TO mdh_connector_intake;

GRANT USAGE ON SCHEMA hub, analysis, orchestration, sync, region_talk, migration, joplin,
    integration, search
    TO mdh_mcp_reader;
REVOKE INSERT, UPDATE, DELETE ON hub.canonical_state FROM mdh_mcp_reader, mdh_mcp_editor;

-- A single non-login role owns the one bounded canonical-revision transition.
GRANT USAGE ON SCHEMA hub, integration, sync TO mdh_canonical_committer;
GRANT SELECT ON hub.canonical_state, integration.batch, integration.batch_payload,
    integration.data_product, integration.daily_statistic, integration.watermark,
    integration.quarantine,
    sync.external_outbox TO mdh_canonical_committer;
GRANT UPDATE (status, committed_at) ON integration.batch TO mdh_canonical_committer;
GRANT INSERT ON integration.daily_statistic, integration.batch_event,
    integration.watermark, integration.quarantine, sync.external_outbox
    TO mdh_canonical_committer;
GRANT UPDATE ON integration.watermark TO mdh_canonical_committer;
GRANT USAGE ON SCHEMA search TO mdh_canonical_committer;
GRANT SELECT, INSERT, UPDATE ON search.document, search.embedding_job,
    search.index_registry TO mdh_canonical_committer;
GRANT SELECT, INSERT ON search.embedding_768, search.embedding_1024 TO mdh_canonical_committer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA integration, sync TO mdh_canonical_committer;
GRANT EXECUTE ON FUNCTION hub.advance_canonical_revision(bigint) TO mdh_canonical_committer;
GRANT SELECT ON ALL TABLES IN SCHEMA hub, analysis, orchestration, sync, region_talk, joplin
    TO mdh_mcp_reader;
-- Raw migration payloads and exact artifact locators are migration-owner-only.
-- Reader-visible migration evidence is an explicit sanitized accounting surface.
GRANT SELECT ON migration.export_batch, migration.export_batch_kind,
    migration.row_disposition, migration.reconciliation_run,
    migration.reconciliation_finding, migration.row_accounting,
    migration.batch_accounting TO mdh_mcp_reader;
REVOKE ALL ON migration.raw_record, migration.export_file,
    migration.legacy_identity_map, migration.cutover_receipt
    FROM mdh_mcp_reader, mdh_mcp_editor;
GRANT SELECT ON integration.connector, integration.data_product, integration.batch,
    integration.batch_event, integration.watermark, integration.quarantine, integration.receipt,
    integration.provider_resource, integration.provider_operation, integration.provider_event
    TO mdh_mcp_reader;
GRANT SELECT ON search.document, search.embedding_model, search.embedding_job,
    search.embedding_768, search.embedding_1024, search.index_registry,
    search.embedding_coverage TO mdh_mcp_reader;
GRANT USAGE ON SCHEMA auth TO mdh_authenticator;
GRANT SELECT ON auth.oauth_revocation TO mdh_authenticator;
-- Converge installations that applied an earlier draft which exposed the
-- revocation authority to service roles.
REVOKE ALL ON auth.oauth_revocation FROM mdh_mcp_reader, mdh_mcp_editor, mdh_connector_intake;

-- The separately enabled operator profile is limited to two exact canonical
-- relations. Migration 0016 adds per-transaction guards: these table grants
-- cannot commit unless the same transaction records its revision, audit event,
-- semantic outbox operation and immutable operator receipt.
GRANT USAGE ON SCHEMA operator_control, recovery, sync, hub, master_control
    TO mdh_mcp_editor;
GRANT SELECT ON recovery.evidence TO mdh_mcp_editor;
GRANT SELECT ON sync.checkpoint TO mdh_mcp_editor;
GRANT SELECT ON hub.canonical_state, hub.project, hub.content_item TO mdh_mcp_editor;
GRANT SELECT ON master_control.epoch_state TO mdh_mcp_editor;
GRANT INSERT (project_id, slug, name, description, status, metadata),
    UPDATE (slug, name, description, status, metadata), DELETE ON hub.project
    TO mdh_mcp_editor;
GRANT INSERT (
        content_id, content_type, title, summary, body_excerpt, language,
        canonical_url, normalized_url, content_hash, published_at,
        first_observed_at, last_observed_at, status, metadata
    ),
    UPDATE (
        content_type, title, summary, body_excerpt, language, canonical_url,
        normalized_url, content_hash, published_at, first_observed_at,
        last_observed_at, status, metadata
    ), DELETE ON hub.content_item TO mdh_mcp_editor;
GRANT SELECT, INSERT ON operator_control.preview_receipt,
    operator_control.apply_receipt TO mdh_mcp_editor;
GRANT EXECUTE ON FUNCTION master_control.assert_session_write_epoch(),
    operator_control.commit_mcp_change(text,bigint,text,text,integer,text,text,text,text)
    TO mdh_mcp_editor;
GRANT USAGE ON SCHEMA migration, hub, region_talk, sync TO mdh_migration_operator;
GRANT SELECT ON ALL TABLES IN SCHEMA migration TO mdh_migration_operator;
GRANT SELECT ON hub.canonical_state, hub.project TO mdh_migration_operator;
GRANT SELECT, INSERT ON hub.actor, hub.external_account, hub.project_actor,
    hub.provenance_event, region_talk.blogger_profile,
    migration.duplicate_group, migration.duplicate_group_member
    TO mdh_migration_operator;
GRANT INSERT ON sync.external_outbox, sync.audit_event TO mdh_migration_operator;
GRANT EXECUTE ON FUNCTION hub.advance_canonical_revision(bigint) TO mdh_migration_operator;
GRANT INSERT ON migration.export_batch, migration.export_batch_kind,
    migration.export_file, migration.raw_record, migration.row_disposition,
    migration.legacy_identity_map, migration.reconciliation_run,
    migration.reconciliation_finding TO mdh_migration_operator;
GRANT UPDATE ON migration.export_batch, migration.row_disposition,
    migration.reconciliation_run, migration.reconciliation_finding
    TO mdh_migration_operator;
REVOKE UPDATE, DELETE ON migration.export_batch_kind, migration.export_file,
    migration.raw_record, migration.legacy_identity_map FROM mdh_migration_operator;
REVOKE INSERT, UPDATE, DELETE ON migration.cutover_receipt FROM mdh_migration_operator;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA migration TO mdh_migration_operator;

GRANT CONNECT ON DATABASE postgres TO mdh_backup;
GRANT pg_read_all_data TO mdh_backup;
-- Group roles are deliberately NOINHERIT, so pg_read_all_data membership is not relied
-- upon at runtime. Enumerate the current backup surface and re-run this contract after
-- every owner-scoped migration; new relations remain fail-closed until then.
GRANT USAGE ON SCHEMA hub_meta, hub, analysis, orchestration, sync, region_talk,
    migration, joplin, integration, recovery, operator_control, auth, search TO mdh_backup;
GRANT SELECT ON ALL TABLES IN SCHEMA hub_meta, hub, analysis, orchestration, sync,
    region_talk, migration, joplin, integration, recovery, operator_control, auth, search
    TO mdh_backup;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA hub_meta, hub, analysis, orchestration, sync,
    region_talk, migration, joplin, integration, recovery, operator_control, auth, search
    TO mdh_backup;
GRANT pg_monitor TO mdh_monitoring;
GRANT USAGE ON SCHEMA hub, orchestration, integration, recovery, sync TO mdh_monitoring;
GRANT SELECT ON hub.canonical_state, orchestration.queue_health,
    integration.connector, integration.data_product, integration.batch,
    integration.daily_statistic, integration.quarantine,
    integration.provider_resource, recovery.evidence, sync.external_outbox TO mdh_monitoring;

-- The controller is a local-only, non-login capability.  It can move the epoch
-- state machine and bind/revoke ephemeral logins, but cannot read canonical rows.
GRANT USAGE ON SCHEMA master_control TO mdh_master_controller, mdh_checkpoint;
GRANT EXECUTE ON FUNCTION master_control.begin_epoch(uuid,text,bigint,timestamptz),
    master_control.open_write_gate(uuid,bigint),
    master_control.renew_epoch(uuid,bigint,timestamptz),
    master_control.close_write_gate(uuid,bigint,text,text),
    master_control.bind_epoch_credential(uuid,name,uuid,bigint,timestamptz),
    master_control.revoke_epoch_credential(uuid,text)
    TO mdh_master_controller;
GRANT SELECT ON master_control.epoch_state TO mdh_checkpoint;

-- Group defaults are defense in depth.  The broker repeats the same settings on
-- every LOGIN principal because ALTER ROLE settings are evaluated at login.
ALTER ROLE mdh_application SET statement_timeout = '30s';
ALTER ROLE mdh_application SET lock_timeout = '5s';
ALTER ROLE mdh_application SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE mdh_orchestrator SET statement_timeout = '30s';
ALTER ROLE mdh_orchestrator SET lock_timeout = '5s';
ALTER ROLE mdh_connector_intake SET statement_timeout = '30s';
ALTER ROLE mdh_mcp_reader SET default_transaction_read_only = on;
ALTER ROLE mdh_mcp_reader SET statement_timeout = '15s';
ALTER ROLE mdh_mcp_reader SET lock_timeout = '3s';
ALTER ROLE mdh_mcp_editor SET statement_timeout = '30s';
ALTER ROLE mdh_mcp_editor SET lock_timeout = '5s';
ALTER ROLE mdh_mcp_editor SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE mdh_migration_operator SET statement_timeout = '2min';
ALTER ROLE mdh_migration_operator SET lock_timeout = '10s';
ALTER ROLE mdh_canonical_committer SET statement_timeout = '2min';
ALTER ROLE mdh_canonical_committer SET lock_timeout = '10s';

-- New objects fail closed. Re-running this explicit contract after a migration is required
-- before a new object is visible to any service role.
ALTER DEFAULT PRIVILEGES FOR ROLE mdh_owner REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE mdh_owner REVOKE ALL ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE mdh_owner REVOKE ALL ON SEQUENCES FROM PUBLIC;
