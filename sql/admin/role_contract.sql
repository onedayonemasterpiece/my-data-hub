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
        'mdh_authenticator'
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
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO mdh_migrator, mdh_application, mdh_orchestrator, mdh_connector_intake, mdh_mcp_reader, mdh_mcp_editor, mdh_migration_operator, mdh_canonical_committer, mdh_backup, mdh_monitoring, mdh_authenticator', current_database());
    EXECUTE format('GRANT CREATE, TEMPORARY ON DATABASE %I TO mdh_migrator', current_database());
    EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO mdh_application, mdh_orchestrator', current_database());
END
$$;

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;

GRANT USAGE ON SCHEMA hub, analysis, region_talk, joplin, sync, integration,
    orchestration TO mdh_application;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA hub, analysis, region_talk, joplin TO mdh_application;
REVOKE INSERT, UPDATE, DELETE ON hub.canonical_state FROM mdh_application;
GRANT SELECT ON hub.canonical_state TO mdh_application;
GRANT SELECT, INSERT, UPDATE, DELETE ON sync.session, sync.command, sync.command_receipt,
    sync.changeset_header, sync.changeset_operation, sync.applied_changeset,
    sync.conflict, sync.id_remap, sync.external_outbox TO mdh_application;
REVOKE ALL ON sync.checkpoint FROM mdh_application;
GRANT INSERT ON sync.audit_event TO mdh_application;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA hub, analysis, region_talk, joplin, sync TO mdh_application;
GRANT SELECT ON integration.connector, integration.data_product, integration.batch,
    integration.batch_payload, integration.watermark, integration.daily_statistic TO mdh_application;
GRANT SELECT ON orchestration.pipeline, orchestration.pipeline_stage,
    orchestration.stage_run, orchestration.worker_result_inbox TO mdh_application;
GRANT INSERT ON orchestration.worker_result_inbox TO mdh_application;

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
    integration
    TO mdh_mcp_reader;
REVOKE INSERT, UPDATE, DELETE ON hub.canonical_state FROM mdh_mcp_reader, mdh_mcp_editor;

-- A single non-login role owns the one bounded canonical-revision transition.
GRANT USAGE ON SCHEMA hub, integration, sync TO mdh_canonical_committer;
GRANT SELECT ON hub.canonical_state, integration.batch, integration.batch_payload,
    integration.data_product, integration.daily_statistic, integration.watermark,
    sync.external_outbox TO mdh_canonical_committer;
GRANT UPDATE (status, committed_at) ON integration.batch TO mdh_canonical_committer;
GRANT INSERT ON integration.daily_statistic, integration.batch_event,
    integration.watermark, sync.external_outbox TO mdh_canonical_committer;
GRANT UPDATE ON integration.watermark TO mdh_canonical_committer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA integration, sync TO mdh_canonical_committer;
GRANT EXECUTE ON FUNCTION hub.advance_canonical_revision(bigint) TO mdh_canonical_committer;
GRANT SELECT ON ALL TABLES IN SCHEMA hub, analysis, orchestration, sync, region_talk, migration, joplin
    TO mdh_mcp_reader;
GRANT SELECT ON integration.connector, integration.data_product, integration.batch,
    integration.batch_event, integration.watermark, integration.quarantine, integration.receipt,
    integration.provider_resource, integration.provider_operation, integration.provider_event
    TO mdh_mcp_reader;
GRANT USAGE ON SCHEMA auth TO mdh_authenticator;
GRANT SELECT ON auth.oauth_revocation TO mdh_authenticator;
-- Converge installations that applied an earlier draft which exposed the
-- revocation authority to service roles.
REVOKE ALL ON auth.oauth_revocation FROM mdh_mcp_reader, mdh_mcp_editor, mdh_connector_intake;

-- R1 editor production allowlist is intentionally empty. Disposable verification grants
-- are created and removed by scripts/verify_postgres_roles.py.
GRANT USAGE ON SCHEMA operator_control, recovery, sync TO mdh_mcp_editor;
GRANT SELECT ON recovery.evidence TO mdh_mcp_editor;
GRANT SELECT ON sync.checkpoint TO mdh_mcp_editor;
GRANT SELECT, INSERT ON operator_control.preview_receipt,
    operator_control.apply_receipt TO mdh_mcp_editor;
GRANT USAGE ON SCHEMA migration, hub TO mdh_migration_operator;
GRANT SELECT ON ALL TABLES IN SCHEMA migration TO mdh_migration_operator;
GRANT SELECT ON hub.canonical_state, hub.project TO mdh_migration_operator;
GRANT INSERT, UPDATE ON migration.export_batch, migration.export_batch_kind,
    migration.export_file, migration.raw_record, migration.row_disposition,
    migration.legacy_identity_map, migration.reconciliation_run,
    migration.reconciliation_finding TO mdh_migration_operator;
REVOKE INSERT, UPDATE, DELETE ON migration.cutover_receipt FROM mdh_migration_operator;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA migration TO mdh_migration_operator;

GRANT CONNECT ON DATABASE postgres TO mdh_backup;
GRANT pg_read_all_data TO mdh_backup;
-- Group roles are deliberately NOINHERIT, so pg_read_all_data membership is not relied
-- upon at runtime. Enumerate the current backup surface and re-run this contract after
-- every owner-scoped migration; new relations remain fail-closed until then.
GRANT USAGE ON SCHEMA hub_meta, hub, analysis, orchestration, sync, region_talk,
    migration, joplin, integration, recovery, operator_control, auth TO mdh_backup;
GRANT SELECT ON ALL TABLES IN SCHEMA hub_meta, hub, analysis, orchestration, sync,
    region_talk, migration, joplin, integration, recovery, operator_control, auth
    TO mdh_backup;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA hub_meta, hub, analysis, orchestration, sync,
    region_talk, migration, joplin, integration, recovery, operator_control, auth
    TO mdh_backup;
GRANT pg_monitor TO mdh_monitoring;
GRANT USAGE ON SCHEMA hub, orchestration, integration, recovery TO mdh_monitoring;
GRANT SELECT ON hub.canonical_state, orchestration.queue_health,
    integration.connector, integration.data_product, integration.batch,
    integration.provider_resource, recovery.evidence TO mdh_monitoring;

-- New objects fail closed. Re-running this explicit contract after a migration is required
-- before a new object is visible to any service role.
ALTER DEFAULT PRIVILEGES FOR ROLE mdh_owner REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE mdh_owner REVOKE ALL ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE mdh_owner REVOKE ALL ON SEQUENCES FROM PUBLIC;
