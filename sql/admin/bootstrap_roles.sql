-- Password-free cluster roles required before the first owner-scoped migration.
-- Login principals and passwords are provisioned separately from protected secrets.
DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'mdh_owner', 'mdh_migrator', 'mdh_application', 'mdh_orchestrator',
        'mdh_connector_intake', 'mdh_mcp_reader', 'mdh_mcp_editor',
        'mdh_migration_operator', 'mdh_canonical_committer', 'mdh_backup',
        'mdh_monitoring', 'mdh_authenticator', 'mdh_master_controller',
        'mdh_checkpoint'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format(
                'CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
                role_name
            );
        END IF;
        EXECUTE format(
            'ALTER ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
            role_name
        );
    END LOOP;
END
$$;

-- pgvector is not a trusted extension and therefore must be installed at this
-- narrow privileged bootstrap boundary before owner-scoped migrations run.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS vector;
GRANT mdh_owner TO mdh_migrator;
DO $$
BEGIN
    EXECUTE format('GRANT CREATE, TEMPORARY ON DATABASE %I TO mdh_owner', current_database());
END
$$;
