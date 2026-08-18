-- Stabilize the logical Region Talk project across independently bootstrapped
-- master epochs.  Migration 0009 predates the durable cross-epoch request
-- contract and let PostgreSQL generate a different UUID on every empty boot.
-- The fixed value is the project identity already bound by the first durable,
-- owner-authorized Region Talk blogger migration request.

DO $$
DECLARE
    observed_project_id uuid;
    stable_project_id constant uuid := 'da92f94f-5848-4a4b-bca7-12f797288aa7';
BEGIN
    SELECT project_id
      INTO observed_project_id
      FROM hub.project
     WHERE slug = 'region-talk';

    IF observed_project_id IS NULL THEN
        RAISE EXCEPTION 'the Region Talk seed project is absent';
    END IF;

    IF observed_project_id <> stable_project_id THEN
        -- PostgreSQL foreign keys deliberately make this fail closed if a
        -- database already contains project-bound business rows.  The initial
        -- stabilization is valid only before the first verified checkpoint;
        -- subsequent restores already carry the stable identity.
        UPDATE hub.project
           SET project_id = stable_project_id
         WHERE project_id = observed_project_id
           AND slug = 'region-talk';
    END IF;
END;
$$;

UPDATE hub.canonical_state
SET schema_revision = 22,
    updated_at = clock_timestamp()
WHERE singleton = true;
