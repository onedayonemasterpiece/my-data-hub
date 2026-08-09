-- Future Joplin bridge. Joplin remains an authoring/projection surface, not canonical storage.
-- Initial mode is import-only; outbound writes require a later reviewed adapter release.
CREATE TABLE joplin.notebook_link (
    notebook_link_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id          uuid REFERENCES hub.project(project_id) ON DELETE CASCADE,
    bridge_instance_id  text NOT NULL,
    joplin_notebook_id  text NOT NULL,
    direction           text NOT NULL DEFAULT 'import'
                        CHECK (direction IN ('import', 'export_reviewed', 'bidirectional_review')),
    status              text NOT NULL DEFAULT 'paused'
                        CHECK (status IN ('active', 'paused', 'disconnected', 'revoked')),
    mapping             jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bridge_instance_id, joplin_notebook_id)
);
CREATE TRIGGER joplin_notebook_link_set_updated_at
BEFORE UPDATE ON joplin.notebook_link
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE joplin.note_link (
    note_link_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    notebook_link_id    uuid NOT NULL REFERENCES joplin.notebook_link(notebook_link_id) ON DELETE CASCADE,
    joplin_note_id      text NOT NULL,
    object_type         text NOT NULL,
    object_id           uuid NOT NULL,
    last_joplin_hash    text,
    last_hub_revision   bigint CHECK (last_hub_revision IS NULL OR last_hub_revision >= 0),
    last_joplin_updated_time bigint,
    status              text NOT NULL DEFAULT 'linked'
                        CHECK (status IN ('linked', 'conflict', 'deleted', 'paused')),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (notebook_link_id, joplin_note_id),
    UNIQUE (notebook_link_id, object_type, object_id)
);
CREATE INDEX joplin_note_object_idx ON joplin.note_link (object_type, object_id);
CREATE TRIGGER joplin_note_link_set_updated_at
BEFORE UPDATE ON joplin.note_link
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE joplin.note_revision (
    note_revision_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    note_link_id        uuid NOT NULL REFERENCES joplin.note_link(note_link_id) ON DELETE CASCADE,
    source              text NOT NULL CHECK (source IN ('joplin', 'hub')),
    content_hash        text NOT NULL,
    title               text,
    compact_metadata    jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (note_link_id, source, content_hash)
);
CREATE TRIGGER joplin_note_revision_append_only
BEFORE UPDATE OR DELETE ON joplin.note_revision
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE joplin.sync_cursor (
    notebook_link_id    uuid PRIMARY KEY REFERENCES joplin.notebook_link(notebook_link_id) ON DELETE CASCADE,
    cursor_value        text,
    last_sync_at        timestamptz,
    last_success_at     timestamptz,
    last_error          jsonb,
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER joplin_sync_cursor_set_updated_at
BEFORE UPDATE ON joplin.sync_cursor
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE joplin.conflict (
    joplin_conflict_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    note_link_id        uuid NOT NULL REFERENCES joplin.note_link(note_link_id) ON DELETE CASCADE,
    joplin_snapshot     jsonb NOT NULL,
    hub_snapshot        jsonb NOT NULL,
    status              text NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'resolved_joplin', 'resolved_hub', 'resolved_merged', 'dismissed')),
    resolution          jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    resolved_at         timestamptz
);
CREATE INDEX joplin_conflict_open_idx ON joplin.conflict (created_at) WHERE status = 'open';
