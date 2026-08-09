-- Shared identity, content, project membership and provenance model.
CREATE TABLE hub.project (
    project_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    slug                citext NOT NULL UNIQUE,
    name                text NOT NULL,
    description         text,
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'paused', 'archived')),
    revision            bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER project_set_updated_at
BEFORE UPDATE ON hub.project
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE hub.actor (
    actor_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_type          text NOT NULL
                        CHECK (actor_type IN ('person', 'organisation', 'outlet', 'collective', 'unknown')),
    display_name        text NOT NULL,
    canonical_name      text,
    summary             text,
    revision            bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX actor_canonical_name_idx ON hub.actor (lower(canonical_name));
CREATE TRIGGER actor_set_updated_at
BEFORE UPDATE ON hub.actor
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE hub.external_account (
    account_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id            uuid NOT NULL REFERENCES hub.actor(actor_id) ON DELETE CASCADE,
    platform            text NOT NULL,
    external_id         text,
    handle              text,
    url                 text,
    normalized_url      text,
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'inactive', 'unknown', 'deleted')),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    revision            bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (external_id IS NOT NULL OR normalized_url IS NOT NULL OR handle IS NOT NULL)
);
CREATE UNIQUE INDEX external_account_platform_external_id_uq
    ON hub.external_account (platform, external_id)
    WHERE external_id IS NOT NULL;
CREATE UNIQUE INDEX external_account_platform_url_uq
    ON hub.external_account (platform, normalized_url)
    WHERE normalized_url IS NOT NULL;
CREATE INDEX external_account_actor_idx ON hub.external_account (actor_id);
CREATE TRIGGER external_account_set_updated_at
BEFORE UPDATE ON hub.external_account
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE hub.content_item (
    content_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_type        text NOT NULL
                        CHECK (content_type IN ('post', 'article', 'video', 'link', 'note', 'document', 'other')),
    title               text,
    summary             text,
    body_excerpt        text,
    language            text,
    canonical_url       text,
    normalized_url      text,
    content_hash        text,
    published_at        timestamptz,
    first_observed_at   timestamptz NOT NULL DEFAULT now(),
    last_observed_at    timestamptz NOT NULL DEFAULT now(),
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'unavailable', 'deleted', 'superseded', 'unknown')),
    revision            bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    search_document     tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'pg_catalog.russian',
            coalesce(title, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(body_excerpt, '')
        )
    ) STORED,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX content_item_fts_idx ON hub.content_item USING gin (search_document);
CREATE INDEX content_item_title_trgm_idx ON hub.content_item USING gin (title gin_trgm_ops);
CREATE INDEX content_item_published_idx ON hub.content_item (published_at DESC);
CREATE INDEX content_item_hash_idx ON hub.content_item (content_hash) WHERE content_hash IS NOT NULL;
CREATE INDEX content_item_normalized_url_idx ON hub.content_item (normalized_url) WHERE normalized_url IS NOT NULL;
CREATE TRIGGER content_item_set_updated_at
BEFORE UPDATE ON hub.content_item
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE hub.content_identity (
    identity_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    namespace           text NOT NULL,
    external_id         text,
    normalized_value    text NOT NULL,
    is_primary          boolean NOT NULL DEFAULT false,
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (namespace, normalized_value)
);
CREATE INDEX content_identity_content_idx ON hub.content_identity (content_id);

CREATE TABLE hub.content_author (
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    actor_id            uuid NOT NULL REFERENCES hub.actor(actor_id) ON DELETE RESTRICT,
    role                text NOT NULL DEFAULT 'author',
    position            integer,
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (content_id, actor_id, role)
);

CREATE TABLE hub.provenance_event (
    provenance_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id          uuid REFERENCES hub.project(project_id) ON DELETE SET NULL,
    subject_type        text NOT NULL,
    subject_id          uuid,
    event_type          text NOT NULL,
    actor_kind          text NOT NULL DEFAULT 'system',
    actor_ref           text,
    query_text          text,
    source_uri          text,
    run_id              uuid,
    observed_at         timestamptz NOT NULL DEFAULT now(),
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX provenance_subject_idx
    ON hub.provenance_event (subject_type, subject_id, observed_at DESC);
CREATE INDEX provenance_project_idx
    ON hub.provenance_event (project_id, observed_at DESC);
CREATE TRIGGER provenance_event_append_only
BEFORE UPDATE OR DELETE ON hub.provenance_event
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE hub.project_content (
    project_id          uuid NOT NULL REFERENCES hub.project(project_id) ON DELETE CASCADE,
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    status              text NOT NULL DEFAULT 'candidate'
                        CHECK (status IN ('candidate', 'included', 'excluded', 'published', 'archived')),
    provenance_event_id uuid REFERENCES hub.provenance_event(provenance_event_id) ON DELETE SET NULL,
    revision            bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    added_at            timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, content_id)
);
CREATE TRIGGER project_content_set_updated_at
BEFORE UPDATE ON hub.project_content
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE hub.content_asset (
    asset_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    asset_type          text NOT NULL
                        CHECK (asset_type IN ('image', 'video', 'audio', 'document', 'thumbnail', 'other')),
    source_url          text,
    normalized_url      text,
    source_external_id  text,
    position            integer NOT NULL DEFAULT 0 CHECK (position >= 0),
    mime_type           text,
    byte_size           bigint CHECK (byte_size IS NULL OR byte_size >= 0),
    sha256              text,
    perceptual_hash     text,
    width               integer CHECK (width IS NULL OR width > 0),
    height              integer CHECK (height IS NULL OR height > 0),
    status              text NOT NULL DEFAULT 'observed'
                        CHECK (status IN ('observed', 'available', 'unavailable', 'rejected', 'deleted')),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    revision            bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX content_asset_content_idx ON hub.content_asset (content_id, position);
CREATE UNIQUE INDEX content_asset_source_uq
    ON hub.content_asset (content_id, normalized_url)
    WHERE normalized_url IS NOT NULL;
CREATE TRIGGER content_asset_set_updated_at
BEFORE UPDATE ON hub.content_asset
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE hub.entity_alias (
    alias_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type         text NOT NULL,
    alias_namespace     text NOT NULL,
    alias_value         text NOT NULL,
    canonical_id        uuid NOT NULL,
    source_system       text,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (entity_type, alias_namespace, alias_value)
);
CREATE INDEX entity_alias_canonical_idx ON hub.entity_alias (entity_type, canonical_id);
