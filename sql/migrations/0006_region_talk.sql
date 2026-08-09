-- Region Talk product semantics projected on top of shared hub entities.
CREATE TABLE region_talk.source (
    source_id           uuid PRIMARY KEY REFERENCES hub.actor(actor_id) ON DELETE RESTRICT,
    primary_account_id  uuid REFERENCES hub.external_account(account_id) ON DELETE SET NULL,
    source_kind         text NOT NULL,
    externality_verdict text NOT NULL DEFAULT 'unknown'
                        CHECK (externality_verdict IN ('local', 'nonlocal', 'mixed', 'unknown')),
    publisher_type      text,
    verdict_confidence  numeric(5,4) CHECK (verdict_confidence IS NULL OR verdict_confidence BETWEEN 0 AND 1),
    verdict_version     text,
    status              text NOT NULL DEFAULT 'candidate'
                        CHECK (status IN ('candidate', 'active', 'paused', 'excluded', 'terminal')),
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX region_talk_source_status_idx ON region_talk.source (status, externality_verdict);
CREATE TRIGGER region_talk_source_set_updated_at
BEFORE UPDATE ON region_talk.source
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE region_talk.source_candidate (
    source_candidate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           uuid REFERENCES region_talk.source(source_id) ON DELETE SET NULL,
    candidate_url       text,
    candidate_handle    text,
    discovered_by       text NOT NULL,
    discovery_run_id    uuid REFERENCES orchestration.run(run_id) ON DELETE SET NULL,
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'resolved', 'accepted', 'rejected', 'duplicate', 'quarantined')),
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (candidate_url IS NOT NULL OR candidate_handle IS NOT NULL)
);
CREATE INDEX source_candidate_status_idx ON region_talk.source_candidate (status, created_at);

CREATE TABLE region_talk.source_edge (
    source_edge_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    from_source_id      uuid REFERENCES region_talk.source(source_id) ON DELETE CASCADE,
    to_source_id        uuid REFERENCES region_talk.source(source_id) ON DELETE CASCADE,
    edge_kind           text NOT NULL,
    source_ref          text,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (from_source_id IS NOT NULL OR to_source_id IS NOT NULL)
);
CREATE UNIQUE INDEX source_edge_identity_uq
    ON region_talk.source_edge (
        coalesce(from_source_id, '00000000-0000-0000-0000-000000000000'::uuid),
        coalesce(to_source_id, '00000000-0000-0000-0000-000000000000'::uuid),
        edge_kind,
        coalesce(source_ref, '')
    );

CREATE TABLE region_talk.source_status (
    source_status_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           uuid NOT NULL REFERENCES region_talk.source(source_id) ON DELETE CASCADE,
    status              text NOT NULL,
    reason              text,
    gate_version        text,
    run_id              uuid REFERENCES orchestration.run(run_id) ON DELETE SET NULL,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX source_status_source_time_idx
    ON region_talk.source_status (source_id, occurred_at DESC);
CREATE TRIGGER source_status_append_only
BEFORE UPDATE OR DELETE ON region_talk.source_status
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.source_profile (
    source_profile_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           uuid NOT NULL REFERENCES region_talk.source(source_id) ON DELETE CASCADE,
    profile_version     integer NOT NULL CHECK (profile_version >= 1),
    profile             jsonb NOT NULL,
    input_fingerprint   text NOT NULL,
    status              text NOT NULL DEFAULT 'current'
                        CHECK (status IN ('current', 'superseded', 'rejected')),
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, profile_version),
    UNIQUE (source_id, input_fingerprint)
);

CREATE TABLE region_talk.source_profile_evidence (
    evidence_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_profile_id   uuid NOT NULL REFERENCES region_talk.source_profile(source_profile_id) ON DELETE CASCADE,
    evidence_kind       text NOT NULL,
    source_url          text,
    evidence            jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE region_talk.telegram_entity_cache (
    entity_key          text PRIMARY KEY,
    entity_type         text NOT NULL,
    entity_payload      jsonb NOT NULL,
    entity_fingerprint  text NOT NULL,
    observed_at         timestamptz NOT NULL,
    expires_at          timestamptz,
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER telegram_entity_cache_set_updated_at
BEFORE UPDATE ON region_talk.telegram_entity_cache
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE region_talk.post_intake (
    post_intake_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    source_id           uuid REFERENCES region_talk.source(source_id) ON DELETE SET NULL,
    intake_kind         text NOT NULL,
    exact_url           text,
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'fetched', 'evaluated', 'accepted', 'rejected', 'terminal', 'quarantined')),
    legacy_queue_seq    bigint,
    discovery_run_id    uuid REFERENCES orchestration.run(run_id) ON DELETE SET NULL,
    input_fingerprint   text NOT NULL,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (content_id, intake_kind, input_fingerprint)
);
CREATE INDEX post_intake_status_idx ON region_talk.post_intake (status, created_at);
CREATE TRIGGER post_intake_set_updated_at
BEFORE UPDATE ON region_talk.post_intake
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE region_talk.text_evidence (
    text_evidence_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    evidence_kind       text NOT NULL,
    model_id            uuid REFERENCES analysis.model(model_id) ON DELETE RESTRICT,
    encoder_contract    text,
    input_fingerprint   text NOT NULL,
    verdict             jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX text_evidence_identity_uq
    ON region_talk.text_evidence (
        content_id,
        evidence_kind,
        coalesce(model_id, '00000000-0000-0000-0000-000000000000'::uuid),
        coalesce(encoder_contract, ''),
        input_fingerprint
    );
CREATE TRIGGER text_evidence_append_only
BEFORE UPDATE OR DELETE ON region_talk.text_evidence
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.post_evaluation (
    post_evaluation_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    source_id           uuid REFERENCES region_talk.source(source_id) ON DELETE SET NULL,
    eligible            boolean NOT NULL,
    decision            text NOT NULL
                        CHECK (decision IN ('accept', 'needs_source_review', 'needs_text_review', 'reject')),
    primary_reason      text NOT NULL,
    gate_version        text NOT NULL,
    input_fingerprint   text NOT NULL,
    evidence            jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (content_id, gate_version, input_fingerprint)
);
CREATE INDEX post_evaluation_decision_idx
    ON region_talk.post_evaluation (decision, created_at DESC);
CREATE TRIGGER post_evaluation_append_only
BEFORE UPDATE OR DELETE ON region_talk.post_evaluation
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.image_evaluation (
    image_evaluation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id            uuid NOT NULL REFERENCES hub.content_asset(asset_id) ON DELETE CASCADE,
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    verdict             text NOT NULL,
    postcard_score      numeric(7,4),
    model_id            uuid REFERENCES analysis.model(model_id) ON DELETE RESTRICT,
    input_fingerprint   text NOT NULL,
    evidence            jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX image_evaluation_identity_uq
    ON region_talk.image_evaluation (
        asset_id,
        coalesce(model_id, '00000000-0000-0000-0000-000000000000'::uuid),
        input_fingerprint
    );
CREATE INDEX image_evaluation_content_idx
    ON region_talk.image_evaluation (content_id, created_at DESC);
CREATE TRIGGER image_evaluation_append_only
BEFORE UPDATE OR DELETE ON region_talk.image_evaluation
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.candidate_memory (
    candidate_memory_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    memory_kind         text NOT NULL,
    memory_key          text NOT NULL,
    value               jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (content_id, memory_kind, memory_key)
);
CREATE TRIGGER candidate_memory_set_updated_at
BEFORE UPDATE ON region_talk.candidate_memory
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE region_talk.publication_candidate (
    candidate_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    project_id          uuid NOT NULL REFERENCES hub.project(project_id) ON DELETE CASCADE,
    status              text NOT NULL DEFAULT 'draft'
                        CHECK (status IN ('draft', 'ready', 'in_review', 'approved', 'rejected', 'published', 'revoked')),
    current_revision    integer NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
    eligibility_result_id uuid REFERENCES region_talk.post_evaluation(post_evaluation_id) ON DELETE RESTRICT,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (content_id, project_id)
);
CREATE TRIGGER publication_candidate_set_updated_at
BEFORE UPDATE ON region_talk.publication_candidate
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE region_talk.candidate_revision (
    candidate_id        uuid NOT NULL REFERENCES region_talk.publication_candidate(candidate_id) ON DELETE CASCADE,
    revision            integer NOT NULL CHECK (revision >= 1),
    revision_fingerprint text NOT NULL,
    text_payload        jsonb NOT NULL,
    ordered_media       jsonb NOT NULL DEFAULT '[]'::jsonb,
    cta                 jsonb NOT NULL DEFAULT '{}'::jsonb,
    writer_model        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (candidate_id, revision),
    UNIQUE (revision_fingerprint)
);
CREATE TRIGGER candidate_revision_append_only
BEFORE UPDATE OR DELETE ON region_talk.candidate_revision
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.review_decision (
    review_decision_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        uuid NOT NULL,
    candidate_revision  integer NOT NULL,
    decision            text NOT NULL CHECK (decision IN ('approve', 'reject', 'revise', 'revoke')),
    actor_ref           text NOT NULL,
    reason              text,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (candidate_id, candidate_revision)
        REFERENCES region_talk.candidate_revision(candidate_id, revision) ON DELETE RESTRICT
);
CREATE INDEX review_decision_candidate_idx
    ON region_talk.review_decision (candidate_id, occurred_at DESC);
CREATE TRIGGER review_decision_append_only
BEFORE UPDATE OR DELETE ON region_talk.review_decision
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.publication_plan (
    publication_plan_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        uuid NOT NULL,
    candidate_revision  integer NOT NULL,
    channel             text NOT NULL,
    idempotency_key     text NOT NULL UNIQUE,
    status              text NOT NULL DEFAULT 'planned'
                        CHECK (status IN ('planned', 'queued', 'published', 'failed', 'cancelled')),
    scheduled_for       timestamptz,
    payload             jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (candidate_id, candidate_revision)
        REFERENCES region_talk.candidate_revision(candidate_id, revision) ON DELETE RESTRICT
);
CREATE TRIGGER publication_plan_set_updated_at
BEFORE UPDATE ON region_talk.publication_plan
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE region_talk.publication_attempt (
    publication_attempt_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    publication_plan_id uuid NOT NULL REFERENCES region_talk.publication_plan(publication_plan_id) ON DELETE RESTRICT,
    attempt_number      integer NOT NULL CHECK (attempt_number >= 1),
    status              text NOT NULL CHECK (status IN ('started', 'succeeded', 'failed', 'unknown')),
    request_fingerprint text NOT NULL,
    provider_receipt    jsonb,
    error               jsonb,
    occurred_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (publication_plan_id, attempt_number)
);
CREATE TRIGGER publication_attempt_append_only
BEFORE UPDATE OR DELETE ON region_talk.publication_attempt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.discovery_observation (
    observation_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           uuid REFERENCES region_talk.source(source_id) ON DELETE SET NULL,
    content_id          uuid REFERENCES hub.content_item(content_id) ON DELETE SET NULL,
    observation_kind    text NOT NULL,
    external_ref        text,
    run_id              uuid REFERENCES orchestration.run(run_id) ON DELETE SET NULL,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX discovery_observation_run_idx
    ON region_talk.discovery_observation (run_id, observed_at DESC);
CREATE TRIGGER discovery_observation_append_only
BEFORE UPDATE OR DELETE ON region_talk.discovery_observation
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE region_talk.external_publication_source (
    external_publication_source_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           uuid NOT NULL REFERENCES region_talk.source(source_id) ON DELETE CASCADE,
    registry_key        text NOT NULL UNIQUE,
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'paused', 'excluded', 'unknown')),
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER external_publication_source_set_updated_at
BEFORE UPDATE ON region_talk.external_publication_source
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE region_talk.external_publication_intake (
    external_publication_intake_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    external_publication_source_id uuid REFERENCES region_talk.external_publication_source(external_publication_source_id) ON DELETE SET NULL,
    content_id          uuid REFERENCES hub.content_item(content_id) ON DELETE SET NULL,
    intake_key          text NOT NULL UNIQUE,
    exact_url           text NOT NULL,
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'accepted', 'rejected', 'duplicate', 'quarantined')),
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER external_publication_intake_set_updated_at
BEFORE UPDATE ON region_talk.external_publication_intake
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

-- The admission order is immutable. Priority/readiness may change without rewriting history.
CREATE SEQUENCE region_talk.source_queue_seq AS bigint START WITH 1 INCREMENT BY 1 NO CYCLE;
CREATE TABLE region_talk.source_work_projection (
    source_id           uuid NOT NULL REFERENCES region_talk.source(source_id) ON DELETE CASCADE,
    work_item_id        uuid NOT NULL REFERENCES orchestration.work_item(work_item_id) ON DELETE CASCADE,
    queue_seq           bigint NOT NULL DEFAULT nextval('region_talk.source_queue_seq'),
    priority_lane       text NOT NULL DEFAULT 'normal'
                        CHECK (priority_lane IN ('head_repair', 'exact', 'cached', 'resolve', 'normal', 'exploration')),
    priority_score      numeric NOT NULL DEFAULT 0,
    priority_reason     text,
    readiness_state     text NOT NULL DEFAULT 'unknown'
                        CHECK (readiness_state IN (
                            'actionable_cached', 'needs_entity_resolve', 'head_repair', 'cooldown',
                            'retry', 'scan_due', 'access_denied', 'not_found', 'terminal', 'unknown'
                        )),
    admitted_at         timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, work_item_id),
    UNIQUE (queue_seq)
);
ALTER SEQUENCE region_talk.source_queue_seq OWNED BY region_talk.source_work_projection.queue_seq;
CREATE INDEX source_work_scheduler_idx
    ON region_talk.source_work_projection (
        readiness_state, priority_lane, priority_score DESC, queue_seq
    );
CREATE TRIGGER source_work_projection_set_updated_at
BEFORE UPDATE ON region_talk.source_work_projection
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE OR REPLACE FUNCTION region_talk.reject_source_queue_seq_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.queue_seq IS DISTINCT FROM OLD.queue_seq THEN
        RAISE EXCEPTION 'region_talk.source_work_projection.queue_seq is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER source_work_projection_queue_seq_immutable
BEFORE UPDATE OF queue_seq ON region_talk.source_work_projection
FOR EACH ROW EXECUTE FUNCTION region_talk.reject_source_queue_seq_change();
