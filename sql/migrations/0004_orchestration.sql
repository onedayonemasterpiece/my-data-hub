-- Pipeline registry, durable work queue, leases, runs and immutable worker-result intake.
CREATE TABLE orchestration.pipeline (
    pipeline_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workload            text NOT NULL,
    name                text NOT NULL,
    version             text NOT NULL,
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'paused', 'retired')),
    definition          jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workload, name, version)
);
CREATE TRIGGER pipeline_set_updated_at
BEFORE UPDATE ON orchestration.pipeline
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE orchestration.pipeline_stage (
    stage_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id         uuid NOT NULL REFERENCES orchestration.pipeline(pipeline_id) ON DELETE CASCADE,
    stage_key           text NOT NULL,
    stage_version       text NOT NULL,
    compute_lane        text NOT NULL,
    priority            integer NOT NULL DEFAULT 100,
    max_attempts        integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 100),
    timeout_seconds     integer NOT NULL DEFAULT 600 CHECK (timeout_seconds BETWEEN 1 AND 86400),
    contract            jsonb NOT NULL DEFAULT '{}'::jsonb,
    enabled             boolean NOT NULL DEFAULT true,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, stage_key, stage_version)
);
CREATE INDEX pipeline_stage_lookup_idx
    ON orchestration.pipeline_stage (pipeline_id, stage_key, enabled);
CREATE TRIGGER pipeline_stage_set_updated_at
BEFORE UPDATE ON orchestration.pipeline_stage
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE orchestration.run (
    run_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id         uuid NOT NULL REFERENCES orchestration.pipeline(pipeline_id) ON DELETE RESTRICT,
    project_id          uuid REFERENCES hub.project(project_id) ON DELETE SET NULL,
    run_kind            text NOT NULL DEFAULT 'scheduled',
    status              text NOT NULL DEFAULT 'planned'
                        CHECK (status IN ('planned', 'running', 'succeeded', 'partial', 'failed', 'cancelled', 'quarantined')),
    canonical_revision  bigint NOT NULL CHECK (canonical_revision >= 0),
    trigger             jsonb NOT NULL DEFAULT '{}'::jsonb,
    plan                jsonb NOT NULL DEFAULT '{}'::jsonb,
    summary             jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at          timestamptz,
    completed_at        timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orchestration_run_pipeline_time_idx
    ON orchestration.run (pipeline_id, created_at DESC);

CREATE TABLE orchestration.stage_run (
    stage_run_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id              uuid NOT NULL REFERENCES orchestration.run(run_id) ON DELETE CASCADE,
    stage_id            uuid NOT NULL REFERENCES orchestration.pipeline_stage(stage_id) ON DELETE RESTRICT,
    status              text NOT NULL DEFAULT 'planned'
                        CHECK (status IN ('planned', 'dispatched', 'running', 'succeeded', 'partial', 'failed', 'cancelled', 'quarantined')),
    input_manifest_sha256 text,
    input_artifact_locator text,
    result_artifact_locator text,
    attempt_count       integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    metrics             jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at          timestamptz,
    completed_at        timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, stage_id)
);

CREATE SEQUENCE orchestration.work_queue_seq AS bigint START WITH 1 INCREMENT BY 1 NO CYCLE;

CREATE TABLE orchestration.work_item (
    work_item_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_seq           bigint NOT NULL DEFAULT nextval('orchestration.work_queue_seq'),
    pipeline_id         uuid NOT NULL REFERENCES orchestration.pipeline(pipeline_id) ON DELETE RESTRICT,
    stage_id            uuid NOT NULL REFERENCES orchestration.pipeline_stage(stage_id) ON DELETE RESTRICT,
    project_id          uuid REFERENCES hub.project(project_id) ON DELETE CASCADE,
    subject_type        text NOT NULL,
    subject_id          uuid NOT NULL,
    dedupe_key          text NOT NULL,
    input_fingerprint   text NOT NULL,
    priority            integer NOT NULL DEFAULT 100,
    expected_revision   bigint CHECK (expected_revision IS NULL OR expected_revision >= 0),
    payload             jsonb NOT NULL DEFAULT '{}'::jsonb,
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN (
                            'pending', 'leased', 'running', 'succeeded',
                            'failed_retryable', 'failed_terminal', 'quarantined', 'cancelled'
                        )),
    available_at        timestamptz NOT NULL DEFAULT now(),
    attempt_count       integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner         text,
    lease_token         uuid,
    lease_expires_at    timestamptz,
    last_error          jsonb,
    result_ref          jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, stage_id, dedupe_key),
    UNIQUE (queue_seq)
);
ALTER SEQUENCE orchestration.work_queue_seq OWNED BY orchestration.work_item.queue_seq;
CREATE INDEX work_item_claim_idx
    ON orchestration.work_item (stage_id, priority, available_at, queue_seq)
    WHERE status IN ('pending', 'failed_retryable');
CREATE INDEX work_item_project_stage_status_idx
    ON orchestration.work_item (project_id, stage_id, status);
CREATE INDEX work_item_subject_idx
    ON orchestration.work_item (subject_type, subject_id, created_at DESC);
CREATE TRIGGER work_item_set_updated_at
BEFORE UPDATE ON orchestration.work_item
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE OR REPLACE FUNCTION orchestration.reject_queue_seq_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.queue_seq IS DISTINCT FROM OLD.queue_seq THEN
        RAISE EXCEPTION 'orchestration.work_item.queue_seq is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER work_item_queue_seq_immutable
BEFORE UPDATE OF queue_seq ON orchestration.work_item
FOR EACH ROW EXECUTE FUNCTION orchestration.reject_queue_seq_change();

CREATE TABLE orchestration.work_item_dependency (
    work_item_id            uuid NOT NULL REFERENCES orchestration.work_item(work_item_id) ON DELETE CASCADE,
    depends_on_work_item_id uuid NOT NULL REFERENCES orchestration.work_item(work_item_id) ON DELETE CASCADE,
    dependency_kind         text NOT NULL DEFAULT 'success',
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (work_item_id, depends_on_work_item_id),
    CHECK (work_item_id <> depends_on_work_item_id)
);

CREATE TABLE orchestration.work_item_event (
    event_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    work_item_id        uuid NOT NULL REFERENCES orchestration.work_item(work_item_id) ON DELETE CASCADE,
    stage_run_id        uuid REFERENCES orchestration.stage_run(stage_run_id) ON DELETE SET NULL,
    event_kind          text NOT NULL,
    from_status         text,
    to_status           text,
    actor_kind          text NOT NULL,
    actor_ref           text,
    reason              text,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX work_item_event_work_idx
    ON orchestration.work_item_event (work_item_id, occurred_at);
CREATE TRIGGER work_item_event_append_only
BEFORE UPDATE OR DELETE ON orchestration.work_item_event
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE orchestration.worker_artifact (
    artifact_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    stage_run_id        uuid REFERENCES orchestration.stage_run(stage_run_id) ON DELETE SET NULL,
    work_item_id        uuid REFERENCES orchestration.work_item(work_item_id) ON DELETE SET NULL,
    artifact_kind       text NOT NULL,
    locator             text NOT NULL,
    sha256              text NOT NULL,
    byte_size           bigint CHECK (byte_size IS NULL OR byte_size >= 0),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (locator, sha256)
);

CREATE TABLE orchestration.worker_result_inbox (
    result_id               uuid PRIMARY KEY,
    run_id                  uuid NOT NULL REFERENCES orchestration.run(run_id) ON DELETE RESTRICT,
    stage_run_id            uuid REFERENCES orchestration.stage_run(stage_run_id) ON DELETE SET NULL,
    workload                text NOT NULL,
    stage_key               text NOT NULL,
    stage_contract_version  text NOT NULL,
    input_manifest_sha256   text NOT NULL CHECK (input_manifest_sha256 ~ '^[a-f0-9]{64}$'),
    result_sha256           text NOT NULL UNIQUE CHECK (result_sha256 ~ '^[a-f0-9]{64}$'),
    artifact_locator        text,
    byte_size               bigint NOT NULL CHECK (byte_size >= 0),
    producer                jsonb NOT NULL,
    result_status           text NOT NULL CHECK (result_status IN ('succeeded', 'partial', 'failed')),
    envelope                jsonb NOT NULL,
    intake_status           text NOT NULL DEFAULT 'received'
                            CHECK (intake_status IN (
                                'received', 'validated', 'applied', 'rejected', 'quarantined'
                            )),
    acceptance_receipt      jsonb NOT NULL DEFAULT '{}'::jsonb,
    validation_error        jsonb,
    received_at             timestamptz NOT NULL DEFAULT now(),
    applied_at              timestamptz,
    UNIQUE (run_id, stage_key, input_manifest_sha256, result_sha256),
    UNIQUE (stage_run_id, input_manifest_sha256)
);
CREATE INDEX worker_result_inbox_pending_idx
    ON orchestration.worker_result_inbox (received_at, result_id)
    WHERE intake_status IN ('received', 'validated');

CREATE OR REPLACE FUNCTION orchestration.reject_worker_result_payload_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.result_id IS DISTINCT FROM OLD.result_id
       OR NEW.run_id IS DISTINCT FROM OLD.run_id
       OR NEW.stage_run_id IS DISTINCT FROM OLD.stage_run_id
       OR NEW.workload IS DISTINCT FROM OLD.workload
       OR NEW.stage_key IS DISTINCT FROM OLD.stage_key
       OR NEW.stage_contract_version IS DISTINCT FROM OLD.stage_contract_version
       OR NEW.input_manifest_sha256 IS DISTINCT FROM OLD.input_manifest_sha256
       OR NEW.result_sha256 IS DISTINCT FROM OLD.result_sha256
       OR NEW.artifact_locator IS DISTINCT FROM OLD.artifact_locator
       OR NEW.byte_size IS DISTINCT FROM OLD.byte_size
       OR NEW.producer IS DISTINCT FROM OLD.producer
       OR NEW.result_status IS DISTINCT FROM OLD.result_status
       OR NEW.envelope IS DISTINCT FROM OLD.envelope
       OR NEW.received_at IS DISTINCT FROM OLD.received_at THEN
        RAISE EXCEPTION 'worker result payload is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER worker_result_payload_immutable
BEFORE UPDATE ON orchestration.worker_result_inbox
FOR EACH ROW EXECUTE FUNCTION orchestration.reject_worker_result_payload_change();

CREATE TABLE orchestration.batch (
    batch_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_kind          text NOT NULL,
    project_id          uuid REFERENCES hub.project(project_id) ON DELETE CASCADE,
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'succeeded', 'partial', 'failed', 'quarantined', 'cancelled')),
    total_items         integer NOT NULL DEFAULT 0 CHECK (total_items >= 0),
    completed_items     integer NOT NULL DEFAULT 0 CHECK (completed_items >= 0),
    failed_items        integer NOT NULL DEFAULT 0 CHECK (failed_items >= 0),
    quarantined_items   integer NOT NULL DEFAULT 0 CHECK (quarantined_items >= 0),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER orchestration_batch_set_updated_at
BEFORE UPDATE ON orchestration.batch
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();
