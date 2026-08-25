CREATE TABLE kaggle_researches (
    research_id TEXT PRIMARY KEY,
    owner_subject TEXT NOT NULL,
    alias TEXT,
    title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
    goal TEXT NOT NULL CHECK (length(goal) BETWEEN 1 AND 4000),
    state TEXT NOT NULL CHECK (state IN ('DRAFT','READY','RUNNING','REVIEW_REQUIRED','COMPLETED','ARCHIVED')),
    primary_dataset_ref TEXT NOT NULL,
    notebook_ref TEXT,
    current_revision_id TEXT,
    active_run_id TEXT,
    last_completed_run_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX kaggle_researches_owner_alias_uq
ON kaggle_researches(owner_subject, alias) WHERE alias IS NOT NULL;
CREATE INDEX kaggle_researches_owner_updated_idx
ON kaggle_researches(owner_subject, updated_at DESC);
CREATE INDEX kaggle_researches_owner_dataset_idx
ON kaggle_researches(owner_subject, primary_dataset_ref);

CREATE TABLE kaggle_notebook_revisions (
    revision_id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL REFERENCES kaggle_researches(research_id),
    revision_no INTEGER NOT NULL CHECK (revision_no >= 1),
    parent_revision_id TEXT REFERENCES kaggle_notebook_revisions(revision_id),
    state TEXT NOT NULL CHECK (state IN ('DRAFT','FROZEN','SUBMITTED')),
    code_file TEXT NOT NULL,
    kernel_type TEXT NOT NULL CHECK (kernel_type IN ('script','notebook')),
    language TEXT NOT NULL CHECK (language = 'python'),
    source_utf8 TEXT NOT NULL,
    source_sha256 TEXT NOT NULL CHECK (length(source_sha256) = 64),
    runtime_json TEXT NOT NULL,
    inputs_json TEXT NOT NULL,
    inputs_sha256 TEXT NOT NULL CHECK (length(inputs_sha256) = 64),
    provider_source_version INTEGER CHECK (provider_source_version IS NULL OR provider_source_version >= 1),
    created_at TEXT NOT NULL,
    frozen_at TEXT,
    UNIQUE(research_id, revision_no)
);

CREATE TRIGGER kaggle_revisions_frozen_immutable
BEFORE UPDATE ON kaggle_notebook_revisions
WHEN OLD.state IN ('FROZEN','SUBMITTED') AND (
    NEW.research_id != OLD.research_id OR
    NEW.revision_no != OLD.revision_no OR
    coalesce(NEW.parent_revision_id,'') != coalesce(OLD.parent_revision_id,'') OR
    NEW.code_file != OLD.code_file OR NEW.kernel_type != OLD.kernel_type OR
    NEW.language != OLD.language OR NEW.source_utf8 != OLD.source_utf8 OR
    NEW.source_sha256 != OLD.source_sha256 OR NEW.runtime_json != OLD.runtime_json OR
    NEW.inputs_json != OLD.inputs_json OR NEW.inputs_sha256 != OLD.inputs_sha256 OR
    NEW.created_at != OLD.created_at OR NEW.frozen_at IS NOT OLD.frozen_at OR
    NOT (
        NEW.state = OLD.state OR
        (OLD.state = 'FROZEN' AND NEW.state = 'SUBMITTED')
    ) OR
    NOT (
        NEW.provider_source_version IS OLD.provider_source_version OR
        (OLD.provider_source_version IS NULL AND NEW.provider_source_version >= 1)
    )
) BEGIN SELECT RAISE(ABORT, 'frozen Kaggle revision is immutable'); END;

CREATE TABLE kaggle_runs (
    run_id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL REFERENCES kaggle_researches(research_id),
    revision_id TEXT NOT NULL REFERENCES kaggle_notebook_revisions(revision_id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    retry_of_run_id TEXT REFERENCES kaggle_runs(run_id),
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    effect_id TEXT REFERENCES provider_effect_intents(effect_id),
    state TEXT NOT NULL CHECK (state IN (
        'PREPARED','SUBMITTING','SUBMISSION_UNKNOWN','QUEUED','RUNNING','COLLECTING','SUCCEEDED','FAILED'
    )),
    provider_run_ref TEXT,
    provider_kernel_id TEXT,
    provider_source_version INTEGER CHECK (provider_source_version IS NULL OR provider_source_version >= 1),
    provider_source_sha256 TEXT CHECK (provider_source_sha256 IS NULL OR length(provider_source_sha256) = 64),
    last_provider_status TEXT,
    failure_summary TEXT CHECK (failure_summary IS NULL OR length(failure_summary) <= 2000),
    next_poll_at TEXT,
    poll_attempts INTEGER NOT NULL DEFAULT 0 CHECK (poll_attempts BETWEEN 0 AND 10000),
    output_manifest_sha256 TEXT CHECK (output_manifest_sha256 IS NULL OR length(output_manifest_sha256) = 64),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE(research_id, attempt_no)
);

CREATE UNIQUE INDEX kaggle_runs_initial_revision_uq
ON kaggle_runs(revision_id) WHERE retry_of_run_id IS NULL;
CREATE UNIQUE INDEX kaggle_runs_retry_once_uq
ON kaggle_runs(retry_of_run_id) WHERE retry_of_run_id IS NOT NULL;
CREATE UNIQUE INDEX kaggle_runs_one_active_research_uq
ON kaggle_runs(research_id) WHERE state IN (
    'PREPARED','SUBMITTING','SUBMISSION_UNKNOWN','QUEUED','RUNNING','COLLECTING'
);
CREATE INDEX kaggle_runs_due_idx ON kaggle_runs(next_poll_at, state);
CREATE INDEX kaggle_runs_revision_idx ON kaggle_runs(revision_id, attempt_no);

CREATE TRIGGER kaggle_runs_terminal_immutable
BEFORE UPDATE ON kaggle_runs
WHEN OLD.state IN ('SUCCEEDED','FAILED')
BEGIN SELECT RAISE(ABORT, 'terminal Kaggle run is immutable'); END;

CREATE TABLE kaggle_artifacts (
    artifact_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES kaggle_runs(run_id),
    path TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN (
        'summary','metrics','manifest','provenance','diagnostics','log','table','figure','other'
    )),
    media_type TEXT NOT NULL,
    byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
    sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
    storage_mode TEXT NOT NULL CHECK (storage_mode IN ('kaggle','local_cache')),
    cache_relpath TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, path),
    CHECK (
        (storage_mode = 'kaggle' AND cache_relpath IS NULL) OR
        (storage_mode = 'local_cache' AND cache_relpath IS NOT NULL)
    )
);

CREATE TRIGGER kaggle_researches_no_delete
BEFORE DELETE ON kaggle_researches BEGIN SELECT RAISE(ABORT, 'Kaggle research history is append-preserved'); END;
CREATE TRIGGER kaggle_revisions_no_delete
BEFORE DELETE ON kaggle_notebook_revisions BEGIN SELECT RAISE(ABORT, 'Kaggle revision history is append-preserved'); END;
CREATE TRIGGER kaggle_runs_no_delete
BEFORE DELETE ON kaggle_runs BEGIN SELECT RAISE(ABORT, 'Kaggle run history is append-preserved'); END;
CREATE TRIGGER kaggle_artifacts_no_update
BEFORE UPDATE ON kaggle_artifacts BEGIN SELECT RAISE(ABORT, 'Kaggle artifact metadata is immutable'); END;
CREATE TRIGGER kaggle_artifacts_no_delete
BEFORE DELETE ON kaggle_artifacts BEGIN SELECT RAISE(ABORT, 'Kaggle artifact metadata is append-preserved'); END;
