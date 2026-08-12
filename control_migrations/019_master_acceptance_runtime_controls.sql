CREATE TABLE master_acceptance_runtime_controls (
    task_id TEXT PRIMARY KEY REFERENCES master_acceptance_tasks(task_id) ON DELETE CASCADE,
    command_id TEXT NOT NULL REFERENCES master_acceptance_commands(command_id) ON DELETE CASCADE,
    command_sha256 TEXT NOT NULL CHECK (length(command_sha256)=64),
    scenario_id TEXT NOT NULL CHECK (scenario_id IN ('FM08','FM10','FM24')),
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    master_instance_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    callback_state TEXT NOT NULL DEFAULT 'DISARMED'
        CHECK (callback_state IN ('DISARMED','ARMED','CAPTURED','REPLAYED')),
    callback_event_id TEXT,
    callback_body_sha256 TEXT CHECK (callback_body_sha256 IS NULL OR length(callback_body_sha256)=64),
    restart_from_id TEXT,
    restart_to_id TEXT,
    renewal_suspended INTEGER NOT NULL DEFAULT 0 CHECK (renewal_suspended IN (0,1)),
    renewal_acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (renewal_acknowledged IN (0,1)),
    soak_requested_step INTEGER NOT NULL DEFAULT 0 CHECK (soak_requested_step BETWEEN 0 AND 12),
    soak_completed_step INTEGER NOT NULL DEFAULT 0 CHECK (soak_completed_step BETWEEN 0 AND 12),
    updated_at TEXT NOT NULL,
    CHECK (soak_completed_step <= soak_requested_step),
    CHECK ((callback_state IN ('DISARMED','ARMED') AND callback_event_id IS NULL AND callback_body_sha256 IS NULL)
        OR (callback_state IN ('CAPTURED','REPLAYED') AND callback_event_id IS NOT NULL AND callback_body_sha256 IS NOT NULL))
);

CREATE UNIQUE INDEX master_acceptance_runtime_controls_binding_idx
ON master_acceptance_runtime_controls(run_id,attempt_id,epoch)
WHERE callback_state='ARMED' OR renewal_suspended=1 OR soak_requested_step>soak_completed_step;
