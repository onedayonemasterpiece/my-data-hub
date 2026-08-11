DROP TRIGGER runtime_events_no_delete;

CREATE TABLE retention_runs (
    retention_run_id TEXT PRIMARY KEY,
    nonterminal_before TEXT NOT NULL,
    terminal_before TEXT,
    events_deleted INTEGER NOT NULL CHECK (events_deleted >= 0),
    dedupe_keys_deleted INTEGER NOT NULL CHECK (dedupe_keys_deleted >= 0),
    recorded_at TEXT NOT NULL
);

CREATE TRIGGER retention_runs_no_update
BEFORE UPDATE ON retention_runs BEGIN SELECT RAISE(ABORT, 'retention_runs is append-only'); END;
CREATE TRIGGER retention_runs_no_delete
BEFORE DELETE ON retention_runs BEGIN SELECT RAISE(ABORT, 'retention_runs is append-only'); END;
