ALTER TABLE checkpoint_blob_publications
ADD COLUMN authority_kind TEXT NOT NULL DEFAULT 'master'
CHECK (authority_kind IN ('master','acceptance'));

ALTER TABLE checkpoint_blob_publications
ADD COLUMN acceptance_scenario TEXT
CHECK (acceptance_scenario IS NULL OR acceptance_scenario IN ('FM05','FM14','FM15'));

ALTER TABLE checkpoint_blob_publications
ADD COLUMN verifier_evidence_json TEXT;

ALTER TABLE checkpoint_blob_publications
ADD COLUMN source_previous_checkpoint_id TEXT;
