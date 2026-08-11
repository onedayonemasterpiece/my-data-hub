ALTER TABLE master_acceptance_commands ADD COLUMN claim_authority TEXT
    CHECK (claim_authority IN ('runtime','owner_host'));
ALTER TABLE master_acceptance_commands ADD COLUMN claimed_principal_id TEXT;
ALTER TABLE master_acceptance_commands ADD COLUMN claimed_client_id TEXT;

-- A command claimed before this append-only migration could only have been
-- claimed by the exact runtime-token path.
UPDATE master_acceptance_commands
SET claim_authority='runtime'
WHERE state='CLAIMED' AND claim_authority IS NULL;

CREATE INDEX master_acceptance_host_claim_idx
ON master_acceptance_commands(claim_authority,state,task_id);
