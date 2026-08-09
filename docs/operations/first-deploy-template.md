# First devstand verification receipt

> Copy this file to `docs/operations/first-deploy.md` or to a private evidence bundle.
> Record observed values only. Do not include credentials, tokens, connection strings or
> private data.

## Receipt identity

```text
receipt_version:
environment: devstand
operator:
started_at:
finished_at:
repository:
commit:
branch:
working_tree_clean:
workflow_run_or_session_id:
```

## Host and runtime

```text
os/kernel:
docker_version:
compose_version:
python_version:
postgres_image_and_digest:
postgres_version:
pgvector_version:
locale_collation:
instance_id:
```

## Service matrix

| Service | Observed state | Restart policy | Bind/listener | Health result | Evidence |
|---|---|---|---|---|---|
| PostgreSQL | | | | | |
| API/intake | | | | | |
| Orchestrator | | | | | |
| MCP upstream | | | | | |
| TLS/OAuth edge | | | | | |
| Backup job/timer | | | | | |

## Safety gates

| Gate | Expected | Observed | PASS/FAIL |
|---|---:|---:|---:|
| scheduler enabled | false | | |
| production publication enabled | false | | |
| remote MCP enabled before OAuth test | false | | |
| MCP write/operator enabled | false | | |
| Region Talk pipeline | paused | | |

## Database migration and roles

```text
repository_migrations:
applied_migrations:
canonical_schema_revision:
first_migrate_result:
second_migrate_noop_result:
clean_database_verification:
upgrade_path_verification:
```

| Role | Login? | Owner? | Intended grants | Negative probes | Outcome |
|---|---:|---:|---|---|---|
| owner/migrator | | | | | |
| app | | | | | |
| orchestrator | | | | | |
| connector | | | | | |
| MCP reader | | | | | |
| MCP editor | | | | | |
| migration operator | | | | | |
| backup | | | | | |
| monitor | | | | | |

Confirm remote roles do not have superuser, `BYPASSRLS`, `CREATEDB`, `CREATEROLE`,
replication, ownership, extension, server file/program or protected-table rights.

## Network and endpoint

```text
public_hostname: mcp-datahub.kenigevents.ru
public_ports:
private_ports:
dns_record_identity:
certificate_identity_and_fingerprint:
certificate_expiry_monitoring:
reverse_proxy_or_alb:
upstream_binding:
```

Negative checks:

- [ ] PostgreSQL not public
- [ ] API/internal MCP port not public
- [ ] plaintext HTTP redirected/rejected
- [ ] wrong Host rejected
- [ ] wrong Origin rejected
- [ ] missing/expired/wrong-audience OAuth rejected
- [ ] revoked client/token rejected

## Backup and isolated restore

```text
backup_id:
canonical_revision:
local_generation_locator:
off_host_provider_and_resource_class:
encrypted_artifact_sha256:
provider_readback_sha256_match:
backup_started_at:
backup_finished_at:
restore_target_identity:
restore_started_at:
restore_finished_at:
restore_db_verify_result:
object_count_checks:
invariant_checks:
restore_target_destroyed:
```

## Automated workflow evidence

| Workflow | Run ID/link | Commit | Outcome | Receipt/artifact hash |
|---|---|---|---|---|
| PR/CI | | | | |
| post-deploy | | | | |
| nightly | | | | |
| restore drill | | | | |
| Kaggle canary | | | | |

## Synthetic connector evidence

```text
connector_id:
batch_id:
idempotency_key:
payload_sha256:
first_accept_receipt:
canonical_commit_receipt:
exact_replay_receipt:
conflicting_replay_test:
platform_outage_spool_test:
MCP_read_trace:
```

## Kaggle control evidence

```text
inventory_complete:
resources_by_control_class:
protected_notebook_mutation_denied:
protected_dataset_download_version_delete_denied:
MCP_managed_private_dataset_lifecycle:
MCP_managed_notebook_lifecycle:
public_dataset_creation_absent:
exchange_package_test:
cleanup_complete:
```

## Database operator disposable-schema evidence

```text
reader_positive_test:
reader_write_denial:
editor_preview_receipt:
editor_apply_receipt:
DDL_role_secret_server_file_denials:
row_byte_timeout_caps:
stale_or_forged_receipt_denial:
backup_gate_test:
rollback_atomicity_test:
```

## Final state and blockers

```text
scheduler_enabled:
production_publication_enabled:
remote_semantic_read_enabled:
remote_kaggle_write_enabled:
remote_db_reader_enabled:
remote_db_editor_enabled:
region_talk_pipeline_state:
region_talk_inventory_allowed:
remaining_blockers:
operator_decision:
```
