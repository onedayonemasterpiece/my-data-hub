# First lightweight control-plane verification receipt

Record observed values only; never include credentials.

```yaml
receipt_version:
environment: devstand-control-plane
commit:
working_tree_clean:
started_at:
finished_at:
control_service:
  image_id:
  listener: 127.0.0.1:8080
  health_ready:
  master_state: ABSENT
  data_plane_ready: false
  restart_policy:
local_master_absence:
  postgresql_process: false
  listener_5432: false
  pgdata_initialized: false
  production_database_url_present: false
safety:
  scheduler: false
  publication: false
  remote_mcp: false
  remote_mcp_writes: false
  region_talk: paused
residue:
  prepared_releases:
  disabled_legacy_unit:
  empty_validation_volume:
blockers:
```

Master Notebook, private checkpoint, provider, DNS/TLS/OAuth and remote MCP receipts are
separate later-phase artifacts. Never fill them with planned values.
