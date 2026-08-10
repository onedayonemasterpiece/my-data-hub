# PR-A host evidence and first control-plane deployment gate

No deployment was authorized in PR-A. Read-only observation on DevCoveer found no
my-data-hub containers and no listeners on 5432/8080/8765; the legacy unit was disabled
and inactive. No INSTALL receipt or local PostgreSQL process existed, so no local master
migration was applied.

An unattached named Docker volume from a prior validation probe did exist. Read-only
inventory found zero entries; it is classified as empty validation residue, not initialized
PGDATA. It was not deleted blindly. Prepared releases, environment files and the disabled
unit are likewise non-runtime residue. Exact evidence is in
[`evidence/2026-08-10-pr-a-host.json`](evidence/2026-08-10-pr-a-host.json).

Future first deployment may install only the lightweight control plane after separate owner
approval. It must prove DB-free environment, `master=ABSENT` readiness, loopback listener,
autostart of only the control process and fail-closed data methods. Master Notebook,
checkpoints, DNS/VPN/443 and remote MCP are later gates.
