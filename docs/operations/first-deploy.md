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

Future first deployment may install only the lightweight DB-free stack after all operational
gates pass. The one enabled user unit reconciles the control plane, remote MCP resource
server and OAuth authorization server through the explicit `remote-mcp` profile. It must
prove `master=ABSENT` readiness, the three loopback listeners (`8080`, `8765`, `8780`),
process-kill recovery, login-independent reboot recovery and fail-closed data methods.

The installer requires the exact approved merge commit, pre-enabled user lingering,
private split provider/MCP/OAuth environments, a private OAuth signing key, a bounded
overlap public JWKS file, master TLS CA
and bounded release-owned master assets. It rejects PostgreSQL data-plane configuration and
does not create PGDATA, a PostgreSQL volume or a database service.

Master Notebook/checkpoint acceptance remains a separate prerequisite. The dedicated Yandex DNS/TLS edge is already provisioned, but its MCP and OAuth routes last returned `502` because the reviewed application stack and owner secrets are not installed. This branch has not run INSTALL or changed any external service.
