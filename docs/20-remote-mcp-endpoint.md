# Remote MCP endpoint

Status: `RESOURCE SERVER IMPLEMENTED / PUBLIC ENDPOINT NOT DEPLOYED`

The stable MCP endpoint is implemented for the devstand control plane and resolves the
latest ACTIVE Kaggle master through the durable registry and epoch-bound credential broker.
It never binds to a local canonical database. Cold start returns a durable operation/status;
receipts bind instance, epoch and canonical revision. Tests cover Streamable HTTP discovery,
read-only tool visibility, revoked-token denial and `master_state=ABSENT`.

An authorization-code/PKCE OAuth server and resource-server validators exist in code, but
the external owner authenticator, durable secret material, DNS, certificate and isolated
Yandex Cloud edge are not deployed. The current host's unrelated VPN owns ports 80/443 and
must not be modified. Remote writes remain disabled until preview/apply/fencing/checkpoint
gates pass in the real provider matrix. No public endpoint is claimed.
