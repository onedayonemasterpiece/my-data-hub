# Remote MCP endpoint

Status: `TARGET CONTRACT / FROZEN IN PR-A`

The eventual stable MCP endpoint runs on the devstand control plane and resolves the latest
ACTIVE Kaggle master. It never binds to a local canonical database. Cold start returns a
durable operation/status; receipts bind instance, epoch and canonical revision.

TLS/OAuth/DNS/VPN/443 implementation and all remote writes are deferred. PR-A does not
change the edge and does not claim a public endpoint is available.
