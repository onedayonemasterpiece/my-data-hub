# Stable MCP edge — deferred

DNS, TLS, VPN/Xray, port 443 and public MCP changes are explicitly outside PR-A. The stable
MCP endpoint remains a target of the lightweight devstand control plane, but no edge config
or remote write path may be activated before dynamic ACTIVE-master resolution and a later
owner-approved change window.
