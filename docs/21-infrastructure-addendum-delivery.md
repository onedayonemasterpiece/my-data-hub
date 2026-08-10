# Infrastructure addendum delivery — invalidated and reset

The earlier R1 same-host database direction was invalidated by the 2026-08-10 architecture
drift incident. PR-A restores exact-source topology: lightweight devstand control plane,
one writable PostgreSQL-primary in a Kaggle master Notebook and private verified checkpoint
datasets.

Preserved schema/security/connector/MCP work is rebound to that master. Deployment evidence
in PR-A proves containment only; it is not proof of a deployed master or remote MCP.
