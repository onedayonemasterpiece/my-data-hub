# Secrets and configuration

## Devstand

The lightweight control plane receives provider adapter credentials, OAuth policy material
and operational-ledger encryption keys only as needed. It receives no master PostgreSQL URL,
owner/migrator password, connector DB password or checkpoint decryption secret. Runtime
events and audit records contain identities/locators, never credentials.

## Kaggle master Notebook

Master-only secrets are supplied through Kaggle User Secrets or short-lived epoch-bound
issuance: restore/checkpoint access, PostgreSQL restricted-role credentials and service
announcement authentication. They never enter notebook source or Dataset contents. The DB
write gate requires the current epoch and lease.

## Separation

- orchestrator, provider canary and MCP-managed sandbox use distinct Kaggle principals;
- connectors, MCP readers/editors, committer, checkpoint agent and migrator use distinct
  restricted roles inside the master;
- remote MCP never returns provider/database credentials;
- checkpoint encryption keys travel separately from private Dataset artifacts;
- publication secrets are absent until an explicit later release.

Root `.env.example` is disposable integration-test input only. The production
`compose.control-plane.yaml` has no database URL or secret file. DNS/OAuth edge secrets and
remote MCP writes are outside PR-A.
