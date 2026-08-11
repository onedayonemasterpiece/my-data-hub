# Secrets and configuration

## Devstand

The lightweight control plane receives the modern Kaggle provider token, OAuth signing and
owner-auth material, the runtime-token derivation root and operational-ledger encryption
keys only as needed. It receives no master PostgreSQL URL, owner/migrator password,
connector DB password or checkpoint package. Runtime events and audit records contain
identities/locators, never credentials.

## Kaggle master Notebook

Master-only secrets are supplied through Kaggle User Secrets or short-lived epoch-bound
issuance: the modern provider token used by the single adapter implementation inside the
Notebook, YDB viewer access for the bounded import, PostgreSQL restricted-role credentials
and service-announcement authentication. They never enter notebook source or Dataset
contents. The DB write gate requires the current epoch and lease.

## Separation

- orchestrator, provider canary and MCP-managed sandbox use distinct Kaggle principals;
- connectors, MCP readers/editors, committer, checkpoint agent and migrator use distinct
  restricted roles inside the master;
- remote MCP never returns provider/database credentials;
- checkpoint/provider credentials travel separately from private Dataset artifacts;
- publication secrets are absent until an explicit later release.

Root `.env.example` is disposable integration-test input only. The production
`compose.control-plane.yaml` has no database URL. Production secrets belong in a
root-readable service environment or secret store, never in Git, release assets, Notebook
source, Dataset files, callback bodies or receipts. The current implementation PR has not
installed DNS/OAuth edge secrets and has not enabled remote MCP writes.

## Rotation and retrieval boundary

- `KAGGLE_API_TOKEN`: issue with `kaggle auth login` or the provider's supported token
  management UI, store only in the service secret store and the exact Kaggle User Secret
  binding used by protected runtime Notebooks. Re-run the private Notebook exact-read
  canary after rotation.
- runtime-token derivation root: rotate only with a controlled drain; outstanding attempts
  use tokens derived from the old root and otherwise fail closed.
- OAuth signing key: rotate by publishing the new JWK before issuance and retaining the old
  public key through the maximum token lifetime. Private key material never enters the MCP
  resource server response.
- epoch database credentials: are generated for one ACTIVE master epoch, written only to
  the bounded credential handoff directory, and rejected after lease/fence transition.

Never print secret values in an operator command, Actions artifact or final report. A
retrieval instruction may name the secret-store entry and command, but the command output
must remain owner-only.
