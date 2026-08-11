# Secrets and configuration

## Devstand

The lightweight control plane receives exactly one automated Kaggle provider
credential supported by the pinned official SDK: either `KAGGLE_API_TOKEN`, a
private access-token file, or the established `KAGGLE_USERNAME`/`KAGGLE_KEY`
profile used by events-bot/CherryFlash. OAuth signing and owner-auth material,
and operational-ledger encryption keys are supplied only as needed. Per-attempt
callback tokens are randomly generated and delivered only through exact private
status Datasets; no derivation root exists. The devstand receives no master PostgreSQL URL, owner/migrator
password, connector DB password or checkpoint package. Runtime events and audit
records contain identities/locators, never credentials.

## Kaggle master Notebook

Master-only data-plane secrets use per-attempt protected status input or short-lived
epoch-bound issuance: YDB viewer access for the bounded import, PostgreSQL restricted-role
credentials and service-announcement authentication. Control generates the PostgreSQL
server certificate/private key per operation and epoch. The raw key exists only in the
exact private status Dataset and fixed mode-`0600` Notebook path; the ledger stores its
hash plus the public certificate, and remote MCP observes atomic public-certificate
rotation through a shared read-only directory. The public tunnel known-host key is a
reviewed hashed release asset, not a secret.
The Kaggle account credential remains solely in the control-owned provider
adapter and is not copied into the Notebook. No provider credential enters Notebook
source, any Dataset, callback, log, or receipt. The only secret-bearing Dataset is the
task-owned `ORCHESTRATOR_PROTECTED` status Dataset containing the one-time callback token
and operation-bound TLS private key; it is deleted after terminal reconciliation. The DB
write gate requires the current epoch and lease.

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

- Kaggle control credential: keep exactly one complete control-side mode
  (`KAGGLE_API_TOKEN`, private access-token file, or
  `KAGGLE_USERNAME`/`KAGGLE_KEY`). Legacy values remain in the control process
  and are never launch bindings. No separate checkpoint User Secret, interactive browser
  session, or refresh-cookie state is admitted. Until the single central adapter has a
  proven provider-side checkpoint upload/copy path, production master admission returns
  `CENTRAL_CHECKPOINT_UPLOAD_PATH_UNAVAILABLE` before provider mutation.
- runtime callback authority: each attempt has an independent random token. The
  control ledger stores only its SHA-256; terminal cleanup revokes the hash and
  deletes the exact protected status Dataset containing the raw value. There is
  no shared root to rotate or reconstruct after a crash.
- OAuth signing key: rotate by publishing the new JWK before issuance and retaining the old
  public key through the maximum token lifetime. Private key material never enters the MCP
  resource server response.
- epoch database credentials: are generated for one ACTIVE master epoch, written only to
  the bounded credential handoff directory, and rejected after lease/fence transition.

Never print secret values in an operator command, Actions artifact or final report. A
retrieval instruction may name the secret-store entry and command, but the command output
must remain owner-only.
