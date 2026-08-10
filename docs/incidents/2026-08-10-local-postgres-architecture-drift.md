# Incident: local PostgreSQL architecture drift

- Date: 2026-08-10
- Status: corrected by PR-A
- Affected baseline: `bcc02df1f980ac6eefcd305d71cef94817033d70`
- Owner decision: writable PostgreSQL-primary runs only in the Kaggle master Notebook.

## Incident

The exact imported research says that one writable PostgreSQL-primary runs in a Kaggle
Notebook, the devstand is the stable MCP/control plane, and approved internal clients use
a direct data plane after service resolution. Derived ADRs and deployment work inverted
that topology into a normally running PostgreSQL/PGDATA on the devstand without owner
approval. PRs #2 and #3 prepared that incorrect path; the destructive INSTALL token was
not executed.

This record preserves the mistake rather than rewriting history. ADR-0009 and dependent
local-runtime claims are superseded by ADR-0016. The exact imported source file is not
modified.

## Host observation before correction

At `2026-08-10T07:25Z`:

- no `my-data-hub` container existed and no listener used ports 5432, 8080 or 8765;
- the generated `my-data-hub-compose.service` was disabled and inactive;
- no install receipt existed and no local master migrations could have run without a
  PostgreSQL process;
- prepared release directories, generated secret/environment files and a disabled unit
  existed and were classified as preparation residue;
- the named Docker volume `my-data-hub-postgres-data` existed, had no attached container,
  and a read-only inventory found zero entries. It is an empty validation residue, not
  initialized PGDATA. It was not deleted blindly.

The last point means this project does **not** claim that no volume object was ever
created. It claims, with evidence, that no local PostgreSQL/PGDATA runtime was initialized.

## Immediate containment

- owner rejected `INSTALL_MY_DATA_HUB_SAME_HOST`;
- the old installer now exits before prerequisite or filesystem operations;
- the production local database Compose/systemd/workflow paths were removed;
- only an explicitly disposable, tmpfs-backed PostgreSQL integration profile remains;
- the replacement production profile is a database-free control/status process which is
  healthy when `master_state=ABSENT` and fails data operations closed.
