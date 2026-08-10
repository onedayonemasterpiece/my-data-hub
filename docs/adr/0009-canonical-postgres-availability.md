# ADR-0009: Historical local PostgreSQL availability decision

- Status: **SUPERSEDED_BY_ARCHITECTURE_RESET / INVALID DUE TO ARCHITECTURE DRIFT**
- Date: 2026-08-09
- Superseded: 2026-08-10 by owner decision and ADR-0016

This ADR historically selected a supervised writable database on the devstand and rejected
a Kaggle-hosted primary. That inverted the exact imported source without owner approval.
It is retained only as incident evidence and has no normative force.

The binding replacement is ADR-0016: the single writable PostgreSQL-primary runs only in
the Kaggle master Notebook; private Kaggle Datasets hold verified checkpoint generations;
the devstand is a lightweight control plane. No code or deployment may use this historical
record as an allowlist for local PostgreSQL, PGDATA, local committer or master backup.
