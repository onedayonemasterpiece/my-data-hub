# ADR-0007: Joplin integrates through a desktop bridge

- Status: Accepted
- Date: 2026-08-09

## Decision

A Windows-local bridge uses supported Joplin interfaces for selected notebooks
and notes. PostgreSQL remains canonical for hub entities and pipeline state.
The Android client receives note changes through Joplin's own synchronization.
Neither `my-data-hub` nor an agent edits Joplin's internal profile database.

## Consequences

The desktop bridge must be running and Joplin synchronized for note changes to
enter the hub. Initial integration is read/observe-first. Bidirectional writes
require exact note revision, deletion and conflict tests.
