# Joplin integration architecture

Status: `PLANNED_BOUNDARY / NOT_ENABLED`

## 1. Goal

Allow the owner and an agent to use Joplin notebooks as a durable human knowledge surface while `my-data-hub` remains the orchestration and relational core.

The expected device topology is:

```text
Windows desktop Joplin
  ↕ Joplin's configured sync target
Android Joplin

Windows desktop Joplin Data API
  ↕ local/private connector
my-data-hub PostgreSQL + MCP
  ↕
Agent
```

## 2. Integration API

The connector uses the documented Joplin Data API exposed by the desktop Web Clipper service or a narrowly scoped Joplin plugin. It must never read or edit Joplin's internal SQLite database files.

The token remains on the desktop/connector host and is never returned by MCP.

## 3. Source-of-truth policy

- Joplin is authoritative for the human-authored note body unless a note is explicitly managed as generated projection.
- `my-data-hub` is authoritative for project/object identity, pipeline state, provenance and synchronization receipts.
- The hub stores note identity, hash, links and selected compact projection, not an uncontrolled duplicate of all personal notebooks.
- A conflict is recorded when both sides changed since the last receipt; no blind last-write-wins.

## 4. Initial use cases

1. Link a Joplin note to a project or catalog object.
2. Search linked note metadata from MCP.
3. Create a structured research note from a canonical object.
4. Import an owner-marked note into a discovery/research intake.
5. Write pipeline/report links back to a dedicated managed section.
6. Preserve a revision/hash receipt for every automated update.

## 5. Proposed MCP additions after connector installation

- `search_notes(query, notebook_id, limit)` — `notes:read`;
- `get_note_projection(note_id)` — `notes:read`;
- `link_note(object_id, joplin_note_id, policy)` — `notes:link`;
- `create_managed_note(template, object_ids, notebook_id)` — `notes:write`;
- `sync_note(note_id, expected_hash)` — `notes:write`;
- `list_note_conflicts()` — `notes:read`.

These tools remain disabled until the desktop endpoint, token storage, notebook allowlist and conflict behavior are tested.

## 6. Android boundary

The agent does not connect directly to Android. Android participates through Joplin's normal synchronization. Therefore mobile support does not require exposing the desktop Joplin Data API to the public internet.

## 7. Security gates

- explicit notebook allowlist;
- no access to all notes by default;
- token stored outside repository/DB payloads;
- localhost/private tunnel only;
- note body logging disabled;
- hash precondition before update;
- dry-run diff for destructive/large edits;
- tombstone and restore receipt for deletions;
- connector can be disabled independently from core services.
