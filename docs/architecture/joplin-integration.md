# Joplin integration architecture

## Goal

Allow selected Joplin notebooks and notes to participate in my-data-hub while preserving
Joplin's offline-first desktop/mobile experience and keeping agent access controlled.

## Initial topology

1. Joplin Android and Joplin Desktop synchronize through a normal Joplin sync target.
2. A bridge on the Windows desktop uses the supported Joplin Data API or plugin API.
3. The bridge reads only explicitly selected notebooks/tags.
4. It submits semantic note/link changes to my-data-hub MCP or a local application service.
5. Optional outbound updates go through a dedicated outbox and expected-note-revision check.

Joplin Server/sync storage is not treated as an agent API. The desktop bridge is the point
at which synchronized mobile changes become available to my-data-hub.

## Data model

- `joplin.notebook_link.bridge_instance_id` identifies a local bridge/profile without storing its token;
- `joplin.notebook_link` maps a notebook to a hub project/collection;
- `joplin.note_link` maps stable note ID to a hub content/knowledge object;
- `joplin.sync_cursor` records the last observed API update sequence/time;
- `joplin.note_revision` stores hashes and compact metadata, not the Joplin token;
- `joplin.conflict` records concurrent semantic edits requiring policy/manual resolution.

## Conflict policy

- independent tags/relations may union;
- unchanged-body metadata updates may merge;
- concurrent body changes do not use generic last-write-wins;
- deletion is a tombstone with retained provenance;
- outbound edits require the expected Joplin content hash;
- a mismatch creates a conflict and leaves the user note untouched.

## Security

The Joplin API token remains in the Windows credential store or process environment. The
bridge binds only to localhost and never exposes the Joplin Data API remotely. Agents reach
my-data-hub MCP, not port 41184 and not the Joplin profile database.

## Deferred decisions

- exact note markup and front-matter convention;
- which notebooks are authoritative versus reference-only;
- whether outbound note creation is enabled;
- attachment/resource retention and size limits;
- Android-direct MCP client ergonomics.
