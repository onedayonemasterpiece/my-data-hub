# my-data-hub target vision

Status: **canonical target vision**
Final product name: **my-data-hub**
Previous working name: **content platform**
First migration workload: **Region Talk / «О Калининграде говорят»**

## Naming and provenance

The earlier document
`idea-hub/ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md`
is the originating product/architecture research. It is not treated as an ordinary
inbox note and must not be described as a discarded draft. `content platform` was
its temporary name; `my-data-hub` is the final name for the same target system.

This repository turns that target vision into executable contracts while preserving
its main intent: one consolidated, searchable, provenance-rich content and knowledge
base; a controlled orchestrator; compute notebooks; agent access; and future personal
knowledge integration.

## Problem

Useful information is currently fragmented across source-specific stores, pipeline
queues, notebook outputs, repositories and personal notes. A source, author or article
can be rediscovered and reanalysed by several projects because identity, provenance,
processing history and results do not share a common model.

The platform must consolidate:

- people, organisations, editorial outlets and their accounts;
- posts, articles, videos, links and compact semantic records;
- one content object belonging to multiple projects;
- where, when and by which query/agent/pipeline an object was found;
- pipeline work, transitions, retries and failure evidence;
- model, LLM, image and embedding results with exact input/model identity;
- Russian full-text search and model-specific vector search;
- controlled agent writes through MCP;
- notebook workers that may run intermittently;
- later, selected Joplin notebooks and notes without making Joplin a database backend.

Full article bodies, downloaded video and large media are not durable canonical content
by default. The durable core stores links, compact metadata, summaries, fingerprints,
features, analysis evidence and references to private artifacts where required.

## Target outcomes

1. A source or publication is identified once and reused by many projects.
2. Every conclusion can be traced to inputs, model/version, run and policy revision.
3. An agent can search broadly but mutate only through bounded semantic tools.
4. Long-running compute can be moved to Kaggle without moving ownership of state.
5. A failed or duplicated notebook run cannot double-apply results.
6. Region Talk continues from all accumulated YDB knowledge without a behavioural reset.
7. The owner can inspect queue health, funnel loss and exact reasons rather than only jobs.
8. Personal notes may later participate through an explicit bridge and conflict policy.

## Initial deployment decision

The development host is also the initial production host. It runs canonical PostgreSQL,
the orchestrator and the remote MCP service with supervised auto-start. This is simpler
and safer than pretending Kaggle is an online database.

Kaggle remains important:

- CPU-intensive workers run there;
- each worker receives a bounded input manifest;
- each worker emits a strict, immutable result envelope and evidence bundle;
- the orchestrator validates and accepts results into PostgreSQL;
- private datasets may hold encrypted backups/checkpoints and run-history artifacts.

The architecture retains semantic changesets and a transactional outbox for intermittent
MCP/Joplin/notebook producers. These mechanisms are a controlled replication boundary,
not a second database.

## Success boundary for the first release

The first release is successful only when:

- PostgreSQL migrations apply on a clean database;
- Region Talk YDB export is complete and hashed;
- every source row has exactly one disposition: normalized, deduplicated, intentionally excluded, retained raw or quarantined;
- source/content identity counts and critical queue/gate counts reconcile;
- a shadow Region Talk cycle produces the same or deliberately improved decisions;
- MCP read tools work and write tools are scope- and revision-bound;
- Kaggle result replay is idempotent;
- YDB writes are stopped, rollback evidence is retained, and normal cycles use PostgreSQL.

## Accepted infrastructure and operator supplement — 2026-08-09

The following decisions extend the target vision through ADR-0009–ADR-0014:

1. Canonical PostgreSQL remains supervised on the devstand and normally available.
   Kaggle is compute/private artifacts and never a writable master database.
2. Owned systems integrate recurring data through versioned idempotent connectors with
   durable producer spool, intake receipt, staging, quarantine and reconciliation.
3. The remote endpoint is `https://mcp-datahub.kenigevents.ru/mcp` through TLS/OAuth.
4. The default semantic MCP remains bounded, but a separate operator profile may provide
   broad bounded reads and preview/apply DML under restricted PostgreSQL roles. Remote
   superuser/owner/DDL remains forbidden.
5. Kaggle resources are governed by registry control classes. Orchestrator-owned
   resources are status-only through remote MCP; MCP-owned private resources may be
   managed; exchange packages are private, hashed, TTL-bound and non-canonical.
6. Region Talk remains the first migration workload, but infrastructure, backup/restore,
   connector, provider and operator workflows are proven first.
7. An agent may manage Region Talk migration through typed gates, but cannot bypass
   accounting, quarantine, shadow, backup, cutover or rollback evidence.

These are explicit implementation decisions, not a renaming or replacement of the
originating idea-hub vision.
