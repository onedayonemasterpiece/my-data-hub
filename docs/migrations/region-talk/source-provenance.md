# Region Talk donor provenance

The implementation agent must inspect and record exact source commit hashes before copying
or adapting code. The current `onedayonemasterpiece/region-talk` repository is the first
product/operational donor; `events-bot-new` additionally contains historical implementation
and proven MCP/Kaggle lifecycle families, including:

- `scripts/region_talk_orchestrator.py` — queue priorities, operational gates and metrics;
- `kaggle/RegionTalkCandidateReport/` and launcher;
- `kaggle/RegionTalkBgeM3Enrichment/` and launcher;
- `kaggle/RegionTalkImageDiagnostic/` and launcher;
- current finalizer, review sync, planner/publisher and external-publication import code;
- `docs/features/region-talk-channel/` including orchestration, publication queue, source
  onboarding/recovery, external publications and image methodology;
- focused tests and synthetic fixtures;
- `private_events_mcp/` only as a security/transport donor.

For each adapted file, `docs/migrations/region-talk/adaptation-manifest.json` records. The
bootstrap contains a schema-valid pending manifest; it must not be marked verified until
source commits and hashes are pinned:

- source repository and commit;
- source path and SHA-256;
- destination path and SHA-256;
- classification: copied, adapted, behavioural-reference-only;
- architectural changes (for example YDB repository replaced by PostgreSQL UoW);
- tests proving preserved behaviour.

Do not wholesale copy unrelated `events-bot-new` runtime or any environment/session/data
file.
