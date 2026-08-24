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

## Pinned transformation-slice review (2026-08-19)

The pure queue-formation slice is adapted from
`onedayonemasterpiece/events-bot-new@5bbdb681623d5e4e0bff2133e487a6663c1a838a`.
The review used Git object bytes at that exact revision, not the donor working tree:

| Source path | Blob | SHA-256 | Target behaviour |
| --- | --- | --- | --- |
| `scripts/region_talk_external_publication_import.py` | `2f50d386ecab172f4c64b6eb4c0f39681772fd16` | `8259e342408192d2cf5eb191bc56dc400a09cc3bb57fabdca77208058ece868b` | conservative URL/DOI identity, grounded evidence and rights validation |
| `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py` | `7bc15d6175264cd3bfad242f4838c00bd2526394` | `f24a954f2213286ff7cf3ffe3d23706a4cee2d8b94f1421939c4a2f1e12f26c8` | current dual-vector evidence and eligibility v5 |
| `scripts/region_talk_publication_finalizer.py` | `76e928989585d05d2de2291b36109fba6df78dde` | `1cf78a6ff6b2df21475587a83de1b4c4790080f55b84491939f96e9e8ab901fe` | current-evidence fencing and candidate lifecycle |
| `scripts/region_talk_publisher_profile.py` | `649b2327f228e646793ec96dd99485605fccf776` | `3d35075594fd554f5e127ad6d12cc34ce4c5ab5e249e0efbeb13c8f01f82b975` | monotonic dossier merge |
| `scripts/region_talk_review_queue.py` | `d1b7bef64abb4fcc814732c7034a69fed473b534` | `b350bc6c8c74581a443523d5cf4a8bf927d06cad4a7625434a50368ea89acbf1` | disclosed vector/fallback MMR ordering |
| `scripts/region_talk_publication_plan.py` | `020894297aa61d46d0640dba47d3ad11a5e4b4e8` | `73e801fe380cb9b850df7980366263e783359fcc2d6a85be930bcf035d87a2e4` | current-review-bound publication slots |
| `tests/test_region_talk_external_publication_import.py` | `a9b9bc82c059e716f79f87359ff2258f4c118760` | `5f4b09e2d9e5e1ce0059b6a7c2770040927ba45be002fea8ed3d1fb79b920e28` | sanitized structural golden fixture |

The target code is an adaptation, not a copy of the 1.2 MB donor Notebook. No donor
credential, provider client, YDB access, PostgreSQL access or publication side effect was
ported into the transformation package.

## Bounded status and safety claims

The verified target-vision source is pinned separately at
`onedayonemasterpiece/idea-hub@0c3fcf71b2ee8ba8afa49624bef4b779873802f7` with
SHA-256 `c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852`.
That source describes Region Talk as an incomplete architectural prototype, not a finished
production reference. It does not prove access to either Region Talk donor repository or
verify any still-pending donor manifest entry.

In `my-data-hub`, Region Talk remains `paused` while donor provenance, migration accounting,
shadow comparison and canary gates are open. Production publication remains disabled and
requires its own exact-revision canary, receipt and owner approval; importing target-vision
bytes changes none of those gates.
