# Showcase: lecture and audience chips

Owner change, 2026-09-05. Small additive follow-up to merged PR #38.
Base read: `b816f9a5c6f3d166c17b5c2c31110896e40241e2`.
No new tools, services, arbitrary badges, or changes to readiness.

## Display contract

| Lecture metadata | Displayed lecture chips |
| --- | --- |
| `kind: master` | Мастер-лекция (verification is never shown, even if stored true) |
| `kind: author`, confirmed `znanie_verified: true` | Авторская + Верификация РО «Знание» |
| `kind: author`, false or omitted verification | Авторская |
| No `lecture` metadata | No lecture-specific chips; ordinary offers are not relabelled |

Same `CardChips` component on the list and detail page. Chips are informational,
not buttons. Audiences wrap on narrow screens, with no truncation. Detailed
adaptation notes stay in `for_whom` / `requirements`; no new claim of ready-made
versions for every audience is introduced.

Optional card fields, accepted by the existing `create_view` and `apply` tools:

```yaml
lecture:
  kind: author  # master | author; set explicitly for lecture cards
  znanie_verified: false  # actual Boolean; true only from confirmed RO Znanie verification
# Existing audience remains the legacy fallback; explicit audiences replace it
# for chips and the audience filter, not supplement it with a coarse group.
audiences:
  - id: schoolchildren
    label: Школьники
  - id: students
    label: Студенты
  - id: working-youth
    label: Работающая молодёжь
  - id: adults
    label: Взрослые
```

`audiences` may contain up to eight unique ID/label pairs. Omit it to retain the
existing single audience. The filter matches ANY of a card's audiences. Search
indexes their labels. Do not author `lecture_chips`: they are derived output,
not writable source. Do not infer verification from `publish_state: ready`,
generic readiness, an unqualified `verified` flag, or planned submission.

## Integration and catalogue update — separate owner-started Codex task

1. Fresh-read main, this PR, AGENTS and `docs/operations/ideahub-showcase-runtime.md`.
   Integrate this bounded change after passing current-head CI. No architecture
   redesign. Use the existing release path and preserve current runtime fixes.
2. Upgrade BOTH MCP edge (input schemas) and Showcase runtime/renderer, preserving
   state, credentials, all active links, font/PNG support and deployment limits.
   Verify `lecture` and `audiences` in fresh `tools/list` for create/apply before
   writing the new source fields. Do not deploy new source to an old strict parser.
3. Read `showcase.get_source` for `lectures-ai-workflow`. In idea-hub read the current
   `registry/reviews/20260905-lecture-showcase-correction.md`, `ideas/lectures/README.md`
   and the referenced lecture passports. The reviewed catalogue is 35 cards:
   34 author lectures/topics and `catalog-lecture-domestic-tourism` as a partner
   master lecture. Check current membership rather than overwriting newer work.
4. Update ONLY lecture metadata and audiences through MCP (`apply` preview, then
   CAS publish, then get_source/get_link readback). Preserve title, summary,
   benefit, readiness, qualifications, order and contact data. Derive individual
   audiences from the existing approved audience labels, preserving school age
   qualifiers. Do not broaden the audience or invent verification. Set true only
   where verification specifically by RO Znanie is confirmed for this version;
   record unresolved attribution privately rather than showing an unearned chip.
5. Open the same catalogue link at 390x844 and 1440x900. Verify master excludes the
   verification chip, author always shows its type and only confirmed verification;
   separate audience chips and any-audience filtering work on list and detail.
   Check an ordinary non-lecture card for regression. No automatic external sending
   and no link rotation. The owner refreshes/reconnects MCP after server rollout.

## Tests and evidence

`tests/showcase/test_lecture_chips.py`: full type/verification matrix, strict Boolean,
no title/readiness inference, explicit audience fallback, bounds/deduplication,
search, read/write roundtrip and real MCP SDK schema + create/read/update calls.

`tests/showcase/test_browser_lecture_chips.py`: real constructor -> Astro -> local
publication -> Chromium at 360x800, 390x844, 1440x900; chip parity, any-audience
filter, labels rather than controls, visible first card, no horizontal overflow.
Fixture metadata is synthetic and says nothing about real lecture verification.
Existing Showcase Actions job discovers these tests and uploads screenshots/JUnit.

No production source or page was changed during implementation. Local unit/MCP
checks and lint run in ChatGPT; release-wide evidence must use the exact PR-head
Actions results, not older checkpoint results or the offline sandbox dependency mix.
