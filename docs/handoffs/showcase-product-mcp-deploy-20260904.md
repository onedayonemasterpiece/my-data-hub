# Showcase: integration, deployment and live smoke

Status: **implementation prepared; production deployment not performed in ChatGPT**.
Repo: `onedayonemasterpiece/my-data-hub`.
PR: **#38**. Branch: `work/showcase-product-mcp-20260904`.
Base checkpoint: `622031f36f937a3015e17f361a1088b6864a3f03`.
Use current heads, not the checkpoint as a hard gate. Do not start a new architecture.

## What was implemented and checked

The existing eight tools, source store, Astro renderer and publication topology are
retained. Creation accepts existing card IDs, only new definitions must be provided;
`apply` updates with CAS, `mode` separates preview/save/publish. Drafts are readable,
shared-card edits fail closed, preview does not consume a key, errors remain actionable
across the gateway. Pages show task/results, compact filters/actions, local interest,
real contacts, correct card URLs and PNG/OG assets.

Reproducible checkpoints already observed:

- `01eb34808bf06d8e57ac2da2d3806b3a63d86469`: Showcase run `33922903634` passed
  81 tests including Chromium and local publication. General CI `33922903741` passed
  contracts, full pytest, Ruff, mypy and PostgreSQL integration.
- `c4c76d03cde26c98188c535692318a8f3c5bf6d0`: Showcase run `33923137409` passed
  the additional expanded share-menu and disabled-storage cases (84 tests).
- Later commits update the existing live runner and documentation. Read final PR checks
  for the current head; never transfer an older checkpoint's PASS to a different tree.

Browser screenshots use **fixture cards**, not a rewritten live pharma source. Chromium
runs in GitHub Actions; local sandbox navigation was blocked by browser policy. Native
share/clipboard interfaces are stubbed in tests; actual rendering, PNG fetches, source
mutation, Astro build and local publication are real. The wider local sandbox suite had
environment-specific failures (Python/runtime tooling); proper Python 3.12 general CI,
not that sandbox, is the release evidence.

Not checked in production: Docker rebuild under the deployed limits, current credentials,
proxy timeouts, cold OAuth/MCP discovery after upgrade, real Telegram/mobile share delivery,
main/pharma rebuilds and live disposable-source cleanup. These are the purpose of the task
below, not waived checks. No production state, links or idea-hub card source was changed.

## Prompt for the owner's separate Codex run

```text
Самостоятельно внедри один ограниченный релиз Showcase.
Repo: onedayonemasterpiece/my-data-hub
PR: #38
Ветка: work/showcase-product-mcp-20260904

Не проектируй заново, не создавай новый сервис/БД/CMS/набор MCP-методов.
Код написан в ChatGPT; твоя задача — интеграция, проверка, исправление реальных
дефектов внедрения и деплой. Работай небольшими законченными коммитами.

1. Fresh-read main, tip PR #38, комментарии, diff и статусы CI. Прочитай:
   docs/ideahub-showcase.md
   docs/operations/ideahub-showcase-runtime.md
   docs/handoffs/showcase-product-mcp-deploy-20260904.md
   scripts/showcase_live_closure.py
   Старые SHA — checkpoints. Учти новые изменения main, не перезапиши их.
   Выполни обычные проверки репозитория и SHOWCASE_BROWSER=1 pytest -q tests/showcase
   с установленным Chromium. Не заменяй браузер чтением исходников.
   Проверь чистую сборку Docker/Sharp/PNG под реальными лимитами CPU/RAM runtime.
   Устрани дефекты; не ослабляй проверки ради PASS. При зелёном результате интегрируй PR
   (предпочтительно squash, чтобы одноразовые переносные workflows не попадали в main).

2. Перед изменением сервера зафиксируй текущие image IDs, конфигурацию и защищённую
   копию приватного state. Сохрани active-link hashes и исходные YAML/blob hashes
   main и pharma-business-ai. Не сравнивай как raw-источник сериализованные get_source
   разных версий: новая версия добавляет defaults contacts/filters.
   Не выводи ключи, bearer, полные secret URL в Git, обычные логи или итоговый отчёт.

3. Выпусти edge remote-mcp и showcase-runtime из одной проверенной ревизии через
   существующий rollout. Не оставляй новый edge со старым runtime.
   Проверь фактические, а не примерные env:
   MY_DATA_HUB_SHOWCASE_MAX_REQUEST_BYTES=262144 в runtime;
   MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS=240 в edge;
   proxy/operator client budget не короче scoped 300 секунд.
   Старые явно заданные значения не исправятся одной заменой Python defaults.
   Сохрани OAuth boundary, восемь tools, state/slug, CSP/noindex/no-referrer,
   loopback-only runtime, отдельные bounded Git credentials и unprivileged контейнеры.
   Общие лимиты и PostgreSQL/Kaggle topology для этого релиза не перестраивай.

4. Получи fresh tools/list через реальный авторизованный MCP, не сохранённый каталог.
   Проверь create_view с view/mode и без expected_source_revision;
   apply с CAS; новые определения карточек требуют capability_type;
   preview не требует key, legacy формы остаются совместимы.
   Источник ошибок — конкретный вызов/граница, а не общий CHECKPOINT_FAILED master.
   Выполни реальный get_source и preview, прежде чем объявлять методы рабочими.

5. Через MCP пересобери main и pharma-business-ai без редакционных изменений.
   Проверь тот же URL, текущий source revision, HTTP 200, detail links,
   PNG/OG и заголовки. Не меняй ID/slug, не ротируй и не отзывай партнёрские ссылки.
   Содержание карточек и их готовность не повышай автоматически; упаковка текстов
   остаётся следующим owner-approved циклом через MCP, а не прямыми Git-правками.

6. Запусти обновлённый scripts/showcase_live_closure.py с новым безопасным run ID,
   существующим OAuth credential file и private 0600 SHOWCASE_MAIN_LINK_FILE.
   Текущий runner только читает/preview main; writes/update/rotate/revoke проверяет
   на disposable view. Проверь дополнительно ошибку неизвестного item_id,
   draft save/read и запрет публикации draft, stale CAS, защиту shared card,
   повтор preview→publish и идентичный повтор write без второй ссылки/коммита.
   Делай отрицательные проверки только на временных данных, не на партнёрских.
   Удали или явно перечисли остатки только временного source из receipt;
   reused card не удалять. Все disposable ссылки должны быть отозваны.
   При потерянном ответе сначала восстанови состояние того же view/key,
   не создавай второй тестовый экземпляр и не объявляй cleanup успешным вслепую.

7. Лично открой главную и detail page в браузере при 360×800, 390×844, 1440×900.
   Проверь первый экран, раскрытие/сброс фильтров, длинный текст, 44px действия,
   local interest, сообщение с выбранными card URLs, контакты, share/copy/fallback.
   При проверке доставки используй только явно разрешённый тестовый контакт;
   не рассылай партнёрам. OS Web Share и предпросмотр мессенджера могут требовать
   ручной проверки — отличай её от теста payload со stub. Не называй это проверенным,
   если не выполнял фактическую отправку и readback.

8. При дефекте откати согласованную пару образов/конфигурации, сохрани state и
   существующие страницы. При applied_not_published сохрани source revision и
   восстанови rebuild; не скрывай оставшуюся source-запись откатом runtime.
   Итог: код/дата/revision, фактически deployed image IDs, CI и live checks,
   main/pharma стабильность ссылок, PNG/HTTP/browser, cleanup и оставшиеся ограничения.
   Секретные ссылки верни владельцу только по приватному каналу, не в публичный PR.
   Обнови PR/operations evidence фактическими результатами.

После успешного серверного rollout сообщи владельцу, что можно обновить/переподключить
MCP. Он сделает это сам. После переподключения нужен fresh client tools/list и простой
preview новой подборки из существующих карточек; не обещай, что кэш клиента уже обновлён.
```

## Acceptance boundary

Ready means the integrated code, coordinated runtime deployment, real MCP mutation,
public pages and cleanup are independently evidenced. An HTTP 200 alone is not enough.
Native provider delivery and the owner's refreshed ChatGPT session are explicit final
checks; absence of access must be reported, not substituted with fabricated success.
