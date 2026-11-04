# ADR-0020: предложение Dev recovery journal отозвано

- Статус: withdrawn proposal; решение владельца от 04.11.2026 — [физическая база + native WAL](../implementation/regional-novelty/STORAGE-BASE-WAL.md).
- Один notebook, один publisher двух private Dataset. Snapshot-only/logical dump и открытый вопрос о потере хвоста заменены [принятым решением](../implementation/regional-novelty/owner-decisions.md), без отдельного пилотного хранения.
- PostgreSQL/PGDATA, WAL и canonical business tables на Dev запрещены. Live РКБ не меняется; исполняемые invariants не ослаблены.

Предыдущий текст ниже — только история предложения. Он не разрешает перенос собственного журнала и не является исполнительским списком.

<details>
<summary>Предложение до аудита</summary>

# ADR-0020: общий Kaggle runtime и журнал восстановления регионального мониторинга

- Статус: проект инженерной реализации для аудита; прямые продуктовые решения владельца обязательны.
- Дата: 2026-11-04.
- Основание: [решения владельца](../implementation/regional-novelty/owner-decisions.md), [проект](../implementation/regional-novelty/implementation-plan.md).
- Не заменяет: запрет PostgreSQL/PGDATA и канонических бизнес-таблиц на Dev, single writable primary в Kaggle, exact imported research.

## Контекст и решение

Владелец выбрал значимое новое первым результатом MDH, свежий Kaggle executor после исчерпания бесплатных моделей и схему сохранения РКБ без частых per-row Dataset uploads. Появляется один общий package kaggle-runtime, предметные adapters и independent controllers. SQL и миграции реализует система; пользователь не получает инфраструктурных обязанностей.

RKB Dataset-primary durability фактически использует bounded Dev journal с точными командами/результатами. Это несовместимо с буквальным старым описанием MDH «Dev state is control metadata only», если понимать его как запрет любых payload bytes. Объявить такой журнал metadata-only было бы неверно.

Предлагается явное уточнение: существующая control SQLite хранит ограниченный **операционный journal для replay**, содержащий exact payload/outcome, пока они не покрыты verified Dataset. Он не становится второй канонической бизнес-БД, не обслуживает поиск/аналитику, не содержит PGDATA или physical PG archive. Канонические запросы и редактирование выполняет единственный текущий PostgreSQL master в Kaggle. Это та же control authority, без второго ledger service.

Код внедрения W2 должен обновить соответствующую derived документацию, schema и проверки согласованно: `canonical_business_data_on_devstand: forbidden` сохраняется; описание permitted control state явно перечисляет bounded recovery payload. Изменения executable invariants не притворяются уже выполненными данным ADR. Новая ветка хранения не включается до проверки exact replay и ownership на одном сквозном источнике.

Решение владельца использовать долговечность РКБ уже получено. Статус проекта ADR означает техническую проверку способа переноса, а не ожидание повторного разрешения на выбранный продуктовый путь.

## Контракты

- Exact command фиксируется до передачи на PG apply. Lost ACK сверяется по прежней identity.
- PG apply, native outcome, canonical revision и обязательный outbox — одна транзакция.
- Dataset coverage относится к конкретным bytes/CAS token и prefix; поздний write не покрывается прежней версией.
- При kernel loss восстанавливается verified Dataset base и точный сохранившийся journal tail без повторного inference.
- Pending/UNKNOWN нельзя удалять только по возрасту TTL. Сохранённость всей Dev SQLite не объявляется внешней disaster recovery гарантией.
- Physical capture, upload и native archive verification идут внутри Kaggle/напрямую в Dataset, не через Dev диск.
- Внешние copies на диск/другое облако вне текущего объёма. Размер <100 GB относится к active DB; capacity checkpoint считается отдельно как инженерная возможность.
- Исчерпание OpenCode pool меняет executor; PG master живёт отдельно. Совмещённый профиль обязан пройти planned checkpoint/ownership path до STOP.

## Consequences и совместимость

Сохраняются existing connector ingress/durability states, master owner и scopes. Новый content committer регистрируется в bounded dispatcher вместо жёсткой сборки только statistics. Новые SQL migrations аддитивны. BGE content input 512 — отдельная representation policy; existing model max 8192 и blogger search не переписываются.

Shared runtime сохраняет wire compatibility при извлечении: RKB domain handlers остаются в адаптере. Продукт не вводит новый общий limiter, gateway, workflow engine, receipt DB или frontend. Документальный снимок shared code в MDH не импортируется приложением и не поддерживается как fork.

Для принятия инженерного решения нужны различающие результаты W1/W2: два adapters одной dependency, lost-ACK reconciliation, kernel-loss exact replay, правильный Dataset Saved и согласованные size/time limits. Наличие начальных 16 unit tests не доказывает эти integration свойства. Подробные источники и проверки: [RKB evidence](../implementation/regional-novelty/rkb-storage-evidence.md), [маршрут аудита](../implementation/regional-novelty/OPUS-AUDIT.md).

</details>
