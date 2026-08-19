# OpenCode: передача пачек файлов через my-data-hub MCP

Публичный endpoint: `https://mcp-datahub.kenigevents.ru/mcp`.

Если OpenCode был подключён до этого релиза, выключите/включите `my-data-hub` в
панели MCP (или перезапустите OpenCode), чтобы обновить каталог инструментов.
Повторная OAuth-авторизация не нужна, пока текущий grant действителен.

Создание или чтение private Kaggle Dataset **не требует** Kaggle master Notebook и
PostgreSQL. Эти инструменты работают в provider-only режиме при `master_state=ABSENT`.
PostgreSQL нужен только для канонических `bloggers.*`/`data.*` операций.

## Рекомендуемый prompt для OpenCode

```text
Используй только MCP my-data-hub. Создай private disposable mcp_managed Kaggle Dataset
из всех файлов каталога <LOCAL_DIRECTORY>. Не помещай содержимое файлов в один вызов
provider.resources.create. Для каждого файла локально вычисли byte_size и SHA-256,
затем выполни provider.upload.start, передавай файл последовательными raw-чанками не
более 24576 байт через provider.upload.put_chunk (canonical base64, exact offset,
byte_size и SHA-256 каждого чанка). При разрыве вызови provider.upload.status и продолжи
с received_bytes. После READY вызови provider.upload.finalize. Затем проверь
provider.resources.list и скачай каждый файл через provider.resources.download,
проверяя итоговый SHA-256. Верни resource_ref, точную numeric provider_version и
claim_sha256. Не запускай PostgreSQL master Notebook.
```

Для постоянного Dataset установите `disposable=false`. Для временной передачи и тестов
используйте `disposable=true`, а после получения файлов —
`provider.resources.delete` с точным `claim_sha256`.

## Контракт загрузки

1. `provider.upload.start`: один UUID `upload_id`, один UUID `task_id`, уникальные
   `effect_id`/`idempotency_key`, private `mcp_managed`, ordered manifest
   `{path, byte_size, sha256}`.
2. `provider.upload.put_chunk`: raw chunk `<=24576` байт, canonical base64, exact текущий
   offset и SHA-256 чанка.
3. `provider.upload.status`: показывает `received_bytes` по каждому пути и позволяет
   продолжить после рестарта клиента/сервера.
4. `provider.upload.finalize`: принимает только полностью полученный и hash-verified
   upload, затем единственный центральный Kaggle adapter создаёт Dataset.
5. `provider.resources.list` и chunked `provider.resources.download`: проверяют точную
   numeric version, claim и file hashes.

Границы: до 100 файлов, 64 MiB на файл, 256 MiB на upload; до 8 активных upload на
OAuth subject/client и 512 MiB declared bytes на него. Байты временно находятся только
в private central staging и удаляются после finalize/abort/expiry; в SQLite и MCP audit
остаются только метаданные/хэши.

## Наблюдённое доказательство

16 августа 2026 endpoint прошёл public OAuth/MCP canary при `master_state=ABSENT`:
10 файлов, 2,025,000 байт, 87 upload-вызовов, exact list, 20 download-вызовов,
проверка SHA-256 всех файлов, удаление Dataset и полная live-inventory проверка его
отсутствия. Санитизированная запись:
`docs/operations/evidence/2026-08-16-provider-chunked-upload-live.json`.
