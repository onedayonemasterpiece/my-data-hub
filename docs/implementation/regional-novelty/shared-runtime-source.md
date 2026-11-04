# Полный снимок начального kaggle-runtime для аудита

Этот файл — неизменяемый снимок исходников и тестов для аудитора, у которого нет доступа к общему приватному репозиторию. Он не является второй исполняемой копией пакета. Единственный upstream: `onedayonemasterpiece/kaggle-runtime`.

**Source commit:** `52644f2748b602f54304e56424eee720d12a691b`. **Source version:** `0.1.0`, Python ≥3.11. Все 15 файлов ниже сверены с опубликованным Git tree по Git blob SHA. Локально прошли 16 unittest и compileall; production adapters пока не подключены.

[Маршрут аудита](OPUS-AUDIT.md) · [Контекст, provenance и пределы проверки](runtime-code-evidence.md) · [План следующих частей](implementation-plan.md)

## Реестр файлов

| Путь | Bytes UTF-8 | Git blob SHA-1 |
|---|---:|---|
| `.github/workflows/tests.yml` | 441 | `e2b6b0e45ac3668149559b1aee812acda6bcc5ca` |
| `.gitignore` | 55 | `fb7bbf5cc288d35000c19898ff0095bdf3739853` |
| `AGENTS.md` | 3681 | `aa346ac6b989fe1c5f7980052bf309cb95549deb` |
| `README.md` | 5540 | `7f945da928e7f1eeb65ce8737a8aba8e302247b1` |
| `docs/source-evidence.md` | 9733 | `526b19c941ccc50b6c6b58a13c5f1d3b636c73dc` |
| `pyproject.toml` | 371 | `59e64652a68669f73716da31664251ab9e2fb0f0` |
| `src/kaggle_runtime/__init__.py` | 108 | `dd68ccf9b6da9ded46b71b5d9f789636511f23aa` |
| `src/kaggle_runtime/codec.py` | 2403 | `83f88ad6774e7c80dd0f64fd668d139be52c977b` |
| `src/kaggle_runtime/providers.py` | 1278 | `4b420c4653c50d43be86e0f79dac334b755ad086` |
| `src/kaggle_runtime/py.typed` | 0 | `e69de29bb2d1d6434b8b29ae775ad8c2e48c5391` |
| `src/kaggle_runtime/releases.py` | 4197 | `cd3cdb4a30aaad1c4a86250e346910d17e56728e` |
| `src/kaggle_runtime/rotation.py` | 4384 | `528badf81f934966267f90b27e0fb3dbe4969450` |
| `tests/test_codec.py` | 1812 | `d5ef7175a583f359d8b7469c49c4b8eac2b606c7` |
| `tests/test_releases.py` | 3181 | `26d2a8d6937e970b3b4305a5d3cdaa6102e3f370` |
| `tests/test_rotation.py` | 3930 | `c689106e2eb7e2311cf8e23ccb221312b7abafb0` |

Все текстовые файлы сохраняют исходный завершающий перевод строки. `py.typed` — пустой файл. Внутренние Markdown-ссылки в процитированных исходниках относятся к исходному репозиторию, а не к каталогу audit packet.

## `.github/workflows/tests.yml`

Git blob: `e2b6b0e45ac3668149559b1aee812acda6bcc5ca`; SHA-256: `8fb330b213221a21fb6b7fed048f340638345f5312b55ca71dfbd1a7626a7846`.

````yaml
name: unit tests
on: [push, pull_request]
permissions:
  contents: read
jobs:
  tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python: ['3.11', '3.12', '3.13']
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python }}
      - name: Pure policies and bounded codec
        run: PYTHONPATH=src python -m unittest discover -s tests -v
````

## `.gitignore`

Git blob: `fb7bbf5cc288d35000c19898ff0095bdf3739853`; SHA-256: `d7fb2df3c22b25195e563f453abe6734636808b197e7f4aaaec293f16d342a2b`.

````text
__pycache__/
*.py[cod]
*.egg-info/
.venv/
build/
dist/
````

## `AGENTS.md`

Git blob: `aa346ac6b989fe1c5f7980052bf309cb95549deb`; SHA-256: `165e2fbf15f0129f660cfb5ec322ef86424cb2acc2d60622002ed78e7ac7823f`.

````markdown
# Контракт общего Kaggle runtime

Решения владельца и существующие продуктовые инварианты имеют приоритет. Работать над одной конкретной общей обязанностью, сохраняя одного владельца состояния.

## Границы

- В ядре нет предметных сущностей РКБ/MDH/EventsBot/DAEP, SQL-схем, prompts или publication logic.
- Не копировать существующий lifecycle в ещё один сервис. Использовать ledger потребителя; state transitions пакета — значения, не самостоятельная БД.
- Истечение local timeout/lease не доказывает остановку provider execution. Потерянный ответ сверяется по прежнему operation key автоматически.
- Старый worker не меняет canonical state; законченный immutable результат может быть принят с прежним attempt identity для использования текущим владельцем.
- Исчерпание всех разрешённых free маршрутов ведёт к stop/fresh executor по решению владельца. Provider/account ограничения остаются фактическими; новый notebook не считается доказательством новой квоты.
- Не наследовать OpenCode auth по умолчанию. Секреты не входят в source, логи, state objects или Dataset. Допустимы только явно выбранные credential aliases конкретного потребителя.
- OpenCode постоянно обновляется, но версия активного attempt не меняется. Различать discovery, installed binary и running process.
- PostgreSQL/Dataset и BGE — опциональные профили. Кодовый worker не обязан иметь эти зависимости.
- Сохранить current/previous verified Dataset и exact journal replay до полного переноса; не придумывать внешний backup service в текущем scope.

## Проверка и evidence

Использовать различающие тесты реальных границ: повторный JSON key, поздний result, потерянный stop/start ACK, числовая stable selection, сохранённый attempt pin. Не объявлять тест pure policy живой проверкой Kaggle, квоты или двух production consumers.

В изменении указать исходник-донора и точный commit, что переносится/удаляется у потребителя и чем доказана совместимость. Не публиковать source snapshot как универсальный готовый transport без адаптеров. Авторские схемы и wire versions не переименовывать незаметно.

Перед завершением: `PYTHONPATH=src python -m unittest discover -s tests -v`, `python -m compileall -q src tests`, оценка существенных рисков. Новые gates/статусы нужны только при различающемся действии клиента или восстановлении; не добавлять ручное подтверждение в штатный путь.
````

## `README.md`

Git blob: `7f945da928e7f1eeb65ce8737a8aba8e302247b1`; SHA-256: `c24098ec11256ac12ad7ffe739b05b4567bf3b1216f0deb891d641549b9e9be3`.

````markdown
# kaggle-runtime

Общий код Kaggle, транспорта и управления исполнителями для РКБ, My Data Hub, EventsBot и DAEP.

## Состояние

Начальный исходный пакет версии **0.1.0**. Реализованы небольшие общие функции и чистые политики; они проверяются локально без Kaggle и вызовов моделей. Подключение production-потребителей, полный WSS reconnect loop, provider driver и перенос PG/Dataset journal ещё относятся к следующим поставкам. Наличие этого репозитория не означает выполненную межпроектную миграцию.

| Модуль | Реализованное назначение |
|---|---|
| `codec.py` | Ограниченный UTF-8 JSON object, запрет повторных ключей и нечисловых значений; выделено из strict parsing РКБ |
| `releases.py` | Числовой выбор stable в явной major, обнаружение новой major, возраст проверки, неизменяемый pin попытки |
| `rotation.py` | Порядок stop → подтверждение → fresh start; потерянные ответы требуют сверки исходного действия |
| `providers.py` | Граница provider operations и политика credentials без наследования ambient auth |

## Использование и проверка

Python 3.11 или новее; runtime-зависимостей нет.

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
python -m compileall -q src tests
```

Потребитель сохраняет возвращённое состояние операции в **своём существующем ledger до внешнего эффекта**. Пакет не создаёт ещё одну БД/очередь и не обещает exactly-once WebSocket. Канонический предметный эффект обеспечивает transaction/unique key принимающего продукта.

`codec` не вводит новую wire version и не заменяет предметную schema validation. Версию протокола, размер кадра и transport authentication задаёт существующий адаптер. `rotation` подтверждает замену исполнителя, но не восстановление provider quota и не готовность workload. `releases` выбирает кандидат; получение release feed, установка, проверка и активация — обязанности executor adapter.

## Владение и потребители

- Общий пакет: provider lifecycle, нейтральный transport, bootstrap, executor/version policy и опциональный PG/Dataset профиль.
- РКБ: evidence/history/source contracts, BGE и предметные native handlers.
- My Data Hub: материалы, аналитика значимого нового, SQL-схема, ingress и продуктовый MCP.
- EventsBot: событие, расписание, prepared apply, публикация и качество event extraction.
- DAEP: coding tasks, зависимости, Git checkpoints и интеграция кода.

Controllers могут независимо работать на RunCoveer и DevCoveer. Один физический kernel и одна эксклюзивная auth-session имеют одного владельца. Существующая `ai-resource-control` остаётся authority общего provider/account бюджета.

## План извлечения

Следующие поставки описаны в [проекте My Data Hub](https://github.com/onedayonemasterpiece/my-data-hub/blob/main/docs/implementation/regional-novelty/OPUS-AUDIT.md). Там же находится доступный аудитору снимок этого исходного кода. Снимок — исследовательский артефакт, не второй импортируемый пакет.

1. Привязать нейтральные функции к одному RKB adapter и одному MDH adapter, удалить заменённые локальные копии.
2. Извлечь WSS attach/generation/reconnect и Kaggle intent/reconcile, сохранив прежние wire contracts.
3. Подключить deterministic OpenCode bootstrap/update и реальную stop/start/probe последовательность.
4. Перенести RKB exact journal/Dataset сохранение через существующий ledger MDH.
5. Доказать два настоящих потребителя и один полный продуктовый путь MDH.

Все исходники принадлежат этому репозиторию. Изменение поведения вносится здесь, версия зависимости потребителя обновляется явно. Release-теги и проверяемые артефакты оформляются при поставке; текущий source snapshot не выдаётся за опубликованный wheel или живой rollout.
````

## `docs/source-evidence.md`

Git blob: `526b19c941ccc50b6c6b58a13c5f1d3b636c73dc`; SHA-256: `4568502391dcce20804fd48233588645a73954bf97e01dd4a0c75461698406ac`.

````markdown
# kaggle-runtime: provenance and initial executable scope

Date: 2026-11-04. This record covers local source under `implementation/kaggle-runtime`; it is not evidence of deployment or a successful Kaggle run.

## Product decision and scope

The owner requires one maintained implementation shared by My Data Hub, RKB, EventsBot and DAEP. When the eligible free-model pool is exhausted, the executor must be stopped and a fresh executor launched. A fresh executor is not evidence of restored provider quota. Credential inheritance is empty by default; only explicitly required scoped credentials may be supplied. Database authority and durability follow RKB; backups are outside this implementation slice.

The owner of a monitoring or coding job notices interrupted work when the executor exhausts its models. The concrete decisions are stop-before-replace and continuously current OpenCode. The concrete version-selection defect is that official `releases/latest` returned v1.19.5 while the repository also published v2 releases. The shared policy boundary addresses these decisions without adding a service, database, scheduler, retry loop or product schema.

Instructions consulted: [devserver-simplicity at f588486fbc66354b3567fa9b0d539f4c9e0b782a](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/f588486fbc66354b3567fa9b0d539f4c9e0b782a/skills/devserver-simplicity/SKILL.md).

## Exact source provenance

| Implemented part | Source and exact scope |
|---|---|
| `codec._unique_object` | Adapted from the identically named function at RKB `0b7590628dcb29a270727f3920607084dde30720`, `src/regional_knowledge/kaggle_protocol.py`, lines 441–448. |
| `codec._reject_constant` | Adapted from the identically named RKB function at the same SHA, lines 450–451. |
| `codec.decode_object` | Extracted generic byte/UTF-8/object checks and strict JSON parsing from RKB `decode_envelope`, starting line 454. Domain dispatch, Pydantic models, routes and `PROTOCOL_VERSION=2` are deliberately absent. Caller supplies the byte limit and keeps its own envelope validation. |
| `codec.encode_object` | New generic encoding boundary inspired by RKB `encode_envelope` at lines 495–499, without Pydantic or domain round-trip validation. It rejects key coercion and non-finite output. |
| `codec._finite_float` | Small extension: reject exponent overflow such as `1e999`; `json.loads(parse_constant=...)` alone does not catch this path to infinity. |
| `releases.py` | New pure policy for the owner's freshness requirement and observed v1/v2 release selection problem. It is not copied from a previously working updater. |
| `rotation.py` | New pure transition policy implementing the owner's stop→fresh decision, preserving unknown effect outcomes and exact attempt generation. It is not a provider implementation. |
| `providers.py` | Structural Python `Protocol` for future adapters and an explicit credential selector; no network, shell, Kaggle or model operations. |

RKB source: [kaggle_protocol.py at the exact commit](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_protocol.py#L441). Git blob SHA: `2a86ce7f8411925bcf1ee6206cfc793fe642fe23`. The whole source file was read before extracting the generic portion.

Planning evidence: [MDH architecture at 77a62c53c881120164f525a226ec296dcb3c77b3](https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/docs/architecture/regional-monitoring-shared-kaggle-runtime-20261104.md), with the later owner decisions above taking precedence. DAEP lifecycle requirements were inspected at [d3559d731bb74fbb1262114b9aad15687bb54701](https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/docs/implementation-task.md); no DAEP runtime code was claimed or copied.

## Real behavior of this slice

- The codec parses one bounded UTF-8 JSON object, rejecting duplicate keys at every depth, malformed JSON, non-object frames, NaN/Infinity and float overflow. It imposes no new wire format and does not authenticate, authorize or deduplicate repeated frames.
- Release selection uses numeric stable `major.minor.patch` tags, with an optional `v` prefix. It excludes branches, malformed tags, drafts and prereleases; it reports newer major lines without automatically migrating the chosen line. Feed adapters must enumerate the relevant official release pages rather than trust a single `latest` result.
- Freshness is a pure assessment from an actual installed version, a selected candidate, an explicit observation timestamp and a caller-supplied maximum age. Missing facts remain unknown; old discovery remains stale. No default freshness SLA has been invented.
- An existing attempt pin remains unchanged when a new candidate appears. The pin contains the exact executable version and executable SHA-256. This is a binary digest, not the digest of a download archive. Attestation comparison accepts observations supplied by a future launch adapter; it does not independently prove which server process is running.
- Rotation begins at `STOP_REQUIRED`. Persisting `requested()` before the external effect moves it to `STOP_UNKNOWN`; replacement cannot start until the exact old executor/generation is observed stopped. Start also becomes unknown before its external effect. Reconciliation distinguishes an unknown start, a confirmed absent start and an identified fresh executor. A stale or duplicate observation cannot advance the current operation. The operation ID must be retained when retrying a confirmed absent launch.
- There is no model-backoff loop in place of rotation. `COMPLETE` means a replacement executor was observed, not that a provider granted quota. A controller must measure subsequent model availability separately.
- `credential_environment` selects only explicitly named scoped keys and returns an empty mapping by default. It is a credential subset, not a sanitizer for an environment that is subsequently merged with `os.environ`. The future process adapter must pass an explicit environment and isolate OpenCode auth/config directories to prevent credential-file inheritance.

## Concrete next integration boundaries

Not yet implemented: durable operation ledger, controller loop, real Kaggle stop/status/launch and operation reconciliation, WSS sessions/routing/authentication, compatibility adapters for existing RKB messages, checkpoint/restore, database master management, resource-authority client, release fetching/install/promotion/canary/rollback, actual process attestation, model calls, isolated worker process environment and product adapters.

Provider reconciliation must use the exact provider execution identity and operation key. Merely observing that a kernel slug exists or that a stop request was accepted is insufficient to claim the old execution stopped. These are provider-adapter obligations; this package currently exposes only the interface and decision primitives.

The complete release lifecycle still needs a deliberate v1/v2 migration policy, scheduled discovery, artifact verification, compatibility canary, promotion for new attempts only, rollback and a maximum-age/offline policy. The current policy code does not download or upgrade OpenCode, and passing its tests does not establish a production rollout.

## Added-element justification

| Element | Concrete decision/failure | Existing donor boundary and why extraction is needed |
|---|---|---|
| Caller-provided byte cap and duplicate/non-finite rejection | Owner requests strict generic JSON extraction | RKB implements these checks inside domain dispatch; the reusable portion needs no domain dependency. |
| Chosen major, newer-major signal, freshness and immutable attempt pin | Owner requires current OpenCode; latest v1 label coexists with v2 | No executable updater exists in the inspected my-daep tree. Pinning alone would become stale. |
| Stop/start UNKNOWN phases and generation correlation | Owner requires stop old before fresh; lost responses must not create overlap | This slice is a pure decision layer; durable ledger and provider observation remain explicit missing adapters. |
| Known-absent versus unknown launch observation | Unknown cannot authorize another launch; definite absence must allow progress | A bare `None` could not distinguish these outcomes. |
| Explicit scoped credential selector | Owner forbids inherited OpenCode API keys by default | No worker process adapter is present yet; source starts from an empty credential set. |

## Validation

Command, from `implementation/kaggle-runtime`:

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

Local result: **16 tests passed**. The suite covers malformed/oversize/duplicate JSON fields, UTF-8 byte caps, v1 latest versus v2 stable selection, numeric ordering and branch/draft/prerelease rejection, stale or missing discovery, immutable attempt pins, actual-version mismatch, unknown/stale stop evidence, ordered replacement and replay, unknown versus absent start, stale start identities and default empty credentials.

The package declares Python >=3.11 and no runtime dependencies. Local unit tests use only stdlib and do not require a package install. The checked-in GitHub workflow runs the same command on Python 3.11, 3.12 and 3.13; those CI runs have not been observed as part of this local implementation.

Budget: **320 physical production Python lines + 173 test lines = 493 lines** at handoff. Metadata/workflow and this evidence document are separate. No remote changes, deployment, Kaggle launches, model calls or runtime upgrades were performed by this implementation subtask.
````

## `pyproject.toml`

Git blob: `59e64652a68669f73716da31664251ab9e2fb0f0`; SHA-256: `0477a4da9bfce862a212fad2aed1d35f33c4686ee421886ef3fa285936c9b5ac`.

````toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "kaggle-runtime"
version = "0.1.0"
description = "Shared Kaggle runtime primitives and pure execution policies"
requires-python = ">=3.11"
dependencies = []

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
kaggle_runtime = ["py.typed"]
````

## `src/kaggle_runtime/__init__.py`

Git blob: `dd68ccf9b6da9ded46b71b5d9f789636511f23aa`; SHA-256: `dd3435a70482fefe258ad5c2137cf50f502fbd55bfbcc53882677d984153bd69`.

````python
"""Shared primitives; no provider deployment or product wire protocol is implied."""

__version__ = "0.1.0"
````

## `src/kaggle_runtime/codec.py`

Git blob: `83f88ad6774e7c80dd0f64fd668d139be52c977b`; SHA-256: `0426c9d752fd27431a8d6b864d16097308332938b6b2fd090be4f90f900f995e`.

````python
"""Bounded JSON objects derived from RKB kaggle_protocol.py at 0b7590628dcb.

No envelope, route, protocol version, authorization or delivery semantics live here.
Callers retain their existing schema validation and choose their existing byte cap.
"""
from __future__ import annotations

import json
import math


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON field")
        value[key] = item
    return value


def _reject_constant(value: str) -> None:
    raise ValueError("non-finite JSON number")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite JSON number")
    return number


def _check_size(data: bytes, max_bytes: int) -> None:
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("positive byte limit required")
    if len(data) > max_bytes:
        raise ValueError("JSON frame too large")


def decode_object(data: bytes, *, max_bytes: int) -> dict[str, object]:
    """Decode one UTF-8 object; reject duplicate fields and non-finite numbers."""
    if not isinstance(data, bytes):
        raise TypeError("wire frame must be bytes")
    _check_size(data, max_bytes)
    raw = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object,
                     parse_constant=_reject_constant, parse_float=_finite_float)
    if not isinstance(raw, dict):
        raise ValueError("frame must be an object")
    return raw


def _string_keys(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON keys must be strings")
            _string_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _string_keys(item)


def encode_object(value: dict[str, object], *, max_bytes: int) -> bytes:
    """Encode without silently coercing keys, emitting NaN, or truncating."""
    if not isinstance(value, dict):
        raise TypeError("frame must be an object")
    _string_keys(value)
    data = json.dumps(value, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")
    _check_size(data, max_bytes)
    return data
````

## `src/kaggle_runtime/providers.py`

Git blob: `4b420c4653c50d43be86e0f79dac334b755ad086`; SHA-256: `2946ac60e8609c6b48599e6994d3baf5b51f2f3c6f4c04ac1ad370fb1513f522`.

````python
"""Adapter boundary only: no Kaggle calls and no pretend production implementation."""
from typing import Iterable, Mapping, Protocol

from .rotation import StartObservation


def credential_environment(available: Mapping[str, str], *,
                           scoped_keys: Iterable[str] = ()) -> dict[str, str]:
    """Default: inherit no credential, even when a parent environment has keys.

    Explicit scoped names are supplied only when the workload requires them.
    This is the credential subset, not a sanitizer for a merged parent environment.
    The future process adapter must also isolate OpenCode's auth/config directories.
    """
    return {name: available[name] for name in scoped_keys}


class ExecutorProvider(Protocol):
    """An executor identity is not a PG master; stop is never Dataset deletion."""

    def request_stop(self, executor_id: str, *, generation: int,
                     operation_id: str) -> None: ...

    def observe_stopped(self, executor_id: str, *, generation: int) -> bool | None: ...

    def request_start(self, *, operation_id: str, generation: int,
                      credential_env: Mapping[str, str]) -> StartObservation: ...

    def find_started(self, *, operation_id: str, generation: int) -> StartObservation: ...
````

## `src/kaggle_runtime/py.typed`

Git blob: `e69de29bb2d1d6434b8b29ae775ad8c2e48c5391`; SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

````text
````

## `src/kaggle_runtime/releases.py`

Git blob: `cd3cdb4a30aaad1c4a86250e346910d17e56728e`; SHA-256: `d2431537e0b3944308696bf306bd103ef70b5e81b3809507ecf244c6159c9f80`.

````python
"""Pure stable-channel decisions. Fetching, installation and canaries are adapters."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import re
from typing import Iterable


@dataclass(frozen=True, order=True)
class Version:
    major: int
    minor: int
    patch: int

    def __post_init__(self) -> None:
        if any(type(part) is not int or part < 0 for part in (self.major, self.minor, self.patch)):
            raise ValueError("version components must be nonnegative integers")

    @classmethod
    def parse(cls, tag: str) -> Version:
        match = re.fullmatch(r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", tag)
        if match is None:
            raise ValueError("stable numeric version tag required")
        return cls(*(int(part) for part in match.groups()))


@dataclass(frozen=True)
class Release:
    tag: str
    draft: bool = False
    prerelease: bool = False


@dataclass(frozen=True)
class Selection:
    candidate: Version | None
    newer_majors: tuple[int, ...]


def select_stable(releases: Iterable[Release], *, major: int) -> Selection:
    """Choose numerically within the configured major, ignoring the latest label.

    Feed adapters must enumerate releases; releases/latest alone can remain on v1
    while v2 exists. A tag's target branch is not a version or a channel selector.
    """
    if type(major) is not int or major < 1:
        raise ValueError("explicit positive major required")
    versions = set()
    for release in releases:
        if release.draft or release.prerelease:
            continue
        try:
            versions.add(Version.parse(release.tag))
        except ValueError:
            continue
    candidate = max((v for v in versions if v.major == major), default=None)
    return Selection(candidate, tuple(sorted({v.major for v in versions if v.major > major})))


class Freshness(str, Enum):
    UNKNOWN = "unknown"
    STALE_DISCOVERY = "stale_discovery"
    UPDATE_AVAILABLE = "update_available"
    VERSION_MISMATCH = "version_mismatch"
    CURRENT = "current"


def assess_freshness(installed: Version | None, selected: Version | None, *,
                     checked_at: datetime | None, now: datetime,
                     max_age: timedelta) -> Freshness:
    """No network means no new freshness evidence; caller chooses the age policy."""
    if max_age <= timedelta(0) or now.utcoffset() is None:
        raise ValueError("positive age and timezone-aware clock required")
    if checked_at is None:
        return Freshness.UNKNOWN
    if checked_at.utcoffset() is None or checked_at > now:
        raise ValueError("invalid release observation time")
    if now - checked_at > max_age:
        return Freshness.STALE_DISCOVERY
    if installed is None or selected is None:
        return Freshness.UNKNOWN
    if installed.major != selected.major or installed > selected:
        return Freshness.VERSION_MISMATCH
    return Freshness.UPDATE_AVAILABLE if installed < selected else Freshness.CURRENT


@dataclass(frozen=True)
class AttemptPin:
    attempt_id: str
    version: Version
    binary_sha256: str

    def __post_init__(self) -> None:
        if not self.attempt_id or re.fullmatch(r"[0-9a-f]{64}", self.binary_sha256) is None:
            raise ValueError("attempt identity and executable SHA-256 required")

    def matches_actual(self, version: str | None, binary_sha256: str | None) -> bool:
        """Compare launch attestation, not an upstream label or unrelated disk file."""
        try:
            return Version.parse(version or "") == self.version and binary_sha256 == self.binary_sha256
        except ValueError:
            return False


def pin_attempt(attempt_id: str, version: Version, binary_sha256: str, *,
                existing: AttemptPin | None = None) -> AttemptPin:
    """A promoted release only affects new attempts; an existing pin never changes."""
    if existing is not None:
        if existing.attempt_id != attempt_id:
            raise ValueError("pin belongs to another attempt")
        return existing
    return AttemptPin(attempt_id, version, binary_sha256)
````

## `src/kaggle_runtime/rotation.py`

Git blob: `528badf81f934966267f90b27e0fb3dbe4969450`; SHA-256: `1b450c3377b129f9589299ef0d790aede42a66da7ca20105098a0289de65ed42`.

````python
"""Stop-before-replace policy after confirmed exhaustion of eligible free models.

This object is a pure transition value, not a durable ledger or provider driver.
Persist requested() before the external effect. Unknown effects are reconciled by
the same operation key; they never grant permission to launch another executor.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum


class Phase(str, Enum):
    STOP_REQUIRED = "stop_required"
    STOP_UNKNOWN = "stop_unknown"
    START_REQUIRED = "start_required"
    START_UNKNOWN = "start_unknown"
    COMPLETE = "complete"


class Action(str, Enum):
    STOP_OLD = "stop_old"
    RECONCILE_STOP = "reconcile_stop"
    START_FRESH = "start_fresh"
    RECONCILE_START = "reconcile_start"
    COMPLETE = "complete"


@dataclass(frozen=True)
class StartObservation:
    """known=False is UNKNOWN; known=True with no ID is confirmed absence."""
    known: bool
    executor_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.known) is not bool or (not self.known and self.executor_id is not None):
            raise ValueError("unknown start cannot identify an executor")
        if self.executor_id is not None and (not isinstance(self.executor_id, str) or not self.executor_id):
            raise ValueError("executor ID must be nonempty text")


@dataclass(frozen=True)
class Rotation:
    """One exhausted executor; generation identifies its exact provider attempt.

    COMPLETE means replacement observed, never 'provider quota reset'. Product
    checkpoint/durability and workload priority remain in the owning controller.
    """
    operation_id: str
    old_executor_id: str
    generation: int
    phase: Phase = Phase.STOP_REQUIRED
    fresh_executor_id: str | None = None

    def __post_init__(self) -> None:
        if not self.operation_id or not self.old_executor_id:
            raise ValueError("operation and executor identities required")
        if type(self.generation) is not int or self.generation < 1:
            raise ValueError("positive generation required")

    @property
    def action(self) -> Action:
        return {
            Phase.STOP_REQUIRED: Action.STOP_OLD,
            Phase.STOP_UNKNOWN: Action.RECONCILE_STOP,
            Phase.START_REQUIRED: Action.START_FRESH,
            Phase.START_UNKNOWN: Action.RECONCILE_START,
            Phase.COMPLETE: Action.COMPLETE,
        }[self.phase]

    def requested(self) -> Rotation:
        """Persist intent before stop/start; a crash must retain UNKNOWN."""
        next_phase = {Phase.STOP_REQUIRED: Phase.STOP_UNKNOWN,
                      Phase.START_REQUIRED: Phase.START_UNKNOWN}
        if self.phase not in next_phase:
            raise ValueError("reconcile the pending effect instead of repeating it")
        return replace(self, phase=next_phase[self.phase])

    def observe_stop(self, *, executor_id: str, generation: int,
                     stopped: bool | None) -> Rotation:
        """None is uncertain; False is current evidence the old executor still runs."""
        if type(generation) is not int:
            raise TypeError("observed generation must be an integer")
        if (executor_id != self.old_executor_id or generation != self.generation
                or self.phase != Phase.STOP_UNKNOWN):
            return self
        if stopped is None:
            return self
        if type(stopped) is not bool:
            raise TypeError("stop observation must be bool or None")
        return replace(self, phase=Phase.START_REQUIRED if stopped else Phase.STOP_REQUIRED)

    def observe_start(self, *, operation_id: str, generation: int,
                      observation: StartObservation) -> Rotation:
        if type(generation) is not int:
            raise TypeError("observed generation must be an integer")
        if (operation_id != self.operation_id or generation != self.generation + 1
                or self.phase != Phase.START_UNKNOWN or not observation.known):
            return self
        if observation.executor_id is None:
            return replace(self, phase=Phase.START_REQUIRED)
        if observation.executor_id == self.old_executor_id:
            raise ValueError("replacement must be a fresh executor")
        return replace(self, phase=Phase.COMPLETE, fresh_executor_id=observation.executor_id)
````

## `tests/test_codec.py`

Git blob: `d5ef7175a583f359d8b7469c49c4b8eac2b606c7`; SHA-256: `0dbe2e371756d0c0bd982dc47f07c74310bbfad43618f90c57cb7abc60964e58`.

````python
import unittest

from kaggle_runtime.codec import decode_object, encode_object


class CodecTests(unittest.TestCase):
    def test_utf8_round_trip_and_byte_limit(self):
        data = encode_object({"text": "Мира"}, max_bytes=32)
        self.assertEqual(decode_object(data, max_bytes=len(data)), {"text": "Мира"})
        with self.assertRaises(ValueError):
            decode_object(data, max_bytes=len(data) - 1)
        with self.assertRaises(ValueError):
            encode_object({"text": "Мира"}, max_bytes=len(data) - 1)

    def test_duplicate_fields_rejected_at_any_depth(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":{"b":1,"b":2}}'):
            with self.subTest(raw=raw), self.assertRaisesRegex(ValueError, "duplicate"):
                decode_object(raw, max_bytes=128)

    def test_nonfinite_and_malformed_frames_rejected(self):
        frames = [b'{"n":NaN}', b'{"n":Infinity}', b'{"n":-Infinity}',
                  b'{"n":1e999}', b'{"n":}', b'{}{}', b'{"s":"\xff"}', b'[]']
        for raw in frames:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode_object(raw, max_bytes=128)

    def test_encode_rejects_ambiguous_keys_and_nonfinite_numbers(self):
        for value in ({"n": float("nan")}, {"n": float("inf")}, {"x": {1: 1, "1": 2}}):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                encode_object(value, max_bytes=128)

    def test_frame_replay_is_not_mistaken_for_delivery_deduplication(self):
        data = b'{"request_id":"same"}'
        self.assertEqual(decode_object(data, max_bytes=32), decode_object(data, max_bytes=32))
        with self.assertRaises(TypeError):
            decode_object("{}", max_bytes=32)


if __name__ == "__main__":
    unittest.main()
````

## `tests/test_releases.py`

Git blob: `26d2a8d6937e970b3b4305a5d3cdaa6102e3f370`; SHA-256: `69a4facac79eae79738194c5fde508b2e130068745d6432325835711ccab0fe8`.

````python
from datetime import datetime, timedelta, timezone
import unittest

from kaggle_runtime.releases import (
    Freshness, Release, Version, assess_freshness, pin_attempt, select_stable,
)


class ReleaseTests(unittest.TestCase):
    def test_latest_label_on_v1_does_not_hide_v2_or_force_major_migration(self):
        feed = [Release("v1.19.5"), Release("v2.2.5"), Release("v2.2.4")]
        self.assertEqual(select_stable(feed, major=2).candidate, Version(2, 2, 5))
        v1 = select_stable(feed, major=1)
        self.assertEqual(v1.candidate, Version(1, 19, 5))
        self.assertEqual(v1.newer_majors, (2,))

    def test_numeric_order_excludes_branches_drafts_and_prereleases(self):
        feed = [Release("v2.9.9"), Release("v2.10.0"), Release("dev"), Release("latest"),
                Release("v2.11.0-rc.1"), Release("v2.12.0", prerelease=True),
                Release("v3.0.0", draft=True), Release("v02.20.0"), Release("v2.20.2٥")]
        selected = select_stable(feed, major=2)
        self.assertEqual(selected.candidate, Version(2, 10, 0))
        self.assertEqual(selected.newer_majors, ())
        self.assertIsNone(select_stable(feed, major=4).candidate)

    def test_freshness_requires_recent_discovery_and_actual_installed_version(self):
        now = datetime(2026, 11, 4, tzinfo=timezone.utc)
        def assess(installed, selected, checked_at=now):
            return assess_freshness(installed, selected, checked_at=checked_at,
                                    now=now, max_age=timedelta(days=1))
        current = Version(2, 2, 5)
        self.assertEqual(assess(current, current), Freshness.CURRENT)
        self.assertEqual(assess(None, current), Freshness.UNKNOWN)
        self.assertEqual(assess(current, current, None), Freshness.UNKNOWN)
        self.assertEqual(assess(current, current, now - timedelta(days=2)), Freshness.STALE_DISCOVERY)
        self.assertEqual(assess(Version(2, 2, 4), current), Freshness.UPDATE_AVAILABLE)
        self.assertEqual(assess(Version(1, 19, 5), current), Freshness.VERSION_MISMATCH)
        self.assertEqual(assess(Version(2, 3, 0), current), Freshness.VERSION_MISMATCH)
        with self.assertRaises(ValueError):
            assess(current, current, now + timedelta(seconds=1))

    def test_new_release_never_rewrites_an_active_attempt_pin(self):
        old = pin_attempt("attempt-1", Version(2, 2, 4), "a" * 64)
        same = pin_attempt("attempt-1", Version(2, 2, 5), "b" * 64, existing=old)
        self.assertIs(same, old)
        fresh = pin_attempt("attempt-2", Version(2, 2, 5), "b" * 64)
        self.assertEqual(fresh.version, Version(2, 2, 5))
        with self.assertRaises(ValueError):
            pin_attempt("attempt-2", Version(2, 2, 5), "b" * 64, existing=old)

    def test_attestation_does_not_trust_upstream_version_or_unobserved_process(self):
        pin = pin_attempt("attempt-1", Version(2, 2, 5), "a" * 64)
        self.assertTrue(pin.matches_actual("v2.2.5", "a" * 64))
        for version, digest in ((None, None), ("v2.2.5", "b" * 64), ("v2.2.4", "a" * 64)):
            self.assertFalse(pin.matches_actual(version, digest))


if __name__ == "__main__":
    unittest.main()
````

## `tests/test_rotation.py`

Git blob: `c689106e2eb7e2311cf8e23ccb221312b7abafb0`; SHA-256: `eec88b3db61d232adf9498e10c654675e04b3d4f2e2902907c14b88622f9ceca`.

````python
import unittest

from kaggle_runtime.providers import credential_environment
from kaggle_runtime.rotation import Action, Rotation, StartObservation


class RotationTests(unittest.TestCase):
    def setUp(self):
        self.rotation = Rotation("rotation-1", "old-executor", 3)

    def stopped(self):
        return self.rotation.requested().observe_stop(
            executor_id="old-executor", generation=3, stopped=True)

    def test_exhaustion_requests_stop_not_backoff_or_parallel_replacement(self):
        self.assertEqual(self.rotation.action, Action.STOP_OLD)
        pending = self.rotation.requested()
        self.assertEqual(pending.action, Action.RECONCILE_STOP)
        with self.assertRaises(ValueError):
            pending.requested()

    def test_unknown_or_stale_stop_never_releases_replacement(self):
        pending = self.rotation.requested()
        for executor_id, generation, stopped in (
            ("old-executor", 3, None), ("other", 3, True), ("old-executor", 2, True)
        ):
            self.assertEqual(pending.observe_stop(executor_id=executor_id,
                             generation=generation, stopped=stopped), pending)
        still_running = pending.observe_stop(executor_id="old-executor", generation=3, stopped=False)
        self.assertEqual(still_running.action, Action.STOP_OLD)
        with self.assertRaises(TypeError):
            pending.observe_stop(executor_id="old-executor", generation=3.0, stopped=True)

    def test_stop_confirmation_then_launch_and_replay_are_ordered(self):
        stopped = self.stopped()
        self.assertEqual(stopped.action, Action.START_FRESH)
        pending = stopped.requested()
        self.assertEqual(pending.action, Action.RECONCILE_START)
        observed = StartObservation(True, "fresh-executor")
        done = pending.observe_start(operation_id="rotation-1", generation=4, observation=observed)
        self.assertEqual(done.action, Action.COMPLETE)
        self.assertEqual(done.fresh_executor_id, "fresh-executor")
        self.assertEqual(done.observe_start(operation_id="rotation-1", generation=4, observation=observed), done)
        self.assertEqual(done.observe_stop(executor_id="old-executor", generation=3, stopped=False), done)

    def test_unknown_launch_and_confirmed_absence_are_distinct(self):
        pending = self.stopped().requested()
        self.assertEqual(pending.observe_start(operation_id="rotation-1", generation=4,
                         observation=StartObservation(False)), pending)
        absent = pending.observe_start(operation_id="rotation-1", generation=4,
                                       observation=StartObservation(True))
        self.assertEqual(absent.action, Action.START_FRESH)
        self.assertEqual(absent.operation_id, pending.operation_id)

    def test_stale_launch_cannot_complete_or_replace_with_old_executor(self):
        pending = self.stopped().requested()
        for operation, generation in (("other", 4), ("rotation-1", 3)):
            self.assertEqual(pending.observe_start(operation_id=operation, generation=generation,
                             observation=StartObservation(True, "fresh")), pending)
        with self.assertRaises(ValueError):
            pending.observe_start(operation_id="rotation-1", generation=4,
                                  observation=StartObservation(True, "old-executor"))

    def test_credentials_are_empty_by_default_and_explicit_when_required(self):
        parent = {"OPENAI_API_KEY": "parent-key", "REQUIRED_SCOPED_KEY": "scoped-key"}
        self.assertEqual(credential_environment(parent), {})
        self.assertEqual(credential_environment(parent, scoped_keys=["REQUIRED_SCOPED_KEY"]),
                         {"REQUIRED_SCOPED_KEY": "scoped-key"})
        with self.assertRaises(KeyError):
            credential_environment(parent, scoped_keys=["MISSING"])


if __name__ == "__main__":
    unittest.main()
````
