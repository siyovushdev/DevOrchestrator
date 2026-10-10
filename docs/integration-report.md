# Интеграция F1/F2/F3 и semantic durability #47

## Результат и границы

Исправления объединены в `/home/siovush/DevOrchestrator-integration` поверх общего
base commit `6de216192084b9902c73f5a81e1965d1d9b5c7a1`. Финальный полный прогон:
**669 PASS за 308,263 секунды, exit 0, без пропусков** — результат первоначальной
интеграции. Независимое ревью затем воспроизвело Chief delivery stall при
pre-dispatch worker.lock contention. Исправление и новая проверка описаны в
`docs/chief-delivery-fix-report.md`; эта проверка определяет текущую готовность.

Исходные `DevOrchestrator-review-fix` и `DevOrchestrator-dev` использовались только
для чтения. Их HEAD, Git status и SHA-256 всех 40 / 9 изменённых и untracked-файлов
сверены с начальным снимком. Production checkout, live state, сервисы и рабочие
конфигурации не изменялись. Commit, push, PR, merge, deployment и restart не выполнялись.
Перезапуски в тестах — восстановление объектов из файлов временных fixtures.
Это результат интеграционной проверки, не production-ready и не реальная приёмка.

## Как совмещены изменения

- F3 сохраняет единственного lifecycle writer: только он применяет reducer-ы и
  возобновляет lifecycle frames. I/O принимает неизменяемые JSON-команды и возвращает
  копии; live state, callbacks, scheduler и writer context не передаются executor-ам.
- PREPARE/LAUNCH/VERIFY/DRAIN/HELD, immutable envelopes и request IDs, receipts,
  reservations, locks, fencing и generation-bound cleanup перенесены без замены
  конфликтующих модулей целиком. Сначала применён diff F3, затем совместимые hunks
  #47 и ручное объединение четырёх пересечений.
- `save_state` использует semantic projection #47, сохраняя fatal
  `DurabilityFailure` F3. Ошибка authoritative checkpoint останавливает writer до
  следующего dispatch; только после checkpoint сохраняется необязательная telemetry.
- Фильтрация касается root `timings` и шести известных полей в `runtime_health`:
  `observed_at`, `last_heartbeat_at`, `heartbeat_sequence`, `lease_expires_at`,
  `heartbeat_age_seconds`, `lease_state`. Классификация, причины, generation,
  transition sequence/time, cleanup, retry deadlines, request arguments, receipts,
  locks, Chief/owner gates и publication journals остаются durable. Неизвестные
  health-поля сохраняются консервативно.
- `load_state` мигрирует legacy volatile-поля только в памяти; следующий save
  заменяет старый файл один раз. Recovery не читает telemetry. Status принимает
  bounded sidecar только при совпадении task identity и semantic health digest,
  не меняет классификацию и не выдаёт разрешений.
- Один generator outbox обслуживает синхронный и resumable интерфейсы. Telegram
  payload остаётся неизменяемым; свежие duration/title/action вычисляются в send
  buffer после backoff и eligibility. `SENDING` durable до передачи I/O; потерянное
  подтверждение после restart остаётся `UNCERTAIN` и не вызывает повторную отправку.
- F1/F2 transient preflight и reconciliation diagnostics, sandbox, независимое
  review, Chief и owner gates сохранены. Наблюдаемое закрытие sticky; COMMIT/PUSH/
  CREATE_PR/MERGE требуют свежего issue и текущего approval stamp на dispatch,
  включая ожидание capacity. Accepted/uncertain journals сохраняют repository hold
  и после закрытого restart сверяются без нового primary mutation.

## Разрешённые пересечения и несовместимости

Конфликтующие модули анализировались по обоим diff и объединялись по hunks.
Архитектурного противоречия между F3 и #47 не обнаружено.

| Пересечение | Итоговое решение |
| --- | --- |
| `task_bridge.py` | Semantic load/save #47 внутри fatal checkpoint boundary F3; bounded telemetry после authoritative save. Async freshness checks F3 сохранены, payload regeneration заменена transient send buffer |
| `bridge_effects.py` | Сохранён единый resumable generator F3; renderer #47 подключён к sync и async adapters после eligibility/backoff, до durable SENDING и отправки |
| `devctl.py` | Worker binding/receipt/fencing protocol F3 сохранён; read-only status дополнен identity-bound telemetry #47 |
| `operational_state.py` | Repository holds и migration actions F3 сохранены; timing/runtime display #47 не участвует в recovery или gates |

Тестовые fixtures #47 адаптированы к новому event-driven ControlLoop F3.
В transferred F3-тесте общий запрет всех write вызовов заменён проверкой именно
authoritative destination: optional sidecar разрешён, content/inode/mtime state
по-прежнему обязаны оставаться неизменными. Во втором полном прогоне эта проверка прошла.

## Полный список изменённых файлов

### Реализация — изменённые существующие модули

| Файл | Изменение |
| --- | --- |
| `bridge_effects.py` | Общий resumable outbox core, exact comment receipts, send uncertainty; transient Telegram renderer в обоих adapters |
| `chief_review.py` | Активная transition/migration закрывает approval |
| `devctl.py` | Worker bindings/signatures v3/v4, receipt inspection/fencing, source validation, report identity; status с optional telemetry |
| `execution_profiles.py` | Разделение UNAVAILABLE, REJECTED, STALE_INPUT и FENCED; bounded/redacted diagnostics |
| `mcp_server.py` | Async handlers и отдельная control/work capacity, capability/inspection/binding protocol |
| `notifications.py` | Управляемое включение clock-derived duration |
| `operational_state.py` | Publication repository holds и migration actions; read-only semantic status/telemetry |
| `publication.py` | Общая journal/recovery логика sync/resumable, guarded mutations и uncertain PR/completion handling |
| `scheduler.py` | Сохранение/восстановление transition/publication ownership и fail-closed admission |
| `task_bridge.py` | F1/F2/F3 lifecycle integration, reconciliation, closure/publication gates; shared semantic load/save и immutable notification path |
| `worker_runtime.py` | Start-request inspection, nonblocking locks, durable tombstones, exact receipt replay и orphan protection |

### Реализация — новые модули

| Файл | Назначение |
| --- | --- |
| `bridge_telemetry.py` | Semantic projection, bounded optional display sidecar и identity-bound status samples |
| `control_loop.py` | Event-driven single writer с отдельной polling/operation capacity |
| `review_coordinator.py` | Writer-only durable review checkpoints, authorization, adoption и drain |
| `review_driver.py` | Планирование review phases и применение bound completions |
| `review_migration.py` | Консервативная recovery legacy review envelopes |
| `review_transitions.py` | Версионированный pure protocol, bindings и неизменяемые IDs |
| `transition_executor.py` | Detached provider/repository I/O только с копиями данных |
| `transition_io.py` | JSON Command, WriterContinuation, Dispatcher и удержание uncertain in-flight ownership |

### Тесты и измерение

Изменены: `tests/test_bridge_integration.py`, `tests/test_cleanup_attestation.py`,
`tests/test_control_loop.py`, `tests/test_execution_profiles.py`,
`tests/test_mcp_adapter.py`, `tests/test_recovery.py`, `tests/test_runtime_liveness.py`.

Добавлены из F3: `tests/test_mcp_concurrency.py`, `tests/test_publication_closure.py`,
`tests/test_request_protocol.py`, `tests/test_resumable_control.py`,
`tests/test_review_binding.py`, `tests/test_review_coordinator.py`,
`tests/test_review_driver.py`, `tests/test_review_launch_retry.py`,
`tests/test_review_migration.py`, `tests/test_review_transitions.py`,
`tests/test_transition_io.py`. В idle F3-тесте отделён запрет authoritative writes
от допустимых sidecar writes; добавлены content stability и observability assertions.

Добавлены из #47: `tests/test_semantic_durability.py` и
`tests/measure_semantic_durability.py`. Loop fixtures адаптированы к explicit
F3 progression с сохранением всех no-write/no-send/restart assertions. Замер
settles continuations, использует `ControlLoop.wait()` и поддерживает `--output`.

Добавлен `tests/test_resumable_semantic_durability.py`: пять совместных регрессий
проверяют semantic persistence всех F3 checkpoints, sidecar failure после
checkpoint, fatal authoritative failure, legacy migration с неизменяемым envelope,
принятый reviewer launch с потерянным ответом, heartbeat polling и restart без
повторного запуска. Corrupt telemetry не влияет на ownership, recovery или gates.

### Документация и evidence

- `docs/execution-profiles.md` — transient preflight policy.
- `docs/operational-control-loop.md` — single writer, responsiveness, closure и telemetry.
- `docs/resumable-transitions.md` — protocol, receipts, migration, publication gates и совместная durability.
- `docs/semantic-durability.md` — sidecar schema, final-loop measurements и явно выделенные исторические результаты #47.
- `docs/integration-report.md` — этот полный отчёт.
- `docs/integration-sources/DevOrchestrator-review-fix-PLAN.md` и
  `docs/integration-sources/DevOrchestrator-review-fix-PROGRESS.md` — исходные исторические планы/evidence, сохранены без изменений.
- `PLAN.md`, `PROGRESS.md` — этапы и фактические checkpoints интеграции.
- `docs/integration-evidence/measurement-baseline.json`, `measurement-fixed.json`,
  `measurement-deferred-baseline.json`, `measurement-deferred-fixed.json` — сырые замеры.
- `docs/integration-evidence/source-manifest.json` — начальные hashes/status/HEAD источников.
- `docs/integration-evidence/test-inventory.json` — полный список 669 test IDs и версии Python/MCP.
- `docs/integration-evidence/verification.json` — итог тестов, проверки источников,
  полный Git file inventory и hashes проверенной реализации/тестов.

Интеграционный `.venv` игнорируется Git; зависимости скопированы только чтением из
уже установленного source venv. Никакие runtime model/project configuration files
не менялись. Временные debug/reference fixtures и логи находятся в `/tmp`.

## Проверки и восстановление

| Проверка | Результат |
| --- | --- |
| Совместные F3/#47 + semantic + closed coding restart | 20 PASS, 16.644s, no skips |
| F3 control после адаптации sidecar assertion | 7 PASS, 6.937s |
| Первый полный набор | 669 tests, 297.650s; одна несовместимая sidecar assertion, исправлена |
| Полный набор первоначальной интеграции | **669 PASS, 308,263s, exit 0, no skips** |
| Compilation всех source/test modules | PASS |
| Tracked и untracked whitespace / conflict markers | PASS |
| Source HEAD/status/content hashes | Не изменились |

Миграция и restart покрыты на PREPARE, LAUNCH, VERIFY и DRAIN; legacy requests
сохраняют точные envelopes/signatures, uncertain/prepared ownership и gates.
Проверены worker fencing и prepared replay, adopted receipts без второго launch,
matching cleanup generations, закрытые accepted COMMIT/PUSH/CREATE_PR/MERGE без
повторной primary mutation. Реальные Git fixtures локальны и временные; HTTP MCP
использует ephemeral loopback listener и fake devctl.

Sandbox прерывал пробуждение asyncio после threaded I/O; это воспроизводилось и
на неизменённой копии F3. Финальные регрессионные/measurement прогоны выполнены
вне sandbox с разрешением пользователя. Shutdown/recovery код ради этого не
менялся. Сам тестируемый worker sandbox и policy gates сохранены.

Инструмент сократил часть streamed output полного прогона; полный raw log не
заявляется. Итоговая строка `Ran 669 tests in 308.263s / OK` и exit 0 получены.
Результаты и полный discovery inventory сохранены в evidence.

Команды воспроизведения:

```sh
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m py_compile *.py tests/*.py
git diff --check
.venv/bin/python tests/measure_semantic_durability.py --mode baseline --seconds 60 --output /tmp/baseline.json
.venv/bin/python tests/measure_semantic_durability.py --mode fixed --seconds 60 --output /tmp/fixed.json
.venv/bin/python tests/measure_semantic_durability.py --mode review-baseline --deferred-notification --seconds 60 --output /tmp/deferred-baseline.json
.venv/bin/python tests/measure_semantic_durability.py --mode fixed --deferred-notification --seconds 60 --output /tmp/deferred-fixed.json
```

## Write amplification на финальной реализации

Каждое окно — около 60 секунд, payload 2,28 МБ, control cadence 10s, discovery 30s,
17 writer steps, одна discovery и пять свежих reports. Baseline меняет сравниваемое
поведение только в памяти, не checkout и не live state.

| Сценарий | Authoritative replacements / bytes | Telemetry replacements / bytes | Process write_bytes / syscw |
| --- | ---: | ---: | ---: |
| Whole-state baseline | 10 / 22 822 915 | 0 / 0 | 22 855 680 / 30 |
| Integrated fixed | 0 / 0 | 10 / 7 049 | 40 960 / 10 |
| Deferred regeneration baseline с semantic filtering | 4 / 9 134 368 | 15 / 14 767 | 9 203 712 / 27 |
| Integrated fixed + deferred notification | 0 / 0 | 15 / 14 739 | 61 440 / 15 |

В обоих fixed-сценариях content/inode/mtime authoritative file не изменились.
Deferred event остался PENDING, payload стабилен и отправок нет. Во время измерений
не выполнялись workflow mutations; записи small telemetry остаются bounded.
Это локальное воспроизведение, не замер production и не доказательство его прежнего
107 MiB/min наблюдения. Последующая правка затронула только test assertion;
измеренная реализация интеграционного дерева и measurement script не менялись.

## Самопроверка и оставшиеся риски

Самопроверка сравнивает итоговый diff с обоими источниками, включая untracked:
single writer и JSON isolation; exact request/envelope/signature identity;
checkpoint-before-dispatch и fatal save; generation cleanup; locks/reservations;
semantic health filtering; immutable send buffer; sticky closure, свежий issue и
quality stamp; recovery accepted/uncertain journals без blind replay. Проверены
миграция, corrupt/missing sidecar, её отказ и сохранение всех safety records.

В первоначальной проверке Git status проверен: изменения находятся только в интеграционном worktree и
соответствуют приведённому inventory. `git diff --check` и untracked whitespace
проверки прошли. Реализация и тесты после успешного полного прогона не менялись.
Независимый reviewer может проверить итоговый diff и все untracked-файлы, сверить
hashes/evidence и воспроизвести команды выше. Commit/PR для этого не создавался.

Дополнение после независимого ревью меняет только runtime paths в
`bridge_effects.py` и `task_bridge.py`, добавляет `tests/test_chief_delivery_retry.py`,
обновляет Chief runtime documentation и evidence/отчёт. Первоначальные measurements,
test inventory и `verification.json` сохраняются как исторические снимки.
Актуальные результаты follow-up приведены в `docs/chief-delivery-fix-report.md`.

Оставшиеся ограничения:

1. GitHub issue read и внешний Git/PR/merge effect не являются одной атомарной
   транзакцией. После уже выполненного dispatch закрытие может пересечься с
   принятием операции; journal/hold требует reconciliation фактического результата.
2. Prepared stale requests, generation-less legacy runtimes и unqueryable external
   acceptance могут требовать operator inspection. Сброс receipts/locks и unsafe
   resend не добавлены; некоторые неоднозначные ситуации намеренно не прогрессируют.
3. Rollout потребует совместимого worker/MCP (bindings v3/v4 и inspection/fencing)
   до включения bridge. Автоматического downgrade при активных phase records нет.
4. Реальные Cursor/SSH/network-failure soak, live migration, rollout/rollback и
   operational acceptance не выполнялись. Локальные fsync/CPU stalls не имеют
   hard real-time гарантии; shutdown может ждать медленное I/O.
5. Semantic checkpoint всё ещё сравнивает сериализацию authoritative файла;
   ноль замен файла не означает ноль CPU/read I/O. Optional telemetry продолжает
   небольшие записи. Error retries и настоящие workflow transitions пишутся durable
   по назначению; измерение отражает steady idle с меняющимися observations.
