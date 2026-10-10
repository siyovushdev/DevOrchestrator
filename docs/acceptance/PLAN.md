# План приёмки DevOrchestrator `adab547`

Роль: независимая инженерная приёмка перед controlled live tests. Этот документ не разрешает rollout и не объявляет production ready.

Исполнение кода latch завершено. Вердикт live activation — `CHANGES_REQUIRED` в [ACCEPTANCE_REPORT.md](ACCEPTANCE_REPORT.md). Порядок будущего переключения — в [ROLLOUT_PLAN.md](ROLLOUT_PLAN.md). Инвентарь снимка уточнён: `reconciliation_retry` есть у #3, #13, #15 и #16. #37 таким полем не обладает; это отдельный `review_hold` при `FRESH_REVIEW_REQUIRED`.

Перед любым controlled live activation нужен опубликованный и независимо принятый release commit с этими исправлениями и `external_effects.py`. `adab547` таким commit не является. При latch worker report и разрешённые read-only MCP tools не запускают cleanup и не пишут worker metadata. Archived MERGE не создаёт `PENDING` без уже авторизованной отправки. Recovery backup снимается только после quiescence всех писателей state и worker metadata. Перед сменой checkout сверяются точный SHA, наличие guard-модулей и source manifest.

## Объект

| Среда | Путь | Commit | Роль |
| --- | --- | --- | --- |
| Staging | `/home/siovush/DevOrchestrator-acceptance` | `adab547` (`acceptance/f3-47`) | Код приёмки F1/F2/F3 + #47 |
| Production checkout | `/home/siovush/DevOrchestrator` | `6de2161` | Рабочий код MCP и TaskBridge |
| Live state | `/home/siovush/.local/share/devorchestrator/task_bridge_state.json` | — | Авторитетное состояние, только чтение |
| Snapshot | `/home/siovush/.local/share/devorchestrator-rollout-backups/20261010-131011/task_bridge_state.json` | — | Единственный источник migration dry-run |

Production и staging смотрят на один репозиторий разными worktree. `origin/master` уже содержит `adab547`; локальный production worktree отстаёт на два коммита и продолжает исполнять `6de2161`.

## Что уже известно о runtime

- User units: `devorchestrator-mcp.service`, `devorchestrator-task-bridge.service`. Bridge `Requires=` MCP.
- Оба unit: `WorkingDirectory=%h/DevOrchestrator`, `ExecStart=%h/DevOrchestrator/.venv/bin/python` соответствующего скрипта, `Restart=always`, `RestartSec=5`.
- Общие drop-in `30-work-enrollment.conf`: worker `work`, queue `work:queued`, лимиты coding 1 / review 1 / agents 2 / machines `work=1,firefly=1`.
- Bridge дополнительно читает `%h/.config/devorchestrator/telegram.env` и `network.env`. Содержимое секретов не читается и не копируется.
- MCP слушает `127.0.0.1:8787` (`streamable-http`, stateless). Python production venv: 3.12.3, пакет `mcp` 2.3.0.
- Реестр проектов: `/home/siovush/DevOrchestrator/projects.json`. Staging-копии `projects.json` нет.
- Состояние, receipts (`requests/`), task metadata (`tasks/`), `worker_leases.json`, `worker_consistency.json` и `worker.lock` лежат в одном `%h/.local/share/devorchestrator`.
- Наблюдаемые процессы на момент baseline: MCP PID 1541743 и TaskBridge PID 1541746, старт 2026-10-09 11:54. Их не останавливать и не перезапускать.

## Инварианты приёмки

- Один lifecycle writer. Внешний эффект только после durable checkpoint.
- `DurabilityFailure` — `BaseException`. Цикл `main` ловит только `Exception` и не продолжает шаг из грязной памяти.
- Неоднозначный launch, неизвестная схема, отсутствующая capability и conflict lock сохраняют владение. Приёмка не снимает lock и не переигрывает launch.
- Исторические request ID, signature 1/2, receipts, publication journals и Chief/owner gates не переписываются.
- Новые reviewer start — signature v3 и `review_binding`. Новые coding start — signature v4 и `repository_binding`. Старый MCP этих аргументов и tools не имеет.
- Idle observations не перезаписывают авторитетный state. Реальный semantic transition пишет checkpoint.
- Снимок и live state не изменяются. Все записи — в staging или `.acceptance-lab/`.

## Изоляция

1. Тесты запускаются интерпретатором production venv, но с кодом staging (`cwd` / `sys.path`). Venv не меняется.
2. `HOME` и `TMPDIR` указывают в `.acceptance-lab/`. `Path.home()` и `DATA_DIR` не попадают в live state.
3. Секретные переменные окружения в тестовый процесс не передаются.
4. GitHub, Telegram и worker launch в тестах — mock/fixture. `tests/test_mcp_concurrency.py` поднимает ephemeral loopback, не `:8787`.
5. Migration probe копирует только backup, подменяет `subprocess` и bridge transport на fail-closed и не вызывает `ControlLoop.step()`.
6. Перед прогоном проверяется, что импорт не резолвит live `task_bridge_state.json`.

## Этапы

1. Baseline systemd, путей, протокола и отличий `6de2161` → `adab547` (только чтение).
2. Полный `python -m unittest discover -s tests` без skip, плюс `py_compile` и `git diff --check`.
3. Дополнительные fsync-проверки в `tests/test_acceptance_fsync.py`: отказ file fsync до rename и directory fsync после rename.
4. Migration dry-run снимка: load, `LegacyReviewRecovery.enroll` через конструктор `ControlLoop`, первый semantic save, повторный save без записи.
5. Сверка инвариантов #47, записей `reconciliation_retry`, отдельного состояния #37, queue, `pending_rpc`, `review_transition`, journals и identity digest. Отдельное read-only сравнение live drift, без использования live файла как источника миграции.
6. Failure injection существующим regression suite и новыми fsync-тестами. Матрица: slow RPC, timeout, обрыв, crash между checkpoint, ambiguous launch, duplicate completion, close во время операции, failed fsync.
7. Совместимость binding v3/v4, inspection/fencing и старых signature. Зафиксировать единственный безопасный порядок переключения общего каталога.
8. Повторить `tests/measure_semantic_durability.py` на изолированном fixture около 2,28 МБ: baseline, fixed, deferred baseline, deferred fixed. Окна по 60 с.
9. Отчёт и rollout plan. Вердикт только `GO_FOR_CONTROLLED_LIVE_TESTS` или `CHANGES_REQUIRED`.

## Критерий PASS этапа

- Regression: 0 fail, 0 error, 0 skip.
- Migration: workflow digest после enroll и semantic save совпадает с durable-проекцией снимка; неожиданных добавлений, удалений и изменений нет. Допустимо одно удаление root `timings` и шести volatile health-полей с одной записью файла. Второй и третий save не пишут.
- Lock #47 сохраняется. Записи `reconciliation_retry` сохраняются как есть; отсутствующая запись не создаётся. `pending_rpc` и `review_transition` не появляются, если их не было.
- Fixed measurement: 0 authoritative replacements, inode/mtime/content стабильны. Deferred notification остаётся `PENDING` и не отправляется.
- Fsync file failure не меняет checkpoint и не вызывает `submit`. Directory fsync failure не вызывает `submit`.
- Production checkout, unit files, live state и receipts не изменялись этой приёмкой.

## Вне scope

Реальные Cursor/Codex/SSH, production MCP `:8787`, GitHub mutations, restart unit-ов, commit, push, deploy. Если для них нужен факт, в отчёте будет точная команда и требуемое разрешение, без выполнения.
