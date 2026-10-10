# Ход приёмки `adab547`

Финал: вердикт `CHANGES_REQUIRED` для live activation в [ACCEPTANCE_REPORT.md](ACCEPTANCE_REPORT.md). Четыре блокера latch и read-side cleanup в `get_task_report` закрыты в рабочем дереве. Rollout не выполнялся. `adab547` не является smoke-целью.

## Baseline

- Staging `adab54726e7427d83cf42d9241c570ec619dceec`, production checkout `6de216192084`.
- User units `devorchestrator-mcp.service` и `devorchestrator-task-bridge.service`, общий `WorkingDirectory=/home/siovush/DevOrchestrator`, `Restart=always`.
- PID 1541743 и 1541746 живы с 2026-10-09 11:54, на финальной проверке те же PID и `NRestarts=0`.
- Python 3.12.3, `mcp` 2.3.0. Staging venv нет; прогоны используют production interpreter при `HOME` в `.acceptance-lab`.
- MCP слушает `127.0.0.1:8787`.
- Production MCP не имеет `get_transition_capabilities`, `inspect_start_request`, `review_binding`, `repository_binding`. `devctl.py` читается с диска на каждый tool call.

## Regression

```sh
env -i HOME=.../.acceptance-lab/home TMPDIR=.../.acceptance-lab/tmp \
  PATH=/home/siovush/DevOrchestrator/.venv/bin:/usr/bin:/bin \
  PYTHONPATH=/home/siovush/DevOrchestrator-acceptance \
  /home/siovush/DevOrchestrator/.venv/bin/python -m unittest discover -s tests -v
```

Промежуточно 680 OK, затем 682 OK. После pre-live hardening: **693 tests, 343.833s, OK**. После latch fix: **710 tests, 280.263s, OK**. После report latch: **717 tests, 345.296s, OK**, 0 failed, 0 errors, 0 skipped.

`py_compile` и `git diff --check` прошли.

## Migration

Probe на копии backup `20261010-131011`: enroll без записи, один semantic save `2738791` → `2730869`, дальше save молчит. Digest workflow стабилен. Удалены только `timings` и шесть volatile health-полей (58 путей).

Снимок: 38 issues, 90 `DONE`, lock owner `47`, retries `3/13/15/16`, `pending_rpc=0`, `review_transition=0`. #37 — `FRESH_REVIEW_REQUIRED` и `review_hold`, не `reconciliation_retry`. #36 — закрытый MERGE journal без `done`.

`release("47")` на загруженной копии вернул false. `recover([])` lock не изменил. Текущий identity worktree совпал с lock key.

К 14:02 live отличался от снимка двумя устойчивыми полями: снят `consistency_diagnostics.control` у #37 и добавлен `reconciliation_retry` у #42 (`attempts=1`, `review_status=FAILED`). Lock owner остался `47`, очередь 90 `DONE`, journal #36 без `outcome`. Старый bridge продолжал писать live file; backup не изменялся.

## Failure injection и durability

Матрица slow RPC, timeout, обрыв, crash, ambiguous launch, duplicate completion, close и fsync закрыта тестами внутри 710. Fixed measurement 60 с: 0 authoritative writes, content/mtime/inode стабильны. Deferred fixed: notification осталась `PENDING`, payload стабилен, отправки нет.

## Pre-live hardening

Исправлен archived MERGE: неотправлённый journal не получает новый `PENDING` или convergence; латченный `PENDING` и наблюдение `MERGED` не делают новый PUT. Probe режет допустимые удаления и путь вывода. Rollout backup перенесён на момент после quiescence bridge и MCP. Capability probe не умеет вызвать другой tool.

## Latch fix

`github_is_read` принимает только точные read-команды транспорта. `gh api` с `-X`, `--method=`, `-F` или `-f` до subprocess не доходит. `notifications.send` и dispatcher отказывают до Telegram. Новый запрещённый outbox-эффект не переходит в `SENDING`/`UNCERTAIN` и не растит `attempts`. Уже отправленный ambiguous POST и crash-receipt `SENDING` сохраняют сверку. `start_task`/`continue_task` при latch не пишут `pending_rpc` и очередь. Publication subprocess и fence не создают journal и receipt. Изолированный `ControlLoop` с pending Chief comment и queued coding task не вызывает subprocess, сеть, `Popen` и `capture`; старый receipt после restart тот же. Снятие latch отправляет тот же Chief comment. Live activation ждёт опубликованный и независимо принятый release commit, не `adab547`.

## Read-side report latch

`get_task_report` и `list_tasks` при latch читают уже записанный exit/status и не вызывают `reconcile_workers`, remote SSH refresh, `cleanup_group` или запись metadata/leases. Вшитый Firefly `report` при той же переменной не вызывает `cleanup_group`. `doctor`, catalog `--discover` и preflight без receipt отказывают до `Popen`. Выключенный latch по-прежнему доходит до `SIGTERM` на валидном process-group receipt. Live activation по-прежнему ждёт опубликованный release commit.

## Не делалось

Останов unit-ов, смена production checkout, вызовы production MCP, GitHub mutations, `doctor`/SSH, реальные Cursor/Codex workers, commit и push.
