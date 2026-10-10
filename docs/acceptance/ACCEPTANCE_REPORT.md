# Приёмка DevOrchestrator: latch перед controlled smoke

The final Safety Latch remediation and its verification are recorded in the
2026-10-10 addendum at the end of this report. Earlier results below are historical.

**Вердикт: `CHANGES_REQUIRED` для live activation.**

Четыре блокера latch и последующий read-side defect `get_task_report` закрыты в рабочем дереве staging и покрыты regression. Live activation запрещён: эти изменения не опубликованы и не приняты независимым ревью. Staging HEAD остаётся `adab54726e7427d83cf42d9241c570ec619dceec`; этот commit не содержит исправленный `external_effects.py` и не является целью smoke. `GO_FOR_CONTROLLED_LIVE_TESTS` этим прогоном не выдаётся. `PRODUCTION_READY` не объявляется. После публикации и независимого принятия конкретного release commit smoke идёт по [ROLLOUT_PLAN.md](ROLLOUT_PLAN.md): точный SHA, наличие guard-модулей и совпадение source manifest.

## Среда

| Объект | Факт |
| --- | --- |
| Staging | `/home/siovush/DevOrchestrator-acceptance`, ветка `acceptance/f3-47`, `adab54726e7427d83cf42d9241c570ec619dceec` |
| Production checkout | `/home/siovush/DevOrchestrator`, `6de216192084`, рабочее дерево чистое |
| Процессы | MCP PID 1541743 и TaskBridge PID 1541746 с 2026-10-09 11:54, `NRestarts=0`, `ActiveState=active` |
| Каталог обоих unit-ов | `/home/siovush/DevOrchestrator` |
| Интерпретатор | `/home/siovush/DevOrchestrator/.venv/bin/python`, Python 3.12.3, пакет `mcp` 2.3.0 |
| MCP | `127.0.0.1:8787`, состояние сокета LISTEN |
| Лимиты | coding 1, review 1, agents 2, machines `work=1`, `firefly=1`, очередь `work:queued` |
| Snapshot миграции | backup `20261010-131011`, 2 738 791 байт, sha256 `cbbef88149cf30325a813319fd326728e82f397942b00f193b7abcc0634479c5` |

К моменту последней read-only сверки, 2026-10-10 14:02:34 +0300, live state занимал 2 738 836 байт, inode `28068223`. Его по-прежнему переписывает старый bridge. Backup 13:10 не менялся: 2 738 791 байт, inode `28465580`. Этот снимок для rollback уже устарел. Свежий backup снимается непосредственно перед остановкой unit-ов.

## Изоляция

Тесты и probe запускались через `env -i` с `HOME` и `TMPDIR` внутри `.acceptance-lab/`. Импорт staging резолвил state в lab, не в live-файл. GitHub, Telegram, SSH и production MCP в этих прогонах не вызывались. Probe дополнительно подменял `subprocess` и bridge transport на немедленный отказ и не вызывал `ControlLoop.step()`. Venv и production checkout не изменялись.

## Результаты

| Проверка | Результат | Команда |
| --- | --- | --- |
| Полный regression после report latch | **PASS**, 717 tests, 345.296s, 0 failed, 0 errors, 0 skipped | см. ниже |
| `py_compile` | **PASS** | `/home/siovush/DevOrchestrator/.venv/bin/python -m py_compile *.py tests/*.py docs/acceptance/migration_probe.py` |
| `git diff --check` | **PASS** | `git diff --check` |
| Migration dry-run снимка | **PASS** | `docs/acceptance/migration_probe.py` |
| Lock #47 при `Scheduler.recover([])` и `release("47")` | **PASS**, lock сохранён | in-memory копия снимка |
| Совпадение lock key с текущим workspace identity | **PASS**, owner `47` | read-only `git rev-parse` локального worktree |
| Fsync file / directory | **PASS** | входят в 710 |
| Historical closed MERGE при чужом lock | **PASS**, journal не меняется, repository I/O нет | входят в 710 |
| Historical closed MERGE без ранее авторизованной отправки | **PASS**: нет PUT, нет нового `outcome` и `post_merge_convergence` | входят в 710 |
| Уже записанный `PENDING` и наблюдение `MERGED` | **PASS**: запись сохраняется, нового PUT нет | входят в 710 |
| Write amplification fixed | **PASS**, 0 authoritative writes | четыре окна по 60 с |
| Live MCP / GitHub / restart / doctor | **SKIP** | требуют отдельного разрешения |

Авторитетный прогон:

```sh
env -i \
  HOME=/home/siovush/DevOrchestrator-acceptance/.acceptance-lab/home \
  TMPDIR=/home/siovush/DevOrchestrator-acceptance/.acceptance-lab/tmp \
  PATH=/home/siovush/DevOrchestrator/.venv/bin:/usr/bin:/bin \
  LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH=/home/siovush/DevOrchestrator-acceptance \
  /home/siovush/DevOrchestrator/.venv/bin/python -m unittest discover -s tests -v
```

`Ran 710 tests in 280.263s` / `OK` / exit 0. Лог: `.acceptance-lab/evidence/unittest-latch.txt` (в git не входит). Прогон вне sandbox: внутри sandbox `killpg` получает `PermissionError`, это ограничение среды, не дефект кода.

До latch fix был прогон 693 tests / 343.833s / OK. В зачёт берётся 710.

## Migration dry-run

Команда, источник — только backup, запись — только в lab и redacted summary:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/acceptance/migration_probe.py \
  --source /home/siovush/.local/share/devorchestrator-rollout-backups/20261010-131011/task_bridge_state.json \
  --work /home/siovush/DevOrchestrator-acceptance/.acceptance-lab/migration \
  --output docs/acceptance/evidence/migration-summary.json \
  --label backup-20261010-131011
```

`ControlLoop()` выполнил `LegacyReviewRecovery.enroll()` и не делал poll. `enroll_wrote=false`. Первый `save_state` записал semantic-проекцию (`2 738 791` → `2 730 869` байт). Второй и третий save вернули «без изменений». Workflow digest проекции до и после совпал.

Сырой файл против сохранённой проекции: 0 добавлений, 0 изменений значений, 58 удалений. Все удаления — root `timings` (64 sample) и шесть volatile health-полей. Подробности: [migration-diff.json](evidence/migration-diff.json).

| Инвариант снимка | До | После enroll и semantic save |
| --- | --- | --- |
| Issues | 38 | 38 |
| Queue jobs | 90, все `DONE` | 90 `DONE` |
| `pending_rpc` | 0 | 0 |
| `review_transition` | 0 | 0 |
| `review_migration` | 0 | 0 |
| `repo_locks` owner | `47` | `47` |
| `reconciliation_retry` | `3`, `13`, `15`, `16` | те же, attempts `1` |
| Outbox | 457 `DONE` | без изменений digest |
| Publication journals | 12× COMMIT/PUSH/CREATE_PR/MERGE; 47 `done`, один MERGE без `done` | без изменений digest |

На загруженной копии `chief_repository_hold(#47)` истинно, `approval_complete(#47)` ложно, `lifecycle` = `AWAITING_CHIEF_REVIEW`, `Scheduler.release("47")` вернул false. `recover([])` не вызывал resolver и не изменил lock. `review_hold` issue #37 не изменился.

Повторное сравнение live с backup в 14:02 нашло два устойчивых отличия вне volatile health и `timings`. У live issue #37 больше нет `consistency_diagnostics.control`. У live issue #42 появился `reconciliation_retry` с ключами `attempts` и `next_attempt`, `attempts=1`; `review_status` остался `FAILED`. На снимке 13:10 этой записи не было. Lock owner по-прежнему только `47`, очередь 90 `DONE`, `pending_rpc=0`, `review_transition=0`, `review_migration=0`. Journal MERGE у #36 сохранил ключи `base`, `command`, `repo`, без `done`, без `outcome` и без `post_merge_convergence`. Это запись старого bridge за время приёмки, не результат probe. Откат поверх live файлом 13:10 стёр бы retry #42.

### Что означает «пять retries»

В снимке ровно четыре записи `reconciliation_retry`: #3, #13, #15, #16. Поля — `attempts` и `next_attempt`. К 14:02 старый bridge добавил такую же запись issue #42. Issue #37 такой записи не имеет ни в снимке, ни в live. Её состояние: `review_status=FRESH_REVIEW_REQUIRED`, `review_hold.previous_status=PASS`, `chief_status=STALE`, `chief_required=true`, lock не принадлежит #37. `chief_repository_hold` для #37 тоже истинно, но владелец `repo_locks` — #47. Миграция сохранила оба вида удержания и ничего не снимала.

### Issue #36

Закрытая issue (`runtime_exclusion=ISSUE_CLOSED`) с `owner_state=PR_CREATED`, `merged=false`, method `merge`. COMMIT/PUSH/CREATE_PR помечены `done`. MERGE содержит `command`, `base`, `repo` и не содержит `done` / `outcome`. `post_merge_convergence` нет.

Этого пути нет в production `6de2161`. Новый `reconcile_archived` рассматривает такой journal как незавершённый. Две проверки на fixture:

- Пока lock того же repository принадлежит другому owner, recovery делает MCP-отчёт и identity-чтение, затем выходит. Journal, convergence и repository calls не меняются. Текущий lock #47 совпадает с identity `work` + канонический toplevel `/home/siovush/DevOrchestrator-dev` (`git rev-parse`, без SSH). Семь queue jobs #47 ссылаются на тот же lock.
- Если lock не мешает и snapshot workspace совпадает с историческим commit, merge PUT не отправляется. Для journal без уже авторизованной отправки ядро больше не пишет `outcome` и `post_merge_convergence`. Уже принятый `MERGED` или ранее записанный `PENDING` сохраняется и не приводит к новому PUT.

Сейчас worktree `dev/self-development` dirty, HEAD не равен commit issue #36. Controlled start всё равно обязан увидеть lock owner `47` до старта bridge и после первого poll убедиться, что journal #36 не получил `outcome`.

## Pre-live hardening

- `reconcile_only` отказывает в новом MERGE до записи `PENDING`, если нет latch уже авторизованной или принятой операции. Неопределённый `PENDING` и наблюдение `MERGED` не получают второй PUT.
- `migration_probe.allowed_removal` пропускает только root `timings` и шесть volatile-полей по пути `issues.*.runtime_health.*`. Summary и work directory отказываются писать вне staging, в production checkout и в live state.
- Smoke latch `DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS=1` закрывает GitHub-мутации, publication subprocess, spawn/fence и worker RPC до внешнего вызова. Проверка capability — `docs/acceptance/capability_probe.py`, без bridge и без выбора другого tool.

## Failure injection

Все сценарии ниже входят в 710 и завершились `ok`.

| Сценарий | Наблюдение | Тесты |
| --- | --- | --- |
| Медленный RPC | Polling других команд продолжается, launch intent один | `test_slow_initial_rpc_keeps_control_polling_and_single_launch_intent`, `test_slow_reviewer_rpc_does_not_block_owner_polling_or_duplicate_launch` |
| Timeout / зависание | Слот живой операции не освобождается отрицательным ACK; после deadline receipt не запускает второй callback | `test_control_polling_continues_during_rpc_cancellation_cleanup`, `test_deadline_after_worker_acceptance_replays_receipt_without_duplicate` |
| Обрыв соединения | Ошибка после возможного принятия не делает второй POST; отсутствие ответа не считается доказательством отсутствия эффекта | `test_github_comment_accepted_before_crash_is_not_repeated`, `test_delayed_comment_post_never_resends_on_absence_after_restart`, `test_telegram_crash_after_acceptance_restart_does_not_resend` |
| Crash между checkpoint | Повтор использует тот же request id / journal и не создаёт второй внешний эффект | `test_crash_after_pending_receipt_cannot_trigger_second_callback`, `test_accepted_unacknowledged_commit_survives_closure_and_two_restarts`, `test_accepted_review_response_lost_heartbeat_polling_and_restart_no_relaunch` |
| Ambiguous launch | ABSENT не доказывает отсутствие старта; prepared/generation-less legacy удерживается | `test_absence_does_not_claim_no_launch_or_create_receipt`, `test_prepared_legacy_launch_is_held_without_resume_or_observation_rewrites`, `test_changed_prepared_binding_is_uncertain_and_cannot_be_fenced` |
| Duplicate completion | Повторный receipt и повторная projection не делают второй launch, comment или close | `test_receipt_replays_never_launch_twice_even_after_error`, `test_completion_projection_requires_convergence_and_is_idempotent` |
| Закрытие issue во время операции | Новые COMMIT/PUSH/CREATE_PR/MERGE и coding launch не авторизуются; уже принятый результат сверяется без второго primary effect | `tests/test_publication_closure.py`, `test_close_observed_during_preflight_prevents_new_launch` |
| Failed fsync | Ошибка file fsync до rename оставляет прежний checkpoint. Ошибка directory fsync после rename не вызывает `submit`. `DurabilityFailure` является `BaseException` и не попадает в `except Exception` у `main` | `test_file_fsync_failure_keeps_checkpoint_and_blocks_dispatch`, `test_directory_fsync_failure_after_replace_still_blocks_dispatch`, `test_control_reducer_save_failure_stops_before_further_dispatch` |

`Restart=always` после `DurabilityFailure` поднимет процесс заново уже с диска. Тот же шаг не продолжает dispatch из грязной памяти. Устойчивая ошибка диска выглядит как crash loop, и это ожидаемая остановка, а не повод повышать лимиты.

## Worker / MCP

Production MCP на диске `6de2161` не экспортирует `get_transition_capabilities`, `inspect_start_request`, `review_binding`, `repository_binding`. Новый bridge посылает `repository_binding` на новом coding start и требует точный capability dict перед reviewer prepare. Отсутствующая capability не включает legacy fallback.

Каждый tool call исполняет `devctl.py` с диска. Обновление checkout при живом процессе смешивает старый код в памяти родителя и новый `devctl.py` в subprocess. Безопасное переключение — остановить оба unit-а, затем сменить дерево, затем поднять MCP, проверить capability, затем поднять bridge. Порядок и rollback: [ROLLOUT_PLAN.md](ROLLOUT_PLAN.md).

Worker receipts сейчас: 88 файлов, все `STARTED`; `signature_version` отсутствует у 84 и равен 2 у 4. Версий 3, 4 и статуса `FENCED` нет. `worker_leases.json` пустой. Новый worker не переписывает signature 1/2. После появления v3/v4, `review_transition`, `review_migration` или `FENCED` откат бинарника запрещён: старый `launch()` воспринимает receipt без signature как уже выполненный запуск, если вызывающий передал пустую signature.

## Durability

Изолированный fixture около 2,28 МБ, cadence 10 с / discovery 30 с, внешние чтения подменены. Окна по 60 с:

| Окно | Authoritative replacements / bytes | Telemetry replacements / bytes | `write_bytes` / `syscw` | Authoritative content |
| --- | ---: | ---: | ---: | --- |
| baseline | 10 / 22 822 909 | 0 / 0 | 22 855 680 / 30 | изменился |
| fixed | 0 / 0 | 10 / 7 043 | 40 960 / 10 | inode, mtime и bytes стабильны |
| deferred baseline | 5 / 11 417 965 | 16 / 16 433 | 11 493 376 / 31 | изменился; notification осталась `PENDING`, payload изменился |
| fixed + deferred | 0 / 0 | 16 / 16 441 | 65 536 / 16 | стабилен; notification `PENDING`, payload стабилен, отправки нет |

В каждом окне одно discovery-чтение и пять report-чтений. Размер authoritative fixture: 2 281 718 байт в fixed и 2 282 484 в baseline. JSON: `docs/acceptance/evidence/measurement-*.json`.

Числа deferred-baseline отличаются от исторического integration-отчёта (там 4 authoritative replacements). Критерий приёмки выполнен: fixed-окна не перезаписывают authoritative state, а semantic transition по-прежнему проходит через `write_json_if_changed` и покрыт `test_semantic_transition_and_critical_receipts_persist_immediately`.

На реальном снимке первая запись нового writer — одноразовая проекция, дальше idle save молчит. После включения production ожидается одна такая запись, затем стабильные inode/mtime на poll без нового workflow.

## Оставшиеся риски

1. GitHub read и уже отправленный Git/PR/merge effect не атомарны. Закрытие после dispatch требует сверки journal, а не повторной отправки.
2. Prepared stale requests, legacy runtime без generation и недоказуемое внешнее принятие остаются на удержании. Автоматического снятия lock нет.
3. Исторический MERGE #36 без `done` опасен только если lock #47 исчезнет и workspace окажется на старом commit. Код отказывает в новом PUT и не записывает новый `PENDING`, пока отправка раньше не была авторизована.
4. `doctor` / `orchestrator_health` при выключенном latch ходит по SSH на firefly и запускает version-probe. При latch tool отказывает до `Popen`. В этой приёмке doctor не запускался.
5. Локальный fsync и расписание CPU не дают real-time гарантии. Shutdown ждёт уже отправленный I/O.
6. Cursor/Codex/SSH soak и реальный poll production bridge не выполнялись.
7. При latch `get_task_report` и `list_tasks` не запускают cleanup и не пишут worker metadata. Capability и `inspect_start_request` без fence по-прежнему только читают. `orchestrator_health`, catalog `--discover` и preflight без receipt при latch не probe-ят. Разрешённые GitHub-чтения poll по-прежнему выполняются. Локальный `projection.health` может стать `DRIFT`, если наблюдаемые labels отстают от desired label; сама правка label при latch не отправляется. Успешный durable `DONE` по-прежнему может поставить локальный desired label `dev:ready-for-review` и `PENDING` outbox; отправка закрыта latch-ем. Это не `FAILED`, не retry и не cleanup proof.

## Live-проверки, которые ещё нужны

Их нет в вердикте как уже сделанные, и сейчас они запрещены. Точные шаги и критерии остановки — в rollout plan. Кратко, только после принятого release commit: quiescence обоих unit-ов и любых процессов `devctl`, recovery backup после этого, проверка SHA и source manifest, смена checkout на этот commit, старт только MCP с latch, `capability_probe.py`, затем bridge с тем же latch.

## Самопроверка

- Production `git status --porcelain` пуст, HEAD остался `6de216192084`. Этот проход unit-ы, live state и backup не читал и не менял. PID и `NRestarts` из baseline этим проходом не перепроверялись.
- Полный прогон после report latch: 717 tests, 345.296s, OK, exit 0. Лог `.acceptance-lab/evidence/unittest-report-latch.txt`. Предыдущий полный прогон того же дерева один раз упал на `test_accepted_push_is_reconciled_after_closed_restart` (`OPEN != CLOSED`); отдельно и во втором полном прогоне этот тест прошёл.
- Backup и live drift, зафиксированные раньше, в этот проход не перечитывались.
- Записи приёмки: staging docs/tests, `.acceptance-lab/` (gitignore), redacted JSON в `docs/acceptance/evidence/`.
- В отчёт и evidence не попали prompts, тела комментариев, токены и env-файлы.
- Production checkout не менялся. `source_manifest.py --root` на production завершается отказом: файла `external_effects.py` там нет. В staging исправлены archived MERGE, probe guard и smoke latch. Commit, push и переключение не делались.

## Diff для ревью

- `.gitignore` — каталог `.acceptance-lab/`
- `external_effects.py` — latch `DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS=1`; GitHub-чтения только точным allowlist
- `notifications.py`, `transition_executor.py` — Telegram и dispatcher-отказ до транспорта
- `bridge_effects.py`, `task_bridge.py`, `control_loop.py` — запрещённый новый эффект остаётся `PENDING` без роста `attempts` и без `SENDING`/`UNCERTAIN`; launch authority не создаётся
- `publication.py` — `reconcile_only` не создаёт новый `PENDING` без уже авторизованной отправки; publication subprocess закрыт latch-ем
- `devctl.py`, `worker_runtime.py`, `runtime_liveness.py`, `remote_worker.py`, `mcp_server.py` — latch до spawn, fence, cleanup, remote report и worker RPC
- `tests/test_acceptance_report_latch.py`
- `tests/test_acceptance_fsync.py`
- `tests/test_acceptance_archived_merge.py`
- `tests/test_acceptance_effect_gate.py`
- `tests/test_acceptance_effect_latch.py`
- `tests/test_acceptance_migration_probe.py`
- `docs/acceptance/source_manifest.py`
- `docs/acceptance/PLAN.md`
- `docs/acceptance/PROGRESS.md`
- `docs/acceptance/ACCEPTANCE_REPORT.md`
- `docs/acceptance/ROLLOUT_PLAN.md`
- `docs/acceptance/migration_probe.py`
- `docs/acceptance/capability_probe.py`
- `docs/acceptance/evidence/*.json`

## 2026-10-10 — Final Safety Latch remediation

This addendum covers the acceptance working tree only. Implementation,
regressions and self-review are complete. Independent review and owner
authorization remain required. No production
checkout, live state, receipt, service, remote worker or other worktree was used
or changed. There was no repository commit, push, merge, restart or live smoke.
Tests used disposable local Git repositories and worker processes, isolated HOME,
mocked GitHub/Telegram transports and mocked remote cleanup.

### Root causes and decision

F1 was an authorization defect, not just a missing transport check. Latched
reports could advance Bridge's desired labels, signatures and workflow gates,
then create ordinary PENDING effects. A later flush or projection repair treated
those durable observations as authority after the environment flag disappeared.
Temporary withholding also gave pre-upgrade smoke entries no distinct origin.

The boundary now treats read-only observations as incapable of authorizing
workflow changes. Reports carry `observation_confidence=READ_ONLY`. Bridge's
single writer records a durable `safety_latch_hold` before reading/applying them;
ordinary report reducers, archived reconciliation, detached result adoption,
projection repair and dispatch respect that hold. Latched polling uses a separate
observational path without scheduler recovery or command consumption. Scheduler
retains the original queue ownership and repository locks, including recovery
after restart. Status exposes `OBSERVATION_HELD` and an authorization audit action.

New effects originating under the latch are durable QUARANTINED tombstones with
`effect_origin=safety_latch`. Re-enqueue cannot authorize them, even if event
wording changes or an old reset path changes status to PENDING. Dispatch checks
origin as well as status before callbacks. Enqueue also holds the tracked issue,
preventing regeneration under a different event key. Removing the environment
flag clears neither issue holds nor effect quarantine.

Normal new entries record `effect_origin=runtime`; this is provenance, not a
replacement for Chief/owner approval or freshness gates. Pre-existing entries
with this provenance remain intact and can proceed through the existing gates
after removal. Unrelated issue entries are preserved. A pre-upgrade PENDING entry
on an observed issue has no evidence distinguishing legitimate work from smoke:
it is retained as `legacy_unverified`/QUARANTINED, with payload, key, attempts and
retry information preserved. Both Bridge flush and the generic outbox API perform
this migration. SENDING/UNCERTAIN crash receipts and completion comments already
across their send boundary retain their reconciliation semantics. Nothing is
silently deleted. There is deliberately no automatic quarantine release.

F2 came from trusting exit code zero and cached terminal status without the
normal runtime's final-result parser. Read-only local and remote diagnosis now
uses the same parser, validates generation-bound liveness when present, rejects
prepared/new turns and incomplete remote logs, and checks exit-marker agreement.
Empty/invalid final output with exit zero yields FAILED when bound terminal
evidence establishes that failure; valid final output yields DONE. Cached terminal
metadata alone and stale/mismatched receipts yield UNKNOWN. Every latched result,
including deterministic DONE/FAILED diagnosis, remains non-actionable in Bridge.
No final-report failure receipt is written by a read-only observation.

F3 came from synthesizing RUNNING_HEALTHY when health was absent and trusting a
cached LOST_ORPHANED observation as current failure evidence. Latched health now
reports UNKNOWN/reconciliation_blocked, with no ownership probes or writes. A
missing or cached health receipt cannot promote RUNNING or FAILED in Bridge.

### MCP, CLI and remote audit

An environment variable does not cross an MCP network connection. Latched Bridge
therefore requests the distinct `get_task_observation` and
`list_task_observations` tools. They force the child devctl environment to enable
the latch even when the server process is unlatched. Ordinary report tools retain
their defaults and optionally support an explicit read-only request. Separate
tool names are essential: the installed MCP SDK's argument model can silently
drop an unknown optional mode argument on an older server. An older server lacking
the observation tool returns an error; Bridge never falls back to a mutating
report. Dispatch provenance is retained across in-flight latch removal and nested
review/discovery results.

| Entry point | Latched behavior |
| --- | --- |
| Registered report/task-list tools | Local receipt reads, parsed completion, UNKNOWN health, READ_ONLY provenance; no cleanup, SSH or worker writes |
| Dedicated observation tools | Same behavior, with the latch forced in the devctl child regardless of server environment |
| Static projects/capabilities/catalog; non-fencing request inspection | Observational only |
| Start/continue/adopt; fencing; doctor; runtime catalog discovery; new live preflight | Refused before launch/probe/mutation; direct server preflight may inspect an existing receipt without probing |
| Bridge MCP dispatch | Exact observation allowlist; doctor, live preflight/discovery, unknown tools and mutations refused |
| `devctl exec`, spawn/runtime runner and cleanup attestation | Refused before subprocess, worker metadata or cleanup proof mutation |
| Direct Firefly SCRIPT | Start refused before mkdir/lock; report reads bounded logs and returns UNKNOWN/READ_ONLY without watchdog writes or cleanup |
| Telegram setup and operational `mcp_smoke` CLI | Refused before credentials/input/config writes/network; the latter cannot send doctor/recovery requests to an unlatched server |
| GitHub, Telegram and publication transports | Existing verified send guards, exact receipts and Chief/owner gates retained |

No in-scope automated bypass remains in the audited entry points. This is a
process/request boundary, not an operating-system sandbox or a way to cancel
external work that was already dispatched with legitimate authority. Arbitrary
external clients directly calling an unlatched server's ordinary report tool do
not inherit another process's environment latch; controlled smoke uses the
observation endpoints. No such external client or production service was invoked.

### F4 — separate control-loop finding

The stale discovery result can still overwrite the fresher CLOSED value in the
in-memory issue cache. The regression reproduces this explicitly. Durable
`issue_closed`/archival remains sticky, and existing publication/launch boundaries
check closure. A held issue is excluded from discovery application and lifecycle
dispatch, including after latch removal. Thus F4 does not restore authority to
smoke observations and does not block these Safety Latch criteria. Discovery
ordering itself is deferred for separate review; no unrelated ordering refactor
was introduced.

### Verification and replay evidence

Final targeted result: **55 tests, 9.773s, PASS; 0 failures, 0 errors, 0 skips**.
Final full result: **737 tests, 320.331s, PASS; 0 failures, 0 errors, 0 skips**.

The authoritative runs use the acceptance venv and start with an empty inherited
environment. From `/home/siovush/DevOrchestrator-acceptance`:

```sh
env -i HOME="$PWD/.acceptance-lab/final-remediation/home" \
  TMPDIR="$PWD/.acceptance-lab/final-remediation/tmp" \
  PATH="$PWD/.venv/bin:/usr/bin:/bin" PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH=tests:. .venv/bin/python -m unittest \
  test_safety_latch_final test_acceptance_report_latch \
  test_acceptance_effect_latch test_acceptance_effect_gate \
  test_acceptance_archived_merge -v

env -i HOME="$PWD/.acceptance-lab/final-remediation/home" \
  TMPDIR="$PWD/.acceptance-lab/final-remediation/tmp" \
  PATH="$PWD/.venv/bin:/usr/bin:/bin" LANG=C.UTF-8 LC_ALL=C.UTF-8 \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=tests:. \
  .venv/bin/python -m unittest discover -s tests -v
```

Logs, the 17-source manifest and dependency versions are retained under
`.acceptance-lab/final-remediation/evidence/`. The source manifest includes the
new safety boundary modules; it does not identify HEAD `adab547` as an accepted
release. `py_compile` of all workspace/test/acceptance Python sources and
`git diff --check` also pass.

Evidence: [targeted log](../../.acceptance-lab/final-remediation/evidence/targeted.log),
[complete suite log](../../.acceptance-lab/final-remediation/evidence/full.log),
[verification summary](../../.acceptance-lab/final-remediation/evidence/verification.json),
[source manifest](../../.acceptance-lab/final-remediation/evidence/source-manifest.json).
Twenty new regression cases were added; the complete run includes the existing
717-test baseline. Intermediate complete runs of 728 and 731 tests also passed,
but only the 737-test run validates the final implementation.

The new `tests/test_safety_latch_final.py` covers the required A–E scenarios and
the deferred paths found in self-review. Its combined polling/restart test reads
terminal and running reports through registered MCP dispatch, executes tick,
control pass and ControlLoop polling, and asserts unchanged worker files and zero
process signals, worker spawning and SSH mutations. No actionable workflow state
or new report effect is recorded. Three legacy smoke effects are quarantined.
After latch removal and a real durable reload, repeated flush and reconciliation
publish only the unrelated, pre-authorized comment; the smoke effects stay
quarantined, no replacements appear, and original queue/lock ownership survives.

Separate tests cover new quarantined effects, regeneration with a different key,
status-reset attempts, the generic flush API, missing health, stale generation,
remote cached completion, valid/invalid final results, delayed nested results,
read-only reports embedded in inline launch responses (original pending request
and reservation retained), the unlatched-server child environment and refusal of
older observation protocols.
Existing normal-runtime tests cover cleanup/recovery, exact GitHub comment
reconciliation, Telegram uncertainty, Chief/owner approval, publication closure,
archived MERGE behavior, request IDs and durability. The verified GitHub, Telegram,
Chief outbox and archived MERGE fixes were not rewritten.

An initial targeted fixture failed because its unrelated queued issue correctly
generated normal intake rejection effects after latch removal. The fixture now
represents an independent authorized outbox operation without introducing new
queue work. The sandboxed executor test stalled with an idle worker thread and a
pending loop future; its diagnostic is retained. Isolated regressions ran outside
the sandbox after approval and passed. The new real task-list child test also
exposed the intentionally absent acceptance `projects.json`; its fixture now
injects a disposable registry inside the child while retaining the adapter's
forced environment. No production registry was read or copied. These conditions
were investigated rather than dismissed as flaky.

### Files and remaining risks

This remediation changes `bridge_effects.py`, `control_loop.py`, `devctl.py`,
`external_effects.py`, `mcp_server.py`, `mcp_smoke.py`, `operational_state.py`,
`remote_worker.py`, `runtime_liveness.py`, `scheduler.py`, `task_bridge.py`,
`telegram_setup.py` and `transition_executor.py`; adds
`tests/test_safety_latch_final.py`; clarifies one pre-existing effect test's name;
and updates this report and `source_manifest.py`. Pre-existing working-tree edits
to `notifications.py`, `publication.py`, `worker_runtime.py` and `.gitignore` were
preserved. The acceptance workspace now has its own ignored test venv (MCP 2.3.0);
the production interpreter was not used or changed.

Holds intentionally trade automatic recovery for safety. Resuming an observed
issue requires an explicit owner-approved audit/recovery of its workflow and
effect authority; a later poll, environment change or status reset is insufficient.
Pre-upgrade effects cannot be reliably attributed retrospectively without outside
evidence. Legitimate ambiguous entries remain available for that audit. F4 remains
a separate cache-ordering defect. Any failed quarantine checkpoint must be
resolved before using state with the latch removed. No automatic hold-release
command or approval bypass was added.

Final recommendation: **PASS** for Safety Latch remediation only.
Await independent review and owner authorization; production activation is not
authorized by this report.
