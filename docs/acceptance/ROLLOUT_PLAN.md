# План controlled rollout после принятого release commit

Этот документ готовит переключение. Он не является разрешением его выполнить. Останов unit-ов, смена production checkout, вызов production MCP и любой GitHub/worker effect делаются только после отдельного разрешения. `PRODUCTION_READY` здесь не утверждается.

Live activation сейчас запрещён. Исправления latch лежат в рабочем дереве staging и не опубликованы. HEAD staging остаётся `adab54726e7427d83cf42d9241c570ec619dceec`; в этом commit нет исправленного `external_effects.py`. Controlled smoke выполняется только на конкретном release commit, который содержит эти исправления и `external_effects.py`, после того как этот commit опубликован и независимо принят.

Цель будущего controlled live test: поднять совместимую пару MCP+bridge на этом принятом commit, убедиться в capability, в latch и в сохранности lock/journals, и остановиться до новой coding/review публикации.

## Почему переключение атомарное

Оба unit-а используют один `WorkingDirectory=/home/siovush/DevOrchestrator`. MCP на каждый tool call запускает `devctl.py` с диска. Живой процесс держит старый `mcp_server.py` в памяти. Смешанное состояние «новый файл на диске, старый процесс в памяти» не используется.

Новый bridge несовместим с MCP `6de2161`: coding start передаёт `repository_binding`, reviewer prepare требует ответ

```json
{"schema_version": 1, "review_binding": 1, "start_request_reconciliation": 1}
```

Старый MCP этих аргументов и tools не имеет. Отказ capability не включает запуск без binding.

## Preflight, всё ещё read-only

Повторить непосредственно перед окном, потому что live state старого bridge меняется:

1. `git -C /home/siovush/DevOrchestrator status --porcelain` пуст, HEAD `6de216192084`.
2. Оба unit-а `active/running`. Записать PID и `NRestarts`.
3. На копии свежего state, не на живом файле, подтвердить:
   - `pending_rpc = 0`
   - `review_transition = 0`
   - единственный `repo_locks` owner = `47`
   - identity `work` + toplevel DevOrchestrator-dev совпадает с этим lock key
   - `reconciliation_retry` по-прежнему только у тех issue, которые видны в снимке; #37 остаётся `FRESH_REVIEW_REQUIRED` / `review_hold`, без превращения в retry
   - MERGE journal #36 без `done` и без `outcome`
   - в `requests/` нет `signature_version` 3/4 и нет `FENCED`
4. Worktree `/home/siovush/DevOrchestrator-dev` не находится на commit issue #36. Исторический MERGE без уже авторизованной отправки больше не получает новый `PENDING`, но чистое совпадение HEAD всё равно отменяет окно: recovery не должен даже доходить до сверки этого journal.
5. Свободное место на файловой системе state: одна проекция около 2,7 МБ плюс копии backup. Лимиты capacity не менять: coding 1, review 1, agents 2, `work=1`, `firefly=1`.

Команды structural-сверки — `docs/acceptance/migration_probe.py` и read-only сравнение digest. В вывод попадают счётчики, owner и enum-статусы. Prompts, comments и env не печатать.

## Recovery backup после quiescence

Копия, снятая при живом bridge или MCP, не является recovery backup: bridge пишет `task_bridge_state.json`, MCP через `devctl.py` пишет `requests/`, `tasks/`, leases и lock-файлы. `Restart=always` поднимет unit снова, если его не остановить через systemd.

Порядок:

1. Read-only preflight выше. Это инвентарь, не копия для отката.
2. `systemctl --user stop devorchestrator-task-bridge.service`. Дождаться `inactive` и отсутствия процесса `task_bridge.py`.
3. `systemctl --user stop devorchestrator-mcp.service`. Дождаться `inactive`, отсутствия `mcp_server.py` и `devctl.py`, и того, что `127.0.0.1:8787` не слушает.
4. Quiescence до начала копирования. Не копировать, пока любой из пунктов не пуст:
   - процессы, в командной строке которых есть `task_bridge.py`, `mcp_server.py` или `devctl.py`;
   - процессы, держащие открытыми файлы в `/home/siovush/.local/share/devorchestrator` (`lsof` или `fuser`).
5. Только теперь копировать каталог `0700`. Минимум:
   - `task_bridge_state.json`
   - `requests/`
   - `tasks/`
   - `worker_leases.json`
   - `worker_consistency.json`
   - `worker.lock` и `bridge.lock` как факты наличия, без удаления
   - `/home/siovush/DevOrchestrator/projects.json`
   - unit-файлы и drop-in `30-work-enrollment.conf`
   - копии `telegram.env` и `network.env` в тот же закрытый каталог, без включения в отчёт и git
6. Пока писатели всё ещё отсутствуют, записать sha256 и размер state и receipts. Копия лежит вне `/home/siovush/.local/share/devorchestrator` и читается как JSON.
7. Повторить проверку процессов и `lsof` после копирования. Если за время копии появился писатель, копия недействительна: остановить его и снять копию заново.

Снимок `20261010-131011` для отката уже не годится. К 14:02 live state новее: у #37 снят `consistency_diagnostics.control`, у #42 появился `reconciliation_retry` (`attempts=1`, `review_status=FAILED`). В recovery backup входит фактический набор на момент quiescence, а не этот старый снимок.

## Переключение

1. Quiescence и recovery backup из предыдущего раздела уже выполнены. Оба unit-а `inactive`. Checkout ещё `6de2161`.
2. Поставить drop-in обоим unit-ам до первого старта нового кода:

```
[Service]
Environment=DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS=1
```

Значение ровно `1`. Это не комментарий в плане и не ожидание, что bridge «не вызовет» команду. Пока переменная в окружении процесса нового кода, GitHub-мутации, Telegram, publication subprocess и worker launch/continue/adopt/fence поднимают `ExternalEffectBlocked` до `subprocess`, до сети и до RPC. Разрешены только точные read-формы: paginated poll открытых issues, comments и labels, плюс два `gh issue view` с фиксированным `--json`. `gh api` само по себе чтением не является: `-X`, `--method=`, `-F`, `-f` и любой лишний аргумент блокируются. `get_task_report` и `list_tasks` при latch остаются чтением уже записанного статуса: они не вызывают `cleanup_group`, не шлют `SIGTERM`/`SIGKILL`, не делают SSH report на Firefly и не пишут worker metadata, receipts или leases. `orchestrator_health`, `execution-catalog --discover` и preflight без уже существующего receipt отказывают до probe. Новый запрещённый outbox-эффект остаётся `PENDING` и не увеличивает `attempts`. Уже записанный `SENDING` после настоящего POST по-прежнему сверяется как неоднозначный receipt. Такое чтение не создаёт `reconciliation_retry`, approval, cleanup proof и не переводит задачу в `FAILED` только потому, что cleanup заблокирован. При выключенном latch обычный cleanup и remote refresh работают как раньше. Старый checkout эту переменную игнорирует, поэтому unit-ы не запускать, пока checkout не сменён. Снятие latch — отдельное позднее разрешение, не часть этого smoke.
3. В остановленном checkout перейти только на принятый release commit. `adab547` для этого шага не используется. Перед fast-forward, пока процессы ещё не запущены:
   - `git rev-parse HEAD` после обновления равен SHA, записанному в независимом принятии;
   - `git status --porcelain` пуст;
   - `external_effects.py` есть в дереве;
   - `docs/acceptance/source_manifest.py --root <checkout>` печатает JSON, совпадающий с manifest того же принятия. Отсутствие `external_effects.py` или любого файла из списка завершает скрипт с ненулевым кодом. Это NO-GO.
   Не копировать staging `.acceptance-lab` и приёмочные docs в production, если они не входят в commit.
4. На диске принятого commit должны быть `get_transition_capabilities`, `inspect_start_request`, аргументы `--review-binding` и `--repository-binding`, а также `external_effects.py` с sha256 из manifest. Процессы ещё не запущены.
5. Запустить только MCP. Bridge не запускать.
6. Capability check — только `docs/acceptance/capability_probe.py --url http://127.0.0.1:8787/mcp`. У скрипта нет аргумента tool: единственный вызов зашит как `get_transition_capabilities`. Ожидаемый объект — dict из трёх полей выше. Не запускать `mcp_smoke.py`: он вызывает `orchestrator_health` и `list_tasks`. Handler capability не вызывает `devctl`.
7. Если capability не совпала: MCP остановить, drop-in с latch снять, checkout вернуть на `6de2161`, MCP и bridge поднять в прежнем порядке. State не восстанавливать, если новый процесс его не писал. На этом шаге bridge ещё не запускался, проекции быть не должно.
8. Перед стартом bridge доказать latch снаружи процесса, не по журналу команд. Прочитать drop-in-файл и из `/proc/<pid>/environ` MCP вывести только `DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS`. Должно быть `1`. Иначе NO-GO, даже если в логе нет `start_task`.
9. Запустить bridge с тем же drop-in. Сразу так же проверить environ bridge. Первый checkpoint может один раз переписать state, убрав `timings` и шесть volatile health-полей. Это локальная проекция, не внешний эффект.
10. Наблюдать не меньше двух минут и не меньше трёх poll-интервалов по 10 с. GitHub-чтения poll разрешены latch-ем. Мутации нет: попытка упирается в исключение до внешнего вызова, а не в отсутствие строки в логе.

## Health и GO / NO-GO

GO для продолжения наблюдения, всё ещё без новой публикации:

- оба unit-а `active/running`, `NRestarts` не растёт;
- capability совпала до старта bridge, клиентом был только `capability_probe.py`;
- у MCP и bridge в environ ровно `DEVORCHESTRATOR_BLOCK_EXTERNAL_EFFECTS=1`;
- owner lock по-прежнему `47`, `release` логически закрыт Chief-hold (`AWAITING_CHIEF_REVIEW`, approval не завершён);
- `pending_rpc=0`, `review_transition=0`, новых `review_migration` нет;
- retries #3/#13/#15/#16 на месте; #37 сохраняет `FRESH_REVIEW_REQUIRED` и `review_hold`;
- journal #36 не получил `outcome` и не получил `post_merge_convergence`;
- после первой проекции authoritative file не заменяется на каждом poll: стабильные размер, inode и mtime при живом процессе;
- GitHub не получает новых комментариев, label transition и merge/PR вызовов от этого старта;
- receipts не получают signature 3/4 и `FENCED`, пока никто не запускает новый coding/review.

NO-GO, немедленная остановка bridge, затем MCP:

- capability не совпала или MCP отверг binding-аргумент;
- latch отсутствует в drop-in или в environ любого из двух процессов;
- lock #47 исчез, сменил owner или `pending_rpc` стал ненулевым без операторского запуска;
- journal #36 получил `outcome` / `PENDING` / convergence;
- authoritative file перезаписывается каждые 10 с после первой проекции;
- `NRestarts` растёт (`DurabilityFailure` и `Restart=always` дают crash loop при ошибке диска);
- в логе bridge есть попытка `start_task` / publication mutation;
- любой неопределённый external effect: комментарий или label мог быть принят. Повторять его нельзя.

## Мониторинг после GO наблюдения

Смотреть unit state, `NRestarts`, размер и mtime `task_bridge_state.json`, счётчики `pending_rpc`, `review_transition`, owner lock, journal #36. Телеметрия `task_bridge_telemetry.json` может обновляться; она не участвует в recovery. Рост telemetry при стабильном authoritative file совместим с приёмкой.

Не повышать `MAX_CODING`, `MAX_REVIEW`, `MAX_AGENTS`. Не снимать lock вручную. Не удалять `requests/`, phase records и journals, чтобы «упростить» откат.

`doctor` и полный `mcp_smoke.py` в это окно не входят: health-tool ходит на firefly по SSH. Отдельное разрешение нужно и на version-probe, и на любой реальный Cursor/Codex start.

## Rollback

Откат бинарника разрешён только пока после старта нового bridge не появились `review_transition`, `review_migration`, signature 3/4 или `FENCED`.

1. Остановить bridge, затем MCP.
2. Сравнить durable-проекцию текущего state с backup этого окна.
3. Если отличие только в удалённых `timings` и шести volatile health-полях, вернуть checkout на `6de2161` и поднять MCP, затем bridge. State можно оставить проекцией: старый код дописывает volatile поля сам. Восстанавливать backup только если нужно вернуть байты снимка и после остановки не было workflow-переходов.
4. Если есть любой workflow-переход, новый phase record или новый receipt, бинарник не откатывать и backup поверх текущего state не класть. Идти вперёд от текущего файла.

Старый `worker_runtime.launch` при receipt без signature и пустой signature вызывающего считает запуск уже состоявшимся. Поэтому `FENCED` и v3/v4 несовместимы с откатом worker.

## Если во время окна появились phase records

Сохранить файл как есть. Не удалять record, не переводить phase в другое значение и не вызывать fence вручную. Остановить новые запуски отказом от операторских команд, оставить процесс на новом коде и разбирать конкретный request id по receipt: `STARTED`/`PENDING` удерживают ownership, `ABSENT` не доказывает отсутствие запуска, `FENCED` допустим только когда worker сам записал tombstone при отсутствии metadata и lease.

Issue #47 в этом окне не получает автоматический Chief verdict и не публикуется. Chief остаётся внешним шагом. Lock остаётся, пока `chief_repository_hold` истинно.

## Что эта приёмка уже доказала без live start

Изолированный regression, migration копии снимка, сохранение lock #47, отказ нового MERGE без ложного `PENDING`, нулевая authoritative-запись idle fixture и fail-closed latch на GitHub, Telegram, outbox, publication и worker launch. Этого достаточно, чтобы готовить controlled live test на будущем принятом commit, и недостаточно, чтобы активировать его сейчас. Пока release commit не опубликован и не принят, шаги остановки unit-ов и смены checkout не выполняются. `PRODUCTION_READY` не утверждается.
