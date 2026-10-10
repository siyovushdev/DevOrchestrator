# Исправление Chief evidence delivery после worker.lock contention

## Статус и границы

Это follow-up к независимому integration review F1/F2/F3 + #47. Исправление
реализовано только в `/home/siovush/DevOrchestrator-integration`.
**108 целевых PASS; полный набор 677 PASS за 263,395s, exit 0, без пропусков.**
Работа готова к независимой проверке. Код и тесты после начала полного прогона не
менялись. Первоначальные 669 PASS относятся к предыдущей реализации.

Production, live state, конфигурации, сервисы и исходные worktrees не изменялись.
Commit, push, PR, merge, deployment и restart сервисов не выполнялись. Git fixtures,
локальные dummy workers и восстановление объектов в тестах изолированы во временных
каталогах; GitHub/Telegram POST и provider RPC заменены deterministic fakes.

## Причина и исправление

Outbox правильно checkpoint-ит `SENDING` до потенциальной отправки. Однако callback
Chief comment до POST пытается взять `worker.lock`. Отказ `flock(LOCK_NB)` прежде
попадал в общий exception handler: `SENDING -> UNCERTAIN`, хотя POST не начинался.
Отсутствие exact receipt после освобождения lock не доказывает no-send, поэтому
прежний reconciliation навсегда удерживал такой effect.

Теперь `CommentNotDispatched` является явным writer-local доказательством no-send.
Он создаётся только узким handler-ом `BlockingIOError` при `ExitStack.enter_context`
захвата worker lock. На этой границе ещё не выполнялись tree checks под lock и не
выдавался `post_comment` I/O command. Lock всегда берётся с `blocking=False`,
включая compatibility callers. Exception handling не охватывает тело lock context,
внешний вызов, последующий tree check или освобождение lock.

Общий outbox generator обрабатывает только этот специальный отказ как retryable:
durable `PENDING`, increment attempts, `next_attempt = now + min(300, 2**min(attempts,8))`.
Это 2s на первой попытке и максимум 256s в существующей формуле. Дедлайн durable,
payload/event key неизменяемы. Отказ самого retry checkpoint остаётся fatal:
никакого нового dispatch после failed save.

`BlockingIOError` из GitHub POST — обычная неопределённая ошибка. Как timeout,
обрыв соединения или ошибка после возможного принятия, он оставляет `UNCERTAIN`.
Ни отсутствие комментария, ни освобождённый lock не разрешают blind retry.
Exactly one exact-body/effect-key receipt переводит запись в DONE; отсутствие или
несколько receipts оставляют hold. Sync и async adapters используют тот же generator.

## Переходы и crash/restart

| Граница | Durable результат | Следующая попытка |
| --- | --- | --- |
| Lock не получен, POST command не выдавался | PENDING с bounded backoff | После дедлайна: свежие gates, receipt preflight, lock и одна отправка |
| POST мог быть выполнен, callback бросил любую обычную ошибку | UNCERTAIN | Только exact receipt reconciliation, без нового POST |
| Crash/failed save до фиксации no-send результата | SENDING | Прежняя консервативная recovery; отсутствие receipt не разрешает resend |
| Crash после принятия POST до DONE checkpoint | SENDING | Exact receipt подтверждает DONE без duplicate delivery |
| Evidence доставлена, Chief verdict отсутствует | Chief complete после проверки receipts; approval закрыт | Требуется текущий авторизованный Chief PASS; owner gates сохранены |

Изменения additive и не требуют новой версии durable schema. Используются прежние
PENDING/attempts/next_attempt и SENDING/UNCERTAIN/DONE поля. Legacy envelopes,
request IDs, binding signatures, receipts, reservations и publication journals
не переписываются. Recovery не читает optional telemetry.

Старые UNCERTAIN записи, созданные предыдущей версией при lock contention, не
содержат durable no-send proof. Автоматического сброса таких записей нет: им по-прежнему
нужны receipt reconciliation или независимая операторская инспекция. Это сохраняет
защиту от дублей, в том числе для действительно неопределённых старых POST.

## Изменения относительно прошедшего первоначального ревью дерева

- `bridge_effects.py`: специальный CommentNotDispatched и retry checkpoint в shared generator.
- `task_bridge.py`: ExitStack и узкий acquisition-only handler; nonblocking worker lock.
- `tests/test_chief_delivery_retry.py`: восемь регрессий, описанных ниже.
- `docs/chief-review-runtime.md`: no-send/uncertain/crash policy и backoff.
- `docs/integration-report.md`: первоначальный результат явно отмечен историческим; ссылка на follow-up.
- `docs/chief-delivery-fix-report.md`: этот отчёт.
- `PLAN.md`, `PROGRESS.md`: follow-up stages и фактические результаты.
- `docs/integration-evidence/chief-delivery-test-inventory.json`: полный discovery список 677 IDs.
- `docs/integration-evidence/chief-delivery-verification.json`: финальная проверка, hashes и результаты.
- `docs/integration-evidence/measurement-fixed-after-chief.json` и
  `measurement-deferred-fixed-after-chief.json`: повторные финальные 60s замеры #47.

Сравнение двух изменённых runtime-модулей с первоначальным integration snapshot
восстановлено и сверено по SHA-256 из исторического verification.json. Оно включает
только описанные hunks; другие runtime modules не менялись. Предыдущие measurement
и verification snapshots сохранены без замены.

## Регрессии и проверки

Восемь новых тестов проверяют:

1. Двенадцать явных no-send отказов: bounded durable backoff, restart, immutable
   payload, отсутствие writes до дедлайна, успешную доставку и отсутствие повторов.
2. Crash до no-send checkpoint: на диске SENDING, reconciliation без blind resend.
3. Crash после принятого POST: отсутствие/duplicate/exact receipts и одна доставка.
4. Failed no-send checkpoint: DurabilityFailure, SENDING на диске, отсутствие повторной отправки.
5. Реальный flock: ноль POST при contention, restart в PENDING, recovery после
   освобождения lock, ровно один manifest/chunk receipt, закрытые Chief/owner gates.
6. BlockingIOError внутри POST без видимого receipt: UNCERTAIN после restart, без retry.
7. Принятый POST с BlockingIOError: restart находит receipt, дублей нет.
8. Реальный ControlLoop: минимум четыре control polls при занятом worker lock,
   pending Chief delivery и закрытые quality/owner gates.

В flock regression state расширен на 2,28 MB. После durable retry checkpoint 40
deferred flush/heartbeat/timing updates не вызывают ни одного authoritative
write_json; content/inode/mtime стабильны, optional telemetry продолжает обновляться.

| Проверка | Результат |
| --- | --- |
| Chief family, включая восемь новых тестов | 63 PASS, 47.595s, no skips |
| Совместные resumable + semantic durability | 19 PASS, 17.247s, no skips |
| Recovery | 19 PASS, 0.097s, no skips |
| Resumable ControlLoop | 7 PASS, 6.362s, no skips |
| Полная регрессия | **677 PASS, 263.395s, exit 0, no skips** |
| Compilation | PASS: 72 Python files |
| Final whitespace/source/hash checks | PASS; оба source worktree неизменны, 40 / 9 file hashes |

Суммарно целевые наборы: 108 PASS. Exploratory shell-wrapper invocation
остановлен после уже известного sandbox thread/asyncio wakeup stall. Прямые unittest
команды завершились; production shutdown/loop код ради этого не менялся.

Полный captured log: `/tmp/integration-chief-full.log`; все 677 уникальных
executed IDs совпадают с discovery inventory. SHA-256 лога и проверенных source/test
файлов сохранены в `chief-delivery-verification.json`. Реальный MCP HTTP concurrency
тест выполнен; MCP 2.3.0, Python 3.12.3. Последние правки только docs/evidence.

Команды воспроизведения:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_chief*.py' -v
.venv/bin/python -m unittest discover -s tests -p 'test_*semantic*.py' -v
.venv/bin/python -m unittest discover -s tests -p test_recovery.py -v
.venv/bin/python -m unittest discover -s tests -p test_resumable_control.py -v
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m py_compile *.py tests/*.py
git diff --check
.venv/bin/python tests/measure_semantic_durability.py --mode fixed --seconds 60 --output /tmp/fixed-after-chief.json
.venv/bin/python tests/measure_semantic_durability.py --mode fixed --deferred-notification --seconds 60 --output /tmp/deferred-fixed-after-chief.json
```

## Write amplification и self-review

Повторные wall-clock measurement окна выполнены на текущем коде, с изолированным
2,28 MB fixture, production ControlLoop, меняющимися reports/heartbeats и optional
deferred notification. Каждое окно включает 17 writer steps, одну discovery и
пять свежих reports. Deferred event остался PENDING с неизменным payload.

| Текущий сценарий | Время | Authoritative replacements / bytes | Telemetry replacements / bytes | Process write_bytes / syscw |
| --- | ---: | ---: | ---: | ---: |
| Fixed idle | 60.029s | 0 / 0 | 10 / 7,037 | 40,960 / 10 |
| Fixed deferred notification | 60.026s | 0 / 0 | 16 / 16,438 | 65,536 / 16 |

В обоих случаях content/inode/mtime authoritative file не изменились. Это
воспроизводимые локальные measurements, не production замер. Первоначальные
baseline значения остаются историческими:
10 replacements / 22,822,915 authoritative bytes и deferred 4 / 9,134,368.

Self-review проверяет узость acquisition handler-а, отсутствие no-send inference
по типу внешней ошибки, durable SENDING-before-POST и PENDING-after-proof, backoff
до eligibility, restart receipts, fatal save и single writer. Payload/IDs, tree
freshness, Chief/owner/publication gates, locks и semantic projection не ослаблены.
No-send exception и её cause не сериализуются: diagnostic фиксированный,
credentials/transport error text не сохраняются.

Оставшиеся ограничения: старые uncertain effects без durable proof требуют
инспекции; crash до retry checkpoint намеренно сохраняет conservative hold;
реальные GitHub/SSH/provider failure soak и production acceptance не выполнялись.
Небольшие telemetry записи и записи реальных retry transitions ожидаемы; ноль
перезаписей idle state не означает ноль сериализации/CPU/read I/O.

Полная регрессия и финальная self-review завершены. PLAN.md и PROGRESS.md обновлены;
Git status и tracked/untracked diff checks проверены. Все 40 / 9 source hashes,
HEAD и status совпадают с исходным manifest. Работа остановлена для независимой
проверки; production-ready не объявляется. Других операций не запланировано.
