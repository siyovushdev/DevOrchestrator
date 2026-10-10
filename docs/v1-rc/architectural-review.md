# Focused V1 architectural and safety review

Reviewed the production-to-candidate tracked diff, untracked Safety Latch module
and tests, exact integration-to-acceptance intersections, request/transition
schema, shared outbox generator, worker cleanup proof and owner publication paths.
All runtime Python sources are byte-identical to the frozen accepted Safety Latch;
the only candidate test-source correction is in tests/test_operational_stability.py.
The independently inspected Chief fix is present inside the accepted snapshot;
no duplicate implementation or schema replay was introduced.

| Invariant | Evidence and disposition |
| --- | --- |
| Single lifecycle writer | WriterContinuation advances authoritative frames; copied JSON Commands/results cross detached I/O; expired flights retain ownership; production bridge.lock entrypoint retained. Fatal checkpoint failures escape ordinary retry handlers. |
| Immutable requests and recovery | Existing request-ID algorithm, source-turn/generation bindings, PREPARE/LAUNCH/VERIFY/DRAIN/HELD validation, accepted receipts and uncertain reservations retained. Legacy migration never infers unlaunched proof. |
| Worker lifecycle/cleanup | Generation-bound runtime receipts, boot/start identities and surviving recorded anchors remain authoritative. Unknown descendants without a valid anchor stay AMBIGUOUS; terminal cleanup proof is required for release/publication. Only test-premise races changed. |
| Chief/owner gates | Delivered Chief evidence is not a verdict. Identity, fresh reviewed repository tree, authorized Chief/owner authors and owner action epoch remain required. Publication rechecks current repository/closure at locked effect boundaries. |
| Durable outbox/idempotency | Shared sync/async SENDING-before-POST; narrow acquisition-only no-send proof restores PENDING with durable backoff. Transport failures remain UNCERTAIN; absent/duplicate exact receipts do not permit blind resend. Failed retry checkpoint leaves conservative recovery. |
| Safety Latch/quarantine | Dedicated MCP observation tools force read-only child environment; older servers fail closed. READ_ONLY provenance survives nested/delayed results. Sticky issue holds preserve queue/repository ownership; quarantine/origin guards precede callbacks and survive latch removal/status tampering. |
| Semantic state and telemetry | Only enumerated volatile liveness/timing fields leave authoritative state. Unknown fields, holds, quarantine provenance, requests and receipts remain durable. Optional bounded telemetry never grants workflow authority or affects recovery. Two 60-second integrated measurements show zero authoritative writes. |
| Repository/environment isolation | Independent RC Git/object store, no remotes, no source commit. Tests mount code read-only; host production/config/state are absent, with private PID/network namespaces and initially empty HOME/TMPDIR. Real test MCP uses an ephemeral private loopback port. |

No confirmed runtime or architectural release blocker was found in this scope.
Independent Chief/Safety Latch review: 106 PASS, ACCEPTED. Independent review of
final cleanup test premises and awk mount: 14 PASS, ACCEPTED. Final full regression
completed: 737 PASS / 277.412s, no failures/errors/skips.

## Confirmed verification defects and minimal fixes

The initial namespace omitted the target of /usr/bin/awk's alternatives symlink.
The real fingerprint pipeline failed with return 127. The runner now binds only
the resolved awk binary read-only at /etc/alternatives/awk; it does not expose
host configuration or change application code.

The unrecorded-survivor test conflated a durable exit marker with supervisor exit.
It now waits/reaps its exact disposable wrapper before removing receipt anchors.
The wrapper-identity mismatch subcase now explicitly models a present matching
leader for that exact PID; all other process reads remain real. An absent leader
with an exact recorded surviving child is legitimate ownership under the accepted
contract. Zero-signal and AMBIGUOUS assertions remain intact. Applying reaping to
the shared fixture was rejected by independent review and superseded by these
narrow corrections; failed intermediate logs are retained.

## Deferred nonblocking work / V2

The accepted F4 stale discovery cache can overwrite a fresher CLOSED in-memory
view. Sticky durable closure/archive and fresh publication/launch boundaries
remain in place and covered by existing closure tests. No ordering redesign was
included. Real provider/SSH/GitHub/Telegram failure soak, compatible worker/MCP
rollout and operational acceptance of historical ambiguous holds require a
separately authorized acceptance window. No automatic hold/quarantine release
or unrelated milestone was started.

Production source/config/Git metadata remain unchanged. Live state is actively
written by the preexisting production bridge; its byte hash changed during this
read-only task. The candidate has no access to that state during tests. The
preservation report records this external drift and does not claim that a
running production system was frozen or that its live state is a recovery backup.
