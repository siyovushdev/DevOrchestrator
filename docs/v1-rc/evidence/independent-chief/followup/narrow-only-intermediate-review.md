# Independent regression failure follow-up

Scope: review of RC test-only orphan synchronization and minimal isolated runner dependency mount. Runtime remains unchanged from reviewed acceptance; no source edits by reviewer.

Independently reproduced baseline failure with the exact original parent bwrap mount/environment configuration and reviewer-owned test fixture: awk shell command return code 127, stderr `awk: command not found`, index and snapshot fingerprints null. Mounting only resolved /usr/bin/mawk read-only at /etc/alternatives/awk makes return code 0 and both fingerprints populated. Evidence: baseline-without-awk.json and baseline-with-awk.json.

Original cleanup failure follows from invalid fixture timing: durable exit_code precedes wrapper exit; a surviving exact wrapper identity is an independent valid group anchor even when receipt members is emptied. Waiting/reaping the exact dummy wrapper and asserting process_details absent establishes the intended orphan precondition. No runtime correction is justified by that failure alone.

First broad fixture correction passed the original no-anchor cleanup regression and baseline regression, but independently exposed another invalid timing assumption: `test_active_and_identity_or_turn_mismatch_are_never_signalled` changed wrapper start to 1 and expected no signals while no live leader remained to compare it to. Existing exact recorded child anchor can prove OWNED after wrapper exit by the runtime contract. First follow-up run: 13 tests, 12 PASS / 1 FAIL, 3.920s; mock killpg received SIGTERM and SIGKILL in the identity case. Retain this failing evidence; do not hide it.

The test needs an explicit live-wrapper observation for its live-leader start-identity rejection case, or a real live wrapper fixture, while preserving original zero-signal assertions and real orphan/unknown-member tests. Reviewer-owned deterministic anchor probes check live wrapper + members=[] gives OWNED; absent wrapper + unknown child gives AMBIGUOUS; and live wrapper start mismatch gives AMBIGUOUS even with recorded child anchor.

Final correction is narrowed to `test_unrecorded_survivors_fail_closed` only: wait the exact tracked dummy wrapper if present, reap, assert process_details absent, then empty receipt members. Shared launch_orphan is restored byte-for-byte. All original AMBIGUOUS and zero-signal assertions remain. The prior mismatch case is unchanged; no runtime change was made.

**Final independent decision: ACCEPTED** for the narrowed fixture correction and resolved-awk read-only runner mount. Final isolated follow-up: **14 PASS, 2.695s (unittest), exit 0, no skips/errors/failures**. This includes all 10 OwnedCleanupTests, the original fingerprint regression, and 3 reviewer-owned deterministic anchor probes. Log SHA256: 6b44c02acaec6be80798d9b243fb3b1a86f9dec24467730d793f9cdcdbb2b064.

Source revalidation: of the 82 previously compared runtime/test/Chief-doc files, 81 remain byte-identical to acceptance; the only changed file is tests/test_operational_stability.py, SHA256 fae0826e46f98310a2b27f832c407ef7b1e1e2a7b5fe9e79b1525a6c474c38d2. Runner docs/v1-rc/run_verification.py SHA256: 25664becce8838f1862d9442a90374b3aa0eeb819d2c683ee06e5199dec2cf5a. Runtime hashes remain unchanged from acceptance.

No confirmed runtime defect is established by these regression failures. Prior Chief/Safety Latch technical review remains ACCEPTED. A successful full-suite rerun on the final narrower bytes is still required; full independent release acceptance and production acceptance remain owner gates.
