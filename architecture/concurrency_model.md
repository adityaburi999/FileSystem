# CONCURRENCY MODEL

## STRUCTURE
- Snapshot Reader
- Optimistic Writer
- CAS Commit Gate
- Directory Mutation Guard
- Conflict Resolver

## FLOW
- Reader loads version V snapshot
- Writer A loads version V
- Writer B loads version V
- Writer A commits V+1 via CAS
- Writer B CAS fails on V expectation
- Writer B retries with latest metadata or aborts
- Reader completes on original snapshot V

## RULES
- Every mutation requires CAS.
- Readers are snapshot-based.
- Conflicts are explicit, not silent merge.
- Retry policy must be bounded.

## FAILURES
- High contention -> exponential backoff + retry cap.
- Writer starvation -> fairness scheduling.
- Concurrent rename/delete race -> ordered mutation gate.
- Stale metadata cache -> version-aware invalidation.
- Conflict storm -> return EBUSY after retry budget.

## INVARIANTS
- No torn read of partially committed write.
- At most one winner per version step.
- Version numbers are strictly increasing.
