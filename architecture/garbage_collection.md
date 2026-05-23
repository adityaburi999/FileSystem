# GARBAGE COLLECTION

## STRUCTURE
- Live Reference Scanner
- WAL Inflight Scanner
- Candidate Set Builder
- Sweep Executor
- Audit Logger

## FLOW
- Start GC cycle
- Scan committed metadata -> live refs
- Scan WAL inflight txns -> protected refs
- Enumerate object store chunks
- Build orphan candidates = chunks not referenced
- Apply retention delay window
- Revalidate candidate references
- Delete confirmed orphan chunks
- Persist audit records

## RULES
- GC is conservative by default.
- Single uncertainty blocks candidate deletion.
- Revalidation required before delete.
- GC must not block foreground IO path.

## FAILURES
- Metadata snapshot stale -> rerun with fresh snapshot.
- Inflight txn detected -> defer candidate.
- Delete IO failure -> retry/backoff queue.
- Crash during sweep -> restart idempotent pass.
- Audit write failure -> halt destructive phase.

## INVARIANTS
- Live chunk is never deleted.
- GC actions are replay-safe.
- Every delete has an audit record.
