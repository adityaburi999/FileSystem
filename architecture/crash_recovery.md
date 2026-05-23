# CRASH RECOVERY

## STRUCTURE
- WAL Loader
- Transaction Classifier
- Metadata Verifier
- Staging Cleaner
- Index Rebuilder
- Mount Gate

## FLOW
- Boot sequence starts
- Load WAL segments
- Replay events in order
- Classify txn: committed/aborted/incomplete
- Apply committed txns
- Rollback incomplete txns
- Verify metadata-chunk links
- Cleanup stale staging slots
- Rebuild index if required
- Run post-replay consistency check
- Open mount gate

## RULES
- Replay order is append order.
- Incomplete txn is non-committed.
- Mount blocked until consistency check passes.
- Recovery actions must be idempotent.

## FAILURES
- WAL corruption -> bounded salvage or fail mount.
- Missing chunk for committed metadata -> quarantine object.
- Staging cleanup failure -> reschedule cleanup job.
- Index rebuild failure -> mount degraded/read-only.
- Repeated txn replay fault -> quarantine txn record.

## INVARIANTS
- Recovery never invents a new committed state.
- Visible state after boot is WAL-consistent.
- Replay can be re-run without divergence.
