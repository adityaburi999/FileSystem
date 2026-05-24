# STAGING SYSTEM

## STRUCTURE
- Transaction Slot Manager
- Temp Buffer Store
- Temp Chunk Map
- Txn State Marker
- Cleanup Worker

## FLOW
- Begin write transaction -> allocate staging slot
- Stream incoming blocks to temp buffer
- Chunk + hash + object store persist
- Record chunk refs in staging state
- Append WAL events
- Commit path -> metadata CAS + WAL commit
- Mark staging txn committed
- Cleanup slot artifacts
- Boot recovery -> purge stale uncommitted slots

## RULES
- Staging data is non-visible to users.
- Commit truth = WAL + metadata CAS.
- Uncommitted staging state is discardable.
- Staging quota enforcement is mandatory.

## FAILURES
- Staging quota exceeded -> ENOSPC/backpressure.
- Crash before commit -> rollback via recovery.
- Slot leak -> periodic stale-slot cleanup.
- Concurrent txns same path -> CAS resolves winner.
- Cleanup failure -> deferred cleanup queue.

## INVARIANTS
- Uncommitted staging does not affect namespace.
- Every committed txn has durable WAL evidence.
- Staging cleanup is idempotent.
