# WRITE FLOW

## STRUCTURE
- FUSE Write Handler
- Staging Manager
- Stream Buffer
- Chunk Engine
- Object Store
- WAL Engine
- Metadata Engine
- Cache Invalidation

## FLOW
- write(path, bytes) -> FUSE
- Begin staging transaction
- Append bytes to stream buffer
- Buffer threshold reached -> create chunk
- Hash chunk -> chunk_id
- Store chunk if absent
- Append chunk event to WAL
- Repeat until close/fsync
- Finalize trailing chunk
- Build new redirect metadata
- CAS commit metadata
- WAL commit transaction
- Invalidate stale cache entries
- Cleanup staging artifacts

## RULES
- No metadata commit without WAL durability.
- CAS is required for commit.
- User-visible state changes only after CAS success.
- Dedup check does not skip WAL event.

## FAILURES
- Object store full -> ENOSPC + abort txn.
- WAL append/fsync fail -> abort txn.
- CAS conflict -> retry or return EBUSY.
- Crash before commit -> recovery rollback.
- Staging cleanup failure -> deferred cleanup on boot.

## INVARIANTS
- Commit is atomic at metadata boundary.
- Last committed version remains readable on failure.
- Orphan chunks are non-visible.
