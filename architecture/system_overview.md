# SYSTEM OVERVIEW

## STRUCTURE
- FUSE Layer
- Path Resolver
- Metadata Engine
- Read Engine
- Write Engine
- Chunk Engine
- Object Store
- WAL Engine
- Cache Engine
- Index Engine
- GC Engine
- Recovery Engine
- Staging Engine

## FLOW
- Application syscall -> FUSE
- FUSE -> Path Resolver
- Path Resolver -> Metadata Engine
- Read request -> Read Engine -> Cache -> Object Store
- Write request -> Write Engine -> Chunk Engine -> Object Store
- Write path -> WAL append -> Metadata CAS commit
- Delete request -> Metadata tombstone -> WAL append
- GC cycle -> live-ref scan -> orphan cleanup
- Crash boot -> Recovery replay -> mount enable

## RULES
- Metadata is authority.
- Chunks are immutable.
- No in-place overwrite.
- All state changes require WAL.
- All metadata mutations require CAS.
- Mount enabled only after recovery success.

## FAILURES
- Path resolution miss -> return ENOENT.
- Metadata/chunk mismatch -> block object + recovery flag.
- WAL write failure -> abort transaction.
- CAS conflict -> retry with latest version.
- Recovery failure -> mount read-only or fail mount.

## INVARIANTS
- Visible state is committed state.
- Version order is monotonic.
- Uncertain state is never exposed.
