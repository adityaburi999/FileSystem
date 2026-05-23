# SYSTEM OVERVIEW (FLOW STYLE)

Project: RedirectFS / FileSystem
Goal: versioned + immutable + crash-safe filesystem

## MAIN PATH
User App
  ↓ POSIX syscall
FUSE Layer
  ↓
Path Resolver
  ↓
Metadata Engine
  ↓
Read/Write Engine
  ↓
Chunk Engine (4MB + BLAKE3)
  ↓
Object Store
  ↓
Disk Layout

Support modules (parallel):
- WAL Engine
- Cache Engine
- Index Engine
- GC Engine
- Staging Engine
- Recovery Engine

## MODULE CONNECTION MAP
FUSE -> Path Resolver -> Metadata
Write Engine -> Chunk Engine -> Object Store -> WAL -> Metadata(CAS)
Read Engine -> Cache -> Object Store -> Reconstruct -> FUSE
Delete -> Metadata Tombstone -> WAL -> GC Trigger
Recovery -> WAL Replay -> Metadata Fix -> Staging Cleanup

## STORAGE LAYOUT
/wal
/metadata
/objects
/cache
/staging
/system

## GLOBAL BEHAVIOR
- No in-place overwrite
- Metadata controls truth
- Chunks are immutable
- Delete is logical first
- GC does physical cleanup later

## WHAT CAN GO WRONG?
1) Path resolver misses object
   -> fallback to index scan/graph walk
   -> if still missing: ENOENT

2) Metadata/object mismatch
   -> mark inconsistency
   -> recovery/repair on startup

3) Chunk corrupt hash
   -> read fails EIO
   -> quarantine + alert + restore from version

4) Crash during write
   -> WAL + staging preserve intent
   -> replay/rollback on boot

## FINAL RULE
If module output is uncertain,
-> do not expose partial data,
-> fail safe,
-> keep last committed version visible.
