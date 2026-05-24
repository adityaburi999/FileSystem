# SYSTEM MAP

This repository currently contains architecture/design documents. No production module directories are implemented yet.  
This map defines the required boundaries and operational contracts for implementation.

## 1) Module Responsibilities

### `/fuse`
- Owns syscall-facing handlers (`open`, `read`, `write`, `unlink`, etc.).
- Performs request validation and path handoff.
- Must not implement storage, WAL, or metadata commit logic.

### `/metadata`
- Owns file/directory objects, version chains, tombstones, and namespace graph.
- Enforces CAS (`expected_version -> new_version`) for every mutation.
- Source of truth for visible filesystem state.

### `/wal`
- Owns append-only transaction logging and replay metadata.
- Guarantees durable write intent before metadata commit.
- Recovery classification: committed / aborted / incomplete.

### `/chunk_store`
- Owns chunking, BLAKE3 hashing, immutable chunk persistence, and dedup checks.
- Must verify `chunk_id == BLAKE3(content)` before accepting or serving chunk data.
- Must not publish namespace-visible state.

### `/cache`
- Owns L1 (RAM) and L2 (SSD) caches for chunk and metadata acceleration.
- May optimize latency only; cannot override metadata truth.
- Must invalidate on version change, delete/tombstone, and recovery events.

### `/gc`
- Owns conservative orphan detection and physical deletion.
- Must scan committed metadata + WAL inflight protection set before deletion.
- Deletes only after revalidation and retention delay.

### `/core`
- Owns orchestration between modules, startup ordering, and mount gate.
- Enforces recovery-before-mount and fail-closed policy for uncertain state.
- Must keep module boundaries strict; no cross-module shortcut paths.

## 2) Data Flow

### Write Path (authoritative order)
1. `/fuse`: receive write and validate request.
2. `/core`: open transaction context.
3. `/chunk_store`: stream bytes -> chunk -> compute BLAKE3.
4. `/chunk_store`: verify digest match; reject on mismatch.
5. `/chunk_store`: persist immutable chunk (dedup allowed, not bypassing logging).
6. `/wal`: append chunk/write events and fsync durability point.
7. `/metadata`: build new redirect/version payload.
8. `/metadata`: CAS commit with `expected_version`; reject conflicts.
9. `/wal`: append/mark commit completion record.
10. `/cache`: invalidate/promote impacted keys.
11. `/gc`: later handles any orphaned chunks from failed/incomplete attempts.

Hard rule: no metadata commit is valid unless WAL durability is established first.

### Read Path
1. `/fuse`: receive read(path, offset, size).
2. `/metadata`: resolve path and load latest committed version snapshot.
3. `/cache`: check L1, then L2 for required chunk ranges.
4. Cache miss -> `/chunk_store`: fetch immutable chunk.
5. `/chunk_store`: verify BLAKE3 before returning bytes.
6. `/core`: reconstruct ordered range and return to `/fuse`.

Hard rule: unverified chunks are never served.

## 3) Consistency Model

- Metadata is the visibility boundary: only committed metadata versions are externally visible.
- Writes use optimistic concurrency with CAS; conflicts fail explicitly (retry is bounded).
- Reads are snapshot-based for stable per-request views.
- WAL + CAS provide atomicity across crash boundaries.
- Incomplete transactions are treated as non-committed during recovery.
- Deletion is logical first (tombstone), physical later (GC).

## 4) Failure Handling Strategy (Fail-Closed)

- WAL append/fsync failure: abort transaction, do not commit metadata.
- CAS mismatch/conflict: reject commit (retry with cap or return busy/conflict).
- Hash mismatch (ingest/read): reject chunk, quarantine record, raise integrity alert.
- Missing chunk for committed metadata: mark inconsistency, quarantine object, block unsafe serve path.
- Recovery failure/corrupt WAL beyond salvage: block mount or force read-only degraded mode.
- Ambiguous state at any step: fail operation; never guess or silently heal in foreground path.

## 5) Recovery and Concurrency Safety

- Recovery replay order is WAL append order; replay must be idempotent.
- Mount gate opens only after consistency checks pass.
- GC must be conservative under races: inflight/WAL-protected references block deletion.
- Last known good committed metadata version remains readable when new writes fail.

## 6) Explicit Assumptions (to validate during implementation)

- WAL durability means persisted append + fsync at defined transaction boundaries.
- CAS key includes object identity and expected version, not only path.
- BLAKE3 verification is required on both chunk ingest and object-store read miss paths.

If any assumption cannot be guaranteed by implementation, the system must fail closed and reject the operation.
