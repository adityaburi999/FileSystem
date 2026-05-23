# Concurrency Model — RedirectFS

This document describes how RedirectFS handles concurrent reads, writes, deletes, and background operations safely without data races, corruption, or silent overwrites.

---

## Core Principle: Optimistic Concurrency Control

RedirectFS uses **optimistic concurrency control (OCC)** rather than pessimistic locking.

- Reads proceed without acquiring locks.
- Writes optimistically proceed, then validate at commit time using **Compare-And-Swap (CAS)**.
- If a conflict is detected at commit → abort/retry. No lock contention, no deadlocks.

This model is well-suited to filesystems because most concurrent operations target different files, and conflicts on the same file are relatively rare.

---

## Version Numbers

Every `FileObject` and `DirObject` has a monotonically increasing **version number**:

```rust
struct FileObject {
    object_id: ObjectId,
    version:   u64,       // incremented on every successful CAS commit
    // ...
}
```

Version numbers are the **single source of truth for concurrency control**. No locks, no timestamps.

---

## Compare-And-Swap (CAS) Rule

Every metadata write must pass a CAS check:

```
PRECONDITION:  stored_version == expected_version
EFFECT:        stored_version  = expected_version + 1
POSTCONDITION: new metadata committed atomically

IF stored_version != expected_version:
    ABORT → return VersionConflict error
```

This prevents two concurrent writers from silently overwriting each other:

```
Writer A reads version 5, prepares new FileObject v6
Writer B reads version 5, prepares new FileObject v6

Writer A commits first  →  stored_version becomes 6
Writer B tries to commit → sees stored_version=6, expected=5 → CONFLICT
Writer B must retry:
  - re-read current FileObject (version 6)
  - recompute its changes on top of version 6
  - attempt CAS commit to version 7
```

---

## Concurrent Operation Matrix

| Operation A | Operation B | Outcome |
|---|---|---|
| Read | Read | Fully concurrent, no conflict |
| Read | Write (different file) | Fully concurrent, no conflict |
| Read | Write (same file) | Read sees old committed version (snapshot isolation) |
| Write | Write (different files) | Fully concurrent |
| Write | Write (same file) | One wins, other gets VersionConflict → retry |
| Write | Delete (same file) | Delete wins if it commits first; write gets conflict |
| Delete | GC | GC has safety window; never deletes active references |
| GC | Read | GC only deletes unreferenced chunks; read has active reference |
| Crash Recovery | Any | Recovery runs before FUSE activates; no concurrent ops |

---

## Read Snapshot Isolation

Reads always operate on a **committed snapshot**:

1. The `FileObject` version loaded at the start of a read is stable for the duration of that read.
2. If a concurrent write commits a new version mid-read, the read continues with the old version — no torn reads.
3. `chunk_ids` in a committed `FileObject` always point to immutable chunks that are never modified or deleted while an active read references them.

Implementation: reads take a version number at open time. Chunks are only eligible for GC if no active read holds a reference.

---

## Write-Write Conflict Resolution

When two writers target the same file:

```
Writer A: intended operation = append 100 bytes
Writer B: intended operation = append 200 bytes

Both read version N, both prepare version N+1.

Writer A commits first → version = N+1
Writer B hits VersionConflict.

Resolution options (application decides):
  1. Retry: re-read version N+1, apply B's changes on top → commit as N+2.
  2. Merge: if operations are commutative (e.g., independent append regions).
  3. Fail: return EBUSY to the application (let it handle the conflict).
```

RedirectFS's write engine provides automatic **retry with backoff** for up to `MAX_CAS_RETRIES` attempts (configurable, default: 5).

---

## Directory Concurrency

Directory operations (create file, delete file, rename) also use CAS on `DirObject`:

```
mkdir /data/new_dir:
  read parent DirObject version N
  add new entry "new_dir" → new_dir_object_id
  CAS commit → version N+1

Concurrent rmdir /data/other_dir:
  independent CAS on same parent DirObject
  one succeeds first, other retries
```

Each directory operation is an atomic CAS on the parent directory's entry map. No directory-level locks are needed.

---

## WAL and Ordering

The WAL provides a **global serialization point** for all committed operations:

- Every committed transaction gets a monotonically increasing `TxnId`.
- WAL entries are appended in order → total order of all operations.
- On crash recovery, this total order is used to reconstruct filesystem state.

Within a single transaction, WAL entries are ordered as:
```
TxnBegin → ChunkWritten... → MetaCommit → TxnCommit
```

No transaction can be partially committed: `TxnCommit` is the point of no return.

---

## Background Operations (GC, Indexing)

### GC Concurrency

GC runs as a background Tokio task. It uses a **read-only metadata snapshot** taken at the start of each GC cycle:

- GC reads metadata but never holds locks on it.
- The safety window ensures GC does not delete chunks that belong to transactions committed after the snapshot was taken.
- GC calls `object-store.delete_chunk()` which is atomic at the file system level (Linux `unlink` is atomic).

### Index Engine Concurrency

The path index is updated **after** a CAS metadata commit succeeds. This means:
- The index may briefly lag metadata (window of ~microseconds).
- If the index is stale, `path-resolver` falls back to graph traversal.
- Index updates are idempotent: re-inserting the same path → object_id is safe.

---

## Preventing Race Conditions

### Race: Concurrent Delete + Read

```
Thread 1: read("/data/file.txt") → loads FileObject v3, starts fetching chunks
Thread 2: unlink("/data/file.txt") → tombstones FileObject → GC runs

GC safety window: chunks are not deleted until SAFETY_WINDOW seconds after tombstone.
Thread 1 will finish reading before the safety window expires.
Result: Read completes successfully; file is deleted afterward.
```

### Race: Concurrent Write + GC

```
Thread 1: write in progress, chunks written to object store, WAL logged
Thread 2: GC scans — chunk appears unreferenced (metadata not yet committed)

GC check: "Is chunk_id referenced in any open WAL transaction?"
If yes → skip deletion.
Result: GC skips the chunk; Write commits; chunk becomes permanently referenced.
```

### Race: Two Concurrent Creates (Same Path)

```
Thread 1: create "/data/file.txt" → resolves parent dir, no conflict
Thread 2: create "/data/file.txt" → same path

Both attempt to add "file.txt" to parent DirObject via CAS.
One succeeds (version N → N+1).
Other gets VersionConflict → retry finds "file.txt" already exists → return EEXIST.
```

---

## Tokio Async Model

RedirectFS uses Tokio for async I/O:

- Each FUSE request is handled as an async task.
- Chunk I/O (reads and writes) is fully async — no thread blocking.
- CAS commits on `metadata-engine` use `tokio::sync::Mutex` on the metadata record, held only during the commit check — not during I/O.
- Background tasks (GC, index compaction) run on a separate Tokio worker thread pool.

---

## Summary of Concurrency Rules

1. **Reads are always lock-free** — they use version snapshots.
2. **Writes use CAS, not locks** — conflicts cause retry, not blocking.
3. **Directory updates are CAS-atomic** — no directory-level mutex.
4. **WAL provides global transaction ordering** — total order of commits.
5. **GC is always background and conservative** — safety window prevents conflicts.
6. **Version numbers never decrease** — monotonic, always forward progress.
