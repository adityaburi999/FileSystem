# Delete Flow — RedirectFS

This document describes the complete delete pipeline, from the user's unlink call through logical deletion, tombstoning, WAL logging, and eventual physical garbage collection.

---

## Core Principle

RedirectFS uses **two-phase deletion**:

1. **Logical delete** — file is immediately removed from the live namespace (invisible to users), but data is not yet freed from disk.
2. **Physical delete** — GC engine later detects the orphaned chunks and permanently removes them.

This design ensures:
- Atomicity: delete is instant and crash-safe.
- Safety: no data is freed until it is confirmed unreferenced.
- Versioning: old versions of a file are not deleted until policy allows.

---

## Overview

```
user: unlink("/data/file.txt")
    │
    ▼
[FUSE Layer]  →  receives unlink syscall
    │
    ▼
[Path Resolver]  →  resolves path → object_id
    │
    ▼
[Metadata Engine]  →  sets FileObject.state = Tombstone
                       bumps version via CAS
    │
    ▼
[WAL Engine]  →  logs Tombstone + TxnCommit
    │
    ▼
[Path Resolver / Index Engine]  →  removes path → object_id mapping
    │
    ▼
FUSE returns success (file is gone from namespace)
    │
    ▼ (later, async)
[GC Engine]  →  detects tombstoned objects
                finds unreferenced chunks
                physically deletes chunks + metadata objects
```

---

## Step-by-Step Pipeline

### Step 1 — FUSE Receives Unlink

The user calls `rm file.txt`, which becomes `unlink("/data/file.txt")` delivered to FUSE.

`fuse-layer` passes the virtual path to the delete pipeline.

---

### Step 2 — Path Resolution

`path-resolver` resolves the path to an `ObjectId`:
- If path does not exist → return `ENOENT`.
- If path is a directory and non-empty → return `ENOTEMPTY`.
- If path is a directory and empty → handle as directory removal (similar flow).

---

### Step 3 — Logical Delete (Tombstone)

`metadata-engine` performs a CAS update on the `FileObject`:

```rust
FileObject {
    object_id:   <id>,
    version:     current_version + 1,
    state:       FileState::Tombstone,
    tombstone_at: now(),    // timestamp for GC retention policy
    // chunk_ids remain intact — GC will handle them
}
```

The **chunk list is preserved** in the tombstone. GC needs it later to know which chunks to release.

If `expected_version` does not match (concurrent modification) → abort and return `EBUSY`.

---

### Step 4 — WAL Logging

`wal-engine` appends:

```
WalEntry::Tombstone    { object_id }
WalEntry::TxnCommit    { txn_id }
```

The WAL `fsync` ensures the tombstone survives a crash immediately after. On crash recovery, the tombstone is replayed and the file remains logically deleted.

---

### Step 5 — Namespace Removal

`index-engine` removes the `path → object_id` mapping:

```
index.remove("/data/file.txt")
```

`metadata-engine` removes the child entry from the parent directory object via CAS:

```
parent_dir.entries.remove("file.txt")
```

At this point, the file is **completely invisible** to users: no path resolves to it, no directory lists it.

---

### Step 6 — Cache Invalidation

`cache-engine` invalidates the file's metadata cache entry:

```
cache.invalidate_meta(object_id)
```

Chunk cache entries are *not* immediately invalidated — they remain until naturally evicted. They will not cause harm because no new reads can reach them (no path resolves to the deleted file).

---

### Step 7 — GC Trigger (Async)

The GC engine runs on a configurable schedule (or is triggered by a tombstone count threshold). See `garbage_collection.md` for the full GC pipeline.

For the delete flow, GC will:
1. Find the tombstoned `FileObject` older than the retention window.
2. Check whether any other file versions still reference the same chunk IDs (dedup sharing).
3. If a chunk is exclusively owned by the tombstoned object → mark for deletion.
4. Physically delete the chunk files from `/objects/`.
5. Delete the tombstoned `FileObject` from `/metadata/`.

---

## Directory Delete

Deleting an empty directory follows the same pattern:
- `DirObject` is tombstoned (not immediately removed).
- Parent directory entry is removed via CAS.
- GC cleans up the `DirObject` metadata file.

Deleting a non-empty directory requires recursive deletion (each child is tombstoned first). This is done inside a single WAL transaction where possible, or as a series of ordered CAS operations.

---

## Versioned File Delete

If a file has multiple versions (from previous writes), the tombstone applies to the **latest version pointer** only. Older versions:
- Remain valid until GC version pruning removes them per retention policy.
- May continue to be referenced by snapshots or version history queries.

---

## Failure Scenarios

| Failure Point | State | Recovery |
|---|---|---|
| Crash after CAS tombstone, before WAL commit | Tombstone committed but WAL incomplete | WAL replay re-applies tombstone on boot |
| Crash after WAL commit, before index removal | File is tombstoned; index still has stale entry | Path resolver checks FileObject state — tombstone → ENOENT; index cleaned on next GC pass |
| Crash during GC physical delete | Some chunks deleted, some not | GC is idempotent: re-scan on next run finds remaining orphans |

---

## Key Invariants

- A file is considered deleted the moment its `FileObject.state == Tombstone`, regardless of whether GC has run.
- Tombstoned objects are **never returned** by reads or directory listings.
- Physical chunk deletion only happens after GC confirms no active references remain.
- The retention window (default: GC safety delay) ensures no race between delete and a concurrent read that was in-flight.
