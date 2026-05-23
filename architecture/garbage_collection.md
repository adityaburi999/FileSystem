# Garbage Collection — RedirectFS

This document describes how RedirectFS detects and safely removes unreferenced chunks and obsolete metadata versions.

---

## Why GC Is Needed

RedirectFS never modifies data in place. Every write creates new chunks; every delete creates a tombstone. Over time this produces:

1. **Orphaned chunks** — chunks written during an aborted or crashed transaction that no metadata object references.
2. **Old file versions** — previous `FileObject` versions whose chunks are no longer needed.
3. **Tombstoned objects** — logically deleted files/directories whose metadata and chunks still occupy disk.

GC is the only mechanism that physically frees this data.

---

## GC Is Always Safe to Run

GC is designed to be **conservative and idempotent**:
- It only deletes data that is provably unreferenced.
- If GC is interrupted mid-run, the next run will cleanly pick up where it left off.
- GC never touches data that has an active reference in any live metadata version.

---

## Two-Phase GC Architecture

### Phase 1 — Mark (Reference Counting Scan)

GC builds a complete picture of which chunks are **alive** by scanning all active metadata:

```
1. Open a consistent metadata snapshot (read-only).
2. Iterate over all active FileObjects (state == Active).
3. For each FileObject, collect all chunk_ids from all versions still within retention policy.
4. Build a live_set: HashSet<ChunkId>.
```

Then scan the object store:

```
5. Walk /objects/ directory tree.
6. For each chunk file, extract its ChunkId from the filename (BLAKE3 hash).
7. If chunk_id NOT IN live_set  →  add to candidate_orphans list.
```

---

### Phase 2 — Sweep (Safe Deletion)

Before physically deleting candidates:

```
1. For each candidate_orphan:
   a. Re-check against metadata (race: a concurrent write may have just created a reference).
   b. Check the WAL for any in-flight transactions referencing this chunk_id.
   c. If still unreferenced AND not in any open transaction:
      → add to confirmed_delete list.

2. Apply safety delay: only delete chunks that have been orphaned for > GC_SAFETY_WINDOW seconds.
   (Default: 5 minutes. Prevents deleting chunks from in-flight writes that haven't committed yet.)

3. Delete confirmed chunks from /objects/ one by one.
4. Record each deletion in GC audit log (/system/gc/gc_<timestamp>.log).
```

---

## Version Pruning

Old versions of files accumulate over time. Version pruning removes them according to a configurable retention policy.

### Retention Policy Options

| Policy | Description |
|---|---|
| `keep_last_n` | Keep only the N most recent versions of each file (default: N=5). |
| `time_based` | Keep all versions created within the last T days (default: 30 days). |
| `keep_all` | Never prune versions (useful for audit mode; requires external storage management). |
| `combined` | Keep last N versions OR versions within T days, whichever retains more. |

### Pruning Process

```
For each file that has more versions than policy allows:
  1. Sort versions by version number (ascending).
  2. Mark versions outside retention window as PRUNED.
  3. Collect chunk_ids from pruned versions.
  4. Check if any retained version (or other file) references these chunk_ids.
  5. Chunks exclusively owned by pruned versions → add to orphan candidate list.
  6. Delete pruned FileObject records from /metadata/.
```

After version pruning, Phase 2 (sweep) handles the newly orphaned chunks.

---

## Tombstone Collection

Tombstoned objects (from logical deletes) are cleaned up by GC:

```
1. Iterate over all FileObjects with state == Tombstone.
2. Filter: tombstone_at + TOMBSTONE_RETENTION_SECONDS < now().
   (Default retention: 10 minutes — gives time for in-flight reads to complete.)
3. Collect chunk_ids from the tombstoned FileObject.
4. Verify no live FileObject version references these chunks (dedup sharing check).
5. Add exclusively owned chunks to orphan candidate list.
6. Delete tombstoned FileObject from /metadata/.
7. Remove any stale index entries for the tombstoned object.
```

---

## GC and Deduplication

Because multiple files may share the same chunk (dedup), GC must **reference count** chunks, not just check if one file references them:

```
chunk_reference_count[chunk_id] = 
    count of all active FileObject versions that include chunk_id
```

A chunk is only eligible for deletion when its reference count drops to zero.

This is computed during Phase 1 (Mark) and stored temporarily in `/system/gc/refcount.tmp`.

---

## GC Scheduling

GC can be triggered by:

| Trigger | Description |
|---|---|
| **Timer** | Runs periodically (default: every 30 minutes). |
| **Threshold** | Triggered when tombstone count exceeds a limit (default: 1000). |
| **Manual** | System administrator calls GC explicitly via management API. |
| **Post-crash** | GC scan is triggered after crash recovery to find chunks from aborted transactions. |

GC runs as a **background Tokio task** in `gc-engine`. It holds a read-only view of metadata and does not block reads or writes.

---

## GC Safety Guarantees

| Guarantee | Mechanism |
|---|---|
| Never deletes a live chunk | Phase 1 builds complete live_set from all active metadata |
| Never deletes a chunk being written | GC_SAFETY_WINDOW delay + WAL in-flight check |
| Never exposes partial state | Each chunk deleted atomically (file rename → delete) |
| Idempotent | GC can be re-run safely any number of times |
| Crash-safe | GC state stored in /system/gc/; incomplete GC run is harmless |

---

## GC Audit Log Format

Every GC run writes an audit log to `/system/gc/gc_<epoch_ms>.log`:

```
GC_RUN_START  timestamp=<unix_ms>  trigger=timer
ORPHAN_FOUND  chunk_id=<hash>  reason=no_reference
ORPHAN_SKIP   chunk_id=<hash>  reason=safety_window_not_elapsed
VERSION_PRUNE object_id=<id>  version=3  policy=keep_last_5
TOMBSTONE_GC  object_id=<id>  tombstone_age=720s
CHUNK_DELETED chunk_id=<hash>
GC_RUN_END    chunks_deleted=142  versions_pruned=17  duration_ms=380
```

---

## Interaction with Other Modules

| Module | Interaction |
|---|---|
| `metadata-engine` | GC reads metadata (read-only); never writes during scan |
| `object-store` | GC calls `delete_chunk` to physically remove chunks |
| `wal-engine` | GC checks WAL for in-flight chunk references before deletion |
| `cache-engine` | GC calls `invalidate_chunk` after deletion to prevent stale cache reads |
| `staging` | Incomplete staging slots are cleaned up post-crash (see `crash_recovery.md`) |
