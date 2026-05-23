# Staging System — RedirectFS

This document describes the staging area: the temporary write buffer that makes in-progress writes invisible to the live filesystem and crash-safe.

---

## Core Purpose

The staging system solves the **partial write problem**:

> A write takes time. During that time, the file must appear unchanged to all readers. If the system crashes mid-write, no partial data should be visible or accessible after recovery.

Staging achieves this by keeping all in-flight write data in a **hidden, isolated area** that is only promoted to the live filesystem at the moment of atomic commit.

---

## Staging Is Invisible to Users

The `/staging/` directory is:
- Hidden from FUSE directory listings.
- Never returned by path resolution.
- Not accessible via any POSIX filesystem call from user processes.

FUSE enforces this via a path filter: any attempt to `open`, `stat`, or `readdir` on `/staging/` returns `EACCES`.

---

## Staging Slot Structure

Every in-progress write transaction gets its own **staging slot**, identified by its `TxnId`:

```
/staging/
  txn_<TxnId>/
    buffer.tmp        ← raw incoming bytes from FUSE (partial, streaming)
    chunks.tmp        ← list of chunk IDs that have been persisted to object store
    redirect.tmp      ← partial FileObject being built (JSON or Bincode)
    meta.json         ← transaction metadata (created_at, object_id, state)
```

### `buffer.tmp`

The streaming write buffer. FUSE write blocks are appended here until a full chunk (4 MB) is accumulated. At that point, `chunk-engine` processes the buffer and flushes it.

`buffer.tmp` may be empty if all buffered data has already been chunked.

### `chunks.tmp`

An append-only list of `ChunkId` values (32 bytes each) that have been successfully written to the object store and logged in the WAL. This list is the "progress marker" for recovery.

Format:
```
[u32 count]  [ChunkId × count]
```

### `redirect.tmp`

The partial `FileObject` being assembled. Updated incrementally as chunks are added:
```rust
PartialFileObject {
    object_id:  ObjectId,
    size_so_far: u64,
    chunk_ids:  Vec<ChunkId>,   // grows as chunks are committed
    expected_version: u64,      // version to CAS-commit against
}
```

### `meta.json`

Lightweight JSON metadata for the staging slot:
```json
{
  "txn_id": "txn_0042",
  "object_id": "fa82c1...",
  "created_at": 1716454800000,
  "state": "open"
}
```

`state` transitions: `open` → `committed` or `aborted`.

---

## Staging Lifecycle

```
open(TxnId)
    │
    ▼ (repeated for each 4MB chunk)
write(handle, raw_bytes)
    │
    ▼
record_chunk(handle, chunk_id)   ← after chunk persisted to object store + WAL
    │
    ▼ (on file close)
commit(handle)    ← metadata CAS commit has succeeded
    │              staging directory deleted
    │
    OR
    │
    ▼ (on error / abort)
discard(handle)   ← staging directory deleted, transaction rolled back
```

---

## Crash Safety Guarantees

Staging makes the write pipeline crash-safe at every step:

| Crash Point | State in Staging | Recovery Action |
|---|---|---|
| Before any chunk written | `buffer.tmp` has partial data, `chunks.tmp` empty | Discard staging; no object store cleanup needed |
| After N chunks written, before commit | `chunks.tmp` has N chunk IDs | Recovery: chunks exist in object store, not in metadata → GC orphans them |
| After metadata commit, before staging cleanup | `meta.json` state = `committed` | Recovery: staging is stale → safe to delete |
| During staging cleanup | Staging directory partially deleted | Recovery: finish deletion on next boot |

**Key insight:** After a crash, the system can always determine the correct action by checking:
1. Does `meta.json` show `state = committed`? → delete staging, all is well.
2. Is there a matching `TxnCommit` entry in WAL? → same as above.
3. Otherwise? → discard the staging slot; chunks will be orphaned and GC handles them.

---

## Staging and Concurrent Writes

Each open file handle gets its own staging slot. Concurrent writes to different files use entirely separate staging directories — there is no shared mutable state between slots.

Concurrent writes to the **same file** from different handles:
- Each gets its own staging slot.
- Both stage their chunks independently.
- CAS at commit time ensures only one wins.
- The loser's staging slot is discarded; its chunks become GC orphans.

---

## Staging Atomicity

The transition from "write in progress" to "committed" is made atomic by two mechanisms:

1. **CAS metadata commit** — the `FileObject` is only visible after a successful CAS in metadata-engine. Until then, the old version (or nothing, for new files) remains the live state.

2. **WAL `TxnCommit`** — after the CAS succeeds, `wal-engine` appends `TxnCommit`. This is the point of no return. After this entry, the new version is permanent.

The staging directory cleanup (deleting `txn_<TxnId>/`) is a **post-commit housekeeping step**. If it fails (crash), recovery detects the committed state and cleans up on the next boot.

---

## Staging Cleanup on Boot

During crash recovery (Step 6 in `crash_recovery.md`), `staging.list_open()` returns all staging slots that were open before the crash.

For each open slot:
```
1. Read meta.json → check state.
2. Check WAL for TxnCommit entry.
3. If committed (either signal) → cleanup staging directory.
4. If not committed → discard staging directory.
   - Chunks in chunks.tmp will not be referenced by any metadata.
   - They become orphan candidates for post-recovery GC scan.
```

---

## Staging Size Limits

To prevent unbounded disk usage from long-running writes:

| Limit | Default | Behavior when exceeded |
|---|---|---|
| Max single staging slot size | 10 GB | Write returns `ENOSPC`; transaction aborted |
| Max total staging directory size | 50 GB | New writes block until space is freed |
| Max number of open staging slots | 1000 | New opens return `EMFILE`; wait for closes |

These limits are configurable per filesystem instance.

---

## Staging vs. WAL

Staging and WAL serve complementary roles:

| Aspect | Staging | WAL |
|---|---|---|
| What it stores | Raw write data + chunk progress | Ordered log of committed events |
| Visibility | Hidden from users | Internal to recovery engine |
| Lifetime | Deleted on commit or abort | Retained until checkpoint |
| Primary role | Isolate in-progress writes | Enable crash recovery + ordering |
| Size | Up to GB per transaction | Small (log entries only, no data) |

The WAL does not duplicate staging data. The WAL records *what happened* (chunk IDs, commit events); staging stores *the actual in-flight data*.
