# Write Flow — RedirectFS

This document describes the complete streaming write pipeline from the moment a FUSE write request arrives to the final atomic metadata commit.

---

## Overview

```
FUSE write(path, data, offset)
    │
    ▼
[Staging Layer]  ←── receives raw byte stream
    │
    ▼
[Chunk Engine]  ←── splits stream into 4 MB chunks
    │              computes BLAKE3 hash per chunk
    ▼
[Object Store]  ←── persists each chunk immediately
    │              (dedup: skip if chunk_id already exists)
    ▼
[WAL Engine]  ←── logs chunk_id + txn_id after each store
    │
    ▼ (on file close / fsync)
[Metadata Engine]  ←── CAS-commits new FileObject version
    │
    ▼
[Cache Engine]  ←── invalidates/updates metadata cache
    │
    ▼
[Staging]  ←── cleanup: discard staging slot
    │
    ▼
FUSE returns success to application
```

---

## Step-by-Step Pipeline

### Step 1 — FUSE Intercepts the Write Syscall

When an application calls `write(fd, buf, count)` or `open(..., O_WRONLY | O_CREAT)`, the Linux kernel delivers the request to `fuse-layer`.

`fuse-layer` records:
- Virtual path / inode (mapped back via `inode_map`).
- Byte data and write offset.
- File handle state (is this a new file or an existing file?).

For a new file, `fuse-layer` requests a new `ObjectId` from `metadata-engine` and opens a `StagingHandle` for this transaction.

---

### Step 2 — Staging Slot Opened

`staging` opens a new slot identified by a `TxnId`:

```
staging/
  txn_<TxnId>/
    buffer.tmp     ← raw incoming bytes
    chunks.tmp     ← list of committed chunk_ids so far
    redirect.tmp   ← partial FileObject being built
```

The staging area is **not visible** to the live filesystem. Even if the system crashes mid-write, no partial data is exposed.

---

### Step 3 — Streaming Buffer and Chunking

FUSE delivers writes in small blocks (typically 4 KB–128 KB). The `write-engine` accumulates these into a **streaming buffer** inside the staging area.

When the buffer reaches the target **chunk size (4 MB)**, `chunk-engine` processes it:

```
1. Take 4 MB slice from buffer
2. Compute BLAKE3 hash  →  ChunkId
3. Create Chunk { id, data, size, index }
4. Emit chunk to object-store
```

The last chunk of a file may be smaller than 4 MB (that is fine — it is emitted on file close).

---

### Step 4 — Chunk Written to Object Store

`object-store` receives each chunk:

1. **Dedup check** — `has_chunk(chunk_id)` returns true → skip write (chunk already stored from a previous file or version). This is the deduplication fast path.
2. **Atomic chunk write** — if new, write to:
   ```
   /objects/<first2hex>/<next2hex>/<full_hash>.chunk
   ```
   Write is atomic: data is written to a temp file first, then renamed.
3. `chunk_id` is appended to `chunks.tmp` in the staging slot.

---

### Step 5 — WAL Logging

After each chunk is durably written to the object store, `wal-engine` appends a log entry:

```
WalEntry::ChunkWritten { chunk_id, txn_id }
```

The WAL is **append-only**. A `fsync` is issued after each append to ensure durability.

This means: if the system crashes after Step 4 but before Step 8, crash recovery can:
- See the chunk in the WAL.
- Verify the chunk exists in the object store.
- Either roll it into a new metadata version or mark it as orphaned for GC.

---

### Step 6 — Repeat for All Data

Steps 3–5 repeat as FUSE delivers more data. Large files produce many chunks; each is independently hashed, stored, and logged before moving on.

---

### Step 7 — File Close / Fsync

When the application closes the file descriptor (`close(fd)`) or calls `fsync(fd)`:

1. `chunk-engine` flushes the remaining buffer (< 4 MB) as the final chunk.
2. All chunk_ids are now known.
3. A new `FileObject` is assembled:

```rust
FileObject {
    object_id:    existing_or_new_id,
    version:      current_version + 1,
    size:         total_bytes_written,
    chunk_ids:    ordered Vec<ChunkId>,
    modified_at:  now(),
    content_hash: blake3_of_all_chunks,
    state:        FileState::Active,
}
```

---

### Step 8 — Atomic CAS Commit

`metadata-engine` performs a **Compare-And-Swap (CAS) commit**:

```
IF stored_version == expected_version:
    write new FileObject version atomically
    RETURN new_version_number
ELSE:
    RETURN VersionConflict error
```

On `VersionConflict`, the write engine can:
- **Retry** (re-read current version, merge if applicable).
- **Abort** (return `EIO` or `EBUSY` to the application).

On success, `wal-engine` appends:
```
WalEntry::MetaCommit { object_id, version: new_version }
WalEntry::TxnCommit  { txn_id }
```

The file is now **live** in the filesystem with its new version.

---

### Step 9 — Cache Update

`cache-engine` is updated:
- `invalidate_meta(object_id)` — evict stale FileObject from metadata cache.
- `put_meta(object_id, new_file_object)` — optionally warm the new version in.
- New chunks are inserted into chunk cache (if they fit in RAM budget).

---

### Step 10 — Staging Cleanup

`staging.commit(handle)` marks the staging slot as done. The staging directory for this transaction is deleted:

```
rm -rf staging/txn_<TxnId>/
```

The path index is updated via `index-engine` if this was a new file (insert) or rename.

---

## Handling Write Failures

| Failure Point | State Left | Recovery Action |
|---|---|---|
| Crash during chunk write (Step 4) | Partial chunk on disk | GC detects orphan chunk; no metadata reference → safe delete |
| Crash during WAL append (Step 5) | Chunk exists, not in WAL | Chunk is orphaned on next GC scan |
| Crash during CAS commit (Step 8) | Chunks stored, meta not committed | WAL replay finds committed chunks but no MetaCommit → GC orphans them |
| CAS conflict (concurrent writer) | No change | Write engine retries or returns error |

All failure paths are safe: no partial or corrupt state is ever exposed to users.

---

## Concurrency During Writes

- Multiple concurrent writes to **different files** are fully independent.
- Concurrent writes to the **same file** are serialized via CAS: only one writer wins; the other must retry.
- Read operations during an in-progress write see the **previous committed version** (no dirty reads).

---

## Write Performance Notes

- Streaming model: FUSE blocks are processed incrementally; no full file is held in memory.
- Deduplication at chunk granularity: identical chunks across files/versions stored only once.
- WAL fsync overhead: each chunk requires one WAL fsync; for sequential large-file writes this is amortized.
- Chunk size tuning: 4 MB default balances dedup granularity vs. per-chunk overhead. Configurable.
