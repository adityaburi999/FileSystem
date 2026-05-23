# Read Flow — RedirectFS

This document describes the complete, step-by-step pipeline that RedirectFS executes when a user (or application) reads a file.

---

## Overview

```
path string
    │
    ▼
[Path Resolver]  →  object_id
    │
    ▼
[Metadata Engine]  →  FileObject (chunk list + size + version)
    │
    ▼
[Cache Engine]  →  HIT: return cached bytes
    │ MISS
    ▼
[Object Store]  →  raw chunk bytes (parallel fetch)
    │
    ▼
[Chunk Engine]  →  reassemble ordered chunk stream
    │
    ▼
[FUSE Layer]  →  return bytes to application
```

---

## Step-by-Step Pipeline

### Step 1 — FUSE Intercepts the Read Syscall

The user opens a file with a POSIX call such as `open("/data/reports/q1.csv", O_RDONLY)` or `read(fd, buf, size)`. The Linux kernel forwards the request to the FUSE driver, which calls into `fuse-layer`.

`fuse-layer` extracts:
- The virtual path (or inode, which is mapped back to a path via `inode_map`).
- The byte range requested (`offset`, `length`).

---

### Step 2 — Path Resolution

`path-resolver` receives the virtual path and returns the `ObjectId` for the target file.

**Resolution strategy (fast path first):**

1. **Index lookup** — query `index-engine` B-tree/LSM for a direct `path → object_id` mapping. This is O(log N) and avoids graph traversal for hot paths.
2. **Graph walk fallback** — if the index misses (e.g., cold start, index not yet built), walk the metadata directory graph from the root redirect object, resolving each path component until the leaf is found.
3. On `NotFound` → return `ENOENT` to FUSE.

---

### Step 3 — Metadata Load

`metadata-engine` loads the `FileObject` for the resolved `ObjectId`.

```
FileObject {
    object_id:    ObjectId,
    version:      u64,
    size:         u64,            // total file size in bytes
    chunk_ids:    Vec<ChunkId>,   // ordered list of chunk identifiers
    created_at:   u64,
    modified_at:  u64,
    content_hash: [u8; 32],       // BLAKE3 hash of full file (optional verify)
    state:        FileState,      // Active | Tombstone
}
```

- If `state == Tombstone` → return `ENOENT` (file is logically deleted).
- The `chunk_ids` list is the complete ordered map of the file's content.

---

### Step 4 — Byte-Range to Chunk Mapping

The read engine converts the requested `(offset, length)` byte range into the minimal set of chunk IDs needed.

```
chunk_index  = offset / CHUNK_SIZE          // which chunk to start in
chunk_offset = offset % CHUNK_SIZE          // byte offset within first chunk
chunks_needed = ceil((chunk_offset + length) / CHUNK_SIZE)
```

Only the chunks that overlap the requested byte range are fetched.

---

### Step 5 — Cache Lookup

For each needed chunk ID, `cache-engine` is checked:

```
For each chunk_id in needed_chunks:
    if cache.get_chunk(chunk_id) == HIT:
        use cached bytes
    else:
        add to fetch_list
```

**Cache hierarchy:**
1. RAM cache (μs latency — hot data)
2. SSD cache (ms latency — warm data)
3. Object store (ms–tens of ms — cold data)

Metadata objects also have a separate metadata cache. If the `FileObject` is already in cache, Step 3 is served from there without a disk read.

---

### Step 6 — Parallel Chunk Fetch (Cache Miss)

For all chunks not in cache, `object-store` fetches them. Multiple chunks are fetched **concurrently** using Tokio async tasks:

```
let futures: Vec<_> = fetch_list
    .iter()
    .map(|id| object_store.read_chunk(id))
    .collect();

let results = futures::future::join_all(futures).await;
```

Each chunk is loaded from:
```
/objects/<first2hex>/<next2hex>/<full_hash>.chunk
```

On load, each chunk's BLAKE3 hash is recomputed and verified against its `ChunkId`. Hash mismatch → return `EIO` (I/O error) to FUSE and log a corruption event.

---

### Step 7 — Cache Population

Freshly fetched chunks are inserted into `cache-engine` so subsequent reads hit cache:

```
for (id, bytes) in fetched_chunks:
    cache.put_chunk(&id, bytes)
```

Eviction policy (LRU/LFU hybrid) manages cache capacity transparently.

---

### Step 8 — File Reconstruction

`chunk-engine` assembles the fetched bytes in order, applying:

- The correct `chunk_offset` for the first partial chunk.
- Truncation for the last partial chunk.
- Concatenation of all middle chunks in full.

The result is a contiguous byte buffer matching exactly the requested `(offset, length)` range.

---

### Step 9 — Return to Application

`fuse-layer` copies the assembled buffer into the FUSE reply buffer, completing the `read` syscall. The application receives its data transparently.

---

## Error Handling During Reads

| Error | Cause | FUSE Response |
|---|---|---|
| `ENOENT` | Path not found or tombstoned | `ENOENT` |
| `EIO` | Chunk hash mismatch (corruption) | `EIO` |
| `EIO` | Chunk file missing from object store | `EIO` |
| `ENOMEM` | Cache allocation failure | `ENOMEM` |
| `EAGAIN` | Temporary lock / retry needed | `EAGAIN` |

---

## Read Performance Characteristics

| Scenario | Expected Latency |
|---|---|
| Small file, RAM cache hit | < 100 µs |
| Large file, SSD cache hit | 1–10 ms |
| Cold read, object store | 10–100 ms (parallel chunks) |
| Repeated access (warm) | sub-millisecond (RAM cache) |

---

## Optimizations

- **Read-ahead prefetch** — after a sequential read pattern is detected, cache-engine pre-fetches subsequent chunks asynchronously.
- **Metadata cache** — `FileObject` is cached so path-only repeated accesses skip disk metadata reads.
- **Zero-copy where possible** — chunks served from RAM cache use `Bytes` reference-counting to avoid memcpy.
- **Parallel chunk I/O** — all chunks in a request are fetched concurrently via Tokio.
