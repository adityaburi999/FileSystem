# Cache System — RedirectFS

This document describes the multi-layer caching architecture of RedirectFS: what is cached, where, how data flows between layers, and how eviction and invalidation are handled.

---

## Why Caching Is Critical

RedirectFS stores data as immutable chunks on disk. Without caching:
- Every read requires at least one disk seek to the object store.
- Metadata lookups require SQLite reads.
- Hot files (frequently accessed) would saturate disk I/O.

The cache system ensures that **hot data is served from RAM at microsecond latency** and **warm data is served from SSD at millisecond latency**, with cold data falling through to the object store.

---

## Cache Architecture Overview

```
Application Read
      │
      ▼
┌─────────────────────────────────────┐
│         L1 — RAM Cache              │   < 100 µs
│  Hot chunks + hot metadata objects  │
│  In-process memory (Tokio-safe)     │
│  LRU/LFU eviction                   │
└──────────────────┬──────────────────┘
                   │ miss
                   ▼
┌─────────────────────────────────────┐
│         L2 — SSD Cache              │   1–10 ms
│  Warm chunks + reconstructed files  │
│  Stored in /cache/ssd/              │
│  Larger than RAM cache              │
└──────────────────┬──────────────────┘
                   │ miss
                   ▼
┌─────────────────────────────────────┐
│     L3 — Object Store (Disk)        │   10–100 ms
│  Cold data, all chunks              │
│  /objects/ directory tree           │
└─────────────────────────────────────┘
```

---

## What Gets Cached

| Data Type | Cache Level | Key | TTL |
|---|---|---|---|
| Chunk data | RAM + SSD | `ChunkId` | Access-based (LRU) |
| FileObject metadata | RAM | `ObjectId` | On commit (invalidate) |
| DirObject metadata | RAM | `ObjectId` | On commit (invalidate) |
| Path → ObjectId | RAM (B-tree) | Path string | On rename/delete |
| Reconstructed file buffers | SSD | `ObjectId + version` | Short TTL (minutes) |
| Metadata SQLite results | RAM | Query hash | Short TTL |

---

## L1 — RAM Cache

### Structure

```rust
pub struct RamCache {
    // Chunk cache: ChunkId → Bytes
    chunk_store: LruCache<ChunkId, Bytes>,
    chunk_store_bytes: usize,     // current bytes in use
    chunk_store_limit: usize,     // configurable (e.g., 512 MB)

    // Metadata cache: ObjectId → CachedMeta
    meta_store: LruCache<ObjectId, CachedMeta>,
    meta_store_limit: usize,      // configurable (e.g., 128 MB)
}

pub struct CachedMeta {
    pub object: MetaObject,    // FileObject or DirObject
    pub cached_at: Instant,
}

pub enum MetaObject {
    File(FileObject),
    Dir(DirObject),
}
```

### Eviction Policy

RedirectFS uses a **LRU/LFU hybrid**:

- **LRU (Least Recently Used)** is the baseline — evict the entry not accessed for the longest time.
- **LFU boost** — items accessed more than `FREQ_THRESHOLD` times (default: 5) in a window are protected from eviction even if not recently used.

This prevents "scan pollution" — a large sequential read scanning thousands of chunks should not evict hot small-file chunks from the cache.

Implementation: `indexmap` + access frequency counter per entry.

### Thread Safety

The RAM cache is wrapped in `tokio::sync::RwLock`:
- Multiple concurrent readers can access the cache simultaneously.
- Writers (insert / evict) hold an exclusive write lock for the minimum time possible.

---

## L2 — SSD Cache

### Purpose

The SSD cache holds chunks that were recently evicted from RAM but are likely to be accessed again. It acts as a large, slower extension of the RAM cache.

### Storage Layout

```
/cache/
  ssd/
    <prefix_hex>/
      <chunk_hash>.cached
  reconstructed/
    <object_id>_v<version>.reconstructed
```

### Format

SSD cache files use the same chunk format as the object store (header + data + trailer) for consistency. This means integrity checks are identical for both.

### SSD Cache Management

```rust
pub struct SsdCache {
    pub root: PathBuf,          // /cache/ssd/
    pub capacity_bytes: u64,    // configurable (e.g., 20 GB)
    pub used_bytes: AtomicU64,
    pub eviction_log: LruTracker<ChunkId>, // LRU tracker for eviction decisions
}
```

When SSD cache is full, the least-recently-used cached file is deleted to make room for the new entry.

### Promotion to RAM

When a chunk is read from SSD cache, it is promoted to RAM cache automatically (if it fits). This ensures repeated access to the same chunk gradually migrates it to the fastest tier.

---

## L3 — Object Store (Cold Path)

When neither RAM nor SSD cache has a chunk, `object-store` loads it directly from `/objects/`. After loading, the chunk is inserted into both SSD and RAM caches (if capacity allows).

---

## Metadata Cache

### File and Directory Object Caching

`FileObject` and `DirObject` reads are extremely common (every FUSE operation requires at least one metadata load). These are cached in the RAM L1 metadata store.

**Invalidation rules:**
- On successful `commit_file(id, new, v)` → `cache.invalidate_meta(id)` then `cache.put_meta(id, new)`.
- On `tombstone_file(id, v)` → `cache.invalidate_meta(id)`.
- On `add_dir_entry` or `remove_dir_entry` → `cache.invalidate_meta(dir_id)`.

### Path Cache

The path index B-tree (see `indexing_system.md`) doubles as a metadata cache for path → ObjectId lookups. It is maintained separately from the chunk/object cache.

---

## Cache Invalidation

RedirectFS uses **event-driven invalidation** (not TTL-based for metadata):

| Event | Cache Action |
|---|---|
| New file version committed | `invalidate_meta(object_id)` + `put_meta(object_id, new)` |
| File tombstoned | `invalidate_meta(object_id)` |
| Chunk deleted by GC | `invalidate_chunk(chunk_id)` from all levels |
| Directory updated | `invalidate_meta(dir_id)` |
| Crash recovery completes | Full cache flush (state may be inconsistent) |

For chunks, invalidation is only needed when GC physically deletes them. Since chunks are immutable (content never changes), cache entries are always valid as long as the chunk exists.

---

## Read-Ahead Prefetch

The cache engine implements **sequential access detection** and read-ahead:

```
If last 3 consecutive reads are sequential (offset increasing by ~CHUNK_SIZE):
    Pre-fetch the next N chunks asynchronously (default: N=4)
    Store in RAM cache before the application requests them
```

This hides the latency of the next read entirely for sequential workloads (log processing, video streaming, large file copies).

---

## Write Buffering

For write operations:
- New chunks written during a transaction are inserted into RAM cache immediately after being persisted to the object store.
- This ensures a read following a write is served from cache, not cold storage.
- On transaction abort, these cache entries are invalidated.

---

## Cache Warming on Startup

On system startup (after crash recovery), the cache is cold. Optionally:
- Read `/system/state/hot_chunks.log` (written periodically during operation) to pre-warm the top N most accessed chunks.
- Pre-load root `DirObject` and top-level directory objects into metadata cache.

This reduces the cold-start latency spike that users would otherwise experience.

---

## Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `cache.ram.chunk_limit_mb` | 512 | RAM chunk cache size |
| `cache.ram.meta_limit_mb` | 128 | RAM metadata cache size |
| `cache.ssd.capacity_gb` | 20 | SSD cache capacity |
| `cache.ssd.path` | `/cache/ssd/` | SSD cache directory |
| `cache.readahead.chunks` | 4 | Read-ahead chunk count |
| `cache.eviction.freq_threshold` | 5 | LFU protection frequency |
| `cache.warmup.enabled` | true | Pre-warm on startup |

---

## Cache Bypass

Certain operations bypass the cache intentionally:
- **GC scans** — scanning all chunks for orphan detection should not pollute the cache with cold data.
- **Crash recovery** — reads during recovery use direct object store access.
- **Integrity verification** — explicit `fsck`-style checks bypass cache to hit real disk state.
