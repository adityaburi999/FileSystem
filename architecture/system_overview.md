# System Overview — RedirectFS

## What Is RedirectFS?

RedirectFS is a research-grade virtual filesystem and object storage engine built in Rust. It mounts as a regular Linux filesystem via FUSE, but internally replaces the traditional "mutable file on disk" model with a **versioned, immutable, content-addressed object graph**.

Every file is represented as a *redirect object* — a pointer that resolves to a list of immutable chunks. Updates never modify existing data; they create a new version of the redirect object pointing to new or reused chunks.

---

## Design Goals

| Goal | Mechanism |
|---|---|
| Crash safety | Write-Ahead Logging (WAL) + atomic CAS commit |
| Data integrity | BLAKE3 content hashing of every chunk |
| Deduplication | Content-addressed object store |
| Versioning | Immutable redirect objects with version numbers |
| Performance | Multi-layer cache + indexed metadata |
| Scalability | Sharded layout, future distributed design |

---

## High-Level Layer Stack

```
┌─────────────────────────────────────────────────────┐
│                  User Applications                   │
└────────────────────────┬────────────────────────────┘
                         │ POSIX syscalls
┌────────────────────────▼────────────────────────────┐
│                    FUSE Layer                        │
│   Intercepts open/read/write/mkdir/unlink/stat       │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────┐
│                  Path Resolver                       │
│   /a/b/c.txt  →  object_id via index + graph walk   │
└────────┬───────────────────────────┬────────────────┘
         │                           │
┌────────▼────────┐       ┌──────────▼───────────────┐
│ Metadata Engine │       │      Index Engine         │
│ file/dir objects│       │  B-tree / LSM fast lookup │
│ versioned state │       └──────────────────────────┘
└────────┬────────┘
         │
┌────────▼──────────────────────────┐
│         Write / Read Engine        │
│  streaming writes, CAS commits     │
│  chunk fetch, file reconstruction  │
└────────┬──────────────────────────┘
         │
┌────────▼────────┐    ┌────────────────┐
│  Chunk Engine   │    │  Cache Engine  │
│  split / hash   │    │  RAM + SSD     │
└────────┬────────┘    └────────────────┘
         │
┌────────▼────────────────────────────┐
│           Object Store               │
│  Immutable chunks, BLAKE3-named,     │
│  sharded on disk under /objects/     │
└────────┬────────────────────────────┘
         │
┌────────▼────────────────────────────┐
│           Disk Storage               │
│  /wal /metadata /objects /staging    │
│  /cache /system                      │
└─────────────────────────────────────┘
```

Supporting systems (run alongside the main pipeline):

```
WAL Engine     → logs every operation before execution
GC Engine      → background orphan detection + chunk deletion
Staging Layer  → crash-safe temporary buffer for in-flight writes
```

---

## Module Roles (Summary)

| Module | Responsibility |
|---|---|
| `fuse-layer` | Translate Linux VFS calls into RedirectFS operations |
| `path-resolver` | Walk the metadata graph to map a path to an object_id |
| `metadata-engine` | Store, version, and atomically update file/dir objects |
| `write-engine` | Buffer streaming FUSE writes, manage WAL transactions |
| `chunk-engine` | Split data into fixed-size chunks, compute BLAKE3 hashes |
| `object-store` | Persist and retrieve immutable chunks from disk |
| `wal-engine` | Append-only log for crash recovery and transaction ordering |
| `gc-engine` | Detect orphaned chunks/versions and safely free them |
| `cache-engine` | Multi-level (RAM/SSD) cache for hot data |
| `index-engine` | Fast path-to-object_id lookup via B-tree / LSM indexes |
| `staging` | Temporary write buffer invisible to the live filesystem |
| `system-core` | Orchestrates startup, shutdown, and module wiring |

---

## Data Flow at a Glance

### Read
```
FUSE read(path)
  → path-resolver resolves path → object_id
  → metadata-engine loads redirect object (chunk list)
  → cache-engine checked first
  → on miss: object-store fetches chunks in parallel
  → chunk-engine reassembles file stream
  → FUSE returns bytes to application
```

### Write
```
FUSE write(path, data)
  → staging receives buffered data
  → chunk-engine splits into 4 MB chunks, BLAKE3-hashes each
  → object-store writes new chunks atomically
  → wal-engine logs chunk IDs + transaction state
  → on close: metadata-engine CAS-commits new redirect version
  → cache-engine updated
  → staging cleaned up
```

### Delete
```
FUSE unlink(path)
  → metadata-engine marks redirect object as tombstone
  → wal-engine logs deletion
  → file removed from live namespace immediately
  → gc-engine later detects orphaned chunks → physical deletion
```

---

## Disk Layout Summary

```
/storage_root/
├── wal/          ← append-only transaction logs
├── metadata/     ← versioned redirect + directory objects
├── objects/      ← immutable BLAKE3-named chunks
├── cache/        ← disposable RAM/SSD hot data
├── staging/      ← in-flight write buffers (not visible to users)
└── system/       ← GC state, index data, snapshots (hidden)
```

---

## Technology Stack (Key Choices)

| Area | Technology |
|---|---|
| Language | Rust |
| Filesystem interface | FUSE (Linux) |
| Content hashing | BLAKE3 |
| Metadata DB (initial) | SQLite + WAL mode |
| Metadata DB (future) | RocksDB / custom LSM |
| Serialization | Serde + Bincode |
| Async runtime | Tokio |
| Caching eviction | LRU/LFU hybrid |
| Indexing | B-tree (initial) → LSM at scale |
| Frontend (future) | Tauri + React + TypeScript |

---

## Key System Invariants

1. **No in-place writes** — all mutations produce new object versions.
2. **All commits are atomic** — WAL + CAS, never partial.
3. **Object store is append-only** — chunks are written once, never modified.
4. **Metadata is the source of truth** — object store is inert without it.
5. **Deletion is always logical first** — physical removal happens via GC only.
6. **Every chunk is verified** — BLAKE3 hash checked on every read.
