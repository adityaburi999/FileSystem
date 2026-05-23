# Indexing System — RedirectFS

This document describes how RedirectFS achieves fast path-to-object_id lookups using a multi-structure indexing approach.

---

## Why Indexing Is Needed

Without an index, resolving a path like `/data/projects/reports/q1.csv` requires:
1. Load root `DirObject`.
2. Scan entries → find `data` → load its `DirObject`.
3. Scan entries → find `projects` → load its `DirObject`.
4. Scan entries → find `reports` → load its `DirObject`.
5. Scan entries → find `q1.csv` → return `ObjectId`.

Each step is a random disk read. For a 10-component path in a large directory, this is 10 sequential metadata reads — unacceptable at scale.

The index engine provides **O(log N) path lookup** in a single query.

---

## Index Architecture

RedirectFS uses a **two-tier index**:

```
┌──────────────────────────────────────────────────────┐
│ Tier 1 — In-Memory B-tree (hot path)                 │
│  path string → ObjectId                              │
│  Resident in RAM; fits hot working set               │
│  Sub-microsecond lookup on cache hit                 │
└──────────────────────────────────────────────────────┘
                      │ miss
┌──────────────────────────────────────────────────────┐
│ Tier 2 — On-Disk LSM-Tree (persistent, full index)   │
│  Stored under /system/index/                         │
│  Handles full path namespace (all files)             │
│  Millisecond lookup; compacted in background         │
└──────────────────────────────────────────────────────┘
                      │ miss (cold start / rebuild)
┌──────────────────────────────────────────────────────┐
│ Fallback — Metadata Graph Walk                       │
│  Walk DirObject chain from root                      │
│  Used only when index is unavailable or stale        │
└──────────────────────────────────────────────────────┘
```

---

## Tier 1 — In-Memory B-tree

### Structure

```rust
use std::collections::BTreeMap;

struct MemIndex {
    // Sorted map: full path → ObjectId
    entries: BTreeMap<String, ObjectId>,
    // Approximate size limit (evict LRU when exceeded)
    max_entries: usize,
}
```

The in-memory B-tree is sorted by path string, enabling:
- Exact path lookup: `O(log N)`.
- Prefix scans: `range("/data/projects/"..)` to list all files in a directory subtree.
- Directory listing: filter entries with prefix `/dir/`.

### Eviction

When memory pressure is detected, the least-recently-used entries are evicted. They remain in Tier 2 and will be reloaded on next access.

### Consistency

The in-memory index is a **cache** — it may be stale by microseconds. Path resolver always falls back to Tier 2 or graph walk if the in-memory entry is absent or if the metadata version has advanced.

---

## Tier 2 — On-Disk LSM-Tree

### Why LSM?

Log-Structured Merge-trees are optimal for write-heavy index workloads:
- Writes are always sequential (append to memtable → flush to SSTable).
- No in-place updates → crash-safe by design.
- Compaction merges older SSTables → keeps read performance bounded.

### Structure

```
/system/index/
  memtable.log     ← recent writes (append-only WAL for the index itself)
  level0/
    sst_0001.sst
    sst_0002.sst
  level1/
    sst_0001.sst
  level2/
    sst_0001.sst
```

Each SSTable (`.sst`) file is an immutable sorted map of `path_bytes → ObjectId`.

### Index Entry Format

```rust
pub struct IndexEntry {
    pub path:      String,    // full virtual path, e.g. "/data/reports/q1.csv"
    pub object_id: ObjectId,  // target file or directory ObjectId
    pub version:   u64,       // metadata version at time of index write
    pub deleted:   bool,       // tombstone marker (path removed)
}
```

Deleted paths are written as tombstone entries. During compaction, tombstones suppress older entries and are then removed.

### Lookup

```
1. Check memtable (in-memory log buffer) — O(log M)
2. Check Level 0 SSTables (newest first) — O(K * log N) where K = number of L0 files
3. Check Level 1 SSTable (single merged file) — O(log N)
4. Check Level 2+ — O(log N) per level
```

Bloom filters on each SSTable make negative lookups (path not present) very fast (one bit-array check per file).

### Compaction

Background Tokio task compacts the LSM-tree:
- **Minor compaction**: flush memtable to a new Level 0 SSTable when memtable exceeds threshold.
- **Major compaction**: merge Level 0 SSTables into Level 1 when Level 0 has too many files.
- **Full compaction**: merge all levels (triggered manually or on large namespace operations).

During compaction, the index remains fully readable (old SSTables are still valid until new ones are atomically promoted).

---

## Path Index API

```rust
pub trait PathIndex: Send + Sync {
    /// Exact path → ObjectId lookup.
    fn lookup(&self, path: &VirtualPath) -> Result<Option<ObjectId>, IndexError>;

    /// Insert a path → ObjectId mapping.
    fn insert(&self, path: &VirtualPath, id: &ObjectId) -> Result<(), IndexError>;

    /// Update mapping on rename/move.
    fn rename(&self, old_path: &VirtualPath, new_path: &VirtualPath) -> Result<(), IndexError>;

    /// Remove a path from the index.
    fn remove(&self, path: &VirtualPath) -> Result<(), IndexError>;

    /// List all entries with a given path prefix (directory listing).
    fn list_prefix(
        &self,
        prefix: &VirtualPath,
    ) -> Result<impl Iterator<Item = (String, ObjectId)>, IndexError>;
}
```

---

## Index Consistency Guarantees

| Situation | Index Behavior |
|---|---|
| New file created | Inserted after successful CAS metadata commit |
| File renamed | Old path removed, new path inserted atomically |
| File deleted (tombstone) | Path removed immediately after CAS tombstone |
| Index rebuild after crash | Walk entire metadata directory graph, repopulate LSM |
| Index stale (rare edge case) | Path resolver falls back to metadata graph walk |

The index is **eventually consistent** with metadata: it may lag by a few microseconds during high write throughput. This is safe because:
- Metadata is the source of truth.
- Index misses fall back to graph traversal.
- No stale index entry can cause incorrect data to be returned (worst case: slightly slower lookup).

---

## Index Rebuild Process

If the index is detected as corrupt or missing on startup:

```
1. Scan /metadata/dirs/ starting from ROOT_OBJECT_ID.
2. For each DirObject, iterate its entries.
3. For each FileObject entry: insert(full_path, object_id).
4. Recurse into sub-directories.
5. Write a fresh memtable + flush to Level 0 SSTable.
6. Mark index as rebuilt in /system/state/index_status.
```

Rebuild time: roughly proportional to file count. Expected ~1 million files/minute on modern SSD.

---

## Directory Listing Optimization

`list_prefix("/data/")` returns all paths under `/data/`. Combined with the in-memory B-tree's range query, directory listings are served without loading each child's `DirObject` individually:

```rust
let entries = index.list_prefix(&VirtualPath::new("/data/"))?;
// entries: [("/data/a.txt", id1), ("/data/b.txt", id2), ("/data/sub/c.txt", id3)]
```

This provides `ls /data/` in near-constant time regardless of directory depth.

---

## Future Index Improvements

- **Prefix compression** in SSTables — paths share common prefixes; compress them for space savings.
- **Directory-scoped sub-indexes** — separate index shards per top-level directory for parallel access.
- **Distributed index** — at 100TB scale, partition the index by path prefix across multiple nodes.
