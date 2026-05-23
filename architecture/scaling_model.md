# Scaling Model — RedirectFS

This document describes how RedirectFS scales from a single gigabyte on one machine to 100 TB across a distributed cluster, including the architectural changes required at each tier.

---

## Overview

RedirectFS is designed with scaling in mind from the start. The core architecture (immutable chunks, content-addressing, versioned metadata) is the same at all scales. What changes between tiers is:
- Where metadata is stored and indexed.
- How many machines serve chunk reads/writes.
- How the namespace is partitioned.
- What replication and consistency guarantees are provided.

---

## Tier 1 — Single Machine (1 GB → ~1 TB)

### Profile

| Dimension | Value |
|---|---|
| Storage | 1 GB – 1 TB local SSD/HDD |
| Files | Up to ~1 million |
| Users | Single user or small team |
| Hardware | 1 server or workstation |

### Architecture

- Single instance of all modules running in one Rust process.
- Metadata stored in SQLite (WAL mode) on local disk.
- Object store in a local `/objects/` directory.
- RAM cache: 512 MB – 4 GB (proportional to available RAM).
- SSD cache: 20 GB – 200 GB.
- Index: in-memory B-tree fits entirely in RAM.

### Bottlenecks at This Tier

- Very few; single-machine I/O is the ceiling.
- SQLite metadata engine is sufficient for < 5 million file records.
- No sharding, replication, or distributed coordination needed.

### Performance Expectations

| Operation | Latency |
|---|---|
| Metadata lookup (RAM cache) | < 100 µs |
| Small file read (RAM cache) | < 200 µs |
| Large file read (SSD, parallel) | 50–500 ms |
| Write (4 MB chunk) | 5–50 ms |

---

## Tier 2 — Single Machine, High Volume (1 TB → 10 TB)

### Profile

| Dimension | Value |
|---|---|
| Storage | 1 TB – 10 TB (NVMe SSD required) |
| Files | 1 million – 50 million |
| Users | Team or department |
| Hardware | 1 high-spec server (NVMe, 32+ GB RAM) |

### Architecture Changes

- **Migrate metadata from SQLite → RocksDB** (LSM-tree based, handles 50M+ keys efficiently).
- **Expand index engine** to use full on-disk LSM-tree (two-tier index, see `indexing_system.md`).
- **Increase RAM cache** to 8–32 GB.
- **SSD tiering**: separate NVMe for `/objects/` (sequential writes) from SSD used for `/cache/`.
- **Async GC** is more important: runs every 15 minutes vs. 30 minutes at Tier 1.
- **Index compaction** runs on a background core to maintain query performance.

### Key Optimizations Activated

- **Read-ahead prefetch** becomes critical for large sequential workloads.
- **Chunk dedup ratio** starts to matter significantly — deduplicated storage saves meaningful GBs.
- **Metadata sharding within a single node**: shard metadata files by object_id prefix into separate RocksDB column families for parallel access.

### Bottlenecks to Watch

- Metadata write throughput (RocksDB WAL fsync is the bottleneck for write-heavy workloads).
- GC scan time grows linearly with chunk count — ensure GC runs frequently enough.
- Index LSM compaction pauses can cause brief latency spikes.

---

## Tier 3 — Multi-Node Cluster (10 TB → 100 TB)

### Profile

| Dimension | Value |
|---|---|
| Storage | 10 TB – 100 TB distributed |
| Files | 50 million – 1 billion |
| Users | Organization-scale |
| Hardware | 3–100 nodes |

### Architecture Changes

This tier introduces **distributed design**. The monolithic single-process architecture splits into distinct roles:

```
┌───────────────────────────────────────────────────────────┐
│                    Client Nodes                            │
│   (run FUSE layer, path resolver, cache engine)           │
└────────────────────────┬──────────────────────────────────┘
                         │ gRPC / UNIX socket
┌────────────────────────▼──────────────────────────────────┐
│                  Metadata Service Cluster                  │
│   (3–5 nodes, Raft consensus, RocksDB storage)            │
│   Each node owns a shard of the namespace                 │
└────────────────────────┬──────────────────────────────────┘
                         │
┌────────────────────────▼──────────────────────────────────┐
│                  Object Store Cluster                      │
│   (N storage nodes, each with local /objects/ directories)│
│   Chunks distributed by consistent hashing of ChunkId     │
└───────────────────────────────────────────────────────────┘
```

### Namespace Sharding (Metadata)

The file namespace is partitioned across metadata nodes by **path prefix**:

| Shard | Owns |
|---|---|
| Shard 0 | `/a/` – `/f/` |
| Shard 1 | `/g/` – `/n/` |
| Shard 2 | `/o/` – `/z/` |

Or more precisely, by **ObjectId range** (consistent hash ring), which ensures even distribution regardless of naming patterns.

Each metadata shard is replicated across 3 nodes for fault tolerance (Raft consensus: 2 nodes can fail and the shard remains available).

### Chunk Distribution (Object Store)

Chunks are distributed across storage nodes using **consistent hashing**:

```
storage_node = consistent_hash(chunk_id) mod num_storage_nodes
```

When a storage node is added or removed, only a proportional fraction of chunks need to be migrated (consistent hashing minimizes resharding cost).

Each chunk is replicated to **3 storage nodes** (configurable). Reads are served from any replica; writes must reach 2 of 3 replicas before being acknowledged (quorum write).

### CAS and Concurrency at Scale

Distributed CAS uses the metadata service cluster:

- CAS operations are routed to the correct metadata shard leader.
- The Raft leader serializes all writes to that shard.
- Version numbers remain monotonic within each shard.
- Cross-shard operations (e.g., move a file between namespace partitions) use a two-phase commit coordinated by `system-core`.

### Distributed Cache

Each client node has its own local RAM + SSD cache. A **distributed cache coherence protocol** is needed:

- When a client commits a new file version, it broadcasts an invalidation message to other clients via a pub/sub channel (e.g., Redis Pub/Sub or a custom gossip protocol).
- Other clients evict the stale metadata entry from their local cache.
- Chunk caches do not need coherence — chunks are immutable.

### Distributed GC

GC at Tier 3 runs as a distributed coordinator:

```
GC Coordinator node:
  1. Request metadata snapshot from all metadata shards.
  2. Build global live_set of chunk IDs.
  3. Distribute scan work: each storage node scans its own /objects/.
  4. Storage nodes report candidate orphans to coordinator.
  5. Coordinator cross-checks against live_set.
  6. Coordinator instructs storage nodes to delete confirmed orphans.
```

GC is safe to run even if some nodes are temporarily unavailable — chunks on offline nodes are skipped and scanned in the next GC cycle.

---

## Scaling Milestones Summary

| Stage | Storage | Architecture | Key Technology |
|---|---|---|---|
| **1** | 1 GB – 1 TB | Single process | SQLite, local disk, in-memory B-tree |
| **2** | 1 TB – 10 TB | Single process, optimized | RocksDB, NVMe, on-disk LSM index |
| **3** | 10 TB – 100 TB | Multi-node cluster | Raft metadata, consistent-hash object store, distributed GC |
| **4** (future) | 100 TB+ | Geo-distributed | Multi-region replication, eventual consistency for reads |

---

## Scaling Design Invariants

These properties hold at every scale:

1. **Chunk immutability** — immutable chunks work identically on 1 node or 100 nodes.
2. **Content addressing** — `ChunkId = BLAKE3(content)` is independent of node topology.
3. **CAS-based versioning** — works with a local mutex, a Raft leader, or a distributed lock service.
4. **WAL ordering** — per-shard WAL in distributed mode; global ordering via Raft log.
5. **Two-phase GC** — Mark+Sweep is embarrassingly parallelizable across nodes.
6. **Staging isolation** — per-client staging directories work regardless of cluster size.

---

## Migration Path Between Tiers

Upgrading from Tier 1 to Tier 2:
1. Export SQLite metadata to RocksDB format (offline migration tool).
2. Rebuild index from metadata files.
3. Restart with new configuration.
4. Zero data loss — `/objects/` is unchanged.

Upgrading from Tier 2 to Tier 3:
1. Stand up metadata service cluster.
2. Bulk-import metadata from single-node RocksDB into distributed shards.
3. Add storage nodes; redistribute chunks via consistent hash migration.
4. Switch client configuration to point to cluster endpoints.
5. Zero data loss — chunk files are rsync-migrated, not rewritten.
