# 🧠 RedirectFS — Next-Generation Virtual Filesystem & Storage Engine

RedirectFS is a **research-grade virtual filesystem + object storage engine** that rethinks how files are stored, accessed, secured, and scaled in modern systems.

It combines:
- FUSE-based Linux filesystem interface
- Versioned metadata graph (redirect objects)
- Immutable chunk-based storage
- Write-Ahead Logging (WAL)
- Streaming write pipeline
- Garbage collection system
- Multi-layer caching and indexing

---

# 🚀 What Problem This Solves

Traditional filesystems (ext4, NTFS, etc.) struggle with:

- Weak crash recovery in complex writes
- In-place mutation leading to corruption risk
- Slow metadata lookup at large scale
- Poor deduplication across files
- No native versioning system
- Limited scalability for TB-scale workloads

RedirectFS replaces the file model with a **versioned, immutable storage graph**.

---

# 🧩 Core Idea

Instead of storing files directly:

> Files are represented as **redirect objects pointing to immutable chunks**

This enables:
- Versioning
- Atomic updates
- Crash-safe writes
- Deduplication
- Efficient recovery

---

# 🏗️ System Architecture

## Layers:

[FUSE Layer] ↓ [Path Resolver] ↓ [Metadata Engine] ↓ [Write / Read Engine] ↓ [Chunk Engine] ↓ [Object Store] ↓ [Disk Storage]

Supporting systems:
- WAL Engine (transactions)
- GC Engine (cleanup)
- Cache Engine (performance)
- Index Engine (fast lookup)
- Staging Layer (crash safety)

---

# 📂 Disk Layout

/wal/         → transaction logs (append-only) /metadata/    → file + directory objects (versioned) /objects/     → immutable chunk storage (content-addressed) /cache/       → RAM/SSD hot data cache /staging/     → temporary incomplete writes /system/      → internal engine state (hidden)

---

# 📖 File System Model

## Files
- Represented as **redirect objects**
- Point to immutable chunk lists
- Fully versioned

## Directories
- Metadata graph nodes
- Store parent-child relationships
- Use name → object_id mapping

---

# ✍️ Write Flow (Streaming + Atomic)

FUSE write request → streaming buffer → chunking (4MB blocks) → BLAKE3 hash per chunk → store chunk immediately → WAL logging → staging update → CAS validation → atomic metadata commit → cache update

---

# 📖 Read Flow

path → metadata graph traversal → redirect object resolution → chunk list fetch → cache lookup → parallel chunk retrieval → file reconstruction

---

# 🗑️ Delete Flow

user delete → remove from live namespace → mark tombstone (logical delete) → WAL log entry → GC handles physical deletion later

---

# ♻️ Garbage Collection

Two-phase system:

## 1. Orphan Detection
- scan chunks
- check metadata references
- mark unreferenced chunks

## 2. Version Pruning
- remove old file versions (policy-based)
- free associated chunks

## 3. Cleanup
- delete orphan chunks after safety delay

---

# 🔁 Crash Recovery

system restart → read WAL logs → detect incomplete transactions → validate chunks → rebuild metadata state → discard staging data → restore consistent filesystem

---

# 🧵 Concurrency Model

- Optimistic concurrency control
- Version-based metadata
- Compare-And-Swap (CAS) commits

Rule:

IF version mismatch → abort transaction ELSE → atomic commit

Prevents silent overwrites.

---

# 📁 Folder System

- Folders are metadata directory objects (NOT real disk folders)
- Path resolution uses graph traversal
- Each folder uses:
  name → object_id mapping

---

# 🔐 Security & Tamper Resistance

RedirectFS improves security through **structure, not secrecy**:

## 1. Immutable Storage
- No in-place file modification
- All updates create new versions

## 2. Content-Addressed Chunks
- Stored using BLAKE3 hashes
- Any modification breaks integrity checks

## 3. Versioned Metadata
- Atomic pointer swaps only
- Prevents partial-state corruption

## 4. WAL Transaction System
- Every operation logged before execution
- Enables full recovery after crash

⚠️ Not “unhackable”, but:
- harder to silently corrupt
- easier to detect tampering
- safer under failure conditions

---

# ⚡ Performance & Optimization

## 1. Fast Path Lookup
- Indexed metadata engine (B-tree / LSM-tree)
- No sequential directory scanning

## 2. Multi-Level Cache
- RAM cache (hot data)
- SSD cache (warm data)
- reconstructed cache

## 3. Chunk Parallelism
- File chunks fetched in parallel
- Faster large file reads

## 4. Deduplication
- Identical chunks stored once
- Saves storage + reduces IO

## 5. Streaming I/O
- No full file memory loading
- Efficient for large files (GB–TB scale)

---

# 📊 Scaling Model

- 1GB → single-machine filesystem
- 1TB → caching + chunking required
- 10TB → indexing + compaction needed
- 100TB → distributed system (sharding + replication)

---

# 🧠 Core System Rules

- No in-place writes
- All data is versioned
- All writes are streaming
- All commits are atomic (WAL + CAS)
- Metadata is source of truth
- Object storage is immutable
- Deletion is logical first, physical later

---

# 🚀 Goal

To build a filesystem that is:

- Safer than traditional filesystems
- Naturally versioned like Git
- Crash-resilient by design
- Optimized for large-scale workloads
- Ready for distributed scaling

---

# 📌 Summary

RedirectFS replaces traditional file storage with a:

> versioned, immutable, transaction-safe object graph instead of a mutable directory tree
