# 🧠 RedirectFS — Next-Generation Virtual Filesystem & Storage Engine

RedirectFS is a **research-grade virtual filesystem + object storage engine** designed to rethink how operating systems store, access, and manage files at scale.

It combines:
- A FUSE-based filesystem interface (Linux-compatible)
- A versioned metadata graph (redirect-based file model)
- An immutable chunked object store
- A write-ahead logging (WAL) transaction system
- A crash-safe staging layer
- A background garbage collection system

---

# 🚀 What Problem This Project Solves

Traditional filesystems (ext4, NTFS, etc.) suffer from:

- Weak crash recovery guarantees in complex writes
- Inefficient handling of massive numbers of small files
- Poor deduplication across files
- Expensive metadata operations at large scale
- Limited versioning and rollback capabilities
- In-place mutation leading to corruption risks

RedirectFS is designed to solve these limitations by fundamentally changing the storage model.

---

# 🧩 Core Idea

Instead of storing files directly on disk:

> Files are represented as **redirect objects** that point to immutable chunk data.

This introduces a fully versioned, atomic, and recoverable storage model.

---

# 🏗️ System Architecture Overview

RedirectFS is composed of multiple independent subsystems:

## 1. FUSE Layer
Interfaces with the Linux kernel to intercept filesystem calls:
- open()
- read()
- write()
- mkdir()
- unlink()

---

## 2. Metadata Engine
Stores:
- File redirect objects (versioned pointers to chunks)
- Directory objects (graph-based hierarchy)

Provides:
- Path resolution
- Version tracking
- Atomic metadata updates (CAS-based)

---

## 3. Chunk Engine
- Splits file data into fixed-size streaming chunks
- Uses BLAKE3 hashing for content addressing
- Enables deduplication across the system

---

## 4. Object Store
- Stores immutable chunks on disk
- Content-addressed storage layout
- Never overwrites existing data

---

## 5. Write-Ahead Log (WAL)
- Records every operation before execution
- Enables crash recovery and transaction replay
- Ensures system consistency after failure

---

## 6. Write Engine (Streaming System)
- Handles FUSE write streams
- Performs real-time chunking
- Commits data incrementally

---

## 7. Staging Layer
- Temporary storage for incomplete writes
- Fully isolated from live filesystem
- Automatically discarded on crash or commit

---

## 8. Garbage Collection Engine
- Removes unreferenced chunks
- Prunes old file versions
- Applies retention policies
- Ensures long-term storage efficiency

---

## 9. Cache Engine
- Multi-tier caching (RAM / SSD)
- Accelerates read performance
- Stores hot chunks and reconstructed files

---

## 10. Index Engine
- Accelerates path resolution
- Maps filesystem paths to object IDs
- Optimized for large-scale datasets (10TB–100TB+)

---

# ⚡ Key Design Principles

RedirectFS is built on strict system guarantees:

- 📌 No in-place file modification
- 📌 All writes are streamed and chunked
- 📌 All data is versioned
- 📌 All commits are atomic (CAS + WAL)
- 📌 Metadata is the single source of truth
- 📌 Object storage is immutable
- 📌 Deletion is logical first, physical later

---

# 🔁 File Lifecycle Model

## Write
staging → chunking → WAL logging → object store → metadata commit → atomic switch

## Read
path → metadata graph → redirect object → chunk fetch → reconstruction → cache

## Delete
logical tombstone → WAL log → GC later removes data safely

---

# 💽 Disk Model

- `/wal` → transaction logs
- `/metadata` → file & directory objects
- `/objects` → immutable chunks
- `/cache` → performance layer
- `/staging` → temporary writes
- `/system` → internal engine state (hidden)

---

# 🧠 Why This Project Is Interesting

RedirectFS explores ideas from:
- Modern distributed storage systems (S3-like design)
- Git-style versioning
- Database WAL + recovery models
- Content-addressed storage systems
- Filesystem-level virtualization

It is designed as a **research + systems engineering project**, not just a basic filesystem.

---

# ⚠️ Current Status

This project is currently in:
> Architecture & design phase

No production implementation yet.

Next steps involve:
- Rust module implementation
- WAL engine coding
- Object store development
- FUSE integration
- Full end-to-end prototype

---

# 🧭 Goal

To build a filesystem that is:

- Safer than traditional filesystems
- More scalable at large datasets
- Naturally versioned
- Crash-resilient by design
- Efficient for modern AI/data workloads

---

# 📌 Summary

RedirectFS reimagines storage as:

> A versioned, immutable, transaction-safe object graph instead of a mutable file tree.

---
