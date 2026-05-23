# System Contracts — RedirectFS

This document defines the global invariants and rules that every module in RedirectFS must obey at all times. Violations of these contracts indicate bugs, not edge cases.

---

## What Is a System Contract?

A system contract is a **property that must always be true** regardless of:
- What operations are in progress.
- Whether the system just recovered from a crash.
- How many concurrent writers there are.
- What GC, indexing, or caching is doing in the background.

Any module that violates a contract is incorrect, and the violation should be treated as a critical bug.

---

## Contract 1 — No In-Place Modification

> **An existing object (chunk or metadata version) is NEVER modified after it has been committed.**

- The object store is append-only. `write_chunk` is a no-op if the chunk already exists.
- A committed `FileObject.vN.meta` file is never overwritten; new state creates `.v(N+1).meta`.
- A `DirObject` entry map is never mutated directly; mutations create a new version via CAS.

**Implication:** Any code that opens an existing chunk file or metadata file with write access is violating this contract.

---

## Contract 2 — CAS Is the Only Write Path for Metadata

> **Every metadata write MUST go through a CAS operation. Direct metadata file writes without CAS are forbidden.**

- `commit_file`, `tombstone_file`, `add_dir_entry`, `remove_dir_entry` — all require `expected_version`.
- No module may write a `FileObject` or `DirObject` to disk without first reading the current version and confirming the expected version matches.

**Implication:** Concurrent writers cannot silently overwrite each other. Every conflict is detected and results in a `VersionConflict` error.

---

## Contract 3 — WAL Before State

> **Every operation that changes persistent state MUST be logged in the WAL BEFORE the state change takes effect.**

Specifically:
- `ChunkWritten` entry must be appended and fsynced **before** the WAL transaction is considered durable.
- `MetaCommit` must be appended after CAS succeeds, before the function returns success to the caller.
- `TxnCommit` must be the last entry before a transaction is considered committed.

**Implication:** On crash recovery, any state not reflected in a `TxnCommit` WAL entry is treated as incomplete and rolled back.

---

## Contract 4 — No Partial State Visible to Users

> **Users (via FUSE) MUST never observe a file or directory in a partial or intermediate state.**

- In-progress writes are confined to the staging area, which is not accessible via FUSE.
- A file's `FileObject` transitions atomically from version N to version N+1; there is no "version 1.5".
- A directory entry either exists or does not; it is never in a partially-added state.

**Implication:** Staging must be used for all writes. No write engine code may update live metadata before the full chunk list and CAS commit are complete.

---

## Contract 5 — Chunk IDs Are Immutable Commitments

> **A ChunkId is permanently and exclusively bound to the content it was derived from. No two different contents may share a ChunkId, and no ChunkId may be reused for different content.**

- BLAKE3 collision resistance guarantees this computationally.
- The system must never manually assign a `ChunkId` to a chunk; it must always be computed from the chunk's content.
- Dedup is only safe because this contract holds: `has_chunk(id) == true` means the correct content is stored.

**Implication:** Any code that generates or assigns a `ChunkId` without computing `BLAKE3(content)` is violating this contract.

---

## Contract 6 — GC Must Not Delete Live Data

> **The GC engine must NEVER physically delete a chunk that is referenced by any active (non-tombstoned) FileObject version or any open WAL transaction.**

- Phase 1 (Mark) builds a complete live set before Phase 2 (Sweep) deletes anything.
- The GC safety window (minimum: `GC_SAFETY_WINDOW_SECS`) ensures chunks from recently committed transactions are not immediately eligible for deletion.
- GC must check the WAL for in-flight transaction references before deleting any candidate.

**Implication:** GC incorrectly deleting a live chunk would cause reads to fail with `EIO`. This is a data loss scenario and a contract violation.

---

## Contract 7 — Metadata Is the Source of Truth

> **The object store, index, and cache are all derived from metadata. Metadata is authoritative.**

- If the index says a path maps to `ObjectId A`, but metadata says `ObjectId A` does not exist → the index is wrong; return `ENOENT`.
- If the cache has a stale `FileObject`, but metadata has a newer version → the cache must be invalidated and the newer version used.
- If the object store has a chunk but no `FileObject` references it → the chunk is an orphan eligible for GC. The chunk's existence does not make it "live".

**Implication:** Any code that bypasses metadata to serve data from the cache or object store is violating this contract.

---

## Contract 8 — Version Numbers Are Monotonically Increasing

> **A FileObject or DirObject's version number must NEVER decrease.**

- Each CAS commit increments the version by exactly 1.
- Recovery may repair incomplete transactions, but it must never revert a committed version to a lower number.
- Version numbers are `u64`; overflow is not a practical concern (would take billions of writes to a single file).

**Implication:** Any code that assigns a lower version number to a committed object is violating this contract and may cause concurrent writers to incorrectly pass CAS validation.

---

## Contract 9 — Tombstoned Objects Are Dead

> **A FileObject or DirObject with `state == Tombstone` must NEVER be returned by any read, listing, or path resolution operation.**

- `path-resolver` must check `state` after loading a `FileObject`.
- `metadata-engine` must not return tombstoned objects in any active-file iteration.
- The cache must not serve tombstoned objects as live data.
- GC may read tombstoned objects (to find their chunk lists for cleanup) — this is the only permitted use.

**Implication:** Returning a tombstoned object to a user would be a use-after-delete bug. Files that appear "resurrected" after deletion indicate a violation of this contract.

---

## Contract 10 — Staging Is Invisible

> **No FUSE operation may return data from, or expose the existence of, the `/staging/` directory or any staging slot.**

- `readdir` on the root must not include `staging`.
- `open`, `stat`, or `read` on any path under `/staging/` must return `EACCES` or `ENOENT`.
- Path resolution must never resolve to an object in the staging area.

**Implication:** Any code path that allows a user to read staging data exposes partial, uncommitted writes — a correctness and security violation.

---

## Contract 11 — Crash Recovery Is Complete Before FUSE Activation

> **The FUSE mount must not be activated until crash recovery has fully completed.**

- All incomplete WAL transactions must be resolved (committed or rolled back).
- All open staging slots must be cleaned up.
- The metadata index must be in a consistent state.
- Post-recovery GC scan must complete (or at least be launched).

**Implication:** Activating FUSE before recovery completes could expose users to stale, inconsistent, or partially-written files. This contract ensures the filesystem is always in a known-good state before users can access it.

---

## Contract 12 — Every Chunk Read Must Be Verified

> **Every chunk loaded from the object store must have its BLAKE3 hash verified before its data is used or returned to the caller.**

- A chunk whose BLAKE3 hash does not match its `ChunkId` is corrupt and must not be served.
- Cache hits also require integrity: chunks stored in the SSD cache are verified on load.
- RAM cache hits do not require re-verification (data has not left process memory).

**Implication:** Skipping integrity verification to improve performance trades correctness for speed in a way that violates the core security guarantee of content-addressing.

---

## Contract Summary Table

| # | Contract | Who Must Enforce |
|---|---|---|
| 1 | No in-place modification | object-store, metadata-engine |
| 2 | CAS is the only write path | metadata-engine, all callers |
| 3 | WAL before state | wal-engine, write-engine |
| 4 | No partial state visible | staging, fuse-layer, write-engine |
| 5 | ChunkIds are content-bound | chunk-engine, object-store |
| 6 | GC must not delete live data | gc-engine |
| 7 | Metadata is source of truth | all modules |
| 8 | Versions are monotonic | metadata-engine, crash recovery |
| 9 | Tombstones are dead | metadata-engine, path-resolver, cache |
| 10 | Staging is invisible | fuse-layer, path-resolver |
| 11 | Recovery before FUSE mount | system-core |
| 12 | Verify every chunk read | object-store, cache-engine |
