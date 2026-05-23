# Crash Recovery — RedirectFS

This document describes what happens when RedirectFS restarts after a crash, power loss, or unclean shutdown. The goal is to restore a fully consistent filesystem state without data loss or corruption exposure.

---

## Recovery Guarantee

> After any crash, RedirectFS will return to the **last fully committed state**. No partial writes are ever exposed. Any in-flight operations are either completed or cleanly rolled back.

---

## What Can Go Wrong (Crash Scenarios)

| Crash Point | Effect |
|---|---|
| During chunk write to object store | Partial or complete chunk on disk; not in WAL |
| After chunk write, before WAL append | Chunk exists on disk; WAL has no record of it |
| After WAL append, before CAS meta commit | Chunks logged; metadata not updated |
| During CAS meta commit | Metadata may be partially written |
| During GC physical delete | Some chunks deleted, some not |
| During staging cleanup | Staging directory partially deleted |

---

## Recovery Architecture

Recovery is performed by `system-core` on every boot, before activating the FUSE mount. It orchestrates:

1. `wal-engine` (WAL replay)
2. `metadata-engine` (metadata validation and repair)
3. `staging` (cleanup of incomplete write buffers)
4. `gc-engine` (post-recovery orphan scan)

---

## Step-by-Step Recovery Process

### Step 1 — Read WAL Segments

`wal-engine` loads all WAL segment files from `/wal/`:

```
segment_0001.log
segment_0002.log
...
segment_NNNN.log
```

Segments are read from the last known **checkpoint** forward. Entries before the checkpoint were already committed and do not need replay.

---

### Step 2 — Reconstruct Transaction Map

The WAL is replayed to build a map of all transactions:

```rust
struct TxnRecord {
    txn_id:     TxnId,
    state:      TxnState,   // Open | Committed | Aborted
    chunks:     Vec<ChunkId>,
    meta_commit: Option<(ObjectId, u64)>,
}
```

Transaction states determined by WAL entries:

| Entries Found | Final State |
|---|---|
| `TxnBegin` only | **Incomplete** (crash during write) |
| `TxnBegin` + `ChunkWritten` entries | **Incomplete** (crash before commit) |
| `TxnBegin` + `ChunkWritten` + `MetaCommit` + `TxnCommit` | **Committed** (fully complete) |
| `TxnBegin` + `TxnAbort` | **Aborted** (intentionally rolled back) |

---

### Step 3 — Validate Committed Transactions

For each **Committed** transaction:

1. Load the corresponding `FileObject` from `/metadata/`.
2. Verify each `chunk_id` in the chunk list exists in `/objects/`.
3. Verify each chunk's BLAKE3 hash matches its filename.
4. If all checks pass → transaction is confirmed valid.
5. If a chunk is missing or corrupt:
   - Mark the `FileObject` as `Corrupt`.
   - Log a corruption event.
   - Do not mount the file as accessible (return `EIO`).

---

### Step 4 — Repair Incomplete Transactions

For each **Incomplete** transaction:

```
Option A — Repair (if all chunks are present and valid):
    1. All ChunkWritten entries have valid chunks in object store.
    2. Reconstruct the FileObject from WAL chunk list.
    3. Attempt CAS commit of the new FileObject version.
    4. Append MetaCommit + TxnCommit to WAL.
    → Transaction is completed post-crash.

Option B — Roll Back (if any chunk is missing or WAL is incomplete):
    1. Mark the transaction as Aborted in WAL.
    2. Leave chunks on disk (GC will clean them up).
    3. Do not update metadata.
    → File remains at its last committed version (or does not exist if new file).
```

The recovery engine **prefers Option A** when it is safe, to avoid losing work done before the crash. However, it never creates an inconsistent file state.

---

### Step 5 — Validate and Rebuild Metadata

After transaction replay:

1. Scan `/metadata/` for any `FileObject` or `DirObject` that:
   - Has an inconsistent version (higher version than any WAL entry).
   - Is in an unexpected state (e.g., empty chunk list on an Active file).
2. For inconsistent objects:
   - Roll back to the last known-good version found in the WAL.
   - Log the rollback.

The metadata B-tree / SQLite database is checked for structural integrity. If the database is corrupt:
- Attempt recovery using SQLite's built-in WAL journal (separate from RedirectFS WAL).
- If unrecoverable → rebuild metadata index from raw `/metadata/` files (slower but complete).

---

### Step 6 — Staging Cleanup

`staging` lists all open (incomplete) staging slots:

```
staging/
  txn_0042/   ← still open from pre-crash write
  txn_0051/   ← still open
```

For each open staging slot:
1. Check WAL: if `TxnCommit` exists → staging was committed but not cleaned up → safe to delete.
2. If `TxnAbort` or no commit → write was incomplete → discard staging contents.
3. Call `staging.discard(handle)` to remove the directory.

Chunks recorded in `chunks.tmp` that are not referenced by any committed metadata are flagged as orphans for GC.

---

### Step 7 — Post-Recovery GC Scan

After all transactions are resolved, a targeted GC scan is triggered:

1. Collect all chunk_ids from aborted/discarded transactions.
2. Verify none are referenced by committed metadata.
3. Mark them as orphans; add to deletion queue.
4. GC sweep runs immediately post-boot to free these chunks.

This is a fast, targeted scan (only aborted transaction chunks), not a full GC run.

---

### Step 8 — Index Rebuild (if needed)

If the index engine's B-tree or LSM files are detected as corrupt or missing:
- Rebuild the path index by walking the metadata graph from the root directory object.
- This is O(N) in the number of files but is a one-time startup cost.
- Normal operation does not require an index rebuild.

---

### Step 9 — Activate FUSE Mount

Once all recovery steps complete successfully:
- `system-core` activates the FUSE mount.
- The filesystem is now accessible to users.
- A startup event is logged to `/system/state/boot.log`.

If any unrecoverable error occurs during steps 1–8:
- The FUSE mount is **not activated**.
- A detailed error report is written to `/system/state/recovery_failure.log`.
- The administrator must intervene.

---

## Recovery Performance

| Scenario | Expected Recovery Time |
|---|---|
| Clean shutdown (checkpoint up to date) | < 1 second |
| Crash with few incomplete transactions | 1–10 seconds |
| Crash with large staging (GB of partial data) | 10–60 seconds |
| Full metadata index rebuild | Minutes (proportional to file count) |

---

## Key Invariants During Recovery

- No FUSE activity is allowed until recovery completes.
- Recovery never creates new data — it only confirms, rolls back, or repairs existing state.
- Every decision made during recovery is logged to the WAL or audit log.
- Recovery is **deterministic**: running it twice on the same state produces the same result.
