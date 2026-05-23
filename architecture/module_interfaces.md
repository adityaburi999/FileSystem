# Module Interfaces — RedirectFS

This document defines the API contracts (Rust traits and key function boundaries) between every module in RedirectFS. All cross-module communication must go through these interfaces; direct field access across module boundaries is forbidden.

---

## 1. FUSE Layer → Path Resolver

```rust
/// Implemented by path-resolver.
pub trait PathResolver: Send + Sync {
    /// Resolve an absolute virtual path to a stable object_id.
    /// Returns None if the path does not exist.
    fn resolve(&self, path: &VirtualPath) -> Result<Option<ObjectId>, ResolveError>;

    /// Resolve a path and also return the parent directory object_id.
    fn resolve_with_parent(
        &self,
        path: &VirtualPath,
    ) -> Result<Option<(ObjectId, ObjectId)>, ResolveError>;
}
```

**Types:**
- `VirtualPath` — validated absolute path string wrapper.
- `ObjectId` — 32-byte BLAKE3-derived identifier (or UUID for metadata objects).
- `ResolveError` — covers `NotFound`, `PermissionDenied`, `CorruptIndex`.

---

## 2. Path Resolver → Metadata Engine

```rust
/// Implemented by metadata-engine.
pub trait MetadataStore: Send + Sync {
    // --- File objects ---

    /// Load the current (latest) version of a file redirect object.
    fn get_file(&self, id: &ObjectId) -> Result<FileObject, MetaError>;

    /// Load a specific historical version.
    fn get_file_version(&self, id: &ObjectId, version: u64) -> Result<FileObject, MetaError>;

    /// Atomically commit a new file version using CAS.
    /// Fails with `VersionConflict` if `expected_version` does not match stored version.
    fn commit_file(
        &self,
        id: &ObjectId,
        new_object: FileObject,
        expected_version: u64,
    ) -> Result<u64, MetaError>; // returns new version number

    /// Mark a file as tombstoned (logical delete).
    fn tombstone_file(&self, id: &ObjectId, expected_version: u64) -> Result<(), MetaError>;

    // --- Directory objects ---

    /// Load a directory object by id.
    fn get_dir(&self, id: &ObjectId) -> Result<DirObject, MetaError>;

    /// Atomically add a child entry to a directory.
    fn add_dir_entry(
        &self,
        dir_id: &ObjectId,
        name: &str,
        child_id: &ObjectId,
        expected_version: u64,
    ) -> Result<u64, MetaError>;

    /// Remove a child entry from a directory.
    fn remove_dir_entry(
        &self,
        dir_id: &ObjectId,
        name: &str,
        expected_version: u64,
    ) -> Result<u64, MetaError>;
}
```

**Types:**
- `FileObject` — see `metadata_design.md` for full struct definition.
- `DirObject` — directory metadata node.
- `MetaError` — covers `NotFound`, `VersionConflict`, `IoError`, `Corrupt`.

---

## 3. Write Engine → WAL Engine

```rust
/// Implemented by wal-engine.
pub trait WalWriter: Send + Sync {
    /// Begin a new transaction; returns an opaque transaction handle.
    fn begin_txn(&self) -> Result<TxnHandle, WalError>;

    /// Append a log entry to an open transaction.
    fn append(&self, txn: &TxnHandle, entry: WalEntry) -> Result<(), WalError>;

    /// Durably flush and mark transaction as committed.
    fn commit(&self, txn: TxnHandle) -> Result<(), WalError>;

    /// Abort a transaction (mark as rolled-back in the log).
    fn abort(&self, txn: TxnHandle) -> Result<(), WalError>;
}

/// Implemented by wal-engine; used during crash recovery.
pub trait WalReader: Send + Sync {
    /// Iterate over all log entries since the last checkpoint.
    fn replay(&self) -> Result<impl Iterator<Item = WalEntry>, WalError>;

    /// Write a checkpoint marker, allowing earlier segments to be deleted.
    fn checkpoint(&self) -> Result<(), WalError>;
}
```

**`WalEntry` variants (enum):**
```rust
pub enum WalEntry {
    ChunkWritten  { chunk_id: ChunkId, txn_id: TxnId },
    TxnBegin      { txn_id: TxnId, timestamp: u64 },
    TxnCommit     { txn_id: TxnId },
    TxnAbort      { txn_id: TxnId },
    MetaCommit    { object_id: ObjectId, version: u64 },
    Tombstone     { object_id: ObjectId },
}
```

---

## 4. Write Engine → Chunk Engine

```rust
/// Implemented by chunk-engine.
pub trait Chunker: Send + Sync {
    /// Feed a slice of raw bytes from the FUSE write buffer.
    /// Calls `on_chunk` each time a full chunk is ready.
    fn feed(
        &mut self,
        data: &[u8],
        on_chunk: impl FnMut(Chunk) -> Result<(), ChunkError>,
    ) -> Result<(), ChunkError>;

    /// Flush the internal buffer, emitting a final (possibly smaller) chunk.
    fn flush(
        &mut self,
        on_chunk: impl FnMut(Chunk) -> Result<(), ChunkError>,
    ) -> Result<(), ChunkError>;
}
```

**`Chunk` struct:**
```rust
pub struct Chunk {
    pub id: ChunkId,       // BLAKE3 hash of content
    pub data: Bytes,       // raw chunk bytes
    pub size: usize,
    pub index: u32,        // position in file (0-based chunk index)
}
```

---

## 5. Chunk Engine → Object Store

```rust
/// Implemented by object-store.
pub trait ObjectStore: Send + Sync {
    /// Persist a chunk. No-op if chunk_id already exists (dedup).
    fn write_chunk(&self, chunk: &Chunk) -> Result<(), StoreError>;

    /// Read a chunk by its id; returns raw bytes.
    fn read_chunk(&self, id: &ChunkId) -> Result<Bytes, StoreError>;

    /// Check whether a chunk is already stored (for dedup fast-path).
    fn has_chunk(&self, id: &ChunkId) -> Result<bool, StoreError>;

    /// Delete a chunk from disk (called only by GC).
    fn delete_chunk(&self, id: &ChunkId) -> Result<(), StoreError>;
}
```

---

## 6. Read Engine → Cache Engine

```rust
/// Implemented by cache-engine.
pub trait CacheLayer: Send + Sync {
    /// Try to retrieve a chunk from cache.
    fn get_chunk(&self, id: &ChunkId) -> Option<Bytes>;

    /// Insert a chunk into cache.
    fn put_chunk(&self, id: &ChunkId, data: Bytes);

    /// Invalidate a cached chunk (called after GC deletes it).
    fn invalidate_chunk(&self, id: &ChunkId);

    /// Try to retrieve a cached metadata object.
    fn get_meta(&self, id: &ObjectId) -> Option<CachedMeta>;

    /// Insert or refresh a metadata entry in cache.
    fn put_meta(&self, id: &ObjectId, meta: CachedMeta);

    /// Invalidate a metadata cache entry (after CAS commit).
    fn invalidate_meta(&self, id: &ObjectId);
}
```

---

## 7. Write Engine → Staging

```rust
/// Implemented by staging.
pub trait StagingArea: Send + Sync {
    /// Open a new staging slot for an in-progress write transaction.
    fn open(&self, txn_id: &TxnId) -> Result<StagingHandle, StagingError>;

    /// Append raw bytes to the staging buffer.
    fn write(&self, handle: &StagingHandle, data: &[u8]) -> Result<(), StagingError>;

    /// Record a chunk that has been fully persisted to the object store.
    fn record_chunk(&self, handle: &StagingHandle, chunk_id: ChunkId) -> Result<(), StagingError>;

    /// Promote: mark staging as committed (safe to clean up after meta commit).
    fn commit(&self, handle: StagingHandle) -> Result<(), StagingError>;

    /// Discard: called on abort or crash recovery cleanup.
    fn discard(&self, handle: StagingHandle) -> Result<(), StagingError>;

    /// List all open (incomplete) staging slots — used during crash recovery.
    fn list_open(&self) -> Result<Vec<TxnId>, StagingError>;
}
```

---

## 8. GC Engine → Metadata Engine + Object Store

```rust
/// GC reads metadata to detect orphans; it does not write metadata.
pub trait GcMetadataReader: Send + Sync {
    /// Iterate over all active redirect objects.
    fn iter_active_files(&self) -> Result<impl Iterator<Item = FileObject>, MetaError>;

    /// Iterate over all tombstoned objects older than a given timestamp.
    fn iter_tombstones(&self, older_than: u64) -> Result<impl Iterator<Item = ObjectId>, MetaError>;

    /// Fetch all chunk_ids referenced by a file object (all versions).
    fn referenced_chunks(&self, id: &ObjectId) -> Result<Vec<ChunkId>, MetaError>;
}
```

GC uses `ObjectStore::delete_chunk` (see §5) for physical deletion.

---

## 9. Crash Recovery → WAL Engine + Staging + Metadata Engine

The `system-core` orchestrator calls these on every boot:

```rust
pub trait RecoveryOrchestrator {
    /// Replay WAL and return a summary of incomplete transactions.
    fn replay_wal(&self) -> Result<RecoverySummary, RecoveryError>;

    /// For each incomplete txn: verify chunks exist, roll back if not.
    fn repair_incomplete_txns(
        &self,
        summary: &RecoverySummary,
        meta: &dyn MetadataStore,
        store: &dyn ObjectStore,
    ) -> Result<(), RecoveryError>;

    /// Discard all open staging slots left over from before the crash.
    fn cleanup_staging(&self, staging: &dyn StagingArea) -> Result<(), RecoveryError>;
}
```

---

## 10. Index Engine → Path Resolver

```rust
/// Implemented by index-engine.
pub trait PathIndex: Send + Sync {
    /// Fast lookup: path string → object_id.
    fn lookup(&self, path: &VirtualPath) -> Result<Option<ObjectId>, IndexError>;

    /// Insert a new path → object_id mapping.
    fn insert(&self, path: &VirtualPath, id: &ObjectId) -> Result<(), IndexError>;

    /// Update mapping when a file is renamed or moved.
    fn rename(&self, old_path: &VirtualPath, new_path: &VirtualPath) -> Result<(), IndexError>;

    /// Remove a path from the index (on delete/tombstone).
    fn remove(&self, path: &VirtualPath) -> Result<(), IndexError>;
}
```

---

## Error Type Convention

All error types implement `std::error::Error + Send + Sync`. Each module defines its own error enum. Cross-module results are wrapped at the call site using `?` and a module-level `From` impl or `thiserror` derive.

```rust
// Example from metadata-engine
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("object not found: {0}")]
    NotFound(ObjectId),
    #[error("version conflict: expected {expected}, found {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt metadata: {0}")]
    Corrupt(String),
}
```
