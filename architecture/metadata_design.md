# Metadata Design — RedirectFS

This document describes the structure of all metadata objects in RedirectFS: file objects, directory objects, the versioned redirect model, and how they are stored on disk.

---

## Design Goals

- **Versioned** — every change to a file or directory creates a new version; old versions are queryable.
- **Immutable at rest** — once a version is committed, it is never modified.
- **Crash-safe** — CAS commits + WAL ensure no partial state is ever visible.
- **Efficiently stored** — binary-encoded (Bincode), sharded by object ID prefix.

---

## Core Concepts

### Object ID

Every metadata object has a globally unique `ObjectId`:

```rust
pub struct ObjectId([u8; 32]);
```

For file and directory objects, the ID is derived at creation time using:
```
ObjectId = BLAKE3(parent_dir_id || name || creation_timestamp || random_nonce)
```

This ensures uniqueness even if the same name is created twice in the same directory.

### Chunk ID

Chunks in the object store are identified by their content hash:

```rust
pub struct ChunkId([u8; 32]);  // BLAKE3 hash of chunk content
```

Chunk IDs are content-derived (not random), enabling deduplication.

---

## FileObject — File Metadata

A `FileObject` represents one version of a file. It is the **redirect object** — it does not store file content, only a pointer to the chunk list.

```rust
pub struct FileObject {
    // Identity
    pub object_id:    ObjectId,       // stable across versions
    pub version:      u64,            // monotonic; incremented on each write

    // State
    pub state:        FileState,      // Active | Tombstone

    // Content
    pub size:         u64,            // total file size in bytes
    pub chunk_ids:    Vec<ChunkId>,   // ordered list; chunk[0] = bytes [0, CHUNK_SIZE)
    pub content_hash: [u8; 32],       // BLAKE3 of full file (all chunks concatenated)

    // Timestamps (Unix epoch, milliseconds)
    pub created_at:   u64,
    pub modified_at:  u64,
    pub tombstone_at: Option<u64>,    // set when state = Tombstone

    // Ownership (future)
    pub owner_uid:    u32,
    pub owner_gid:    u32,
    pub permissions:  u32,            // Unix mode bits

    // Inline storage optimization
    pub inline_data:  Option<Bytes>,  // for files < INLINE_THRESHOLD (e.g., 4 KB)
}

pub enum FileState {
    Active,
    Tombstone,
}
```

### Inline Small Files

Files smaller than the inline threshold (default: 4 KB) are stored **directly inside the `FileObject`** in the `inline_data` field. No chunks are written to the object store for these files. This avoids the overhead of chunk creation and lookup for tiny files.

When `inline_data` is set:
- `chunk_ids` is empty.
- `size` reflects the actual byte count of `inline_data`.
- Inline data is included in the metadata binary and stored under `/metadata/files/`.

---

## DirObject — Directory Metadata

A `DirObject` represents a directory node in the metadata graph.

```rust
pub struct DirObject {
    // Identity
    pub object_id:  ObjectId,
    pub version:    u64,

    // State
    pub state:      DirState,         // Active | Tombstone

    // Entries
    pub entries:    BTreeMap<String, DirEntry>,  // name → child
    pub parent_id:  Option<ObjectId>, // None for root

    // Timestamps
    pub created_at: u64,
    pub modified_at: u64,
}

pub struct DirEntry {
    pub child_id:   ObjectId,         // points to FileObject or DirObject
    pub entry_type: EntryType,        // File | Directory
}

pub enum EntryType {
    File,
    Directory,
}

pub enum DirState {
    Active,
    Tombstone,
}
```

`entries` uses a `BTreeMap` (sorted by name) so directory listings are always alphabetically ordered without additional sorting.

---

## Versioned Redirect Model

Each write to a file does not modify the existing `FileObject` — it creates a new version:

```
file.txt at version 1:  chunk_ids = [A, B, C]
file.txt at version 2:  chunk_ids = [A, B, D]   ← only chunk C changed
file.txt at version 3:  chunk_ids = [A, B, D, E] ← new chunk added
```

Chunks A and B are **shared** between versions 1, 2, and 3. GC reference counting handles this correctly (chunk only freed when reference count drops to zero).

The **current version** is always the highest-numbered committed version for a given `ObjectId`.

---

## Metadata Storage on Disk

Metadata files are stored under `/metadata/` using a two-level shard:

```
/metadata/
  files/
    <prefix_2hex>/
      <prefix_4hex>/
        <object_id_hex>.v<version>.meta
```

Example:
```
/metadata/files/fa/fa82/fa82c1d3...v1.meta
/metadata/files/fa/fa82/fa82c1d3...v2.meta
```

Each `.meta` file contains one `FileObject`, serialized with Bincode.

Directories:
```
/metadata/dirs/
    <prefix_2hex>/
      <prefix_4hex>/
        <object_id_hex>.v<version>.meta
```

### Why Keep Multiple Version Files?

- Historical versions are needed for:
  - GC version pruning (read old version's chunk list before deleting).
  - Future snapshot / rollback features.
  - Audit and forensic access.
- Once GC prunes a version, the `.vN.meta` file is deleted.
- The current version is always the highest version number file for a given `object_id_hex`.

---

## Metadata Database (SQLite Layer)

In addition to raw `.meta` files, a SQLite database (`/system/state/metadata.db`) provides fast queries:

| Table | Contents |
|---|---|
| `file_objects` | Latest version pointer: `object_id → version, state, size` |
| `dir_objects` | Latest version pointer: `object_id → version, state` |
| `object_versions` | Full version history index: `object_id, version, file_path` |
| `path_map` | Path string → object_id lookup cache |

SQLite is opened in **WAL journal mode** for concurrent read safety and crash durability.

The raw `.meta` files are the ground truth. The SQLite DB is a derived index that can be rebuilt from them if needed.

---

## Root Object

The root of the filesystem is a well-known `DirObject` with a fixed, deterministic ID:

```rust
pub const ROOT_OBJECT_ID: ObjectId = ObjectId([0u8; 32]); // or a well-known hash
```

Path resolution always starts from `ROOT_OBJECT_ID` and walks the directory graph.

---

## Metadata Engine Operations

| Operation | CAS Required | Description |
|---|---|---|
| `get_file(id)` | No | Load current FileObject |
| `get_file_version(id, v)` | No | Load specific historical version |
| `commit_file(id, new, expected_v)` | Yes | Write new FileObject version |
| `tombstone_file(id, expected_v)` | Yes | Set state = Tombstone |
| `get_dir(id)` | No | Load DirObject |
| `add_dir_entry(dir_id, name, child_id, v)` | Yes | Add child to directory |
| `remove_dir_entry(dir_id, name, v)` | Yes | Remove child from directory |

---

## Metadata Integrity

Every `.meta` file includes a checksum:

```rust
pub struct MetaEnvelope {
    pub payload:  Vec<u8>,   // Bincode-serialized FileObject or DirObject
    pub checksum: [u8; 4],   // CRC32 of payload
}
```

On load, the checksum is verified. A mismatch indicates a corrupt metadata file → log error, attempt to load the previous version, escalate if all versions are corrupt.
