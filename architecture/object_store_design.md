# Object Store Design — RedirectFS

This document describes the design of the immutable chunk-based object store: how chunks are formatted, hashed, laid out on disk, and how deduplication works.

---

## Design Principles

1. **Immutability** — once written, a chunk is never modified or overwritten.
2. **Content-addressing** — chunk identity is derived from content (BLAKE3 hash), not location.
3. **Deduplication** — identical content is stored exactly once, regardless of which file it belongs to.
4. **Crash safety** — chunks are written atomically (temp file + rename).
5. **Append-only growth** — the object store only grows; GC is the only mechanism that removes data.

---

## Chunk Format

A chunk is the atomic unit of storage. Every chunk consists of:

```
┌─────────────────────────────────────────────────────┐
│                  CHUNK FILE FORMAT                   │
├──────────┬──────────────────────────────────────────┤
│ Header   │ 64 bytes                                  │
│          │  magic:      [u8; 4]  = 0x52 0x44 0x43 0x4B ("RDCK") │
│          │  version:    u8       = 1                 │
│          │  flags:      u8       = 0 (reserved)      │
│          │  chunk_size: u32      = actual data bytes │
│          │  content_hash: [u8; 32] = BLAKE3 of data  │
│          │  header_crc: u32      = CRC32 of header bytes [0..59] │
├──────────┼──────────────────────────────────────────┤
│ Data     │ <chunk_size> bytes (raw content)          │
├──────────┼──────────────────────────────────────────┤
│ Trailer  │ 4 bytes                                   │
│          │  data_crc: u32 = CRC32 of data section    │
└──────────┴──────────────────────────────────────────┘
```

Total file size: `64 + chunk_size + 4` bytes.

---

## BLAKE3 Hashing

Every chunk is identified by the **BLAKE3 hash of its raw data** (the Data section, excluding header and trailer).

Why BLAKE3:
- Faster than SHA-256 on modern CPUs (uses SIMD, parallelism).
- Cryptographically secure (collision-resistant for integrity purposes).
- Produces 32-byte (256-bit) output.
- Streaming-friendly: can hash data as it arrives without buffering the full chunk.

Hash computation:

```rust
use blake3::Hasher;

fn hash_chunk(data: &[u8]) -> ChunkId {
    let hash = blake3::hash(data);
    ChunkId(*hash.as_bytes())
}
```

The resulting `ChunkId` is both the **filename** and the **integrity key** for the chunk.

---

## Chunk Size

| Scenario | Chunk Size |
|---|---|
| Default | 4 MB |
| Small files (< inline threshold) | Stored inline in metadata (no chunk) |
| Last chunk of any file | Variable (0 to 4 MB — whatever remains) |

The 4 MB chunk size balances:
- **Dedup granularity** — smaller chunks = more dedup opportunities; larger = less overhead.
- **Per-chunk overhead** — each chunk requires a WAL entry, a hash, and a file on disk.
- **Parallel fetch efficiency** — larger chunks mean fewer concurrent fetches needed.

The chunk size is configurable per filesystem instance.

---

## Disk Layout

Chunks are stored under `/objects/` using a **2-level prefix shard** based on the chunk's BLAKE3 hash:

```
/objects/
  <first_byte_hex>/         ← 00 to ff (256 top-level dirs)
    <second_byte_hex>/      ← 00 to ff (256 dirs per top-level = 65,536 total)
      <full_hash_hex>.chunk
```

Example for a chunk with hash `ab cd 3f a9 c1 b2 ...`:
```
/objects/ab/cd/abcd3fa9c1b2...chunk
```

### Why 2-Level Sharding?

A filesystem with millions of chunks in a single directory becomes slow (directory entry lookup degrades). With 2-level sharding:
- Up to 65,536 directories at the second level.
- Each directory holds at most a few thousand chunks before re-sharding is needed.
- At 100 TB with 4 MB chunks: ~25 million chunks → ~382 chunks per leaf directory on average.

---

## Chunk Write Process (Atomic)

```
1. Generate temp filename:
   /objects/<prefix>/<sub>/<full_hash>.tmp

2. Write header (64 bytes)
3. Stream data bytes (up to 4 MB)
4. Write trailer (4 bytes, CRC32 of data)
5. fsync the temp file
6. rename(<hash>.tmp, <hash>.chunk)  ← atomic on Linux
```

The `rename` syscall is atomic on POSIX systems. After rename, the chunk is either fully present or not present — never partially written.

If the system crashes after step 5 but before step 6, the `.tmp` file is an orphan. GC detects `.tmp` files during boot cleanup and removes them.

---

## Chunk Read Process

```
1. Build path: /objects/<hash[0..1]>/<hash[2..3]>/<full_hash>.chunk
2. open() the chunk file
3. Read header (64 bytes), verify magic + header_crc
4. Read data (header.chunk_size bytes)
5. Read trailer (4 bytes), verify data_crc
6. Compute BLAKE3 of data → compare to header.content_hash and filename
7. If any check fails → return integrity error, log corruption event
8. Return data bytes
```

Three integrity layers:
- Header CRC (detects header corruption).
- Trailer CRC (detects data corruption at write time or storage layer).
- BLAKE3 re-hash (cryptographic verification — detects tampering and silent corruption).

---

## Deduplication

Content-addressed storage provides deduplication automatically:

```rust
fn write_chunk_with_dedup(store: &ObjectStore, chunk: &Chunk) -> Result<ChunkId> {
    if store.has_chunk(&chunk.id)? {
        // Already exists — no write needed
        return Ok(chunk.id);
    }
    store.write_chunk(chunk)?;
    Ok(chunk.id)
}
```

`has_chunk` checks if the path `/objects/<prefix>/<sub>/<hash>.chunk` exists. This is a single `stat` syscall — extremely fast.

### Dedup Scope

- **Cross-file dedup** — two different files sharing the same 4 MB block store only one copy.
- **Cross-version dedup** — a new version of a file that only changes some chunks reuses unchanged chunks.
- **Cross-user dedup** — all users share the same object store; identical content is stored once.

### Dedup Limitations

- Dedup operates at **chunk granularity** (4 MB). Sub-chunk similarity is not detected.
- Two files with the same content but different chunking offsets (e.g., one byte prepended) will not dedup — this is a known limitation of fixed-size chunking.
- Future: variable-length chunking (content-defined chunking / CDC) can improve cross-offset dedup.

---

## Object Store API

```rust
pub trait ObjectStore: Send + Sync {
    fn write_chunk(&self, chunk: &Chunk) -> Result<(), StoreError>;
    fn read_chunk(&self, id: &ChunkId) -> Result<Bytes, StoreError>;
    fn has_chunk(&self, id: &ChunkId) -> Result<bool, StoreError>;
    fn delete_chunk(&self, id: &ChunkId) -> Result<(), StoreError>;   // GC only
    fn list_all_chunks(&self) -> Result<impl Iterator<Item=ChunkId>, StoreError>; // GC scan
}
```

---

## Storage Space Estimation

| Files | Avg File Size | Dedup Ratio | Chunks | Disk Usage (est.) |
|---|---|---|---|---|
| 10,000 | 1 MB | 1x | ~2,500 | ~10 GB |
| 100,000 | 10 MB | 2x | ~125,000 | ~500 GB |
| 1,000,000 | 50 MB | 3x | ~4.2 million | ~16 TB |

---

## Future Improvements

- **Compression** — compress chunk data before writing (LZ4 / Zstd). The header `flags` field reserves a bit for compression type.
- **Erasure coding** — for distributed mode, chunks can be split into k+m erasure-coded shards.
- **Variable-length chunking** — content-defined chunking (e.g., FastCDC) for better cross-offset dedup.
- **Tiered storage** — automatically migrate cold chunks from SSD to HDD or object storage (S3-compatible).
