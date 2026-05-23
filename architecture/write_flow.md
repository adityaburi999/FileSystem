# WRITE FLOW (STREAMING)

## FLOW
write("/a/b/file", data)
  ↓
FUSE receives blocks (4KB-128KB)
  ↓
Open staging txn slot
  ↓
Append to stream buffer
  ↓ buffer >= chunk_size(4MB)
Chunk data + compute BLAKE3
  ↓
Write chunk to object store
  ↓
Append chunk event to WAL
  ↓ repeat until close/fsync
Finalize last chunk
  ↓
Build new redirect metadata version
  ↓
CAS commit metadata
  ↓
WAL txn commit
  ↓
Invalidate/update cache
  ↓
Cleanup staging

## DEDUP PATH
Before chunk write:
- if chunk_id exists -> skip physical write
- still record reference in txn

## WHAT CAN GO WRONG?
1) Crash after chunk write before commit
   -> chunk may exist orphaned
   -> GC cleans later

2) WAL append/fsync fail
   -> do not commit metadata
   -> abort txn

3) CAS conflict (parallel writer)
   -> version mismatch
   -> retry or return EBUSY

4) Staging cleanup fails
   -> recovered and removed on boot

5) Object store full
   -> ENOSPC
   -> txn abort, no partial visibility

## WRITE SAFETY RULES
- No metadata commit without WAL durability
- No user-visible update before CAS success
- Last committed version remains visible on failure
