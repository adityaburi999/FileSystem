# READ FLOW (PATH -> METADATA -> CHUNKS -> OUTPUT)

## FLOW
open/read("/a/b/file")
  ↓
FUSE receives request
  ↓
Resolve path to object_id
  ↓
Load latest File Redirect metadata
  ↓
Map byte-range to chunk indexes
  ↓
Check cache (RAM -> SSD)
  ↓ miss
Fetch missing chunks from object store (parallel)
  ↓
Verify chunk hash (BLAKE3)
  ↓
Reconstruct output bytes in order
  ↓
Return data to application

## RANGE LOGIC
offset + size -> start_chunk, end_chunk
Read only required chunks
Trim first/last chunk by offset boundaries

## FAST PATH
- Path index hit
- Metadata cache hit
- Chunk cache hit
=> sub-ms read

## SLOW PATH
- Index miss -> graph traversal
- Cache miss -> disk object fetch
- Large file -> multi-chunk parallel fetch

## WHAT CAN GO WRONG?
1) Path not found
   -> ENOENT

2) Metadata points to missing chunk
   -> EIO
   -> mark corruption

3) Hash mismatch on fetched chunk
   -> deny read of bad data
   -> log integrity incident

4) Cache has stale metadata
   -> invalidate + reload metadata

5) Partial object-store outage
   -> retry with backoff
   -> fail request if quorum/availability not met

## READ SAFETY RULES
- Serve only committed versions
- Never serve chunk without hash verification
- On uncertainty, fail read instead of serving corrupted bytes
