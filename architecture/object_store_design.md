# OBJECT STORE DESIGN

## STRUCTURE
- Chunk Writer
- Chunk Reader
- Hash Verifier
- Dedup Checker
- Temp File Committer
- Sharded Path Layout

## FLOW
- Receive chunk bytes
- Compute BLAKE3 hash -> chunk_id
- Check existing chunk path
- Absent -> write temp chunk file
- fsync temp file
- Atomic rename to final chunk path
- Read request -> locate chunk path
- Read bytes -> verify hash
- Return data on match

## RULES
- Chunk id equals content hash.
- Existing chunk_id is immutable.
- Finalization uses atomic rename.
- Read path requires integrity verify.

## FAILURES
- Temp write interrupted -> cleanup temp artifacts.
- Hash mismatch on read -> EIO + quarantine chunk.
- Disk IO failure -> retry/backoff then fail.
- Path shard overload -> rebalance shard policy.
- Duplicate finalization race -> retain first valid chunk.

## INVARIANTS
- Same chunk_id implies same bytes.
- No in-place chunk mutation.
- Invalid chunk is never served.
