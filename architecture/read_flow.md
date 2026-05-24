# READ FLOW

## STRUCTURE
- FUSE Read Handler
- Path Resolver
- Metadata Engine
- Cache L1/L2
- Object Store
- Integrity Verifier
- Reconstructor

## FLOW
- read(path, offset, size) -> FUSE
- Resolve path -> object_id
- Load latest committed metadata
- Map range -> chunk indexes
- Query L1 cache
- L1 miss -> query L2 cache
- L2 miss -> fetch chunk from object store
- Verify chunk hash
- Reconstruct ordered byte range
- Return bytes to caller

## RULES
- Serve only committed metadata version.
- Verify hash before serving fetched chunk.
- Respect range boundaries strictly.
- Cache miss cannot alter correctness path.

## FAILURES
- Path not found -> ENOENT.
- Metadata missing/corrupt -> EIO + recovery flag.
- Chunk absent -> EIO + inconsistency mark.
- Hash mismatch -> reject chunk + integrity alert.
- Object store timeout -> bounded retry then fail.

## INVARIANTS
- Returned bytes map to one committed version.
- No unverified chunk is served.
- Read snapshot is stable during request.
