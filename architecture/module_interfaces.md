# MODULE INTERFACES

## STRUCTURE
- FUSE API
- Path Resolver API
- Metadata API
- Read API
- Write API
- Chunk API
- Object Store API
- WAL API
- Cache API
- GC API
- Recovery API

## FLOW
- FUSE open/read/write/unlink -> Path Resolver
- Path Resolver resolve(path) -> object_id
- Read API get_range(object_id, offset, size)
- Write API begin_txn -> append -> finalize
- Chunk API chunk(bytes) -> chunk_id
- Object Store write_chunk/read_chunk/has_chunk
- WAL begin/append/commit/abort
- Metadata commit(expected_version, redirect)
- Recovery replay_wal -> fix_txns -> cleanup_staging

## RULES
- Interface errors are explicit.
- No silent fallback that changes correctness.
- Write commit path must call WAL before metadata commit.
- Metadata commit must enforce CAS.
- Recovery APIs run before service enable.

## FAILURES
- Invalid path input -> EINVAL.
- Missing object -> ENOENT.
- Object store IO failure -> retry then EIO.
- WAL fsync failure -> transaction invalid.
- CAS mismatch -> return conflict.

## INVARIANTS
- Each interface has deterministic output class.
- Transaction status is singular: committed or aborted.
- Recovery API is idempotent.
