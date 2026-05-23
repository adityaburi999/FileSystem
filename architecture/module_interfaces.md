# MODULE INTERFACES (CONTRACT FRAGMENTS)

Format: Caller -> Callee : Contract

## 1) FUSE -> Path Resolver
Input: absolute path
Output: object_id or not_found
Failure:
- invalid path -> EINVAL
- missing path -> ENOENT

## 2) Path Resolver -> Metadata Engine
resolve(path)
get_file(object_id)
get_dir(object_id)
Failure:
- stale index -> fallback graph walk
- corrupt metadata -> recovery flag

## 3) Write Engine -> Chunk Engine
feed(bytes)
flush_final_chunk()
Output: ordered chunk stream
Failure:
- chunk overflow -> split and continue
- hash failure -> abort txn

## 4) Chunk Engine -> Object Store
write_chunk(chunk_id, data)
read_chunk(chunk_id)
has_chunk(chunk_id)
Failure:
- write IO fail -> retry/backoff
- duplicate chunk -> treat as dedup hit

## 5) Write Engine -> WAL Engine
begin_txn()
append_event(...)
commit_txn()
abort_txn()
Failure:
- wal append fail -> stop commit
- fsync fail -> txn invalid

## 6) Write Engine -> Metadata Engine (CAS)
commit_file(expected_version, new_redirect)
Failure:
- version mismatch -> conflict retry
- commit partial -> rollback by WAL

## 7) Read Engine -> Cache Engine
get_chunk()
put_chunk()
invalidate_*()
Failure:
- stale cache -> invalidate + reload
- memory pressure -> eviction policy

## 8) GC -> Metadata + Object Store
scan_live_refs()
find_orphans()
delete_safe_orphans()
Failure:
- uncertain ref state -> skip delete
- active txn reference -> defer

## 9) Recovery -> WAL + Staging + Metadata
replay_wal()
fix_incomplete_txn()
cleanup_staging()
Failure:
- replay corruption -> fail closed
- unresolved txn -> keep older version

## INTERFACE RULES
- All writes are transactional
- All metadata changes are CAS based
- All errors are explicit (no silent ignore)
- If uncertain, preserve data and defer cleanup
