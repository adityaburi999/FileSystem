# CONCURRENCY MODEL (CAS + VERSION)

## CORE MODEL
- Reads: mostly lock-free snapshot reads
- Writes: optimistic concurrency + CAS
- Delete: CAS tombstone update
- GC: background, conservative checks

## WRITE CONFLICT FLOW
Writer A and Writer B read version V
  ↓
A commits V+1 first
  ↓
B CAS fails (expected V, found V+1)
  ↓
B retry with latest metadata or abort

## READ VS WRITE
Read starts on version V
  ↓
Write commits V+1
  ↓
Read continues on V snapshot
(no torn/partial view)

## DIRECTORY CONCURRENCY
- Entry add/remove via parent dir CAS
- Name collision -> EEXIST
- Remove missing -> ENOENT

## WHAT CAN GO WRONG?
1) Retry storm under heavy contention
   -> exponential backoff
   -> cap retries

2) Starvation of one writer
   -> fairness policy / queue hints

3) GC racing with active write
   -> WAL in-flight refs block unsafe delete

4) Cache serving stale metadata during fast updates
   -> version-aware cache invalidation

5) Concurrent rename/delete edge
   -> use ordered txn semantics
   -> resolve by version and path lock scope

## CONCURRENCY RULES
- CAS is mandatory for all metadata mutation
- Version numbers are monotonic
- No silent overwrite ever
- If conflict unresolved, fail explicit and safe
