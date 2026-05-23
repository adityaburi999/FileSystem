# GARBAGE COLLECTION (SAFE SWEEP)

## TWO PHASE MODEL
PHASE 1: MARK LIVE
- Scan active metadata versions
- Build live chunk reference set
- Add in-flight WAL txn references

PHASE 2: SWEEP CANDIDATES
- Scan object store chunks
- chunk not in live set -> orphan candidate
- apply safety delay window
- delete confirmed orphan chunks

## VERSION PRUNING
- Keep last N versions OR time window
- Mark older versions for prune
- Recompute references before physical chunk delete

## TOMBSTONE HANDLING
- Tombstoned metadata older than retention
- verify no active refs
- delete metadata + orphan chunks

## WHAT CAN GO WRONG?
1) False orphan detection due to stale snapshot
   -> recheck before delete
   -> if uncertain, skip

2) Chunk still referenced by open txn
   -> detect from WAL open txns
   -> defer deletion

3) Crash during sweep
   -> idempotent rerun
   -> already deleted items skipped

4) Massive orphan burst
   -> throttle deletes
   -> avoid IO starvation

## GC RULES
- Conservative by default
- Never delete on first doubt
- Metadata + WAL together decide liveness
- Audit every delete action
