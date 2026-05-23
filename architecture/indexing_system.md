# INDEXING SYSTEM (FAST PATH LOOKUP)

## GOAL
Resolve path -> object_id quickly
Avoid deep directory scans on every read/write

## TWO LAYERS
L1: in-memory B-tree (hot)
L2: on-disk LSM index (durable)
Fallback: metadata graph traversal

## LOOKUP FLOW
query path
  ↓
check L1 B-tree
  ↓ miss
check L2 LSM
  ↓ miss
graph walk from root dir metadata
  ↓
return object_id or ENOENT

## UPDATE FLOW
on commit/rename/delete:
- update metadata first
- then update index entries
- stale index tolerated briefly (fallback safe)

## WHAT CAN GO WRONG?
1) Index stale after crash
   -> rebuild from metadata graph

2) LSM compaction lag
   -> temporary slower lookups
   -> no correctness loss

3) Index says present but metadata missing
   -> treat as stale index
   -> remove bad index entry

4) Massive rename operation
   -> batched index updates required
   -> fallback traversal for misses during transition

## INDEX RULES
- Metadata remains source of truth
- Index is accelerator, not authority
- Rebuild path must always exist
