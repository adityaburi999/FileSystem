# INDEXING SYSTEM

## STRUCTURE
- L1 In-Memory Index
- L2 Durable LSM Index
- Metadata Graph Fallback
- Index Rebuilder

## FLOW
- Lookup path request
- Query L1 index
- L1 miss -> query L2 index
- L2 miss -> traverse metadata graph
- Return object_id or ENOENT
- Commit/rename/delete -> update metadata
- Apply index update events
- Background compaction and rebuild tasks

## RULES
- Metadata graph is source of truth.
- Index is acceleration layer only.
- Miss path must have graph fallback.
- Rebuild path must be always available.

## FAILURES
- Stale index entry -> fallback graph + purge entry.
- Compaction lag -> degraded latency, no correctness loss.
- Rebuild interruption -> resume from checkpoint.
- Index corruption -> rebuild from metadata.
- Rename burst backlog -> batch updates + fallback serving.

## INVARIANTS
- Correct lookup is possible without index.
- Index divergence is recoverable.
- Committed metadata remains resolvable.
