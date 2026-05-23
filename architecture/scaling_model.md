# SCALING MODEL (1GB -> 100TB)

## STAGE 1: 1GB -> 1TB (SINGLE NODE)
- local object store
- sqlite metadata
- in-memory index + cache
- simple GC

Risk points:
- single disk failure
- limited metadata throughput

## STAGE 2: 1TB -> 10TB (HEAVY SINGLE NODE)
- stronger NVMe layout
- rocksdb/lsm metadata path
- larger cache tiers
- faster parallel chunk IO

Risk points:
- compaction spikes
- GC scan cost growth

## STAGE 3: 10TB -> 100TB (DISTRIBUTED)
- metadata sharding
- object store sharding by hash
- replication + quorum
- distributed cache invalidation

Risk points:
- network partition
- cross-shard txn complexity
- replica lag

## SCALE TRANSITION FLOW
capacity threshold reached
  ↓
enable next-tier config
  ↓
migrate metadata/index
  ↓
rebalance object shards
  ↓
verify consistency
  ↓
switch traffic

## WHAT CAN GO WRONG?
1) Migration interrupted
   -> rollback to previous tier snapshot

2) Shard hot-spot
   -> rehash/rebalance strategy

3) Quorum loss
   -> read-only degraded mode

4) Cluster metadata split-brain risk
   -> consensus guard (raft-like)

## SCALING RULES
- keep same invariants at every scale
- move complexity gradually
- always keep rollback path during migration
