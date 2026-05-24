# SCALING MODEL

## STRUCTURE
- Single-Node Tier
- High-Capacity Node Tier
- Distributed Shard Tier
- Migration Controller
- Consistency Verifier

## FLOW
- Monitor capacity and latency thresholds
- Threshold reached -> choose next tier
- Freeze migration boundary snapshot
- Migrate metadata/index components
- Rebalance object shards
- Validate consistency checks
- Shift traffic to new tier
- Keep rollback checkpoint until stable window ends

## RULES
- Scale transitions are staged.
- Core invariants remain unchanged across tiers.
- Migration requires rollback checkpoint.
- Consistency validation gates cutover.

## FAILURES
- Migration interruption -> rollback to checkpoint.
- Shard hot-spot -> rebalance partitions.
- Replica lag -> throttle writes or degrade mode.
- Quorum loss -> read-only safety mode.
- Cross-shard inconsistency -> stop cutover and repair.

## INVARIANTS
- Cutover only after consistency pass.
- Rollback path exists during migration.
- Committed data remains addressable after scaling.
