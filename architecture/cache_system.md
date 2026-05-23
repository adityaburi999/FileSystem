# CACHE SYSTEM

## STRUCTURE
- L1 RAM Cache
- L2 SSD Cache
- Metadata Cache
- Eviction Manager
- Invalidation Bus

## FLOW
- Read chunk request
- Query L1 cache
- L1 miss -> query L2 cache
- L2 miss -> fetch object store chunk
- Verify chunk hash
- Insert into L2 and promote to L1
- Return bytes
- Commit/delete/recovery events -> invalidate affected keys

## RULES
- Cache cannot override metadata truth.
- Cache fill on read path only.
- Invalidation required on version change.
- Integrity verification required before caching fetched chunk.

## FAILURES
- Stale metadata cache -> version check + invalidate.
- SSD cache corruption -> drop entry + refetch.
- Memory pressure -> enforce eviction policy.
- Invalidation delay -> serve only version-checked entries.
- Cache stampede -> request coalescing.

## INVARIANTS
- Cached data maps to committed version.
- Unverified data is never cached.
- Cache miss does not alter correctness.
