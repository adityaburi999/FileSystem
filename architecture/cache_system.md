# CACHE SYSTEM (RAM/SSD + INVALIDATION)

## TIERS
L1 RAM cache (hot chunks + metadata)
L2 SSD cache (warm chunks)
L3 object store (cold source)

## READ FLOW WITH CACHE
request chunk
  ↓
L1 hit? return
  ↓ miss
L2 hit? promote to L1 + return
  ↓ miss
fetch from object store
  ↓
verify hash
  ↓
insert L2 + L1
  ↓
return bytes

## EVICTION
- LRU/LFU hybrid
- memory pressure -> evict coldest entries
- metadata and chunk caches tracked separately

## INVALIDATION EVENTS
- metadata CAS commit -> invalidate old metadata cache
- delete/tombstone -> invalidate path/meta entries
- GC physical delete -> invalidate chunk cache
- recovery complete -> optional full cache flush

## WHAT CAN GO WRONG?
1) Stale metadata cache causes old version read
   -> version check + invalidate

2) Cache pollution from large scan
   -> protect frequently used entries (LFU bias)

3) SSD cache corruption
   -> hash verify on read detects bad entry
   -> fallback to object store

4) Memory exhaustion
   -> aggressive eviction + backpressure

## CACHE RULES
- Cache never overrides metadata truth
- Any integrity doubt -> bypass and reload
- Correctness first, speed second
