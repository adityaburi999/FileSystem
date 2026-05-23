# OBJECT STORE DESIGN (CHUNKS + BLAKE3)

## CHUNK FORMAT
chunk_id = BLAKE3(chunk_bytes)
Stored unit:
- header (magic/version/size/hash)
- data bytes
- trailer (checksum)

## DISK LAYOUT
/objects/<p1>/<p2>/<full_hash>.chunk
Example:
/objects/ab/cd/abcd....chunk

## WRITE FLOW
receive chunk
  ↓
compute BLAKE3
  ↓
check existing chunk (dedup)
  ↓ if absent
write temp file
  ↓ fsync
atomic rename to final chunk path

## READ FLOW
locate chunk by hash path
  ↓
read bytes
  ↓
recompute BLAKE3
  ↓ match?
yes -> return
no  -> EIO + integrity alert

## DEDUP RULES
- Same hash => same content assumption
- Store once, reference many times
- Deletion only when refcount reaches zero

## WHAT CAN GO WRONG?
1) Partial temp file after crash
   -> cleanup temp artifacts on boot/GC

2) Hash collision (extremely unlikely)
   -> optional secondary verify (size + checksum)

3) Disk path shard imbalance
   -> rebalance shard strategy if needed

4) Silent bit rot
   -> detected on hash verify during read/scrub

## OBJECT STORE RULES
- Immutable chunks only
- No overwrite of existing chunk id
- Always verify content on load path
