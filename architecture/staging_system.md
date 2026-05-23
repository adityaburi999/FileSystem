# STAGING SYSTEM (TEMP WRITE AREA)

## PURPOSE
Keep in-progress writes isolated
Prevent partial user-visible file states

## STAGING STRUCTURE
/staging/txn_<id>/
- buffer.tmp
- chunks.tmp
- redirect.tmp
- txn_state

## FLOW
open txn slot
  ↓
stream write blocks into buffer
  ↓
chunk + hash + persist chunk
  ↓
record chunk id in staging + WAL
  ↓
close/fsync triggers CAS metadata commit
  ↓ success
mark txn committed
  ↓
delete staging slot

## ABORT FLOW
error/conflict/crash
  ↓
txn not committed
  ↓
staging discarded on recovery
  ↓
orphan chunks handled by GC

## WHAT CAN GO WRONG?
1) Staging grows too large
   -> enforce quota
   -> return ENOSPC/backpressure

2) Crash during staging cleanup
   -> replay detects committed txn
   -> remove stale slot later

3) Staging slot leaks
   -> periodic stale txn scanner

4) Two txns same file
   -> CAS at commit resolves winner

## STAGING RULES
- Never expose staging paths to users
- Never treat staging data as committed data
- Commit truth is CAS + WAL, not tmp files
