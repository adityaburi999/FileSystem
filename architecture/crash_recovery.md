# CRASH RECOVERY (BOOT FLOW)

## BOOT RECOVERY FLOW
System startup
  ↓
Load WAL segments
  ↓
Replay txn timeline
  ↓
Classify txns: committed / aborted / incomplete
  ↓
Validate committed metadata-chunk links
  ↓
Repair or rollback incomplete txns
  ↓
Rebuild/check metadata index if required
  ↓
Cleanup stale staging slots
  ↓
Trigger post-recovery orphan scan
  ↓
Activate FUSE only after consistency reached

## TXN DECISION RULE
- TxnCommit present -> committed
- TxnAbort present -> aborted
- Missing final marker -> incomplete -> repair/rollback logic

## WHAT CAN GO WRONG?
1) WAL segment corruption
   -> stop normal mount
   -> attempt bounded salvage
   -> require admin intervention if unresolved

2) Metadata exists but chunk missing
   -> mark object unhealthy
   -> block reads to bad object

3) Staging txn with no commit marker
   -> discard staging
   -> treat chunks as potential orphan

4) Recovery loop repeatedly fails same txn
   -> quarantine txn id
   -> continue with safe subset

5) Index corruption
   -> rebuild from metadata graph

## RECOVERY RULES
- Never expose filesystem before replay completes
- Prefer rollback over risky auto-repair
- Preserve last known committed state
- Log every recovery decision path
