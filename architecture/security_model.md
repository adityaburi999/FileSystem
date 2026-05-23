# SECURITY MODEL (INTEGRITY-FIRST)

## SECURITY BASE
- Immutable chunks
- Versioned metadata
- WAL traceability
- Hash-based verification (BLAKE3)

## THREATS CONSIDERED
- silent corruption
- partial writes after crash
- stale or replayed metadata state
- unsafe deletes causing data loss

## INTEGRITY FLOW
read chunk
  ↓
verify checksum + BLAKE3
  ↓ pass
serve data
  ↓ fail
EIO + alert + quarantine path

## WRITE SAFETY FLOW
staging write
  ↓
WAL append
  ↓
CAS commit
  ↓
user-visible update

## DELETE SAFETY FLOW
logical tombstone
  ↓
retention delay
  ↓
reference check
  ↓
physical delete

## WHAT CAN GO WRONG?
1) Direct disk tampering
   -> hash mismatch detection

2) WAL tamper/truncation
   -> replay inconsistency alarm

3) Unauthorized metadata edit
   -> version/journal mismatch detection

4) Root attacker
   -> can still alter storage; detection possible, prevention limited

## SECURITY RULES
- Never trust raw disk bytes without verification
- Never skip WAL on state-changing operation
- Never bypass metadata authority
- On integrity doubt, deny service of suspect data
