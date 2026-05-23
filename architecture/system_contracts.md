# SYSTEM CONTRACTS (GLOBAL RULES)

These rules are mandatory for all modules.

## CONTRACT 1
No in-place overwrite of committed data.

## CONTRACT 2
All metadata mutations use CAS.

## CONTRACT 3
State-changing operations must be WAL-tracked.

## CONTRACT 4
No partial write visible to users.

## CONTRACT 5
Chunk ID must equal BLAKE3(content).

## CONTRACT 6
Metadata is source of truth.

## CONTRACT 7
Delete is logical first, physical later.

## CONTRACT 8
GC never deletes uncertain/live-referenced chunks.

## CONTRACT 9
Version numbers are monotonic.

## CONTRACT 10
Recovery completes before mount activation.

## CONTRACT 11
Tombstoned objects are not visible in namespace.

## CONTRACT 12
Cache/index may accelerate, but cannot override truth.

## WHAT IF CONTRACT BREAKS?
- Break #1/#5 -> data integrity risk
- Break #2/#9 -> write conflict corruption risk
- Break #3/#10 -> crash recovery risk
- Break #7/#8 -> data loss risk
- Break #11/#12 -> stale/ghost visibility risk

## ENFORCEMENT FLOW
operation request
  ↓
validate contract preconditions
  ↓ fail
reject operation + log reason
  ↓ pass
execute operation
  ↓
validate postconditions
  ↓
commit + audit

## FINAL RULE
If any contract cannot be proven true,
-> do not commit,
-> keep last safe state.
