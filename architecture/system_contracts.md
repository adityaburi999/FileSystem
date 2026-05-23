# SYSTEM CONTRACTS

## STRUCTURE
- Mutation Contracts
- Visibility Contracts
- Integrity Contracts
- Deletion Contracts
- Recovery Contracts

## FLOW
- Receive operation request
- Validate preconditions
- Execute operation steps
- Validate postconditions
- Commit state
- Emit audit record
- Reject operation on any contract failure

## RULES
- Contract C1: no in-place committed overwrite.
- Contract C2: all metadata mutations require CAS.
- Contract C3: all mutating operations require WAL.
- Contract C4: no partial state exposure.
- Contract C5: chunk_id must match BLAKE3(content).
- Contract C6: metadata is source of truth.
- Contract C7: delete is logical before physical.
- Contract C8: recovery completes before mount enable.

## FAILURES
- C2 failure -> reject commit and retry path.
- C3 failure -> abort operation.
- C4 failure risk -> rollback to last commit.
- C5 failure -> quarantine chunk and alert.
- C8 failure -> block mount or force read-only mode.

## INVARIANTS
- Contract validation precedes commit.
- Failed contract implies non-commit.
- Last safe committed state is preserved.
