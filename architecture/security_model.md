# SECURITY MODEL

## STRUCTURE
- Integrity Verifier
- WAL Auditor
- Metadata Version Guard
- Delete Safety Guard
- Quarantine Manager

## FLOW
- Write path -> staging -> WAL -> CAS commit
- Read path -> fetch bytes -> hash verify -> serve
- Delete path -> tombstone -> retention -> refcheck -> physical delete
- Boot path -> WAL replay -> consistency check -> service enable
- Integrity fault -> quarantine + alert

## RULES
- No state mutation without WAL record.
- No served data without integrity verification.
- No physical delete without reference proof.
- Version chain checks required on metadata updates.

## FAILURES
- Hash mismatch -> reject data + quarantine object.
- WAL inconsistency -> block mutable operations.
- Metadata tamper signal -> force recovery scan.
- Unauthorized mutation attempt -> deny + audit.
- Integrity subsystem unavailable -> fail closed for unsafe ops.

## INVARIANTS
- Integrity checks gate visibility.
- Audit trail exists for mutating actions.
- Safety checks precede destructive actions.
