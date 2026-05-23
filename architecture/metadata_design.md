# METADATA DESIGN

## STRUCTURE
- File Redirect Object
- Directory Object
- Version Chain
- Metadata Index
- Tombstone State

## FLOW
- Create file -> allocate object_id
- Write content -> produce chunk_id list
- Build metadata version Vn
- CAS publish Vn as latest
- Update metadata index
- Delete request -> write tombstone version
- GC eligibility after retention policy

## RULES
- File content references immutable chunks.
- Metadata updates are append-version only.
- Directory entries map name -> child object_id.
- Tombstone marks logical deletion state.
- Index can accelerate, not override metadata truth.

## FAILURES
- Version gap anomaly -> WAL timeline verification.
- Directory entry dangling -> graph repair pass.
- Active/tombstone mismatch -> index correction.
- Metadata corruption -> quarantine + recovery job.
- CAS mismatch on publish -> retry with latest.

## INVARIANTS
- One latest version pointer per object.
- Graph links remain type-consistent.
- Tombstoned object is non-active.
