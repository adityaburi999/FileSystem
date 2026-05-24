# DELETE FLOW

## STRUCTURE
- FUSE Delete Handler
- Path Resolver
- Metadata Engine
- WAL Engine
- Namespace Index
- GC Trigger

## FLOW
- unlink(path) -> FUSE
- Resolve path -> object_id
- CAS set metadata state = TOMBSTONE
- Append delete event to WAL
- Remove namespace entry
- Invalidate path/metadata cache
- Enqueue GC candidate scan
- GC validates references
- GC physically deletes safe orphans

## RULES
- Delete is logical before physical.
- Tombstone write requires CAS.
- Physical delete requires no live reference.
- Namespace visibility follows tombstone state.

## FAILURES
- Path missing -> ENOENT.
- CAS conflict -> retry with latest version.
- WAL failure -> abort delete transaction.
- Namespace update failure -> recovery replay repair.
- Shared chunk candidate -> skip physical delete.

## INVARIANTS
- Tombstoned object is not user-visible.
- Live-referenced chunk is never deleted.
- Delete replay is idempotent.
