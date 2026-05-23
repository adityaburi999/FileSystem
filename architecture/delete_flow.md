# DELETE FLOW (LOGICAL -> PHYSICAL)

## FLOW
unlink("/a/b/file")
  ↓
FUSE receives delete request
  ↓
Resolve path -> object_id
  ↓
CAS update metadata state = TOMBSTONE
  ↓
Append delete event to WAL
  ↓
Remove name from live directory namespace
  ↓
Invalidate metadata cache
  ↓
Trigger GC candidate scan (async)
  ↓
Later: GC verifies unreferenced chunks
  ↓
Physical chunk delete

## LOGICAL DELETE EFFECT
- File disappears immediately from user namespace
- Data/chunks remain until safe GC window passes

## WHAT CAN GO WRONG?
1) Delete request on missing file
   -> ENOENT

2) CAS conflict while deleting
   -> retry with latest version
   -> if changed, re-evaluate delete

3) Crash after tombstone before namespace cleanup
   -> replay WAL
   -> cleanup stale path entry on recovery

4) GC tries deleting shared chunk
   -> refcount check prevents deletion

5) Delete of non-empty directory
   -> ENOTEMPTY (or recursive flow if enabled)

## DELETE SAFETY RULES
- Never physical-delete before reference proof
- Tombstone is source of delete truth
- If uncertainty exists, keep data and defer GC
