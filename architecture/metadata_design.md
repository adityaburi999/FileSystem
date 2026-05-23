# METADATA DESIGN (FILE/DIR + REDIRECT MODEL)

## FILE OBJECT (REDIRECT)
Fields:
- object_id
- version
- state (ACTIVE/TOMBSTONE)
- size
- chunk_ids (ordered)
- timestamps
- optional inline_data (small file)

Meaning:
- file content is not stored directly (except tiny inline)
- metadata redirects to immutable chunk list

## DIR OBJECT
Fields:
- object_id
- version
- state
- parent_id
- entries: name -> (child_id, type)

## VERSION MODEL
Write creates new file version:
V1 -> [A,B,C]
V2 -> [A,B,D]
V3 -> [A,B,D,E]
Shared chunks remain deduped

## STORAGE MODEL
/metadata/files/<shard>/<object>.vN.meta
/metadata/dirs/<shard>/<object>.vN.meta
Latest version resolved by metadata index

## WHAT CAN GO WRONG?
1) Version gap appears unexpectedly
   -> inspect WAL timeline
   -> mark suspicious update path

2) Dir entry points to missing child
   -> metadata inconsistency
   -> recovery graph repair

3) Inline file too large due to threshold bug
   -> force chunk migration

4) Tombstoned object still indexed as active
   -> index cleanup + rebuild path

## METADATA RULES
- Never update in-place
- Every mutation is new version + CAS
- Directory and file states must stay graph-consistent
- If graph uncertain, block mutation until repaired
