# Security Model — RedirectFS

This document describes the security architecture of RedirectFS: how data integrity is enforced, what threats the system defends against, and what its limitations are.

---

## Security Philosophy

RedirectFS is designed for **structural security** — security properties that emerge from the system's architecture, not from a single security boundary or secret.

> "Harder to silently corrupt. Easier to detect tampering. Safer under failure conditions."

RedirectFS does not claim to be a complete security solution. It must be combined with OS-level access control, network security, and encryption at rest (if required) for a full security posture.

---

## Threat Model

### In-Scope Threats

| Threat | Description |
|---|---|
| **Silent data corruption** | Bit rot, faulty storage hardware writing wrong data |
| **Partial writes** | System crash leaving files in an inconsistent state |
| **Stale reads** | Reading an old version of data due to cache/metadata inconsistency |
| **Accidental deletion** | Files deleted before their data is fully orphaned from references |
| **Unauthorized metadata modification** | An attacker or buggy process modifying file metadata directly |
| **Replay attacks (local)** | Replaying old WAL entries to revert state |

### Out-of-Scope Threats (Not Currently Addressed)

| Threat | Reason Out of Scope |
|---|---|
| **Encryption at rest** | Not implemented; use full-disk encryption (LUKS) at the OS layer |
| **Authentication / authorization** | Relies on standard POSIX UID/GID + kernel permission checks |
| **Network attacks** | RedirectFS is a local filesystem; no network surface in its current form |
| **Privileged attacker with root access** | A root attacker can modify `/objects/` directly; integrity checks detect but cannot prevent this |
| **Side-channel attacks** | Out of scope for a research filesystem |

---

## Layer 1 — Immutable Object Store

**Mechanism:** Once a chunk is written to `/objects/`, it is never modified. The filename is the BLAKE3 hash of the content.

**Security property:** Any modification to a stored chunk changes the file's content but not its name. When the chunk is next read and re-hashed, the hash will not match the stored `ChunkId` → corruption is detected immediately.

**Attacker scenario:** An attacker modifies `/objects/ab/cd/abcd...chunk` directly.
**Detection:** On next read, BLAKE3 re-hash fails → `EIO` returned to application, corruption event logged.

---

## Layer 2 — Content Hashing (BLAKE3)

Every chunk is verified by three integrity checks on every read:

1. **Header CRC32** — detects header corruption (magic, size fields).
2. **Data CRC32** — detects data corruption written at store time (fast, lightweight).
3. **BLAKE3 re-hash** — cryptographically verifies the data matches the chunk's identity.

BLAKE3 is:
- Collision-resistant: it is computationally infeasible to create two different data blocks with the same BLAKE3 hash.
- Second-preimage resistant: given a chunk hash, it is infeasible to produce a different chunk with the same hash.
- This means the `ChunkId` is a **cryptographic commitment** to the chunk's content.

**Limitation:** BLAKE3 is not keyed — a sufficiently motivated attacker who can both modify chunks on disk AND update their BLAKE3 filenames could defeat this check. This requires write access to the underlying storage medium.

---

## Layer 3 — Versioned Metadata (CAS)

**Mechanism:** Every metadata update is a CAS (Compare-And-Swap) operation. Old versions are never overwritten — they are preserved as separate `.vN.meta` files.

**Security property:**
- Any unauthorized change to a `FileObject` would need to write a new version file and update the SQLite index, which requires access to `/metadata/` at the OS level.
- The version history acts as an **immutable audit trail**: every state the file has ever been in is preserved (until GC pruning).

**Detection:** An unexpected version jump (e.g., version jumps from 3 to 7) indicates unauthorized writes. The audit log and WAL can be used to identify the gap.

---

## Layer 4 — Write-Ahead Logging

**Mechanism:** Every operation is logged in the WAL before it takes effect.

**Security property:**
- The WAL provides an **ordered, append-only audit trail** of all filesystem operations.
- A transaction that does not appear in the WAL cannot have been committed.
- Attempting to roll back committed state would require modifying or truncating the WAL file — which is a detectable attack.

**Tamper detection:** WAL segment files include a checksum in each entry. A corrupted or truncated WAL segment is detected during recovery and triggers a consistency check.

---

## Layer 5 — Tombstone-Based Deletion

**Mechanism:** Files are never immediately erased. They are tombstoned (logical delete) and physically removed only after GC confirms all references are gone and the retention window has passed.

**Security property:**
- Accidental or malicious delete of a file is not immediately irreversible.
- During the retention window, an administrator can recover the file by removing the tombstone flag.
- This provides a **soft-delete safety net** against ransomware or accidental bulk deletion.

**Limitation:** If GC runs and the retention window has elapsed, data is permanently deleted. Backups are needed for long-term recovery.

---

## Layer 6 — Staging Isolation

**Mechanism:** In-progress writes are stored in `/staging/`, which is hidden from the FUSE namespace.

**Security property:**
- No partial write is ever visible to users or applications.
- An attacker cannot read or tamper with an in-progress write through the FUSE interface.
- Staging slots are only cleaned up after a successful commit or explicit discard.

---

## Integrity Verification (`fsck` Mode)

RedirectFS provides an offline integrity verification mode (analogous to `fsck`):

```
1. Scan all /metadata/ files.
2. For each FileObject: verify all chunk_ids exist in /objects/.
3. For each chunk: recompute BLAKE3 hash, verify matches filename.
4. Verify DirObject entries point to valid FileObjects or DirObjects.
5. Verify index consistency: every active FileObject has a path entry.
6. Report any inconsistencies.
```

This can be run without mounting the filesystem (offline check).

---

## Access Control

RedirectFS stores `owner_uid`, `owner_gid`, and `permissions` (Unix mode bits) in each `FileObject` and `DirObject`. FUSE enforces these via the standard Linux permission model:

- The FUSE `allow_other` option must be set to allow non-root users to access the filesystem.
- Kernel permission checks happen before RedirectFS sees the request.
- RedirectFS re-checks permissions at the metadata level for defense-in-depth.

**Note:** RedirectFS does not currently implement POSIX ACLs or extended attributes for ACL storage. This is a future enhancement.

---

## Security Invariants

1. **A chunk's identity cannot be forged** — BLAKE3 ensures the `ChunkId` cryptographically commits to the content.
2. **No committed state can be silently removed** — tombstones and GC retention windows prevent immediate data loss.
3. **Every write is logged** — WAL provides a complete, ordered history of all operations.
4. **Partial writes are never exposed** — staging ensures atomicity from the user's perspective.
5. **Old versions are preserved** — CAS versioning retains history until explicitly pruned by GC.

---

## Recommendations for Deployment

| Security Goal | Recommended Action |
|---|---|
| Encryption at rest | Use LUKS full-disk encryption on the storage volume |
| Multi-user access control | Configure FUSE `allow_other` + enforce POSIX permissions |
| Audit logging | Enable structured WAL logging + ship to a SIEM system |
| Ransomware protection | Set a long tombstone retention window (e.g., 7 days) |
| Backup | Periodically snapshot `/metadata/` + `/objects/` to off-site storage |
| Tamper detection | Run `fsck` mode periodically and alert on any integrity failures |
