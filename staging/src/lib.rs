use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StagingError {
    #[error("staging quota exceeded")]
    QuotaExceeded,

    #[error("slot not found")]
    SlotNotFound,

    #[error("staging lock poisoned")]
    Poisoned,

    #[error("invalid slot id")]
    InvalidSlotId,

    #[error("staging io error: {0}")]
    Io(#[from] std::io::Error),
}

pub trait StagingManager: Send + Sync {
    fn begin_slot(&self, tx_id: &str, path: &str) -> Result<(), StagingError>;
    fn mark_committed(&self, tx_id: &str) -> Result<(), StagingError>;
    fn cleanup_slot(&self, tx_id: &str) -> Result<(), StagingError>;
    fn purge_stale_uncommitted(&self) -> Result<usize, StagingError>;
    fn purge_all_slots(&self) -> Result<usize, StagingError> {
        Ok(0)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Slot {
    path: String,
    created_at: SystemTime,
    committed: bool,
}

pub struct InMemoryStaging {
    quota: usize,
    slots: Mutex<HashMap<String, Slot>>,
}

impl InMemoryStaging {
    pub fn new(quota: usize) -> Self {
        Self {
            quota: quota.max(1),
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub fn active_slots(&self) -> Result<usize, StagingError> {
        let slots = self.slots.lock().map_err(|_| StagingError::Poisoned)?;
        Ok(slots.len())
    }
}

impl StagingManager for InMemoryStaging {
    fn begin_slot(&self, tx_id: &str, path: &str) -> Result<(), StagingError> {
        let mut slots = self.slots.lock().map_err(|_| StagingError::Poisoned)?;
        if slots.contains_key(tx_id) {
            return Ok(());
        }
        if slots.len() >= self.quota {
            return Err(StagingError::QuotaExceeded);
        }

        slots.insert(
            tx_id.to_string(),
            Slot {
                path: path.to_string(),
                created_at: SystemTime::now(),
                committed: false,
            },
        );
        Ok(())
    }

    fn mark_committed(&self, tx_id: &str) -> Result<(), StagingError> {
        let mut slots = self.slots.lock().map_err(|_| StagingError::Poisoned)?;
        let slot = slots.get_mut(tx_id).ok_or(StagingError::SlotNotFound)?;
        slot.committed = true;
        Ok(())
    }

    fn cleanup_slot(&self, tx_id: &str) -> Result<(), StagingError> {
        let mut slots = self.slots.lock().map_err(|_| StagingError::Poisoned)?;
        // Idempotent cleanup.
        slots.remove(tx_id);
        Ok(())
    }

    fn purge_stale_uncommitted(&self) -> Result<usize, StagingError> {
        let mut slots = self.slots.lock().map_err(|_| StagingError::Poisoned)?;
        let before = slots.len();
        slots.retain(|_, slot| slot.committed);
        Ok(before - slots.len())
    }

    fn purge_all_slots(&self) -> Result<usize, StagingError> {
        let mut slots = self.slots.lock().map_err(|_| StagingError::Poisoned)?;
        let purged = slots.len();
        slots.clear();
        Ok(purged)
    }
}

pub struct FsStaging {
    root: PathBuf,
    quota: usize,
    lock: Mutex<()>,
}

impl FsStaging {
    pub fn open<P: AsRef<Path>>(root: P, quota: usize) -> Result<Self, StagingError> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
            quota: quota.max(1),
            lock: Mutex::new(()),
        })
    }

    pub fn active_slots(&self) -> Result<usize, StagingError> {
        let _guard = self.lock.lock().map_err(|_| StagingError::Poisoned)?;
        Ok(self.list_slot_ids()?.len())
    }

    fn slot_dir(&self, tx_id: &str) -> Result<PathBuf, StagingError> {
        if tx_id.is_empty()
            || tx_id.contains('/')
            || tx_id.contains('\\')
            || tx_id.contains('\0')
            || tx_id.contains("..")
        {
            return Err(StagingError::InvalidSlotId);
        }
        Ok(self.root.join(tx_id))
    }

    fn meta_path(slot_dir: &Path) -> PathBuf {
        slot_dir.join("slot.meta")
    }

    fn list_slot_ids(&self) -> Result<Vec<String>, StagingError> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let id = name.to_string_lossy().to_string();
            ids.push(id);
        }
        Ok(ids)
    }

    fn write_slot_meta(slot_dir: &Path, path: &str, committed: bool) -> Result<(), StagingError> {
        let tmp = slot_dir.join("slot.meta.tmp");
        let final_path = Self::meta_path(slot_dir);
        let mut file = fs::File::create(&tmp)?;
        // Simple durable staging slot metadata format.
        writeln!(file, "path={path}")?;
        writeln!(file, "committed={}", if committed { "1" } else { "0" })?;
        file.sync_all()?;
        fs::rename(tmp, final_path)?;
        Ok(())
    }

    fn read_slot_committed(slot_dir: &Path) -> bool {
        let data = match fs::read_to_string(Self::meta_path(slot_dir)) {
            Ok(data) => data,
            Err(_) => return false,
        };
        for line in data.lines() {
            if let Some(value) = line.strip_prefix("committed=") {
                return value.trim() == "1";
            }
        }
        false
    }
}

impl StagingManager for FsStaging {
    fn begin_slot(&self, tx_id: &str, path: &str) -> Result<(), StagingError> {
        let _guard = self.lock.lock().map_err(|_| StagingError::Poisoned)?;
        let slot_dir = self.slot_dir(tx_id)?;
        if slot_dir.is_dir() {
            return Ok(());
        }
        if self.list_slot_ids()?.len() >= self.quota {
            return Err(StagingError::QuotaExceeded);
        }
        fs::create_dir_all(&slot_dir)?;
        Self::write_slot_meta(&slot_dir, path, false)?;
        Ok(())
    }

    fn mark_committed(&self, tx_id: &str) -> Result<(), StagingError> {
        let _guard = self.lock.lock().map_err(|_| StagingError::Poisoned)?;
        let slot_dir = self.slot_dir(tx_id)?;
        if !slot_dir.is_dir() {
            return Err(StagingError::SlotNotFound);
        }
        let data = fs::read_to_string(Self::meta_path(&slot_dir)).unwrap_or_default();
        let mut path = String::new();
        for line in data.lines() {
            if let Some(value) = line.strip_prefix("path=") {
                path = value.to_string();
                break;
            }
        }
        Self::write_slot_meta(&slot_dir, &path, true)?;
        Ok(())
    }

    fn cleanup_slot(&self, tx_id: &str) -> Result<(), StagingError> {
        let _guard = self.lock.lock().map_err(|_| StagingError::Poisoned)?;
        let slot_dir = self.slot_dir(tx_id)?;
        if slot_dir.exists() {
            fs::remove_dir_all(slot_dir)?;
        }
        Ok(())
    }

    fn purge_stale_uncommitted(&self) -> Result<usize, StagingError> {
        let _guard = self.lock.lock().map_err(|_| StagingError::Poisoned)?;
        let ids = self.list_slot_ids()?;
        let mut purged = 0;
        for id in ids {
            let slot_dir = self.root.join(id);
            if !Self::read_slot_committed(&slot_dir) {
                fs::remove_dir_all(slot_dir)?;
                purged += 1;
            }
        }
        Ok(purged)
    }

    fn purge_all_slots(&self) -> Result<usize, StagingError> {
        let _guard = self.lock.lock().map_err(|_| StagingError::Poisoned)?;
        let ids = self.list_slot_ids()?;
        let mut purged = 0;
        for id in ids {
            let slot_dir = self.root.join(id);
            if slot_dir.exists() {
                fs::remove_dir_all(slot_dir)?;
                purged += 1;
            }
        }
        Ok(purged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn staging_quota_commit_and_purge() {
        let s = InMemoryStaging::new(1);
        s.begin_slot("tx1", "/a").expect("slot begin should work");
        assert!(s.begin_slot("tx2", "/b").is_err());

        s.mark_committed("tx1").expect("commit marker should work");
        s.cleanup_slot("tx1").expect("cleanup should work");
        assert_eq!(s.active_slots().expect("active count should work"), 0);

        s.begin_slot("tx3", "/c").expect("slot begin should work");
        let purged = s.purge_stale_uncommitted().expect("purge should work");
        assert_eq!(purged, 1);
    }

    #[test]
    fn slot_fields_are_populated() {
        let s = InMemoryStaging::new(2);
        s.begin_slot("tx1", "/a").expect("slot begin should work");

        let slots = s.slots.lock().expect("lock should work");
        let slot = slots.get("tx1").expect("slot should exist");
        assert_eq!(slot.path, "/a");
        assert!(slot.created_at <= SystemTime::now());
    }

    #[test]
    fn fs_staging_persists_slots_across_reopen_and_purges_stale() {
        let temp = tempdir().expect("temp dir should exist");
        let root = temp.path().join("staging");

        {
            let s = FsStaging::open(&root, 4).expect("fs staging should open");
            s.begin_slot("tx1", "/a").expect("slot begin should work");
            s.begin_slot("tx2", "/b").expect("slot begin should work");
            s.mark_committed("tx2")
                .expect("commit marker should work");
            assert_eq!(s.active_slots().expect("active slots should work"), 2);
        }

        {
            let s = FsStaging::open(&root, 4).expect("fs staging should reopen");
            let purged = s
                .purge_stale_uncommitted()
                .expect("purge stale should work");
            assert_eq!(purged, 1);
            assert_eq!(s.active_slots().expect("active slots should work"), 1);
            s.cleanup_slot("tx2").expect("cleanup should work");
            assert_eq!(s.active_slots().expect("active slots should work"), 0);
        }
    }

    #[test]
    fn fs_staging_quota_is_enforced_from_on_disk_state() {
        let temp = tempdir().expect("temp dir should exist");
        let root = temp.path().join("staging");

        let s = FsStaging::open(&root, 1).expect("fs staging should open");
        s.begin_slot("tx1", "/a").expect("slot begin should work");
        let err = s
            .begin_slot("tx2", "/b")
            .expect_err("second slot should exceed quota");
        assert!(matches!(err, StagingError::QuotaExceeded));
    }

    #[test]
    fn fs_staging_rejects_invalid_slot_id() {
        let temp = tempdir().expect("temp dir should exist");
        let root = temp.path().join("staging");
        let s = FsStaging::open(&root, 2).expect("fs staging should open");
        let err = s
            .begin_slot("../bad", "/a")
            .expect_err("invalid slot id should fail");
        assert!(matches!(err, StagingError::InvalidSlotId));
    }

    #[test]
    fn in_memory_purge_all_slots_clears_committed_and_uncommitted() {
        let s = InMemoryStaging::new(8);
        s.begin_slot("tx1", "/a").expect("slot begin should work");
        s.begin_slot("tx2", "/b").expect("slot begin should work");
        s.mark_committed("tx2")
            .expect("commit marker should work");

        let purged = s.purge_all_slots().expect("purge all should work");
        assert_eq!(purged, 2);
        assert_eq!(s.active_slots().expect("active count should work"), 0);
    }

    #[test]
    fn fs_staging_purge_all_slots_clears_committed_and_uncommitted() {
        let temp = tempdir().expect("temp dir should exist");
        let root = temp.path().join("staging");
        let s = FsStaging::open(&root, 8).expect("fs staging should open");
        s.begin_slot("tx1", "/a").expect("slot begin should work");
        s.begin_slot("tx2", "/b").expect("slot begin should work");
        s.mark_committed("tx2")
            .expect("commit marker should work");

        let purged = s.purge_all_slots().expect("purge all should work");
        assert_eq!(purged, 2);
        assert_eq!(s.active_slots().expect("active count should work"), 0);
    }
}
