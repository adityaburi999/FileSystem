use std::collections::HashMap;
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
}

pub trait StagingManager: Send + Sync {
    fn begin_slot(&self, tx_id: &str, path: &str) -> Result<(), StagingError>;
    fn mark_committed(&self, tx_id: &str) -> Result<(), StagingError>;
    fn cleanup_slot(&self, tx_id: &str) -> Result<(), StagingError>;
    fn purge_stale_uncommitted(&self) -> Result<usize, StagingError>;
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
