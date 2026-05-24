use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use wal::TxStatus;

#[derive(Debug, Error)]
pub enum GcError {
    #[error("live reference source error: {0}")]
    LiveRef(String),

    #[error("wal inflight source error: {0}")]
    Inflight(String),

    #[error("chunk inventory source error: {0}")]
    Inventory(String),

    #[error("chunk delete failure for {chunk_id}: {reason}")]
    Delete { chunk_id: String, reason: String },

    #[error("gc internal lock poisoned")]
    Poisoned,
}

pub trait LiveRefSource: Send + Sync {
    fn live_chunk_ids(&self) -> Result<HashSet<String>, String>;
}

pub trait InflightRefSource: Send + Sync {
    fn inflight_chunk_ids(&self) -> Result<HashSet<String>, String>;
}

pub trait ChunkInventory: Send + Sync {
    fn all_chunk_ids(&self) -> Result<Vec<String>, String>;
    fn delete_chunk(&self, chunk_id: &str) -> Result<(), String>;
}

pub trait GcTrigger: Send + Sync {
    fn enqueue_candidate_scan(&self) -> Result<(), String>;
}

pub trait GcWorker: Send + Sync {
    fn run_enqueued_once(&self) -> Result<Vec<GcReport>, String>;
}

#[derive(Debug, Clone)]
pub struct GcAuditRecord {
    pub timestamp: SystemTime,
    pub chunk_id: String,
    pub action: &'static str,
}

#[derive(Debug, Clone)]
pub struct GcReport {
    pub candidates: usize,
    pub deleted: usize,
    pub deferred: usize,
}

pub struct GarbageCollector<L, I, S>
where
    L: LiveRefSource,
    I: InflightRefSource,
    S: ChunkInventory,
{
    live_source: L,
    inflight_source: I,
    store: S,
    retention: Duration,
    enqueued_scans: AtomicUsize,
    orphan_first_seen: Mutex<HashMap<String, SystemTime>>,
    audit_log: Mutex<Vec<GcAuditRecord>>,
}

impl<L, I, S> GarbageCollector<L, I, S>
where
    L: LiveRefSource,
    I: InflightRefSource,
    S: ChunkInventory,
{
    pub fn new(live_source: L, inflight_source: I, store: S, retention: Duration) -> Self {
        Self {
            live_source,
            inflight_source,
            store,
            retention,
            enqueued_scans: AtomicUsize::new(0),
            orphan_first_seen: Mutex::new(HashMap::new()),
            audit_log: Mutex::new(Vec::new()),
        }
    }

    pub fn run_enqueued(&self) -> Result<Vec<GcReport>, GcError> {
        let runs = self.enqueued_scans.swap(0, Ordering::SeqCst);
        let mut reports = Vec::new();
        for _ in 0..runs {
            reports.push(self.sweep_once()?);
        }
        Ok(reports)
    }

    pub fn scan_candidates(&self) -> Result<Vec<String>, GcError> {
        let live_refs = self.live_source.live_chunk_ids().map_err(GcError::LiveRef)?;
        let inflight_refs = self
            .inflight_source
            .inflight_chunk_ids()
            .map_err(GcError::Inflight)?;
        let all_chunks = self.store.all_chunk_ids().map_err(GcError::Inventory)?;

        let now = SystemTime::now();
        let mut first_seen = self.orphan_first_seen.lock().map_err(|_| GcError::Poisoned)?;
        let mut candidates = Vec::new();
        let mut current_orphans = HashSet::new();

        for chunk_id in all_chunks {
            if live_refs.contains(&chunk_id) || inflight_refs.contains(&chunk_id) {
                first_seen.remove(&chunk_id);
                continue;
            }

            current_orphans.insert(chunk_id.clone());
            let seen = first_seen.entry(chunk_id.clone()).or_insert(now);
            let age_ok = now
                .duration_since(*seen)
                .map(|d| d >= self.retention)
                .unwrap_or(false);
            if age_ok {
                candidates.push(chunk_id);
            }
        }

        // Remove stale orphan-tracking entries no longer present in object store.
        first_seen.retain(|chunk_id, _| current_orphans.contains(chunk_id));
        Ok(candidates)
    }

    pub fn sweep_once(&self) -> Result<GcReport, GcError> {
        let candidates = self.scan_candidates()?;
        let mut deleted = 0;
        let mut deferred = 0;

        for chunk_id in &candidates {
            // Revalidate right before deletion.
            let live_refs = self.live_source.live_chunk_ids().map_err(GcError::LiveRef)?;
            let inflight_refs = self
                .inflight_source
                .inflight_chunk_ids()
                .map_err(GcError::Inflight)?;
            if live_refs.contains(chunk_id) || inflight_refs.contains(chunk_id) {
                deferred += 1;
                continue;
            }

            self.store
                .delete_chunk(chunk_id)
                .map_err(|reason| GcError::Delete {
                    chunk_id: chunk_id.clone(),
                    reason,
                })?;

            self.orphan_first_seen
                .lock()
                .map_err(|_| GcError::Poisoned)?
                .remove(chunk_id);

            self.audit_log
                .lock()
                .map_err(|_| GcError::Poisoned)?
                .push(GcAuditRecord {
                    timestamp: SystemTime::now(),
                    chunk_id: chunk_id.clone(),
                    action: "delete",
                });
            deleted += 1;
        }

        Ok(GcReport {
            candidates: candidates.len(),
            deleted,
            deferred,
        })
    }

    pub fn audit_log(&self) -> Result<Vec<GcAuditRecord>, GcError> {
        let log = self.audit_log.lock().map_err(|_| GcError::Poisoned)?;
        Ok(log.clone())
    }
}

impl<L, I, S> GcTrigger for GarbageCollector<L, I, S>
where
    L: LiveRefSource,
    I: InflightRefSource,
    S: ChunkInventory,
{
    fn enqueue_candidate_scan(&self) -> Result<(), String> {
        // Non-blocking foreground hook: enqueue background GC work.
        self.enqueued_scans.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl<L, I, S> GcWorker for GarbageCollector<L, I, S>
where
    L: LiveRefSource,
    I: InflightRefSource,
    S: ChunkInventory,
{
    fn run_enqueued_once(&self) -> Result<Vec<GcReport>, String> {
        self.run_enqueued().map_err(|e| e.to_string())
    }
}

impl LiveRefSource for metadata::InMemoryMetadataHook {
    fn live_chunk_ids(&self) -> Result<HashSet<String>, String> {
        self.all_live_chunk_ids().map_err(|e| e.to_string())
    }
}

impl LiveRefSource for metadata::FileBackedMetadataHook {
    fn live_chunk_ids(&self) -> Result<HashSet<String>, String> {
        self.all_live_chunk_ids().map_err(|e| e.to_string())
    }
}

impl InflightRefSource for wal::WalLog {
    fn inflight_chunk_ids(&self) -> Result<HashSet<String>, String> {
        let entries = self.read_all().map_err(|e| e.to_string())?;

        let mut latest_by_tx: BTreeMap<String, wal::WalEntry> = BTreeMap::new();
        for entry in entries {
            let replace = latest_by_tx
                .get(&entry.transaction_id)
                .map(|existing| entry.sequence > existing.sequence)
                .unwrap_or(true);
            if replace {
                latest_by_tx.insert(entry.transaction_id.clone(), entry);
            }
        }

        let mut protected = HashSet::new();
        for entry in latest_by_tx.into_values() {
            if entry.status == TxStatus::Pending {
                for chunk_id in entry.chunk_ids {
                    protected.insert(chunk_id);
                }
            }
        }
        Ok(protected)
    }
}

impl ChunkInventory for chunk_store::FsChunkStore {
    fn all_chunk_ids(&self) -> Result<Vec<String>, String> {
        self.list_chunk_ids().map_err(|e| e.to_string())
    }

    fn delete_chunk(&self, chunk_id: &str) -> Result<(), String> {
        self.remove_chunk(chunk_id).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Live {
        refs: Mutex<HashSet<String>>,
    }
    impl LiveRefSource for Live {
        fn live_chunk_ids(&self) -> Result<HashSet<String>, String> {
            self.refs
                .lock()
                .map(|s| s.clone())
                .map_err(|_| "poisoned".to_string())
        }
    }

    struct Inflight {
        refs: Mutex<HashSet<String>>,
    }
    impl InflightRefSource for Inflight {
        fn inflight_chunk_ids(&self) -> Result<HashSet<String>, String> {
            self.refs
                .lock()
                .map(|s| s.clone())
                .map_err(|_| "poisoned".to_string())
        }
    }

    struct Store {
        chunks: Mutex<HashSet<String>>,
    }
    impl ChunkInventory for Store {
        fn all_chunk_ids(&self) -> Result<Vec<String>, String> {
            self.chunks
                .lock()
                .map(|s| s.iter().cloned().collect())
                .map_err(|_| "poisoned".to_string())
        }

        fn delete_chunk(&self, chunk_id: &str) -> Result<(), String> {
            let mut s = self.chunks.lock().map_err(|_| "poisoned".to_string())?;
            s.remove(chunk_id);
            Ok(())
        }
    }

    #[test]
    fn gc_skips_live_and_inflight_and_deletes_orphans() {
        let live = Live {
            refs: Mutex::new(HashSet::from(["c1".to_string()])),
        };
        let inflight = Inflight {
            refs: Mutex::new(HashSet::from(["c2".to_string()])),
        };
        let store = Store {
            chunks: Mutex::new(HashSet::from([
                "c1".to_string(),
                "c2".to_string(),
                "c3".to_string(),
            ])),
        };

        let gc = GarbageCollector::new(live, inflight, store, Duration::from_secs(0));
        let report = gc.sweep_once().expect("sweep should succeed");

        assert_eq!(report.deleted, 1);
        assert_eq!(report.candidates, 1);
    }

    #[test]
    fn enqueue_is_non_blocking_until_worker_runs() {
        let live = Live {
            refs: Mutex::new(HashSet::new()),
        };
        let inflight = Inflight {
            refs: Mutex::new(HashSet::new()),
        };
        let store = Store {
            chunks: Mutex::new(HashSet::from(["c1".to_string()])),
        };
        let gc = GarbageCollector::new(live, inflight, store, Duration::from_secs(0));

        gc.enqueue_candidate_scan().expect("enqueue should succeed");
        // No worker run yet; chunk remains present.
        assert_eq!(
            gc.store
                .all_chunk_ids()
                .expect("inventory should work")
                .len(),
            1
        );

        let reports = gc.run_enqueued().expect("worker should run");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].deleted, 1);
    }
}
