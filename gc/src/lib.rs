use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};
use thiserror::Error;
use wal::TxStatus;

const DEFAULT_MAX_ENQUEUED_SCANS: usize = 64;

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

#[derive(Debug, Clone, Default)]
pub struct GcSchedulerMetrics {
    pub ticks: usize,
    pub trigger_errors: usize,
    pub worker_errors: usize,
    pub worker_runs: usize,
    pub deleted: usize,
    pub deferred: usize,
}

#[derive(Default)]
struct SchedulerCounters {
    ticks: AtomicUsize,
    trigger_errors: AtomicUsize,
    worker_errors: AtomicUsize,
    worker_runs: AtomicUsize,
    deleted: AtomicUsize,
    deferred: AtomicUsize,
}

pub struct BackgroundGcScheduler {
    trigger: Arc<dyn GcTrigger>,
    worker: Arc<dyn GcWorker>,
    interval: Duration,
    stop: Arc<AtomicBool>,
    counters: Arc<SchedulerCounters>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl BackgroundGcScheduler {
    pub fn new(trigger: Arc<dyn GcTrigger>, worker: Arc<dyn GcWorker>, interval: Duration) -> Self {
        Self {
            trigger,
            worker,
            interval: interval.max(Duration::from_millis(1)),
            stop: Arc::new(AtomicBool::new(false)),
            counters: Arc::new(SchedulerCounters::default()),
            thread: Mutex::new(None),
        }
    }

    pub fn start(&self) -> Result<(), String> {
        let mut slot = self.thread.lock().map_err(|_| "gc scheduler lock poisoned".to_string())?;
        if slot.is_some() {
            return Err("gc scheduler already running".to_string());
        }

        self.stop.store(false, Ordering::Release);
        let stop = Arc::clone(&self.stop);
        let trigger = Arc::clone(&self.trigger);
        let worker = Arc::clone(&self.worker);
        let counters = Arc::clone(&self.counters);
        let interval = self.interval;
        *slot = Some(thread::spawn(move || loop {
            thread::sleep(interval);
            if stop.load(Ordering::Acquire) {
                break;
            }

            counters.ticks.fetch_add(1, Ordering::SeqCst);
            if trigger.enqueue_candidate_scan().is_err() {
                counters.trigger_errors.fetch_add(1, Ordering::SeqCst);
                continue;
            }

            match worker.run_enqueued_once() {
                Ok(reports) => {
                    let runs = reports.len();
                    let mut deleted = 0;
                    let mut deferred = 0;
                    for report in reports {
                        deleted += report.deleted;
                        deferred += report.deferred;
                    }
                    counters.worker_runs.fetch_add(runs, Ordering::SeqCst);
                    counters.deleted.fetch_add(deleted, Ordering::SeqCst);
                    counters.deferred.fetch_add(deferred, Ordering::SeqCst);
                }
                Err(_) => {
                    counters.worker_errors.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
        Ok(())
    }

    pub fn stop(&self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        let handle = self
            .thread
            .lock()
            .map_err(|_| "gc scheduler lock poisoned".to_string())?
            .take();
        if let Some(handle) = handle {
            handle
                .join()
                .map_err(|_| "gc scheduler worker panicked".to_string())?;
        }
        self.stop.store(false, Ordering::Release);
        Ok(())
    }

    pub fn metrics(&self) -> Result<GcSchedulerMetrics, String> {
        let _lock = self
            .thread
            .lock()
            .map_err(|_| "gc scheduler lock poisoned".to_string())?;
        Ok(GcSchedulerMetrics {
            ticks: self.counters.ticks.load(Ordering::SeqCst),
            trigger_errors: self.counters.trigger_errors.load(Ordering::SeqCst),
            worker_errors: self.counters.worker_errors.load(Ordering::SeqCst),
            worker_runs: self.counters.worker_runs.load(Ordering::SeqCst),
            deleted: self.counters.deleted.load(Ordering::SeqCst),
            deferred: self.counters.deferred.load(Ordering::SeqCst),
        })
    }
}

impl Drop for BackgroundGcScheduler {
    fn drop(&mut self) {
        let _ = self.stop();
    }
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
    delete_budget: Option<usize>,
    max_enqueued_scans: usize,
    enqueued_scans: AtomicUsize,
    dropped_enqueues: AtomicUsize,
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
            delete_budget: None,
            max_enqueued_scans: DEFAULT_MAX_ENQUEUED_SCANS,
            enqueued_scans: AtomicUsize::new(0),
            dropped_enqueues: AtomicUsize::new(0),
            orphan_first_seen: Mutex::new(HashMap::new()),
            audit_log: Mutex::new(Vec::new()),
        }
    }

    pub fn with_delete_budget(mut self, max_deletes_per_sweep: usize) -> Self {
        self.delete_budget = Some(max_deletes_per_sweep.max(1));
        self
    }

    pub fn with_max_enqueued_scans(mut self, max_enqueued_scans: usize) -> Self {
        self.max_enqueued_scans = max_enqueued_scans.max(1);
        self
    }

    pub fn enqueued_scan_depth(&self) -> usize {
        self.enqueued_scans.load(Ordering::SeqCst)
    }

    pub fn dropped_enqueues(&self) -> usize {
        self.dropped_enqueues.load(Ordering::SeqCst)
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

        for (idx, chunk_id) in candidates.iter().enumerate() {
            if let Some(limit) = self.delete_budget {
                if deleted >= limit {
                    deferred += candidates.len().saturating_sub(idx);
                    break;
                }
            }

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
        // Non-blocking foreground hook: enqueue bounded background GC work.
        let mut queued = self.enqueued_scans.load(Ordering::Acquire);
        loop {
            if queued >= self.max_enqueued_scans {
                self.dropped_enqueues.fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
            match self.enqueued_scans.compare_exchange_weak(
                queued,
                queued + 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => queued = observed,
            }
        }
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

impl LiveRefSource for metadata::SqliteMetadataHook {
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
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::thread;

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
        assert_eq!(gc.store.all_chunk_ids().expect("inventory should work").len(), 1);

        let reports = gc.run_enqueued().expect("worker should run");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].deleted, 1);
    }

    #[test]
    fn delete_budget_limits_deletes_per_sweep() {
        let live = Live {
            refs: Mutex::new(HashSet::new()),
        };
        let inflight = Inflight {
            refs: Mutex::new(HashSet::new()),
        };
        let store = Store {
            chunks: Mutex::new(HashSet::from([
                "c1".to_string(),
                "c2".to_string(),
                "c3".to_string(),
            ])),
        };

        let gc = GarbageCollector::new(live, inflight, store, Duration::from_secs(0))
            .with_delete_budget(1);
        let report = gc.sweep_once().expect("sweep should succeed");
        assert_eq!(report.candidates, 3);
        assert_eq!(report.deleted, 1);
        assert_eq!(report.deferred, 2);

        let report2 = gc.sweep_once().expect("second sweep should succeed");
        assert_eq!(report2.deleted, 1);
        assert_eq!(report2.deferred, 1);

        let report3 = gc.sweep_once().expect("third sweep should succeed");
        assert_eq!(report3.deleted, 1);
        assert_eq!(report3.deferred, 0);
    }

    #[test]
    fn enqueue_is_bounded_by_max_pending_and_counts_drops() {
        let live = Live {
            refs: Mutex::new(HashSet::new()),
        };
        let inflight = Inflight {
            refs: Mutex::new(HashSet::new()),
        };
        let store = Store {
            chunks: Mutex::new(HashSet::from(["c1".to_string(), "c2".to_string()])),
        };
        let gc = GarbageCollector::new(live, inflight, store, Duration::from_secs(0))
            .with_max_enqueued_scans(2);

        for _ in 0..5 {
            gc.enqueue_candidate_scan().expect("enqueue should not fail");
        }

        assert_eq!(gc.enqueued_scan_depth(), 2);
        assert_eq!(gc.dropped_enqueues(), 3);

        let reports = gc.run_enqueued().expect("worker should run queued scans");
        assert_eq!(reports.len(), 2);
        assert_eq!(gc.enqueued_scan_depth(), 0);
    }

    struct CountingTrigger {
        calls: AtomicUsize,
    }

    impl CountingTrigger {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl GcTrigger for CountingTrigger {
        fn enqueue_candidate_scan(&self) -> Result<(), String> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    struct CountingWorker {
        calls: AtomicUsize,
    }

    impl CountingWorker {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl GcWorker for CountingWorker {
        fn run_enqueued_once(&self) -> Result<Vec<GcReport>, String> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(vec![GcReport {
                candidates: 2,
                deleted: 1,
                deferred: 1,
            }])
        }
    }

    #[test]
    fn scheduler_runs_trigger_and_worker_periodically() {
        let trigger = Arc::new(CountingTrigger::new());
        let worker = Arc::new(CountingWorker::new());
        let trigger_dyn: Arc<dyn GcTrigger> = trigger.clone();
        let worker_dyn: Arc<dyn GcWorker> = worker.clone();
        let scheduler = BackgroundGcScheduler::new(trigger_dyn, worker_dyn, Duration::from_millis(5));
        scheduler.start().expect("scheduler should start");

        for _ in 0..40 {
            if worker.calls.load(AtomicOrdering::SeqCst) >= 2 {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        scheduler.stop().expect("scheduler should stop");

        let trigger_calls = trigger.calls.load(AtomicOrdering::SeqCst);
        let worker_calls = worker.calls.load(AtomicOrdering::SeqCst);
        assert!(trigger_calls >= 2, "expected periodic trigger calls");
        assert!(worker_calls >= 2, "expected periodic worker calls");

        let metrics = scheduler.metrics().expect("metrics should be readable");
        assert!(metrics.ticks >= 2);
        assert_eq!(metrics.trigger_errors, 0);
        assert_eq!(metrics.worker_errors, 0);
        assert!(metrics.worker_runs >= 2);
        assert!(metrics.deleted >= 2);
        assert!(metrics.deferred >= 2);
    }

    #[test]
    fn scheduler_start_rejects_duplicate_running_loop() {
        let trigger = Arc::new(CountingTrigger::new());
        let worker = Arc::new(CountingWorker::new());
        let trigger_dyn: Arc<dyn GcTrigger> = trigger;
        let worker_dyn: Arc<dyn GcWorker> = worker;
        let scheduler = BackgroundGcScheduler::new(trigger_dyn, worker_dyn, Duration::from_millis(10));
        scheduler.start().expect("scheduler should start");
        let err = scheduler
            .start()
            .expect_err("scheduler should reject duplicate start");
        assert!(err.contains("already running"));
        scheduler.stop().expect("scheduler should stop");
    }
}
