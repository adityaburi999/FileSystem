use cache::{CacheKey, ChunkCache};
use gc::{GcReport, GcTrigger, GcWorker};
use metadata::MetadataRead;
use staging::StagingManager;
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use thiserror::Error;
use wal::{
    new_transaction_id, ChunkStore, MetadataCommit, MetadataDelete, OperationType, RecoveryReport,
    WalError, WalRecovery, WritePipeline, WriteResult,
};

const MAX_DELETE_CAS_RETRIES: usize = 3;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("mount gate closed: recovery must complete before service use")]
    MountGateClosed,

    #[error("invalid range")]
    InvalidRange,

    #[error("path not found")]
    NotFound,

    #[error("data integrity mismatch")]
    IntegrityMismatch,

    #[error("wal error: {0}")]
    Wal(#[from] WalError),

    #[error("metadata error: {0}")]
    Metadata(String),

    #[error("operation busy after conflict retry budget")]
    Busy,

    #[error("chunk store error: {0}")]
    ChunkStore(String),

    #[error("cache error: {0}")]
    Cache(String),

    #[error("staging error: {0}")]
    Staging(String),

    #[error("gc worker error: {0}")]
    Gc(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct FileSystemCore<C, M, K>
where
    C: ChunkStore,
    M: MetadataCommit + MetadataRead + MetadataDelete,
    K: ChunkCache,
{
    pipeline: WritePipeline<C, M>,
    cache: K,
    gc_trigger: Option<Arc<dyn GcTrigger>>,
    gc_worker: Option<Arc<dyn GcWorker>>,
    staging: Option<Arc<dyn StagingManager>>,
    mount_gate_open: AtomicBool,
}

impl<C, M, K> FileSystemCore<C, M, K>
where
    C: ChunkStore,
    M: MetadataCommit + MetadataRead + MetadataDelete,
    K: ChunkCache,
{
    pub fn new(pipeline: WritePipeline<C, M>, cache: K) -> Self {
        Self {
            pipeline,
            cache,
            gc_trigger: None,
            gc_worker: None,
            staging: None,
            mount_gate_open: AtomicBool::new(false),
        }
    }

    pub fn with_gc_trigger(mut self, gc_trigger: Arc<dyn GcTrigger>) -> Self {
        self.gc_trigger = Some(gc_trigger);
        self
    }

    pub fn with_gc_worker(mut self, gc_worker: Arc<dyn GcWorker>) -> Self {
        self.gc_worker = Some(gc_worker);
        self
    }

    pub fn with_staging(mut self, staging: Arc<dyn StagingManager>) -> Self {
        self.staging = Some(staging);
        self
    }

    pub fn startup_recover(&self) -> Result<RecoveryReport, CoreError> {
        let recovery = WalRecovery::new(
            self.pipeline.wal(),
            self.pipeline.chunk_store(),
            self.pipeline.metadata(),
        );
        let report = recovery.recover()?;
        if let Some(staging) = &self.staging {
            let _ = staging.purge_stale_uncommitted();
        }
        // Recovery event invalidates cache to avoid serving stale pre-recovery entries.
        self.cache
            .invalidate_all()
            .map_err(|e| CoreError::Cache(e.to_string()))?;
        self.mount_gate_open.store(true, Ordering::Release);
        Ok(report)
    }

    pub fn write_stream<R: Read>(
        &self,
        file_path: &str,
        expected_version: u64,
        reader: R,
    ) -> Result<WriteResult, CoreError> {
        self.ensure_mount_open()?;
        let tx_id = new_transaction_id();
        if let Some(staging) = &self.staging {
            staging
                .begin_slot(&tx_id, file_path)
                .map_err(|e| CoreError::Staging(e.to_string()))?;
        }

        let result = self
            .pipeline
            .write_stream_with_tx_id(file_path, expected_version, tx_id.clone(), reader)
            .map_err(|e| {
                if let Some(staging) = &self.staging {
                    let _ = staging.cleanup_slot(&tx_id);
                }
                match e {
                    WalError::Conflict => CoreError::Busy,
                    _ => CoreError::Wal(e),
                }
            })?;

        if let Some(staging) = &self.staging {
            let _ = staging.mark_committed(&tx_id);
            let _ = staging.cleanup_slot(&tx_id);
        }

        // Invalidate stale cache entries on version change.
        let _ = self.cache.invalidate_path(file_path);
        Ok(result)
    }

    pub fn read_range(
        &self,
        file_path: &str,
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>, CoreError> {
        self.ensure_mount_open()?;

        let view = self
            .pipeline
            .metadata()
            .read_committed(file_path)
            .map_err(CoreError::Metadata)?
            .ok_or(CoreError::NotFound)?;

        if view.chunk_ids.len() != view.chunk_hashes.len() {
            return Err(CoreError::IntegrityMismatch);
        }

        let mut bytes = Vec::new();
        for (chunk_id, expected_hash) in view.chunk_ids.iter().zip(view.chunk_hashes.iter()) {
            let cache_key = CacheKey {
                file_path: file_path.to_string(),
                version: view.version,
                chunk_id: chunk_id.clone(),
            };
            let (chunk, cache_miss_fetched) = match self
                .cache
                .get(&cache_key)
                .map_err(|e| CoreError::Cache(e.to_string()))?
            {
                Some(bytes) => (bytes, false),
                None => {
                    let fetched = self
                        .pipeline
                        .chunk_store()
                        .get_chunk(chunk_id)
                        .map_err(CoreError::ChunkStore)?
                        .ok_or(CoreError::IntegrityMismatch)?;
                    (fetched, true)
                }
            };

            let observed = blake3::hash(&chunk).to_hex().to_string();
            if &observed != expected_hash || &observed != chunk_id {
                return Err(CoreError::IntegrityMismatch);
            }

            if cache_miss_fetched {
                // Cache fill is allowed only after integrity verification.
                self.cache
                    .put(cache_key, chunk.clone())
                    .map_err(|e| CoreError::Cache(e.to_string()))?;
            }

            bytes.extend_from_slice(&chunk);
        }

        let total = bytes.len();
        if offset > total {
            return Err(CoreError::InvalidRange);
        }

        let end = offset.saturating_add(size).min(total);
        let range: Range<usize> = offset..end;
        Ok(bytes[range].to_vec())
    }

    pub fn unlink(&self, file_path: &str, expected_version: u64) -> Result<(), CoreError> {
        self.ensure_mount_open()?;
        if self
            .pipeline
            .metadata()
            .read_committed(file_path)
            .map_err(CoreError::Metadata)?
            .is_none()
        {
            return Err(CoreError::NotFound);
        }

        let mut expected = expected_version;
        for attempt in 0..=MAX_DELETE_CAS_RETRIES {
            let tx_id = new_transaction_id();
            if let Some(staging) = &self.staging {
                staging
                    .begin_slot(&tx_id, file_path)
                    .map_err(|e| CoreError::Staging(e.to_string()))?;
            }

            let txn = self.pipeline.wal().begin_transaction_with_id(
                tx_id.clone(),
                file_path,
                OperationType::Delete,
                expected,
            )?;
            let tx_id = txn.transaction_id().to_string();

            match self
                .pipeline
                .metadata()
                .commit_delete(&tx_id, file_path, expected)
            {
                Ok(()) => {
                    self.pipeline.wal().commit_transaction(&txn)?;
                    if let Some(staging) = &self.staging {
                        let _ = staging.mark_committed(&tx_id);
                        let _ = staging.cleanup_slot(&tx_id);
                    }
                    let _ = self.cache.invalidate_path(file_path);

                    if let Some(trigger) = &self.gc_trigger {
                        // GC enqueue is best-effort and must not block/delete foreground correctness.
                        let _ = trigger.enqueue_candidate_scan();
                    }
                    return Ok(());
                }
                Err(e) => {
                    self.pipeline.wal().abort_transaction(&txn)?;
                    if let Some(staging) = &self.staging {
                        let _ = staging.cleanup_slot(&tx_id);
                    }

                    if is_cas_conflict(&e) && attempt < MAX_DELETE_CAS_RETRIES {
                        expected = self.latest_visible_version(file_path)?;
                        // Exponential backoff for contention handling.
                        let backoff_ms = 1_u64 << attempt;
                        thread::sleep(Duration::from_millis(backoff_ms));
                        continue;
                    }

                    if is_cas_conflict(&e) {
                        return Err(CoreError::Busy);
                    }
                    return Err(CoreError::Metadata(e));
                }
            }
        }
        Err(CoreError::Busy)
    }

    pub fn run_background_once(&self) -> Result<Vec<GcReport>, CoreError> {
        if let Some(worker) = &self.gc_worker {
            return worker
                .run_enqueued_once()
                .map_err(|e| CoreError::Gc(e.to_string()));
        }
        Ok(Vec::new())
    }

    pub fn is_mount_open(&self) -> bool {
        self.mount_gate_open.load(Ordering::Acquire)
    }

    fn ensure_mount_open(&self) -> Result<(), CoreError> {
        if !self.mount_gate_open.load(Ordering::Acquire) {
            return Err(CoreError::MountGateClosed);
        }
        Ok(())
    }

    fn latest_visible_version(&self, file_path: &str) -> Result<u64, CoreError> {
        let view = self
            .pipeline
            .metadata()
            .read_committed(file_path)
            .map_err(CoreError::Metadata)?;
        match view {
            Some(view) => Ok(view.version),
            None => Err(CoreError::NotFound),
        }
    }
}

fn is_cas_conflict(message: &str) -> bool {
    message.contains("cas conflict")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache::{CacheError, TwoTierChunkCache};
    use chunk_store::FsChunkStore;
    use gc::{GcReport, GcTrigger, GcWorker};
    use metadata::{InMemoryMetadataHook, ReadView};
    use staging::{InMemoryStaging, StagingError, StagingManager};
    use std::fs;
    use std::io::{Seek, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use wal::{WalLog, WritePipeline};

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

    struct QueueGc {
        queued: AtomicUsize,
    }

    impl QueueGc {
        fn new() -> Self {
            Self {
                queued: AtomicUsize::new(0),
            }
        }
    }

    impl GcTrigger for QueueGc {
        fn enqueue_candidate_scan(&self) -> Result<(), String> {
            self.queued.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    impl GcWorker for QueueGc {
        fn run_enqueued_once(&self) -> Result<Vec<GcReport>, String> {
            let runs = self.queued.swap(0, AtomicOrdering::SeqCst);
            let mut reports = Vec::new();
            for _ in 0..runs {
                reports.push(GcReport {
                    candidates: 0,
                    deleted: 0,
                    deferred: 0,
                });
            }
            Ok(reports)
        }
    }

    struct FailingInvalidateCache;

    impl ChunkCache for FailingInvalidateCache {
        fn get(&self, _key: &CacheKey) -> Result<Option<Vec<u8>>, CacheError> {
            Ok(None)
        }

        fn put(&self, _key: CacheKey, _value: Vec<u8>) -> Result<(), CacheError> {
            Ok(())
        }

        fn invalidate_path(&self, _file_path: &str) -> Result<(), CacheError> {
            Err(CacheError::Poisoned)
        }

        fn invalidate_all(&self) -> Result<(), CacheError> {
            Ok(())
        }
    }

    struct FailingInvalidateAllCache;

    impl ChunkCache for FailingInvalidateAllCache {
        fn get(&self, _key: &CacheKey) -> Result<Option<Vec<u8>>, CacheError> {
            Ok(None)
        }

        fn put(&self, _key: CacheKey, _value: Vec<u8>) -> Result<(), CacheError> {
            Ok(())
        }

        fn invalidate_path(&self, _file_path: &str) -> Result<(), CacheError> {
            Ok(())
        }

        fn invalidate_all(&self) -> Result<(), CacheError> {
            Err(CacheError::Poisoned)
        }
    }

    struct FailingPostCommitStaging;

    impl StagingManager for FailingPostCommitStaging {
        fn begin_slot(&self, _tx_id: &str, _path: &str) -> Result<(), StagingError> {
            Ok(())
        }

        fn mark_committed(&self, _tx_id: &str) -> Result<(), StagingError> {
            Err(StagingError::Poisoned)
        }

        fn cleanup_slot(&self, _tx_id: &str) -> Result<(), StagingError> {
            Err(StagingError::Poisoned)
        }

        fn purge_stale_uncommitted(&self) -> Result<usize, StagingError> {
            Ok(0)
        }
    }

    struct MissingChunkStore;

    impl ChunkStore for MissingChunkStore {
        fn put_chunk(&self, _chunk_id: &str, _data: &[u8]) -> Result<(), String> {
            Ok(())
        }

        fn get_chunk(&self, _chunk_id: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(None)
        }
    }

    struct StaticReadMetadata;

    impl MetadataCommit for StaticReadMetadata {
        fn commit_write(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
            _chunk_ids: &[String],
            _chunk_hashes: &[String],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl MetadataDelete for StaticReadMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl MetadataRead for StaticReadMetadata {
        fn read_committed(&self, file_path: &str) -> Result<Option<ReadView>, String> {
            if file_path == "/missing-chunk" {
                return Ok(Some(ReadView {
                    version: 1,
                    chunk_ids: vec!["abcd".to_string()],
                    chunk_hashes: vec!["abcd".to_string()],
                }));
            }
            Ok(None)
        }
    }

    struct AlwaysConflictDeleteMetadata;

    impl MetadataCommit for AlwaysConflictDeleteMetadata {
        fn commit_write(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
            _chunk_ids: &[String],
            _chunk_hashes: &[String],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl MetadataDelete for AlwaysConflictDeleteMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Err("cas conflict".to_string())
        }
    }

    impl MetadataRead for AlwaysConflictDeleteMetadata {
        fn read_committed(&self, _file_path: &str) -> Result<Option<ReadView>, String> {
            Ok(Some(ReadView {
                version: 1,
                chunk_ids: Vec::new(),
                chunk_hashes: Vec::new(),
            }))
        }
    }

    struct AlwaysConflictWriteMetadata;

    impl MetadataCommit for AlwaysConflictWriteMetadata {
        fn commit_write(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
            _chunk_ids: &[String],
            _chunk_hashes: &[String],
        ) -> Result<(), String> {
            Err("cas conflict".to_string())
        }
    }

    impl MetadataDelete for AlwaysConflictWriteMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl MetadataRead for AlwaysConflictWriteMetadata {
        fn read_committed(&self, _file_path: &str) -> Result<Option<ReadView>, String> {
            Ok(Some(ReadView {
                version: 0,
                chunk_ids: Vec::new(),
                chunk_hashes: Vec::new(),
            }))
        }
    }

    #[test]
    fn startup_recovery_opens_mount_gate_and_allows_read_write() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");

        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);
        assert!(!core.is_mount_open());

        core.startup_recover().expect("recovery should succeed");
        assert!(core.is_mount_open());

        core.write_stream("/a", 0, &b"abcdefgh"[..])
            .expect("write should succeed");

        let got = core.read_range("/a", 2, 3).expect("read should succeed");
        assert_eq!(got, b"cde");

        core.unlink("/a", 1).expect("unlink should succeed");
        let err = core.read_range("/a", 0, 1).expect_err("tombstoned file should be hidden");
        assert!(matches!(err, CoreError::NotFound));
    }

    #[test]
    fn operations_fail_closed_before_startup_recovery() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let write_err = core
            .write_stream("/gated.txt", 0, &b"blocked"[..])
            .expect_err("write must fail before startup recovery");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/gated.txt", 0, 1)
            .expect_err("read must fail before startup recovery");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/gated.txt", 0)
            .expect_err("unlink must fail before startup recovery");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn committed_write_recovers_after_restart() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // First runtime instance: write committed data.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let chunks =
                Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
            let metadata = Arc::new(InMemoryMetadataHook::new());
            let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
                .expect("pipeline should initialize");
            let cache = TwoTierChunkCache::new(64, 256);
            let core = FileSystemCore::new(pipeline, cache);

            core.startup_recover().expect("recovery should succeed");
            core.write_stream("/persist.txt", 0, &b"durable-by-wal"[..])
                .expect("write should succeed");
        }

        // Second runtime instance: fresh metadata process state, same WAL + chunk store.
        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should reopen"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover()
            .expect("recovery should rebuild metadata");
        let got = core
            .read_range("/persist.txt", 0, "durable-by-wal".len())
            .expect("recovered file should be readable");
        assert_eq!(got, b"durable-by-wal");
    }

    #[test]
    fn unlink_enqueues_gc_scan_when_trigger_present() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");

        let cache = TwoTierChunkCache::new(64, 256);
        let trigger = Arc::new(CountingTrigger::new());
        let core = FileSystemCore::new(pipeline, cache).with_gc_trigger(trigger.clone());

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/a", 0, &b"abcdefgh"[..])
            .expect("write should succeed");
        core.unlink("/a", 1).expect("unlink should succeed");

        assert_eq!(trigger.calls.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn staging_slots_are_cleaned_after_write_and_unlink() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");

        let cache = TwoTierChunkCache::new(64, 256);
        let staging = Arc::new(InMemoryStaging::new(16));
        let core = FileSystemCore::new(pipeline, cache).with_staging(staging.clone());

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/a", 0, &b"abcdefgh"[..])
            .expect("write should succeed");
        core.unlink("/a", 1).expect("unlink should succeed");

        assert_eq!(staging.active_slots().expect("slots count should work"), 0);
    }

    #[test]
    fn run_background_once_drains_enqueued_gc_work() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");

        let cache = TwoTierChunkCache::new(64, 256);
        let queue_gc = Arc::new(QueueGc::new());
        let core = FileSystemCore::new(pipeline, cache)
            .with_gc_trigger(queue_gc.clone())
            .with_gc_worker(queue_gc.clone());

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/a", 0, &b"abcdefgh"[..])
            .expect("write should succeed");
        core.unlink("/a", 1).expect("unlink should succeed");

        let reports = core.run_background_once().expect("gc worker run should succeed");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].deleted, 0);
        assert_eq!(
            queue_gc.queued.load(AtomicOrdering::SeqCst),
            0,
            "queue should be drained"
        );
    }

    #[test]
    fn startup_recover_aborts_pending_write_and_keeps_it_invisible() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());

        // Simulate crash state:
        // WAL has a pending write txn with persisted chunk refs, but metadata CAS not yet applied.
        let tx_id = "tx-recover-pending".to_string();
        let mut txn = wal
            .begin_transaction_with_id(tx_id, "/replay.txt", OperationType::Write, 0)
            .expect("wal begin should work");
        let payload = b"replayed";
        let chunk_hash = blake3::hash(payload).to_hex().to_string();
        chunks
            .put_chunk(&chunk_hash, payload)
            .expect("chunk should persist");
        wal.append_chunk(&mut txn, chunk_hash.clone(), chunk_hash.clone())
            .expect("wal pending chunk append should work");

        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        // First startup recovery should abort pending txn; file must remain invisible.
        core.startup_recover().expect("first recovery should succeed");
        let err = core
            .read_range("/replay.txt", 0, payload.len())
            .expect_err("pending write should not be visible after recovery");
        assert!(matches!(err, CoreError::NotFound));

        // Second startup recovery should remain idempotent with same invisible state.
        core.startup_recover().expect("second recovery should succeed");
        let err_again = core
            .read_range("/replay.txt", 0, payload.len())
            .expect_err("pending write should remain invisible");
        assert!(matches!(err_again, CoreError::NotFound));
    }

    #[test]
    fn startup_recover_aborts_pending_delete_and_keeps_file_visible() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // First runtime: commit a file and leave a pending delete in WAL.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let chunks =
                Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
            let metadata = Arc::new(InMemoryMetadataHook::new());
            let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
                .expect("pipeline should initialize");
            let cache = TwoTierChunkCache::new(64, 256);
            let core = FileSystemCore::new(pipeline, cache);

            core.startup_recover().expect("recovery should succeed");
            core.write_stream("/keep.txt", 0, &b"abcdefgh"[..])
                .expect("write should succeed");

            let _pending_delete = core
                .pipeline
                .wal()
                .begin_transaction_with_id(
                    "tx-pending-delete".to_string(),
                    "/keep.txt",
                    OperationType::Delete,
                    1,
                )
                .expect("pending delete should be logged");
        }

        // Second runtime: recovery must abort pending delete and preserve visibility.
        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should reopen"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        let got = core
            .read_range("/keep.txt", 0, 8)
            .expect("file should remain visible after aborting pending delete");
        assert_eq!(got, b"abcdefgh");
    }

    #[test]
    fn startup_recover_failure_keeps_mount_gate_closed() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());

        // Create a committed WAL write that references a missing chunk.
        let mut txn = wal
            .begin_transaction_with_id(
                "tx-missing-chunk".to_string(),
                "/bad.txt",
                OperationType::Write,
                0,
            )
            .expect("wal begin should succeed");
        wal.append_chunk(&mut txn, "abcdef".to_string(), "abcdef".to_string())
            .expect("wal pending chunk append should succeed");
        wal.commit_transaction(&txn)
            .expect("wal commit marker should succeed");

        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("recovery should fail on committed missing chunk"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Wal(_)));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after recovery failure"
        );
    }

    #[test]
    fn ops_remain_mount_gated_after_missing_chunk_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());

        let mut txn = wal
            .begin_transaction_with_id(
                "tx-missing-chunk-ops".to_string(),
                "/bad.txt",
                OperationType::Write,
                0,
            )
            .expect("wal begin should succeed");
        wal.append_chunk(&mut txn, "abcdef".to_string(), "abcdef".to_string())
            .expect("wal pending chunk append should succeed");
        wal.commit_transaction(&txn)
            .expect("wal commit marker should succeed");

        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let startup_err = match core.startup_recover() {
            Ok(_) => panic!("recovery should fail on committed missing chunk"),
            Err(err) => err,
        };
        assert!(matches!(startup_err, CoreError::InvalidRange | CoreError::Wal(_) | CoreError::ChunkStore(_) | CoreError::Metadata(_) | CoreError::IntegrityMismatch));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after recovery failure"
        );

        let write_err = core
            .write_stream("/x", 0, &b"abc"[..])
            .expect_err("write should remain mount-gated after failed startup");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/x", 0, 1)
            .expect_err("read should remain mount-gated after failed startup");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/x", 0)
            .expect_err("unlink should remain mount-gated after failed startup");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn startup_recover_cache_invalidation_failure_keeps_mount_gate_closed() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = FailingInvalidateAllCache;
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail when invalidate_all fails"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Cache(_)));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed when startup cache invalidation fails"
        );
    }

    #[test]
    fn startup_recover_fails_on_non_tail_wal_corruption() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // Build WAL with at least two frames.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _tx_a = wal
                .begin_transaction_with_id("tx-a".to_string(), "/a.txt", OperationType::Write, 0)
                .expect("first tx should append");
            let _tx_b = wal
                .begin_transaction_with_id("tx-b".to_string(), "/b.txt", OperationType::Write, 0)
                .expect("second tx should append");
        }

        // Corrupt checksum of first frame so corruption is non-tail.
        let bytes = fs::read(&wal_path).expect("wal bytes should be readable");
        let payload_len = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .expect("len prefix should have 4 bytes"),
        ) as usize;
        let checksum_start = 4 + payload_len;
        let mut writer = fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .expect("wal should reopen for corruption");
        writer
            .seek(std::io::SeekFrom::Start(checksum_start as u64))
            .expect("seek to checksum should work");
        let corrupt_byte = bytes[checksum_start] ^ 0xFF;
        writer
            .write_all(&[corrupt_byte])
            .expect("single-byte corruption should be written");

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("recovery should fail on non-tail wal corruption"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Wal(WalError::ChecksumMismatch)));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after non-tail wal corruption"
        );
    }

    #[test]
    fn ops_remain_mount_gated_after_non_tail_wal_corruption_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // Build WAL with at least two frames.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _tx_a = wal
                .begin_transaction_with_id("tx-a".to_string(), "/a.txt", OperationType::Write, 0)
                .expect("first tx should append");
            let _tx_b = wal
                .begin_transaction_with_id("tx-b".to_string(), "/b.txt", OperationType::Write, 0)
                .expect("second tx should append");
        }

        // Corrupt checksum byte in first frame (non-tail corruption).
        let bytes = fs::read(&wal_path).expect("wal bytes should be readable");
        let payload_len = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .expect("len prefix should have 4 bytes"),
        ) as usize;
        let checksum_start = 4 + payload_len;
        let mut writer = fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .expect("wal should reopen for corruption");
        writer
            .seek(std::io::SeekFrom::Start(checksum_start as u64))
            .expect("seek to checksum should work");
        let corrupt_byte = bytes[checksum_start] ^ 0xFF;
        writer
            .write_all(&[corrupt_byte])
            .expect("single-byte corruption should be written");

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let startup_err = match core.startup_recover() {
            Ok(_) => panic!("recovery should fail on non-tail wal corruption"),
            Err(err) => err,
        };
        assert!(matches!(startup_err, CoreError::Wal(WalError::ChecksumMismatch)));

        let write_err = core
            .write_stream("/x", 0, &b"abc"[..])
            .expect_err("write should remain mount-gated after failed startup");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/x", 0, 1)
            .expect_err("read should remain mount-gated after failed startup");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/x", 0)
            .expect_err("unlink should remain mount-gated after failed startup");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn startup_recover_fails_on_malformed_delete_entry_with_chunks() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-delete".to_string(),
            "/bad-delete.txt".to_string(),
            OperationType::Delete,
            0,
        );
        bad.chunk_ids.push("id-1".to_string());
        bad.chunk_hashes.push("id-1".to_string());
        let bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed delete should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on malformed delete WAL entry"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Wal(WalError::InvalidEntry(_))));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after malformed delete WAL recovery failure"
        );
    }

    #[test]
    fn ops_remain_mount_gated_after_malformed_delete_entry_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-delete".to_string(),
            "/bad-delete.txt".to_string(),
            OperationType::Delete,
            0,
        );
        bad.chunk_ids.push("id-1".to_string());
        bad.chunk_hashes.push("id-1".to_string());
        let bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed delete should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let startup_err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on malformed delete entry"),
            Err(err) => err,
        };
        assert!(matches!(startup_err, CoreError::Wal(WalError::InvalidEntry(_))));

        let write_err = core
            .write_stream("/x", 0, &b"abc"[..])
            .expect_err("write should remain mount-gated after failed startup");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/x", 0, 1)
            .expect_err("read should remain mount-gated after failed startup");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/x", 0)
            .expect_err("unlink should remain mount-gated after failed startup");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn startup_recover_fails_on_empty_transaction_id_entry() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "".to_string(),
            "/bad-txid.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on empty transaction id entry"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Wal(WalError::InvalidEntry(_))));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after malformed tx-id WAL recovery failure"
        );
    }

    #[test]
    fn ops_remain_mount_gated_after_empty_transaction_id_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "".to_string(),
            "/bad-txid.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let startup_err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on empty transaction id entry"),
            Err(err) => err,
        };
        assert!(matches!(startup_err, CoreError::Wal(WalError::InvalidEntry(_))));

        let write_err = core
            .write_stream("/x", 0, &b"abc"[..])
            .expect_err("write should remain mount-gated after failed startup");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/x", 0, 1)
            .expect_err("read should remain mount-gated after failed startup");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/x", 0)
            .expect_err("unlink should remain mount-gated after failed startup");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn startup_recover_fails_on_nul_transaction_id_entry() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx\0bad".to_string(),
            "/bad-txid.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on NUL transaction id entry"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Wal(WalError::InvalidEntry(_))));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after malformed NUL tx-id WAL recovery failure"
        );
    }

    #[test]
    fn ops_remain_mount_gated_after_nul_transaction_id_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx\0bad".to_string(),
            "/bad-txid.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let startup_err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on NUL transaction id entry"),
            Err(err) => err,
        };
        assert!(matches!(startup_err, CoreError::Wal(WalError::InvalidEntry(_))));

        let write_err = core
            .write_stream("/x", 0, &b"abc"[..])
            .expect_err("write should remain mount-gated after failed startup");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/x", 0, 1)
            .expect_err("read should remain mount-gated after failed startup");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/x", 0)
            .expect_err("unlink should remain mount-gated after failed startup");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn startup_recover_fails_on_nul_file_path_entry() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-nul-path".to_string(),
            "/bad\0path.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on NUL file path entry"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Wal(WalError::InvalidEntry(_))));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after malformed NUL-path WAL recovery failure"
        );
    }

    #[test]
    fn ops_remain_mount_gated_after_nul_file_path_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-nul-path".to_string(),
            "/bad\0path.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let startup_err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on NUL file path entry"),
            Err(err) => err,
        };
        assert!(matches!(startup_err, CoreError::Wal(WalError::InvalidEntry(_))));

        let write_err = core
            .write_stream("/x", 0, &b"abc"[..])
            .expect_err("write should remain mount-gated after failed startup");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/x", 0, 1)
            .expect_err("read should remain mount-gated after failed startup");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/x", 0)
            .expect_err("unlink should remain mount-gated after failed startup");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn startup_recover_fails_on_relative_file_path_entry() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-rel-path".to_string(),
            "rel/path.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on relative file path entry"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::Wal(WalError::InvalidEntry(_))));
        assert!(
            !core.is_mount_open(),
            "mount gate must remain closed after malformed relative-path WAL recovery failure"
        );
    }

    #[test]
    fn ops_remain_mount_gated_after_relative_file_path_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-rel-path".to_string(),
            "rel/path.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let startup_err = match core.startup_recover() {
            Ok(_) => panic!("startup recovery should fail on relative file path entry"),
            Err(err) => err,
        };
        assert!(matches!(startup_err, CoreError::Wal(WalError::InvalidEntry(_))));

        let write_err = core
            .write_stream("/x", 0, &b"abc"[..])
            .expect_err("write should remain mount-gated after failed startup");
        assert!(matches!(write_err, CoreError::MountGateClosed));

        let read_err = core
            .read_range("/x", 0, 1)
            .expect_err("read should remain mount-gated after failed startup");
        assert!(matches!(read_err, CoreError::MountGateClosed));

        let unlink_err = core
            .unlink("/x", 0)
            .expect_err("unlink should remain mount-gated after failed startup");
        assert!(matches!(unlink_err, CoreError::MountGateClosed));
    }

    #[test]
    fn startup_recover_salvages_non_json_wal_tail() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // First runtime writes committed data, then WAL gets a non-JSON tail frame.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let chunks =
                Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
            let metadata = Arc::new(InMemoryMetadataHook::new());
            let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
                .expect("pipeline should initialize");
            let cache = TwoTierChunkCache::new(64, 256);
            let core = FileSystemCore::new(pipeline, cache);

            core.startup_recover().expect("recovery should succeed");
            core.write_stream("/tail.txt", 0, &b"abcdefgh"[..])
                .expect("write should succeed");
        }

        // Append a valid-checksum frame whose payload is not JSON.
        let payload = b"\x00\xFFnot-json".to_vec();
        let payload_len = u32::try_from(payload.len()).expect("payload length should fit u32");
        let checksum = blake3::hash(&payload);
        let mut raw = Vec::new();
        raw.extend_from_slice(&payload_len.to_le_bytes());
        raw.extend_from_slice(&payload);
        raw.extend_from_slice(checksum.as_bytes());
        fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .expect("wal should reopen for append")
            .write_all(&raw)
            .expect("corrupt tail frame should be appended");

        // Second runtime should salvage tail and still recover committed state.
        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should reopen"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover()
            .expect("recovery should salvage non-json tail");
        let got = core
            .read_range("/tail.txt", 0, 8)
            .expect("committed data should remain readable");
        assert_eq!(got, b"abcdefgh");
    }

    #[test]
    fn startup_recover_corrupt_tail_with_pending_txn_allows_subsequent_writes() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // First runtime: create pending WAL transaction, then append corrupt tail frame.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _pending = wal
                .begin_transaction_with_id(
                    "tx-pending-before-corrupt-tail".to_string(),
                    "/pending.txt",
                    OperationType::Write,
                    0,
                )
                .expect("pending tx should be logged");

            let payload = b"\x00\xFFnot-json".to_vec();
            let payload_len = u32::try_from(payload.len()).expect("payload length should fit u32");
            let checksum = blake3::hash(&payload);
            let mut raw = Vec::new();
            raw.extend_from_slice(&payload_len.to_le_bytes());
            raw.extend_from_slice(&payload);
            raw.extend_from_slice(checksum.as_bytes());
            fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .expect("wal should reopen for append")
                .write_all(&raw)
                .expect("corrupt tail frame should be appended");
        }

        // Second runtime: startup recovery should repair tail and abort pending txn.
        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        let report = core
            .startup_recover()
            .expect("recovery should repair corrupt tail and succeed");
        assert_eq!(report.pending_aborted, 1);
        assert!(core.is_mount_open(), "mount gate should open after successful recovery");

        core.write_stream("/after-recover.txt", 0, &b"abcdefgh"[..])
            .expect("write should succeed after recovery");
        let got = core
            .read_range("/after-recover.txt", 0, 8)
            .expect("written data should be readable");
        assert_eq!(got, b"abcdefgh");
    }

    #[test]
    fn unlink_retries_cas_conflict_with_latest_version() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/stale-unlink", 0, &b"abc"[..])
            .expect("write should succeed");

        // Stale expected_version=0 should conflict first, then retry with latest version and succeed.
        core.unlink("/stale-unlink", 0)
            .expect("unlink should succeed after bounded retry");
        let err = core
            .read_range("/stale-unlink", 0, 1)
            .expect_err("deleted file should not be visible");
        assert!(matches!(err, CoreError::NotFound));
    }

    #[test]
    fn unlink_returns_busy_after_retry_budget_exhausted() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(AlwaysConflictDeleteMetadata);
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        let err = core
            .unlink("/always-conflict", 0)
            .expect_err("unlink must fail after bounded retry budget");
        assert!(matches!(err, CoreError::Busy));
    }

    #[test]
    fn write_returns_busy_on_cas_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(AlwaysConflictWriteMetadata);
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        let err = core
            .write_stream("/always-conflict-write", 0, &b"abc"[..])
            .expect_err("write must fail on cas conflict");
        assert!(matches!(err, CoreError::Busy));
    }

    #[test]
    fn unlink_missing_returns_not_found_without_wal_append() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        let err = core
            .unlink("/does-not-exist", 0)
            .expect_err("unlink of missing file must fail");
        assert!(matches!(err, CoreError::NotFound));

        let wal_verify = WalLog::open(&wal_path).expect("wal should reopen");
        let entries = wal_verify.read_all().expect("wal read should succeed");
        assert!(
            entries.is_empty(),
            "missing unlink should not append transactional WAL records"
        );
    }

    #[test]
    fn write_succeeds_even_if_post_commit_staging_fails() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let staging = Arc::new(FailingPostCommitStaging);
        let core = FileSystemCore::new(pipeline, cache).with_staging(staging);

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/post-commit-staging", 0, &b"abc"[..])
            .expect("write must remain successful after durable commit");
        let got = core
            .read_range("/post-commit-staging", 0, 3)
            .expect("committed data should be readable");
        assert_eq!(got, b"abc");
    }

    #[test]
    fn unlink_succeeds_even_if_post_commit_staging_fails() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let staging = Arc::new(FailingPostCommitStaging);
        let core = FileSystemCore::new(pipeline, cache).with_staging(staging);

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/post-commit-unlink", 0, &b"abc"[..])
            .expect("seed write should succeed");
        core.unlink("/post-commit-unlink", 1)
            .expect("unlink must remain successful after durable commit");
        let err = core
            .read_range("/post-commit-unlink", 0, 1)
            .expect_err("deleted file should be hidden");
        assert!(matches!(err, CoreError::NotFound));
    }

    #[test]
    fn write_succeeds_even_if_post_commit_cache_invalidation_fails() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = FailingInvalidateCache;
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/cache-fail-write", 0, &b"abc"[..])
            .expect("write must remain successful after durable commit");
        let got = core
            .read_range("/cache-fail-write", 0, 3)
            .expect("committed data should be readable");
        assert_eq!(got, b"abc");
    }

    #[test]
    fn unlink_succeeds_even_if_post_commit_cache_invalidation_fails() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunk store should initialize"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = FailingInvalidateCache;
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        core.write_stream("/cache-fail-unlink", 0, &b"abc"[..])
            .expect("seed write should succeed");
        core.unlink("/cache-fail-unlink", 1)
            .expect("unlink must remain successful after durable commit");
        let err = core
            .read_range("/cache-fail-unlink", 0, 1)
            .expect_err("deleted file should be hidden");
        assert!(matches!(err, CoreError::NotFound));
    }

    #[test]
    fn read_with_missing_chunk_reports_integrity_mismatch() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(MissingChunkStore);
        let metadata = Arc::new(StaticReadMetadata);
        let pipeline = WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4)
            .expect("pipeline should initialize");
        let cache = TwoTierChunkCache::new(64, 256);
        let core = FileSystemCore::new(pipeline, cache);

        core.startup_recover().expect("recovery should succeed");
        let err = core
            .read_range("/missing-chunk", 0, 1)
            .expect_err("missing committed chunk must fail integrity path");
        assert!(matches!(err, CoreError::IntegrityMismatch));
    }
}
