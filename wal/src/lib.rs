pub mod error;
pub mod log;
pub mod pipeline;
pub mod recovery;
pub mod types;

pub use error::WalError;
pub use log::{WalLog, WalTransaction};
pub use pipeline::{ChunkStore, MetadataCommit, MetadataDelete, WritePipeline, WriteResult};
pub use recovery::{RecoveryReport, WalRecovery};
pub use types::{new_transaction_id, OperationType, TxStatus, WalEntry};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::io::{Error, ErrorKind, Read, Seek, Write};
    use std::sync::{Arc, Mutex};

    struct InMemoryChunkStore {
        chunks: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl InMemoryChunkStore {
        fn new() -> Self {
            Self {
                chunks: Mutex::new(HashMap::new()),
            }
        }
    }

    impl ChunkStore for InMemoryChunkStore {
        fn put_chunk(&self, chunk_id: &str, data: &[u8]) -> Result<(), String> {
            let mut chunks = self.chunks.lock().map_err(|_| "poisoned lock".to_string())?;
            chunks.entry(chunk_id.to_string()).or_insert_with(|| data.to_vec());
            Ok(())
        }

        fn get_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>, String> {
            let chunks = self.chunks.lock().map_err(|_| "poisoned lock".to_string())?;
            Ok(chunks.get(chunk_id).cloned())
        }
    }

    struct FailSecondPutChunkStore {
        chunks: Mutex<HashMap<String, Vec<u8>>>,
        puts: Mutex<usize>,
    }

    impl FailSecondPutChunkStore {
        fn new() -> Self {
            Self {
                chunks: Mutex::new(HashMap::new()),
                puts: Mutex::new(0),
            }
        }
    }

    impl ChunkStore for FailSecondPutChunkStore {
        fn put_chunk(&self, chunk_id: &str, data: &[u8]) -> Result<(), String> {
            let mut puts = self.puts.lock().map_err(|_| "poisoned lock".to_string())?;
            *puts += 1;
            if *puts >= 2 {
                return Err("simulated chunk store failure".to_string());
            }
            let mut chunks = self.chunks.lock().map_err(|_| "poisoned lock".to_string())?;
            chunks.entry(chunk_id.to_string()).or_insert_with(|| data.to_vec());
            Ok(())
        }

        fn get_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>, String> {
            let chunks = self.chunks.lock().map_err(|_| "poisoned lock".to_string())?;
            Ok(chunks.get(chunk_id).cloned())
        }
    }

    struct InMemoryMetadata {
        files: Mutex<HashMap<String, (u64, String)>>,
        deleted: Mutex<HashSet<String>>,
    }

    impl InMemoryMetadata {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                deleted: Mutex::new(HashSet::new()),
            }
        }

        fn is_deleted(&self, file_path: &str) -> bool {
            self.deleted
                .lock()
                .map(|d| d.contains(file_path))
                .unwrap_or(false)
        }

        fn version(&self, file_path: &str) -> Option<u64> {
            self.files
                .lock()
                .ok()
                .and_then(|f| f.get(file_path).map(|(v, _)| *v))
        }
    }

    impl MetadataCommit for InMemoryMetadata {
        fn commit_write(
            &self,
            tx_id: &str,
            file_path: &str,
            expected_version: u64,
            _chunk_ids: &[String],
            _chunk_hashes: &[String],
        ) -> Result<(), String> {
            let mut files = self.files.lock().map_err(|_| "poisoned lock".to_string())?;
            let (current, last_tx) = files
                .get(file_path)
                .cloned()
                .unwrap_or((0_u64, String::new()));

            if last_tx == tx_id {
                return Ok(());
            }

            if current != expected_version {
                return Err("cas conflict".to_string());
            }

            files.insert(file_path.to_string(), (current + 1, tx_id.to_string()));
            let mut deleted = self.deleted.lock().map_err(|_| "poisoned lock".to_string())?;
            deleted.remove(file_path);
            Ok(())
        }
    }

    impl MetadataDelete for InMemoryMetadata {
        fn commit_delete(&self, tx_id: &str, file_path: &str, expected_version: u64) -> Result<(), String> {
            let mut files = self.files.lock().map_err(|_| "poisoned lock".to_string())?;
            let (current, last_tx) = files
                .get(file_path)
                .cloned()
                .unwrap_or((0_u64, String::new()));

            if last_tx == tx_id {
                return Ok(());
            }
            if current != expected_version {
                return Err("cas conflict".to_string());
            }

            files.insert(file_path.to_string(), (current + 1, tx_id.to_string()));
            let mut deleted = self.deleted.lock().map_err(|_| "poisoned lock".to_string())?;
            deleted.insert(file_path.to_string());
            Ok(())
        }
    }

    struct AlwaysConflictMetadata;

    impl MetadataCommit for AlwaysConflictMetadata {
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

    impl MetadataDelete for AlwaysConflictMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn wal_open_defaults_to_sync_writes() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        assert!(wal.sync_writes_enabled());
    }

    #[test]
    fn wal_open_with_sync_false_uses_buffered_mode() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open_with_sync(&wal_path, false).expect("wal should open");
        assert!(!wal.sync_writes_enabled());

        let txn = wal
            .begin_transaction("/buffered.txt", OperationType::Write, 0)
            .expect("transaction should be appended");
        wal.commit_transaction(&txn)
            .expect("commit should append in buffered mode");

        let entries = wal.read_all().expect("wal should be readable");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].status, TxStatus::Pending);
        assert_eq!(entries[1].status, TxStatus::Committed);
    }

    #[test]
    fn write_and_recover_committed_transaction() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let pipeline =
            WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4).expect("pipeline init");

        let payload = b"abcdefghijkl";
        let result = pipeline
            .write_stream("/a.txt", 0, &payload[..])
            .expect("write should commit");
        assert!(result.committed);

        let recovery = WalRecovery::new(pipeline.wal(), &chunks, &metadata);
        let report = recovery.recover().expect("recovery should succeed");
        assert!(report.committed_reapplied >= 1);
    }

    #[test]
    fn recover_pending_delete_transaction() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());

        // Seed committed file version 1 so delete with expected_version=1 is valid.
        metadata
            .commit_write("seed", "/dead.txt", 0, &[], &[])
            .expect("seed write should succeed");

        // Simulate crash after WAL begin for delete, before commit/abort marker.
        let _pending = wal
            .begin_transaction("/dead.txt", OperationType::Delete, 1)
            .expect("pending delete should be logged");

        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let report = recovery.recover().expect("recovery should succeed");
        assert_eq!(report.pending_aborted, 1);
        assert!(!metadata.is_deleted("/dead.txt"));
    }

    #[test]
    fn recover_pending_write_transaction_is_aborted_not_applied() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());

        let mut txn = wal
            .begin_transaction("/pending.txt", OperationType::Write, 0)
            .expect("pending write begin should be logged");
        let payload = b"pending";
        let chunk_id = blake3::hash(payload).to_hex().to_string();
        chunks
            .put_chunk(&chunk_id, payload)
            .expect("chunk should persist for recovery verification");
        wal.append_chunk(&mut txn, chunk_id.clone(), chunk_id.clone())
            .expect("pending write chunk snapshot should be logged");

        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let report = recovery.recover().expect("recovery should succeed");
        assert_eq!(report.pending_aborted, 1);
        assert_eq!(metadata.version("/pending.txt"), None);

        let entries = wal.read_all().expect("wal should be readable");
        let tx_id = txn.transaction_id().to_string();
        let latest = entries
            .iter()
            .rev()
            .find(|e| e.transaction_id == tx_id)
            .expect("latest tx entry should exist");
        assert_eq!(latest.status, TxStatus::Aborted);
    }

    struct FailAfterFirstChunkReader {
        data: Vec<u8>,
        pos: usize,
        failed: bool,
    }

    impl Read for FailAfterFirstChunkReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.failed {
                return Err(Error::new(ErrorKind::Interrupted, "simulated reader failure"));
            }
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let remaining = self.data.len() - self.pos;
            let n = remaining.min(buf.len());
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            self.failed = true;
            Ok(n)
        }
    }

    #[test]
    fn write_stream_reader_failure_is_aborted_and_not_replayed() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let pipeline =
            WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4).expect("pipeline init");

        let reader = FailAfterFirstChunkReader {
            data: b"abcdefgh".to_vec(),
            pos: 0,
            failed: false,
        };
        let err = pipeline
            .write_stream("/partial.txt", 0, reader)
            .expect_err("write should fail on reader error");
        assert!(matches!(err, WalError::Io(_)));

        let entries = pipeline.wal().read_all().expect("wal should be readable");
        let tx_id = entries
            .first()
            .map(|e| e.transaction_id.clone())
            .expect("tx should exist");
        let latest = entries
            .iter()
            .rev()
            .find(|e| e.transaction_id == tx_id)
            .expect("latest tx entry should exist");
        assert_eq!(latest.status, TxStatus::Aborted);

        let recovery = WalRecovery::new(pipeline.wal(), &chunks, &metadata);
        let _ = recovery.recover().expect("recovery should succeed");
        assert_eq!(metadata.version("/partial.txt"), None);
    }

    #[test]
    fn write_stream_chunk_store_failure_is_aborted_and_not_replayed() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(FailSecondPutChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let pipeline =
            WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4).expect("pipeline init");

        let err = pipeline
            .write_stream("/partial-chunks.txt", 0, &b"abcdefgh"[..])
            .expect_err("write should fail on chunk store error");
        assert!(matches!(err, WalError::ChunkStore(_)));

        let entries = pipeline.wal().read_all().expect("wal should be readable");
        let tx_id = entries
            .first()
            .map(|e| e.transaction_id.clone())
            .expect("tx should exist");
        let latest = entries
            .iter()
            .rev()
            .find(|e| e.transaction_id == tx_id)
            .expect("latest tx entry should exist");
        assert_eq!(latest.status, TxStatus::Aborted);

        let recovery = WalRecovery::new(pipeline.wal(), &chunks, &metadata);
        let _ = recovery.recover().expect("recovery should succeed");
        assert_eq!(metadata.version("/partial-chunks.txt"), None);
    }

    #[test]
    fn write_stream_cas_conflict_is_aborted_and_rejected() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(AlwaysConflictMetadata);
        let pipeline =
            WritePipeline::new(wal, Arc::clone(&chunks), Arc::clone(&metadata), 4).expect("pipeline init");

        let err = pipeline
            .write_stream("/conflict.txt", 0, &b"abcdefgh"[..])
            .expect_err("write should fail on cas conflict");
        assert!(matches!(err, WalError::Conflict));

        let entries = pipeline.wal().read_all().expect("wal should be readable");
        let tx_id = entries
            .first()
            .map(|e| e.transaction_id.clone())
            .expect("tx should exist");
        let latest = entries
            .iter()
            .rev()
            .find(|e| e.transaction_id == tx_id)
            .expect("latest tx entry should exist");
        assert_eq!(latest.status, TxStatus::Aborted);
    }

    #[test]
    fn oversized_wal_frame_is_rejected() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        // Raw frame with oversized payload length and no payload bytes.
        fs::write(&wal_path, u32::MAX.to_le_bytes()).expect("raw wal bytes should be written");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let err = wal.read_all().expect_err("oversized wal frame should fail closed");
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn pending_abort_marker_is_idempotent_across_recovery_runs() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());

        let txn = wal
            .begin_transaction_with_id(
                "tx-pending-idempotent".to_string(),
                "/pending.txt",
                OperationType::Write,
                0,
            )
            .expect("pending write begin should be logged");
        let tx_id = txn.transaction_id().to_string();

        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let first = recovery.recover().expect("first recovery should succeed");
        assert_eq!(first.pending_aborted, 1);

        let second = recovery.recover().expect("second recovery should succeed");
        assert_eq!(second.pending_aborted, 0);

        let err = match wal.begin_transaction_with_id(
            "tx-pending-idempotent".to_string(),
            "/pending.txt",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("finalized transaction id should not be reusable"),
            Err(err) => err,
        };
        match err {
            WalError::InvalidEntry(msg) => {
                assert_eq!(msg, "transaction already finalized");
            }
            other => panic!("unexpected error: {other}"),
        }

        let entries = wal.read_all().expect("wal should be readable");
        let aborted_count = entries
            .iter()
            .filter(|e| e.transaction_id == tx_id && e.status == TxStatus::Aborted)
            .count();
        assert_eq!(aborted_count, 1);
    }

    #[test]
    fn repair_truncated_tail_salvages_invalid_tail_frame() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let _txn = wal
            .begin_transaction_with_id(
                "tx-valid".to_string(),
                "/valid.txt",
                OperationType::Write,
                0,
            )
            .expect("valid tx should be appended");

        // Corrupt tail: oversized payload len marker without payload bytes.
        fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .expect("wal should be reopenable")
            .write_all(&u32::MAX.to_le_bytes())
            .expect("corrupt tail bytes should be appended");

        wal.repair_truncated_tail()
            .expect("tail repair should salvage invalid tail frame");
        let entries = wal.read_all().expect("wal should be readable after repair");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].transaction_id, "tx-valid");
        assert_eq!(entries[0].status, TxStatus::Pending);
    }

    #[test]
    fn repair_truncated_tail_salvages_non_json_tail_frame() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let _txn = wal
            .begin_transaction_with_id(
                "tx-valid-json".to_string(),
                "/valid.txt",
                OperationType::Write,
                0,
            )
            .expect("valid tx should be appended");

        // Corrupt tail: valid frame structure + checksum, but payload is not JSON.
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
            .expect("wal should be reopenable")
            .write_all(&raw)
            .expect("corrupt json tail frame should be appended");

        wal.repair_truncated_tail()
            .expect("tail repair should salvage non-json tail frame");
        let entries = wal.read_all().expect("wal should be readable after repair");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].transaction_id, "tx-valid-json");
        assert_eq!(entries[0].status, TxStatus::Pending);
    }

    #[test]
    fn duplicate_begin_transaction_id_is_rejected_while_active() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let _txn = wal
            .begin_transaction_with_id("tx-dup".to_string(), "/x", OperationType::Write, 0)
            .expect("first begin should work");

        let err = match wal.begin_transaction_with_id("tx-dup".to_string(), "/x", OperationType::Write, 0) {
            Ok(_) => panic!("duplicate active tx begin should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn append_after_commit_is_rejected() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let mut txn = wal
            .begin_transaction_with_id("tx-finalized".to_string(), "/x", OperationType::Write, 0)
            .expect("begin should work");
        wal.commit_transaction(&txn)
            .expect("commit marker should work");

        let err = wal
            .append_chunk(&mut txn, "c1".to_string(), "c1".to_string())
            .expect_err("append after commit should be rejected");
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn double_commit_is_rejected() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let txn = wal
            .begin_transaction_with_id("tx-double-commit".to_string(), "/x", OperationType::Write, 0)
            .expect("begin should work");

        wal.commit_transaction(&txn)
            .expect("first commit should work");
        let err = wal
            .commit_transaction(&txn)
            .expect_err("second commit should be rejected");
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn finalized_transaction_id_cannot_be_reused_after_commit() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let txn = wal
            .begin_transaction_with_id("tx-once".to_string(), "/x", OperationType::Write, 0)
            .expect("begin should work");
        wal.commit_transaction(&txn)
            .expect("commit should work");

        let err = match wal.begin_transaction_with_id("tx-once".to_string(), "/x", OperationType::Write, 1) {
            Ok(_) => panic!("reusing finalized tx id should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn finalized_transaction_id_cannot_be_reused_after_abort() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let txn = wal
            .begin_transaction_with_id("tx-aborted-once".to_string(), "/x", OperationType::Write, 0)
            .expect("begin should work");
        wal.abort_transaction(&txn)
            .expect("abort should work");

        let err = match wal.begin_transaction_with_id(
            "tx-aborted-once".to_string(),
            "/x",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("reusing finalized tx id should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn finalized_transaction_id_is_rejected_after_wal_reopen() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let txn = wal
                .begin_transaction_with_id("tx-reopen-finalized".to_string(), "/x", OperationType::Write, 0)
                .expect("begin should work");
            wal.commit_transaction(&txn)
                .expect("commit should work");
        }

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let err = match wal.begin_transaction_with_id(
            "tx-reopen-finalized".to_string(),
            "/x",
            OperationType::Write,
            1,
        ) {
            Ok(_) => panic!("reusing finalized tx id after reopen should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn pending_transaction_id_is_rejected_after_wal_reopen() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _txn = wal
                .begin_transaction_with_id("tx-reopen-pending".to_string(), "/x", OperationType::Write, 0)
                .expect("begin should work");
        }

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let err = match wal.begin_transaction_with_id(
            "tx-reopen-pending".to_string(),
            "/x",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("reusing pending tx id after reopen should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn corrupted_tail_requires_repair_before_mutation() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _txn = wal
                .begin_transaction_with_id("tx-valid".to_string(), "/x", OperationType::Write, 0)
                .expect("valid tx begin should work");
        }

        // Append a valid-checksum non-JSON tail frame.
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

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let err = match wal.begin_transaction_with_id(
            "tx-after-corrupt-open".to_string(),
            "/x",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("mutation should be blocked until repair"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));

        wal.repair_truncated_tail()
            .expect("tail repair should succeed");
        let _txn = wal
            .begin_transaction_with_id(
                "tx-after-repair".to_string(),
                "/x",
                OperationType::Write,
                0,
            )
            .expect("mutation should be allowed after repair");
    }

    #[test]
    fn recovery_repairs_corrupt_tail_and_allows_followup_mutations() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        // Seed WAL with a pending transaction.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _txn = wal
                .begin_transaction_with_id(
                    "tx-pending-before-corruption".to_string(),
                    "/pending.txt",
                    OperationType::Write,
                    0,
                )
                .expect("pending write begin should work");
        }

        // Append checksum-valid non-JSON tail frame.
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

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let report = recovery.recover().expect("recovery should succeed");
        assert_eq!(report.pending_aborted, 1);

        // Recovery runs repair first; WAL should accept new mutation afterwards.
        let _txn = wal
            .begin_transaction_with_id(
                "tx-after-recovery".to_string(),
                "/x",
                OperationType::Write,
                0,
            )
            .expect("mutation should be allowed after recovery repair");
    }

    #[test]
    fn repair_truncated_tail_fails_on_non_tail_corruption() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let _a = wal
            .begin_transaction_with_id("tx-a".to_string(), "/a", OperationType::Write, 0)
            .expect("first tx should append");
        let _b = wal
            .begin_transaction_with_id("tx-b".to_string(), "/b", OperationType::Write, 0)
            .expect("second tx should append");

        // Corrupt checksum byte of the first frame so corruption is non-tail.
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

        let repaired = WalLog::open(&wal_path).expect("wal reopen should work");
        let err = repaired
            .repair_truncated_tail()
            .expect_err("non-tail corruption should fail closed");
        assert!(matches!(err, WalError::ChecksumMismatch));
    }

    #[test]
    fn recovery_fails_on_non_tail_corruption() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        // Build WAL with at least two frames.
        let wal = WalLog::open(&wal_path).expect("wal should open");
        let _a = wal
            .begin_transaction_with_id("tx-a".to_string(), "/a", OperationType::Write, 0)
            .expect("first tx should append");
        let _b = wal
            .begin_transaction_with_id("tx-b".to_string(), "/b", OperationType::Write, 0)
            .expect("second tx should append");

        // Corrupt checksum byte of first frame so corruption is non-tail.
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

        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let err = match recovery.recover() {
            Ok(_) => panic!("recovery should fail on non-tail corruption"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::ChecksumMismatch));
    }

    #[test]
    fn non_tail_corruption_blocks_mutation_and_repair_fails() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _a = wal
                .begin_transaction_with_id("tx-a".to_string(), "/a", OperationType::Write, 0)
                .expect("first tx should append");
            let _b = wal
                .begin_transaction_with_id("tx-b".to_string(), "/b", OperationType::Write, 0)
                .expect("second tx should append");
        }

        // Corrupt checksum byte of first frame so corruption is non-tail.
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

        let corrupted = WalLog::open(&wal_path).expect("wal reopen should work");
        let begin_err = match corrupted.begin_transaction_with_id(
            "tx-after-corrupt-open".to_string(),
            "/x",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("mutation should be blocked before repair"),
            Err(err) => err,
        };
        assert!(matches!(begin_err, WalError::InvalidEntry(_)));

        let repair_err = match corrupted.repair_truncated_tail() {
            Ok(_) => panic!("repair must fail on non-tail corruption"),
            Err(err) => err,
        };
        assert!(matches!(repair_err, WalError::ChecksumMismatch));
    }

    #[test]
    fn failed_non_tail_repair_keeps_mutation_blocked() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");

        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _a = wal
                .begin_transaction_with_id("tx-a".to_string(), "/a", OperationType::Write, 0)
                .expect("first tx should append");
            let _b = wal
                .begin_transaction_with_id("tx-b".to_string(), "/b", OperationType::Write, 0)
                .expect("second tx should append");
        }

        // Corrupt checksum byte of first frame so corruption is non-tail.
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

        let corrupted = WalLog::open(&wal_path).expect("wal reopen should work");
        let repair_err = match corrupted.repair_truncated_tail() {
            Ok(_) => panic!("repair must fail on non-tail corruption"),
            Err(err) => err,
        };
        assert!(matches!(repair_err, WalError::ChecksumMismatch));

        let begin_err = match corrupted.begin_transaction_with_id(
            "tx-after-failed-repair".to_string(),
            "/x",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("mutation should remain blocked after failed repair"),
            Err(err) => err,
        };
        assert!(matches!(begin_err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn recovery_rejects_delete_entry_with_chunk_vectors() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let mut bad = WalEntry::new_pending(
            "tx-bad-delete".to_string(),
            "/x".to_string(),
            OperationType::Delete,
            0,
        );
        bad.chunk_ids.push("id-1".to_string());
        bad.chunk_hashes.push("id-1".to_string());
        let bad = bad.with_status(TxStatus::Committed);
        wal.append(&bad).expect("malformed delete frame should append");

        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);

        let err = match recovery.recover() {
            Ok(_) => panic!("recovery should fail on malformed delete entry"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn recovery_rejects_entry_with_empty_file_path() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let mut bad = WalEntry::new_pending(
            "tx-bad-path".to_string(),
            "".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(TxStatus::Committed);
        wal.append(&bad).expect("malformed frame should append");

        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let err = match recovery.recover() {
            Ok(_) => panic!("recovery should fail on empty file path"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn recovery_rejects_entry_with_relative_file_path() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let mut bad = WalEntry::new_pending(
            "tx-bad-rel-path".to_string(),
            "rel/path.txt".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(TxStatus::Committed);
        wal.append(&bad).expect("malformed frame should append");

        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let err = match recovery.recover() {
            Ok(_) => panic!("recovery should fail on relative file path"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn recovery_rejects_entry_with_nul_file_path() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let mut bad = WalEntry::new_pending(
            "tx-bad-nul-path".to_string(),
            "/bad\0path".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(TxStatus::Committed);
        wal.append(&bad).expect("malformed frame should append");

        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let err = match recovery.recover() {
            Ok(_) => panic!("recovery should fail on NUL file path"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn recovery_rejects_entry_with_empty_transaction_id() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let mut bad = WalEntry::new_pending(
            "".to_string(),
            "/x".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(TxStatus::Committed);
        wal.append(&bad).expect("malformed frame should append");

        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let err = match recovery.recover() {
            Ok(_) => panic!("recovery should fail on empty transaction id"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn recovery_rejects_entry_with_nul_transaction_id() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let mut bad = WalEntry::new_pending(
            "tx\0bad".to_string(),
            "/x".to_string(),
            OperationType::Write,
            0,
        );
        bad = bad.with_status(TxStatus::Committed);
        wal.append(&bad).expect("malformed frame should append");

        let chunks = Arc::new(InMemoryChunkStore::new());
        let metadata = Arc::new(InMemoryMetadata::new());
        let recovery = WalRecovery::new(&wal, &chunks, &metadata);
        let err = match recovery.recover() {
            Ok(_) => panic!("recovery should fail on NUL transaction id"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn read_all_does_not_break_subsequent_appends() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let _first = wal
            .begin_transaction_with_id("tx-first".to_string(), "/a", OperationType::Write, 0)
            .expect("first tx should append");
        let entries_before = wal.read_all().expect("read_all should succeed");
        assert_eq!(entries_before.len(), 1);

        let _second = wal
            .begin_transaction_with_id("tx-second".to_string(), "/b", OperationType::Write, 0)
            .expect("append after read_all should succeed");
        let entries_after = wal.read_all().expect("read_all should still succeed");
        assert_eq!(entries_after.len(), 2);
    }

    #[test]
    fn append_chunk_rejects_id_hash_mismatch() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut txn = wal
            .begin_transaction_with_id("tx-mismatch".to_string(), "/x", OperationType::Write, 0)
            .expect("begin should succeed");

        let err = wal
            .append_chunk(&mut txn, "id-a".to_string(), "id-b".to_string())
            .expect_err("append_chunk should reject mismatch");
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn append_chunk_rejects_empty_id_or_hash() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut txn = wal
            .begin_transaction_with_id("tx-empty".to_string(), "/x", OperationType::Write, 0)
            .expect("begin should succeed");

        let err = wal
            .append_chunk(&mut txn, "".to_string(), "".to_string())
            .expect_err("append_chunk should reject empty id/hash");
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn append_chunk_rejects_delete_transactions() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut txn = wal
            .begin_transaction_with_id("tx-del".to_string(), "/x", OperationType::Delete, 0)
            .expect("begin delete should succeed");

        let before = wal.read_all().expect("wal should be readable");
        assert_eq!(before.len(), 1, "only begin frame should exist");

        let err = wal
            .append_chunk(&mut txn, "id-ok".to_string(), "id-ok".to_string())
            .expect_err("append_chunk should be invalid for delete transactions");
        assert!(matches!(err, WalError::InvalidEntry(_)));

        let after_reject = wal.read_all().expect("wal should be readable");
        assert_eq!(
            after_reject.len(),
            1,
            "reject path must not append extra WAL frames"
        );

        wal.commit_transaction(&txn)
            .expect("delete transaction should still commit");
        let final_entries = wal.read_all().expect("wal should be readable");
        let latest = final_entries
            .iter()
            .rev()
            .find(|e| e.transaction_id == "tx-del")
            .expect("tx entries should exist");
        assert_eq!(latest.status, TxStatus::Committed);
        assert!(latest.chunk_ids.is_empty());
        assert!(latest.chunk_hashes.is_empty());
    }

    #[test]
    fn rejected_append_chunk_does_not_mutate_txn_or_wal_state() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut txn = wal
            .begin_transaction_with_id("tx-reject-preserve".to_string(), "/x", OperationType::Write, 0)
            .expect("begin should succeed");

        let before = wal.read_all().expect("wal should be readable");
        assert_eq!(before.len(), 1, "only initial pending entry should exist");

        let err = wal
            .append_chunk(&mut txn, "id-a".to_string(), "id-b".to_string())
            .expect_err("append_chunk should reject mismatch");
        assert!(matches!(err, WalError::InvalidEntry(_)));

        let after_reject = wal.read_all().expect("wal should still be readable");
        assert_eq!(
            after_reject.len(),
            1,
            "reject path must not append any additional WAL frame"
        );

        // Transaction should still be usable for a valid chunk append and commit.
        wal.append_chunk(&mut txn, "id-ok".to_string(), "id-ok".to_string())
            .expect("valid append should still work");
        wal.commit_transaction(&txn)
            .expect("commit should still work");

        let final_entries = wal.read_all().expect("wal should be readable");
        let latest = final_entries
            .iter()
            .rev()
            .find(|e| e.transaction_id == "tx-reject-preserve")
            .expect("tx entries should exist");
        assert_eq!(latest.status, TxStatus::Committed);
        assert_eq!(latest.chunk_ids, vec!["id-ok".to_string()]);
        assert_eq!(latest.chunk_hashes, vec!["id-ok".to_string()]);
    }

    #[test]
    fn begin_transaction_rejects_empty_transaction_id() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let err = match wal.begin_transaction_with_id("".to_string(), "/x", OperationType::Write, 0) {
            Ok(_) => panic!("empty tx id should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn begin_transaction_rejects_nul_in_transaction_id_and_does_not_reserve_it() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let err = match wal.begin_transaction_with_id(
            "tx\0bad".to_string(),
            "/x",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("NUL tx id should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));

        // Validation failure must not reserve tx id.
        let _txn = wal
            .begin_transaction_with_id("tx-good".to_string(), "/x", OperationType::Write, 0)
            .expect("valid tx id should still begin after validation failure");
    }

    #[test]
    fn begin_transaction_rejects_empty_file_path() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let err = match wal.begin_transaction_with_id(
            "tx-empty-path".to_string(),
            "",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("empty path should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn begin_transaction_empty_path_failure_does_not_reserve_txid() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let err = match wal.begin_transaction_with_id(
            "tx-empty-path-retry".to_string(),
            "",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("empty path should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));

        // Validation failure must not reserve tx id as active/finalized.
        let _txn = wal
            .begin_transaction_with_id(
                "tx-empty-path-retry".to_string(),
                "/good.txt",
                OperationType::Write,
                0,
            )
            .expect("same tx id should still be usable after validation failure");
    }

    #[test]
    fn begin_transaction_rejects_relative_file_path() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let err = match wal.begin_transaction_with_id(
            "tx-relative-path".to_string(),
            "relative/path.txt",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("relative path should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));
    }

    #[test]
    fn begin_transaction_relative_path_failure_does_not_reserve_txid() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let err = match wal.begin_transaction_with_id(
            "tx-relative-retry".to_string(),
            "relative/path.txt",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("relative path should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));

        // Validation failure must not reserve tx id as active/finalized.
        let _txn = wal
            .begin_transaction_with_id(
                "tx-relative-retry".to_string(),
                "/good.txt",
                OperationType::Write,
                0,
            )
            .expect("same tx id should still be usable after validation failure");
    }

    #[test]
    fn begin_transaction_rejects_nul_in_file_path_and_does_not_reserve_txid() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let wal_path = temp.path().join("wal.log");
        let wal = WalLog::open(&wal_path).expect("wal should open");

        let err = match wal.begin_transaction_with_id(
            "tx-nul-path".to_string(),
            "/bad\0path",
            OperationType::Write,
            0,
        ) {
            Ok(_) => panic!("NUL path should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, WalError::InvalidEntry(_)));

        // Validation failure must not reserve tx id as active/finalized.
        let _txn = wal
            .begin_transaction_with_id("tx-nul-path".to_string(), "/good.txt", OperationType::Write, 0)
            .expect("same tx id should still be usable after validation failure");
    }
}
