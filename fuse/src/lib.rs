use cache::ChunkCache;
use fs_core::{CoreError, FileSystemCore};
use metadata::MetadataRead;
use path_resolver::{DefaultPathResolver, PathResolver};
use thiserror::Error;
use wal::{ChunkStore, MetadataCommit, MetadataDelete};

#[derive(Debug, Error)]
pub enum FuseError {
    #[error("invalid path")]
    InvalidPath,

    #[error("invalid read range")]
    InvalidRange,

    #[error("object not found")]
    NotFound,

    #[error("io error")]
    Io,

    #[error("conflict")]
    Conflict,

    #[error("service unavailable")]
    Unavailable,
}

pub struct FuseApi<C, M, K>
where
    C: ChunkStore,
    M: MetadataCommit + MetadataRead + MetadataDelete,
    K: ChunkCache,
{
    core: FileSystemCore<C, M, K>,
    resolver: DefaultPathResolver,
}

impl<C, M, K> FuseApi<C, M, K>
where
    C: ChunkStore,
    M: MetadataCommit + MetadataRead + MetadataDelete,
    K: ChunkCache,
{
    pub fn new(core: FileSystemCore<C, M, K>) -> Self {
        Self {
            core,
            resolver: DefaultPathResolver::new(),
        }
    }

    pub fn startup_recover(&self) -> Result<(), FuseError> {
        match self.core.startup_recover() {
            Ok(_) => Ok(()),
            Err(_) => Err(FuseError::Unavailable),
        }
    }

    pub fn open(&self, path: &str) -> Result<(), FuseError> {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        // Metadata authority check through zero-length read.
        self.core
            .read_range(&resolved.canonical_path, 0, 0)
            .map(|_| ())
            .map_err(map_core_error)
    }

    pub fn read(&self, path: &str, offset: usize, size: usize) -> Result<Vec<u8>, FuseError> {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .read_range(&resolved.canonical_path, offset, size)
            .map_err(map_core_error)
    }

    pub fn write(
        &self,
        path: &str,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_stream(&resolved.canonical_path, expected_version, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn unlink(&self, path: &str, expected_version: u64) -> Result<(), FuseError> {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .unlink(&resolved.canonical_path, expected_version)
            .map_err(map_core_error)
    }
}

fn map_core_error(error: CoreError) -> FuseError {
    match error {
        CoreError::MountGateClosed => FuseError::Unavailable,
        CoreError::InvalidRange => FuseError::InvalidRange,
        CoreError::NotFound => FuseError::NotFound,
        CoreError::IntegrityMismatch => FuseError::Io,
        CoreError::Busy => FuseError::Conflict,
        CoreError::Wal(_)
        | CoreError::ChunkStore(_)
        | CoreError::Cache(_)
        | CoreError::Gc(_)
        | CoreError::Staging(_)
        | CoreError::Io(_) => FuseError::Io,
        CoreError::Metadata(message) => {
            if message.contains("cas conflict") {
                FuseError::Conflict
            } else {
                FuseError::Io
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache::TwoTierChunkCache;
    use chunk_store::FsChunkStore;
    use metadata::{InMemoryMetadataHook, ReadView};
    use std::fs;
    use std::io::{Seek, Write};
    use std::sync::Arc;
    use wal::{WalLog, WritePipeline};

    struct AlwaysConflictWriteMetadata;

    impl wal::MetadataCommit for AlwaysConflictWriteMetadata {
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

    impl wal::MetadataDelete for AlwaysConflictWriteMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl metadata::MetadataRead for AlwaysConflictWriteMetadata {
        fn read_committed(&self, _file_path: &str) -> Result<Option<ReadView>, String> {
            Ok(Some(ReadView {
                version: 0,
                chunk_ids: Vec::new(),
                chunk_hashes: Vec::new(),
            }))
        }
    }

    struct AlwaysConflictDeleteMetadata;

    impl wal::MetadataCommit for AlwaysConflictDeleteMetadata {
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

    impl wal::MetadataDelete for AlwaysConflictDeleteMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Err("cas conflict".to_string())
        }
    }

    impl metadata::MetadataRead for AlwaysConflictDeleteMetadata {
        fn read_committed(&self, _file_path: &str) -> Result<Option<ReadView>, String> {
            Ok(Some(ReadView {
                version: 1,
                chunk_ids: vec!["dummy".to_string()],
                chunk_hashes: vec!["dummy".to_string()],
            }))
        }
    }

    #[test]
    fn fuse_api_validate_and_route() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        assert!(fuse.open("bad").is_err());
        assert!(fuse.read("/a/../b", 0, 1).is_err());

        fuse.write("/a", 0, b"hello").expect("write should pass");
        let got = fuse.read("/a", 1, 3).expect("read should pass");
        assert_eq!(got, b"ell");

        // Canonicalized alias path must resolve to the same object.
        let got_alias = fuse.read("//a", 1, 3).expect("canonicalized read should pass");
        assert_eq!(got_alias, b"ell");

        fuse.unlink("/a", 1).expect("unlink should pass");
        assert!(matches!(fuse.read("/a", 0, 1), Err(FuseError::NotFound)));
    }

    #[test]
    fn fuse_write_cas_conflict_maps_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(AlwaysConflictWriteMetadata);
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let err = fuse
            .write("/a", 0, b"hello")
            .expect_err("write should report conflict");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_unlink_cas_conflict_maps_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(AlwaysConflictDeleteMetadata);
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let err = fuse
            .unlink("/a", 1)
            .expect_err("unlink should report conflict");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_ops_are_unavailable_before_startup_recovery() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let open_err = fuse.open("/x").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/x", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/x", 0, b"data")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse.unlink("/x", 0).expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_failure_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        // Create a committed WAL write referencing a missing chunk.
        let mut txn = wal
            .begin_transaction_with_id(
                "tx-missing-chunk".to_string(),
                "/bad.txt",
                wal::OperationType::Write,
                0,
            )
            .expect("wal begin should succeed");
        wal.append_chunk(&mut txn, "abcdef".to_string(), "abcdef".to_string())
            .expect("wal pending chunk append should succeed");
        wal.commit_transaction(&txn)
            .expect("wal commit marker should succeed");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_non_tail_wal_corruption_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // Build WAL with at least two frames.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _tx_a = wal
                .begin_transaction_with_id(
                    "tx-a".to_string(),
                    "/a.txt",
                    wal::OperationType::Write,
                    0,
                )
                .expect("first tx should append");
            let _tx_b = wal
                .begin_transaction_with_id(
                    "tx-b".to_string(),
                    "/b.txt",
                    wal::OperationType::Write,
                    0,
                )
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

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on non-tail corruption");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_non_tail_wal_corruption_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // Build WAL with at least two frames.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _tx_a = wal
                .begin_transaction_with_id(
                    "tx-a".to_string(),
                    "/a.txt",
                    wal::OperationType::Write,
                    0,
                )
                .expect("first tx should append");
            let _tx_b = wal
                .begin_transaction_with_id(
                    "tx-b".to_string(),
                    "/b.txt",
                    wal::OperationType::Write,
                    0,
                )
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

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on non-tail corruption");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        // Create a committed WAL write referencing a missing chunk.
        let mut txn = wal
            .begin_transaction_with_id(
                "tx-missing-chunk-ops".to_string(),
                "/bad.txt",
                wal::OperationType::Write,
                0,
            )
            .expect("wal begin should succeed");
        wal.append_chunk(&mut txn, "abcdef".to_string(), "abcdef".to_string())
            .expect("wal pending chunk append should succeed");
        wal.commit_transaction(&txn)
            .expect("wal commit marker should succeed");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_malformed_delete_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-delete".to_string(),
            "/bad-delete.txt".to_string(),
            wal::OperationType::Delete,
            0,
        );
        bad.chunk_ids.push("id-1".to_string());
        bad.chunk_hashes.push("id-1".to_string());
        let bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed delete should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on malformed delete entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_malformed_delete_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-delete-ops".to_string(),
            "/bad-delete.txt".to_string(),
            wal::OperationType::Delete,
            0,
        );
        bad.chunk_ids.push("id-1".to_string());
        bad.chunk_hashes.push("id-1".to_string());
        let bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed delete should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on malformed delete entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_empty_transaction_id_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on empty transaction id entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_nul_transaction_id_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx\0bad".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL transaction id entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_nul_file_path_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-nul-path".to_string(),
            "/bad\0path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL file path entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_empty_transaction_id_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on empty transaction id entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_nul_transaction_id_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx\0bad".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL transaction id entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_nul_file_path_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-nul-path".to_string(),
            "/bad\0path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL file path entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_relative_file_path_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-rel-path".to_string(),
            "rel/path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on relative file path entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_relative_file_path_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-rel-path".to_string(),
            "rel/path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on relative file path entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }
}
