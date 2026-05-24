pub mod hooks;

use std::sync::Arc;
use wal::{MetadataCommit, MetadataDelete};

pub use hooks::{FileBackedMetadataHook, FileMetadataState, InMemoryMetadataHook, MetadataError};

pub struct ReadView {
    pub version: u64,
    pub chunk_ids: Vec<String>,
    pub chunk_hashes: Vec<String>,
}

pub trait MetadataRead {
    fn read_committed(&self, file_path: &str) -> Result<Option<ReadView>, String>;
}

impl MetadataCommit for InMemoryMetadataHook {
    fn commit_write(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
        chunk_ids: &[String],
        chunk_hashes: &[String],
    ) -> Result<(), String> {
        hooks::MetadataWalHook::commit_from_wal(
            self,
            tx_id,
            file_path,
            expected_version,
            chunk_ids,
            chunk_hashes,
        )
        .map_err(|e| e.to_string())
    }
}

impl MetadataCommit for FileBackedMetadataHook {
    fn commit_write(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
        chunk_ids: &[String],
        chunk_hashes: &[String],
    ) -> Result<(), String> {
        hooks::MetadataWalHook::commit_from_wal(
            self,
            tx_id,
            file_path,
            expected_version,
            chunk_ids,
            chunk_hashes,
        )
        .map_err(|e| e.to_string())
    }
}

impl MetadataDelete for InMemoryMetadataHook {
    fn commit_delete(&self, tx_id: &str, file_path: &str, expected_version: u64) -> Result<(), String> {
        hooks::MetadataDeleteHook::tombstone_from_wal(self, tx_id, file_path, expected_version)
            .map_err(|e| e.to_string())
    }
}

impl MetadataDelete for FileBackedMetadataHook {
    fn commit_delete(&self, tx_id: &str, file_path: &str, expected_version: u64) -> Result<(), String> {
        hooks::MetadataDeleteHook::tombstone_from_wal(self, tx_id, file_path, expected_version)
            .map_err(|e| e.to_string())
    }
}

impl MetadataRead for InMemoryMetadataHook {
    fn read_committed(&self, file_path: &str) -> Result<Option<ReadView>, String> {
        hooks::MetadataReadHook::read_committed(self, file_path)
            .map(|opt| {
                opt.map(|(version, chunk_ids, chunk_hashes)| ReadView {
                    version,
                    chunk_ids,
                    chunk_hashes,
                })
            })
            .map_err(|e| e.to_string())
    }
}

impl MetadataRead for FileBackedMetadataHook {
    fn read_committed(&self, file_path: &str) -> Result<Option<ReadView>, String> {
        hooks::MetadataReadHook::read_committed(self, file_path)
            .map(|opt| {
                opt.map(|(version, chunk_ids, chunk_hashes)| ReadView {
                    version,
                    chunk_ids,
                    chunk_hashes,
                })
            })
            .map_err(|e| e.to_string())
    }
}

impl<T: MetadataRead + ?Sized> MetadataRead for Arc<T> {
    fn read_committed(&self, file_path: &str) -> Result<Option<ReadView>, String> {
        (**self).read_committed(file_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn cas_and_idempotent_commit() {
        let meta = InMemoryMetadataHook::new();

        meta.commit_write("tx-1", "/x", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("first commit should pass");

        // Idempotent replay.
        meta.commit_write("tx-1", "/x", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("same tx replay should pass");

        let err = meta
            .commit_write("tx-2", "/x", 0, &["c2".to_string()], &["c2".to_string()])
            .expect_err("stale version should fail");
        assert!(err.contains("cas conflict"));
    }

    #[test]
    fn tombstone_hides_file_from_committed_reads() {
        let meta = InMemoryMetadataHook::new();
        meta.commit_write("tx-1", "/x", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("write should succeed");
        meta.commit_delete("tx-2", "/x", 1)
            .expect("delete should succeed");

        let view = MetadataRead::read_committed(&meta, "/x").expect("read should not fail");
        assert!(view.is_none());
    }

    #[test]
    fn file_backed_store_persists_across_reopen() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.json");

        {
            let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");
            meta.commit_write("tx-1", "/x", 0, &["c1".to_string()], &["c1".to_string()])
                .expect("write should succeed");
            meta.commit_delete("tx-2", "/x", 1)
                .expect("delete should succeed");
        }

        {
            let meta = FileBackedMetadataHook::open(&path).expect("metadata should reopen");
            let version = hooks::MetadataWalHook::current_version(&meta, "/x")
                .expect("version should be available");
            assert_eq!(version, 2);
            let view = MetadataRead::read_committed(&meta, "/x").expect("read should not fail");
            assert!(view.is_none());
        }
    }

    #[test]
    fn file_backed_store_rolls_back_memory_on_persist_failure() {
        let temp = tempdir().expect("temp dir should be created");
        let parent = temp.path().join("meta_parent");
        fs::create_dir_all(&parent).expect("parent dir should be created");
        let path = parent.join("metadata.json");

        let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");

        // Break persistence target: replace parent directory with a file.
        fs::remove_file(&path).expect("metadata file should be removable");
        fs::remove_dir(&parent).expect("parent dir should be removable");
        fs::write(&parent, b"not-a-directory").expect("parent path should become a file");

        let err = meta
            .commit_write("tx-fail", "/x", 0, &["c1".to_string()], &["c1".to_string()])
            .expect_err("commit should fail when persist path is invalid");
        assert!(err.contains("metadata io error"));

        // In-memory state must remain unchanged (rollback applied).
        let version = hooks::MetadataWalHook::current_version(&meta, "/x")
            .expect("version lookup should succeed");
        assert_eq!(version, 0);
        let view = MetadataRead::read_committed(&meta, "/x").expect("read should succeed");
        assert!(view.is_none());
    }

    #[test]
    fn file_backed_delete_rolls_back_memory_on_persist_failure() {
        let temp = tempdir().expect("temp dir should be created");
        let parent = temp.path().join("meta_parent");
        fs::create_dir_all(&parent).expect("parent dir should be created");
        let path = parent.join("metadata.json");

        let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");
        meta.commit_write("tx-1", "/x", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("seed write should succeed");

        // Break persistence target: replace parent directory with a file.
        fs::remove_file(&path).expect("metadata file should be removable");
        fs::remove_dir(&parent).expect("parent dir should be removable");
        fs::write(&parent, b"not-a-directory").expect("parent path should become a file");

        let err = meta
            .commit_delete("tx-fail-del", "/x", 1)
            .expect_err("delete should fail when persist path is invalid");
        assert!(err.contains("metadata io error"));

        // In-memory state must remain unchanged (no tombstone applied).
        let version = hooks::MetadataWalHook::current_version(&meta, "/x")
            .expect("version lookup should succeed");
        assert_eq!(version, 1);
        let view = MetadataRead::read_committed(&meta, "/x").expect("read should succeed");
        assert!(view.is_some());
    }
}
