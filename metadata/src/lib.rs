pub mod hooks;
pub mod sqlite;

use std::sync::Arc;
use wal::{MetadataCommit, MetadataDelete};

pub use hooks::{FileBackedMetadataHook, FileMetadataState, InMemoryMetadataHook, MetadataError};
pub use sqlite::SqliteMetadataHook;

pub struct ReadView {
    pub version: u64,
    pub chunk_ids: Vec<String>,
    pub chunk_hashes: Vec<String>,
}

pub trait MetadataRead {
    fn read_committed(&self, file_path: &str) -> Result<Option<ReadView>, String>;
}

pub trait MetadataNamespace {
    fn create_directory(&self, path: &str) -> Result<(), String>;
    fn list_children(&self, path: &str) -> Result<Vec<String>, String>;
    fn remove_directory(&self, path: &str) -> Result<(), String>;
    fn rename_path(&self, src: &str, dst: &str) -> Result<(), String>;
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

impl MetadataCommit for SqliteMetadataHook {
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

impl MetadataDelete for SqliteMetadataHook {
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

impl MetadataRead for SqliteMetadataHook {
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

impl MetadataNamespace for InMemoryMetadataHook {
    fn create_directory(&self, path: &str) -> Result<(), String> {
        InMemoryMetadataHook::create_directory(self, path).map_err(|e| e.to_string())
    }

    fn list_children(&self, path: &str) -> Result<Vec<String>, String> {
        InMemoryMetadataHook::list_children(self, path).map_err(|e| e.to_string())
    }

    fn remove_directory(&self, path: &str) -> Result<(), String> {
        InMemoryMetadataHook::remove_directory(self, path).map_err(|e| e.to_string())
    }

    fn rename_path(&self, src: &str, dst: &str) -> Result<(), String> {
        InMemoryMetadataHook::rename_path(self, src, dst).map_err(|e| e.to_string())
    }
}

impl MetadataNamespace for FileBackedMetadataHook {
    fn create_directory(&self, path: &str) -> Result<(), String> {
        FileBackedMetadataHook::create_directory(self, path).map_err(|e| e.to_string())
    }

    fn list_children(&self, path: &str) -> Result<Vec<String>, String> {
        FileBackedMetadataHook::list_children(self, path).map_err(|e| e.to_string())
    }

    fn remove_directory(&self, path: &str) -> Result<(), String> {
        FileBackedMetadataHook::remove_directory(self, path).map_err(|e| e.to_string())
    }

    fn rename_path(&self, src: &str, dst: &str) -> Result<(), String> {
        FileBackedMetadataHook::rename_path(self, src, dst).map_err(|e| e.to_string())
    }
}

impl MetadataNamespace for SqliteMetadataHook {
    fn create_directory(&self, path: &str) -> Result<(), String> {
        SqliteMetadataHook::create_directory(self, path).map_err(|e| e.to_string())
    }

    fn list_children(&self, path: &str) -> Result<Vec<String>, String> {
        SqliteMetadataHook::list_children(self, path).map_err(|e| e.to_string())
    }

    fn remove_directory(&self, path: &str) -> Result<(), String> {
        SqliteMetadataHook::remove_directory(self, path).map_err(|e| e.to_string())
    }

    fn rename_path(&self, src: &str, dst: &str) -> Result<(), String> {
        SqliteMetadataHook::rename_path(self, src, dst).map_err(|e| e.to_string())
    }
}

impl<T: MetadataRead + ?Sized> MetadataRead for Arc<T> {
    fn read_committed(&self, file_path: &str) -> Result<Option<ReadView>, String> {
        (**self).read_committed(file_path)
    }
}

impl<T: MetadataNamespace + ?Sized> MetadataNamespace for Arc<T> {
    fn create_directory(&self, path: &str) -> Result<(), String> {
        (**self).create_directory(path)
    }

    fn list_children(&self, path: &str) -> Result<Vec<String>, String> {
        (**self).list_children(path)
    }

    fn remove_directory(&self, path: &str) -> Result<(), String> {
        (**self).remove_directory(path)
    }

    fn rename_path(&self, src: &str, dst: &str) -> Result<(), String> {
        (**self).rename_path(src, dst)
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

    #[test]
    fn sqlite_store_persists_across_reopen() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");

        {
            let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");
            meta.commit_write("tx-1", "/x", 0, &["c1".to_string()], &["c1".to_string()])
                .expect("write should succeed");
            meta.commit_delete("tx-2", "/x", 1)
                .expect("delete should succeed");
        }

        {
            let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should reopen");
            let version = hooks::MetadataWalHook::current_version(&meta, "/x")
                .expect("version should be available");
            assert_eq!(version, 2);
            let view = MetadataRead::read_committed(&meta, "/x").expect("read should not fail");
            assert!(view.is_none());
        }
    }

    #[test]
    fn sqlite_cas_and_idempotent_replay() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");

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
    fn sqlite_live_chunk_scan_excludes_tombstones() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");

        meta.commit_write("tx-1", "/alive", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("alive write should pass");
        meta.commit_write("tx-2", "/dead", 0, &["c2".to_string()], &["c2".to_string()])
            .expect("dead write should pass");
        meta.commit_delete("tx-3", "/dead", 1)
            .expect("dead tombstone should pass");

        let live = meta.all_live_chunk_ids().expect("live chunk scan should pass");
        assert!(live.contains("c1"));
        assert!(!live.contains("c2"));
    }

    #[test]
    fn in_memory_namespace_create_and_list_children() {
        let meta = InMemoryMetadataHook::new();
        meta.create_directory("/docs")
            .expect("directory create should work");
        meta.commit_write("tx-1", "/docs/a.txt", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("file write should work");
        meta.commit_write("tx-2", "/docs/b.txt", 0, &["c2".to_string()], &["c2".to_string()])
            .expect("file write should work");
        meta.create_directory("/tmp")
            .expect("directory create should work");
        meta.commit_write("tx-3", "/tmp/x", 0, &["c3".to_string()], &["c3".to_string()])
            .expect("file write should work");

        let root = meta.list_children("/").expect("root listing should work");
        assert_eq!(root, vec!["docs".to_string(), "tmp".to_string()]);

        let docs = meta
            .list_children("/docs")
            .expect("docs listing should work");
        assert_eq!(docs, vec!["a.txt".to_string(), "b.txt".to_string()]);
    }

    #[test]
    fn in_memory_namespace_rejects_missing_parent() {
        let meta = InMemoryMetadataHook::new();
        let err = meta
            .create_directory("/missing/child")
            .expect_err("mkdir should fail without parent");
        assert!(err.to_string().contains("parent directory does not exist"));

        let err = meta
            .commit_write(
                "tx-1",
                "/missing/file.txt",
                0,
                &["c1".to_string()],
                &["c1".to_string()],
            )
            .expect_err("write should fail without parent");
        assert!(err.contains("parent directory does not exist"));
    }

    #[test]
    fn in_memory_rmdir_rules() {
        let meta = InMemoryMetadataHook::new();
        meta.create_directory("/docs")
            .expect("directory create should work");
        meta.commit_write(
            "tx-1",
            "/docs/readme.txt",
            0,
            &["c1".to_string()],
            &["c1".to_string()],
        )
        .expect("file write should work");

        let err = meta
            .remove_directory("/docs")
            .expect_err("non-empty directory remove should fail");
        assert!(err.to_string().contains("directory not empty"));

        meta.commit_delete("tx-2", "/docs/readme.txt", 1)
            .expect("file delete should work");
        meta.remove_directory("/docs")
            .expect("empty directory remove should work");
        let root = meta.list_children("/").expect("root listing should work");
        assert!(root.is_empty());
    }

    #[test]
    fn in_memory_list_missing_directory_fails() {
        let meta = InMemoryMetadataHook::new();
        let err = meta
            .list_children("/missing")
            .expect_err("missing directory listing should fail");
        assert!(err.to_string().contains("directory not found"));
    }

    #[test]
    fn in_memory_path_type_conflicts() {
        let meta = InMemoryMetadataHook::new();
        meta.commit_write("tx-1", "/docs", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("file write should work");
        let err = meta
            .create_directory("/docs")
            .expect_err("mkdir over live file should fail");
        assert!(err.to_string().contains("path type conflict"));

        meta.create_directory("/dir")
            .expect("directory create should work");
        let err = meta
            .commit_write("tx-2", "/dir", 0, &["c2".to_string()], &["c2".to_string()])
            .expect_err("write over directory should fail");
        assert!(err.contains("path type conflict"));
    }

    #[test]
    fn file_backed_namespace_persists_directories() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.json");

        {
            let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");
            meta.create_directory("/persisted")
                .expect("directory create should work");
            meta.commit_write(
                "tx-1",
                "/persisted/file.txt",
                0,
                &["c1".to_string()],
                &["c1".to_string()],
            )
            .expect("file write should work");
        }

        {
            let meta = FileBackedMetadataHook::open(&path).expect("metadata should reopen");
            let children = meta
                .list_children("/persisted")
                .expect("listing should work");
            assert_eq!(children, vec!["file.txt".to_string()]);
        }
    }

    #[test]
    fn file_backed_namespace_rejects_missing_parent() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.json");
        let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");

        let err = meta
            .create_directory("/missing/child")
            .expect_err("mkdir should fail without parent");
        assert!(err.to_string().contains("parent directory does not exist"));
    }

    #[test]
    fn file_backed_rmdir_rules() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.json");
        let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");

        meta.create_directory("/docs")
            .expect("directory create should work");
        meta.commit_write(
            "tx-1",
            "/docs/readme.txt",
            0,
            &["c1".to_string()],
            &["c1".to_string()],
        )
        .expect("file write should work");
        let err = meta
            .remove_directory("/docs")
            .expect_err("non-empty directory remove should fail");
        assert!(err.to_string().contains("directory not empty"));
    }

    #[test]
    fn file_backed_list_missing_directory_fails() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.json");
        let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");
        let err = meta
            .list_children("/missing")
            .expect_err("missing directory listing should fail");
        assert!(err.to_string().contains("directory not found"));
    }

    #[test]
    fn file_backed_path_type_conflicts() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.json");
        let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");
        meta.commit_write("tx-1", "/docs", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("file write should work");
        let err = meta
            .create_directory("/docs")
            .expect_err("mkdir over live file should fail");
        assert!(err.to_string().contains("path type conflict"));
    }

    #[test]
    fn sqlite_namespace_persists_directories() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");

        {
            let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");
            meta.create_directory("/apps")
                .expect("directory create should work");
            meta.commit_write(
                "tx-1",
                "/apps/demo.bin",
                0,
                &["c1".to_string()],
                &["c1".to_string()],
            )
            .expect("file write should work");
        }

        {
            let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should reopen");
            let children = meta.list_children("/apps").expect("listing should work");
            assert_eq!(children, vec!["demo.bin".to_string()]);
        }
    }

    #[test]
    fn sqlite_namespace_rejects_missing_parent() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");
        let err = meta
            .create_directory("/missing/child")
            .expect_err("mkdir should fail without parent");
        assert!(err.to_string().contains("parent directory does not exist"));
    }

    #[test]
    fn sqlite_rmdir_rules() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");
        meta.create_directory("/docs")
            .expect("directory create should work");
        meta.commit_write(
            "tx-1",
            "/docs/readme.txt",
            0,
            &["c1".to_string()],
            &["c1".to_string()],
        )
        .expect("file write should work");
        let err = meta
            .remove_directory("/docs")
            .expect_err("non-empty directory remove should fail");
        assert!(err.to_string().contains("directory not empty"));
    }

    #[test]
    fn sqlite_list_missing_directory_fails() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");
        let err = meta
            .list_children("/missing")
            .expect_err("missing directory listing should fail");
        assert!(err.to_string().contains("directory not found"));
    }

    #[test]
    fn sqlite_path_type_conflicts() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");
        meta.commit_write("tx-1", "/docs", 0, &["c1".to_string()], &["c1".to_string()])
            .expect("file write should work");
        let err = meta
            .create_directory("/docs")
            .expect_err("mkdir over live file should fail");
        assert!(err.to_string().contains("path type conflict"));
    }

    #[test]
    fn in_memory_rename_file_and_directory() {
        let meta = InMemoryMetadataHook::new();
        meta.create_directory("/docs").expect("mkdir should work");
        meta.commit_write(
            "tx-1",
            "/docs/a.txt",
            0,
            &["c1".to_string()],
            &["c1".to_string()],
        )
        .expect("write should work");
        meta.rename_path("/docs/a.txt", "/docs/b.txt")
            .expect("file rename should work");
        assert!(meta
            .read_committed("/docs/a.txt")
            .expect("read should work")
            .is_none());
        assert!(meta
            .read_committed("/docs/b.txt")
            .expect("read should work")
            .is_some());

        meta.create_directory("/docs/sub").expect("subdir should work");
        meta.commit_write(
            "tx-2",
            "/docs/sub/x.txt",
            0,
            &["c2".to_string()],
            &["c2".to_string()],
        )
        .expect("write should work");
        meta.rename_path("/docs/sub", "/docs/sub2")
            .expect("dir rename should work");
        let children = meta.list_children("/docs").expect("list should work");
        assert!(children.contains(&"sub2".to_string()));
        assert!(meta
            .read_committed("/docs/sub2/x.txt")
            .expect("read should work")
            .is_some());
    }

    #[test]
    fn sqlite_rename_conflict_and_missing_parent() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");
        meta.create_directory("/docs").expect("mkdir should work");
        meta.commit_write(
            "tx-1",
            "/docs/a.txt",
            0,
            &["c1".to_string()],
            &["c1".to_string()],
        )
        .expect("write should work");
        meta.commit_write(
            "tx-2",
            "/docs/b.txt",
            0,
            &["c2".to_string()],
            &["c2".to_string()],
        )
        .expect("write should work");

        let conflict = meta
            .rename_path("/docs/a.txt", "/docs/b.txt")
            .expect_err("rename into existing path should fail");
        assert!(conflict.to_string().contains("path type conflict"));

        let missing_parent = meta
            .rename_path("/docs/a.txt", "/missing/c.txt")
            .expect_err("rename into missing parent should fail");
        assert!(missing_parent
            .to_string()
            .contains("parent directory does not exist"));
    }

    #[test]
    fn in_memory_mkdir_reclaims_tombstoned_file_path() {
        let meta = InMemoryMetadataHook::new();
        meta.commit_write(
            "tx-1",
            "/docs",
            0,
            &["c1".to_string()],
            &["c1".to_string()],
        )
        .expect("file create should work");
        meta.commit_delete("tx-2", "/docs", 1)
            .expect("delete should work");

        meta.create_directory("/docs")
            .expect("mkdir should reclaim tombstoned file path");
        let children = meta.list_children("/").expect("root list should work");
        assert!(children.contains(&"docs".to_string()));

        let err = meta
            .commit_write(
                "tx-3",
                "/docs",
                0,
                &["c2".to_string()],
                &["c2".to_string()],
            )
            .expect_err("write to directory path should fail");
        assert!(err.to_string().contains("path type conflict"));
    }

    #[test]
    fn file_backed_rename_file_reclaims_tombstoned_destination_path() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.json");
        let meta = FileBackedMetadataHook::open(&path).expect("metadata should open");

        meta.create_directory("/docs").expect("mkdir should work");
        meta.commit_write(
            "tx-1",
            "/docs/a.txt",
            0,
            &["ca".to_string()],
            &["ca".to_string()],
        )
        .expect("first write should work");
        meta.commit_write(
            "tx-2",
            "/docs/b.txt",
            0,
            &["cb".to_string()],
            &["cb".to_string()],
        )
        .expect("second write should work");
        meta.commit_delete("tx-3", "/docs/b.txt", 1)
            .expect("delete should work");

        meta.rename_path("/docs/a.txt", "/docs/b.txt")
            .expect("rename over tombstoned destination should work");

        let moved = meta
            .read_committed("/docs/b.txt")
            .expect("read should work")
            .expect("destination should be present");
        assert_eq!(moved.chunk_ids, vec!["ca".to_string()]);
        assert!(meta
            .read_committed("/docs/a.txt")
            .expect("read should work")
            .is_none());
    }

    #[test]
    fn sqlite_rename_directory_reclaims_tombstoned_destination_descendants() {
        let temp = tempdir().expect("temp dir should be created");
        let path = temp.path().join("metadata.db");
        let meta = SqliteMetadataHook::open(&path).expect("sqlite metadata should open");

        meta.create_directory("/src").expect("src mkdir should work");
        meta.commit_write(
            "tx-1",
            "/src/x.txt",
            0,
            &["cx".to_string()],
            &["cx".to_string()],
        )
        .expect("src write should work");

        meta.create_directory("/dst")
            .expect("temporary destination mkdir should work");
        meta.commit_write(
            "tx-2",
            "/dst/x.txt",
            0,
            &["old".to_string()],
            &["old".to_string()],
        )
        .expect("destination seed write should work");
        meta.commit_delete("tx-3", "/dst/x.txt", 1)
            .expect("destination delete should work");
        meta.remove_directory("/dst")
            .expect("destination directory should now be empty and removable");

        meta.rename_path("/src", "/dst")
            .expect("directory rename should reclaim tombstoned descendant paths");

        assert!(meta
            .list_children("/")
            .expect("root list should work")
            .contains(&"dst".to_string()));
        assert!(meta
            .read_committed("/src/x.txt")
            .expect("read should work")
            .is_none());
        let moved = meta
            .read_committed("/dst/x.txt")
            .expect("read should work")
            .expect("destination file should exist");
        assert_eq!(moved.chunk_ids, vec!["cx".to_string()]);
    }
}
