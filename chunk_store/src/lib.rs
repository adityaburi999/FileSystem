use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid chunk id")]
    InvalidChunkId,

    #[error("chunk integrity mismatch")]
    IntegrityMismatch,

    #[error("chunk not found")]
    NotFound,
}

pub struct FsChunkStore {
    root: PathBuf,
}

const IO_RETRY_ATTEMPTS: usize = 3;
const IO_RETRY_BASE_BACKOFF_MS: u64 = 2;

impl FsChunkStore {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self, ChunkStoreError> {
        fs::create_dir_all(root.as_ref())?;
        let store = Self {
            root: root.as_ref().to_path_buf(),
        };
        store.cleanup_stale_temp_artifacts()?;
        Ok(store)
    }

    pub fn put_verified(&self, data: &[u8]) -> Result<String, ChunkStoreError> {
        retry_on_io(|| self.put_verified_once(data))
    }

    fn put_verified_once(&self, data: &[u8]) -> Result<String, ChunkStoreError> {
        let chunk_id = blake3::hash(data).to_hex().to_string();
        let path = self.chunk_path(&chunk_id)?;

        if path.exists() {
            return Ok(chunk_id);
        }

        let parent = path.parent().ok_or(ChunkStoreError::InvalidChunkId)?;
        fs::create_dir_all(parent)?;

        let temp_path = parent.join(format!(".{chunk_id}.tmp"));
        write_temp_chunk(&temp_path, data)?;

        finalize_temp_chunk(&temp_path, &path)?;

        Ok(chunk_id)
    }

    pub fn get_verified(&self, chunk_id: &str) -> Result<Vec<u8>, ChunkStoreError> {
        retry_on_io(|| self.get_verified_once(chunk_id))
    }

    fn get_verified_once(&self, chunk_id: &str) -> Result<Vec<u8>, ChunkStoreError> {
        let path = self.chunk_path(chunk_id)?;
        if !path.exists() {
            return Err(ChunkStoreError::NotFound);
        }

        let mut file = File::open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        let observed = blake3::hash(&bytes).to_hex().to_string();
        if observed != chunk_id {
            let _ = quarantine_corrupt_chunk(&path);
            return Err(ChunkStoreError::IntegrityMismatch);
        }

        Ok(bytes)
    }

    fn chunk_path(&self, chunk_id: &str) -> Result<PathBuf, ChunkStoreError> {
        if chunk_id.len() < 6 || !chunk_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(ChunkStoreError::InvalidChunkId);
        }

        let shard1 = &chunk_id[0..2];
        let shard2 = &chunk_id[2..4];
        Ok(self
            .root
            .join(shard1)
            .join(shard2)
            .join(format!("{chunk_id}.chunk")))
    }

    pub fn list_chunk_ids(&self) -> Result<Vec<String>, ChunkStoreError> {
        let mut out = Vec::new();
        self.scan_chunks(&self.root, &mut out)?;
        Ok(out)
    }

    pub fn remove_chunk(&self, chunk_id: &str) -> Result<(), ChunkStoreError> {
        let path = self.chunk_path(chunk_id)?;
        if !path.exists() {
            return Err(ChunkStoreError::NotFound);
        }
        fs::remove_file(path)?;
        Ok(())
    }

    fn scan_chunks(&self, dir: &Path, out: &mut Vec<String>) -> Result<(), ChunkStoreError> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                self.scan_chunks(&path, out)?;
                continue;
            }

            if path.extension().and_then(|s| s.to_str()) != Some("chunk") {
                continue;
            }

            let file_stem = path.file_stem().and_then(|s| s.to_str());
            if let Some(chunk_id) = file_stem {
                if chunk_id.chars().all(|c| c.is_ascii_hexdigit()) {
                    out.push(chunk_id.to_string());
                }
            }
        }
        Ok(())
    }

    fn cleanup_stale_temp_artifacts(&self) -> Result<(), ChunkStoreError> {
        let mut paths = Vec::new();
        collect_temp_artifacts(&self.root, &mut paths)?;
        for path in paths {
            if path.is_file() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
}

fn quarantine_corrupt_chunk(path: &Path) -> Result<(), ChunkStoreError> {
    let mut quarantine = path.to_path_buf();
    quarantine.set_extension("chunk.quarantine");
    if quarantine.exists() {
        // Best-effort cleanup of older quarantine artifact.
        let _ = fs::remove_file(&quarantine);
    }
    fs::rename(path, quarantine)?;
    Ok(())
}

fn retry_on_io<T, F>(mut op: F) -> Result<T, ChunkStoreError>
where
    F: FnMut() -> Result<T, ChunkStoreError>,
{
    let mut last_err: Option<ChunkStoreError> = None;
    for attempt in 0..=IO_RETRY_ATTEMPTS {
        match op() {
            Ok(v) => return Ok(v),
            Err(ChunkStoreError::Io(e)) if attempt < IO_RETRY_ATTEMPTS => {
                last_err = Some(ChunkStoreError::Io(e));
                let backoff = IO_RETRY_BASE_BACKOFF_MS * (1_u64 << attempt);
                thread::sleep(Duration::from_millis(backoff));
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        ChunkStoreError::Io(std::io::Error::other(
            "io retry exhausted without captured error",
        ))
    }))
}

fn write_temp_chunk(temp_path: &Path, data: &[u8]) -> Result<(), ChunkStoreError> {
    write_temp_chunk_with(temp_path, data, |file, bytes| {
        file.write_all(bytes)?;
        file.sync_data()?;
        Ok(())
    })
}

fn write_temp_chunk_with<F>(temp_path: &Path, data: &[u8], mut writer: F) -> Result<(), ChunkStoreError>
where
    F: FnMut(&mut File, &[u8]) -> std::io::Result<()>,
{
    let mut temp = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(temp_path)?;

    match writer(&mut temp, data) {
        Ok(()) => {
            drop(temp);
            Ok(())
        }
        Err(e) => {
            // Temp write interrupted; cleanup artifact to avoid stale temp buildup.
            drop(temp);
            fs::remove_file(temp_path).ok();
            Err(ChunkStoreError::Io(e))
        }
    }
}

fn finalize_temp_chunk(temp_path: &Path, path: &Path) -> Result<(), ChunkStoreError> {
    match fs::rename(temp_path, path) {
        Ok(_) => Ok(()),
        Err(e) if path.exists() => {
            if path.is_file() {
                // Concurrent finalization race: keep first valid chunk.
                fs::remove_file(temp_path).ok();
                if !path.exists() {
                    return Err(ChunkStoreError::Io(e));
                }
                Ok(())
            } else {
                // Existing non-file destination is invalid and unsafe.
                fs::remove_file(temp_path).ok();
                Err(ChunkStoreError::Io(e))
            }
        }
        Err(e) => {
            // Finalization failed; do not leave behind temp artifact.
            fs::remove_file(temp_path).ok();
            Err(ChunkStoreError::Io(e))
        }
    }
}

fn collect_temp_artifacts(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), ChunkStoreError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_temp_artifacts(&path, out)?;
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) == Some("tmp") {
            out.push(path);
        }
    }
    Ok(())
}

impl wal::ChunkStore for FsChunkStore {
    fn put_chunk(&self, chunk_id: &str, data: &[u8]) -> Result<(), String> {
        let observed = blake3::hash(data).to_hex().to_string();
        if observed != chunk_id {
            return Err("chunk id does not match BLAKE3(content)".to_string());
        }

        self.put_verified(data)
            .and_then(|persisted_id| {
                if persisted_id == chunk_id {
                    Ok(())
                } else {
                    Err(ChunkStoreError::IntegrityMismatch)
                }
            })
            .map_err(|e| e.to_string())
    }

    fn get_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>, String> {
        match self.get_verified(chunk_id) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(ChunkStoreError::NotFound) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{ErrorKind, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn immutable_dedup_and_verify() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let store = FsChunkStore::new(temp.path()).expect("store should initialize");

        let data = b"hello chunk";
        let id1 = store.put_verified(data).expect("first put should work");
        let id2 = store.put_verified(data).expect("second put should dedup");
        assert_eq!(id1, id2);

        let read = store.get_verified(&id1).expect("read should verify");
        assert_eq!(read, data);
    }

    #[test]
    fn retries_io_then_succeeds() {
        let attempts = AtomicUsize::new(0);
        let got = retry_on_io(|| {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                return Err(ChunkStoreError::Io(std::io::Error::new(
                    ErrorKind::Interrupted,
                    "transient",
                )));
            }
            Ok(42_u8)
        })
        .expect("retry should eventually succeed");

        assert_eq!(got, 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn integrity_mismatch_quarantines_chunk() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let store = FsChunkStore::new(temp.path()).expect("store should initialize");

        let data = b"original";
        let chunk_id = store.put_verified(data).expect("put should succeed");
        let chunk_path = store.chunk_path(&chunk_id).expect("chunk path should resolve");

        // Corrupt underlying bytes after commit.
        fs::write(&chunk_path, b"tampered").expect("corruption write should succeed");

        let err = store
            .get_verified(&chunk_id)
            .expect_err("read should fail integrity check");
        assert!(matches!(err, ChunkStoreError::IntegrityMismatch));

        // Corrupt file should be quarantined away from the active chunk path.
        assert!(!chunk_path.exists());
        let mut quarantine = chunk_path.clone();
        quarantine.set_extension("chunk.quarantine");
        assert!(quarantine.exists());

        // Subsequent reads must not serve quarantined data.
        let err2 = store
            .get_verified(&chunk_id)
            .expect_err("quarantined chunk should not be served");
        assert!(matches!(err2, ChunkStoreError::NotFound));
    }

    #[test]
    fn finalization_failure_cleans_up_temp_artifact() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let temp_path = temp.path().join(".x.tmp");
        let final_path = temp.path().join("missing-parent").join("x.chunk");

        fs::write(&temp_path, b"payload").expect("temp payload should exist");
        let err = finalize_temp_chunk(&temp_path, &final_path)
            .expect_err("finalization should fail when destination parent is missing");
        assert!(matches!(err, ChunkStoreError::Io(_)));

        assert!(
            !temp_path.exists(),
            "temp artifact should be cleaned after finalization failure"
        );
    }

    #[test]
    fn temp_write_failure_cleans_up_temp_artifact() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let temp_path = temp.path().join(".x.tmp");

        let err = write_temp_chunk_with(&temp_path, b"payload", |file, _| {
            file.write_all(b"partial")?;
            Err(std::io::Error::new(
                ErrorKind::Interrupted,
                "simulated temp write failure",
            ))
        })
        .expect_err("temp write should fail");
        assert!(matches!(err, ChunkStoreError::Io(_)));
        assert!(
            !temp_path.exists(),
            "temp artifact should be cleaned after write failure"
        );
    }

    #[test]
    fn new_cleans_stale_temp_artifacts() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        let root = temp.path();

        let stale_dir = root.join("aa").join("bb");
        fs::create_dir_all(&stale_dir).expect("stale dir should be created");
        let stale_tmp = stale_dir.join(".orphan.tmp");
        fs::write(&stale_tmp, b"orphan").expect("stale tmp should be created");

        let _store = FsChunkStore::new(root).expect("store should initialize");
        assert!(
            !stale_tmp.exists(),
            "stale temp artifact should be cleaned during startup"
        );
    }
}
