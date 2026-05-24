use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadataState {
    pub version: u64,
    pub last_tx_id: Option<String>,
    pub chunk_ids: Vec<String>,
    pub chunk_hashes: Vec<String>,
    pub tombstoned: bool,
}

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("cas conflict")]
    CasConflict,

    #[error("invalid chunk vector: ids and hashes length mismatch")]
    InvalidChunkVector,

    #[error("internal lock poisoned")]
    Poisoned,

    #[error("metadata io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("metadata serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub trait MetadataWalHook: Send + Sync {
    fn commit_from_wal(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
        chunk_ids: &[String],
        chunk_hashes: &[String],
    ) -> Result<(), MetadataError>;

    fn current_version(&self, file_path: &str) -> Result<u64, MetadataError>;
}

pub trait MetadataDeleteHook: Send + Sync {
    fn tombstone_from_wal(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
    ) -> Result<(), MetadataError>;
}

pub trait MetadataReadHook: Send + Sync {
    fn read_committed(
        &self,
        file_path: &str,
    ) -> Result<Option<(u64, Vec<String>, Vec<String>)>, MetadataError>;
}

pub struct InMemoryMetadataHook {
    state: Mutex<MetadataState>,
}

struct MetadataState {
    files: HashMap<String, FileMetadataState>,
    applied_transactions: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentMetadataState {
    files: HashMap<String, FileMetadataState>,
    applied_transactions: HashSet<String>,
}

impl InMemoryMetadataHook {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MetadataState {
                files: HashMap::new(),
                applied_transactions: HashSet::new(),
            }),
        }
    }

    pub fn get(&self, file_path: &str) -> Result<Option<FileMetadataState>, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        Ok(state.files.get(file_path).cloned())
    }

    pub fn all_live_chunk_ids(&self) -> Result<std::collections::HashSet<String>, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let mut set = std::collections::HashSet::new();
        for file in state.files.values() {
            if file.tombstoned {
                continue;
            }
            for chunk_id in &file.chunk_ids {
                set.insert(chunk_id.clone());
            }
        }
        Ok(set)
    }
}

impl MetadataWalHook for InMemoryMetadataHook {
    fn commit_from_wal(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
        chunk_ids: &[String],
        chunk_hashes: &[String],
    ) -> Result<(), MetadataError> {
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let MetadataState {
            files,
            applied_transactions,
        } = &mut *state;
        apply_write(
            files,
            applied_transactions,
            tx_id,
            file_path,
            expected_version,
            chunk_ids,
            chunk_hashes,
        )
    }

    fn current_version(&self, file_path: &str) -> Result<u64, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        Ok(state.files.get(file_path).map(|s| s.version).unwrap_or(0))
    }
}

impl MetadataReadHook for InMemoryMetadataHook {
    fn read_committed(
        &self,
        file_path: &str,
    ) -> Result<Option<(u64, Vec<String>, Vec<String>)>, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let file = match state.files.get(file_path) {
            Some(file) => file,
            None => return Ok(None),
        };
        if file.tombstoned {
            return Ok(None);
        }

        Ok(Some((
            file.version,
            file.chunk_ids.clone(),
            file.chunk_hashes.clone(),
        )))
    }
}

impl MetadataDeleteHook for InMemoryMetadataHook {
    fn tombstone_from_wal(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
    ) -> Result<(), MetadataError> {
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let MetadataState {
            files,
            applied_transactions,
        } = &mut *state;
        apply_delete(
            files,
            applied_transactions,
            tx_id,
            file_path,
            expected_version,
        )
    }
}

pub struct FileBackedMetadataHook {
    path: PathBuf,
    state: Mutex<MetadataState>,
}

impl FileBackedMetadataHook {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MetadataError> {
        let path = path.as_ref().to_path_buf();
        let state = load_or_init(&path)?;
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    pub fn all_live_chunk_ids(&self) -> Result<std::collections::HashSet<String>, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        collect_live_chunk_ids(&state.files)
    }
}

impl MetadataWalHook for FileBackedMetadataHook {
    fn commit_from_wal(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
        chunk_ids: &[String],
        chunk_hashes: &[String],
    ) -> Result<(), MetadataError> {
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let prev_files = state.files.clone();
        let prev_applied = state.applied_transactions.clone();
        let MetadataState {
            files,
            applied_transactions,
        } = &mut *state;
        apply_write(
            files,
            applied_transactions,
            tx_id,
            file_path,
            expected_version,
            chunk_ids,
            chunk_hashes,
        )?;
        if let Err(e) = persist_state(&self.path, &state) {
            state.files = prev_files;
            state.applied_transactions = prev_applied;
            return Err(e);
        }
        Ok(())
    }

    fn current_version(&self, file_path: &str) -> Result<u64, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        Ok(state.files.get(file_path).map(|s| s.version).unwrap_or(0))
    }
}

impl MetadataReadHook for FileBackedMetadataHook {
    fn read_committed(
        &self,
        file_path: &str,
    ) -> Result<Option<(u64, Vec<String>, Vec<String>)>, MetadataError> {
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let file = match state.files.get(file_path) {
            Some(file) => file,
            None => return Ok(None),
        };
        if file.tombstoned {
            return Ok(None);
        }
        Ok(Some((
            file.version,
            file.chunk_ids.clone(),
            file.chunk_hashes.clone(),
        )))
    }
}

impl MetadataDeleteHook for FileBackedMetadataHook {
    fn tombstone_from_wal(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
    ) -> Result<(), MetadataError> {
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let prev_files = state.files.clone();
        let prev_applied = state.applied_transactions.clone();
        let MetadataState {
            files,
            applied_transactions,
        } = &mut *state;
        apply_delete(
            files,
            applied_transactions,
            tx_id,
            file_path,
            expected_version,
        )?;
        if let Err(e) = persist_state(&self.path, &state) {
            state.files = prev_files;
            state.applied_transactions = prev_applied;
            return Err(e);
        }
        Ok(())
    }
}

fn collect_live_chunk_ids(
    files: &HashMap<String, FileMetadataState>,
) -> Result<std::collections::HashSet<String>, MetadataError> {
    let mut set = std::collections::HashSet::new();
    for file in files.values() {
        if file.tombstoned {
            continue;
        }
        for chunk_id in &file.chunk_ids {
            set.insert(chunk_id.clone());
        }
    }
    Ok(set)
}

fn apply_write(
    files: &mut HashMap<String, FileMetadataState>,
    applied_transactions: &mut HashSet<String>,
    tx_id: &str,
    file_path: &str,
    expected_version: u64,
    chunk_ids: &[String],
    chunk_hashes: &[String],
) -> Result<(), MetadataError> {
    if chunk_ids.len() != chunk_hashes.len() {
        return Err(MetadataError::InvalidChunkVector);
    }
    if applied_transactions.contains(tx_id) {
        return Ok(());
    }

    let file_state = files.entry(file_path.to_string()).or_insert(FileMetadataState {
        version: 0,
        last_tx_id: None,
        chunk_ids: Vec::new(),
        chunk_hashes: Vec::new(),
        tombstoned: false,
    });

    if file_state.last_tx_id.as_deref() == Some(tx_id) {
        return Ok(());
    }
    if file_state.version != expected_version {
        return Err(MetadataError::CasConflict);
    }

    file_state.version += 1;
    file_state.last_tx_id = Some(tx_id.to_string());
    file_state.chunk_ids = chunk_ids.to_vec();
    file_state.chunk_hashes = chunk_hashes.to_vec();
    file_state.tombstoned = false;
    applied_transactions.insert(tx_id.to_string());
    Ok(())
}

fn apply_delete(
    files: &mut HashMap<String, FileMetadataState>,
    applied_transactions: &mut HashSet<String>,
    tx_id: &str,
    file_path: &str,
    expected_version: u64,
) -> Result<(), MetadataError> {
    if applied_transactions.contains(tx_id) {
        return Ok(());
    }

    let file_state = files.get_mut(file_path).ok_or(MetadataError::CasConflict)?;
    if file_state.version != expected_version {
        return Err(MetadataError::CasConflict);
    }
    file_state.version += 1;
    file_state.last_tx_id = Some(tx_id.to_string());
    file_state.tombstoned = true;
    applied_transactions.insert(tx_id.to_string());
    Ok(())
}

fn load_or_init(path: &Path) -> Result<MetadataState, MetadataError> {
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let state = MetadataState {
            files: HashMap::new(),
            applied_transactions: HashSet::new(),
        };
        persist_state(path, &state)?;
        return Ok(state);
    }

    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(MetadataState {
            files: HashMap::new(),
            applied_transactions: HashSet::new(),
        });
    }
    let persisted: PersistentMetadataState = serde_json::from_slice(&bytes)?;
    Ok(MetadataState {
        files: persisted.files,
        applied_transactions: persisted.applied_transactions,
    })
}

fn persist_state(path: &Path, state: &MetadataState) -> Result<(), MetadataError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    let payload = serde_json::to_vec(&PersistentMetadataState {
        files: state.files.clone(),
        applied_transactions: state.applied_transactions.clone(),
    })?;

    let mut tmp = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)?;
    tmp.write_all(&payload)?;
    tmp.sync_data()?;
    drop(tmp);

    fs::rename(&tmp_path, path)?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(dir)?.sync_data()?;
    Ok(())
}
