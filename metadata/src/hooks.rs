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

    #[error("metadata sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("invalid namespace path")]
    InvalidPath,

    #[error("parent directory does not exist")]
    ParentMissing,

    #[error("directory not found")]
    DirectoryNotFound,

    #[error("directory not empty")]
    DirectoryNotEmpty,

    #[error("path type conflict")]
    PathTypeConflict,

    #[error("path not found")]
    PathNotFound,
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
    directories: HashSet<String>,
    applied_transactions: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentMetadataState {
    files: HashMap<String, FileMetadataState>,
    #[serde(default = "default_directories")]
    directories: HashSet<String>,
    applied_transactions: HashSet<String>,
}

fn default_directories() -> HashSet<String> {
    HashSet::from(["/".to_string()])
}

impl InMemoryMetadataHook {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MetadataState {
                files: HashMap::new(),
                directories: default_directories(),
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

    pub fn create_directory(&self, path: &str) -> Result<(), MetadataError> {
        validate_directory_path(path)?;
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        if path != "/" {
            let parent = parent_directory(path)?;
            if !state.directories.contains(&parent) {
                return Err(MetadataError::ParentMissing);
            }
        }
        if let Some(existing) = state.files.get(path) {
            if !existing.tombstoned {
                return Err(MetadataError::PathTypeConflict);
            }
            // Reclaim historical tombstoned file path before creating a directory node.
            state.files.remove(path);
        }
        state.directories.insert(path.to_string());
        Ok(())
    }

    pub fn list_children(&self, path: &str) -> Result<Vec<String>, MetadataError> {
        validate_directory_path(path)?;
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        if !state.directories.contains(path) {
            return Err(MetadataError::DirectoryNotFound);
        }
        collect_children(path, &state.directories, &state.files)
    }

    pub fn remove_directory(&self, path: &str) -> Result<(), MetadataError> {
        validate_directory_path(path)?;
        if path == "/" {
            return Err(MetadataError::InvalidPath);
        }
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        if !state.directories.contains(path) {
            return Err(MetadataError::DirectoryNotFound);
        }
        let children = collect_children(path, &state.directories, &state.files)?;
        if !children.is_empty() {
            return Err(MetadataError::DirectoryNotEmpty);
        }
        state.directories.remove(path);
        Ok(())
    }

    pub fn rename_path(&self, src: &str, dst: &str) -> Result<(), MetadataError> {
        validate_directory_path(src)?;
        validate_directory_path(dst)?;
        if src == "/" || dst == "/" || src == dst {
            return Err(MetadataError::InvalidPath);
        }
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        rename_in_state(&mut state, src, dst)
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
            directories,
            applied_transactions,
            ..
        } = &mut *state;
        apply_write(
            files,
            directories,
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
            ..
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

    pub fn create_directory(&self, path: &str) -> Result<(), MetadataError> {
        validate_directory_path(path)?;
        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        if path != "/" {
            let parent = parent_directory(path)?;
            if !state.directories.contains(&parent) {
                return Err(MetadataError::ParentMissing);
            }
        }
        if let Some(existing) = state.files.get(path) {
            if !existing.tombstoned {
                return Err(MetadataError::PathTypeConflict);
            }
            // Reclaim historical tombstoned file path before creating a directory node.
            state.files.remove(path);
        }
        let prev_files = state.files.clone();
        let prev_dirs = state.directories.clone();
        state.directories.insert(path.to_string());
        if let Err(e) = persist_state(&self.path, &state) {
            state.files = prev_files;
            state.directories = prev_dirs;
            return Err(e);
        }
        Ok(())
    }

    pub fn list_children(&self, path: &str) -> Result<Vec<String>, MetadataError> {
        validate_directory_path(path)?;
        let state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        if !state.directories.contains(path) {
            return Err(MetadataError::DirectoryNotFound);
        }
        collect_children(path, &state.directories, &state.files)
    }

    pub fn remove_directory(&self, path: &str) -> Result<(), MetadataError> {
        validate_directory_path(path)?;
        if path == "/" {
            return Err(MetadataError::InvalidPath);
        }

        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        if !state.directories.contains(path) {
            return Err(MetadataError::DirectoryNotFound);
        }
        let children = collect_children(path, &state.directories, &state.files)?;
        if !children.is_empty() {
            return Err(MetadataError::DirectoryNotEmpty);
        }

        let prev_dirs = state.directories.clone();
        state.directories.remove(path);
        if let Err(e) = persist_state(&self.path, &state) {
            state.directories = prev_dirs;
            return Err(e);
        }
        Ok(())
    }

    pub fn rename_path(&self, src: &str, dst: &str) -> Result<(), MetadataError> {
        validate_directory_path(src)?;
        validate_directory_path(dst)?;
        if src == "/" || dst == "/" || src == dst {
            return Err(MetadataError::InvalidPath);
        }

        let mut state = self.state.lock().map_err(|_| MetadataError::Poisoned)?;
        let prev_files = state.files.clone();
        let prev_dirs = state.directories.clone();
        let result = rename_in_state(&mut state, src, dst);
        if let Err(e) = result {
            state.files = prev_files;
            state.directories = prev_dirs;
            return Err(e);
        }
        if let Err(e) = persist_state(&self.path, &state) {
            state.files = prev_files;
            state.directories = prev_dirs;
            return Err(e);
        }
        Ok(())
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
        let prev_dirs = state.directories.clone();
        let MetadataState {
            files,
            directories,
            applied_transactions,
            ..
        } = &mut *state;
        apply_write(
            files,
            directories,
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
            state.directories = prev_dirs;
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
        let prev_dirs = state.directories.clone();
        let MetadataState {
            files,
            applied_transactions,
            ..
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
            state.directories = prev_dirs;
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

fn is_live_file_at_path(files: &HashMap<String, FileMetadataState>, path: &str) -> bool {
    files.get(path).map(|f| !f.tombstoned).unwrap_or(false)
}

fn validate_directory_path(path: &str) -> Result<(), MetadataError> {
    if path.is_empty() || !path.starts_with('/') || path.contains('\0') {
        return Err(MetadataError::InvalidPath);
    }
    if path.len() > 1 && path.ends_with('/') {
        return Err(MetadataError::InvalidPath);
    }
    Ok(())
}

fn validate_file_path(path: &str) -> Result<(), MetadataError> {
    validate_directory_path(path)?;
    if path == "/" {
        return Err(MetadataError::InvalidPath);
    }
    Ok(())
}

fn parent_directory(path: &str) -> Result<String, MetadataError> {
    validate_directory_path(path)?;
    if path == "/" {
        return Ok("/".to_string());
    }
    let idx = path.rfind('/').ok_or(MetadataError::InvalidPath)?;
    if idx == 0 {
        return Ok("/".to_string());
    }
    Ok(path[..idx].to_string())
}

fn collect_children(
    parent: &str,
    directories: &HashSet<String>,
    files: &HashMap<String, FileMetadataState>,
) -> Result<Vec<String>, MetadataError> {
    let mut children = HashSet::new();

    for dir in directories {
        if let Some(name) = direct_child_name(parent, dir) {
            children.insert(name.to_string());
        }
    }

    for (path, state) in files {
        if state.tombstoned {
            continue;
        }
        if let Some(name) = direct_child_name(parent, path) {
            children.insert(name.to_string());
        }
    }

    let mut out: Vec<String> = children.into_iter().collect();
    out.sort();
    Ok(out)
}

fn direct_child_name<'a>(parent: &str, candidate: &'a str) -> Option<&'a str> {
    if parent == "/" {
        if candidate == "/" || !candidate.starts_with('/') {
            return None;
        }
        let rest = &candidate[1..];
        if rest.is_empty() {
            return None;
        }
        return rest.split('/').next();
    }

    let prefix = format!("{parent}/");
    let rest = candidate.strip_prefix(&prefix)?;
    if rest.is_empty() {
        return None;
    }
    rest.split('/').next()
}

fn rename_in_state(state: &mut MetadataState, src: &str, dst: &str) -> Result<(), MetadataError> {
    let src_is_dir = state.directories.contains(src);
    let src_is_file = is_live_file_at_path(&state.files, src);
    if !src_is_dir && !src_is_file {
        return Err(MetadataError::PathNotFound);
    }

    let dst_parent = parent_directory(dst)?;
    if !state.directories.contains(&dst_parent) {
        return Err(MetadataError::ParentMissing);
    }
    if state.directories.contains(dst) || is_live_file_at_path(&state.files, dst) {
        return Err(MetadataError::PathTypeConflict);
    }

    if src_is_dir {
        let prefix = format!("{src}/");
        let dst_prefix = format!("{dst}/");
        if dst.starts_with(&prefix) {
            return Err(MetadataError::PathTypeConflict);
        }

        // Rewrite directory nodes.
        let old_dirs: Vec<String> = state.directories.iter().cloned().collect();
        let mut new_dirs = HashSet::new();
        for dir in old_dirs {
            if dir == src {
                new_dirs.insert(dst.to_string());
            } else if let Some(suffix) = dir.strip_prefix(&prefix) {
                new_dirs.insert(format!("{dst_prefix}{suffix}"));
            } else {
                new_dirs.insert(dir);
            }
        }
        state.directories = new_dirs;

        // Rewrite all file paths under moved directory, preserving metadata values.
        let old_files: Vec<(String, FileMetadataState)> = state
            .files
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut new_files = HashMap::new();
        let mut moved_files = Vec::new();
        for (path, value) in old_files {
            if let Some(suffix) = path.strip_prefix(&prefix) {
                moved_files.push((format!("{dst_prefix}{suffix}"), value));
            } else {
                new_files.insert(path, value);
            }
        }
        if let Some(existing) = new_files.get(dst) {
            if !existing.tombstoned {
                return Err(MetadataError::PathTypeConflict);
            }
        }
        new_files.remove(dst);
        for (target_path, moved_state) in moved_files {
            if let Some(existing) = new_files.get(&target_path) {
                if !existing.tombstoned {
                    return Err(MetadataError::PathTypeConflict);
                }
            }
            // Reclaim tombstoned destination history before installing moved live entry.
            new_files.remove(&target_path);
            new_files.insert(target_path, moved_state);
        }
        state.files = new_files;
        return Ok(());
    }

    // File rename.
    if let Some(existing) = state.files.get(dst) {
        if !existing.tombstoned {
            return Err(MetadataError::PathTypeConflict);
        }
    }
    state.files.remove(dst);
    let value = state.files.remove(src).ok_or(MetadataError::PathNotFound)?;
    state.files.insert(dst.to_string(), value);
    Ok(())
}

fn apply_write(
    files: &mut HashMap<String, FileMetadataState>,
    directories: &HashSet<String>,
    applied_transactions: &mut HashSet<String>,
    tx_id: &str,
    file_path: &str,
    expected_version: u64,
    chunk_ids: &[String],
    chunk_hashes: &[String],
) -> Result<(), MetadataError> {
    validate_file_path(file_path)?;
    if chunk_ids.len() != chunk_hashes.len() {
        return Err(MetadataError::InvalidChunkVector);
    }
    let parent = parent_directory(file_path)?;
    if !directories.contains(&parent) {
        return Err(MetadataError::ParentMissing);
    }
    if directories.contains(file_path) {
        return Err(MetadataError::PathTypeConflict);
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
            directories: default_directories(),
            applied_transactions: HashSet::new(),
        };
        persist_state(path, &state)?;
        return Ok(state);
    }

    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(MetadataState {
            files: HashMap::new(),
            directories: default_directories(),
            applied_transactions: HashSet::new(),
        });
    }
    let persisted: PersistentMetadataState = serde_json::from_slice(&bytes)?;
    let mut directories = persisted.directories;
    directories.insert("/".to_string());
    Ok(MetadataState {
        files: persisted.files,
        directories,
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
        directories: state.directories.clone(),
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
