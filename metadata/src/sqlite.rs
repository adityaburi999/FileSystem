use crate::hooks::{FileMetadataState, MetadataDeleteHook, MetadataError, MetadataReadHook, MetadataWalHook};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

pub struct SqliteMetadataHook {
    conn: Mutex<Connection>,
}

impl SqliteMetadataHook {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MetadataError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path).map_err(MetadataError::Sqlite)?;
        initialize_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn all_live_chunk_ids(&self) -> Result<HashSet<String>, MetadataError> {
        let conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        let mut stmt = conn
            .prepare("SELECT chunk_ids FROM files WHERE tombstoned = 0")
            .map_err(MetadataError::Sqlite)?;
        let mut rows = stmt.query([]).map_err(MetadataError::Sqlite)?;

        let mut set = HashSet::new();
        while let Some(row) = rows.next().map_err(MetadataError::Sqlite)? {
            let chunk_ids_json: String = row.get(0).map_err(MetadataError::Sqlite)?;
            let chunk_ids: Vec<String> = serde_json::from_str(&chunk_ids_json)?;
            for chunk_id in chunk_ids {
                set.insert(chunk_id);
            }
        }
        Ok(set)
    }

    pub fn create_directory(&self, path: &str) -> Result<(), MetadataError> {
        validate_directory_path(path)?;
        let conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        if path != "/" {
            let parent = parent_directory(path)?;
            if !directory_exists(&conn, &parent)? {
                return Err(MetadataError::ParentMissing);
            }
        }
        if let Some(tombstoned) = file_tombstone_state(&conn, path)? {
            if !tombstoned {
                return Err(MetadataError::PathTypeConflict);
            }
            // Reclaim historical tombstoned file path before creating a directory node.
            conn.execute("DELETE FROM files WHERE file_path = ?1", params![path])
                .map_err(MetadataError::Sqlite)?;
        }
        conn.execute(
            "INSERT OR IGNORE INTO directories (path) VALUES (?1)",
            params![path],
        )
        .map_err(MetadataError::Sqlite)?;
        Ok(())
    }

    pub fn list_children(&self, path: &str) -> Result<Vec<String>, MetadataError> {
        validate_directory_path(path)?;
        let conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        if !directory_exists(&conn, path)? {
            return Err(MetadataError::DirectoryNotFound);
        }
        list_children_from_db(&conn, path)
    }

    pub fn remove_directory(&self, path: &str) -> Result<(), MetadataError> {
        validate_directory_path(path)?;
        if path == "/" {
            return Err(MetadataError::InvalidPath);
        }
        let conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        if !directory_exists(&conn, path)? {
            return Err(MetadataError::DirectoryNotFound);
        }
        if !list_children_from_db(&conn, path)?.is_empty() {
            return Err(MetadataError::DirectoryNotEmpty);
        }

        conn.execute("DELETE FROM directories WHERE path = ?1", params![path])
            .map_err(MetadataError::Sqlite)?;
        Ok(())
    }

    pub fn rename_path(&self, src: &str, dst: &str) -> Result<(), MetadataError> {
        validate_directory_path(src)?;
        validate_directory_path(dst)?;
        if src == "/" || dst == "/" || src == dst {
            return Err(MetadataError::InvalidPath);
        }

        let mut conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(MetadataError::Sqlite)?;

        let src_is_dir = directory_exists(&tx, src)?;
        let src_is_file = live_file_exists(&tx, src)?;
        if !src_is_dir && !src_is_file {
            return Err(MetadataError::PathNotFound);
        }

        let dst_parent = parent_directory(dst)?;
        if !directory_exists(&tx, &dst_parent)? {
            return Err(MetadataError::ParentMissing);
        }
        if directory_exists(&tx, dst)? || live_file_exists(&tx, dst)? {
            return Err(MetadataError::PathTypeConflict);
        }
        if let Some(tombstoned) = file_tombstone_state(&tx, dst)? {
            if !tombstoned {
                return Err(MetadataError::PathTypeConflict);
            }
            tx.execute("DELETE FROM files WHERE file_path = ?1", params![dst])
                .map_err(MetadataError::Sqlite)?;
        }

        if src_is_dir {
            let src_prefix = format!("{src}/");
            let dst_prefix = format!("{dst}/");
            if dst.starts_with(&src_prefix) {
                return Err(MetadataError::PathTypeConflict);
            }

            let src_file_paths = list_file_paths_by_prefix(&tx, &src_prefix)?;
            let mut tombstoned_conflicts_to_delete = Vec::new();
            for src_path in src_file_paths {
                if let Some(suffix) = src_path.strip_prefix(&src_prefix) {
                    let mapped_dst_path = format!("{dst_prefix}{suffix}");
                    if let Some(tombstoned) = file_tombstone_state(&tx, &mapped_dst_path)? {
                        if !tombstoned {
                            return Err(MetadataError::PathTypeConflict);
                        }
                        tombstoned_conflicts_to_delete.push(mapped_dst_path);
                    }
                }
            }
            for path in tombstoned_conflicts_to_delete {
                tx.execute("DELETE FROM files WHERE file_path = ?1", params![path])
                    .map_err(MetadataError::Sqlite)?;
            }

            tx.execute(
                "UPDATE directories SET path = ?1 WHERE path = ?2",
                params![dst, src],
            )
            .map_err(MetadataError::Sqlite)?;
            tx.execute(
                "UPDATE directories SET path = ?1 || substr(path, ?2) WHERE path LIKE ?3",
                params![dst_prefix, (src.len() + 2) as i64, format!("{src}/%")],
            )
            .map_err(MetadataError::Sqlite)?;
            tx.execute(
                "UPDATE files SET file_path = ?1 || substr(file_path, ?2) WHERE file_path LIKE ?3",
                params![dst_prefix, (src.len() + 2) as i64, format!("{src}/%")],
            )
            .map_err(MetadataError::Sqlite)?;
        } else {
            tx.execute(
                "UPDATE files SET file_path = ?1 WHERE file_path = ?2",
                params![dst, src],
            )
            .map_err(MetadataError::Sqlite)?;
        }

        tx.commit().map_err(MetadataError::Sqlite)?;
        Ok(())
    }
}

impl MetadataWalHook for SqliteMetadataHook {
    fn commit_from_wal(
        &self,
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

        let mut conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(MetadataError::Sqlite)?;

        let parent = parent_directory(file_path)?;
        if !directory_exists(&tx, &parent)? {
            return Err(MetadataError::ParentMissing);
        }
        if directory_exists(&tx, file_path)? {
            return Err(MetadataError::PathTypeConflict);
        }

        if tx_already_applied(&tx, tx_id)? {
            tx.commit().map_err(MetadataError::Sqlite)?;
            return Ok(());
        }

        let current = read_state(&tx, file_path)?;
        if current.version != expected_version {
            return Err(MetadataError::CasConflict);
        }

        let next = FileMetadataState {
            version: current.version + 1,
            last_tx_id: Some(tx_id.to_string()),
            chunk_ids: chunk_ids.to_vec(),
            chunk_hashes: chunk_hashes.to_vec(),
            tombstoned: false,
        };

        persist_state(&tx, file_path, &next)?;
        mark_tx_applied(&tx, tx_id)?;
        tx.commit().map_err(MetadataError::Sqlite)?;
        Ok(())
    }

    fn current_version(&self, file_path: &str) -> Result<u64, MetadataError> {
        let conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        let version: Option<u64> = conn
            .query_row(
                "SELECT version FROM files WHERE file_path = ?1",
                params![file_path],
                |row| row.get(0),
            )
            .optional()
            .map_err(MetadataError::Sqlite)?;
        Ok(version.unwrap_or(0))
    }
}

impl MetadataDeleteHook for SqliteMetadataHook {
    fn tombstone_from_wal(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
    ) -> Result<(), MetadataError> {
        let mut conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(MetadataError::Sqlite)?;

        if tx_already_applied(&tx, tx_id)? {
            tx.commit().map_err(MetadataError::Sqlite)?;
            return Ok(());
        }

        let current = read_existing_state(&tx, file_path)?.ok_or(MetadataError::CasConflict)?;
        if current.version != expected_version {
            return Err(MetadataError::CasConflict);
        }

        let mut next = current;
        next.version += 1;
        next.last_tx_id = Some(tx_id.to_string());
        next.tombstoned = true;
        persist_state(&tx, file_path, &next)?;
        mark_tx_applied(&tx, tx_id)?;
        tx.commit().map_err(MetadataError::Sqlite)?;
        Ok(())
    }
}

impl MetadataReadHook for SqliteMetadataHook {
    fn read_committed(
        &self,
        file_path: &str,
    ) -> Result<Option<(u64, Vec<String>, Vec<String>)>, MetadataError> {
        let conn = self.conn.lock().map_err(|_| MetadataError::Poisoned)?;
        let state = read_existing_state(&conn, file_path)?;
        let state = match state {
            Some(v) => v,
            None => return Ok(None),
        };
        if state.tombstoned {
            return Ok(None);
        }
        Ok(Some((state.version, state.chunk_ids, state.chunk_hashes)))
    }
}

fn initialize_schema(conn: &Connection) -> Result<(), MetadataError> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = FULL;
        CREATE TABLE IF NOT EXISTS files (
            file_path TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            last_tx_id TEXT NULL,
            chunk_ids TEXT NOT NULL,
            chunk_hashes TEXT NOT NULL,
            tombstoned INTEGER NOT NULL CHECK (tombstoned IN (0, 1))
        );
        CREATE TABLE IF NOT EXISTS applied_transactions (
            tx_id TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS directories (
            path TEXT PRIMARY KEY
        );
        INSERT OR IGNORE INTO directories (path) VALUES ('/');
        ",
    )
    .map_err(MetadataError::Sqlite)?;
    Ok(())
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

fn directory_exists(conn: &Connection, path: &str) -> Result<bool, MetadataError> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM directories WHERE path = ?1",
            params![path],
            |row| row.get(0),
        )
        .optional()
        .map_err(MetadataError::Sqlite)?;
    Ok(found.is_some())
}

fn live_file_exists(conn: &Connection, path: &str) -> Result<bool, MetadataError> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM files WHERE file_path = ?1 AND tombstoned = 0",
            params![path],
            |row| row.get(0),
        )
        .optional()
        .map_err(MetadataError::Sqlite)?;
    Ok(found.is_some())
}

fn file_tombstone_state(conn: &Connection, path: &str) -> Result<Option<bool>, MetadataError> {
    let tombstoned: Option<i64> = conn
        .query_row(
            "SELECT tombstoned FROM files WHERE file_path = ?1",
            params![path],
            |row| row.get(0),
        )
        .optional()
        .map_err(MetadataError::Sqlite)?;
    Ok(tombstoned.map(|v| v != 0))
}

fn list_file_paths_by_prefix(conn: &Connection, prefix: &str) -> Result<Vec<String>, MetadataError> {
    let mut stmt = conn
        .prepare("SELECT file_path FROM files WHERE file_path LIKE ?1")
        .map_err(MetadataError::Sqlite)?;
    let rows = stmt
        .query_map(params![format!("{prefix}%")], |row| row.get::<_, String>(0))
        .map_err(MetadataError::Sqlite)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(MetadataError::Sqlite)?);
    }
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

fn list_children_from_db(conn: &Connection, path: &str) -> Result<Vec<String>, MetadataError> {
    let mut names = HashSet::new();

    let mut dir_stmt = conn
        .prepare("SELECT path FROM directories")
        .map_err(MetadataError::Sqlite)?;
    let dir_iter = dir_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(MetadataError::Sqlite)?;
    for item in dir_iter {
        let dir = item.map_err(MetadataError::Sqlite)?;
        if let Some(name) = direct_child_name(path, &dir) {
            names.insert(name.to_string());
        }
    }

    let mut file_stmt = conn
        .prepare("SELECT file_path FROM files WHERE tombstoned = 0")
        .map_err(MetadataError::Sqlite)?;
    let file_iter = file_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(MetadataError::Sqlite)?;
    for item in file_iter {
        let file = item.map_err(MetadataError::Sqlite)?;
        if let Some(name) = direct_child_name(path, &file) {
            names.insert(name.to_string());
        }
    }

    let mut out: Vec<String> = names.into_iter().collect();
    out.sort();
    Ok(out)
}

fn tx_already_applied(conn: &Connection, tx_id: &str) -> Result<bool, MetadataError> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM applied_transactions WHERE tx_id = ?1",
            params![tx_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(MetadataError::Sqlite)?;
    Ok(found.is_some())
}

fn mark_tx_applied(conn: &Connection, tx_id: &str) -> Result<(), MetadataError> {
    conn.execute(
        "INSERT OR IGNORE INTO applied_transactions (tx_id) VALUES (?1)",
        params![tx_id],
    )
    .map_err(MetadataError::Sqlite)?;
    Ok(())
}

fn read_state(conn: &Connection, file_path: &str) -> Result<FileMetadataState, MetadataError> {
    Ok(read_existing_state(conn, file_path)?.unwrap_or(FileMetadataState {
        version: 0,
        last_tx_id: None,
        chunk_ids: Vec::new(),
        chunk_hashes: Vec::new(),
        tombstoned: false,
    }))
}

fn read_existing_state(
    conn: &Connection,
    file_path: &str,
) -> Result<Option<FileMetadataState>, MetadataError> {
    conn.query_row(
        "SELECT version, last_tx_id, chunk_ids, chunk_hashes, tombstoned FROM files WHERE file_path = ?1",
        params![file_path],
        |row| {
            let version: u64 = row.get(0)?;
            let last_tx_id: Option<String> = row.get(1)?;
            let chunk_ids_json: String = row.get(2)?;
            let chunk_hashes_json: String = row.get(3)?;
            let tombstoned_num: i64 = row.get(4)?;
            Ok((
                version,
                last_tx_id,
                chunk_ids_json,
                chunk_hashes_json,
                tombstoned_num,
            ))
        },
    )
    .optional()
    .map_err(MetadataError::Sqlite)?
    .map(|(version, last_tx_id, chunk_ids_json, chunk_hashes_json, tombstoned_num)| {
        Ok(FileMetadataState {
            version,
            last_tx_id,
            chunk_ids: serde_json::from_str(&chunk_ids_json)?,
            chunk_hashes: serde_json::from_str(&chunk_hashes_json)?,
            tombstoned: tombstoned_num != 0,
        })
    })
    .transpose()
}

fn persist_state(conn: &Connection, file_path: &str, state: &FileMetadataState) -> Result<(), MetadataError> {
    let chunk_ids_json = serde_json::to_string(&state.chunk_ids)?;
    let chunk_hashes_json = serde_json::to_string(&state.chunk_hashes)?;
    conn.execute(
        "
        INSERT INTO files (
            file_path, version, last_tx_id, chunk_ids, chunk_hashes, tombstoned
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(file_path) DO UPDATE SET
            version = excluded.version,
            last_tx_id = excluded.last_tx_id,
            chunk_ids = excluded.chunk_ids,
            chunk_hashes = excluded.chunk_hashes,
            tombstoned = excluded.tombstoned
        ",
        params![
            file_path,
            state.version,
            state.last_tx_id,
            chunk_ids_json,
            chunk_hashes_json,
            if state.tombstoned { 1_i64 } else { 0_i64 },
        ],
    )
    .map_err(MetadataError::Sqlite)?;
    Ok(())
}
