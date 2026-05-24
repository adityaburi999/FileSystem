use crate::error::WalError;
use crate::types::{new_transaction_id, OperationType, TxStatus, WalEntry};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

const LEN_SIZE: usize = std::mem::size_of::<u32>();
const CHECKSUM_SIZE: usize = 32;
const MAX_WAL_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;

pub struct WalLog {
    file: Mutex<File>,
    active_transactions: Mutex<HashSet<String>>,
    finalized_transactions: Mutex<HashSet<String>>,
    requires_repair: AtomicBool,
}

pub struct WalTransaction {
    entry: WalEntry,
}

impl WalLog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path_buf)?;
        let (active_transactions, finalized_transactions, requires_repair) = load_tx_state(&path_buf)?;

        Ok(Self {
            file: Mutex::new(file),
            active_transactions: Mutex::new(active_transactions),
            finalized_transactions: Mutex::new(finalized_transactions),
            requires_repair: AtomicBool::new(requires_repair),
        })
    }

    pub fn append(&self, entry: &WalEntry) -> Result<(), WalError> {
        self.ensure_mutations_allowed()?;
        let payload = serde_json::to_vec(entry)?;
        if payload.len() > MAX_WAL_PAYLOAD_BYTES {
            return Err(WalError::InvalidEntry("wal payload exceeds max size".to_string()));
        }
        let len = u32::try_from(payload.len())
            .map_err(|_| WalError::InvalidEntry("wal payload length overflow".to_string()))?;
        let checksum = blake3::hash(&payload);

        let mut frame = Vec::with_capacity(LEN_SIZE + payload.len() + CHECKSUM_SIZE);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(checksum.as_bytes());

        let mut file = self.file.lock().expect("wal mutex poisoned");
        file.write_all(&frame)?;
        file.sync_data()?;
        Ok(())
    }

    pub fn begin_transaction(
        &self,
        file_path: &str,
        operation: OperationType,
        expected_version: u64,
    ) -> Result<WalTransaction, WalError> {
        self.begin_transaction_with_id(
            new_transaction_id(),
            file_path,
            operation,
            expected_version,
        )
    }

    pub fn begin_transaction_with_id(
        &self,
        transaction_id: String,
        file_path: &str,
        operation: OperationType,
        expected_version: u64,
    ) -> Result<WalTransaction, WalError> {
        self.ensure_mutations_allowed()?;
        if transaction_id.is_empty() {
            return Err(WalError::InvalidEntry(
                "transaction id must be non-empty".to_string(),
            ));
        }
        if transaction_id.contains('\0') {
            return Err(WalError::InvalidEntry(
                "transaction id must not contain NUL bytes".to_string(),
            ));
        }
        if file_path.is_empty() {
            return Err(WalError::InvalidEntry(
                "file path must be non-empty".to_string(),
            ));
        }
        if !file_path.starts_with('/') {
            return Err(WalError::InvalidEntry(
                "file path must be absolute".to_string(),
            ));
        }
        if file_path.contains('\0') {
            return Err(WalError::InvalidEntry(
                "file path must not contain NUL bytes".to_string(),
            ));
        }
        // Lock order is always active -> finalized to avoid deadlocks with finalization.
        {
            let mut active = self
                .active_transactions
                .lock()
                .expect("wal active transaction mutex poisoned");
            let finalized = self
                .finalized_transactions
                .lock()
                .expect("wal finalized transaction mutex poisoned");
            if finalized.contains(&transaction_id) {
                return Err(WalError::InvalidEntry(
                    "transaction already finalized".to_string(),
                ));
            }
            if active.contains(&transaction_id) {
                return Err(WalError::InvalidEntry(
                    "transaction already active".to_string(),
                ));
            }
            active.insert(transaction_id.clone());
        }

        let entry = WalEntry::new_pending(
            transaction_id,
            file_path.to_string(),
            operation,
            expected_version,
        );
        if let Err(e) = self.append(&entry) {
            let mut active = self
                .active_transactions
                .lock()
                .expect("wal active transaction mutex poisoned");
            active.remove(&entry.transaction_id);
            return Err(e);
        }
        Ok(WalTransaction { entry })
    }

    pub fn append_chunk(
        &self,
        txn: &mut WalTransaction,
        chunk_id: String,
        chunk_hash: String,
    ) -> Result<(), WalError> {
        self.ensure_active_transaction(txn.transaction_id())?;
        if txn.entry.operation != OperationType::Write {
            return Err(WalError::InvalidEntry(
                "append_chunk is only valid for write transactions".to_string(),
            ));
        }
        if chunk_id.is_empty() || chunk_hash.is_empty() {
            return Err(WalError::InvalidEntry(
                "chunk id/hash must be non-empty".to_string(),
            ));
        }
        if chunk_id != chunk_hash {
            return Err(WalError::InvalidEntry(
                "chunk id/hash mismatch".to_string(),
            ));
        }
        txn.entry.chunk_ids.push(chunk_id);
        txn.entry.chunk_hashes.push(chunk_hash);
        let snapshot = txn.entry.with_status(TxStatus::Pending);
        self.append(&snapshot)?;
        txn.entry = snapshot;
        Ok(())
    }

    pub fn commit_transaction(&self, txn: &WalTransaction) -> Result<WalEntry, WalError> {
        self.ensure_active_transaction(txn.transaction_id())?;
        let committed = txn.entry.with_status(TxStatus::Committed);
        self.append(&committed)?;
        self.mark_transaction_finalized(txn.transaction_id());
        Ok(committed)
    }

    pub fn abort_transaction(&self, txn: &WalTransaction) -> Result<WalEntry, WalError> {
        self.ensure_active_transaction(txn.transaction_id())?;
        let aborted = txn.entry.with_status(TxStatus::Aborted);
        self.append(&aborted)?;
        self.mark_transaction_finalized(txn.transaction_id());
        Ok(aborted)
    }

    pub(crate) fn mark_transaction_finalized(&self, transaction_id: &str) {
        let mut active = self
            .active_transactions
            .lock()
            .expect("wal active transaction mutex poisoned");
        active.remove(transaction_id);
        let mut finalized = self
            .finalized_transactions
            .lock()
            .expect("wal finalized transaction mutex poisoned");
        finalized.insert(transaction_id.to_string());
    }

    pub fn read_all(&self) -> Result<Vec<WalEntry>, WalError> {
        let mut file = self.file.lock().expect("wal mutex poisoned");
        file.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();

        loop {
            match read_next_frame(&mut file)? {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }

        // Keep the shared append handle positioned at end-of-file.
        file.seek(SeekFrom::End(0))?;

        Ok(entries)
    }

    pub fn repair_truncated_tail(&self) -> Result<(), WalError> {
        let mut file = self.file.lock().expect("wal mutex poisoned");
        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len();
        let valid_len = loop {
            let cursor = file.stream_position()?;
            match read_next_frame(&mut file) {
                Ok(Some(_)) => {}
                Ok(None) => break file.stream_position()?,
                Err(WalError::TruncatedFrame) => {
                    // Truncation is salvageable only at physical EOF.
                    let at_eof = file.stream_position()? == file_len;
                    if at_eof {
                        break cursor;
                    }
                    return Err(WalError::TruncatedFrame);
                }
                Err(WalError::ChecksumMismatch) => {
                    // Checksum mismatch is salvageable only for the terminal frame.
                    let at_eof = file.stream_position()? == file_len;
                    if at_eof {
                        break cursor;
                    }
                    return Err(WalError::ChecksumMismatch);
                }
                Err(WalError::InvalidEntry(message)) => {
                    // Invalid tail frame is salvageable only when encountered at EOF.
                    let at_eof = file.stream_position()? == file_len;
                    if at_eof {
                        break cursor;
                    }
                    return Err(WalError::InvalidEntry(message));
                }
                Err(WalError::Serde(error)) => {
                    // Non-JSON tail frame is salvageable only when encountered at EOF.
                    let at_eof = file.stream_position()? == file_len;
                    if at_eof {
                        break cursor;
                    }
                    return Err(WalError::Serde(error));
                }
                Err(e) => return Err(e),
            }
        };

        file.set_len(valid_len)?;
        file.seek(SeekFrom::End(0))?;
        file.sync_data()?;
        self.requires_repair.store(false, Ordering::Release);
        Ok(())
    }
}

impl WalLog {
    fn ensure_mutations_allowed(&self) -> Result<(), WalError> {
        if self.requires_repair.load(Ordering::Acquire) {
            return Err(WalError::InvalidEntry(
                "wal requires repair before mutation".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_active_transaction(&self, transaction_id: &str) -> Result<(), WalError> {
        let active = self
            .active_transactions
            .lock()
            .expect("wal active transaction mutex poisoned");
        if active.contains(transaction_id) {
            return Ok(());
        }
        Err(WalError::InvalidEntry(
            "transaction is not active".to_string(),
        ))
    }
}

impl WalTransaction {
    pub fn transaction_id(&self) -> &str {
        &self.entry.transaction_id
    }

    pub fn chunk_ids(&self) -> &[String] {
        &self.entry.chunk_ids
    }

    pub fn chunk_hashes(&self) -> &[String] {
        &self.entry.chunk_hashes
    }
}

fn read_next_frame(file: &mut File) -> Result<Option<WalEntry>, WalError> {
    let frame_start = file.stream_position()?;
    let mut len_bytes = [0_u8; LEN_SIZE];
    match file.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
            // EOF exactly at frame boundary means clean end-of-log.
            if frame_start == file.metadata()?.len() {
                return Ok(None);
            }
            return Err(WalError::TruncatedFrame);
        }
        Err(e) => return Err(WalError::Io(e)),
    }

    let payload_len = u32::from_le_bytes(len_bytes) as usize;
    if payload_len > MAX_WAL_PAYLOAD_BYTES {
        return Err(WalError::InvalidEntry(
            "wal frame payload exceeds max size".to_string(),
        ));
    }
    let mut payload = vec![0_u8; payload_len];
    file.read_exact(&mut payload)
        .map_err(|_| WalError::TruncatedFrame)?;

    let mut checksum_bytes = [0_u8; CHECKSUM_SIZE];
    file.read_exact(&mut checksum_bytes)
        .map_err(|_| WalError::TruncatedFrame)?;

    let expected = blake3::hash(&payload);
    if expected.as_bytes() != &checksum_bytes {
        return Err(WalError::ChecksumMismatch);
    }

    let entry = serde_json::from_slice::<WalEntry>(&payload)?;
    Ok(Some(entry))
}

fn load_tx_state(path: &Path) -> Result<(HashSet<String>, HashSet<String>, bool), WalError> {
    let mut file = File::open(path)?;
    let mut latest_status_by_tx: HashMap<String, TxStatus> = HashMap::new();
    let mut requires_repair = false;

    loop {
        match read_next_frame(&mut file) {
            Ok(Some(entry)) => {
                latest_status_by_tx.insert(entry.transaction_id, entry.status);
            }
            Ok(None) => break,
            Err(WalError::TruncatedFrame)
            | Err(WalError::ChecksumMismatch)
            | Err(WalError::InvalidEntry(_))
            | Err(WalError::Serde(_)) => {
                // Tail corruption is salvageable at startup recovery.
                requires_repair = true;
                break;
            }
            Err(e) => return Err(e),
        }
    }

    let mut active = HashSet::new();
    let mut finalized = HashSet::new();
    for (tx_id, status) in latest_status_by_tx {
        match status {
            TxStatus::Pending => {
                active.insert(tx_id);
            }
            TxStatus::Committed | TxStatus::Aborted => {
                finalized.insert(tx_id);
            }
        }
    }

    Ok((active, finalized, requires_repair))
}
