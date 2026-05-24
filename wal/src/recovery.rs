use crate::error::WalError;
use crate::log::WalLog;
use crate::pipeline::{ChunkStore, MetadataCommit, MetadataDelete};
use crate::types::{OperationType, TxStatus, WalEntry};
use std::collections::HashMap;

pub struct RecoveryReport {
    pub committed_reapplied: usize,
    pub pending_aborted: usize,
    pub skipped_aborted: usize,
}

pub struct WalRecovery<'a, C, M> {
    wal: &'a WalLog,
    chunk_store: &'a C,
    metadata: &'a M,
}

impl<'a, C, M> WalRecovery<'a, C, M>
where
    C: ChunkStore,
    M: MetadataCommit + MetadataDelete,
{
    pub fn new(wal: &'a WalLog, chunk_store: &'a C, metadata: &'a M) -> Self {
        Self {
            wal,
            chunk_store,
            metadata,
        }
    }

    pub fn recover(&self) -> Result<RecoveryReport, WalError> {
        self.wal.repair_truncated_tail()?;
        let entries = self.wal.read_all()?;
        let scan = scan_latest_transactions(entries);

        let mut committed_reapplied = 0;
        let mut pending_aborted = 0;
        let mut skipped_aborted = 0;

        for entry in scan {
            validate_entry_shape(&entry)?;
            match entry.status {
                TxStatus::Committed => {
                    apply_committed(self.chunk_store, self.metadata, &entry)?;
                    committed_reapplied += 1;
                }
                TxStatus::Pending => {
                    // Incomplete transactions are non-committed by definition.
                    let aborted = entry.with_status(TxStatus::Aborted);
                    self.wal.append(&aborted)?;
                    self.wal.mark_transaction_finalized(&entry.transaction_id);
                    pending_aborted += 1;
                }
                TxStatus::Aborted => {
                    skipped_aborted += 1;
                }
            }
        }

        Ok(RecoveryReport {
            committed_reapplied,
            pending_aborted,
            skipped_aborted,
        })
    }
}

fn verify_chunks<C: ChunkStore>(chunk_store: &C, entry: &WalEntry) -> Result<(), WalError> {
    if entry.chunk_ids.len() != entry.chunk_hashes.len() {
        return Err(WalError::InvalidEntry(
            "chunk_ids and chunk_hashes length mismatch".to_string(),
        ));
    }

    for (chunk_id, chunk_hash) in entry.chunk_ids.iter().zip(entry.chunk_hashes.iter()) {
        if chunk_id != chunk_hash {
            return Err(WalError::InvalidEntry(format!(
                "chunk id/hash mismatch for chunk_id={chunk_id}"
            )));
        }

        let data = chunk_store
            .get_chunk(chunk_id)
            .map_err(WalError::ChunkStore)?
            .ok_or_else(|| WalError::InvalidEntry(format!("missing chunk {chunk_id}")))?;

        let observed = blake3::hash(&data).to_hex().to_string();
        if &observed != chunk_hash {
            return Err(WalError::InvalidEntry(format!(
                "chunk hash mismatch for chunk_id={chunk_id}"
            )));
        }
    }

    Ok(())
}

fn validate_entry_shape(entry: &WalEntry) -> Result<(), WalError> {
    if entry.transaction_id.is_empty() {
        return Err(WalError::InvalidEntry(
            "transaction id must be non-empty".to_string(),
        ));
    }
    if entry.transaction_id.contains('\0') {
        return Err(WalError::InvalidEntry(
            "transaction id must not contain NUL bytes".to_string(),
        ));
    }
    if entry.file_path.is_empty() {
        return Err(WalError::InvalidEntry("file path must be non-empty".to_string()));
    }
    if !entry.file_path.starts_with('/') {
        return Err(WalError::InvalidEntry(
            "file path must be absolute".to_string(),
        ));
    }
    if entry.file_path.contains('\0') {
        return Err(WalError::InvalidEntry(
            "file path must not contain NUL bytes".to_string(),
        ));
    }
    Ok(())
}

fn apply_committed<C: ChunkStore, M: MetadataCommit + MetadataDelete>(
    chunk_store: &C,
    metadata: &M,
    entry: &WalEntry,
) -> Result<(), WalError> {
    match entry.operation {
        OperationType::Write => {
            verify_chunks(chunk_store, entry)?;
            metadata
                .commit_write(
                    &entry.transaction_id,
                    &entry.file_path,
                    entry.expected_version,
                    &entry.chunk_ids,
                    &entry.chunk_hashes,
                )
                .map_err(WalError::Metadata)?;
        }
        OperationType::Delete => {
            if !entry.chunk_ids.is_empty() || !entry.chunk_hashes.is_empty() {
                return Err(WalError::InvalidEntry(
                    "delete transaction must not include chunk vectors".to_string(),
                ));
            }
            metadata
                .commit_delete(&entry.transaction_id, &entry.file_path, entry.expected_version)
                .map_err(WalError::Metadata)?;
        }
    }

    Ok(())
}

fn scan_latest_transactions(entries: Vec<WalEntry>) -> Vec<WalEntry> {
    let mut latest_by_tx: HashMap<String, (usize, WalEntry)> = HashMap::new();
    for (index, entry) in entries.into_iter().enumerate() {
        latest_by_tx.insert(entry.transaction_id.clone(), (index, entry));
    }

    let mut latest: Vec<(usize, WalEntry)> = latest_by_tx.into_values().collect();
    // Recovery replay order is strictly WAL append order.
    latest.sort_by_key(|(index, _)| *index);
    let latest: Vec<WalEntry> = latest.into_iter().map(|(_, entry)| entry).collect();
    latest
}

#[cfg(test)]
mod tests {
    use super::scan_latest_transactions;
    use crate::types::{OperationType, TxStatus, WalEntry};

    fn entry(tx: &str, status: TxStatus, sequence: u64) -> WalEntry {
        WalEntry {
            sequence,
            transaction_id: tx.to_string(),
            file_path: "/x".to_string(),
            chunk_ids: Vec::new(),
            chunk_hashes: Vec::new(),
            timestamp_millis: 1,
            operation: OperationType::Write,
            status,
            expected_version: 0,
        }
    }

    #[test]
    fn scanner_uses_last_append_not_highest_sequence() {
        let entries = vec![
            entry("tx-1", TxStatus::Pending, 100),
            entry("tx-2", TxStatus::Pending, 101),
            // Simulate a restart where sequence counter restarts lower.
            entry("tx-1", TxStatus::Committed, 1),
        ];

        let scanned = scan_latest_transactions(entries);
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].transaction_id, "tx-2");
        assert_eq!(scanned[0].status, TxStatus::Pending);
        assert_eq!(scanned[1].transaction_id, "tx-1");
        assert_eq!(scanned[1].status, TxStatus::Committed);
    }

    #[test]
    fn scanner_returns_latest_state_per_tx_in_append_order() {
        let entries = vec![
            entry("a", TxStatus::Pending, 1),
            entry("b", TxStatus::Pending, 2),
            entry("a", TxStatus::Aborted, 3),
            entry("c", TxStatus::Pending, 4),
            entry("b", TxStatus::Committed, 5),
        ];

        let scanned = scan_latest_transactions(entries);
        let txs: Vec<(&str, TxStatus)> = scanned
            .iter()
            .map(|e| (e.transaction_id.as_str(), e.status))
            .collect();
        assert_eq!(
            txs,
            vec![
                ("a", TxStatus::Aborted),
                ("c", TxStatus::Pending),
                ("b", TxStatus::Committed),
            ]
        );
    }
}
