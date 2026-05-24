use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TX_COUNTER: AtomicU64 = AtomicU64::new(1);
static SEQ_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OperationType {
    Write,
    Delete,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TxStatus {
    Pending,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalEntry {
    pub sequence: u64,
    pub transaction_id: String,
    pub file_path: String,
    pub chunk_ids: Vec<String>,
    pub chunk_hashes: Vec<String>,
    pub timestamp_millis: u128,
    pub operation: OperationType,
    pub status: TxStatus,
    pub expected_version: u64,
}

impl WalEntry {
    pub fn new_pending(
        transaction_id: String,
        file_path: String,
        operation: OperationType,
        expected_version: u64,
    ) -> Self {
        Self {
            sequence: next_sequence(),
            transaction_id,
            file_path,
            chunk_ids: Vec::new(),
            chunk_hashes: Vec::new(),
            timestamp_millis: now_millis(),
            operation,
            status: TxStatus::Pending,
            expected_version,
        }
    }

    pub fn with_status(&self, status: TxStatus) -> Self {
        let mut clone = self.clone();
        clone.sequence = next_sequence();
        clone.timestamp_millis = now_millis();
        clone.status = status;
        clone
    }
}

pub fn new_transaction_id() -> String {
    let counter = TX_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("tx-{}-{}", now_millis(), counter)
}

fn next_sequence() -> u64 {
    SEQ_COUNTER.fetch_add(1, Ordering::SeqCst)
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be >= unix epoch")
        .as_millis()
}
