use crate::error::WalError;
use crate::log::{WalLog, WalTransaction};
use crate::types::{new_transaction_id, OperationType};
use std::io::Read;
use std::sync::Arc;

pub trait ChunkStore {
    fn put_chunk(&self, chunk_id: &str, data: &[u8]) -> Result<(), String>;
    fn get_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>, String>;
}

pub trait MetadataCommit {
    fn commit_write(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
        chunk_ids: &[String],
        chunk_hashes: &[String],
    ) -> Result<(), String>;
}

pub trait MetadataDelete {
    fn commit_delete(&self, tx_id: &str, file_path: &str, expected_version: u64) -> Result<(), String>;
}

impl<T: ChunkStore + ?Sized> ChunkStore for Arc<T> {
    fn put_chunk(&self, chunk_id: &str, data: &[u8]) -> Result<(), String> {
        (**self).put_chunk(chunk_id, data)
    }

    fn get_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>, String> {
        (**self).get_chunk(chunk_id)
    }
}

impl<T: MetadataCommit + ?Sized> MetadataCommit for Arc<T> {
    fn commit_write(
        &self,
        tx_id: &str,
        file_path: &str,
        expected_version: u64,
        chunk_ids: &[String],
        chunk_hashes: &[String],
    ) -> Result<(), String> {
        (**self).commit_write(tx_id, file_path, expected_version, chunk_ids, chunk_hashes)
    }
}

impl<T: MetadataDelete + ?Sized> MetadataDelete for Arc<T> {
    fn commit_delete(&self, tx_id: &str, file_path: &str, expected_version: u64) -> Result<(), String> {
        (**self).commit_delete(tx_id, file_path, expected_version)
    }
}

pub struct WritePipeline<C, M> {
    wal: WalLog,
    chunk_store: C,
    metadata: M,
    chunk_size: usize,
}

#[derive(Debug, Clone)]
pub struct WriteResult {
    pub transaction_id: String,
    pub committed: bool,
    pub chunk_count: usize,
}

impl<C, M> WritePipeline<C, M>
where
    C: ChunkStore,
    M: MetadataCommit,
{
    fn abort_on_failure(&self, txn: &WalTransaction, primary: WalError) -> WalError {
        match self.wal.abort_transaction(txn) {
            Ok(_) => primary,
            Err(abort_err) => abort_err,
        }
    }

    pub fn new(wal: WalLog, chunk_store: C, metadata: M, chunk_size: usize) -> Result<Self, WalError> {
        if chunk_size == 0 {
            return Err(WalError::InvalidEntry("chunk_size must be > 0".to_string()));
        }

        Ok(Self {
            wal,
            chunk_store,
            metadata,
            chunk_size,
        })
    }

    // Write flow diagram:
    // client stream -> chunk buffer -> BLAKE3 -> chunk_store put -> WAL append(PENDING snapshots)
    // -> metadata CAS commit -> WAL append(COMMITTED) -> done
    pub fn write_stream<R: Read>(
        &self,
        file_path: &str,
        expected_version: u64,
        reader: R,
    ) -> Result<WriteResult, WalError> {
        let tx_id = new_transaction_id();
        self.write_stream_with_tx_id(file_path, expected_version, tx_id, reader)
    }

    pub fn write_stream_with_tx_id<R: Read>(
        &self,
        file_path: &str,
        expected_version: u64,
        tx_id: String,
        mut reader: R,
    ) -> Result<WriteResult, WalError> {
        let mut txn = self
            .wal
            .begin_transaction_with_id(tx_id, file_path, OperationType::Write, expected_version)?;
        let tx_id = txn.transaction_id().to_string();

        let mut buf = vec![0_u8; self.chunk_size];
        loop {
            let read_n = match reader.read(&mut buf) {
                Ok(n) => n,
                Err(e) => {
                    return Err(self.abort_on_failure(&txn, WalError::Io(e)));
                }
            };
            if read_n == 0 {
                break;
            }

            let chunk = &buf[..read_n];
            let digest = blake3::hash(chunk);
            let chunk_hash = digest.to_hex().to_string();
            let chunk_id = chunk_hash.clone();

            // Strict integrity check before accepting chunk.
            if blake3::hash(chunk).to_hex().to_string() != chunk_id {
                return Err(self.abort_on_failure(
                    &txn,
                    WalError::InvalidEntry("chunk integrity check failed".to_string()),
                ));
            }

            if let Err(e) = self.chunk_store.put_chunk(&chunk_id, chunk) {
                return Err(self.abort_on_failure(&txn, WalError::ChunkStore(e)));
            }

            if let Err(e) = self.wal.append_chunk(&mut txn, chunk_id, chunk_hash) {
                return Err(self.abort_on_failure(&txn, e));
            }
        }

        if let Err(e) = self.metadata.commit_write(
            &tx_id,
            file_path,
            expected_version,
            txn.chunk_ids(),
            txn.chunk_hashes(),
        ) {
            self.wal.abort_transaction(&txn)?;
            if e.contains("cas conflict") {
                return Err(WalError::Conflict);
            }
            return Err(WalError::Metadata(e));
        }

        // Metadata is committed; now durably mark WAL transaction complete.
        self.wal.commit_transaction(&txn)?;

        Ok(WriteResult {
            transaction_id: tx_id,
            committed: true,
            chunk_count: txn.chunk_ids().len(),
        })
    }

    pub fn wal(&self) -> &WalLog {
        &self.wal
    }

    pub fn chunk_store(&self) -> &C {
        &self.chunk_store
    }

    pub fn metadata(&self) -> &M {
        &self.metadata
    }
}
