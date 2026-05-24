use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("wal frame checksum mismatch")]
    ChecksumMismatch,

    #[error("wal frame truncated")]
    TruncatedFrame,

    #[error("transaction conflict")]
    Conflict,

    #[error("metadata commit rejected: {0}")]
    Metadata(String),

    #[error("chunk store error: {0}")]
    ChunkStore(String),

    #[error("invalid entry: {0}")]
    InvalidEntry(String),
}
