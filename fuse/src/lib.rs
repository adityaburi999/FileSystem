pub mod demo_config;

use cache::ChunkCache;
use fs_core::{CoreError, FileSystemCore, NodeKind as CoreNodeKind};
use metadata::{MetadataNamespace, MetadataRead};
use path_resolver::{DefaultPathResolver, PathResolver};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use thiserror::Error;
use wal::{ChunkStore, MetadataCommit, MetadataDelete};

#[derive(Debug, Error)]
pub enum FuseError {
    #[error("invalid path")]
    InvalidPath,

    #[error("invalid read range")]
    InvalidRange,

    #[error("object not found")]
    NotFound,

    #[error("io error")]
    Io,

    #[error("conflict")]
    Conflict,

    #[error("service unavailable")]
    Unavailable,

    #[error("request timed out")]
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuseNodeKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseDirEntry {
    pub name: String,
    pub kind: FuseNodeKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FusePathEntry {
    pub path: String,
    pub kind: FuseNodeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseTreeSummary {
    pub files: usize,
    pub directories: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseGcReport {
    pub candidates: usize,
    pub deleted: usize,
    pub deferred: usize,
}

pub struct FuseApi<C, M, K>
where
    C: ChunkStore,
    M: MetadataCommit + MetadataRead + MetadataDelete,
    K: ChunkCache,
{
    core: FileSystemCore<C, M, K>,
    resolver: DefaultPathResolver,
}

enum DaemonRequest {
    Health,
    HealthDelay { millis: u64 },
    IsMountOpen,
    EnqueueGcScan,
    StartupRecover,
    Open {
        path: String,
    },
    Read {
        path: String,
        offset: usize,
        size: usize,
    },
    ReadAll {
        path: String,
    },
    Write {
        path: String,
        expected_version: u64,
        bytes: Vec<u8>,
    },
    WriteIfVersion {
        path: String,
        expected_version: u64,
        bytes: Vec<u8>,
    },
    CompareAndSwapFile {
        path: String,
        expected_version: u64,
        expected_bytes: Vec<u8>,
        bytes: Vec<u8>,
    },
    WriteIfHash {
        path: String,
        expected_version: u64,
        expected_hash: String,
        bytes: Vec<u8>,
    },
    WriteIfSize {
        path: String,
        expected_version: u64,
        expected_size: usize,
        bytes: Vec<u8>,
    },
    WriteIfExists {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfEmpty {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfNotEmpty {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfStartsWith {
        path: String,
        required_prefix: Vec<u8>,
        bytes: Vec<u8>,
    },
    WriteIfEndsWith {
        path: String,
        required_suffix: Vec<u8>,
        bytes: Vec<u8>,
    },
    WriteIfContains {
        path: String,
        required_subsequence: Vec<u8>,
        bytes: Vec<u8>,
    },
    WriteIfNotContains {
        path: String,
        forbidden_subsequence: Vec<u8>,
        bytes: Vec<u8>,
    },
    WriteIfExact {
        path: String,
        expected_bytes: Vec<u8>,
        bytes: Vec<u8>,
    },
    WriteIfNotExact {
        path: String,
        forbidden_bytes: Vec<u8>,
        bytes: Vec<u8>,
    },
    WriteIfMinSize {
        path: String,
        min_size: usize,
        bytes: Vec<u8>,
    },
    WriteIfMaxSize {
        path: String,
        max_size: usize,
        bytes: Vec<u8>,
    },
    WriteIfSizeBetween {
        path: String,
        min_size: usize,
        max_size: usize,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotBetween {
        path: String,
        min_size: usize,
        max_size: usize,
        bytes: Vec<u8>,
    },
    WriteIfSizeMultipleOf {
        path: String,
        divisor: usize,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotMultipleOf {
        path: String,
        divisor: usize,
        bytes: Vec<u8>,
    },
    WriteIfSizeOdd {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeEven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwo {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwo {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePrime {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPrime {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeFibonacci {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotFibonacci {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeSquare {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotSquare {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeCube {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotCube {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeTriangular {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotTriangular {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeFactorial {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotFactorial {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeComposite {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotComposite {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePerfect {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPerfect {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeAbundant {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotAbundant {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeDeficient {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotDeficient {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeSemiprime {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotSemiprime {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePalindrome {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPalindrome {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeArmstrong {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotArmstrong {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeHappy {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotHappy {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeAutomorphic {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotAutomorphic {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeHarshad {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotHarshad {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeKaprekar {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotKaprekar {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeRepdigit {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotRepdigit {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeTribonacci {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotTribonacci {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePell {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPell {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeLucas {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotLucas {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeMersenne {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotMersenne {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThree {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThree {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfFour {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfFour {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfFive {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfFive {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfSix {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfSix {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfSeven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfSeven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfEight {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfEight {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfNine {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfNine {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfEleven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfEleven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwelve {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwelve {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfFourteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfFourteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfFifteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfFifteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfSixteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfSixteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfSeventeen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfSeventeen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfEighteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfEighteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfNineteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfNineteen {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwenty {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwenty {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentyOne {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentyOne {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentyTwo {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentyTwo {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentyThree {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentyThree {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentyFour {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentyFour {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentyFive {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentyFive {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentySix {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentySix {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentySeven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentySeven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentyEight {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentyEight {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfTwentyNine {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfTwentyNine {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirty {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirty {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtyOne {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtyOne {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtyTwo {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtyTwo {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtyThree {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtyThree {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtyFour {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtyFour {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtyFive {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtyFive {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtySix {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtySix {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtySeven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtySeven {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtyEight {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtyEight {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizePowerOfThirtyNine {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfSizeNotPowerOfThirtyNine {
        path: String,
        bytes: Vec<u8>,
    },
    WriteIfMissing {
        path: String,
        bytes: Vec<u8>,
    },
    EnsureFile {
        path: String,
    },
    CopyFile {
        src: String,
        dst: String,
        expected_version: u64,
    },
    TouchFile {
        path: String,
        expected_version: u64,
    },
    TruncateFile {
        path: String,
        new_size: usize,
        expected_version: u64,
    },
    AppendFile {
        path: String,
        expected_version: u64,
        bytes: Vec<u8>,
    },
    OverwriteRange {
        path: String,
        offset: usize,
        expected_version: u64,
        bytes: Vec<u8>,
    },
    InsertRange {
        path: String,
        offset: usize,
        expected_version: u64,
        bytes: Vec<u8>,
    },
    DeleteRange {
        path: String,
        offset: usize,
        len: usize,
        expected_version: u64,
    },
    ReplaceRange {
        path: String,
        offset: usize,
        len: usize,
        expected_version: u64,
        bytes: Vec<u8>,
    },
    FileSize {
        path: String,
    },
    FileHash {
        path: String,
    },
    Unlink {
        path: String,
        expected_version: u64,
    },
    RemoveDir {
        path: String,
    },
    RemoveTree {
        path: String,
    },
    RemovePath {
        path: String,
    },
    Mkdir {
        path: String,
    },
    MkdirAll {
        path: String,
    },
    ReadDir {
        path: String,
    },
    ReadDirDetailed {
        path: String,
    },
    WalkDir {
        path: String,
    },
    TreeSummary {
        path: String,
    },
    TreeBytes {
        path: String,
    },
    RunBackgroundOnce,
    GcScanOnce,
    Exists {
        path: String,
    },
    Stat {
        path: String,
    },
    Rename {
        src: String,
        dst: String,
    },
    Shutdown,
}

#[derive(Debug)]
enum DaemonResponse {
    Empty,
    Bytes(Vec<u8>),
    TxId(String),
    Names(Vec<String>),
    DirEntries(Vec<FuseDirEntry>),
    PathEntries(Vec<FusePathEntry>),
    TreeSummary(FuseTreeSummary),
    GcReports(Vec<FuseGcReport>),
    Bool(bool),
    Count(usize),
    Text(String),
    Kind(FuseNodeKind),
}

struct DaemonEnvelope {
    request: DaemonRequest,
    reply_tx: Sender<Result<DaemonResponse, FuseError>>,
}

pub struct FuseDaemon {
    request_tx: Sender<DaemonEnvelope>,
    worker: Option<JoinHandle<()>>,
    request_timeout: Option<Duration>,
    pending_requests: Arc<AtomicUsize>,
    max_pending_requests: Arc<AtomicUsize>,
}

const DEFAULT_MAX_PENDING_REQUESTS: usize = 1024;

impl<C, M, K> FuseApi<C, M, K>
where
    C: ChunkStore,
    M: MetadataCommit + MetadataRead + MetadataDelete,
    K: ChunkCache,
{
    pub fn new(core: FileSystemCore<C, M, K>) -> Self {
        Self {
            core,
            resolver: DefaultPathResolver::new(),
        }
    }

    pub fn startup_recover(&self) -> Result<(), FuseError> {
        match self.core.startup_recover() {
            Ok(_) => Ok(()),
            Err(_) => Err(FuseError::Unavailable),
        }
    }

    pub fn is_mount_open(&self) -> bool {
        self.core.is_mount_open()
    }

    pub fn health(&self) -> Result<(), FuseError> {
        Ok(())
    }

    pub fn health_with_delay(&self, delay: Duration) -> Result<(), FuseError> {
        thread::sleep(delay);
        Ok(())
    }

    pub fn open(&self, path: &str) -> Result<(), FuseError> {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        // Metadata authority check through zero-length read.
        self.core
            .read_range(&resolved.canonical_path, 0, 0)
            .map(|_| ())
            .map_err(map_core_error)
    }

    pub fn read(&self, path: &str, offset: usize, size: usize) -> Result<Vec<u8>, FuseError> {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .read_range(&resolved.canonical_path, offset, size)
            .map_err(map_core_error)
    }

    pub fn read_all(&self, path: &str) -> Result<Vec<u8>, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .read_all(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn write(
        &self,
        path: &str,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_stream(&resolved.canonical_path, expected_version, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_missing(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_missing(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_version(
        &self,
        path: &str,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_version(&resolved.canonical_path, expected_version, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn compare_and_swap_file(
        &self,
        path: &str,
        expected_version: u64,
        expected_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .compare_and_swap_file(
                &resolved.canonical_path,
                expected_version,
                expected_bytes,
                bytes,
            )
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_hash(
        &self,
        path: &str,
        expected_version: u64,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_hash(
                &resolved.canonical_path,
                expected_version,
                expected_hash,
                bytes,
            )
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size(
        &self,
        path: &str,
        expected_version: u64,
        expected_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size(
                &resolved.canonical_path,
                expected_version,
                expected_size,
                bytes,
            )
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_exists(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_exists(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_empty(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_empty(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_not_empty(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_not_empty(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_starts_with(
        &self,
        path: &str,
        required_prefix: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_starts_with(&resolved.canonical_path, required_prefix, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_ends_with(
        &self,
        path: &str,
        required_suffix: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_ends_with(&resolved.canonical_path, required_suffix, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_contains(
        &self,
        path: &str,
        required_subsequence: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_contains(&resolved.canonical_path, required_subsequence, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_not_contains(
        &self,
        path: &str,
        forbidden_subsequence: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_not_contains(&resolved.canonical_path, forbidden_subsequence, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_exact(
        &self,
        path: &str,
        expected_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_exact(&resolved.canonical_path, expected_bytes, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_not_exact(
        &self,
        path: &str,
        forbidden_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_not_exact(&resolved.canonical_path, forbidden_bytes, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_min_size(
        &self,
        path: &str,
        min_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_min_size(&resolved.canonical_path, min_size, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_max_size(
        &self,
        path: &str,
        max_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_max_size(&resolved.canonical_path, max_size, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_between(
        &self,
        path: &str,
        min_size: usize,
        max_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_between(&resolved.canonical_path, min_size, max_size, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_between(
        &self,
        path: &str,
        min_size: usize,
        max_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_between(&resolved.canonical_path, min_size, max_size, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_multiple_of(
        &self,
        path: &str,
        divisor: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_multiple_of(&resolved.canonical_path, divisor, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_multiple_of(
        &self,
        path: &str,
        divisor: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_multiple_of(&resolved.canonical_path, divisor, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_odd(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_odd(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_even(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_even(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_two(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_two(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_prime(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_prime(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_prime(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_prime(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_fibonacci(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_fibonacci(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_fibonacci(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_fibonacci(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_square(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_square(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_square(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_square(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_cube(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_cube(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_cube(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_cube(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_triangular(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_triangular(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_triangular(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_triangular(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_factorial(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_factorial(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_factorial(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_factorial(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_composite(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_composite(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_composite(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_composite(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_perfect(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_perfect(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_perfect(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_perfect(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_abundant(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_abundant(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_abundant(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_abundant(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_deficient(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_deficient(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_deficient(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_deficient(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_semiprime(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_semiprime(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_semiprime(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_semiprime(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_palindrome(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_palindrome(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_palindrome(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_palindrome(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_armstrong(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_armstrong(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_armstrong(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_armstrong(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_happy(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_happy(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_happy(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_happy(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_automorphic(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_automorphic(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_automorphic(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_automorphic(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_harshad(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_harshad(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_harshad(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_harshad(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_kaprekar(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_kaprekar(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_kaprekar(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_kaprekar(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_repdigit(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_repdigit(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_repdigit(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_repdigit(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_tribonacci(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_tribonacci(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_tribonacci(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_tribonacci(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_pell(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_pell(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_pell(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_pell(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_lucas(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_lucas(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_lucas(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_lucas(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_mersenne(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_mersenne(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_mersenne(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_mersenne(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_three(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_three(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_four(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_four(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_five(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_five(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_six(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_six(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_seven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_seven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_eight(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_eight(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_nine(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_nine(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_ten(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_ten(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_ten(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_ten(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_eleven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_eleven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_eleven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_eleven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twelve(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twelve(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twelve(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twelve(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_fourteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_fourteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_fourteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_fourteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_fifteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_fifteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_fifteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_fifteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_sixteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_sixteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_sixteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_sixteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_seventeen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_seventeen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_seventeen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_seventeen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_eighteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_eighteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_eighteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_eighteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_nineteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_nineteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_nineteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_nineteen(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_one(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_one(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_two(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_two(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_three(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_three(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_four(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_four(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_five(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_five(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_six(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_six(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_seven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_seven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_eight(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_eight(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_twenty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_twenty_nine(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_twenty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_twenty_nine(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_one(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_one(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_two(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_two(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_three(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_three(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_four(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_four(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_five(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_five(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_six(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_six(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_seven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_seven(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_eight(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_eight(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_power_of_thirty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_power_of_thirty_nine(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn write_if_size_not_power_of_thirty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .write_if_size_not_power_of_thirty_nine(&resolved.canonical_path, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn ensure_file(&self, path: &str) -> Result<bool, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .ensure_file(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn copy_file(
        &self,
        src: &str,
        dst: &str,
        expected_version: u64,
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let src = self.resolver.resolve(src).map_err(|_| FuseError::InvalidPath)?;
        let dst = self.resolver.resolve(dst).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .copy_file(&src.canonical_path, &dst.canonical_path, expected_version)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn touch_file(&self, path: &str, expected_version: u64) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .touch_file(&resolved.canonical_path, expected_version)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn truncate_file(
        &self,
        path: &str,
        new_size: usize,
        expected_version: u64,
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .truncate_file(&resolved.canonical_path, new_size, expected_version)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn append_file(
        &self,
        path: &str,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .append_file(&resolved.canonical_path, expected_version, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn overwrite_range(
        &self,
        path: &str,
        offset: usize,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .overwrite_range(&resolved.canonical_path, offset, expected_version, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn insert_range(
        &self,
        path: &str,
        offset: usize,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .insert_range(&resolved.canonical_path, offset, expected_version, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn delete_range(
        &self,
        path: &str,
        offset: usize,
        len: usize,
        expected_version: u64,
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .delete_range(&resolved.canonical_path, offset, len, expected_version)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn replace_range(
        &self,
        path: &str,
        offset: usize,
        len: usize,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let result = self
            .core
            .replace_range(&resolved.canonical_path, offset, len, expected_version, bytes)
            .map_err(map_core_error)?;
        Ok(result.transaction_id)
    }

    pub fn file_size(&self, path: &str) -> Result<usize, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .file_size(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn file_hash(&self, path: &str) -> Result<String, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .file_hash(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn unlink(&self, path: &str, expected_version: u64) -> Result<(), FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .unlink_checked(&resolved.canonical_path, expected_version)
            .map_err(map_core_error)
    }

    pub fn mkdir(&self, path: &str) -> Result<(), FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core.mkdir(&resolved.canonical_path).map_err(map_core_error)
    }

    pub fn mkdir_p(&self, path: &str) -> Result<usize, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .mkdir_p(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn readdir(&self, path: &str) -> Result<Vec<String>, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .list_dir(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn readdir_with_kinds(&self, path: &str) -> Result<Vec<FuseDirEntry>, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let entries = self
            .core
            .list_dir_with_kinds(&resolved.canonical_path)
            .map_err(map_core_error)?;
        Ok(entries
            .into_iter()
            .map(|(name, kind)| FuseDirEntry {
                name,
                kind: match kind {
                    CoreNodeKind::File => FuseNodeKind::File,
                    CoreNodeKind::Directory => FuseNodeKind::Directory,
                },
            })
            .collect())
    }

    pub fn walk_dir(&self, path: &str) -> Result<Vec<FusePathEntry>, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let entries = self
            .core
            .walk_dir(&resolved.canonical_path)
            .map_err(map_core_error)?;
        Ok(entries
            .into_iter()
            .map(|(path, kind)| FusePathEntry {
                path,
                kind: match kind {
                    CoreNodeKind::File => FuseNodeKind::File,
                    CoreNodeKind::Directory => FuseNodeKind::Directory,
                },
            })
            .collect())
    }

    pub fn tree_summary(&self, path: &str) -> Result<FuseTreeSummary, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let summary = self
            .core
            .tree_summary(&resolved.canonical_path)
            .map_err(map_core_error)?;
        Ok(FuseTreeSummary {
            files: summary.files,
            directories: summary.directories,
        })
    }

    pub fn tree_bytes(&self, path: &str) -> Result<usize, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .tree_bytes(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn run_background_once(&self) -> Result<Vec<FuseGcReport>, FuseError> {
        let reports = self.core.run_background_once().map_err(map_core_error)?;
        Ok(reports
            .into_iter()
            .map(|report| FuseGcReport {
                candidates: report.candidates,
                deleted: report.deleted,
                deferred: report.deferred,
            })
            .collect())
    }

    pub fn gc_scan_once(&self) -> Result<Vec<FuseGcReport>, FuseError> {
        let reports = self.core.gc_scan_once().map_err(map_core_error)?;
        Ok(reports
            .into_iter()
            .map(|report| FuseGcReport {
                candidates: report.candidates,
                deleted: report.deleted,
                deferred: report.deferred,
            })
            .collect())
    }

    pub fn enqueue_gc_scan(&self) -> Result<bool, FuseError> {
        self.core.enqueue_gc_scan().map_err(map_core_error)
    }

    pub fn exists(&self, path: &str) -> Result<bool, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .path_exists(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn stat(&self, path: &str) -> Result<FuseNodeKind, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        let kind = self
            .core
            .stat_path(&resolved.canonical_path)
            .map_err(map_core_error)?;
        Ok(match kind {
            CoreNodeKind::File => FuseNodeKind::File,
            CoreNodeKind::Directory => FuseNodeKind::Directory,
        })
    }

    pub fn rmdir(&self, path: &str) -> Result<(), FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core.rmdir(&resolved.canonical_path).map_err(map_core_error)
    }

    pub fn rmtree(&self, path: &str) -> Result<usize, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core.rmtree(&resolved.canonical_path).map_err(map_core_error)
    }

    pub fn remove_path(&self, path: &str) -> Result<usize, FuseError>
    where
        M: MetadataNamespace,
    {
        let resolved = self.resolver.resolve(path).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .remove_path(&resolved.canonical_path)
            .map_err(map_core_error)
    }

    pub fn rename(&self, src: &str, dst: &str) -> Result<(), FuseError>
    where
        M: MetadataNamespace,
    {
        let src = self.resolver.resolve(src).map_err(|_| FuseError::InvalidPath)?;
        let dst = self.resolver.resolve(dst).map_err(|_| FuseError::InvalidPath)?;
        self.core
            .rename_path(&src.canonical_path, &dst.canonical_path)
            .map_err(map_core_error)
    }
}

impl FuseDaemon {
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub fn set_request_timeout(&mut self, timeout: Option<Duration>) {
        self.request_timeout = timeout;
    }

    pub fn with_max_pending_requests(self, max_pending_requests: usize) -> Self {
        self.max_pending_requests
            .store(max_pending_requests.max(1), Ordering::Release);
        self
    }

    pub fn set_max_pending_requests(&self, max_pending_requests: usize) {
        self.max_pending_requests
            .store(max_pending_requests.max(1), Ordering::Release);
    }

    pub fn pending_requests(&self) -> usize {
        self.pending_requests.load(Ordering::Acquire)
    }

    pub fn shutdown(mut self) -> Result<(), FuseError> {
        let _ = self.request_blocking(DaemonRequest::Shutdown)?;
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| FuseError::Unavailable)?;
        }
        Ok(())
    }

    pub fn startup_recover(&self) -> Result<(), FuseError> {
        let response = self.request(DaemonRequest::StartupRecover)?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    pub fn is_mount_open(&self) -> Result<bool, FuseError> {
        let response = self.request(DaemonRequest::IsMountOpen)?;
        match response {
            DaemonResponse::Bool(value) => Ok(value),
            _ => Err(FuseError::Io),
        }
    }

    pub fn health(&self) -> Result<(), FuseError> {
        let response = self.request(DaemonRequest::Health)?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    pub fn health_with_delay(&self, delay: Duration) -> Result<(), FuseError> {
        let millis = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        let response = self.request(DaemonRequest::HealthDelay { millis })?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    pub fn is_worker_alive(&self) -> bool {
        self.worker
            .as_ref()
            .map(|worker| !worker.is_finished())
            .unwrap_or(false)
    }

    pub fn open(&self, path: &str) -> Result<(), FuseError> {
        let response = self.request(DaemonRequest::Open {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    pub fn read(&self, path: &str, offset: usize, size: usize) -> Result<Vec<u8>, FuseError> {
        let response = self.request(DaemonRequest::Read {
            path: path.to_string(),
            offset,
            size,
        })?;
        match response {
            DaemonResponse::Bytes(bytes) => Ok(bytes),
            _ => Err(FuseError::Io),
        }
    }

    pub fn read_all(&self, path: &str) -> Result<Vec<u8>, FuseError> {
        let response = self.request(DaemonRequest::ReadAll {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Bytes(bytes) => Ok(bytes),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write(&self, path: &str, expected_version: u64, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::Write {
            path: path.to_string(),
            expected_version,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_semiprime(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeSemiprime {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_semiprime(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotSemiprime {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_palindrome(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePalindrome {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_palindrome(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPalindrome {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_armstrong(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeArmstrong {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_armstrong(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotArmstrong {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_happy(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeHappy {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_happy(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotHappy {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_automorphic(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeAutomorphic {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_automorphic(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotAutomorphic {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_harshad(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeHarshad {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_harshad(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotHarshad {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_kaprekar(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeKaprekar {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_kaprekar(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotKaprekar {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_repdigit(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeRepdigit {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_repdigit(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotRepdigit {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_tribonacci(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeTribonacci {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_tribonacci(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotTribonacci {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_pell(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePell {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_pell(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPell {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_lucas(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeLucas {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_lucas(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotLucas {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_mersenne(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeMersenne {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_mersenne(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotMersenne {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThree {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThree {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfFour {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfFour {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfFive {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfFive {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfSix {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfSix {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfSeven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfSeven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfEight {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfEight {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfNine {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfNine {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_ten(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_ten(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_eleven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfEleven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_eleven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfEleven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twelve(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwelve {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twelve(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwelve {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_fourteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfFourteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_fourteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfFourteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_fifteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfFifteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_fifteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfFifteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_sixteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfSixteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_sixteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfSixteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_seventeen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfSeventeen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_seventeen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfSeventeen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_eighteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfEighteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_eighteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfEighteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_nineteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfNineteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_nineteen(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfNineteen {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwenty {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwenty {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentyOne {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentyOne {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentyTwo {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentyTwo {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentyThree {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentyThree {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentyFour {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentyFour {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentyFive {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentyFive {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentySix {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentySix {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentySeven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentySeven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentyEight {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentyEight {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_twenty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwentyNine {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_twenty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwentyNine {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirty {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirty {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtyOne {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_one(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtyOne {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtyTwo {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtyTwo {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtyThree {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_three(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtyThree {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtyFour {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_four(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtyFour {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtyFive {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_five(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtyFive {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtySix {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_six(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtySix {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtySeven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_seven(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtySeven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtyEight {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_eight(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtyEight {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_thirty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfThirtyNine {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_thirty_nine(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfThirtyNine {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_missing(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfMissing {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_version(
        &self,
        path: &str,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfVersion {
            path: path.to_string(),
            expected_version,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn compare_and_swap_file(
        &self,
        path: &str,
        expected_version: u64,
        expected_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::CompareAndSwapFile {
            path: path.to_string(),
            expected_version,
            expected_bytes: expected_bytes.to_vec(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_hash(
        &self,
        path: &str,
        expected_version: u64,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfHash {
            path: path.to_string(),
            expected_version,
            expected_hash: expected_hash.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size(
        &self,
        path: &str,
        expected_version: u64,
        expected_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSize {
            path: path.to_string(),
            expected_version,
            expected_size,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_exists(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfExists {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_empty(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfEmpty {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_not_empty(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfNotEmpty {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_starts_with(
        &self,
        path: &str,
        required_prefix: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfStartsWith {
            path: path.to_string(),
            required_prefix: required_prefix.to_vec(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_ends_with(
        &self,
        path: &str,
        required_suffix: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfEndsWith {
            path: path.to_string(),
            required_suffix: required_suffix.to_vec(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_contains(
        &self,
        path: &str,
        required_subsequence: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfContains {
            path: path.to_string(),
            required_subsequence: required_subsequence.to_vec(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_not_contains(
        &self,
        path: &str,
        forbidden_subsequence: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfNotContains {
            path: path.to_string(),
            forbidden_subsequence: forbidden_subsequence.to_vec(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_exact(
        &self,
        path: &str,
        expected_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfExact {
            path: path.to_string(),
            expected_bytes: expected_bytes.to_vec(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_not_exact(
        &self,
        path: &str,
        forbidden_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfNotExact {
            path: path.to_string(),
            forbidden_bytes: forbidden_bytes.to_vec(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_min_size(
        &self,
        path: &str,
        min_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfMinSize {
            path: path.to_string(),
            min_size,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_max_size(
        &self,
        path: &str,
        max_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfMaxSize {
            path: path.to_string(),
            max_size,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_between(
        &self,
        path: &str,
        min_size: usize,
        max_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeBetween {
            path: path.to_string(),
            min_size,
            max_size,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_between(
        &self,
        path: &str,
        min_size: usize,
        max_size: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotBetween {
            path: path.to_string(),
            min_size,
            max_size,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_multiple_of(
        &self,
        path: &str,
        divisor: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeMultipleOf {
            path: path.to_string(),
            divisor,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_multiple_of(
        &self,
        path: &str,
        divisor: usize,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotMultipleOf {
            path: path.to_string(),
            divisor,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_odd(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeOdd {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_even(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeEven {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_power_of_two(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePowerOfTwo {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_power_of_two(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPowerOfTwo {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_prime(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePrime {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_prime(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPrime {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_fibonacci(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeFibonacci {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_fibonacci(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotFibonacci {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_square(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeSquare {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_square(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotSquare {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_cube(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeCube {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_cube(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotCube {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_triangular(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeTriangular {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_triangular(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotTriangular {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_factorial(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeFactorial {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_factorial(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotFactorial {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_composite(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeComposite {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_composite(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotComposite {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_perfect(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizePerfect {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_perfect(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotPerfect {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_abundant(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeAbundant {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_abundant(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotAbundant {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_deficient(&self, path: &str, bytes: &[u8]) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeDeficient {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn write_if_size_not_deficient(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::WriteIfSizeNotDeficient {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn ensure_file(&self, path: &str) -> Result<bool, FuseError> {
        let response = self.request(DaemonRequest::EnsureFile {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Bool(created) => Ok(created),
            _ => Err(FuseError::Io),
        }
    }

    pub fn copy_file(
        &self,
        src: &str,
        dst: &str,
        expected_version: u64,
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::CopyFile {
            src: src.to_string(),
            dst: dst.to_string(),
            expected_version,
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn touch_file(&self, path: &str, expected_version: u64) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::TouchFile {
            path: path.to_string(),
            expected_version,
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn truncate_file(
        &self,
        path: &str,
        new_size: usize,
        expected_version: u64,
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::TruncateFile {
            path: path.to_string(),
            new_size,
            expected_version,
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn append_file(
        &self,
        path: &str,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::AppendFile {
            path: path.to_string(),
            expected_version,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn overwrite_range(
        &self,
        path: &str,
        offset: usize,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::OverwriteRange {
            path: path.to_string(),
            offset,
            expected_version,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn insert_range(
        &self,
        path: &str,
        offset: usize,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::InsertRange {
            path: path.to_string(),
            offset,
            expected_version,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn delete_range(
        &self,
        path: &str,
        offset: usize,
        len: usize,
        expected_version: u64,
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::DeleteRange {
            path: path.to_string(),
            offset,
            len,
            expected_version,
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn replace_range(
        &self,
        path: &str,
        offset: usize,
        len: usize,
        expected_version: u64,
        bytes: &[u8],
    ) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::ReplaceRange {
            path: path.to_string(),
            offset,
            len,
            expected_version,
            bytes: bytes.to_vec(),
        })?;
        match response {
            DaemonResponse::TxId(tx_id) => Ok(tx_id),
            _ => Err(FuseError::Io),
        }
    }

    pub fn file_size(&self, path: &str) -> Result<usize, FuseError> {
        let response = self.request(DaemonRequest::FileSize {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Count(size) => Ok(size),
            _ => Err(FuseError::Io),
        }
    }

    pub fn file_hash(&self, path: &str) -> Result<String, FuseError> {
        let response = self.request(DaemonRequest::FileHash {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Text(hash) => Ok(hash),
            _ => Err(FuseError::Io),
        }
    }

    pub fn unlink(&self, path: &str, expected_version: u64) -> Result<(), FuseError> {
        let response = self.request(DaemonRequest::Unlink {
            path: path.to_string(),
            expected_version,
        })?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    pub fn mkdir(&self, path: &str) -> Result<(), FuseError> {
        let response = self.request(DaemonRequest::Mkdir {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    pub fn mkdir_p(&self, path: &str) -> Result<usize, FuseError> {
        let response = self.request(DaemonRequest::MkdirAll {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Count(count) => Ok(count),
            _ => Err(FuseError::Io),
        }
    }

    pub fn readdir(&self, path: &str) -> Result<Vec<String>, FuseError> {
        let response = self.request(DaemonRequest::ReadDir {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Names(names) => Ok(names),
            _ => Err(FuseError::Io),
        }
    }

    pub fn readdir_with_kinds(&self, path: &str) -> Result<Vec<FuseDirEntry>, FuseError> {
        let response = self.request(DaemonRequest::ReadDirDetailed {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::DirEntries(entries) => Ok(entries),
            _ => Err(FuseError::Io),
        }
    }

    pub fn walk_dir(&self, path: &str) -> Result<Vec<FusePathEntry>, FuseError> {
        let response = self.request(DaemonRequest::WalkDir {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::PathEntries(entries) => Ok(entries),
            _ => Err(FuseError::Io),
        }
    }

    pub fn tree_summary(&self, path: &str) -> Result<FuseTreeSummary, FuseError> {
        let response = self.request(DaemonRequest::TreeSummary {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::TreeSummary(summary) => Ok(summary),
            _ => Err(FuseError::Io),
        }
    }

    pub fn tree_bytes(&self, path: &str) -> Result<usize, FuseError> {
        let response = self.request(DaemonRequest::TreeBytes {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Count(bytes) => Ok(bytes),
            _ => Err(FuseError::Io),
        }
    }

    pub fn run_background_once(&self) -> Result<Vec<FuseGcReport>, FuseError> {
        let response = self.request(DaemonRequest::RunBackgroundOnce)?;
        match response {
            DaemonResponse::GcReports(reports) => Ok(reports),
            _ => Err(FuseError::Io),
        }
    }

    pub fn gc_scan_once(&self) -> Result<Vec<FuseGcReport>, FuseError> {
        let response = self.request(DaemonRequest::GcScanOnce)?;
        match response {
            DaemonResponse::GcReports(reports) => Ok(reports),
            _ => Err(FuseError::Io),
        }
    }

    pub fn enqueue_gc_scan(&self) -> Result<bool, FuseError> {
        let response = self.request(DaemonRequest::EnqueueGcScan)?;
        match response {
            DaemonResponse::Bool(value) => Ok(value),
            _ => Err(FuseError::Io),
        }
    }

    pub fn exists(&self, path: &str) -> Result<bool, FuseError> {
        let response = self.request(DaemonRequest::Exists {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Bool(value) => Ok(value),
            _ => Err(FuseError::Io),
        }
    }

    pub fn stat(&self, path: &str) -> Result<FuseNodeKind, FuseError> {
        let response = self.request(DaemonRequest::Stat {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Kind(kind) => Ok(kind),
            _ => Err(FuseError::Io),
        }
    }

    pub fn rmdir(&self, path: &str) -> Result<(), FuseError> {
        let response = self.request(DaemonRequest::RemoveDir {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    pub fn rmtree(&self, path: &str) -> Result<usize, FuseError> {
        let response = self.request(DaemonRequest::RemoveTree {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Count(count) => Ok(count),
            _ => Err(FuseError::Io),
        }
    }

    pub fn remove_path(&self, path: &str) -> Result<usize, FuseError> {
        let response = self.request(DaemonRequest::RemovePath {
            path: path.to_string(),
        })?;
        match response {
            DaemonResponse::Count(count) => Ok(count),
            _ => Err(FuseError::Io),
        }
    }

    pub fn rename(&self, src: &str, dst: &str) -> Result<(), FuseError> {
        let response = self.request(DaemonRequest::Rename {
            src: src.to_string(),
            dst: dst.to_string(),
        })?;
        match response {
            DaemonResponse::Empty => Ok(()),
            _ => Err(FuseError::Io),
        }
    }

    fn request(&self, request: DaemonRequest) -> Result<DaemonResponse, FuseError> {
        self.request_with_timeout(request, self.request_timeout)
    }

    fn request_blocking(&self, request: DaemonRequest) -> Result<DaemonResponse, FuseError> {
        self.request_with_timeout(request, None)
    }

    fn request_with_timeout(
        &self,
        request: DaemonRequest,
        timeout: Option<Duration>,
    ) -> Result<DaemonResponse, FuseError> {
        if !self.try_acquire_request_slot() {
            return Err(FuseError::Unavailable);
        }
        let (reply_tx, reply_rx) = mpsc::channel();
        if self
            .request_tx
            .send(DaemonEnvelope { request, reply_tx })
            .is_err()
        {
            decrement_pending(&self.pending_requests);
            return Err(FuseError::Unavailable);
        }
        match timeout {
            Some(timeout) => reply_rx
                .recv_timeout(timeout)
                .map_err(|e| match e {
                    RecvTimeoutError::Timeout => FuseError::Timeout,
                    RecvTimeoutError::Disconnected => FuseError::Unavailable,
                })?,
            None => reply_rx.recv().map_err(|_| FuseError::Unavailable)?,
        }
    }

    fn try_acquire_request_slot(&self) -> bool {
        let limit = self.max_pending_requests.load(Ordering::Acquire).max(1);
        let mut current = self.pending_requests.load(Ordering::Acquire);
        loop {
            if current >= limit {
                return false;
            }
            match self.pending_requests.compare_exchange_weak(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for FuseDaemon {
    fn drop(&mut self) {
        let (reply_tx, _reply_rx) = mpsc::channel();
        let _ = self.request_tx.send(DaemonEnvelope {
            request: DaemonRequest::Shutdown,
            reply_tx,
        });
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl<C, M, K> FuseApi<C, M, K>
where
    C: ChunkStore + Send + Sync + 'static,
    M: MetadataCommit + MetadataRead + MetadataDelete + MetadataNamespace + Send + Sync + 'static,
    K: ChunkCache + Send + Sync + 'static,
{
    pub fn spawn_daemon(self) -> FuseDaemon {
        self.spawn_daemon_with_limits(DEFAULT_MAX_PENDING_REQUESTS)
    }

    pub fn spawn_daemon_with_limits(self, max_pending_requests: usize) -> FuseDaemon {
        let (request_tx, request_rx) = mpsc::channel();
        let pending_requests = Arc::new(AtomicUsize::new(0));
        let pending_for_worker = Arc::clone(&pending_requests);
        let worker = thread::spawn(move || daemon_loop(self, request_rx, pending_for_worker));
        FuseDaemon {
            request_tx,
            worker: Some(worker),
            request_timeout: None,
            pending_requests,
            max_pending_requests: Arc::new(AtomicUsize::new(max_pending_requests.max(1))),
        }
    }
}

fn daemon_loop<C, M, K>(
    api: FuseApi<C, M, K>,
    request_rx: Receiver<DaemonEnvelope>,
    pending_requests: Arc<AtomicUsize>,
)
where
    C: ChunkStore + Send + Sync + 'static,
    M: MetadataCommit + MetadataRead + MetadataDelete + MetadataNamespace + Send + Sync + 'static,
    K: ChunkCache + Send + Sync + 'static,
{
    while let Ok(envelope) = request_rx.recv() {
        let result = match envelope.request {
            DaemonRequest::Health => Ok(DaemonResponse::Empty),
            DaemonRequest::HealthDelay { millis } => {
                thread::sleep(Duration::from_millis(millis));
                Ok(DaemonResponse::Empty)
            }
            DaemonRequest::IsMountOpen => Ok(DaemonResponse::Bool(api.is_mount_open())),
            DaemonRequest::EnqueueGcScan => api.enqueue_gc_scan().map(DaemonResponse::Bool),
            DaemonRequest::StartupRecover => api.startup_recover().map(|_| DaemonResponse::Empty),
            DaemonRequest::Open { path } => api.open(&path).map(|_| DaemonResponse::Empty),
            DaemonRequest::Read { path, offset, size } => {
                api.read(&path, offset, size).map(DaemonResponse::Bytes)
            }
            DaemonRequest::ReadAll { path } => api.read_all(&path).map(DaemonResponse::Bytes),
            DaemonRequest::Write {
                path,
                expected_version,
                bytes,
            } => api
                .write(&path, expected_version, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfVersion {
                path,
                expected_version,
                bytes,
            } => api
                .write_if_version(&path, expected_version, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::CompareAndSwapFile {
                path,
                expected_version,
                expected_bytes,
                bytes,
            } => api
                .compare_and_swap_file(&path, expected_version, &expected_bytes, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfHash {
                path,
                expected_version,
                expected_hash,
                bytes,
            } => api
                .write_if_hash(&path, expected_version, &expected_hash, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSize {
                path,
                expected_version,
                expected_size,
                bytes,
            } => api
                .write_if_size(&path, expected_version, expected_size, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfExists { path, bytes } => {
                api.write_if_exists(&path, &bytes).map(DaemonResponse::TxId)
            }
            DaemonRequest::WriteIfEmpty { path, bytes } => {
                api.write_if_empty(&path, &bytes).map(DaemonResponse::TxId)
            }
            DaemonRequest::WriteIfNotEmpty { path, bytes } => {
                api.write_if_not_empty(&path, &bytes).map(DaemonResponse::TxId)
            }
            DaemonRequest::WriteIfStartsWith {
                path,
                required_prefix,
                bytes,
            } => api
                .write_if_starts_with(&path, &required_prefix, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfEndsWith {
                path,
                required_suffix,
                bytes,
            } => api
                .write_if_ends_with(&path, &required_suffix, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfContains {
                path,
                required_subsequence,
                bytes,
            } => api
                .write_if_contains(&path, &required_subsequence, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfNotContains {
                path,
                forbidden_subsequence,
                bytes,
            } => api
                .write_if_not_contains(&path, &forbidden_subsequence, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfExact {
                path,
                expected_bytes,
                bytes,
            } => api
                .write_if_exact(&path, &expected_bytes, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfNotExact {
                path,
                forbidden_bytes,
                bytes,
            } => api
                .write_if_not_exact(&path, &forbidden_bytes, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfMinSize {
                path,
                min_size,
                bytes,
            } => api
                .write_if_min_size(&path, min_size, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfMaxSize {
                path,
                max_size,
                bytes,
            } => api
                .write_if_max_size(&path, max_size, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeBetween {
                path,
                min_size,
                max_size,
                bytes,
            } => api
                .write_if_size_between(&path, min_size, max_size, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotBetween {
                path,
                min_size,
                max_size,
                bytes,
            } => api
                .write_if_size_not_between(&path, min_size, max_size, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeMultipleOf {
                path,
                divisor,
                bytes,
            } => api
                .write_if_size_multiple_of(&path, divisor, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotMultipleOf {
                path,
                divisor,
                bytes,
            } => api
                .write_if_size_not_multiple_of(&path, divisor, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeOdd { path, bytes } => {
                api.write_if_size_odd(&path, &bytes).map(DaemonResponse::TxId)
            }
            DaemonRequest::WriteIfSizeEven { path, bytes } => {
                api.write_if_size_even(&path, &bytes).map(DaemonResponse::TxId)
            }
            DaemonRequest::WriteIfSizePowerOfTwo { path, bytes } => api
                .write_if_size_power_of_two(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwo { path, bytes } => api
                .write_if_size_not_power_of_two(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePrime { path, bytes } => {
                api.write_if_size_prime(&path, &bytes).map(DaemonResponse::TxId)
            }
            DaemonRequest::WriteIfSizeNotPrime { path, bytes } => api
                .write_if_size_not_prime(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeFibonacci { path, bytes } => api
                .write_if_size_fibonacci(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotFibonacci { path, bytes } => api
                .write_if_size_not_fibonacci(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeSquare { path, bytes } => api
                .write_if_size_square(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotSquare { path, bytes } => api
                .write_if_size_not_square(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeCube { path, bytes } => api
                .write_if_size_cube(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotCube { path, bytes } => api
                .write_if_size_not_cube(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeTriangular { path, bytes } => api
                .write_if_size_triangular(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotTriangular { path, bytes } => api
                .write_if_size_not_triangular(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeFactorial { path, bytes } => api
                .write_if_size_factorial(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotFactorial { path, bytes } => api
                .write_if_size_not_factorial(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeComposite { path, bytes } => api
                .write_if_size_composite(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotComposite { path, bytes } => api
                .write_if_size_not_composite(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePerfect { path, bytes } => api
                .write_if_size_perfect(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPerfect { path, bytes } => api
                .write_if_size_not_perfect(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeAbundant { path, bytes } => api
                .write_if_size_abundant(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotAbundant { path, bytes } => api
                .write_if_size_not_abundant(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeDeficient { path, bytes } => api
                .write_if_size_deficient(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotDeficient { path, bytes } => api
                .write_if_size_not_deficient(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeSemiprime { path, bytes } => api
                .write_if_size_semiprime(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotSemiprime { path, bytes } => api
                .write_if_size_not_semiprime(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePalindrome { path, bytes } => api
                .write_if_size_palindrome(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPalindrome { path, bytes } => api
                .write_if_size_not_palindrome(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeArmstrong { path, bytes } => api
                .write_if_size_armstrong(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotArmstrong { path, bytes } => api
                .write_if_size_not_armstrong(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeHappy { path, bytes } => api
                .write_if_size_happy(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotHappy { path, bytes } => api
                .write_if_size_not_happy(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeAutomorphic { path, bytes } => api
                .write_if_size_automorphic(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotAutomorphic { path, bytes } => api
                .write_if_size_not_automorphic(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeHarshad { path, bytes } => api
                .write_if_size_harshad(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotHarshad { path, bytes } => api
                .write_if_size_not_harshad(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeKaprekar { path, bytes } => api
                .write_if_size_kaprekar(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotKaprekar { path, bytes } => api
                .write_if_size_not_kaprekar(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeRepdigit { path, bytes } => api
                .write_if_size_repdigit(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotRepdigit { path, bytes } => api
                .write_if_size_not_repdigit(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeTribonacci { path, bytes } => api
                .write_if_size_tribonacci(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotTribonacci { path, bytes } => api
                .write_if_size_not_tribonacci(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePell { path, bytes } => api
                .write_if_size_pell(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPell { path, bytes } => api
                .write_if_size_not_pell(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeLucas { path, bytes } => api
                .write_if_size_lucas(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotLucas { path, bytes } => api
                .write_if_size_not_lucas(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeMersenne { path, bytes } => api
                .write_if_size_mersenne(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotMersenne { path, bytes } => api
                .write_if_size_not_mersenne(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThree { path, bytes } => api
                .write_if_size_power_of_three(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThree { path, bytes } => api
                .write_if_size_not_power_of_three(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfFour { path, bytes } => api
                .write_if_size_power_of_four(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfFour { path, bytes } => api
                .write_if_size_not_power_of_four(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfFive { path, bytes } => api
                .write_if_size_power_of_five(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfFive { path, bytes } => api
                .write_if_size_not_power_of_five(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfSix { path, bytes } => api
                .write_if_size_power_of_six(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfSix { path, bytes } => api
                .write_if_size_not_power_of_six(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfSeven { path, bytes } => api
                .write_if_size_power_of_seven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfSeven { path, bytes } => api
                .write_if_size_not_power_of_seven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfEight { path, bytes } => api
                .write_if_size_power_of_eight(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfEight { path, bytes } => api
                .write_if_size_not_power_of_eight(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfNine { path, bytes } => api
                .write_if_size_power_of_nine(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfNine { path, bytes } => api
                .write_if_size_not_power_of_nine(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTen { path, bytes } => api
                .write_if_size_power_of_ten(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTen { path, bytes } => api
                .write_if_size_not_power_of_ten(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfEleven { path, bytes } => api
                .write_if_size_power_of_eleven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfEleven { path, bytes } => api
                .write_if_size_not_power_of_eleven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwelve { path, bytes } => api
                .write_if_size_power_of_twelve(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwelve { path, bytes } => api
                .write_if_size_not_power_of_twelve(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirteen { path, bytes } => api
                .write_if_size_power_of_thirteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirteen { path, bytes } => api
                .write_if_size_not_power_of_thirteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfFourteen { path, bytes } => api
                .write_if_size_power_of_fourteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfFourteen { path, bytes } => api
                .write_if_size_not_power_of_fourteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfFifteen { path, bytes } => api
                .write_if_size_power_of_fifteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfFifteen { path, bytes } => api
                .write_if_size_not_power_of_fifteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfSixteen { path, bytes } => api
                .write_if_size_power_of_sixteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfSixteen { path, bytes } => api
                .write_if_size_not_power_of_sixteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfSeventeen { path, bytes } => api
                .write_if_size_power_of_seventeen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfSeventeen { path, bytes } => api
                .write_if_size_not_power_of_seventeen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfEighteen { path, bytes } => api
                .write_if_size_power_of_eighteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfEighteen { path, bytes } => api
                .write_if_size_not_power_of_eighteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfNineteen { path, bytes } => api
                .write_if_size_power_of_nineteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfNineteen { path, bytes } => api
                .write_if_size_not_power_of_nineteen(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwenty { path, bytes } => api
                .write_if_size_power_of_twenty(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwenty { path, bytes } => api
                .write_if_size_not_power_of_twenty(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentyOne { path, bytes } => api
                .write_if_size_power_of_twenty_one(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentyOne { path, bytes } => api
                .write_if_size_not_power_of_twenty_one(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentyTwo { path, bytes } => api
                .write_if_size_power_of_twenty_two(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentyTwo { path, bytes } => api
                .write_if_size_not_power_of_twenty_two(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentyThree { path, bytes } => api
                .write_if_size_power_of_twenty_three(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentyThree { path, bytes } => api
                .write_if_size_not_power_of_twenty_three(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentyFour { path, bytes } => api
                .write_if_size_power_of_twenty_four(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentyFour { path, bytes } => api
                .write_if_size_not_power_of_twenty_four(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentyFive { path, bytes } => api
                .write_if_size_power_of_twenty_five(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentyFive { path, bytes } => api
                .write_if_size_not_power_of_twenty_five(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentySix { path, bytes } => api
                .write_if_size_power_of_twenty_six(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentySix { path, bytes } => api
                .write_if_size_not_power_of_twenty_six(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentySeven { path, bytes } => api
                .write_if_size_power_of_twenty_seven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentySeven { path, bytes } => api
                .write_if_size_not_power_of_twenty_seven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentyEight { path, bytes } => api
                .write_if_size_power_of_twenty_eight(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentyEight { path, bytes } => api
                .write_if_size_not_power_of_twenty_eight(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfTwentyNine { path, bytes } => api
                .write_if_size_power_of_twenty_nine(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfTwentyNine { path, bytes } => api
                .write_if_size_not_power_of_twenty_nine(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirty { path, bytes } => api
                .write_if_size_power_of_thirty(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirty { path, bytes } => api
                .write_if_size_not_power_of_thirty(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtyOne { path, bytes } => api
                .write_if_size_power_of_thirty_one(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtyOne { path, bytes } => api
                .write_if_size_not_power_of_thirty_one(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtyTwo { path, bytes } => api
                .write_if_size_power_of_thirty_two(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtyTwo { path, bytes } => api
                .write_if_size_not_power_of_thirty_two(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtyThree { path, bytes } => api
                .write_if_size_power_of_thirty_three(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtyThree { path, bytes } => api
                .write_if_size_not_power_of_thirty_three(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtyFour { path, bytes } => api
                .write_if_size_power_of_thirty_four(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtyFour { path, bytes } => api
                .write_if_size_not_power_of_thirty_four(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtyFive { path, bytes } => api
                .write_if_size_power_of_thirty_five(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtyFive { path, bytes } => api
                .write_if_size_not_power_of_thirty_five(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtySix { path, bytes } => api
                .write_if_size_power_of_thirty_six(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtySix { path, bytes } => api
                .write_if_size_not_power_of_thirty_six(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtySeven { path, bytes } => api
                .write_if_size_power_of_thirty_seven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtySeven { path, bytes } => api
                .write_if_size_not_power_of_thirty_seven(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtyEight { path, bytes } => api
                .write_if_size_power_of_thirty_eight(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtyEight { path, bytes } => api
                .write_if_size_not_power_of_thirty_eight(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizePowerOfThirtyNine { path, bytes } => api
                .write_if_size_power_of_thirty_nine(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfSizeNotPowerOfThirtyNine { path, bytes } => api
                .write_if_size_not_power_of_thirty_nine(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::WriteIfMissing { path, bytes } => api
                .write_if_missing(&path, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::EnsureFile { path } => api.ensure_file(&path).map(DaemonResponse::Bool),
            DaemonRequest::CopyFile {
                src,
                dst,
                expected_version,
            } => api
                .copy_file(&src, &dst, expected_version)
                .map(DaemonResponse::TxId),
            DaemonRequest::TouchFile {
                path,
                expected_version,
            } => api
                .touch_file(&path, expected_version)
                .map(DaemonResponse::TxId),
            DaemonRequest::TruncateFile {
                path,
                new_size,
                expected_version,
            } => api
                .truncate_file(&path, new_size, expected_version)
                .map(DaemonResponse::TxId),
            DaemonRequest::AppendFile {
                path,
                expected_version,
                bytes,
            } => api
                .append_file(&path, expected_version, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::OverwriteRange {
                path,
                offset,
                expected_version,
                bytes,
            } => api
                .overwrite_range(&path, offset, expected_version, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::InsertRange {
                path,
                offset,
                expected_version,
                bytes,
            } => api
                .insert_range(&path, offset, expected_version, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::DeleteRange {
                path,
                offset,
                len,
                expected_version,
            } => api
                .delete_range(&path, offset, len, expected_version)
                .map(DaemonResponse::TxId),
            DaemonRequest::ReplaceRange {
                path,
                offset,
                len,
                expected_version,
                bytes,
            } => api
                .replace_range(&path, offset, len, expected_version, &bytes)
                .map(DaemonResponse::TxId),
            DaemonRequest::FileSize { path } => api.file_size(&path).map(DaemonResponse::Count),
            DaemonRequest::FileHash { path } => api.file_hash(&path).map(DaemonResponse::Text),
            DaemonRequest::Unlink {
                path,
                expected_version,
            } => api
                .unlink(&path, expected_version)
                .map(|_| DaemonResponse::Empty),
            DaemonRequest::RemoveDir { path } => api.rmdir(&path).map(|_| DaemonResponse::Empty),
            DaemonRequest::RemoveTree { path } => api.rmtree(&path).map(DaemonResponse::Count),
            DaemonRequest::RemovePath { path } => api.remove_path(&path).map(DaemonResponse::Count),
            DaemonRequest::Mkdir { path } => api.mkdir(&path).map(|_| DaemonResponse::Empty),
            DaemonRequest::MkdirAll { path } => api.mkdir_p(&path).map(DaemonResponse::Count),
            DaemonRequest::ReadDir { path } => api.readdir(&path).map(DaemonResponse::Names),
            DaemonRequest::ReadDirDetailed { path } => {
                api.readdir_with_kinds(&path).map(DaemonResponse::DirEntries)
            }
            DaemonRequest::WalkDir { path } => api.walk_dir(&path).map(DaemonResponse::PathEntries),
            DaemonRequest::TreeSummary { path } => {
                api.tree_summary(&path).map(DaemonResponse::TreeSummary)
            }
            DaemonRequest::TreeBytes { path } => api.tree_bytes(&path).map(DaemonResponse::Count),
            DaemonRequest::RunBackgroundOnce => api
                .run_background_once()
                .map(DaemonResponse::GcReports),
            DaemonRequest::GcScanOnce => api.gc_scan_once().map(DaemonResponse::GcReports),
            DaemonRequest::Exists { path } => api.exists(&path).map(DaemonResponse::Bool),
            DaemonRequest::Stat { path } => api.stat(&path).map(DaemonResponse::Kind),
            DaemonRequest::Rename { src, dst } => api.rename(&src, &dst).map(|_| DaemonResponse::Empty),
            DaemonRequest::Shutdown => {
                let _ = envelope.reply_tx.send(Ok(DaemonResponse::Empty));
                decrement_pending(&pending_requests);
                break;
            }
        };
        let _ = envelope.reply_tx.send(result);
        decrement_pending(&pending_requests);
    }
}

fn decrement_pending(counter: &AtomicUsize) {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if current == 0 {
            return;
        }
        match counter.compare_exchange_weak(current, current - 1, Ordering::SeqCst, Ordering::Acquire) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn map_core_error(error: CoreError) -> FuseError {
    match error {
        CoreError::MountGateClosed => FuseError::Unavailable,
        CoreError::InvalidRange => FuseError::InvalidRange,
        CoreError::NotFound => FuseError::NotFound,
        CoreError::NotEmpty => FuseError::Conflict,
        CoreError::Conflict => FuseError::Conflict,
        CoreError::IntegrityMismatch => FuseError::Io,
        CoreError::Busy => FuseError::Conflict,
        CoreError::Wal(_)
        | CoreError::ChunkStore(_)
        | CoreError::Cache(_)
        | CoreError::Gc(_)
        | CoreError::Staging(_)
        | CoreError::Io(_) => FuseError::Io,
        CoreError::Metadata(message) => {
            if message.contains("cas conflict") {
                FuseError::Conflict
            } else if message.contains("parent directory does not exist") {
                FuseError::NotFound
            } else if message.contains("invalid namespace path") {
                FuseError::InvalidPath
            } else {
                FuseError::Io
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache::TwoTierChunkCache;
    use chunk_store::FsChunkStore;
    use metadata::{InMemoryMetadataHook, ReadView};
    use std::fs;
    use std::io::{Seek, Write};
    use std::sync::Arc;
    use std::time::Duration;
    use wal::{WalLog, WritePipeline};

    struct AlwaysConflictWriteMetadata;

    impl wal::MetadataCommit for AlwaysConflictWriteMetadata {
        fn commit_write(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
            _chunk_ids: &[String],
            _chunk_hashes: &[String],
        ) -> Result<(), String> {
            Err("cas conflict".to_string())
        }
    }

    impl wal::MetadataDelete for AlwaysConflictWriteMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl metadata::MetadataRead for AlwaysConflictWriteMetadata {
        fn read_committed(&self, _file_path: &str) -> Result<Option<ReadView>, String> {
            Ok(Some(ReadView {
                version: 0,
                chunk_ids: Vec::new(),
                chunk_hashes: Vec::new(),
            }))
        }
    }

    impl metadata::MetadataNamespace for AlwaysConflictWriteMetadata {
        fn create_directory(&self, _path: &str) -> Result<(), String> {
            Err("directory not found".to_string())
        }

        fn list_children(&self, _path: &str) -> Result<Vec<String>, String> {
            Err("directory not found".to_string())
        }

        fn remove_directory(&self, _path: &str) -> Result<(), String> {
            Err("directory not found".to_string())
        }

        fn rename_path(&self, _src: &str, _dst: &str) -> Result<(), String> {
            Err("path not found".to_string())
        }
    }

    struct AlwaysConflictDeleteMetadata;

    impl wal::MetadataCommit for AlwaysConflictDeleteMetadata {
        fn commit_write(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
            _chunk_ids: &[String],
            _chunk_hashes: &[String],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    impl wal::MetadataDelete for AlwaysConflictDeleteMetadata {
        fn commit_delete(
            &self,
            _tx_id: &str,
            _file_path: &str,
            _expected_version: u64,
        ) -> Result<(), String> {
            Err("cas conflict".to_string())
        }
    }

    impl metadata::MetadataRead for AlwaysConflictDeleteMetadata {
        fn read_committed(&self, _file_path: &str) -> Result<Option<ReadView>, String> {
            Ok(Some(ReadView {
                version: 1,
                chunk_ids: vec!["dummy".to_string()],
                chunk_hashes: vec!["dummy".to_string()],
            }))
        }
    }

    impl metadata::MetadataNamespace for AlwaysConflictDeleteMetadata {
        fn create_directory(&self, _path: &str) -> Result<(), String> {
            Err("directory not found".to_string())
        }

        fn list_children(&self, _path: &str) -> Result<Vec<String>, String> {
            Err("directory not found".to_string())
        }

        fn remove_directory(&self, _path: &str) -> Result<(), String> {
            Err("directory not found".to_string())
        }

        fn rename_path(&self, _src: &str, _dst: &str) -> Result<(), String> {
            Err("path not found".to_string())
        }
    }

    #[test]
    fn fuse_api_validate_and_route() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        assert!(fuse.open("bad").is_err());
        assert!(fuse.read("/a/../b", 0, 1).is_err());

        fuse.write("/a", 0, b"hello").expect("write should pass");
        let got = fuse.read("/a", 1, 3).expect("read should pass");
        assert_eq!(got, b"ell");
        let full = fuse.read_all("/a").expect("read_all should pass");
        assert_eq!(full, b"hello");

        // Canonicalized alias path must resolve to the same object.
        let got_alias = fuse.read("//a", 1, 3).expect("canonicalized read should pass");
        assert_eq!(got_alias, b"ell");

        fuse.unlink("/a", 1).expect("unlink should pass");
        assert!(matches!(fuse.read("/a", 0, 1), Err(FuseError::NotFound)));
    }

    #[test]
    fn fuse_api_health_probe_is_available_before_and_after_recovery() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.health().expect("health should pass before recovery");
        fuse.health_with_delay(Duration::from_millis(0))
            .expect("delayed health should pass before recovery");
        fuse.startup_recover().expect("recovery should pass");
        fuse.health().expect("health should pass after recovery");
        fuse.health_with_delay(Duration::from_millis(0))
            .expect("delayed health should pass after recovery");
    }

    #[test]
    fn fuse_api_mkdir_and_readdir() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");

        let root = fuse.readdir("/").expect("root list should pass");
        assert_eq!(root, vec!["docs".to_string()]);
        let docs = fuse.readdir("/docs").expect("docs list should pass");
        assert_eq!(docs, vec!["readme.txt".to_string()]);

        fuse.unlink("/docs/readme.txt", 1).expect("unlink should pass");
        fuse.rmdir("/docs").expect("rmdir should pass");
        let root = fuse.readdir("/").expect("root list should pass");
        assert!(root.is_empty());
    }

    #[test]
    fn fuse_api_write_if_missing_creates_and_conflicts_on_existing_path() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let tx = fuse
            .write_if_missing("/docs/new.txt", b"first")
            .expect("write_if_missing should create");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/new.txt").expect("read_all should pass");
        assert_eq!(bytes, b"first");

        let file_err = fuse
            .write_if_missing("/docs/new.txt", b"second")
            .expect_err("write_if_missing should fail when file exists");
        assert!(matches!(file_err, FuseError::Conflict));

        let dir_err = fuse
            .write_if_missing("/docs", b"second")
            .expect_err("write_if_missing should fail when directory exists");
        assert!(matches!(dir_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_version_updates_and_conflicts_on_stale_version() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"v1").expect("write should pass");
        let tx = fuse
            .write_if_version("/docs/a.txt", 1, b"v2")
            .expect("write_if_version should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");
        let err = fuse
            .write_if_version("/docs/a.txt", 1, b"v3")
            .expect_err("stale version should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_compare_and_swap_file_updates_and_conflicts() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"v1").expect("write should pass");
        let tx = fuse
            .compare_and_swap_file("/docs/a.txt", 1, b"v1", b"v2")
            .expect("compare_and_swap_file should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let content_err = fuse
            .compare_and_swap_file("/docs/a.txt", 2, b"other", b"v3")
            .expect_err("content mismatch should fail");
        assert!(matches!(content_err, FuseError::Conflict));

        let version_err = fuse
            .compare_and_swap_file("/docs/a.txt", 1, b"v2", b"v3")
            .expect_err("stale version should fail");
        assert!(matches!(version_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_hash_updates_and_conflicts() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"v1").expect("write should pass");
        let expected_hash = fuse.file_hash("/docs/a.txt").expect("hash should pass");
        let tx = fuse
            .write_if_hash("/docs/a.txt", 1, &expected_hash, b"v2")
            .expect("write_if_hash should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let hash_err = fuse
            .write_if_hash("/docs/a.txt", 2, "deadbeef", b"v3")
            .expect_err("hash mismatch should fail");
        assert!(matches!(hash_err, FuseError::Conflict));

        let version_err = fuse
            .write_if_hash(
                "/docs/a.txt",
                1,
                &fuse.file_hash("/docs/a.txt").expect("hash should pass"),
                b"v3",
            )
            .expect_err("stale version should fail");
        assert!(matches!(version_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_updates_and_conflicts() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"v1").expect("write should pass");
        let size = fuse.file_size("/docs/a.txt").expect("size should pass");
        let tx = fuse
            .write_if_size("/docs/a.txt", 1, size, b"v2")
            .expect("write_if_size should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let size_err = fuse
            .write_if_size("/docs/a.txt", 2, 7, b"v3")
            .expect_err("size mismatch should fail");
        assert!(matches!(size_err, FuseError::Conflict));

        let version_err = fuse
            .write_if_size(
                "/docs/a.txt",
                1,
                fuse.file_size("/docs/a.txt").expect("size should pass"),
                b"v3",
            )
            .expect_err("stale version should fail");
        assert!(matches!(version_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_exists_updates_and_rejects_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"v1").expect("write should pass");

        let tx = fuse
            .write_if_exists("/docs/a.txt", b"v2")
            .expect("write_if_exists should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let missing_err = fuse
            .write_if_exists("/docs/missing.txt", b"v2")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_exists("/docs", b"v2")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_empty_updates_and_rejects_non_empty_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.touch_file("/docs/empty.txt", 0).expect("touch should pass");

        let tx = fuse
            .write_if_empty("/docs/empty.txt", b"v1")
            .expect("write_if_empty should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/empty.txt").expect("read should pass");
        assert_eq!(bytes, b"v1");

        let non_empty_err = fuse
            .write_if_empty("/docs/empty.txt", b"v2")
            .expect_err("non-empty file should fail");
        assert!(matches!(non_empty_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_empty("/docs/missing.txt", b"v2")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_empty("/docs", b"v2")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_not_empty_updates_and_rejects_empty_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"v1").expect("write should pass");

        let tx = fuse
            .write_if_not_empty("/docs/a.txt", b"v2")
            .expect("write_if_not_empty should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        fuse.touch_file("/docs/empty.txt", 0).expect("touch should pass");
        let empty_err = fuse
            .write_if_not_empty("/docs/empty.txt", b"v2")
            .expect_err("empty file should fail");
        assert!(matches!(empty_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_not_empty("/docs/missing.txt", b"v2")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_not_empty("/docs", b"v2")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_starts_with_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"prefix-value")
            .expect("write should pass");

        let tx = fuse
            .write_if_starts_with("/docs/a.txt", b"prefix-", b"next-value")
            .expect("write_if_starts_with should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = fuse
            .write_if_starts_with("/docs/a.txt", b"other-", b"again")
            .expect_err("prefix mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_starts_with("/docs/missing.txt", b"prefix-", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_starts_with("/docs", b"prefix-", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_ends_with_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"value-suffix")
            .expect("write should pass");

        let tx = fuse
            .write_if_ends_with("/docs/a.txt", b"-suffix", b"next-value")
            .expect("write_if_ends_with should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = fuse
            .write_if_ends_with("/docs/a.txt", b"-other", b"again")
            .expect_err("suffix mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_ends_with("/docs/missing.txt", b"-suffix", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_ends_with("/docs", b"-suffix", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_contains_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"alpha-beta-gamma")
            .expect("write should pass");

        let tx = fuse
            .write_if_contains("/docs/a.txt", b"beta", b"next-value")
            .expect("write_if_contains should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = fuse
            .write_if_contains("/docs/a.txt", b"delta", b"again")
            .expect_err("contains mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_contains("/docs/missing.txt", b"beta", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_contains("/docs", b"beta", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_not_contains_updates_and_rejects_match_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"alpha-beta-gamma")
            .expect("write should pass");

        let tx = fuse
            .write_if_not_contains("/docs/a.txt", b"delta", b"next-value")
            .expect("write_if_not_contains should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let match_err = fuse
            .write_if_not_contains("/docs/a.txt", b"value", b"again")
            .expect_err("subsequence present should fail");
        assert!(matches!(match_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_not_contains("/docs/missing.txt", b"beta", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_not_contains("/docs", b"beta", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_exact_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"expected-body")
            .expect("write should pass");

        let tx = fuse
            .write_if_exact("/docs/a.txt", b"expected-body", b"next-value")
            .expect("write_if_exact should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = fuse
            .write_if_exact("/docs/a.txt", b"other-body", b"again")
            .expect_err("mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_exact("/docs/missing.txt", b"expected-body", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_exact("/docs", b"expected-body", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_not_exact_updates_and_rejects_exact_match_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"current-body")
            .expect("write should pass");

        let tx = fuse
            .write_if_not_exact("/docs/a.txt", b"blocked-body", b"next-value")
            .expect("write_if_not_exact should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let match_err = fuse
            .write_if_not_exact("/docs/a.txt", b"next-value", b"again")
            .expect_err("exact forbidden match should fail");
        assert!(matches!(match_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_not_exact("/docs/missing.txt", b"current-body", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_not_exact("/docs", b"current-body", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_min_size_updates_and_rejects_small_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abcdef").expect("write should pass");

        let tx = fuse
            .write_if_min_size("/docs/a.txt", 6, b"next-value")
            .expect("write_if_min_size should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let small_err = fuse
            .write_if_min_size("/docs/a.txt", 20, b"again")
            .expect_err("file smaller than threshold should fail");
        assert!(matches!(small_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_min_size("/docs/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_min_size("/docs", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_max_size_updates_and_rejects_large_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abc").expect("write should pass");

        let tx = fuse
            .write_if_max_size("/docs/a.txt", 3, b"next-value")
            .expect("write_if_max_size should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let large_err = fuse
            .write_if_max_size("/docs/a.txt", 5, b"again")
            .expect_err("file larger than threshold should fail");
        assert!(matches!(large_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_max_size("/docs/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_max_size("/docs", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_between_updates_and_rejects_out_of_range_invalid_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abcdef").expect("write should pass");

        let tx = fuse
            .write_if_size_between("/docs/a.txt", 3, 8, b"next-value")
            .expect("write_if_size_between should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let out_of_range_err = fuse
            .write_if_size_between("/docs/a.txt", 20, 30, b"again")
            .expect_err("out-of-range should fail");
        assert!(matches!(out_of_range_err, FuseError::Conflict));

        let invalid_range_err = fuse
            .write_if_size_between("/docs/a.txt", 10, 5, b"again")
            .expect_err("invalid range should fail");
        assert!(matches!(invalid_range_err, FuseError::InvalidRange));

        let missing_err = fuse
            .write_if_size_between("/docs/missing.txt", 1, 8, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_between("/docs", 1, 8, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_between_updates_and_rejects_in_range_invalid_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abcdef").expect("write should pass");

        let tx = fuse
            .write_if_size_not_between("/docs/a.txt", 1, 3, b"next-value")
            .expect("write_if_size_not_between should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let in_range_err = fuse
            .write_if_size_not_between("/docs/a.txt", 1, 20, b"again")
            .expect_err("in-range size should fail");
        assert!(matches!(in_range_err, FuseError::Conflict));

        let invalid_range_err = fuse
            .write_if_size_not_between("/docs/a.txt", 10, 5, b"again")
            .expect_err("invalid range should fail");
        assert!(matches!(invalid_range_err, FuseError::InvalidRange));

        let missing_err = fuse
            .write_if_size_not_between("/docs/missing.txt", 1, 8, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_between("/docs", 1, 8, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_multiple_of_updates_and_rejects_non_divisible_zero_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abcdef").expect("write should pass");

        let tx = fuse
            .write_if_size_multiple_of("/docs/a.txt", 3, b"next-value")
            .expect("write_if_size_multiple_of should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_divisible_err = fuse
            .write_if_size_multiple_of("/docs/a.txt", 7, b"again")
            .expect_err("non-divisible size should fail");
        assert!(matches!(non_divisible_err, FuseError::Conflict));

        let zero_err = fuse
            .write_if_size_multiple_of("/docs/a.txt", 0, b"again")
            .expect_err("zero divisor should fail");
        assert!(matches!(zero_err, FuseError::InvalidRange));

        let missing_err = fuse
            .write_if_size_multiple_of("/docs/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_multiple_of("/docs", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_multiple_of_updates_and_rejects_divisible_zero_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abcde").expect("write should pass");

        let tx = fuse
            .write_if_size_not_multiple_of("/docs/a.txt", 3, b"next-value")
            .expect("write_if_size_not_multiple_of should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let divisible_err = fuse
            .write_if_size_not_multiple_of("/docs/a.txt", 2, b"again")
            .expect_err("divisible size should fail");
        assert!(matches!(divisible_err, FuseError::Conflict));

        let zero_err = fuse
            .write_if_size_not_multiple_of("/docs/a.txt", 0, b"again")
            .expect_err("zero divisor should fail");
        assert!(matches!(zero_err, FuseError::InvalidRange));

        let missing_err = fuse
            .write_if_size_not_multiple_of("/docs/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_multiple_of("/docs", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_odd_updates_and_rejects_even_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abcde").expect("write should pass");

        let tx = fuse
            .write_if_size_odd("/docs/a.txt", b"next-value")
            .expect("write_if_size_odd should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let even_err = fuse
            .write_if_size_odd("/docs/a.txt", b"again")
            .expect_err("even size should fail");
        assert!(matches!(even_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_odd("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_odd("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_even_updates_and_rejects_odd_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"abcdef").expect("write should pass");

        let tx = fuse
            .write_if_size_even("/docs/a.txt", b"12345678901")
            .expect("write_if_size_even should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/a.txt").expect("read should pass");
        assert_eq!(bytes, b"12345678901");

        let odd_err = fuse
            .write_if_size_even("/docs/a.txt", b"again")
            .expect_err("odd size should fail");
        assert!(matches!(odd_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_even("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_even("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_two_updates_and_rejects_zero_non_power_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/pow2.txt", 0, b"12345678")
            .expect("write should pass");
        fuse.touch_file("/docs/zero.txt", 0).expect("touch should pass");
        fuse.write("/docs/nonpower.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_two("/docs/pow2.txt", b"next-value")
            .expect("write_if_size_power_of_two should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow2.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let zero_err = fuse
            .write_if_size_power_of_two("/docs/zero.txt", b"again")
            .expect_err("zero size should fail");
        assert!(matches!(zero_err, FuseError::Conflict));

        let non_power_err = fuse
            .write_if_size_power_of_two("/docs/nonpower.txt", b"again")
            .expect_err("non-power size should fail");
        assert!(matches!(non_power_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_two("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_two("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_two_updates_and_rejects_power_of_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonpower.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/pow2.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_two("/docs/nonpower.txt", b"next-value")
            .expect("write_if_size_not_power_of_two should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/nonpower.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let power_err = fuse
            .write_if_size_not_power_of_two("/docs/pow2.txt", b"again")
            .expect_err("power-of-two size should fail");
        assert!(matches!(power_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_two("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_two("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_prime_updates_and_rejects_non_prime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/prime.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/nonprime.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_prime("/docs/prime.txt", b"next-value")
            .expect("write_if_size_prime should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/prime.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_prime_err = fuse
            .write_if_size_prime("/docs/nonprime.txt", b"again")
            .expect_err("non-prime size should fail");
        assert!(matches!(non_prime_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_prime("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_prime("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_prime_updates_and_rejects_prime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonprime.txt", 0, b"12345678")
            .expect("write should pass");
        fuse.write("/docs/prime.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_prime("/docs/nonprime.txt", b"next-value")
            .expect("write_if_size_not_prime should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/nonprime.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let prime_err = fuse
            .write_if_size_not_prime("/docs/prime.txt", b"again")
            .expect_err("prime size should fail");
        assert!(matches!(prime_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_prime("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_prime("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_fibonacci_updates_and_rejects_non_fibonacci_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/fib.txt", 0, b"12345678")
            .expect("write should pass");
        fuse.write("/docs/nonfib.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_fibonacci("/docs/fib.txt", b"next-value")
            .expect("write_if_size_fibonacci should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/fib.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_fib_err = fuse
            .write_if_size_fibonacci("/docs/nonfib.txt", b"again")
            .expect_err("non-fibonacci size should fail");
        assert!(matches!(non_fib_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_fibonacci("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_fibonacci("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_fibonacci_updates_and_rejects_fibonacci_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonfib.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/fib.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_fibonacci("/docs/nonfib.txt", b"next-value")
            .expect("write_if_size_not_fibonacci should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/nonfib.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let fib_err = fuse
            .write_if_size_not_fibonacci("/docs/fib.txt", b"again")
            .expect_err("fibonacci size should fail");
        assert!(matches!(fib_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_fibonacci("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_fibonacci("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_square_updates_and_rejects_non_square_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/square.txt", 0, b"123456789")
            .expect("write should pass");
        fuse.write("/docs/nonsquare.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_square("/docs/square.txt", b"next-value")
            .expect("write_if_size_square should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/square.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_square_err = fuse
            .write_if_size_square("/docs/nonsquare.txt", b"again")
            .expect_err("non-square size should fail");
        assert!(matches!(non_square_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_square("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_square("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_square_updates_and_rejects_square_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonsquare.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/square.txt", 0, b"123456789")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_square("/docs/nonsquare.txt", b"next-value")
            .expect("write_if_size_not_square should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonsquare.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let square_err = fuse
            .write_if_size_not_square("/docs/square.txt", b"again")
            .expect_err("square size should fail");
        assert!(matches!(square_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_square("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_square("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_cube_updates_and_rejects_non_cube_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/cube.txt", 0, b"12345678")
            .expect("write should pass");
        fuse.write("/docs/noncube.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_cube("/docs/cube.txt", b"next-value")
            .expect("write_if_size_cube should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/cube.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_cube_err = fuse
            .write_if_size_cube("/docs/noncube.txt", b"again")
            .expect_err("non-cube size should fail");
        assert!(matches!(non_cube_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_cube("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_cube("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_cube_updates_and_rejects_cube_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/noncube.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/cube.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_cube("/docs/noncube.txt", b"next-value")
            .expect("write_if_size_not_cube should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/noncube.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let cube_err = fuse
            .write_if_size_not_cube("/docs/cube.txt", b"again")
            .expect_err("cube size should fail");
        assert!(matches!(cube_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_cube("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_cube("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_triangular_updates_and_rejects_non_triangular_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/tri.txt", 0, b"123456")
            .expect("write should pass");
        fuse.write("/docs/nontri.txt", 0, b"12345")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_triangular("/docs/tri.txt", b"next-value")
            .expect("write_if_size_triangular should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/tri.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_tri_err = fuse
            .write_if_size_triangular("/docs/nontri.txt", b"again")
            .expect_err("non-triangular size should fail");
        assert!(matches!(non_tri_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_triangular("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_triangular("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_triangular_updates_and_rejects_triangular_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nontri.txt", 0, b"12345")
            .expect("write should pass");
        fuse.write("/docs/tri.txt", 0, b"123456")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_triangular("/docs/nontri.txt", b"next-value")
            .expect("write_if_size_not_triangular should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/nontri.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let tri_err = fuse
            .write_if_size_not_triangular("/docs/tri.txt", b"again")
            .expect_err("triangular size should fail");
        assert!(matches!(tri_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_triangular("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_triangular("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_factorial_updates_and_rejects_non_factorial_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/factorial.txt", 0, b"123456")
            .expect("write should pass");
        fuse.write("/docs/nonfactorial.txt", 0, b"12345")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_factorial("/docs/factorial.txt", b"next-value")
            .expect("write_if_size_factorial should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/factorial.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_factorial_err = fuse
            .write_if_size_factorial("/docs/nonfactorial.txt", b"again")
            .expect_err("non-factorial size should fail");
        assert!(matches!(non_factorial_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_factorial("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_factorial("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_factorial_updates_and_rejects_factorial_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonfactorial.txt", 0, b"12345")
            .expect("write should pass");
        fuse.write("/docs/factorial.txt", 0, b"123456")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_factorial("/docs/nonfactorial.txt", b"next-value")
            .expect("write_if_size_not_factorial should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonfactorial.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let factorial_err = fuse
            .write_if_size_not_factorial("/docs/factorial.txt", b"again")
            .expect_err("factorial size should fail");
        assert!(matches!(factorial_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_factorial("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_factorial("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_composite_updates_and_rejects_non_composite_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/composite.txt", 0, b"12345678")
            .expect("write should pass");
        fuse.write("/docs/prime.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_composite("/docs/composite.txt", b"next-value")
            .expect("write_if_size_composite should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/composite.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_composite_err = fuse
            .write_if_size_composite("/docs/prime.txt", b"again")
            .expect_err("non-composite size should fail");
        assert!(matches!(non_composite_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_composite("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_composite("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_composite_updates_and_rejects_composite_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/prime.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/composite.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_composite("/docs/prime.txt", b"next-value")
            .expect("write_if_size_not_composite should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/prime.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let composite_err = fuse
            .write_if_size_not_composite("/docs/composite.txt", b"again")
            .expect_err("composite size should fail");
        assert!(matches!(composite_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_composite("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_composite("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_perfect_updates_and_rejects_non_perfect_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/perfect.txt", 0, b"123456")
            .expect("write should pass");
        fuse.write("/docs/nonperfect.txt", 0, b"12345")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_perfect("/docs/perfect.txt", b"next-value")
            .expect("write_if_size_perfect should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/perfect.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_perfect_err = fuse
            .write_if_size_perfect("/docs/nonperfect.txt", b"again")
            .expect_err("non-perfect size should fail");
        assert!(matches!(non_perfect_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_perfect("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_perfect("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_perfect_updates_and_rejects_perfect_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonperfect.txt", 0, b"12345")
            .expect("write should pass");
        fuse.write("/docs/perfect.txt", 0, b"123456")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_perfect("/docs/nonperfect.txt", b"next-value")
            .expect("write_if_size_not_perfect should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/nonperfect.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let perfect_err = fuse
            .write_if_size_not_perfect("/docs/perfect.txt", b"again")
            .expect_err("perfect size should fail");
        assert!(matches!(perfect_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_perfect("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_perfect("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_abundant_updates_and_rejects_non_abundant_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/abundant.txt", 0, b"123456789012")
            .expect("write should pass");
        fuse.write("/docs/nonabundant.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_abundant("/docs/abundant.txt", b"next-value")
            .expect("write_if_size_abundant should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/abundant.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_abundant_err = fuse
            .write_if_size_abundant("/docs/nonabundant.txt", b"again")
            .expect_err("non-abundant size should fail");
        assert!(matches!(non_abundant_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_abundant("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_abundant("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_abundant_updates_and_rejects_abundant_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonabundant.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/abundant.txt", 0, b"123456789012")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_abundant("/docs/nonabundant.txt", b"next-value")
            .expect("write_if_size_not_abundant should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/nonabundant.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let abundant_err = fuse
            .write_if_size_not_abundant("/docs/abundant.txt", b"again")
            .expect_err("abundant size should fail");
        assert!(matches!(abundant_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_abundant("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_abundant("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_deficient_updates_and_rejects_non_deficient_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/deficient.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/nondeficient.txt", 0, b"123456789012")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_deficient("/docs/deficient.txt", b"next-value")
            .expect("write_if_size_deficient should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/deficient.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_deficient_err = fuse
            .write_if_size_deficient("/docs/nondeficient.txt", b"again")
            .expect_err("non-deficient size should fail");
        assert!(matches!(non_deficient_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_deficient("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_deficient("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_deficient_updates_and_rejects_deficient_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nondeficient.txt", 0, b"123456789012")
            .expect("write should pass");
        fuse.write("/docs/deficient.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_deficient("/docs/nondeficient.txt", b"next-value")
            .expect("write_if_size_not_deficient should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nondeficient.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let deficient_err = fuse
            .write_if_size_not_deficient("/docs/deficient.txt", b"again")
            .expect_err("deficient size should fail");
        assert!(matches!(deficient_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_deficient("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_deficient("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_semiprime_updates_and_rejects_non_semiprime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/semiprime.txt", 0, b"123456")
            .expect("write should pass");
        fuse.write("/docs/nonsemiprime.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_semiprime("/docs/semiprime.txt", b"next-value")
            .expect("write_if_size_semiprime should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/semiprime.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_semiprime_err = fuse
            .write_if_size_semiprime("/docs/nonsemiprime.txt", b"again")
            .expect_err("non-semiprime size should fail");
        assert!(matches!(non_semiprime_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_semiprime("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_semiprime("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_semiprime_updates_and_rejects_semiprime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonsemiprime.txt", 0, b"1234567")
            .expect("write should pass");
        fuse.write("/docs/semiprime.txt", 0, b"123456")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_semiprime("/docs/nonsemiprime.txt", b"next-value")
            .expect("write_if_size_not_semiprime should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonsemiprime.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let semiprime_err = fuse
            .write_if_size_not_semiprime("/docs/semiprime.txt", b"again")
            .expect_err("semiprime size should fail");
        assert!(matches!(semiprime_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_semiprime("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_semiprime("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_palindrome_updates_and_rejects_non_palindrome_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/palindrome.txt", 0, b"12345678901")
            .expect("write should pass");
        fuse.write("/docs/nonpalindrome.txt", 0, b"1234567890")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_palindrome("/docs/palindrome.txt", b"next-value")
            .expect("write_if_size_palindrome should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/palindrome.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_palindrome_err = fuse
            .write_if_size_palindrome("/docs/nonpalindrome.txt", b"again")
            .expect_err("non-palindrome size should fail");
        assert!(matches!(non_palindrome_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_palindrome("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_palindrome("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_palindrome_updates_and_rejects_palindrome_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/nonpalindrome.txt", 0, b"1234567890")
            .expect("write should pass");
        fuse.write("/docs/palindrome.txt", 0, b"12345678901")
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_palindrome("/docs/nonpalindrome.txt", b"next-value")
            .expect("write_if_size_not_palindrome should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpalindrome.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let palindrome_err = fuse
            .write_if_size_not_palindrome("/docs/palindrome.txt", b"again")
            .expect_err("palindrome size should fail");
        assert!(matches!(palindrome_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_palindrome("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_palindrome("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_armstrong_updates_and_rejects_non_armstrong_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let armstrong_seed = vec![b'a'; 153];
        let non_armstrong_seed = vec![b'a'; 154];
        fuse.write("/docs/armstrong.txt", 0, &armstrong_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonarmstrong.txt", 0, &non_armstrong_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_armstrong("/docs/armstrong.txt", b"next-value")
            .expect("write_if_size_armstrong should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/armstrong.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_armstrong_err = fuse
            .write_if_size_armstrong("/docs/nonarmstrong.txt", b"again")
            .expect_err("non-armstrong size should fail");
        assert!(matches!(non_armstrong_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_armstrong("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_armstrong("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_armstrong_updates_and_rejects_armstrong_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_armstrong_seed = vec![b'a'; 154];
        let armstrong_seed = vec![b'a'; 153];
        fuse.write("/docs/nonarmstrong.txt", 0, &non_armstrong_seed[..])
            .expect("write should pass");
        fuse.write("/docs/armstrong.txt", 0, &armstrong_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_armstrong("/docs/nonarmstrong.txt", b"next-value")
            .expect("write_if_size_not_armstrong should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonarmstrong.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let armstrong_err = fuse
            .write_if_size_not_armstrong("/docs/armstrong.txt", b"again")
            .expect_err("armstrong size should fail");
        assert!(matches!(armstrong_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_armstrong("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_armstrong("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_happy_updates_and_rejects_non_happy_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let happy_seed = vec![b'a'; 19];
        let non_happy_seed = vec![b'a'; 20];
        fuse.write("/docs/happy.txt", 0, &happy_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonhappy.txt", 0, &non_happy_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_happy("/docs/happy.txt", b"next-value")
            .expect("write_if_size_happy should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/happy.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_happy_err = fuse
            .write_if_size_happy("/docs/nonhappy.txt", b"again")
            .expect_err("non-happy size should fail");
        assert!(matches!(non_happy_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_happy("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_happy("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_happy_updates_and_rejects_happy_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_happy_seed = vec![b'a'; 20];
        let happy_seed = vec![b'a'; 19];
        fuse.write("/docs/nonhappy.txt", 0, &non_happy_seed[..])
            .expect("write should pass");
        fuse.write("/docs/happy.txt", 0, &happy_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_happy("/docs/nonhappy.txt", b"next-value")
            .expect("write_if_size_not_happy should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/nonhappy.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let happy_err = fuse
            .write_if_size_not_happy("/docs/happy.txt", b"again")
            .expect_err("happy size should fail");
        assert!(matches!(happy_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_happy("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_happy("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_automorphic_updates_and_rejects_non_automorphic_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let automorphic_seed = vec![b'a'; 25];
        let non_automorphic_seed = vec![b'a'; 26];
        fuse.write("/docs/automorphic.txt", 0, &automorphic_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonautomorphic.txt", 0, &non_automorphic_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_automorphic("/docs/automorphic.txt", b"next-value")
            .expect("write_if_size_automorphic should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/automorphic.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_automorphic_err = fuse
            .write_if_size_automorphic("/docs/nonautomorphic.txt", b"again")
            .expect_err("non-automorphic size should fail");
        assert!(matches!(non_automorphic_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_automorphic("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_automorphic("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_automorphic_updates_and_rejects_automorphic_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_automorphic_seed = vec![b'a'; 26];
        let automorphic_seed = vec![b'a'; 25];
        fuse.write("/docs/nonautomorphic.txt", 0, &non_automorphic_seed[..])
            .expect("write should pass");
        fuse.write("/docs/automorphic.txt", 0, &automorphic_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_automorphic("/docs/nonautomorphic.txt", b"next-value")
            .expect("write_if_size_not_automorphic should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonautomorphic.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let automorphic_err = fuse
            .write_if_size_not_automorphic("/docs/automorphic.txt", b"again")
            .expect_err("automorphic size should fail");
        assert!(matches!(automorphic_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_automorphic("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_automorphic("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_harshad_updates_and_rejects_non_harshad_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let harshad_seed = vec![b'a'; 18];
        let non_harshad_seed = vec![b'a'; 19];
        fuse.write("/docs/harshad.txt", 0, &harshad_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonharshad.txt", 0, &non_harshad_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_harshad("/docs/harshad.txt", b"next-value")
            .expect("write_if_size_harshad should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/harshad.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_harshad_err = fuse
            .write_if_size_harshad("/docs/nonharshad.txt", b"again")
            .expect_err("non-harshad size should fail");
        assert!(matches!(non_harshad_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_harshad("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_harshad("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_harshad_updates_and_rejects_harshad_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_harshad_seed = vec![b'a'; 19];
        let harshad_seed = vec![b'a'; 18];
        fuse.write("/docs/nonharshad.txt", 0, &non_harshad_seed[..])
            .expect("write should pass");
        fuse.write("/docs/harshad.txt", 0, &harshad_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_harshad("/docs/nonharshad.txt", b"next-value")
            .expect("write_if_size_not_harshad should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonharshad.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let harshad_err = fuse
            .write_if_size_not_harshad("/docs/harshad.txt", b"again")
            .expect_err("harshad size should fail");
        assert!(matches!(harshad_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_harshad("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_harshad("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_kaprekar_updates_and_rejects_non_kaprekar_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let kaprekar_seed = vec![b'a'; 45];
        let non_kaprekar_seed = vec![b'a'; 46];
        fuse.write("/docs/kaprekar.txt", 0, &kaprekar_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonkaprekar.txt", 0, &non_kaprekar_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_kaprekar("/docs/kaprekar.txt", b"next-value")
            .expect("write_if_size_kaprekar should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/kaprekar.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_kaprekar_err = fuse
            .write_if_size_kaprekar("/docs/nonkaprekar.txt", b"again")
            .expect_err("non-kaprekar size should fail");
        assert!(matches!(non_kaprekar_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_kaprekar("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_kaprekar("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_kaprekar_updates_and_rejects_kaprekar_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_kaprekar_seed = vec![b'a'; 46];
        let kaprekar_seed = vec![b'a'; 45];
        fuse.write("/docs/nonkaprekar.txt", 0, &non_kaprekar_seed[..])
            .expect("write should pass");
        fuse.write("/docs/kaprekar.txt", 0, &kaprekar_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_kaprekar("/docs/nonkaprekar.txt", b"next-value")
            .expect("write_if_size_not_kaprekar should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonkaprekar.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let kaprekar_err = fuse
            .write_if_size_not_kaprekar("/docs/kaprekar.txt", b"again")
            .expect_err("kaprekar size should fail");
        assert!(matches!(kaprekar_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_kaprekar("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_kaprekar("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_repdigit_updates_and_rejects_non_repdigit_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let repdigit_seed = vec![b'a'; 11];
        let non_repdigit_seed = vec![b'a'; 12];
        fuse.write("/docs/repdigit.txt", 0, &repdigit_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonrepdigit.txt", 0, &non_repdigit_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_repdigit("/docs/repdigit.txt", b"next-value")
            .expect("write_if_size_repdigit should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/repdigit.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_repdigit_err = fuse
            .write_if_size_repdigit("/docs/nonrepdigit.txt", b"again")
            .expect_err("non-repdigit size should fail");
        assert!(matches!(non_repdigit_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_repdigit("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_repdigit("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_repdigit_updates_and_rejects_repdigit_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_repdigit_seed = vec![b'a'; 12];
        let repdigit_seed = vec![b'a'; 11];
        fuse.write("/docs/nonrepdigit.txt", 0, &non_repdigit_seed[..])
            .expect("write should pass");
        fuse.write("/docs/repdigit.txt", 0, &repdigit_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_repdigit("/docs/nonrepdigit.txt", b"next-value")
            .expect("write_if_size_not_repdigit should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonrepdigit.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let repdigit_err = fuse
            .write_if_size_not_repdigit("/docs/repdigit.txt", b"again")
            .expect_err("repdigit size should fail");
        assert!(matches!(repdigit_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_repdigit("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_repdigit("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_tribonacci_updates_and_rejects_non_tribonacci_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let tribonacci_seed = vec![b'a'; 24];
        let non_tribonacci_seed = vec![b'a'; 25];
        fuse.write("/docs/tribonacci.txt", 0, &tribonacci_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nontribonacci.txt", 0, &non_tribonacci_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_tribonacci("/docs/tribonacci.txt", b"next-value")
            .expect("write_if_size_tribonacci should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/tribonacci.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_tribonacci_err = fuse
            .write_if_size_tribonacci("/docs/nontribonacci.txt", b"again")
            .expect_err("non-tribonacci size should fail");
        assert!(matches!(non_tribonacci_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_tribonacci("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_tribonacci("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_tribonacci_updates_and_rejects_tribonacci_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_tribonacci_seed = vec![b'a'; 25];
        let tribonacci_seed = vec![b'a'; 24];
        fuse.write("/docs/nontribonacci.txt", 0, &non_tribonacci_seed[..])
            .expect("write should pass");
        fuse.write("/docs/tribonacci.txt", 0, &tribonacci_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_tribonacci("/docs/nontribonacci.txt", b"next-value")
            .expect("write_if_size_not_tribonacci should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nontribonacci.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let tribonacci_err = fuse
            .write_if_size_not_tribonacci("/docs/tribonacci.txt", b"again")
            .expect_err("tribonacci size should fail");
        assert!(matches!(tribonacci_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_tribonacci("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_tribonacci("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_pell_updates_and_rejects_non_pell_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pell_seed = vec![b'a'; 29];
        let non_pell_seed = vec![b'a'; 30];
        fuse.write("/docs/pell.txt", 0, &pell_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpell.txt", 0, &non_pell_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_pell("/docs/pell.txt", b"next-value")
            .expect("write_if_size_pell should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pell.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pell_err = fuse
            .write_if_size_pell("/docs/nonpell.txt", b"again")
            .expect_err("non-pell size should fail");
        assert!(matches!(non_pell_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_pell("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_pell("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_pell_updates_and_rejects_pell_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pell_seed = vec![b'a'; 30];
        let pell_seed = vec![b'a'; 29];
        fuse.write("/docs/nonpell.txt", 0, &non_pell_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pell.txt", 0, &pell_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_pell("/docs/nonpell.txt", b"next-value")
            .expect("write_if_size_not_pell should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpell.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pell_err = fuse
            .write_if_size_not_pell("/docs/pell.txt", b"again")
            .expect_err("pell size should fail");
        assert!(matches!(pell_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_pell("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_pell("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_lucas_updates_and_rejects_non_lucas_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let lucas_seed = vec![b'a'; 29];
        let non_lucas_seed = vec![b'a'; 30];
        fuse.write("/docs/lucas.txt", 0, &lucas_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonlucas.txt", 0, &non_lucas_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_lucas("/docs/lucas.txt", b"next-value")
            .expect("write_if_size_lucas should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/lucas.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_lucas_err = fuse
            .write_if_size_lucas("/docs/nonlucas.txt", b"again")
            .expect_err("non-lucas size should fail");
        assert!(matches!(non_lucas_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_lucas("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_lucas("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_lucas_updates_and_rejects_lucas_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_lucas_seed = vec![b'a'; 30];
        let lucas_seed = vec![b'a'; 29];
        fuse.write("/docs/nonlucas.txt", 0, &non_lucas_seed[..])
            .expect("write should pass");
        fuse.write("/docs/lucas.txt", 0, &lucas_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_lucas("/docs/nonlucas.txt", b"next-value")
            .expect("write_if_size_not_lucas should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonlucas.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let lucas_err = fuse
            .write_if_size_not_lucas("/docs/lucas.txt", b"again")
            .expect_err("lucas size should fail");
        assert!(matches!(lucas_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_lucas("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_lucas("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_mersenne_updates_and_rejects_non_mersenne_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let mersenne_seed = vec![b'a'; 31];
        let non_mersenne_seed = vec![b'a'; 32];
        fuse.write("/docs/mersenne.txt", 0, &mersenne_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonmersenne.txt", 0, &non_mersenne_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_mersenne("/docs/mersenne.txt", b"next-value")
            .expect("write_if_size_mersenne should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/mersenne.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_mersenne_err = fuse
            .write_if_size_mersenne("/docs/nonmersenne.txt", b"again")
            .expect_err("non-mersenne size should fail");
        assert!(matches!(non_mersenne_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_mersenne("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_mersenne("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_mersenne_updates_and_rejects_mersenne_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_mersenne_seed = vec![b'a'; 32];
        let mersenne_seed = vec![b'a'; 31];
        fuse.write("/docs/nonmersenne.txt", 0, &non_mersenne_seed[..])
            .expect("write should pass");
        fuse.write("/docs/mersenne.txt", 0, &mersenne_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_mersenne("/docs/nonmersenne.txt", b"next-value")
            .expect("write_if_size_not_mersenne should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonmersenne.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mersenne_err = fuse
            .write_if_size_not_mersenne("/docs/mersenne.txt", b"again")
            .expect_err("mersenne size should fail");
        assert!(matches!(mersenne_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_mersenne("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_mersenne("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_three_updates_and_rejects_non_power_of_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow3_seed = vec![b'a'; 27];
        let non_pow3_seed = vec![b'a'; 28];
        fuse.write("/docs/pow3.txt", 0, &pow3_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow3.txt", 0, &non_pow3_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_three("/docs/pow3.txt", b"next-value")
            .expect("write_if_size_power_of_three should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow3.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow3_err = fuse
            .write_if_size_power_of_three("/docs/nonpow3.txt", b"again")
            .expect_err("non-power-of-three size should fail");
        assert!(matches!(non_pow3_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_three("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_three("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_three_updates_and_rejects_power_of_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow3_seed = vec![b'a'; 28];
        let pow3_seed = vec![b'a'; 27];
        fuse.write("/docs/nonpow3.txt", 0, &non_pow3_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow3.txt", 0, &pow3_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_three("/docs/nonpow3.txt", b"next-value")
            .expect("write_if_size_not_power_of_three should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow3.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow3_err = fuse
            .write_if_size_not_power_of_three("/docs/pow3.txt", b"again")
            .expect_err("power-of-three size should fail");
        assert!(matches!(pow3_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_three("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_three("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_four_updates_and_rejects_non_power_of_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow4_seed = vec![b'a'; 64];
        let non_pow4_seed = vec![b'a'; 65];
        fuse.write("/docs/pow4.txt", 0, &pow4_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow4.txt", 0, &non_pow4_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_four("/docs/pow4.txt", b"next-value")
            .expect("write_if_size_power_of_four should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow4.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow4_err = fuse
            .write_if_size_power_of_four("/docs/nonpow4.txt", b"again")
            .expect_err("non-power-of-four size should fail");
        assert!(matches!(non_pow4_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_four("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_four("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_four_updates_and_rejects_power_of_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow4_seed = vec![b'a'; 65];
        let pow4_seed = vec![b'a'; 64];
        fuse.write("/docs/nonpow4.txt", 0, &non_pow4_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow4.txt", 0, &pow4_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_four("/docs/nonpow4.txt", b"next-value")
            .expect("write_if_size_not_power_of_four should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow4.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow4_err = fuse
            .write_if_size_not_power_of_four("/docs/pow4.txt", b"again")
            .expect_err("power-of-four size should fail");
        assert!(matches!(pow4_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_four("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_four("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_five_updates_and_rejects_non_power_of_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow5_seed = vec![b'a'; 125];
        let non_pow5_seed = vec![b'a'; 126];
        fuse.write("/docs/pow5.txt", 0, &pow5_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow5.txt", 0, &non_pow5_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_five("/docs/pow5.txt", b"next-value")
            .expect("write_if_size_power_of_five should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow5.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow5_err = fuse
            .write_if_size_power_of_five("/docs/nonpow5.txt", b"again")
            .expect_err("non-power-of-five size should fail");
        assert!(matches!(non_pow5_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_five("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_five("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_five_updates_and_rejects_power_of_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow5_seed = vec![b'a'; 126];
        let pow5_seed = vec![b'a'; 125];
        fuse.write("/docs/nonpow5.txt", 0, &non_pow5_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow5.txt", 0, &pow5_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_five("/docs/nonpow5.txt", b"next-value")
            .expect("write_if_size_not_power_of_five should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow5.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow5_err = fuse
            .write_if_size_not_power_of_five("/docs/pow5.txt", b"again")
            .expect_err("power-of-five size should fail");
        assert!(matches!(pow5_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_five("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_five("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_six_updates_and_rejects_non_power_of_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow6_seed = vec![b'a'; 216];
        let non_pow6_seed = vec![b'a'; 217];
        fuse.write("/docs/pow6.txt", 0, &pow6_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow6.txt", 0, &non_pow6_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_six("/docs/pow6.txt", b"next-value")
            .expect("write_if_size_power_of_six should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow6.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow6_err = fuse
            .write_if_size_power_of_six("/docs/nonpow6.txt", b"again")
            .expect_err("non-power-of-six size should fail");
        assert!(matches!(non_pow6_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_six("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_six("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_six_updates_and_rejects_power_of_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow6_seed = vec![b'a'; 217];
        let pow6_seed = vec![b'a'; 216];
        fuse.write("/docs/nonpow6.txt", 0, &non_pow6_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow6.txt", 0, &pow6_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_six("/docs/nonpow6.txt", b"next-value")
            .expect("write_if_size_not_power_of_six should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow6.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow6_err = fuse
            .write_if_size_not_power_of_six("/docs/pow6.txt", b"again")
            .expect_err("power-of-six size should fail");
        assert!(matches!(pow6_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_six("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_six("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_seven_updates_and_rejects_non_power_of_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow7_seed = vec![b'a'; 343];
        let non_pow7_seed = vec![b'a'; 344];
        fuse.write("/docs/pow7.txt", 0, &pow7_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow7.txt", 0, &non_pow7_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_seven("/docs/pow7.txt", b"next-value")
            .expect("write_if_size_power_of_seven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow7.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow7_err = fuse
            .write_if_size_power_of_seven("/docs/nonpow7.txt", b"again")
            .expect_err("non-power-of-seven size should fail");
        assert!(matches!(non_pow7_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_seven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_seven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_seven_updates_and_rejects_power_of_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow7_seed = vec![b'a'; 344];
        let pow7_seed = vec![b'a'; 343];
        fuse.write("/docs/nonpow7.txt", 0, &non_pow7_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow7.txt", 0, &pow7_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_seven("/docs/nonpow7.txt", b"next-value")
            .expect("write_if_size_not_power_of_seven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow7.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow7_err = fuse
            .write_if_size_not_power_of_seven("/docs/pow7.txt", b"again")
            .expect_err("power-of-seven size should fail");
        assert!(matches!(pow7_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_seven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_seven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_eight_updates_and_rejects_non_power_of_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow8_seed = vec![b'a'; 512];
        let non_pow8_seed = vec![b'a'; 513];
        fuse.write("/docs/pow8.txt", 0, &pow8_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow8.txt", 0, &non_pow8_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_eight("/docs/pow8.txt", b"next-value")
            .expect("write_if_size_power_of_eight should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow8.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow8_err = fuse
            .write_if_size_power_of_eight("/docs/nonpow8.txt", b"again")
            .expect_err("non-power-of-eight size should fail");
        assert!(matches!(non_pow8_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_eight("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_eight("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_eight_updates_and_rejects_power_of_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow8_seed = vec![b'a'; 513];
        let pow8_seed = vec![b'a'; 512];
        fuse.write("/docs/nonpow8.txt", 0, &non_pow8_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow8.txt", 0, &pow8_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_eight("/docs/nonpow8.txt", b"next-value")
            .expect("write_if_size_not_power_of_eight should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow8.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow8_err = fuse
            .write_if_size_not_power_of_eight("/docs/pow8.txt", b"again")
            .expect_err("power-of-eight size should fail");
        assert!(matches!(pow8_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_eight("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_eight("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_nine_updates_and_rejects_non_power_of_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow9_seed = vec![b'a'; 729];
        let non_pow9_seed = vec![b'a'; 730];
        fuse.write("/docs/pow9.txt", 0, &pow9_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow9.txt", 0, &non_pow9_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_nine("/docs/pow9.txt", b"next-value")
            .expect("write_if_size_power_of_nine should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow9.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow9_err = fuse
            .write_if_size_power_of_nine("/docs/nonpow9.txt", b"again")
            .expect_err("non-power-of-nine size should fail");
        assert!(matches!(non_pow9_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_nine("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_nine("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_nine_updates_and_rejects_power_of_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow9_seed = vec![b'a'; 730];
        let pow9_seed = vec![b'a'; 729];
        fuse.write("/docs/nonpow9.txt", 0, &non_pow9_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow9.txt", 0, &pow9_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_nine("/docs/nonpow9.txt", b"next-value")
            .expect("write_if_size_not_power_of_nine should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow9.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow9_err = fuse
            .write_if_size_not_power_of_nine("/docs/pow9.txt", b"again")
            .expect_err("power-of-nine size should fail");
        assert!(matches!(pow9_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_nine("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_nine("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_ten_updates_and_rejects_non_power_of_ten_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow10_seed = vec![b'a'; 1000];
        let non_pow10_seed = vec![b'a'; 1001];
        fuse.write("/docs/pow10.txt", 0, &pow10_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow10.txt", 0, &non_pow10_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_ten("/docs/pow10.txt", b"next-value")
            .expect("write_if_size_power_of_ten should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow10.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow10_err = fuse
            .write_if_size_power_of_ten("/docs/nonpow10.txt", b"again")
            .expect_err("non-power-of-ten size should fail");
        assert!(matches!(non_pow10_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_ten("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_ten("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_ten_updates_and_rejects_power_of_ten_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow10_seed = vec![b'a'; 1001];
        let pow10_seed = vec![b'a'; 1000];
        fuse.write("/docs/nonpow10.txt", 0, &non_pow10_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow10.txt", 0, &pow10_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_ten("/docs/nonpow10.txt", b"next-value")
            .expect("write_if_size_not_power_of_ten should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow10.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow10_err = fuse
            .write_if_size_not_power_of_ten("/docs/pow10.txt", b"again")
            .expect_err("power-of-ten size should fail");
        assert!(matches!(pow10_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_ten("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_ten("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_eleven_updates_and_rejects_non_power_of_eleven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow11_seed = vec![b'a'; 121];
        let non_pow11_seed = vec![b'a'; 122];
        fuse.write("/docs/pow11.txt", 0, &pow11_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow11.txt", 0, &non_pow11_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_eleven("/docs/pow11.txt", b"next-value")
            .expect("write_if_size_power_of_eleven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow11.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow11_err = fuse
            .write_if_size_power_of_eleven("/docs/nonpow11.txt", b"again")
            .expect_err("non-power-of-eleven size should fail");
        assert!(matches!(non_pow11_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_eleven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_eleven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_eleven_updates_and_rejects_power_of_eleven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow11_seed = vec![b'a'; 122];
        let pow11_seed = vec![b'a'; 121];
        fuse.write("/docs/nonpow11.txt", 0, &non_pow11_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow11.txt", 0, &pow11_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_eleven("/docs/nonpow11.txt", b"next-value")
            .expect("write_if_size_not_power_of_eleven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow11.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow11_err = fuse
            .write_if_size_not_power_of_eleven("/docs/pow11.txt", b"again")
            .expect_err("power-of-eleven size should fail");
        assert!(matches!(pow11_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_eleven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_eleven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twelve_updates_and_rejects_non_power_of_twelve_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow12_seed = vec![b'a'; 144];
        let non_pow12_seed = vec![b'a'; 145];
        fuse.write("/docs/pow12.txt", 0, &pow12_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow12.txt", 0, &non_pow12_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twelve("/docs/pow12.txt", b"next-value")
            .expect("write_if_size_power_of_twelve should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow12.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow12_err = fuse
            .write_if_size_power_of_twelve("/docs/nonpow12.txt", b"again")
            .expect_err("non-power-of-twelve size should fail");
        assert!(matches!(non_pow12_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twelve("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twelve("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twelve_updates_and_rejects_power_of_twelve_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow12_seed = vec![b'a'; 145];
        let pow12_seed = vec![b'a'; 144];
        fuse.write("/docs/nonpow12.txt", 0, &non_pow12_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow12.txt", 0, &pow12_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twelve("/docs/nonpow12.txt", b"next-value")
            .expect("write_if_size_not_power_of_twelve should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow12.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow12_err = fuse
            .write_if_size_not_power_of_twelve("/docs/pow12.txt", b"again")
            .expect_err("power-of-twelve size should fail");
        assert!(matches!(pow12_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twelve("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twelve("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirteen_updates_and_rejects_non_power_of_thirteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow13_seed = vec![b'a'; 169];
        let non_pow13_seed = vec![b'a'; 170];
        fuse.write("/docs/pow13.txt", 0, &pow13_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow13.txt", 0, &non_pow13_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirteen("/docs/pow13.txt", b"next-value")
            .expect("write_if_size_power_of_thirteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow13.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow13_err = fuse
            .write_if_size_power_of_thirteen("/docs/nonpow13.txt", b"again")
            .expect_err("non-power-of-thirteen size should fail");
        assert!(matches!(non_pow13_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirteen_updates_and_rejects_power_of_thirteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow13_seed = vec![b'a'; 170];
        let pow13_seed = vec![b'a'; 169];
        fuse.write("/docs/nonpow13.txt", 0, &non_pow13_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow13.txt", 0, &pow13_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirteen("/docs/nonpow13.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow13.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow13_err = fuse
            .write_if_size_not_power_of_thirteen("/docs/pow13.txt", b"again")
            .expect_err("power-of-thirteen size should fail");
        assert!(matches!(pow13_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_fourteen_updates_and_rejects_non_power_of_fourteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow14_seed = vec![b'a'; 196];
        let non_pow14_seed = vec![b'a'; 197];
        fuse.write("/docs/pow14.txt", 0, &pow14_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow14.txt", 0, &non_pow14_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_fourteen("/docs/pow14.txt", b"next-value")
            .expect("write_if_size_power_of_fourteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow14.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow14_err = fuse
            .write_if_size_power_of_fourteen("/docs/nonpow14.txt", b"again")
            .expect_err("non-power-of-fourteen size should fail");
        assert!(matches!(non_pow14_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_fourteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_fourteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_fourteen_updates_and_rejects_power_of_fourteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow14_seed = vec![b'a'; 197];
        let pow14_seed = vec![b'a'; 196];
        fuse.write("/docs/nonpow14.txt", 0, &non_pow14_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow14.txt", 0, &pow14_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_fourteen("/docs/nonpow14.txt", b"next-value")
            .expect("write_if_size_not_power_of_fourteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow14.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow14_err = fuse
            .write_if_size_not_power_of_fourteen("/docs/pow14.txt", b"again")
            .expect_err("power-of-fourteen size should fail");
        assert!(matches!(pow14_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_fourteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_fourteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_fifteen_updates_and_rejects_non_power_of_fifteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow15_seed = vec![b'a'; 225];
        let non_pow15_seed = vec![b'a'; 226];
        fuse.write("/docs/pow15.txt", 0, &pow15_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow15.txt", 0, &non_pow15_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_fifteen("/docs/pow15.txt", b"next-value")
            .expect("write_if_size_power_of_fifteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow15.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow15_err = fuse
            .write_if_size_power_of_fifteen("/docs/nonpow15.txt", b"again")
            .expect_err("non-power-of-fifteen size should fail");
        assert!(matches!(non_pow15_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_fifteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_fifteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_fifteen_updates_and_rejects_power_of_fifteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow15_seed = vec![b'a'; 226];
        let pow15_seed = vec![b'a'; 225];
        fuse.write("/docs/nonpow15.txt", 0, &non_pow15_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow15.txt", 0, &pow15_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_fifteen("/docs/nonpow15.txt", b"next-value")
            .expect("write_if_size_not_power_of_fifteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow15.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow15_err = fuse
            .write_if_size_not_power_of_fifteen("/docs/pow15.txt", b"again")
            .expect_err("power-of-fifteen size should fail");
        assert!(matches!(pow15_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_fifteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_fifteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_sixteen_updates_and_rejects_non_power_of_sixteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow16_seed = vec![b'a'; 256];
        let non_pow16_seed = vec![b'a'; 257];
        fuse.write("/docs/pow16.txt", 0, &pow16_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow16.txt", 0, &non_pow16_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_sixteen("/docs/pow16.txt", b"next-value")
            .expect("write_if_size_power_of_sixteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow16.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow16_err = fuse
            .write_if_size_power_of_sixteen("/docs/nonpow16.txt", b"again")
            .expect_err("non-power-of-sixteen size should fail");
        assert!(matches!(non_pow16_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_sixteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_sixteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_sixteen_updates_and_rejects_power_of_sixteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow16_seed = vec![b'a'; 257];
        let pow16_seed = vec![b'a'; 256];
        fuse.write("/docs/nonpow16.txt", 0, &non_pow16_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow16.txt", 0, &pow16_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_sixteen("/docs/nonpow16.txt", b"next-value")
            .expect("write_if_size_not_power_of_sixteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow16.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow16_err = fuse
            .write_if_size_not_power_of_sixteen("/docs/pow16.txt", b"again")
            .expect_err("power-of-sixteen size should fail");
        assert!(matches!(pow16_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_sixteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_sixteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_seventeen_updates_and_rejects_non_power_of_seventeen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow17_seed = vec![b'a'; 289];
        let non_pow17_seed = vec![b'a'; 290];
        fuse.write("/docs/pow17.txt", 0, &pow17_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow17.txt", 0, &non_pow17_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_seventeen("/docs/pow17.txt", b"next-value")
            .expect("write_if_size_power_of_seventeen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow17.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow17_err = fuse
            .write_if_size_power_of_seventeen("/docs/nonpow17.txt", b"again")
            .expect_err("non-power-of-seventeen size should fail");
        assert!(matches!(non_pow17_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_seventeen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_seventeen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_seventeen_updates_and_rejects_power_of_seventeen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow17_seed = vec![b'a'; 290];
        let pow17_seed = vec![b'a'; 289];
        fuse.write("/docs/nonpow17.txt", 0, &non_pow17_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow17.txt", 0, &pow17_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_seventeen("/docs/nonpow17.txt", b"next-value")
            .expect("write_if_size_not_power_of_seventeen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow17.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow17_err = fuse
            .write_if_size_not_power_of_seventeen("/docs/pow17.txt", b"again")
            .expect_err("power-of-seventeen size should fail");
        assert!(matches!(pow17_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_seventeen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_seventeen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_eighteen_updates_and_rejects_non_power_of_eighteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow18_seed = vec![b'a'; 324];
        let non_pow18_seed = vec![b'a'; 325];
        fuse.write("/docs/pow18.txt", 0, &pow18_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow18.txt", 0, &non_pow18_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_eighteen("/docs/pow18.txt", b"next-value")
            .expect("write_if_size_power_of_eighteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow18.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow18_err = fuse
            .write_if_size_power_of_eighteen("/docs/nonpow18.txt", b"again")
            .expect_err("non-power-of-eighteen size should fail");
        assert!(matches!(non_pow18_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_eighteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_eighteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_eighteen_updates_and_rejects_power_of_eighteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow18_seed = vec![b'a'; 325];
        let pow18_seed = vec![b'a'; 324];
        fuse.write("/docs/nonpow18.txt", 0, &non_pow18_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow18.txt", 0, &pow18_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_eighteen("/docs/nonpow18.txt", b"next-value")
            .expect("write_if_size_not_power_of_eighteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow18.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow18_err = fuse
            .write_if_size_not_power_of_eighteen("/docs/pow18.txt", b"again")
            .expect_err("power-of-eighteen size should fail");
        assert!(matches!(pow18_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_eighteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_eighteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_nineteen_updates_and_rejects_non_power_of_nineteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow19_seed = vec![b'a'; 361];
        let non_pow19_seed = vec![b'a'; 362];
        fuse.write("/docs/pow19.txt", 0, &pow19_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow19.txt", 0, &non_pow19_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_nineteen("/docs/pow19.txt", b"next-value")
            .expect("write_if_size_power_of_nineteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow19.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow19_err = fuse
            .write_if_size_power_of_nineteen("/docs/nonpow19.txt", b"again")
            .expect_err("non-power-of-nineteen size should fail");
        assert!(matches!(non_pow19_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_nineteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_nineteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_nineteen_updates_and_rejects_power_of_nineteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow19_seed = vec![b'a'; 362];
        let pow19_seed = vec![b'a'; 361];
        fuse.write("/docs/nonpow19.txt", 0, &non_pow19_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow19.txt", 0, &pow19_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_nineteen("/docs/nonpow19.txt", b"next-value")
            .expect("write_if_size_not_power_of_nineteen should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow19.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow19_err = fuse
            .write_if_size_not_power_of_nineteen("/docs/pow19.txt", b"again")
            .expect_err("power-of-nineteen size should fail");
        assert!(matches!(pow19_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_nineteen("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_nineteen("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_updates_and_rejects_non_power_of_twenty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow20_seed = vec![b'a'; 400];
        let non_pow20_seed = vec![b'a'; 401];
        fuse.write("/docs/pow20.txt", 0, &pow20_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow20.txt", 0, &non_pow20_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty("/docs/pow20.txt", b"next-value")
            .expect("write_if_size_power_of_twenty should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow20.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow20_err = fuse
            .write_if_size_power_of_twenty("/docs/nonpow20.txt", b"again")
            .expect_err("non-power-of-twenty size should fail");
        assert!(matches!(non_pow20_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_updates_and_rejects_power_of_twenty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow20_seed = vec![b'a'; 401];
        let pow20_seed = vec![b'a'; 400];
        fuse.write("/docs/nonpow20.txt", 0, &non_pow20_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow20.txt", 0, &pow20_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty("/docs/nonpow20.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow20.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow20_err = fuse
            .write_if_size_not_power_of_twenty("/docs/pow20.txt", b"again")
            .expect_err("power-of-twenty size should fail");
        assert!(matches!(pow20_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_one_updates_and_rejects_non_power_of_twenty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow21_seed = vec![b'a'; 441];
        let non_pow21_seed = vec![b'a'; 442];
        fuse.write("/docs/pow21.txt", 0, &pow21_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow21.txt", 0, &non_pow21_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_one("/docs/pow21.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_one should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow21.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow21_err = fuse
            .write_if_size_power_of_twenty_one("/docs/nonpow21.txt", b"again")
            .expect_err("non-power-of-twenty-one size should fail");
        assert!(matches!(non_pow21_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_one("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_one("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_one_updates_and_rejects_power_of_twenty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow21_seed = vec![b'a'; 442];
        let pow21_seed = vec![b'a'; 441];
        fuse.write("/docs/nonpow21.txt", 0, &non_pow21_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow21.txt", 0, &pow21_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_one("/docs/nonpow21.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_one should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow21.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow21_err = fuse
            .write_if_size_not_power_of_twenty_one("/docs/pow21.txt", b"again")
            .expect_err("power-of-twenty-one size should fail");
        assert!(matches!(pow21_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_one("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_one("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_two_updates_and_rejects_non_power_of_twenty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow22_seed = vec![b'a'; 484];
        let non_pow22_seed = vec![b'a'; 485];
        fuse.write("/docs/pow22.txt", 0, &pow22_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow22.txt", 0, &non_pow22_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_two("/docs/pow22.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_two should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow22.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow22_err = fuse
            .write_if_size_power_of_twenty_two("/docs/nonpow22.txt", b"again")
            .expect_err("non-power-of-twenty-two size should fail");
        assert!(matches!(non_pow22_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_two("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_two("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_two_updates_and_rejects_power_of_twenty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow22_seed = vec![b'a'; 485];
        let pow22_seed = vec![b'a'; 484];
        fuse.write("/docs/nonpow22.txt", 0, &non_pow22_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow22.txt", 0, &pow22_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_two("/docs/nonpow22.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_two should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow22.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow22_err = fuse
            .write_if_size_not_power_of_twenty_two("/docs/pow22.txt", b"again")
            .expect_err("power-of-twenty-two size should fail");
        assert!(matches!(pow22_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_two("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_two("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_three_updates_and_rejects_non_power_of_twenty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow23_seed = vec![b'a'; 529];
        let non_pow23_seed = vec![b'a'; 530];
        fuse.write("/docs/pow23.txt", 0, &pow23_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow23.txt", 0, &non_pow23_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_three("/docs/pow23.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_three should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow23.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow23_err = fuse
            .write_if_size_power_of_twenty_three("/docs/nonpow23.txt", b"again")
            .expect_err("non-power-of-twenty-three size should fail");
        assert!(matches!(non_pow23_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_three("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_three("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_three_updates_and_rejects_power_of_twenty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow23_seed = vec![b'a'; 530];
        let pow23_seed = vec![b'a'; 529];
        fuse.write("/docs/nonpow23.txt", 0, &non_pow23_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow23.txt", 0, &pow23_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_three("/docs/nonpow23.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_three should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow23.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow23_err = fuse
            .write_if_size_not_power_of_twenty_three("/docs/pow23.txt", b"again")
            .expect_err("power-of-twenty-three size should fail");
        assert!(matches!(pow23_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_three("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_three("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_four_updates_and_rejects_non_power_of_twenty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow24_seed = vec![b'a'; 576];
        let non_pow24_seed = vec![b'a'; 577];
        fuse.write("/docs/pow24.txt", 0, &pow24_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow24.txt", 0, &non_pow24_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_four("/docs/pow24.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_four should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow24.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow24_err = fuse
            .write_if_size_power_of_twenty_four("/docs/nonpow24.txt", b"again")
            .expect_err("non-power-of-twenty-four size should fail");
        assert!(matches!(non_pow24_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_four("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_four("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_four_updates_and_rejects_power_of_twenty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow24_seed = vec![b'a'; 577];
        let pow24_seed = vec![b'a'; 576];
        fuse.write("/docs/nonpow24.txt", 0, &non_pow24_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow24.txt", 0, &pow24_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_four("/docs/nonpow24.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_four should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow24.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow24_err = fuse
            .write_if_size_not_power_of_twenty_four("/docs/pow24.txt", b"again")
            .expect_err("power-of-twenty-four size should fail");
        assert!(matches!(pow24_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_four("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_four("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_five_updates_and_rejects_non_power_of_twenty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow25_seed = vec![b'a'; 625];
        let non_pow25_seed = vec![b'a'; 626];
        fuse.write("/docs/pow25.txt", 0, &pow25_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow25.txt", 0, &non_pow25_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_five("/docs/pow25.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_five should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow25.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow25_err = fuse
            .write_if_size_power_of_twenty_five("/docs/nonpow25.txt", b"again")
            .expect_err("non-power-of-twenty-five size should fail");
        assert!(matches!(non_pow25_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_five("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_five("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_five_updates_and_rejects_power_of_twenty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow25_seed = vec![b'a'; 626];
        let pow25_seed = vec![b'a'; 625];
        fuse.write("/docs/nonpow25.txt", 0, &non_pow25_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow25.txt", 0, &pow25_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_five("/docs/nonpow25.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_five should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow25.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow25_err = fuse
            .write_if_size_not_power_of_twenty_five("/docs/pow25.txt", b"again")
            .expect_err("power-of-twenty-five size should fail");
        assert!(matches!(pow25_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_five("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_five("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_six_updates_and_rejects_non_power_of_twenty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow26_seed = vec![b'a'; 676];
        let non_pow26_seed = vec![b'a'; 677];
        fuse.write("/docs/pow26.txt", 0, &pow26_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow26.txt", 0, &non_pow26_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_six("/docs/pow26.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_six should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow26.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow26_err = fuse
            .write_if_size_power_of_twenty_six("/docs/nonpow26.txt", b"again")
            .expect_err("non-power-of-twenty-six size should fail");
        assert!(matches!(non_pow26_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_six("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_six("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_six_updates_and_rejects_power_of_twenty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow26_seed = vec![b'a'; 677];
        let pow26_seed = vec![b'a'; 676];
        fuse.write("/docs/nonpow26.txt", 0, &non_pow26_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow26.txt", 0, &pow26_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_six("/docs/nonpow26.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_six should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow26.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow26_err = fuse
            .write_if_size_not_power_of_twenty_six("/docs/pow26.txt", b"again")
            .expect_err("power-of-twenty-six size should fail");
        assert!(matches!(pow26_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_six("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_six("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_seven_updates_and_rejects_non_power_of_twenty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow27_seed = vec![b'a'; 729];
        let non_pow27_seed = vec![b'a'; 730];
        fuse.write("/docs/pow27.txt", 0, &pow27_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow27.txt", 0, &non_pow27_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_seven("/docs/pow27.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow27.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow27_err = fuse
            .write_if_size_power_of_twenty_seven("/docs/nonpow27.txt", b"again")
            .expect_err("non-power-of-twenty-seven size should fail");
        assert!(matches!(non_pow27_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_seven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_seven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_seven_updates_and_rejects_power_of_twenty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow27_seed = vec![b'a'; 730];
        let pow27_seed = vec![b'a'; 729];
        fuse.write("/docs/nonpow27.txt", 0, &non_pow27_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow27.txt", 0, &pow27_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_seven("/docs/nonpow27.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow27.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow27_err = fuse
            .write_if_size_not_power_of_twenty_seven("/docs/pow27.txt", b"again")
            .expect_err("power-of-twenty-seven size should fail");
        assert!(matches!(pow27_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_seven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_seven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_eight_updates_and_rejects_non_power_of_twenty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow28_seed = vec![b'a'; 784];
        let non_pow28_seed = vec![b'a'; 785];
        fuse.write("/docs/pow28.txt", 0, &pow28_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow28.txt", 0, &non_pow28_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_eight("/docs/pow28.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow28.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow28_err = fuse
            .write_if_size_power_of_twenty_eight("/docs/nonpow28.txt", b"again")
            .expect_err("non-power-of-twenty-eight size should fail");
        assert!(matches!(non_pow28_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_eight("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_eight("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_eight_updates_and_rejects_power_of_twenty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow28_seed = vec![b'a'; 785];
        let pow28_seed = vec![b'a'; 784];
        fuse.write("/docs/nonpow28.txt", 0, &non_pow28_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow28.txt", 0, &pow28_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_eight("/docs/nonpow28.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow28.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow28_err = fuse
            .write_if_size_not_power_of_twenty_eight("/docs/pow28.txt", b"again")
            .expect_err("power-of-twenty-eight size should fail");
        assert!(matches!(pow28_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_eight("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_eight("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_twenty_nine_updates_and_rejects_non_power_of_twenty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow29_seed = vec![b'a'; 841];
        let non_pow29_seed = vec![b'a'; 842];
        fuse.write("/docs/pow29.txt", 0, &pow29_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow29.txt", 0, &non_pow29_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_twenty_nine("/docs/pow29.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow29.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow29_err = fuse
            .write_if_size_power_of_twenty_nine("/docs/nonpow29.txt", b"again")
            .expect_err("non-power-of-twenty-nine size should fail");
        assert!(matches!(non_pow29_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_twenty_nine("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_twenty_nine("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_twenty_nine_updates_and_rejects_power_of_twenty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow29_seed = vec![b'a'; 842];
        let pow29_seed = vec![b'a'; 841];
        fuse.write("/docs/nonpow29.txt", 0, &non_pow29_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow29.txt", 0, &pow29_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_twenty_nine("/docs/nonpow29.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow29.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow29_err = fuse
            .write_if_size_not_power_of_twenty_nine("/docs/pow29.txt", b"again")
            .expect_err("power-of-twenty-nine size should fail");
        assert!(matches!(pow29_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_twenty_nine("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_twenty_nine("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_updates_and_rejects_non_power_of_thirty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow30_seed = vec![b'a'; 900];
        let non_pow30_seed = vec![b'a'; 901];
        fuse.write("/docs/pow30.txt", 0, &pow30_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow30.txt", 0, &non_pow30_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty("/docs/pow30.txt", b"next-value")
            .expect("write_if_size_power_of_thirty should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow30.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow30_err = fuse
            .write_if_size_power_of_thirty("/docs/nonpow30.txt", b"again")
            .expect_err("non-power-of-thirty size should fail");
        assert!(matches!(non_pow30_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_updates_and_rejects_power_of_thirty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow30_seed = vec![b'a'; 901];
        let pow30_seed = vec![b'a'; 900];
        fuse.write("/docs/nonpow30.txt", 0, &non_pow30_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow30.txt", 0, &pow30_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty("/docs/nonpow30.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow30.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow30_err = fuse
            .write_if_size_not_power_of_thirty("/docs/pow30.txt", b"again")
            .expect_err("power-of-thirty size should fail");
        assert!(matches!(pow30_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_one_updates_and_rejects_non_power_of_thirty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow31_seed = vec![b'a'; 961];
        let non_pow31_seed = vec![b'a'; 962];
        fuse.write("/docs/pow31.txt", 0, &pow31_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow31.txt", 0, &non_pow31_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_one("/docs/pow31.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_one should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow31.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow31_err = fuse
            .write_if_size_power_of_thirty_one("/docs/nonpow31.txt", b"again")
            .expect_err("non-power-of-thirty-one size should fail");
        assert!(matches!(non_pow31_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_one("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_one("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_one_updates_and_rejects_power_of_thirty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow31_seed = vec![b'a'; 962];
        let pow31_seed = vec![b'a'; 961];
        fuse.write("/docs/nonpow31.txt", 0, &non_pow31_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow31.txt", 0, &pow31_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_one("/docs/nonpow31.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_one should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow31.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow31_err = fuse
            .write_if_size_not_power_of_thirty_one("/docs/pow31.txt", b"again")
            .expect_err("power-of-thirty-one size should fail");
        assert!(matches!(pow31_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_one("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_one("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_two_updates_and_rejects_non_power_of_thirty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow32_seed = vec![b'a'; 1024];
        let non_pow32_seed = vec![b'a'; 1025];
        fuse.write("/docs/pow32.txt", 0, &pow32_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow32.txt", 0, &non_pow32_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_two("/docs/pow32.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_two should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow32.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow32_err = fuse
            .write_if_size_power_of_thirty_two("/docs/nonpow32.txt", b"again")
            .expect_err("non-power-of-thirty-two size should fail");
        assert!(matches!(non_pow32_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_two("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_two("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_two_updates_and_rejects_power_of_thirty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow32_seed = vec![b'a'; 1025];
        let pow32_seed = vec![b'a'; 1024];
        fuse.write("/docs/nonpow32.txt", 0, &non_pow32_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow32.txt", 0, &pow32_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_two("/docs/nonpow32.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_two should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow32.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow32_err = fuse
            .write_if_size_not_power_of_thirty_two("/docs/pow32.txt", b"again")
            .expect_err("power-of-thirty-two size should fail");
        assert!(matches!(pow32_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_two("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_two("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_three_updates_and_rejects_non_power_of_thirty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow33_seed = vec![b'a'; 1089];
        let non_pow33_seed = vec![b'a'; 1090];
        fuse.write("/docs/pow33.txt", 0, &pow33_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow33.txt", 0, &non_pow33_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_three("/docs/pow33.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_three should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow33.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow33_err = fuse
            .write_if_size_power_of_thirty_three("/docs/nonpow33.txt", b"again")
            .expect_err("non-power-of-thirty-three size should fail");
        assert!(matches!(non_pow33_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_three("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_three("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_three_updates_and_rejects_power_of_thirty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow33_seed = vec![b'a'; 1090];
        let pow33_seed = vec![b'a'; 1089];
        fuse.write("/docs/nonpow33.txt", 0, &non_pow33_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow33.txt", 0, &pow33_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_three("/docs/nonpow33.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_three should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow33.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow33_err = fuse
            .write_if_size_not_power_of_thirty_three("/docs/pow33.txt", b"again")
            .expect_err("power-of-thirty-three size should fail");
        assert!(matches!(pow33_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_three("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_three("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_four_updates_and_rejects_non_power_of_thirty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow34_seed = vec![b'a'; 1156];
        let non_pow34_seed = vec![b'a'; 1157];
        fuse.write("/docs/pow34.txt", 0, &pow34_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow34.txt", 0, &non_pow34_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_four("/docs/pow34.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_four should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow34.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow34_err = fuse
            .write_if_size_power_of_thirty_four("/docs/nonpow34.txt", b"again")
            .expect_err("non-power-of-thirty-four size should fail");
        assert!(matches!(non_pow34_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_four("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_four("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_four_updates_and_rejects_power_of_thirty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow34_seed = vec![b'a'; 1157];
        let pow34_seed = vec![b'a'; 1156];
        fuse.write("/docs/nonpow34.txt", 0, &non_pow34_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow34.txt", 0, &pow34_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_four("/docs/nonpow34.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_four should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow34.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow34_err = fuse
            .write_if_size_not_power_of_thirty_four("/docs/pow34.txt", b"again")
            .expect_err("power-of-thirty-four size should fail");
        assert!(matches!(pow34_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_four("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_four("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_five_updates_and_rejects_non_power_of_thirty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow35_seed = vec![b'a'; 1225];
        let non_pow35_seed = vec![b'a'; 1226];
        fuse.write("/docs/pow35.txt", 0, &pow35_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow35.txt", 0, &non_pow35_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_five("/docs/pow35.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_five should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow35.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow35_err = fuse
            .write_if_size_power_of_thirty_five("/docs/nonpow35.txt", b"again")
            .expect_err("non-power-of-thirty-five size should fail");
        assert!(matches!(non_pow35_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_five("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_five("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_five_updates_and_rejects_power_of_thirty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow35_seed = vec![b'a'; 1226];
        let pow35_seed = vec![b'a'; 1225];
        fuse.write("/docs/nonpow35.txt", 0, &non_pow35_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow35.txt", 0, &pow35_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_five("/docs/nonpow35.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_five should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow35.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow35_err = fuse
            .write_if_size_not_power_of_thirty_five("/docs/pow35.txt", b"again")
            .expect_err("power-of-thirty-five size should fail");
        assert!(matches!(pow35_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_five("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_five("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_six_updates_and_rejects_non_power_of_thirty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow36_seed = vec![b'a'; 1296];
        let non_pow36_seed = vec![b'a'; 1297];
        fuse.write("/docs/pow36.txt", 0, &pow36_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow36.txt", 0, &non_pow36_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_six("/docs/pow36.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_six should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow36.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow36_err = fuse
            .write_if_size_power_of_thirty_six("/docs/nonpow36.txt", b"again")
            .expect_err("non-power-of-thirty-six size should fail");
        assert!(matches!(non_pow36_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_six("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_six("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_six_updates_and_rejects_power_of_thirty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow36_seed = vec![b'a'; 1297];
        let pow36_seed = vec![b'a'; 1296];
        fuse.write("/docs/nonpow36.txt", 0, &non_pow36_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow36.txt", 0, &pow36_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_six("/docs/nonpow36.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_six should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow36.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow36_err = fuse
            .write_if_size_not_power_of_thirty_six("/docs/pow36.txt", b"again")
            .expect_err("power-of-thirty-six size should fail");
        assert!(matches!(pow36_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_six("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_six("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_seven_updates_and_rejects_non_power_of_thirty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow37_seed = vec![b'a'; 1369];
        let non_pow37_seed = vec![b'a'; 1370];
        fuse.write("/docs/pow37.txt", 0, &pow37_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow37.txt", 0, &non_pow37_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_seven("/docs/pow37.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow37.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow37_err = fuse
            .write_if_size_power_of_thirty_seven("/docs/nonpow37.txt", b"again")
            .expect_err("non-power-of-thirty-seven size should fail");
        assert!(matches!(non_pow37_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_seven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_seven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_seven_updates_and_rejects_power_of_thirty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow37_seed = vec![b'a'; 1370];
        let pow37_seed = vec![b'a'; 1369];
        fuse.write("/docs/nonpow37.txt", 0, &non_pow37_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow37.txt", 0, &pow37_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_seven("/docs/nonpow37.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow37.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow37_err = fuse
            .write_if_size_not_power_of_thirty_seven("/docs/pow37.txt", b"again")
            .expect_err("power-of-thirty-seven size should fail");
        assert!(matches!(pow37_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_seven("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_seven("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_eight_updates_and_rejects_non_power_of_thirty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow38_seed = vec![b'a'; 1444];
        let non_pow38_seed = vec![b'a'; 1445];
        fuse.write("/docs/pow38.txt", 0, &pow38_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow38.txt", 0, &non_pow38_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_eight("/docs/pow38.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow38.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow38_err = fuse
            .write_if_size_power_of_thirty_eight("/docs/nonpow38.txt", b"again")
            .expect_err("non-power-of-thirty-eight size should fail");
        assert!(matches!(non_pow38_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_eight("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_eight("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_eight_updates_and_rejects_power_of_thirty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow38_seed = vec![b'a'; 1445];
        let pow38_seed = vec![b'a'; 1444];
        fuse.write("/docs/nonpow38.txt", 0, &non_pow38_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow38.txt", 0, &pow38_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_eight("/docs/nonpow38.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow38.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow38_err = fuse
            .write_if_size_not_power_of_thirty_eight("/docs/pow38.txt", b"again")
            .expect_err("power-of-thirty-eight size should fail");
        assert!(matches!(pow38_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_eight("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_eight("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_power_of_thirty_nine_updates_and_rejects_non_power_of_thirty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let pow39_seed = vec![b'a'; 1521];
        let non_pow39_seed = vec![b'a'; 1522];
        fuse.write("/docs/pow39.txt", 0, &pow39_seed[..])
            .expect("write should pass");
        fuse.write("/docs/nonpow39.txt", 0, &non_pow39_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_power_of_thirty_nine("/docs/pow39.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = fuse.read_all("/docs/pow39.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow39_err = fuse
            .write_if_size_power_of_thirty_nine("/docs/nonpow39.txt", b"again")
            .expect_err("non-power-of-thirty-nine size should fail");
        assert!(matches!(non_pow39_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_power_of_thirty_nine("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_power_of_thirty_nine("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_write_if_size_not_power_of_thirty_nine_updates_and_rejects_power_of_thirty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let non_pow39_seed = vec![b'a'; 1522];
        let pow39_seed = vec![b'a'; 1521];
        fuse.write("/docs/nonpow39.txt", 0, &non_pow39_seed[..])
            .expect("write should pass");
        fuse.write("/docs/pow39.txt", 0, &pow39_seed[..])
            .expect("write should pass");

        let tx = fuse
            .write_if_size_not_power_of_thirty_nine("/docs/nonpow39.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read_all("/docs/nonpow39.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow39_err = fuse
            .write_if_size_not_power_of_thirty_nine("/docs/pow39.txt", b"again")
            .expect_err("power-of-thirty-nine size should fail");
        assert!(matches!(pow39_err, FuseError::Conflict));

        let missing_err = fuse
            .write_if_size_not_power_of_thirty_nine("/docs/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = fuse
            .write_if_size_not_power_of_thirty_nine("/docs", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_ensure_file_is_idempotent_and_conflicts_on_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        assert!(fuse
            .ensure_file("/docs/ensured.txt")
            .expect("ensure_file should create"));
        fuse.write("/docs/ensured.txt", 1, b"keep-me")
            .expect("write should pass");
        assert!(!fuse
            .ensure_file("/docs/ensured.txt")
            .expect("ensure_file should be idempotent"));
        let bytes = fuse
            .read_all("/docs/ensured.txt")
            .expect("read_all should pass");
        assert_eq!(bytes, b"keep-me");
        let err = fuse
            .ensure_file("/docs")
            .expect_err("ensure_file on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_read_all_rejects_directory_path() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let err = fuse
            .read_all("/docs")
            .expect_err("read_all on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_mkdir_p_creates_nested_tree() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let created = fuse
            .mkdir_p("/docs/nested/deeper")
            .expect("mkdir_p should pass");
        assert_eq!(created, 3);

        let root = fuse.readdir("/").expect("root list should pass");
        assert_eq!(root, vec!["docs".to_string()]);
        let docs = fuse.readdir("/docs").expect("docs list should pass");
        assert_eq!(docs, vec!["nested".to_string()]);
        let nested = fuse
            .readdir("/docs/nested")
            .expect("nested list should pass");
        assert_eq!(nested, vec!["deeper".to_string()]);
    }

    #[test]
    fn fuse_api_stat_reports_node_kind() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");

        assert_eq!(fuse.stat("/").expect("stat should pass"), FuseNodeKind::Directory);
        assert_eq!(
            fuse.stat("/docs").expect("stat should pass"),
            FuseNodeKind::Directory
        );
        assert_eq!(
            fuse.stat("/docs/readme.txt").expect("stat should pass"),
            FuseNodeKind::File
        );
        let err = fuse
            .stat("/missing")
            .expect_err("stat missing should fail");
        assert!(matches!(err, FuseError::NotFound));
    }

    #[test]
    fn fuse_api_exists_reports_file_dir_and_missing() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");

        assert!(fuse.exists("/").expect("exists should pass"));
        assert!(fuse.exists("/docs").expect("exists should pass"));
        assert!(fuse
            .exists("/docs/readme.txt")
            .expect("exists should pass"));
        assert!(!fuse.exists("/missing").expect("exists should pass"));
    }

    #[test]
    fn fuse_api_readdir_with_kinds_reports_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.mkdir("/docs/sub").expect("mkdir should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");

        let entries = fuse
            .readdir_with_kinds("/docs")
            .expect("readdir_with_kinds should pass");
        assert_eq!(
            entries,
            vec![
                FuseDirEntry {
                    name: "readme.txt".to_string(),
                    kind: FuseNodeKind::File,
                },
                FuseDirEntry {
                    name: "sub".to_string(),
                    kind: FuseNodeKind::Directory,
                },
            ]
        );
    }

    #[test]
    fn fuse_api_copy_file_copies_bytes() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/source.txt", 0, b"copy-me")
            .expect("write should pass");
        let tx = fuse
            .copy_file("/docs/source.txt", "/docs/dest.txt", 0)
            .expect("copy should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read("/docs/dest.txt", 0, usize::MAX)
            .expect("dest read should pass");
        assert_eq!(bytes, b"copy-me");
    }

    #[test]
    fn fuse_api_touch_file_creates_and_updates_empty_file() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let tx_v1 = fuse
            .touch_file("/docs/empty.txt", 0)
            .expect("touch create should pass");
        assert!(!tx_v1.is_empty());
        let bytes = fuse
            .read("/docs/empty.txt", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(bytes.len(), 0);
        let tx_v2 = fuse
            .touch_file("/docs/empty.txt", 1)
            .expect("touch update should pass");
        assert!(!tx_v2.is_empty());

        let err = fuse
            .touch_file("/docs", 0)
            .expect_err("touch on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_truncate_file_shrinks_and_extends_with_zero_fill() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/data.bin", 0, b"abcdef")
            .expect("seed write should pass");

        let tx_v1 = fuse
            .truncate_file("/docs/data.bin", 3, 1)
            .expect("truncate shrink should pass");
        assert!(!tx_v1.is_empty());
        let shrunk = fuse
            .read("/docs/data.bin", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(shrunk, b"abc");

        let tx_v2 = fuse
            .truncate_file("/docs/data.bin", 6, 2)
            .expect("truncate extend should pass");
        assert!(!tx_v2.is_empty());
        let extended = fuse
            .read("/docs/data.bin", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(extended, b"abc\0\0\0");

        let err = fuse
            .truncate_file("/docs", 1, 0)
            .expect_err("truncate on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_append_file_appends_bytes() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/log.txt", 0, b"line1")
            .expect("seed write should pass");
        let tx = fuse
            .append_file("/docs/log.txt", 1, b"-line2")
            .expect("append should pass");
        assert!(!tx.is_empty());
        let bytes = fuse
            .read("/docs/log.txt", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(bytes, b"line1-line2");

        let err = fuse
            .append_file("/docs", 0, b"x")
            .expect_err("append on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_overwrite_range_patches_and_grows() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = fuse
            .overwrite_range("/docs/data.bin", 2, 1, b"XYZ")
            .expect("overwrite in-range should pass");
        assert!(!tx_v1.is_empty());
        let patched = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(patched, b"abXYZf");

        let tx_v2 = fuse
            .overwrite_range("/docs/data.bin", 8, 2, b"Q")
            .expect("overwrite grow should pass");
        assert!(!tx_v2.is_empty());
        let grown = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(grown, b"abXYZf\0\0Q");

        let err = fuse
            .overwrite_range("/docs", 0, 0, b"x")
            .expect_err("overwrite on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_insert_range_inserts_and_grows() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = fuse
            .insert_range("/docs/data.bin", 3, 1, b"XYZ")
            .expect("insert in-range should pass");
        assert!(!tx_v1.is_empty());
        let inserted = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(inserted, b"abcXYZdef");

        let tx_v2 = fuse
            .insert_range("/docs/data.bin", 12, 2, b"Q")
            .expect("insert grow should pass");
        assert!(!tx_v2.is_empty());
        let grown = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(grown, b"abcXYZdef\0\0\0Q");

        let err = fuse
            .insert_range("/docs", 0, 0, b"x")
            .expect_err("insert on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_delete_range_removes_span_and_handles_past_eof() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = fuse
            .delete_range("/docs/data.bin", 2, 2, 1)
            .expect("delete span should pass");
        assert!(!tx_v1.is_empty());
        let deleted = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(deleted, b"abef");

        let tx_v2 = fuse
            .delete_range("/docs/data.bin", 20, 4, 2)
            .expect("past-eof delete should pass");
        assert!(!tx_v2.is_empty());
        let after = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(after, b"abef");

        let err = fuse
            .delete_range("/docs", 0, 1, 0)
            .expect_err("delete on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_replace_range_replaces_window_and_grows_with_gap_fill() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = fuse
            .replace_range("/docs/data.bin", 2, 2, 1, b"XYZ")
            .expect("replace window should pass");
        assert!(!tx_v1.is_empty());
        let replaced = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(replaced, b"abXYZef");

        let tx_v2 = fuse
            .replace_range("/docs/data.bin", 12, 1, 2, b"Q")
            .expect("replace grow should pass");
        assert!(!tx_v2.is_empty());
        let grown = fuse.read_all("/docs/data.bin").expect("read_all should pass");
        assert_eq!(grown, b"abXYZef\0\0\0\0\0Q");

        let err = fuse
            .replace_range("/docs", 0, 1, 0, b"x")
            .expect_err("replace on directory should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_file_size_reports_file_bytes() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"hello")
            .expect("write should pass");
        let size = fuse
            .file_size("/docs/a.txt")
            .expect("file size should pass");
        assert_eq!(size, 5);
        let err = fuse
            .file_size("/docs")
            .expect_err("directory size should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_file_hash_reports_blake3_checksum() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        let payload = b"checksum-me";
        fuse.write("/docs/a.txt", 0, payload)
            .expect("write should pass");
        let hash_v1 = fuse
            .file_hash("/docs/a.txt")
            .expect("file hash should pass");
        assert_eq!(hash_v1.len(), 64);
        assert!(hash_v1.chars().all(|c| c.is_ascii_hexdigit()));
        fuse.write("/docs/a.txt", 1, b"checksum-me-v2")
            .expect("rewrite should pass");
        let hash_v2 = fuse
            .file_hash("/docs/a.txt")
            .expect("file hash should pass");
        assert_eq!(hash_v2.len(), 64);
        assert_ne!(hash_v1, hash_v2);
        let err = fuse
            .file_hash("/docs")
            .expect_err("directory hash should fail");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_walk_dir_reports_recursive_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir_p("/docs/sub").expect("mkdir_p should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");
        fuse.write("/docs/sub/nested.txt", 0, b"world")
            .expect("write should pass");

        let entries = fuse.walk_dir("/docs").expect("walk_dir should pass");
        assert_eq!(
            entries,
            vec![
                FusePathEntry {
                    path: "/docs/readme.txt".to_string(),
                    kind: FuseNodeKind::File,
                },
                FusePathEntry {
                    path: "/docs/sub".to_string(),
                    kind: FuseNodeKind::Directory,
                },
                FusePathEntry {
                    path: "/docs/sub/nested.txt".to_string(),
                    kind: FuseNodeKind::File,
                },
            ]
        );
    }

    #[test]
    fn fuse_api_tree_summary_counts_recursive_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir_p("/docs/sub").expect("mkdir_p should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");
        fuse.write("/docs/sub/nested.txt", 0, b"world")
            .expect("write should pass");

        let summary = fuse
            .tree_summary("/docs")
            .expect("tree summary should pass");
        assert_eq!(
            summary,
            FuseTreeSummary {
                files: 2,
                directories: 1,
            }
        );
    }

    #[test]
    fn fuse_api_tree_bytes_sums_recursive_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir_p("/docs/sub").expect("mkdir_p should pass");
        fuse.write("/docs/a.txt", 0, b"hello")
            .expect("write should pass");
        fuse.write("/docs/sub/b.txt", 0, b"world!")
            .expect("write should pass");

        let bytes = fuse.tree_bytes("/docs").expect("tree bytes should pass");
        assert_eq!(bytes, 11);
    }

    #[test]
    fn fuse_api_run_background_once_returns_empty_without_gc_worker() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let reports = fuse
            .run_background_once()
            .expect("run_background_once should pass");
        assert!(reports.is_empty());
    }

    #[test]
    fn fuse_api_gc_scan_once_returns_empty_without_gc_components() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let reports = fuse.gc_scan_once().expect("gc_scan_once should pass");
        assert!(reports.is_empty());
    }

    #[test]
    fn fuse_api_enqueue_gc_scan_reports_false_without_trigger() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let enqueued = fuse
            .enqueue_gc_scan()
            .expect("enqueue_gc_scan should pass");
        assert!(!enqueued, "enqueue_gc_scan should report no trigger");
    }

    #[test]
    fn fuse_api_rmdir_non_empty_maps_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");

        let err = fuse
            .rmdir("/docs")
            .expect_err("rmdir should fail for non-empty directory");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_rmdir_and_rmtree_on_file_path_map_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.write("/docs", 0, b"hello").expect("write should pass");
        let rmdir_err = fuse
            .rmdir("/docs")
            .expect_err("rmdir on file path should fail");
        assert!(matches!(rmdir_err, FuseError::Conflict));
        let rmtree_err = fuse
            .rmtree("/docs")
            .expect_err("rmtree on file path should fail");
        assert!(matches!(rmtree_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_rmtree_removes_nested_tree() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.mkdir("/docs/sub").expect("mkdir should pass");
        fuse.write("/docs/readme.txt", 0, b"hello")
            .expect("write should pass");
        fuse.write("/docs/sub/notes.txt", 0, b"nested")
            .expect("write should pass");

        let removed = fuse.rmtree("/docs").expect("rmtree should pass");
        assert_eq!(removed, 4);
        let root = fuse.readdir("/").expect("root list should pass");
        assert!(root.is_empty());
        let err = fuse
            .read("/docs/sub/notes.txt", 0, 1)
            .expect_err("removed file read should fail");
        assert!(matches!(err, FuseError::NotFound));
    }

    #[test]
    fn fuse_api_remove_path_removes_file_and_directories() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.mkdir("/docs/sub").expect("mkdir should pass");
        fuse.write("/docs/file.txt", 0, b"hello")
            .expect("write should pass");
        fuse.write("/docs/sub/nested.txt", 0, b"world")
            .expect("write should pass");

        assert_eq!(
            fuse.remove_path("/docs/file.txt")
                .expect("remove file should pass"),
            1
        );
        assert_eq!(
            fuse.remove_path("/docs/sub")
                .expect("remove subtree should pass"),
            2
        );
        assert_eq!(
            fuse.remove_path("/docs")
                .expect("remove empty dir should pass"),
            1
        );
        assert!(!fuse.exists("/docs").expect("exists should pass"));
    }

    #[test]
    fn fuse_api_readdir_missing_maps_to_not_found() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let err = fuse
            .readdir("/missing")
            .expect_err("readdir should fail for missing directory");
        assert!(matches!(err, FuseError::NotFound));
    }

    #[test]
    fn fuse_api_path_type_conflicts_map_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.write("/docs", 0, b"hello").expect("write should pass");
        let mkdir_err = fuse
            .mkdir("/docs")
            .expect_err("mkdir over live file should fail");
        assert!(matches!(mkdir_err, FuseError::Conflict));

        fuse.mkdir("/dir").expect("mkdir should pass");
        let write_err = fuse
            .write("/dir", 0, b"hello")
            .expect_err("write over directory should fail");
        assert!(matches!(write_err, FuseError::Conflict));

        let unlink_err = fuse
            .unlink("/dir", 0)
            .expect_err("unlink over directory should fail");
        assert!(matches!(unlink_err, FuseError::Conflict));
    }

    #[test]
    fn fuse_api_rename_flow_and_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        fuse.mkdir("/docs").expect("mkdir should pass");
        fuse.write("/docs/a.txt", 0, b"a").expect("write should pass");
        fuse.write("/docs/b.txt", 0, b"b").expect("write should pass");

        let conflict = fuse
            .rename("/docs/a.txt", "/docs/b.txt")
            .expect_err("rename conflict should fail");
        assert!(matches!(conflict, FuseError::Conflict));

        fuse.rename("/docs/a.txt", "/docs/c.txt")
            .expect("rename should pass");
        let got = fuse.read("/docs/c.txt", 0, 1).expect("read should pass");
        assert_eq!(got, b"a");
    }

    #[test]
    fn fuse_api_missing_parent_maps_to_not_found() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let mkdir_err = fuse
            .mkdir("/missing/child")
            .expect_err("mkdir should fail with missing parent");
        assert!(matches!(mkdir_err, FuseError::NotFound));

        let write_err = fuse
            .write("/missing/file.txt", 0, b"hello")
            .expect_err("write should fail with missing parent");
        assert!(matches!(write_err, FuseError::NotFound));
    }

    #[test]
    fn fuse_write_cas_conflict_maps_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(AlwaysConflictWriteMetadata);
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let err = fuse
            .write("/a", 0, b"hello")
            .expect_err("write should report conflict");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_unlink_cas_conflict_maps_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(AlwaysConflictDeleteMetadata);
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        fuse.startup_recover().expect("recovery should pass");
        let err = fuse
            .unlink("/a", 1)
            .expect_err("unlink should report conflict");
        assert!(matches!(err, FuseError::Conflict));
    }

    #[test]
    fn fuse_ops_are_unavailable_before_startup_recovery() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let open_err = fuse.open("/x").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/x", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/x", 0, b"data")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse.unlink("/x", 0).expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_api_mount_gate_state_flips_after_startup_recovery() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        assert!(
            !fuse.is_mount_open(),
            "mount gate should be closed before recovery"
        );
        fuse.startup_recover().expect("recovery should pass");
        assert!(
            fuse.is_mount_open(),
            "mount gate should be open after recovery"
        );
    }

    #[test]
    fn fuse_startup_recovery_failure_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        // Create a committed WAL write referencing a missing chunk.
        let mut txn = wal
            .begin_transaction_with_id(
                "tx-missing-chunk".to_string(),
                "/bad.txt",
                wal::OperationType::Write,
                0,
            )
            .expect("wal begin should succeed");
        wal.append_chunk(&mut txn, "abcdef".to_string(), "abcdef".to_string())
            .expect("wal pending chunk append should succeed");
        wal.commit_transaction(&txn)
            .expect("wal commit marker should succeed");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_non_tail_wal_corruption_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // Build WAL with at least two frames.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _tx_a = wal
                .begin_transaction_with_id(
                    "tx-a".to_string(),
                    "/a.txt",
                    wal::OperationType::Write,
                    0,
                )
                .expect("first tx should append");
            let _tx_b = wal
                .begin_transaction_with_id(
                    "tx-b".to_string(),
                    "/b.txt",
                    wal::OperationType::Write,
                    0,
                )
                .expect("second tx should append");
        }

        // Corrupt checksum byte of first frame so corruption is non-tail.
        let bytes = fs::read(&wal_path).expect("wal bytes should be readable");
        let payload_len = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .expect("len prefix should have 4 bytes"),
        ) as usize;
        let checksum_start = 4 + payload_len;
        let mut writer = fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .expect("wal should reopen for corruption");
        writer
            .seek(std::io::SeekFrom::Start(checksum_start as u64))
            .expect("seek to checksum should work");
        let corrupt_byte = bytes[checksum_start] ^ 0xFF;
        writer
            .write_all(&[corrupt_byte])
            .expect("single-byte corruption should be written");

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on non-tail corruption");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_non_tail_wal_corruption_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        // Build WAL with at least two frames.
        {
            let wal = WalLog::open(&wal_path).expect("wal should open");
            let _tx_a = wal
                .begin_transaction_with_id(
                    "tx-a".to_string(),
                    "/a.txt",
                    wal::OperationType::Write,
                    0,
                )
                .expect("first tx should append");
            let _tx_b = wal
                .begin_transaction_with_id(
                    "tx-b".to_string(),
                    "/b.txt",
                    wal::OperationType::Write,
                    0,
                )
                .expect("second tx should append");
        }

        // Corrupt checksum byte of first frame so corruption is non-tail.
        let bytes = fs::read(&wal_path).expect("wal bytes should be readable");
        let payload_len = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .expect("len prefix should have 4 bytes"),
        ) as usize;
        let checksum_start = 4 + payload_len;
        let mut writer = fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .expect("wal should reopen for corruption");
        writer
            .seek(std::io::SeekFrom::Start(checksum_start as u64))
            .expect("seek to checksum should work");
        let corrupt_byte = bytes[checksum_start] ^ 0xFF;
        writer
            .write_all(&[corrupt_byte])
            .expect("single-byte corruption should be written");

        let wal = WalLog::open(&wal_path).expect("wal should reopen");
        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on non-tail corruption");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        // Create a committed WAL write referencing a missing chunk.
        let mut txn = wal
            .begin_transaction_with_id(
                "tx-missing-chunk-ops".to_string(),
                "/bad.txt",
                wal::OperationType::Write,
                0,
            )
            .expect("wal begin should succeed");
        wal.append_chunk(&mut txn, "abcdef".to_string(), "abcdef".to_string())
            .expect("wal pending chunk append should succeed");
        wal.commit_transaction(&txn)
            .expect("wal commit marker should succeed");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_malformed_delete_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-delete".to_string(),
            "/bad-delete.txt".to_string(),
            wal::OperationType::Delete,
            0,
        );
        bad.chunk_ids.push("id-1".to_string());
        bad.chunk_hashes.push("id-1".to_string());
        let bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed delete should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on malformed delete entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_malformed_delete_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-delete-ops".to_string(),
            "/bad-delete.txt".to_string(),
            wal::OperationType::Delete,
            0,
        );
        bad.chunk_ids.push("id-1".to_string());
        bad.chunk_hashes.push("id-1".to_string());
        let bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed delete should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on malformed delete entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_empty_transaction_id_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on empty transaction id entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_nul_transaction_id_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx\0bad".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL transaction id entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_nul_file_path_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-nul-path".to_string(),
            "/bad\0path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL file path entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_empty_transaction_id_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on empty transaction id entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_nul_transaction_id_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx\0bad".to_string(),
            "/bad-txid.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL transaction id entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_nul_file_path_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-nul-path".to_string(),
            "/bad\0path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on NUL file path entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_startup_recovery_relative_file_path_entry_maps_to_unavailable() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-rel-path".to_string(),
            "rel/path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on relative file path entry");
        assert!(matches!(err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_ops_remain_unavailable_after_relative_file_path_recovery_failure() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal_path = temp.path().join("wal.log");
        let chunk_root = temp.path().join("chunks");

        let wal = WalLog::open(&wal_path).expect("wal should open");
        let mut bad = wal::WalEntry::new_pending(
            "tx-bad-rel-path".to_string(),
            "rel/path.txt".to_string(),
            wal::OperationType::Write,
            0,
        );
        bad = bad.with_status(wal::TxStatus::Committed);
        wal.append(&bad)
            .expect("malformed committed entry should be appended");

        let chunks = Arc::new(FsChunkStore::new(&chunk_root).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let fuse = FuseApi::new(core);

        let startup_err = fuse
            .startup_recover()
            .expect_err("startup recovery should fail on relative file path entry");
        assert!(matches!(startup_err, FuseError::Unavailable));

        let open_err = fuse.open("/bad.txt").expect_err("open should be unavailable");
        assert!(matches!(open_err, FuseError::Unavailable));

        let read_err = fuse.read("/bad.txt", 0, 1).expect_err("read should be unavailable");
        assert!(matches!(read_err, FuseError::Unavailable));

        let write_err = fuse
            .write("/bad.txt", 0, b"x")
            .expect_err("write should be unavailable");
        assert!(matches!(write_err, FuseError::Unavailable));

        let unlink_err = fuse
            .unlink("/bad.txt", 0)
            .expect_err("unlink should be unavailable");
        assert!(matches!(unlink_err, FuseError::Unavailable));
    }

    #[test]
    fn fuse_daemon_routes_requests_end_to_end() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();
        assert!(daemon.is_worker_alive(), "daemon worker should be alive after spawn");
        daemon.health().expect("health probe should pass");

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let tx_id = daemon
            .write("/daemon/data.txt", 0, b"daemon-data")
            .expect("write should pass");
        assert!(!tx_id.is_empty());
        let create_only_tx = daemon
            .write_if_missing("/daemon/extra.txt", b"extra")
            .expect("write_if_missing should pass");
        assert!(!create_only_tx.is_empty());

        let got = daemon
            .read("/daemon/data.txt", 7, 4)
            .expect("read should pass");
        assert_eq!(got, b"data");
        let full = daemon
            .read_all("/daemon/data.txt")
            .expect("read_all should pass");
        assert_eq!(full, b"daemon-data");

        let dirs = daemon.readdir("/").expect("root list should pass");
        assert_eq!(dirs, vec!["daemon".to_string()]);
        let files = daemon.readdir("/daemon").expect("dir list should pass");
        assert_eq!(files, vec!["data.txt".to_string(), "extra.txt".to_string()]);
        let extra = daemon
            .read_all("/daemon/extra.txt")
            .expect("extra file should be readable");
        assert_eq!(extra, b"extra");

        daemon
            .rename("/daemon/data.txt", "/daemon/moved.txt")
            .expect("rename should pass");
        let moved = daemon.readdir("/daemon").expect("dir list should pass");
        assert_eq!(moved, vec!["extra.txt".to_string(), "moved.txt".to_string()]);

        daemon.open("/daemon/moved.txt").expect("open should pass");
        daemon.unlink("/daemon/moved.txt", 1).expect("unlink should pass");
        daemon.unlink("/daemon/extra.txt", 1).expect("unlink should pass");
        daemon.rmdir("/daemon").expect("rmdir should pass");
        let dirs_after = daemon.readdir("/").expect("root list should pass");
        assert!(dirs_after.is_empty());
        let err = daemon
            .read("/daemon/moved.txt", 0, 1)
            .expect_err("post-delete read should fail");
        assert!(matches!(err, FuseError::NotFound));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_mount_gate_state_flips_after_startup_recovery() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        assert!(
            !daemon
                .is_mount_open()
                .expect("is_mount_open should work before recovery"),
            "mount gate should be closed before recovery"
        );
        daemon.startup_recover().expect("recovery should pass");
        assert!(
            daemon
                .is_mount_open()
                .expect("is_mount_open should work after recovery"),
            "mount gate should be open after recovery"
        );

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_missing_creates_and_conflicts_on_existing_path() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let tx = daemon
            .write_if_missing("/daemon/new.txt", b"first")
            .expect("write_if_missing should create");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/new.txt")
            .expect("read_all should pass");
        assert_eq!(bytes, b"first");

        let file_err = daemon
            .write_if_missing("/daemon/new.txt", b"second")
            .expect_err("write_if_missing should fail when file exists");
        assert!(matches!(file_err, FuseError::Conflict));

        let dir_err = daemon
            .write_if_missing("/daemon", b"second")
            .expect_err("write_if_missing should fail when directory exists");
        assert!(matches!(dir_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_version_updates_and_conflicts_on_stale_version() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"v1")
            .expect("write should pass");
        let tx = daemon
            .write_if_version("/daemon/a.txt", 1, b"v2")
            .expect("write_if_version should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/a.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"v2");
        let err = daemon
            .write_if_version("/daemon/a.txt", 1, b"v3")
            .expect_err("stale version should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_compare_and_swap_file_updates_and_conflicts() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"v1")
            .expect("write should pass");
        let tx = daemon
            .compare_and_swap_file("/daemon/a.txt", 1, b"v1", b"v2")
            .expect("compare_and_swap_file should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let content_err = daemon
            .compare_and_swap_file("/daemon/a.txt", 2, b"other", b"v3")
            .expect_err("content mismatch should fail");
        assert!(matches!(content_err, FuseError::Conflict));

        let version_err = daemon
            .compare_and_swap_file("/daemon/a.txt", 1, b"v2", b"v3")
            .expect_err("stale version should fail");
        assert!(matches!(version_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_hash_updates_and_conflicts() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"v1")
            .expect("write should pass");
        let expected_hash = daemon.file_hash("/daemon/a.txt").expect("hash should pass");
        let tx = daemon
            .write_if_hash("/daemon/a.txt", 1, &expected_hash, b"v2")
            .expect("write_if_hash should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let hash_err = daemon
            .write_if_hash("/daemon/a.txt", 2, "deadbeef", b"v3")
            .expect_err("hash mismatch should fail");
        assert!(matches!(hash_err, FuseError::Conflict));

        let version_err = daemon
            .write_if_hash(
                "/daemon/a.txt",
                1,
                &daemon.file_hash("/daemon/a.txt").expect("hash should pass"),
                b"v3",
            )
            .expect_err("stale version should fail");
        assert!(matches!(version_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_updates_and_conflicts() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"v1")
            .expect("write should pass");
        let size = daemon.file_size("/daemon/a.txt").expect("size should pass");
        let tx = daemon
            .write_if_size("/daemon/a.txt", 1, size, b"v2")
            .expect("write_if_size should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let size_err = daemon
            .write_if_size("/daemon/a.txt", 2, 7, b"v3")
            .expect_err("size mismatch should fail");
        assert!(matches!(size_err, FuseError::Conflict));

        let version_err = daemon
            .write_if_size(
                "/daemon/a.txt",
                1,
                daemon.file_size("/daemon/a.txt").expect("size should pass"),
                b"v3",
            )
            .expect_err("stale version should fail");
        assert!(matches!(version_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_exists_updates_and_rejects_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"v1")
            .expect("write should pass");

        let tx = daemon
            .write_if_exists("/daemon/a.txt", b"v2")
            .expect("write_if_exists should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        let missing_err = daemon
            .write_if_exists("/daemon/missing.txt", b"v2")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_exists("/daemon", b"v2")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_empty_updates_and_rejects_non_empty_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .touch_file("/daemon/empty.txt", 0)
            .expect("touch should pass");

        let tx = daemon
            .write_if_empty("/daemon/empty.txt", b"v1")
            .expect("write_if_empty should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/empty.txt").expect("read should pass");
        assert_eq!(bytes, b"v1");

        let non_empty_err = daemon
            .write_if_empty("/daemon/empty.txt", b"v2")
            .expect_err("non-empty file should fail");
        assert!(matches!(non_empty_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_empty("/daemon/missing.txt", b"v2")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_empty("/daemon", b"v2")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_not_empty_updates_and_rejects_empty_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon.write("/daemon/a.txt", 0, b"v1").expect("write should pass");

        let tx = daemon
            .write_if_not_empty("/daemon/a.txt", b"v2")
            .expect("write_if_not_empty should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"v2");

        daemon
            .touch_file("/daemon/empty.txt", 0)
            .expect("touch should pass");
        let empty_err = daemon
            .write_if_not_empty("/daemon/empty.txt", b"v2")
            .expect_err("empty file should fail");
        assert!(matches!(empty_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_not_empty("/daemon/missing.txt", b"v2")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_not_empty("/daemon", b"v2")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_starts_with_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"prefix-value")
            .expect("write should pass");

        let tx = daemon
            .write_if_starts_with("/daemon/a.txt", b"prefix-", b"next-value")
            .expect("write_if_starts_with should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = daemon
            .write_if_starts_with("/daemon/a.txt", b"other-", b"again")
            .expect_err("prefix mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_starts_with("/daemon/missing.txt", b"prefix-", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_starts_with("/daemon", b"prefix-", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_ends_with_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"value-suffix")
            .expect("write should pass");

        let tx = daemon
            .write_if_ends_with("/daemon/a.txt", b"-suffix", b"next-value")
            .expect("write_if_ends_with should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = daemon
            .write_if_ends_with("/daemon/a.txt", b"-other", b"again")
            .expect_err("suffix mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_ends_with("/daemon/missing.txt", b"-suffix", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_ends_with("/daemon", b"-suffix", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_contains_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"alpha-beta-gamma")
            .expect("write should pass");

        let tx = daemon
            .write_if_contains("/daemon/a.txt", b"beta", b"next-value")
            .expect("write_if_contains should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = daemon
            .write_if_contains("/daemon/a.txt", b"delta", b"again")
            .expect_err("contains mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_contains("/daemon/missing.txt", b"beta", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_contains("/daemon", b"beta", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_not_contains_updates_and_rejects_match_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"alpha-beta-gamma")
            .expect("write should pass");

        let tx = daemon
            .write_if_not_contains("/daemon/a.txt", b"delta", b"next-value")
            .expect("write_if_not_contains should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let match_err = daemon
            .write_if_not_contains("/daemon/a.txt", b"value", b"again")
            .expect_err("subsequence present should fail");
        assert!(matches!(match_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_not_contains("/daemon/missing.txt", b"beta", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_not_contains("/daemon", b"beta", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_exact_updates_and_rejects_mismatch_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"expected-body")
            .expect("write should pass");

        let tx = daemon
            .write_if_exact("/daemon/a.txt", b"expected-body", b"next-value")
            .expect("write_if_exact should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mismatch_err = daemon
            .write_if_exact("/daemon/a.txt", b"other-body", b"again")
            .expect_err("mismatch should fail");
        assert!(matches!(mismatch_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_exact("/daemon/missing.txt", b"expected-body", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_exact("/daemon", b"expected-body", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_not_exact_updates_and_rejects_exact_match_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"current-body")
            .expect("write should pass");

        let tx = daemon
            .write_if_not_exact("/daemon/a.txt", b"blocked-body", b"next-value")
            .expect("write_if_not_exact should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let match_err = daemon
            .write_if_not_exact("/daemon/a.txt", b"next-value", b"again")
            .expect_err("exact forbidden match should fail");
        assert!(matches!(match_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_not_exact("/daemon/missing.txt", b"current-body", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_not_exact("/daemon", b"current-body", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_min_size_updates_and_rejects_small_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abcdef")
            .expect("write should pass");

        let tx = daemon
            .write_if_min_size("/daemon/a.txt", 6, b"next-value")
            .expect("write_if_min_size should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let small_err = daemon
            .write_if_min_size("/daemon/a.txt", 20, b"again")
            .expect_err("file smaller than threshold should fail");
        assert!(matches!(small_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_min_size("/daemon/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_min_size("/daemon", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_max_size_updates_and_rejects_large_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abc")
            .expect("write should pass");

        let tx = daemon
            .write_if_max_size("/daemon/a.txt", 3, b"next-value")
            .expect("write_if_max_size should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let large_err = daemon
            .write_if_max_size("/daemon/a.txt", 5, b"again")
            .expect_err("file larger than threshold should fail");
        assert!(matches!(large_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_max_size("/daemon/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_max_size("/daemon", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_between_updates_and_rejects_out_of_range_invalid_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abcdef")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_between("/daemon/a.txt", 3, 8, b"next-value")
            .expect("write_if_size_between should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let out_of_range_err = daemon
            .write_if_size_between("/daemon/a.txt", 20, 30, b"again")
            .expect_err("out-of-range should fail");
        assert!(matches!(out_of_range_err, FuseError::Conflict));

        let invalid_range_err = daemon
            .write_if_size_between("/daemon/a.txt", 10, 5, b"again")
            .expect_err("invalid range should fail");
        assert!(matches!(invalid_range_err, FuseError::InvalidRange));

        let missing_err = daemon
            .write_if_size_between("/daemon/missing.txt", 1, 8, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_between("/daemon", 1, 8, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_between_updates_and_rejects_in_range_invalid_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abcdef")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_between("/daemon/a.txt", 1, 3, b"next-value")
            .expect("write_if_size_not_between should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let in_range_err = daemon
            .write_if_size_not_between("/daemon/a.txt", 1, 20, b"again")
            .expect_err("in-range size should fail");
        assert!(matches!(in_range_err, FuseError::Conflict));

        let invalid_range_err = daemon
            .write_if_size_not_between("/daemon/a.txt", 10, 5, b"again")
            .expect_err("invalid range should fail");
        assert!(matches!(invalid_range_err, FuseError::InvalidRange));

        let missing_err = daemon
            .write_if_size_not_between("/daemon/missing.txt", 1, 8, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_between("/daemon", 1, 8, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_multiple_of_updates_and_rejects_non_divisible_zero_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abcdef")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_multiple_of("/daemon/a.txt", 3, b"next-value")
            .expect("write_if_size_multiple_of should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_divisible_err = daemon
            .write_if_size_multiple_of("/daemon/a.txt", 7, b"again")
            .expect_err("non-divisible size should fail");
        assert!(matches!(non_divisible_err, FuseError::Conflict));

        let zero_err = daemon
            .write_if_size_multiple_of("/daemon/a.txt", 0, b"again")
            .expect_err("zero divisor should fail");
        assert!(matches!(zero_err, FuseError::InvalidRange));

        let missing_err = daemon
            .write_if_size_multiple_of("/daemon/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_multiple_of("/daemon", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_multiple_of_updates_and_rejects_divisible_zero_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abcde")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_multiple_of("/daemon/a.txt", 3, b"next-value")
            .expect("write_if_size_not_multiple_of should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let divisible_err = daemon
            .write_if_size_not_multiple_of("/daemon/a.txt", 2, b"again")
            .expect_err("divisible size should fail");
        assert!(matches!(divisible_err, FuseError::Conflict));

        let zero_err = daemon
            .write_if_size_not_multiple_of("/daemon/a.txt", 0, b"again")
            .expect_err("zero divisor should fail");
        assert!(matches!(zero_err, FuseError::InvalidRange));

        let missing_err = daemon
            .write_if_size_not_multiple_of("/daemon/missing.txt", 1, b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_multiple_of("/daemon", 1, b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_odd_updates_and_rejects_even_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abcde")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_odd("/daemon/a.txt", b"next-value")
            .expect("write_if_size_odd should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let even_err = daemon
            .write_if_size_odd("/daemon/a.txt", b"again")
            .expect_err("even size should fail");
        assert!(matches!(even_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_odd("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_odd("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_even_updates_and_rejects_odd_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"abcdef")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_even("/daemon/a.txt", b"12345678901")
            .expect("write_if_size_even should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/a.txt").expect("read should pass");
        assert_eq!(bytes, b"12345678901");

        let odd_err = daemon
            .write_if_size_even("/daemon/a.txt", b"again")
            .expect_err("odd size should fail");
        assert!(matches!(odd_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_even("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_even("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_two_updates_and_rejects_zero_non_power_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/pow2.txt", 0, b"12345678")
            .expect("write should pass");
        daemon
            .touch_file("/daemon/zero.txt", 0)
            .expect("touch should pass");
        daemon
            .write("/daemon/nonpower.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_two("/daemon/pow2.txt", b"next-value")
            .expect("write_if_size_power_of_two should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow2.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let zero_err = daemon
            .write_if_size_power_of_two("/daemon/zero.txt", b"again")
            .expect_err("zero size should fail");
        assert!(matches!(zero_err, FuseError::Conflict));

        let non_power_err = daemon
            .write_if_size_power_of_two("/daemon/nonpower.txt", b"again")
            .expect_err("non-power size should fail");
        assert!(matches!(non_power_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_two("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_two("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_two_updates_and_rejects_power_of_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonpower.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/pow2.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_two("/daemon/nonpower.txt", b"next-value")
            .expect("write_if_size_not_power_of_two should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpower.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let power_err = daemon
            .write_if_size_not_power_of_two("/daemon/pow2.txt", b"again")
            .expect_err("power-of-two size should fail");
        assert!(matches!(power_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_two("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_two("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_prime_updates_and_rejects_non_prime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/prime.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/nonprime.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_prime("/daemon/prime.txt", b"next-value")
            .expect("write_if_size_prime should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/prime.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_prime_err = daemon
            .write_if_size_prime("/daemon/nonprime.txt", b"again")
            .expect_err("non-prime size should fail");
        assert!(matches!(non_prime_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_prime("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_prime("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_prime_updates_and_rejects_prime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonprime.txt", 0, b"12345678")
            .expect("write should pass");
        daemon
            .write("/daemon/prime.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_prime("/daemon/nonprime.txt", b"next-value")
            .expect("write_if_size_not_prime should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonprime.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let prime_err = daemon
            .write_if_size_not_prime("/daemon/prime.txt", b"again")
            .expect_err("prime size should fail");
        assert!(matches!(prime_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_prime("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_prime("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_fibonacci_updates_and_rejects_non_fibonacci_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/fib.txt", 0, b"12345678")
            .expect("write should pass");
        daemon
            .write("/daemon/nonfib.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_fibonacci("/daemon/fib.txt", b"next-value")
            .expect("write_if_size_fibonacci should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/fib.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_fib_err = daemon
            .write_if_size_fibonacci("/daemon/nonfib.txt", b"again")
            .expect_err("non-fibonacci size should fail");
        assert!(matches!(non_fib_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_fibonacci("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_fibonacci("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_fibonacci_updates_and_rejects_fibonacci_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonfib.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/fib.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_fibonacci("/daemon/nonfib.txt", b"next-value")
            .expect("write_if_size_not_fibonacci should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonfib.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let fib_err = daemon
            .write_if_size_not_fibonacci("/daemon/fib.txt", b"again")
            .expect_err("fibonacci size should fail");
        assert!(matches!(fib_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_fibonacci("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_fibonacci("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_square_updates_and_rejects_non_square_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/square.txt", 0, b"123456789")
            .expect("write should pass");
        daemon
            .write("/daemon/nonsquare.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_square("/daemon/square.txt", b"next-value")
            .expect("write_if_size_square should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/square.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_square_err = daemon
            .write_if_size_square("/daemon/nonsquare.txt", b"again")
            .expect_err("non-square size should fail");
        assert!(matches!(non_square_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_square("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_square("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_square_updates_and_rejects_square_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonsquare.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/square.txt", 0, b"123456789")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_square("/daemon/nonsquare.txt", b"next-value")
            .expect("write_if_size_not_square should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonsquare.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let square_err = daemon
            .write_if_size_not_square("/daemon/square.txt", b"again")
            .expect_err("square size should fail");
        assert!(matches!(square_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_square("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_square("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_cube_updates_and_rejects_non_cube_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/cube.txt", 0, b"12345678")
            .expect("write should pass");
        daemon
            .write("/daemon/noncube.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_cube("/daemon/cube.txt", b"next-value")
            .expect("write_if_size_cube should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/cube.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_cube_err = daemon
            .write_if_size_cube("/daemon/noncube.txt", b"again")
            .expect_err("non-cube size should fail");
        assert!(matches!(non_cube_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_cube("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_cube("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_cube_updates_and_rejects_cube_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/noncube.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/cube.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_cube("/daemon/noncube.txt", b"next-value")
            .expect("write_if_size_not_cube should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/noncube.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let cube_err = daemon
            .write_if_size_not_cube("/daemon/cube.txt", b"again")
            .expect_err("cube size should fail");
        assert!(matches!(cube_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_cube("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_cube("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_triangular_updates_and_rejects_non_triangular_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/tri.txt", 0, b"123456")
            .expect("write should pass");
        daemon
            .write("/daemon/nontri.txt", 0, b"12345")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_triangular("/daemon/tri.txt", b"next-value")
            .expect("write_if_size_triangular should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/tri.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_tri_err = daemon
            .write_if_size_triangular("/daemon/nontri.txt", b"again")
            .expect_err("non-triangular size should fail");
        assert!(matches!(non_tri_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_triangular("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_triangular("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_triangular_updates_and_rejects_triangular_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nontri.txt", 0, b"12345")
            .expect("write should pass");
        daemon
            .write("/daemon/tri.txt", 0, b"123456")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_triangular("/daemon/nontri.txt", b"next-value")
            .expect("write_if_size_not_triangular should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nontri.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let tri_err = daemon
            .write_if_size_not_triangular("/daemon/tri.txt", b"again")
            .expect_err("triangular size should fail");
        assert!(matches!(tri_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_triangular("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_triangular("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_factorial_updates_and_rejects_non_factorial_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/factorial.txt", 0, b"123456")
            .expect("write should pass");
        daemon
            .write("/daemon/nonfactorial.txt", 0, b"12345")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_factorial("/daemon/factorial.txt", b"next-value")
            .expect("write_if_size_factorial should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/factorial.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_factorial_err = daemon
            .write_if_size_factorial("/daemon/nonfactorial.txt", b"again")
            .expect_err("non-factorial size should fail");
        assert!(matches!(non_factorial_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_factorial("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_factorial("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_factorial_updates_and_rejects_factorial_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonfactorial.txt", 0, b"12345")
            .expect("write should pass");
        daemon
            .write("/daemon/factorial.txt", 0, b"123456")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_factorial("/daemon/nonfactorial.txt", b"next-value")
            .expect("write_if_size_not_factorial should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonfactorial.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let factorial_err = daemon
            .write_if_size_not_factorial("/daemon/factorial.txt", b"again")
            .expect_err("factorial size should fail");
        assert!(matches!(factorial_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_factorial("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_factorial("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_composite_updates_and_rejects_non_composite_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/composite.txt", 0, b"12345678")
            .expect("write should pass");
        daemon
            .write("/daemon/prime.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_composite("/daemon/composite.txt", b"next-value")
            .expect("write_if_size_composite should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/composite.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_composite_err = daemon
            .write_if_size_composite("/daemon/prime.txt", b"again")
            .expect_err("non-composite size should fail");
        assert!(matches!(non_composite_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_composite("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_composite("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_composite_updates_and_rejects_composite_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/prime.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/composite.txt", 0, b"12345678")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_composite("/daemon/prime.txt", b"next-value")
            .expect("write_if_size_not_composite should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/prime.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let composite_err = daemon
            .write_if_size_not_composite("/daemon/composite.txt", b"again")
            .expect_err("composite size should fail");
        assert!(matches!(composite_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_composite("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_composite("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_perfect_updates_and_rejects_non_perfect_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/perfect.txt", 0, b"123456")
            .expect("write should pass");
        daemon
            .write("/daemon/nonperfect.txt", 0, b"12345")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_perfect("/daemon/perfect.txt", b"next-value")
            .expect("write_if_size_perfect should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/perfect.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_perfect_err = daemon
            .write_if_size_perfect("/daemon/nonperfect.txt", b"again")
            .expect_err("non-perfect size should fail");
        assert!(matches!(non_perfect_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_perfect("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_perfect("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_perfect_updates_and_rejects_perfect_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonperfect.txt", 0, b"12345")
            .expect("write should pass");
        daemon
            .write("/daemon/perfect.txt", 0, b"123456")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_perfect("/daemon/nonperfect.txt", b"next-value")
            .expect("write_if_size_not_perfect should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/nonperfect.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let perfect_err = daemon
            .write_if_size_not_perfect("/daemon/perfect.txt", b"again")
            .expect_err("perfect size should fail");
        assert!(matches!(perfect_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_perfect("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_perfect("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_abundant_updates_and_rejects_non_abundant_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/abundant.txt", 0, b"123456789012")
            .expect("write should pass");
        daemon
            .write("/daemon/nonabundant.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_abundant("/daemon/abundant.txt", b"next-value")
            .expect("write_if_size_abundant should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/abundant.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_abundant_err = daemon
            .write_if_size_abundant("/daemon/nonabundant.txt", b"again")
            .expect_err("non-abundant size should fail");
        assert!(matches!(non_abundant_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_abundant("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_abundant("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_abundant_updates_and_rejects_abundant_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonabundant.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/abundant.txt", 0, b"123456789012")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_abundant("/daemon/nonabundant.txt", b"next-value")
            .expect("write_if_size_not_abundant should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonabundant.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let abundant_err = daemon
            .write_if_size_not_abundant("/daemon/abundant.txt", b"again")
            .expect_err("abundant size should fail");
        assert!(matches!(abundant_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_abundant("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_abundant("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_deficient_updates_and_rejects_non_deficient_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/deficient.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/nondeficient.txt", 0, b"123456789012")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_deficient("/daemon/deficient.txt", b"next-value")
            .expect("write_if_size_deficient should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/deficient.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_deficient_err = daemon
            .write_if_size_deficient("/daemon/nondeficient.txt", b"again")
            .expect_err("non-deficient size should fail");
        assert!(matches!(non_deficient_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_deficient("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_deficient("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_deficient_updates_and_rejects_deficient_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nondeficient.txt", 0, b"123456789012")
            .expect("write should pass");
        daemon
            .write("/daemon/deficient.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_deficient("/daemon/nondeficient.txt", b"next-value")
            .expect("write_if_size_not_deficient should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nondeficient.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let deficient_err = daemon
            .write_if_size_not_deficient("/daemon/deficient.txt", b"again")
            .expect_err("deficient size should fail");
        assert!(matches!(deficient_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_deficient("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_deficient("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_semiprime_updates_and_rejects_non_semiprime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/semiprime.txt", 0, b"123456")
            .expect("write should pass");
        daemon
            .write("/daemon/nonsemiprime.txt", 0, b"1234567")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_semiprime("/daemon/semiprime.txt", b"next-value")
            .expect("write_if_size_semiprime should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/semiprime.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_semiprime_err = daemon
            .write_if_size_semiprime("/daemon/nonsemiprime.txt", b"again")
            .expect_err("non-semiprime size should fail");
        assert!(matches!(non_semiprime_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_semiprime("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_semiprime("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_semiprime_updates_and_rejects_semiprime_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonsemiprime.txt", 0, b"1234567")
            .expect("write should pass");
        daemon
            .write("/daemon/semiprime.txt", 0, b"123456")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_semiprime("/daemon/nonsemiprime.txt", b"next-value")
            .expect("write_if_size_not_semiprime should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonsemiprime.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let semiprime_err = daemon
            .write_if_size_not_semiprime("/daemon/semiprime.txt", b"again")
            .expect_err("semiprime size should fail");
        assert!(matches!(semiprime_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_semiprime("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_semiprime("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_palindrome_updates_and_rejects_non_palindrome_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/palindrome.txt", 0, b"12345678901")
            .expect("write should pass");
        daemon
            .write("/daemon/nonpalindrome.txt", 0, b"1234567890")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_palindrome("/daemon/palindrome.txt", b"next-value")
            .expect("write_if_size_palindrome should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/palindrome.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_palindrome_err = daemon
            .write_if_size_palindrome("/daemon/nonpalindrome.txt", b"again")
            .expect_err("non-palindrome size should fail");
        assert!(matches!(non_palindrome_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_palindrome("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_palindrome("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_palindrome_updates_and_rejects_palindrome_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/nonpalindrome.txt", 0, b"1234567890")
            .expect("write should pass");
        daemon
            .write("/daemon/palindrome.txt", 0, b"12345678901")
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_palindrome("/daemon/nonpalindrome.txt", b"next-value")
            .expect("write_if_size_not_palindrome should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpalindrome.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let palindrome_err = daemon
            .write_if_size_not_palindrome("/daemon/palindrome.txt", b"again")
            .expect_err("palindrome size should fail");
        assert!(matches!(palindrome_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_palindrome("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_palindrome("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_armstrong_updates_and_rejects_non_armstrong_missing_or_directory()
    {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let armstrong_seed = vec![b'a'; 153];
        let non_armstrong_seed = vec![b'a'; 154];
        daemon
            .write("/daemon/armstrong.txt", 0, &armstrong_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonarmstrong.txt", 0, &non_armstrong_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_armstrong("/daemon/armstrong.txt", b"next-value")
            .expect("write_if_size_armstrong should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/armstrong.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_armstrong_err = daemon
            .write_if_size_armstrong("/daemon/nonarmstrong.txt", b"again")
            .expect_err("non-armstrong size should fail");
        assert!(matches!(non_armstrong_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_armstrong("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_armstrong("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_armstrong_updates_and_rejects_armstrong_missing_or_directory()
    {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_armstrong_seed = vec![b'a'; 154];
        let armstrong_seed = vec![b'a'; 153];
        daemon
            .write("/daemon/nonarmstrong.txt", 0, &non_armstrong_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/armstrong.txt", 0, &armstrong_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_armstrong("/daemon/nonarmstrong.txt", b"next-value")
            .expect("write_if_size_not_armstrong should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonarmstrong.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let armstrong_err = daemon
            .write_if_size_not_armstrong("/daemon/armstrong.txt", b"again")
            .expect_err("armstrong size should fail");
        assert!(matches!(armstrong_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_armstrong("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_armstrong("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_happy_updates_and_rejects_non_happy_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let happy_seed = vec![b'a'; 19];
        let non_happy_seed = vec![b'a'; 20];
        daemon
            .write("/daemon/happy.txt", 0, &happy_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonhappy.txt", 0, &non_happy_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_happy("/daemon/happy.txt", b"next-value")
            .expect("write_if_size_happy should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/happy.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_happy_err = daemon
            .write_if_size_happy("/daemon/nonhappy.txt", b"again")
            .expect_err("non-happy size should fail");
        assert!(matches!(non_happy_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_happy("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_happy("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_happy_updates_and_rejects_happy_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_happy_seed = vec![b'a'; 20];
        let happy_seed = vec![b'a'; 19];
        daemon
            .write("/daemon/nonhappy.txt", 0, &non_happy_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/happy.txt", 0, &happy_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_happy("/daemon/nonhappy.txt", b"next-value")
            .expect("write_if_size_not_happy should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonhappy.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let happy_err = daemon
            .write_if_size_not_happy("/daemon/happy.txt", b"again")
            .expect_err("happy size should fail");
        assert!(matches!(happy_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_happy("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_happy("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_automorphic_updates_and_rejects_non_automorphic_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let automorphic_seed = vec![b'a'; 25];
        let non_automorphic_seed = vec![b'a'; 26];
        daemon
            .write("/daemon/automorphic.txt", 0, &automorphic_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonautomorphic.txt", 0, &non_automorphic_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_automorphic("/daemon/automorphic.txt", b"next-value")
            .expect("write_if_size_automorphic should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/automorphic.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_automorphic_err = daemon
            .write_if_size_automorphic("/daemon/nonautomorphic.txt", b"again")
            .expect_err("non-automorphic size should fail");
        assert!(matches!(non_automorphic_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_automorphic("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_automorphic("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_automorphic_updates_and_rejects_automorphic_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_automorphic_seed = vec![b'a'; 26];
        let automorphic_seed = vec![b'a'; 25];
        daemon
            .write("/daemon/nonautomorphic.txt", 0, &non_automorphic_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/automorphic.txt", 0, &automorphic_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_automorphic("/daemon/nonautomorphic.txt", b"next-value")
            .expect("write_if_size_not_automorphic should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonautomorphic.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let automorphic_err = daemon
            .write_if_size_not_automorphic("/daemon/automorphic.txt", b"again")
            .expect_err("automorphic size should fail");
        assert!(matches!(automorphic_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_automorphic("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_automorphic("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_harshad_updates_and_rejects_non_harshad_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let harshad_seed = vec![b'a'; 18];
        let non_harshad_seed = vec![b'a'; 19];
        daemon
            .write("/daemon/harshad.txt", 0, &harshad_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonharshad.txt", 0, &non_harshad_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_harshad("/daemon/harshad.txt", b"next-value")
            .expect("write_if_size_harshad should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/harshad.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_harshad_err = daemon
            .write_if_size_harshad("/daemon/nonharshad.txt", b"again")
            .expect_err("non-harshad size should fail");
        assert!(matches!(non_harshad_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_harshad("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_harshad("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_harshad_updates_and_rejects_harshad_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_harshad_seed = vec![b'a'; 19];
        let harshad_seed = vec![b'a'; 18];
        daemon
            .write("/daemon/nonharshad.txt", 0, &non_harshad_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/harshad.txt", 0, &harshad_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_harshad("/daemon/nonharshad.txt", b"next-value")
            .expect("write_if_size_not_harshad should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonharshad.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let harshad_err = daemon
            .write_if_size_not_harshad("/daemon/harshad.txt", b"again")
            .expect_err("harshad size should fail");
        assert!(matches!(harshad_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_harshad("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_harshad("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_kaprekar_updates_and_rejects_non_kaprekar_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let kaprekar_seed = vec![b'a'; 45];
        let non_kaprekar_seed = vec![b'a'; 46];
        daemon
            .write("/daemon/kaprekar.txt", 0, &kaprekar_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonkaprekar.txt", 0, &non_kaprekar_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_kaprekar("/daemon/kaprekar.txt", b"next-value")
            .expect("write_if_size_kaprekar should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/kaprekar.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_kaprekar_err = daemon
            .write_if_size_kaprekar("/daemon/nonkaprekar.txt", b"again")
            .expect_err("non-kaprekar size should fail");
        assert!(matches!(non_kaprekar_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_kaprekar("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_kaprekar("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_kaprekar_updates_and_rejects_kaprekar_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_kaprekar_seed = vec![b'a'; 46];
        let kaprekar_seed = vec![b'a'; 45];
        daemon
            .write("/daemon/nonkaprekar.txt", 0, &non_kaprekar_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/kaprekar.txt", 0, &kaprekar_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_kaprekar("/daemon/nonkaprekar.txt", b"next-value")
            .expect("write_if_size_not_kaprekar should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonkaprekar.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let kaprekar_err = daemon
            .write_if_size_not_kaprekar("/daemon/kaprekar.txt", b"again")
            .expect_err("kaprekar size should fail");
        assert!(matches!(kaprekar_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_kaprekar("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_kaprekar("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_repdigit_updates_and_rejects_non_repdigit_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let repdigit_seed = vec![b'a'; 11];
        let non_repdigit_seed = vec![b'a'; 12];
        daemon
            .write("/daemon/repdigit.txt", 0, &repdigit_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonrepdigit.txt", 0, &non_repdigit_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_repdigit("/daemon/repdigit.txt", b"next-value")
            .expect("write_if_size_repdigit should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/repdigit.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_repdigit_err = daemon
            .write_if_size_repdigit("/daemon/nonrepdigit.txt", b"again")
            .expect_err("non-repdigit size should fail");
        assert!(matches!(non_repdigit_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_repdigit("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_repdigit("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_repdigit_updates_and_rejects_repdigit_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_repdigit_seed = vec![b'a'; 12];
        let repdigit_seed = vec![b'a'; 11];
        daemon
            .write("/daemon/nonrepdigit.txt", 0, &non_repdigit_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/repdigit.txt", 0, &repdigit_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_repdigit("/daemon/nonrepdigit.txt", b"next-value")
            .expect("write_if_size_not_repdigit should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonrepdigit.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let repdigit_err = daemon
            .write_if_size_not_repdigit("/daemon/repdigit.txt", b"again")
            .expect_err("repdigit size should fail");
        assert!(matches!(repdigit_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_repdigit("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_repdigit("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_tribonacci_updates_and_rejects_non_tribonacci_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let tribonacci_seed = vec![b'a'; 24];
        let non_tribonacci_seed = vec![b'a'; 25];
        daemon
            .write("/daemon/tribonacci.txt", 0, &tribonacci_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nontribonacci.txt", 0, &non_tribonacci_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_tribonacci("/daemon/tribonacci.txt", b"next-value")
            .expect("write_if_size_tribonacci should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/tribonacci.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_tribonacci_err = daemon
            .write_if_size_tribonacci("/daemon/nontribonacci.txt", b"again")
            .expect_err("non-tribonacci size should fail");
        assert!(matches!(non_tribonacci_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_tribonacci("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_tribonacci("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_tribonacci_updates_and_rejects_tribonacci_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_tribonacci_seed = vec![b'a'; 25];
        let tribonacci_seed = vec![b'a'; 24];
        daemon
            .write("/daemon/nontribonacci.txt", 0, &non_tribonacci_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/tribonacci.txt", 0, &tribonacci_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_tribonacci("/daemon/nontribonacci.txt", b"next-value")
            .expect("write_if_size_not_tribonacci should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nontribonacci.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let tribonacci_err = daemon
            .write_if_size_not_tribonacci("/daemon/tribonacci.txt", b"again")
            .expect_err("tribonacci size should fail");
        assert!(matches!(tribonacci_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_tribonacci("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_tribonacci("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_pell_updates_and_rejects_non_pell_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pell_seed = vec![b'a'; 29];
        let non_pell_seed = vec![b'a'; 30];
        daemon
            .write("/daemon/pell.txt", 0, &pell_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpell.txt", 0, &non_pell_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_pell("/daemon/pell.txt", b"next-value")
            .expect("write_if_size_pell should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pell.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pell_err = daemon
            .write_if_size_pell("/daemon/nonpell.txt", b"again")
            .expect_err("non-pell size should fail");
        assert!(matches!(non_pell_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_pell("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_pell("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_pell_updates_and_rejects_pell_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pell_seed = vec![b'a'; 30];
        let pell_seed = vec![b'a'; 29];
        daemon
            .write("/daemon/nonpell.txt", 0, &non_pell_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pell.txt", 0, &pell_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_pell("/daemon/nonpell.txt", b"next-value")
            .expect("write_if_size_not_pell should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpell.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pell_err = daemon
            .write_if_size_not_pell("/daemon/pell.txt", b"again")
            .expect_err("pell size should fail");
        assert!(matches!(pell_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_pell("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_pell("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_lucas_updates_and_rejects_non_lucas_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let lucas_seed = vec![b'a'; 29];
        let non_lucas_seed = vec![b'a'; 30];
        daemon
            .write("/daemon/lucas.txt", 0, &lucas_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonlucas.txt", 0, &non_lucas_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_lucas("/daemon/lucas.txt", b"next-value")
            .expect("write_if_size_lucas should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/lucas.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_lucas_err = daemon
            .write_if_size_lucas("/daemon/nonlucas.txt", b"again")
            .expect_err("non-lucas size should fail");
        assert!(matches!(non_lucas_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_lucas("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_lucas("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_lucas_updates_and_rejects_lucas_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_lucas_seed = vec![b'a'; 30];
        let lucas_seed = vec![b'a'; 29];
        daemon
            .write("/daemon/nonlucas.txt", 0, &non_lucas_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/lucas.txt", 0, &lucas_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_lucas("/daemon/nonlucas.txt", b"next-value")
            .expect("write_if_size_not_lucas should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonlucas.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let lucas_err = daemon
            .write_if_size_not_lucas("/daemon/lucas.txt", b"again")
            .expect_err("lucas size should fail");
        assert!(matches!(lucas_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_lucas("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_lucas("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_mersenne_updates_and_rejects_non_mersenne_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let mersenne_seed = vec![b'a'; 31];
        let non_mersenne_seed = vec![b'a'; 32];
        daemon
            .write("/daemon/mersenne.txt", 0, &mersenne_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonmersenne.txt", 0, &non_mersenne_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_mersenne("/daemon/mersenne.txt", b"next-value")
            .expect("write_if_size_mersenne should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/mersenne.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_mersenne_err = daemon
            .write_if_size_mersenne("/daemon/nonmersenne.txt", b"again")
            .expect_err("non-mersenne size should fail");
        assert!(matches!(non_mersenne_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_mersenne("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_mersenne("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_mersenne_updates_and_rejects_mersenne_missing_or_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_mersenne_seed = vec![b'a'; 32];
        let mersenne_seed = vec![b'a'; 31];
        daemon
            .write("/daemon/nonmersenne.txt", 0, &non_mersenne_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/mersenne.txt", 0, &mersenne_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_mersenne("/daemon/nonmersenne.txt", b"next-value")
            .expect("write_if_size_not_mersenne should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonmersenne.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let mersenne_err = daemon
            .write_if_size_not_mersenne("/daemon/mersenne.txt", b"again")
            .expect_err("mersenne size should fail");
        assert!(matches!(mersenne_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_mersenne("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_mersenne("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_three_updates_and_rejects_non_power_of_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow3_seed = vec![b'a'; 27];
        let non_pow3_seed = vec![b'a'; 28];
        daemon
            .write("/daemon/pow3.txt", 0, &pow3_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow3.txt", 0, &non_pow3_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_three("/daemon/pow3.txt", b"next-value")
            .expect("write_if_size_power_of_three should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow3.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow3_err = daemon
            .write_if_size_power_of_three("/daemon/nonpow3.txt", b"again")
            .expect_err("non-power-of-three size should fail");
        assert!(matches!(non_pow3_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_three("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_three("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_three_updates_and_rejects_power_of_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow3_seed = vec![b'a'; 28];
        let pow3_seed = vec![b'a'; 27];
        daemon
            .write("/daemon/nonpow3.txt", 0, &non_pow3_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow3.txt", 0, &pow3_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_three("/daemon/nonpow3.txt", b"next-value")
            .expect("write_if_size_not_power_of_three should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow3.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow3_err = daemon
            .write_if_size_not_power_of_three("/daemon/pow3.txt", b"again")
            .expect_err("power-of-three size should fail");
        assert!(matches!(pow3_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_three("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_three("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_four_updates_and_rejects_non_power_of_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow4_seed = vec![b'a'; 64];
        let non_pow4_seed = vec![b'a'; 65];
        daemon
            .write("/daemon/pow4.txt", 0, &pow4_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow4.txt", 0, &non_pow4_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_four("/daemon/pow4.txt", b"next-value")
            .expect("write_if_size_power_of_four should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow4.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow4_err = daemon
            .write_if_size_power_of_four("/daemon/nonpow4.txt", b"again")
            .expect_err("non-power-of-four size should fail");
        assert!(matches!(non_pow4_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_four("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_four("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_four_updates_and_rejects_power_of_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow4_seed = vec![b'a'; 65];
        let pow4_seed = vec![b'a'; 64];
        daemon
            .write("/daemon/nonpow4.txt", 0, &non_pow4_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow4.txt", 0, &pow4_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_four("/daemon/nonpow4.txt", b"next-value")
            .expect("write_if_size_not_power_of_four should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow4.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow4_err = daemon
            .write_if_size_not_power_of_four("/daemon/pow4.txt", b"again")
            .expect_err("power-of-four size should fail");
        assert!(matches!(pow4_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_four("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_four("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_five_updates_and_rejects_non_power_of_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow5_seed = vec![b'a'; 125];
        let non_pow5_seed = vec![b'a'; 126];
        daemon
            .write("/daemon/pow5.txt", 0, &pow5_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow5.txt", 0, &non_pow5_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_five("/daemon/pow5.txt", b"next-value")
            .expect("write_if_size_power_of_five should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow5.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow5_err = daemon
            .write_if_size_power_of_five("/daemon/nonpow5.txt", b"again")
            .expect_err("non-power-of-five size should fail");
        assert!(matches!(non_pow5_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_five("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_five("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_five_updates_and_rejects_power_of_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow5_seed = vec![b'a'; 126];
        let pow5_seed = vec![b'a'; 125];
        daemon
            .write("/daemon/nonpow5.txt", 0, &non_pow5_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow5.txt", 0, &pow5_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_five("/daemon/nonpow5.txt", b"next-value")
            .expect("write_if_size_not_power_of_five should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow5.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow5_err = daemon
            .write_if_size_not_power_of_five("/daemon/pow5.txt", b"again")
            .expect_err("power-of-five size should fail");
        assert!(matches!(pow5_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_five("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_five("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_six_updates_and_rejects_non_power_of_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow6_seed = vec![b'a'; 216];
        let non_pow6_seed = vec![b'a'; 217];
        daemon
            .write("/daemon/pow6.txt", 0, &pow6_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow6.txt", 0, &non_pow6_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_six("/daemon/pow6.txt", b"next-value")
            .expect("write_if_size_power_of_six should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow6.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow6_err = daemon
            .write_if_size_power_of_six("/daemon/nonpow6.txt", b"again")
            .expect_err("non-power-of-six size should fail");
        assert!(matches!(non_pow6_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_six("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_six("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_six_updates_and_rejects_power_of_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow6_seed = vec![b'a'; 217];
        let pow6_seed = vec![b'a'; 216];
        daemon
            .write("/daemon/nonpow6.txt", 0, &non_pow6_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow6.txt", 0, &pow6_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_six("/daemon/nonpow6.txt", b"next-value")
            .expect("write_if_size_not_power_of_six should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow6.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow6_err = daemon
            .write_if_size_not_power_of_six("/daemon/pow6.txt", b"again")
            .expect_err("power-of-six size should fail");
        assert!(matches!(pow6_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_six("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_six("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_seven_updates_and_rejects_non_power_of_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow7_seed = vec![b'a'; 343];
        let non_pow7_seed = vec![b'a'; 344];
        daemon
            .write("/daemon/pow7.txt", 0, &pow7_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow7.txt", 0, &non_pow7_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_seven("/daemon/pow7.txt", b"next-value")
            .expect("write_if_size_power_of_seven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow7.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow7_err = daemon
            .write_if_size_power_of_seven("/daemon/nonpow7.txt", b"again")
            .expect_err("non-power-of-seven size should fail");
        assert!(matches!(non_pow7_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_seven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_seven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_seven_updates_and_rejects_power_of_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow7_seed = vec![b'a'; 344];
        let pow7_seed = vec![b'a'; 343];
        daemon
            .write("/daemon/nonpow7.txt", 0, &non_pow7_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow7.txt", 0, &pow7_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_seven("/daemon/nonpow7.txt", b"next-value")
            .expect("write_if_size_not_power_of_seven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow7.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow7_err = daemon
            .write_if_size_not_power_of_seven("/daemon/pow7.txt", b"again")
            .expect_err("power-of-seven size should fail");
        assert!(matches!(pow7_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_seven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_seven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_eight_updates_and_rejects_non_power_of_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow8_seed = vec![b'a'; 512];
        let non_pow8_seed = vec![b'a'; 513];
        daemon
            .write("/daemon/pow8.txt", 0, &pow8_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow8.txt", 0, &non_pow8_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_eight("/daemon/pow8.txt", b"next-value")
            .expect("write_if_size_power_of_eight should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow8.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow8_err = daemon
            .write_if_size_power_of_eight("/daemon/nonpow8.txt", b"again")
            .expect_err("non-power-of-eight size should fail");
        assert!(matches!(non_pow8_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_eight("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_eight("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_eight_updates_and_rejects_power_of_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow8_seed = vec![b'a'; 513];
        let pow8_seed = vec![b'a'; 512];
        daemon
            .write("/daemon/nonpow8.txt", 0, &non_pow8_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow8.txt", 0, &pow8_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_eight("/daemon/nonpow8.txt", b"next-value")
            .expect("write_if_size_not_power_of_eight should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow8.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow8_err = daemon
            .write_if_size_not_power_of_eight("/daemon/pow8.txt", b"again")
            .expect_err("power-of-eight size should fail");
        assert!(matches!(pow8_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_eight("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_eight("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_nine_updates_and_rejects_non_power_of_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow9_seed = vec![b'a'; 729];
        let non_pow9_seed = vec![b'a'; 730];
        daemon
            .write("/daemon/pow9.txt", 0, &pow9_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow9.txt", 0, &non_pow9_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_nine("/daemon/pow9.txt", b"next-value")
            .expect("write_if_size_power_of_nine should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow9.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow9_err = daemon
            .write_if_size_power_of_nine("/daemon/nonpow9.txt", b"again")
            .expect_err("non-power-of-nine size should fail");
        assert!(matches!(non_pow9_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_nine("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_nine("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_nine_updates_and_rejects_power_of_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow9_seed = vec![b'a'; 730];
        let pow9_seed = vec![b'a'; 729];
        daemon
            .write("/daemon/nonpow9.txt", 0, &non_pow9_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow9.txt", 0, &pow9_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_nine("/daemon/nonpow9.txt", b"next-value")
            .expect("write_if_size_not_power_of_nine should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow9.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow9_err = daemon
            .write_if_size_not_power_of_nine("/daemon/pow9.txt", b"again")
            .expect_err("power-of-nine size should fail");
        assert!(matches!(pow9_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_nine("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_nine("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_ten_updates_and_rejects_non_power_of_ten_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow10_seed = vec![b'a'; 1000];
        let non_pow10_seed = vec![b'a'; 1001];
        daemon
            .write("/daemon/pow10.txt", 0, &pow10_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow10.txt", 0, &non_pow10_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_ten("/daemon/pow10.txt", b"next-value")
            .expect("write_if_size_power_of_ten should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow10.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow10_err = daemon
            .write_if_size_power_of_ten("/daemon/nonpow10.txt", b"again")
            .expect_err("non-power-of-ten size should fail");
        assert!(matches!(non_pow10_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_ten("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_ten("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_ten_updates_and_rejects_power_of_ten_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow10_seed = vec![b'a'; 1001];
        let pow10_seed = vec![b'a'; 1000];
        daemon
            .write("/daemon/nonpow10.txt", 0, &non_pow10_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow10.txt", 0, &pow10_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_ten("/daemon/nonpow10.txt", b"next-value")
            .expect("write_if_size_not_power_of_ten should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow10.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow10_err = daemon
            .write_if_size_not_power_of_ten("/daemon/pow10.txt", b"again")
            .expect_err("power-of-ten size should fail");
        assert!(matches!(pow10_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_ten("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_ten("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_eleven_updates_and_rejects_non_power_of_eleven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow11_seed = vec![b'a'; 121];
        let non_pow11_seed = vec![b'a'; 122];
        daemon
            .write("/daemon/pow11.txt", 0, &pow11_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow11.txt", 0, &non_pow11_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_eleven("/daemon/pow11.txt", b"next-value")
            .expect("write_if_size_power_of_eleven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow11.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow11_err = daemon
            .write_if_size_power_of_eleven("/daemon/nonpow11.txt", b"again")
            .expect_err("non-power-of-eleven size should fail");
        assert!(matches!(non_pow11_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_eleven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_eleven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_eleven_updates_and_rejects_power_of_eleven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow11_seed = vec![b'a'; 122];
        let pow11_seed = vec![b'a'; 121];
        daemon
            .write("/daemon/nonpow11.txt", 0, &non_pow11_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow11.txt", 0, &pow11_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_eleven("/daemon/nonpow11.txt", b"next-value")
            .expect("write_if_size_not_power_of_eleven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow11.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow11_err = daemon
            .write_if_size_not_power_of_eleven("/daemon/pow11.txt", b"again")
            .expect_err("power-of-eleven size should fail");
        assert!(matches!(pow11_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_eleven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_eleven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twelve_updates_and_rejects_non_power_of_twelve_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow12_seed = vec![b'a'; 144];
        let non_pow12_seed = vec![b'a'; 145];
        daemon
            .write("/daemon/pow12.txt", 0, &pow12_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow12.txt", 0, &non_pow12_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twelve("/daemon/pow12.txt", b"next-value")
            .expect("write_if_size_power_of_twelve should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow12.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow12_err = daemon
            .write_if_size_power_of_twelve("/daemon/nonpow12.txt", b"again")
            .expect_err("non-power-of-twelve size should fail");
        assert!(matches!(non_pow12_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twelve("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twelve("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twelve_updates_and_rejects_power_of_twelve_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow12_seed = vec![b'a'; 145];
        let pow12_seed = vec![b'a'; 144];
        daemon
            .write("/daemon/nonpow12.txt", 0, &non_pow12_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow12.txt", 0, &pow12_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twelve("/daemon/nonpow12.txt", b"next-value")
            .expect("write_if_size_not_power_of_twelve should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow12.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow12_err = daemon
            .write_if_size_not_power_of_twelve("/daemon/pow12.txt", b"again")
            .expect_err("power-of-twelve size should fail");
        assert!(matches!(pow12_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twelve("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twelve("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirteen_updates_and_rejects_non_power_of_thirteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow13_seed = vec![b'a'; 169];
        let non_pow13_seed = vec![b'a'; 170];
        daemon
            .write("/daemon/pow13.txt", 0, &pow13_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow13.txt", 0, &non_pow13_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirteen("/daemon/pow13.txt", b"next-value")
            .expect("write_if_size_power_of_thirteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow13.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow13_err = daemon
            .write_if_size_power_of_thirteen("/daemon/nonpow13.txt", b"again")
            .expect_err("non-power-of-thirteen size should fail");
        assert!(matches!(non_pow13_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirteen_updates_and_rejects_power_of_thirteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow13_seed = vec![b'a'; 170];
        let pow13_seed = vec![b'a'; 169];
        daemon
            .write("/daemon/nonpow13.txt", 0, &non_pow13_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow13.txt", 0, &pow13_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirteen("/daemon/nonpow13.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow13.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow13_err = daemon
            .write_if_size_not_power_of_thirteen("/daemon/pow13.txt", b"again")
            .expect_err("power-of-thirteen size should fail");
        assert!(matches!(pow13_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_fourteen_updates_and_rejects_non_power_of_fourteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow14_seed = vec![b'a'; 196];
        let non_pow14_seed = vec![b'a'; 197];
        daemon
            .write("/daemon/pow14.txt", 0, &pow14_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow14.txt", 0, &non_pow14_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_fourteen("/daemon/pow14.txt", b"next-value")
            .expect("write_if_size_power_of_fourteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow14.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow14_err = daemon
            .write_if_size_power_of_fourteen("/daemon/nonpow14.txt", b"again")
            .expect_err("non-power-of-fourteen size should fail");
        assert!(matches!(non_pow14_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_fourteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_fourteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_fourteen_updates_and_rejects_power_of_fourteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow14_seed = vec![b'a'; 197];
        let pow14_seed = vec![b'a'; 196];
        daemon
            .write("/daemon/nonpow14.txt", 0, &non_pow14_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow14.txt", 0, &pow14_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_fourteen("/daemon/nonpow14.txt", b"next-value")
            .expect("write_if_size_not_power_of_fourteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow14.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow14_err = daemon
            .write_if_size_not_power_of_fourteen("/daemon/pow14.txt", b"again")
            .expect_err("power-of-fourteen size should fail");
        assert!(matches!(pow14_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_fourteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_fourteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_fifteen_updates_and_rejects_non_power_of_fifteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow15_seed = vec![b'a'; 225];
        let non_pow15_seed = vec![b'a'; 226];
        daemon
            .write("/daemon/pow15.txt", 0, &pow15_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow15.txt", 0, &non_pow15_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_fifteen("/daemon/pow15.txt", b"next-value")
            .expect("write_if_size_power_of_fifteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow15.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow15_err = daemon
            .write_if_size_power_of_fifteen("/daemon/nonpow15.txt", b"again")
            .expect_err("non-power-of-fifteen size should fail");
        assert!(matches!(non_pow15_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_fifteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_fifteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_fifteen_updates_and_rejects_power_of_fifteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow15_seed = vec![b'a'; 226];
        let pow15_seed = vec![b'a'; 225];
        daemon
            .write("/daemon/nonpow15.txt", 0, &non_pow15_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow15.txt", 0, &pow15_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_fifteen("/daemon/nonpow15.txt", b"next-value")
            .expect("write_if_size_not_power_of_fifteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow15.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow15_err = daemon
            .write_if_size_not_power_of_fifteen("/daemon/pow15.txt", b"again")
            .expect_err("power-of-fifteen size should fail");
        assert!(matches!(pow15_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_fifteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_fifteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_sixteen_updates_and_rejects_non_power_of_sixteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow16_seed = vec![b'a'; 256];
        let non_pow16_seed = vec![b'a'; 257];
        daemon
            .write("/daemon/pow16.txt", 0, &pow16_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow16.txt", 0, &non_pow16_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_sixteen("/daemon/pow16.txt", b"next-value")
            .expect("write_if_size_power_of_sixteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow16.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow16_err = daemon
            .write_if_size_power_of_sixteen("/daemon/nonpow16.txt", b"again")
            .expect_err("non-power-of-sixteen size should fail");
        assert!(matches!(non_pow16_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_sixteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_sixteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_sixteen_updates_and_rejects_power_of_sixteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow16_seed = vec![b'a'; 257];
        let pow16_seed = vec![b'a'; 256];
        daemon
            .write("/daemon/nonpow16.txt", 0, &non_pow16_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow16.txt", 0, &pow16_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_sixteen("/daemon/nonpow16.txt", b"next-value")
            .expect("write_if_size_not_power_of_sixteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow16.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow16_err = daemon
            .write_if_size_not_power_of_sixteen("/daemon/pow16.txt", b"again")
            .expect_err("power-of-sixteen size should fail");
        assert!(matches!(pow16_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_sixteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_sixteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_seventeen_updates_and_rejects_non_power_of_seventeen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow17_seed = vec![b'a'; 289];
        let non_pow17_seed = vec![b'a'; 290];
        daemon
            .write("/daemon/pow17.txt", 0, &pow17_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow17.txt", 0, &non_pow17_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_seventeen("/daemon/pow17.txt", b"next-value")
            .expect("write_if_size_power_of_seventeen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow17.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow17_err = daemon
            .write_if_size_power_of_seventeen("/daemon/nonpow17.txt", b"again")
            .expect_err("non-power-of-seventeen size should fail");
        assert!(matches!(non_pow17_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_seventeen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_seventeen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_seventeen_updates_and_rejects_power_of_seventeen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow17_seed = vec![b'a'; 290];
        let pow17_seed = vec![b'a'; 289];
        daemon
            .write("/daemon/nonpow17.txt", 0, &non_pow17_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow17.txt", 0, &pow17_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_seventeen("/daemon/nonpow17.txt", b"next-value")
            .expect("write_if_size_not_power_of_seventeen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow17.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow17_err = daemon
            .write_if_size_not_power_of_seventeen("/daemon/pow17.txt", b"again")
            .expect_err("power-of-seventeen size should fail");
        assert!(matches!(pow17_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_seventeen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_seventeen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_eighteen_updates_and_rejects_non_power_of_eighteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow18_seed = vec![b'a'; 324];
        let non_pow18_seed = vec![b'a'; 325];
        daemon
            .write("/daemon/pow18.txt", 0, &pow18_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow18.txt", 0, &non_pow18_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_eighteen("/daemon/pow18.txt", b"next-value")
            .expect("write_if_size_power_of_eighteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow18.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow18_err = daemon
            .write_if_size_power_of_eighteen("/daemon/nonpow18.txt", b"again")
            .expect_err("non-power-of-eighteen size should fail");
        assert!(matches!(non_pow18_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_eighteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_eighteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_eighteen_updates_and_rejects_power_of_eighteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow18_seed = vec![b'a'; 325];
        let pow18_seed = vec![b'a'; 324];
        daemon
            .write("/daemon/nonpow18.txt", 0, &non_pow18_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow18.txt", 0, &pow18_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_eighteen("/daemon/nonpow18.txt", b"next-value")
            .expect("write_if_size_not_power_of_eighteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow18.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow18_err = daemon
            .write_if_size_not_power_of_eighteen("/daemon/pow18.txt", b"again")
            .expect_err("power-of-eighteen size should fail");
        assert!(matches!(pow18_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_eighteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_eighteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_nineteen_updates_and_rejects_non_power_of_nineteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow19_seed = vec![b'a'; 361];
        let non_pow19_seed = vec![b'a'; 362];
        daemon
            .write("/daemon/pow19.txt", 0, &pow19_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow19.txt", 0, &non_pow19_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_nineteen("/daemon/pow19.txt", b"next-value")
            .expect("write_if_size_power_of_nineteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow19.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow19_err = daemon
            .write_if_size_power_of_nineteen("/daemon/nonpow19.txt", b"again")
            .expect_err("non-power-of-nineteen size should fail");
        assert!(matches!(non_pow19_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_nineteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_nineteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_nineteen_updates_and_rejects_power_of_nineteen_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow19_seed = vec![b'a'; 362];
        let pow19_seed = vec![b'a'; 361];
        daemon
            .write("/daemon/nonpow19.txt", 0, &non_pow19_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow19.txt", 0, &pow19_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_nineteen("/daemon/nonpow19.txt", b"next-value")
            .expect("write_if_size_not_power_of_nineteen should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow19.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow19_err = daemon
            .write_if_size_not_power_of_nineteen("/daemon/pow19.txt", b"again")
            .expect_err("power-of-nineteen size should fail");
        assert!(matches!(pow19_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_nineteen("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_nineteen("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_updates_and_rejects_non_power_of_twenty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow20_seed = vec![b'a'; 400];
        let non_pow20_seed = vec![b'a'; 401];
        daemon
            .write("/daemon/pow20.txt", 0, &pow20_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow20.txt", 0, &non_pow20_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty("/daemon/pow20.txt", b"next-value")
            .expect("write_if_size_power_of_twenty should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow20.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow20_err = daemon
            .write_if_size_power_of_twenty("/daemon/nonpow20.txt", b"again")
            .expect_err("non-power-of-twenty size should fail");
        assert!(matches!(non_pow20_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_updates_and_rejects_power_of_twenty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow20_seed = vec![b'a'; 401];
        let pow20_seed = vec![b'a'; 400];
        daemon
            .write("/daemon/nonpow20.txt", 0, &non_pow20_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow20.txt", 0, &pow20_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty("/daemon/nonpow20.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow20.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow20_err = daemon
            .write_if_size_not_power_of_twenty("/daemon/pow20.txt", b"again")
            .expect_err("power-of-twenty size should fail");
        assert!(matches!(pow20_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_one_updates_and_rejects_non_power_of_twenty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow21_seed = vec![b'a'; 441];
        let non_pow21_seed = vec![b'a'; 442];
        daemon
            .write("/daemon/pow21.txt", 0, &pow21_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow21.txt", 0, &non_pow21_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_one("/daemon/pow21.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_one should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow21.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow21_err = daemon
            .write_if_size_power_of_twenty_one("/daemon/nonpow21.txt", b"again")
            .expect_err("non-power-of-twenty-one size should fail");
        assert!(matches!(non_pow21_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_one("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_one("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_one_updates_and_rejects_power_of_twenty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow21_seed = vec![b'a'; 442];
        let pow21_seed = vec![b'a'; 441];
        daemon
            .write("/daemon/nonpow21.txt", 0, &non_pow21_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow21.txt", 0, &pow21_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_one("/daemon/nonpow21.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_one should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow21.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow21_err = daemon
            .write_if_size_not_power_of_twenty_one("/daemon/pow21.txt", b"again")
            .expect_err("power-of-twenty-one size should fail");
        assert!(matches!(pow21_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_one("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_one("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_two_updates_and_rejects_non_power_of_twenty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow22_seed = vec![b'a'; 484];
        let non_pow22_seed = vec![b'a'; 485];
        daemon
            .write("/daemon/pow22.txt", 0, &pow22_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow22.txt", 0, &non_pow22_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_two("/daemon/pow22.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_two should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow22.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow22_err = daemon
            .write_if_size_power_of_twenty_two("/daemon/nonpow22.txt", b"again")
            .expect_err("non-power-of-twenty-two size should fail");
        assert!(matches!(non_pow22_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_two("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_two("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_two_updates_and_rejects_power_of_twenty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow22_seed = vec![b'a'; 485];
        let pow22_seed = vec![b'a'; 484];
        daemon
            .write("/daemon/nonpow22.txt", 0, &non_pow22_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow22.txt", 0, &pow22_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_two("/daemon/nonpow22.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_two should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow22.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow22_err = daemon
            .write_if_size_not_power_of_twenty_two("/daemon/pow22.txt", b"again")
            .expect_err("power-of-twenty-two size should fail");
        assert!(matches!(pow22_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_two("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_two("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_three_updates_and_rejects_non_power_of_twenty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow23_seed = vec![b'a'; 529];
        let non_pow23_seed = vec![b'a'; 530];
        daemon
            .write("/daemon/pow23.txt", 0, &pow23_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow23.txt", 0, &non_pow23_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_three("/daemon/pow23.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_three should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow23.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow23_err = daemon
            .write_if_size_power_of_twenty_three("/daemon/nonpow23.txt", b"again")
            .expect_err("non-power-of-twenty-three size should fail");
        assert!(matches!(non_pow23_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_three("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_three("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_three_updates_and_rejects_power_of_twenty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow23_seed = vec![b'a'; 530];
        let pow23_seed = vec![b'a'; 529];
        daemon
            .write("/daemon/nonpow23.txt", 0, &non_pow23_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow23.txt", 0, &pow23_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_three("/daemon/nonpow23.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_three should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow23.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow23_err = daemon
            .write_if_size_not_power_of_twenty_three("/daemon/pow23.txt", b"again")
            .expect_err("power-of-twenty-three size should fail");
        assert!(matches!(pow23_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_three("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_three("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_four_updates_and_rejects_non_power_of_twenty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow24_seed = vec![b'a'; 576];
        let non_pow24_seed = vec![b'a'; 577];
        daemon
            .write("/daemon/pow24.txt", 0, &pow24_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow24.txt", 0, &non_pow24_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_four("/daemon/pow24.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_four should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow24.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow24_err = daemon
            .write_if_size_power_of_twenty_four("/daemon/nonpow24.txt", b"again")
            .expect_err("non-power-of-twenty-four size should fail");
        assert!(matches!(non_pow24_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_four("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_four("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_four_updates_and_rejects_power_of_twenty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow24_seed = vec![b'a'; 577];
        let pow24_seed = vec![b'a'; 576];
        daemon
            .write("/daemon/nonpow24.txt", 0, &non_pow24_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow24.txt", 0, &pow24_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_four("/daemon/nonpow24.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_four should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow24.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow24_err = daemon
            .write_if_size_not_power_of_twenty_four("/daemon/pow24.txt", b"again")
            .expect_err("power-of-twenty-four size should fail");
        assert!(matches!(pow24_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_four("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_four("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_five_updates_and_rejects_non_power_of_twenty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow25_seed = vec![b'a'; 625];
        let non_pow25_seed = vec![b'a'; 626];
        daemon
            .write("/daemon/pow25.txt", 0, &pow25_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow25.txt", 0, &non_pow25_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_five("/daemon/pow25.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_five should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow25.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow25_err = daemon
            .write_if_size_power_of_twenty_five("/daemon/nonpow25.txt", b"again")
            .expect_err("non-power-of-twenty-five size should fail");
        assert!(matches!(non_pow25_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_five("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_five("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_five_updates_and_rejects_power_of_twenty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow25_seed = vec![b'a'; 626];
        let pow25_seed = vec![b'a'; 625];
        daemon
            .write("/daemon/nonpow25.txt", 0, &non_pow25_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow25.txt", 0, &pow25_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_five("/daemon/nonpow25.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_five should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow25.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow25_err = daemon
            .write_if_size_not_power_of_twenty_five("/daemon/pow25.txt", b"again")
            .expect_err("power-of-twenty-five size should fail");
        assert!(matches!(pow25_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_five("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_five("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_six_updates_and_rejects_non_power_of_twenty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow26_seed = vec![b'a'; 676];
        let non_pow26_seed = vec![b'a'; 677];
        daemon
            .write("/daemon/pow26.txt", 0, &pow26_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow26.txt", 0, &non_pow26_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_six("/daemon/pow26.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_six should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow26.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow26_err = daemon
            .write_if_size_power_of_twenty_six("/daemon/nonpow26.txt", b"again")
            .expect_err("non-power-of-twenty-six size should fail");
        assert!(matches!(non_pow26_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_six("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_six("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_six_updates_and_rejects_power_of_twenty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow26_seed = vec![b'a'; 677];
        let pow26_seed = vec![b'a'; 676];
        daemon
            .write("/daemon/nonpow26.txt", 0, &non_pow26_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow26.txt", 0, &pow26_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_six("/daemon/nonpow26.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_six should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow26.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow26_err = daemon
            .write_if_size_not_power_of_twenty_six("/daemon/pow26.txt", b"again")
            .expect_err("power-of-twenty-six size should fail");
        assert!(matches!(pow26_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_six("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_six("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_seven_updates_and_rejects_non_power_of_twenty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow27_seed = vec![b'a'; 729];
        let non_pow27_seed = vec![b'a'; 730];
        daemon
            .write("/daemon/pow27.txt", 0, &pow27_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow27.txt", 0, &non_pow27_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_seven("/daemon/pow27.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow27.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow27_err = daemon
            .write_if_size_power_of_twenty_seven("/daemon/nonpow27.txt", b"again")
            .expect_err("non-power-of-twenty-seven size should fail");
        assert!(matches!(non_pow27_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_seven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_seven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_seven_updates_and_rejects_power_of_twenty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow27_seed = vec![b'a'; 730];
        let pow27_seed = vec![b'a'; 729];
        daemon
            .write("/daemon/nonpow27.txt", 0, &non_pow27_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow27.txt", 0, &pow27_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_seven("/daemon/nonpow27.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow27.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow27_err = daemon
            .write_if_size_not_power_of_twenty_seven("/daemon/pow27.txt", b"again")
            .expect_err("power-of-twenty-seven size should fail");
        assert!(matches!(pow27_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_seven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_seven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_eight_updates_and_rejects_non_power_of_twenty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow28_seed = vec![b'a'; 784];
        let non_pow28_seed = vec![b'a'; 785];
        daemon
            .write("/daemon/pow28.txt", 0, &pow28_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow28.txt", 0, &non_pow28_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_eight("/daemon/pow28.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow28.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow28_err = daemon
            .write_if_size_power_of_twenty_eight("/daemon/nonpow28.txt", b"again")
            .expect_err("non-power-of-twenty-eight size should fail");
        assert!(matches!(non_pow28_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_eight("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_eight("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_eight_updates_and_rejects_power_of_twenty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow28_seed = vec![b'a'; 785];
        let pow28_seed = vec![b'a'; 784];
        daemon
            .write("/daemon/nonpow28.txt", 0, &non_pow28_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow28.txt", 0, &pow28_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_eight("/daemon/nonpow28.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow28.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow28_err = daemon
            .write_if_size_not_power_of_twenty_eight("/daemon/pow28.txt", b"again")
            .expect_err("power-of-twenty-eight size should fail");
        assert!(matches!(pow28_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_eight("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_eight("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_twenty_nine_updates_and_rejects_non_power_of_twenty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow29_seed = vec![b'a'; 841];
        let non_pow29_seed = vec![b'a'; 842];
        daemon
            .write("/daemon/pow29.txt", 0, &pow29_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow29.txt", 0, &non_pow29_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_twenty_nine("/daemon/pow29.txt", b"next-value")
            .expect("write_if_size_power_of_twenty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow29.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow29_err = daemon
            .write_if_size_power_of_twenty_nine("/daemon/nonpow29.txt", b"again")
            .expect_err("non-power-of-twenty-nine size should fail");
        assert!(matches!(non_pow29_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_twenty_nine("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_twenty_nine("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_twenty_nine_updates_and_rejects_power_of_twenty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow29_seed = vec![b'a'; 842];
        let pow29_seed = vec![b'a'; 841];
        daemon
            .write("/daemon/nonpow29.txt", 0, &non_pow29_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow29.txt", 0, &pow29_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_twenty_nine("/daemon/nonpow29.txt", b"next-value")
            .expect("write_if_size_not_power_of_twenty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow29.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow29_err = daemon
            .write_if_size_not_power_of_twenty_nine("/daemon/pow29.txt", b"again")
            .expect_err("power-of-twenty-nine size should fail");
        assert!(matches!(pow29_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_twenty_nine("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_twenty_nine("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_updates_and_rejects_non_power_of_thirty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow30_seed = vec![b'a'; 900];
        let non_pow30_seed = vec![b'a'; 901];
        daemon
            .write("/daemon/pow30.txt", 0, &pow30_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow30.txt", 0, &non_pow30_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty("/daemon/pow30.txt", b"next-value")
            .expect("write_if_size_power_of_thirty should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow30.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow30_err = daemon
            .write_if_size_power_of_thirty("/daemon/nonpow30.txt", b"again")
            .expect_err("non-power-of-thirty size should fail");
        assert!(matches!(non_pow30_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_updates_and_rejects_power_of_thirty_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow30_seed = vec![b'a'; 901];
        let pow30_seed = vec![b'a'; 900];
        daemon
            .write("/daemon/nonpow30.txt", 0, &non_pow30_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow30.txt", 0, &pow30_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty("/daemon/nonpow30.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow30.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow30_err = daemon
            .write_if_size_not_power_of_thirty("/daemon/pow30.txt", b"again")
            .expect_err("power-of-thirty size should fail");
        assert!(matches!(pow30_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_one_updates_and_rejects_non_power_of_thirty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow31_seed = vec![b'a'; 961];
        let non_pow31_seed = vec![b'a'; 962];
        daemon
            .write("/daemon/pow31.txt", 0, &pow31_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow31.txt", 0, &non_pow31_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_one("/daemon/pow31.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_one should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow31.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow31_err = daemon
            .write_if_size_power_of_thirty_one("/daemon/nonpow31.txt", b"again")
            .expect_err("non-power-of-thirty-one size should fail");
        assert!(matches!(non_pow31_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_one("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_one("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_one_updates_and_rejects_power_of_thirty_one_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow31_seed = vec![b'a'; 962];
        let pow31_seed = vec![b'a'; 961];
        daemon
            .write("/daemon/nonpow31.txt", 0, &non_pow31_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow31.txt", 0, &pow31_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_one("/daemon/nonpow31.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_one should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow31.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow31_err = daemon
            .write_if_size_not_power_of_thirty_one("/daemon/pow31.txt", b"again")
            .expect_err("power-of-thirty-one size should fail");
        assert!(matches!(pow31_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_one("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_one("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_two_updates_and_rejects_non_power_of_thirty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow32_seed = vec![b'a'; 1024];
        let non_pow32_seed = vec![b'a'; 1025];
        daemon
            .write("/daemon/pow32.txt", 0, &pow32_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow32.txt", 0, &non_pow32_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_two("/daemon/pow32.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_two should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow32.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow32_err = daemon
            .write_if_size_power_of_thirty_two("/daemon/nonpow32.txt", b"again")
            .expect_err("non-power-of-thirty-two size should fail");
        assert!(matches!(non_pow32_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_two("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_two("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_two_updates_and_rejects_power_of_thirty_two_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow32_seed = vec![b'a'; 1025];
        let pow32_seed = vec![b'a'; 1024];
        daemon
            .write("/daemon/nonpow32.txt", 0, &non_pow32_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow32.txt", 0, &pow32_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_two("/daemon/nonpow32.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_two should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow32.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow32_err = daemon
            .write_if_size_not_power_of_thirty_two("/daemon/pow32.txt", b"again")
            .expect_err("power-of-thirty-two size should fail");
        assert!(matches!(pow32_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_two("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_two("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_three_updates_and_rejects_non_power_of_thirty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow33_seed = vec![b'a'; 1089];
        let non_pow33_seed = vec![b'a'; 1090];
        daemon
            .write("/daemon/pow33.txt", 0, &pow33_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow33.txt", 0, &non_pow33_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_three("/daemon/pow33.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_three should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow33.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow33_err = daemon
            .write_if_size_power_of_thirty_three("/daemon/nonpow33.txt", b"again")
            .expect_err("non-power-of-thirty-three size should fail");
        assert!(matches!(non_pow33_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_three("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_three("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_three_updates_and_rejects_power_of_thirty_three_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow33_seed = vec![b'a'; 1090];
        let pow33_seed = vec![b'a'; 1089];
        daemon
            .write("/daemon/nonpow33.txt", 0, &non_pow33_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow33.txt", 0, &pow33_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_three("/daemon/nonpow33.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_three should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow33.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow33_err = daemon
            .write_if_size_not_power_of_thirty_three("/daemon/pow33.txt", b"again")
            .expect_err("power-of-thirty-three size should fail");
        assert!(matches!(pow33_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_three("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_three("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_four_updates_and_rejects_non_power_of_thirty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow34_seed = vec![b'a'; 1156];
        let non_pow34_seed = vec![b'a'; 1157];
        daemon
            .write("/daemon/pow34.txt", 0, &pow34_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow34.txt", 0, &non_pow34_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_four("/daemon/pow34.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_four should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow34.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow34_err = daemon
            .write_if_size_power_of_thirty_four("/daemon/nonpow34.txt", b"again")
            .expect_err("non-power-of-thirty-four size should fail");
        assert!(matches!(non_pow34_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_four("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_four("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_four_updates_and_rejects_power_of_thirty_four_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow34_seed = vec![b'a'; 1157];
        let pow34_seed = vec![b'a'; 1156];
        daemon
            .write("/daemon/nonpow34.txt", 0, &non_pow34_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow34.txt", 0, &pow34_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_four("/daemon/nonpow34.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_four should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow34.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow34_err = daemon
            .write_if_size_not_power_of_thirty_four("/daemon/pow34.txt", b"again")
            .expect_err("power-of-thirty-four size should fail");
        assert!(matches!(pow34_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_four("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_four("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_five_updates_and_rejects_non_power_of_thirty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow35_seed = vec![b'a'; 1225];
        let non_pow35_seed = vec![b'a'; 1226];
        daemon
            .write("/daemon/pow35.txt", 0, &pow35_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow35.txt", 0, &non_pow35_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_five("/daemon/pow35.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_five should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow35.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow35_err = daemon
            .write_if_size_power_of_thirty_five("/daemon/nonpow35.txt", b"again")
            .expect_err("non-power-of-thirty-five size should fail");
        assert!(matches!(non_pow35_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_five("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_five("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_five_updates_and_rejects_power_of_thirty_five_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow35_seed = vec![b'a'; 1226];
        let pow35_seed = vec![b'a'; 1225];
        daemon
            .write("/daemon/nonpow35.txt", 0, &non_pow35_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow35.txt", 0, &pow35_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_five("/daemon/nonpow35.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_five should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow35.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow35_err = daemon
            .write_if_size_not_power_of_thirty_five("/daemon/pow35.txt", b"again")
            .expect_err("power-of-thirty-five size should fail");
        assert!(matches!(pow35_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_five("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_five("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_six_updates_and_rejects_non_power_of_thirty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow36_seed = vec![b'a'; 1296];
        let non_pow36_seed = vec![b'a'; 1297];
        daemon
            .write("/daemon/pow36.txt", 0, &pow36_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow36.txt", 0, &non_pow36_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_six("/daemon/pow36.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_six should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow36.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow36_err = daemon
            .write_if_size_power_of_thirty_six("/daemon/nonpow36.txt", b"again")
            .expect_err("non-power-of-thirty-six size should fail");
        assert!(matches!(non_pow36_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_six("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_six("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_six_updates_and_rejects_power_of_thirty_six_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow36_seed = vec![b'a'; 1297];
        let pow36_seed = vec![b'a'; 1296];
        daemon
            .write("/daemon/nonpow36.txt", 0, &non_pow36_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow36.txt", 0, &pow36_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_six("/daemon/nonpow36.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_six should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow36.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow36_err = daemon
            .write_if_size_not_power_of_thirty_six("/daemon/pow36.txt", b"again")
            .expect_err("power-of-thirty-six size should fail");
        assert!(matches!(pow36_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_six("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_six("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_seven_updates_and_rejects_non_power_of_thirty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow37_seed = vec![b'a'; 1369];
        let non_pow37_seed = vec![b'a'; 1370];
        daemon
            .write("/daemon/pow37.txt", 0, &pow37_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow37.txt", 0, &non_pow37_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_seven("/daemon/pow37.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow37.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow37_err = daemon
            .write_if_size_power_of_thirty_seven("/daemon/nonpow37.txt", b"again")
            .expect_err("non-power-of-thirty-seven size should fail");
        assert!(matches!(non_pow37_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_seven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_seven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_seven_updates_and_rejects_power_of_thirty_seven_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow37_seed = vec![b'a'; 1370];
        let pow37_seed = vec![b'a'; 1369];
        daemon
            .write("/daemon/nonpow37.txt", 0, &non_pow37_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow37.txt", 0, &pow37_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_seven("/daemon/nonpow37.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_seven should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow37.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow37_err = daemon
            .write_if_size_not_power_of_thirty_seven("/daemon/pow37.txt", b"again")
            .expect_err("power-of-thirty-seven size should fail");
        assert!(matches!(pow37_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_seven("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_seven("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_eight_updates_and_rejects_non_power_of_thirty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow38_seed = vec![b'a'; 1444];
        let non_pow38_seed = vec![b'a'; 1445];
        daemon
            .write("/daemon/pow38.txt", 0, &pow38_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow38.txt", 0, &non_pow38_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_eight("/daemon/pow38.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow38.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow38_err = daemon
            .write_if_size_power_of_thirty_eight("/daemon/nonpow38.txt", b"again")
            .expect_err("non-power-of-thirty-eight size should fail");
        assert!(matches!(non_pow38_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_eight("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_eight("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_eight_updates_and_rejects_power_of_thirty_eight_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow38_seed = vec![b'a'; 1445];
        let pow38_seed = vec![b'a'; 1444];
        daemon
            .write("/daemon/nonpow38.txt", 0, &non_pow38_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow38.txt", 0, &pow38_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_eight("/daemon/nonpow38.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_eight should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow38.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow38_err = daemon
            .write_if_size_not_power_of_thirty_eight("/daemon/pow38.txt", b"again")
            .expect_err("power-of-thirty-eight size should fail");
        assert!(matches!(pow38_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_eight("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_eight("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_power_of_thirty_nine_updates_and_rejects_non_power_of_thirty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let pow39_seed = vec![b'a'; 1521];
        let non_pow39_seed = vec![b'a'; 1522];
        daemon
            .write("/daemon/pow39.txt", 0, &pow39_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/nonpow39.txt", 0, &non_pow39_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_power_of_thirty_nine("/daemon/pow39.txt", b"next-value")
            .expect("write_if_size_power_of_thirty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = daemon.read_all("/daemon/pow39.txt").expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let non_pow39_err = daemon
            .write_if_size_power_of_thirty_nine("/daemon/nonpow39.txt", b"again")
            .expect_err("non-power-of-thirty-nine size should fail");
        assert!(matches!(non_pow39_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_power_of_thirty_nine("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_power_of_thirty_nine("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_write_if_size_not_power_of_thirty_nine_updates_and_rejects_power_of_thirty_nine_missing_or_directory(
    ) {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let non_pow39_seed = vec![b'a'; 1522];
        let pow39_seed = vec![b'a'; 1521];
        daemon
            .write("/daemon/nonpow39.txt", 0, &non_pow39_seed[..])
            .expect("write should pass");
        daemon
            .write("/daemon/pow39.txt", 0, &pow39_seed[..])
            .expect("write should pass");

        let tx = daemon
            .write_if_size_not_power_of_thirty_nine("/daemon/nonpow39.txt", b"next-value")
            .expect("write_if_size_not_power_of_thirty_nine should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read_all("/daemon/nonpow39.txt")
            .expect("read should pass");
        assert_eq!(bytes, b"next-value");

        let pow39_err = daemon
            .write_if_size_not_power_of_thirty_nine("/daemon/pow39.txt", b"again")
            .expect_err("power-of-thirty-nine size should fail");
        assert!(matches!(pow39_err, FuseError::Conflict));

        let missing_err = daemon
            .write_if_size_not_power_of_thirty_nine("/daemon/missing.txt", b"again")
            .expect_err("missing file should fail");
        assert!(matches!(missing_err, FuseError::NotFound));

        let directory_err = daemon
            .write_if_size_not_power_of_thirty_nine("/daemon", b"again")
            .expect_err("directory path should fail");
        assert!(matches!(directory_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_ensure_file_is_idempotent_and_conflicts_on_directory() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        assert!(daemon
            .ensure_file("/daemon/ensured.txt")
            .expect("ensure_file should create"));
        daemon
            .write("/daemon/ensured.txt", 1, b"keep-me")
            .expect("write should pass");
        assert!(!daemon
            .ensure_file("/daemon/ensured.txt")
            .expect("ensure_file should be idempotent"));
        let bytes = daemon
            .read_all("/daemon/ensured.txt")
            .expect("read_all should pass");
        assert_eq!(bytes, b"keep-me");
        let err = daemon
            .ensure_file("/daemon")
            .expect_err("ensure_file on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_read_all_rejects_directory_path() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let err = daemon
            .read_all("/daemon")
            .expect_err("read_all on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_mkdir_p_creates_nested_tree() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        let created = daemon
            .mkdir_p("/daemon/sub/tree")
            .expect("mkdir_p should pass");
        assert_eq!(created, 3);

        let root = daemon.readdir("/").expect("root list should pass");
        assert_eq!(root, vec!["daemon".to_string()]);
        let daemon_children = daemon.readdir("/daemon").expect("daemon list should pass");
        assert_eq!(daemon_children, vec!["sub".to_string()]);
        let sub_children = daemon
            .readdir("/daemon/sub")
            .expect("sub list should pass");
        assert_eq!(sub_children, vec!["tree".to_string()]);

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_stat_reports_node_kind() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/file.txt", 0, b"hello")
            .expect("write should pass");

        assert_eq!(
            daemon.stat("/").expect("stat should pass"),
            FuseNodeKind::Directory
        );
        assert_eq!(
            daemon.stat("/daemon").expect("stat should pass"),
            FuseNodeKind::Directory
        );
        assert_eq!(
            daemon
                .stat("/daemon/file.txt")
                .expect("stat should pass"),
            FuseNodeKind::File
        );
        let err = daemon
            .stat("/missing")
            .expect_err("stat missing should fail");
        assert!(matches!(err, FuseError::NotFound));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_exists_reports_file_dir_and_missing() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/file.txt", 0, b"hello")
            .expect("write should pass");

        assert!(daemon.exists("/").expect("exists should pass"));
        assert!(daemon.exists("/daemon").expect("exists should pass"));
        assert!(daemon.exists("/daemon/file.txt").expect("exists should pass"));
        assert!(!daemon.exists("/missing").expect("exists should pass"));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_readdir_with_kinds_reports_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon.mkdir("/daemon/sub").expect("mkdir should pass");
        daemon
            .write("/daemon/file.txt", 0, b"hello")
            .expect("write should pass");

        let entries = daemon
            .readdir_with_kinds("/daemon")
            .expect("readdir_with_kinds should pass");
        assert_eq!(
            entries,
            vec![
                FuseDirEntry {
                    name: "file.txt".to_string(),
                    kind: FuseNodeKind::File,
                },
                FuseDirEntry {
                    name: "sub".to_string(),
                    kind: FuseNodeKind::Directory,
                },
            ]
        );

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_copy_file_copies_bytes() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/source.txt", 0, b"copy-me")
            .expect("write should pass");
        let tx = daemon
            .copy_file("/daemon/source.txt", "/daemon/dest.txt", 0)
            .expect("copy should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read("/daemon/dest.txt", 0, usize::MAX)
            .expect("dest read should pass");
        assert_eq!(bytes, b"copy-me");

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_touch_file_creates_and_updates_empty_file() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let tx_v1 = daemon
            .touch_file("/daemon/empty.txt", 0)
            .expect("touch create should pass");
        assert!(!tx_v1.is_empty());
        let bytes = daemon
            .read("/daemon/empty.txt", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(bytes.len(), 0);
        let tx_v2 = daemon
            .touch_file("/daemon/empty.txt", 1)
            .expect("touch update should pass");
        assert!(!tx_v2.is_empty());

        let err = daemon
            .touch_file("/daemon", 0)
            .expect_err("touch on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_truncate_file_shrinks_and_extends_with_zero_fill() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/data.bin", 0, b"abcdef")
            .expect("seed write should pass");

        let tx_v1 = daemon
            .truncate_file("/daemon/data.bin", 3, 1)
            .expect("truncate shrink should pass");
        assert!(!tx_v1.is_empty());
        let shrunk = daemon
            .read("/daemon/data.bin", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(shrunk, b"abc");

        let tx_v2 = daemon
            .truncate_file("/daemon/data.bin", 6, 2)
            .expect("truncate extend should pass");
        assert!(!tx_v2.is_empty());
        let extended = daemon
            .read("/daemon/data.bin", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(extended, b"abc\0\0\0");

        let err = daemon
            .truncate_file("/daemon", 1, 0)
            .expect_err("truncate on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_append_file_appends_bytes() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/log.txt", 0, b"line1")
            .expect("seed write should pass");
        let tx = daemon
            .append_file("/daemon/log.txt", 1, b"-line2")
            .expect("append should pass");
        assert!(!tx.is_empty());
        let bytes = daemon
            .read("/daemon/log.txt", 0, usize::MAX)
            .expect("read should pass");
        assert_eq!(bytes, b"line1-line2");

        let err = daemon
            .append_file("/daemon", 0, b"x")
            .expect_err("append on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_overwrite_range_patches_and_grows() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = daemon
            .overwrite_range("/daemon/data.bin", 2, 1, b"XYZ")
            .expect("overwrite in-range should pass");
        assert!(!tx_v1.is_empty());
        let patched = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(patched, b"abXYZf");

        let tx_v2 = daemon
            .overwrite_range("/daemon/data.bin", 8, 2, b"Q")
            .expect("overwrite grow should pass");
        assert!(!tx_v2.is_empty());
        let grown = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(grown, b"abXYZf\0\0Q");

        let err = daemon
            .overwrite_range("/daemon", 0, 0, b"x")
            .expect_err("overwrite on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_insert_range_inserts_and_grows() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = daemon
            .insert_range("/daemon/data.bin", 3, 1, b"XYZ")
            .expect("insert in-range should pass");
        assert!(!tx_v1.is_empty());
        let inserted = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(inserted, b"abcXYZdef");

        let tx_v2 = daemon
            .insert_range("/daemon/data.bin", 12, 2, b"Q")
            .expect("insert grow should pass");
        assert!(!tx_v2.is_empty());
        let grown = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(grown, b"abcXYZdef\0\0\0Q");

        let err = daemon
            .insert_range("/daemon", 0, 0, b"x")
            .expect_err("insert on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_delete_range_removes_span_and_handles_past_eof() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = daemon
            .delete_range("/daemon/data.bin", 2, 2, 1)
            .expect("delete span should pass");
        assert!(!tx_v1.is_empty());
        let deleted = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(deleted, b"abef");

        let tx_v2 = daemon
            .delete_range("/daemon/data.bin", 20, 4, 2)
            .expect("past-eof delete should pass");
        assert!(!tx_v2.is_empty());
        let after = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(after, b"abef");

        let err = daemon
            .delete_range("/daemon", 0, 1, 0)
            .expect_err("delete on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_replace_range_replaces_window_and_grows_with_gap_fill() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/data.bin", 0, b"abcdef")
            .expect("seed write should pass");
        let tx_v1 = daemon
            .replace_range("/daemon/data.bin", 2, 2, 1, b"XYZ")
            .expect("replace window should pass");
        assert!(!tx_v1.is_empty());
        let replaced = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(replaced, b"abXYZef");

        let tx_v2 = daemon
            .replace_range("/daemon/data.bin", 12, 1, 2, b"Q")
            .expect("replace grow should pass");
        assert!(!tx_v2.is_empty());
        let grown = daemon
            .read_all("/daemon/data.bin")
            .expect("read_all should pass");
        assert_eq!(grown, b"abXYZef\0\0\0\0\0Q");

        let err = daemon
            .replace_range("/daemon", 0, 1, 0, b"x")
            .expect_err("replace on directory should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_file_size_reports_file_bytes() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon
            .write("/daemon/a.txt", 0, b"hello")
            .expect("write should pass");
        let size = daemon
            .file_size("/daemon/a.txt")
            .expect("file size should pass");
        assert_eq!(size, 5);
        let err = daemon
            .file_size("/daemon")
            .expect_err("directory size should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_file_hash_reports_blake3_checksum() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        let payload = b"checksum-me";
        daemon
            .write("/daemon/a.txt", 0, payload)
            .expect("write should pass");
        let hash_v1 = daemon
            .file_hash("/daemon/a.txt")
            .expect("file hash should pass");
        assert_eq!(hash_v1.len(), 64);
        assert!(hash_v1.chars().all(|c| c.is_ascii_hexdigit()));
        daemon
            .write("/daemon/a.txt", 1, b"checksum-me-v2")
            .expect("rewrite should pass");
        let hash_v2 = daemon
            .file_hash("/daemon/a.txt")
            .expect("file hash should pass");
        assert_eq!(hash_v2.len(), 64);
        assert_ne!(hash_v1, hash_v2);
        let err = daemon
            .file_hash("/daemon")
            .expect_err("directory hash should fail");
        assert!(matches!(err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_walk_dir_reports_recursive_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir_p("/daemon/sub").expect("mkdir_p should pass");
        daemon
            .write("/daemon/file.txt", 0, b"hello")
            .expect("write should pass");
        daemon
            .write("/daemon/sub/nested.txt", 0, b"world")
            .expect("write should pass");

        let entries = daemon.walk_dir("/daemon").expect("walk_dir should pass");
        assert_eq!(
            entries,
            vec![
                FusePathEntry {
                    path: "/daemon/file.txt".to_string(),
                    kind: FuseNodeKind::File,
                },
                FusePathEntry {
                    path: "/daemon/sub".to_string(),
                    kind: FuseNodeKind::Directory,
                },
                FusePathEntry {
                    path: "/daemon/sub/nested.txt".to_string(),
                    kind: FuseNodeKind::File,
                },
            ]
        );

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_tree_summary_counts_recursive_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir_p("/daemon/sub").expect("mkdir_p should pass");
        daemon
            .write("/daemon/file.txt", 0, b"hello")
            .expect("write should pass");
        daemon
            .write("/daemon/sub/nested.txt", 0, b"world")
            .expect("write should pass");

        let summary = daemon
            .tree_summary("/daemon")
            .expect("tree summary should pass");
        assert_eq!(
            summary,
            FuseTreeSummary {
                files: 2,
                directories: 1,
            }
        );

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_tree_bytes_sums_recursive_entries() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir_p("/daemon/sub").expect("mkdir_p should pass");
        daemon
            .write("/daemon/a.txt", 0, b"hello")
            .expect("write should pass");
        daemon
            .write("/daemon/sub/b.txt", 0, b"world!")
            .expect("write should pass");

        let bytes = daemon
            .tree_bytes("/daemon")
            .expect("tree bytes should pass");
        assert_eq!(bytes, 11);

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_run_background_once_returns_empty_without_gc_worker() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        let reports = daemon
            .run_background_once()
            .expect("run_background_once should pass");
        assert!(reports.is_empty());

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_gc_scan_once_returns_empty_without_gc_components() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        let reports = daemon.gc_scan_once().expect("gc_scan_once should pass");
        assert!(reports.is_empty());

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_enqueue_gc_scan_reports_false_without_trigger() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        let enqueued = daemon
            .enqueue_gc_scan()
            .expect("enqueue_gc_scan should pass");
        assert!(!enqueued, "enqueue_gc_scan should report no trigger");

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_rmtree_removes_nested_tree() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon.mkdir("/daemon/sub").expect("mkdir should pass");
        daemon
            .write("/daemon/file.txt", 0, b"hello")
            .expect("write should pass");
        daemon
            .write("/daemon/sub/nested.txt", 0, b"nested")
            .expect("write should pass");

        let removed = daemon.rmtree("/daemon").expect("rmtree should pass");
        assert_eq!(removed, 4);
        let root = daemon.readdir("/").expect("root list should pass");
        assert!(root.is_empty());
        let err = daemon
            .read("/daemon/sub/nested.txt", 0, 1)
            .expect_err("removed file read should fail");
        assert!(matches!(err, FuseError::NotFound));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_remove_path_removes_file_and_directories() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon.mkdir("/daemon").expect("mkdir should pass");
        daemon.mkdir("/daemon/sub").expect("mkdir should pass");
        daemon
            .write("/daemon/file.txt", 0, b"hello")
            .expect("write should pass");
        daemon
            .write("/daemon/sub/nested.txt", 0, b"world")
            .expect("write should pass");

        assert_eq!(
            daemon
                .remove_path("/daemon/file.txt")
                .expect("remove file should pass"),
            1
        );
        assert_eq!(
            daemon
                .remove_path("/daemon/sub")
                .expect("remove subtree should pass"),
            2
        );
        assert_eq!(
            daemon
                .remove_path("/daemon")
                .expect("remove empty dir should pass"),
            1
        );
        assert!(!daemon.exists("/daemon").expect("exists should pass"));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_rmdir_and_rmtree_on_file_path_map_to_conflict() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        daemon.startup_recover().expect("recovery should pass");
        daemon
            .write("/docs", 0, b"hello")
            .expect("write should pass");
        let rmdir_err = daemon
            .rmdir("/docs")
            .expect_err("rmdir on file path should fail");
        assert!(matches!(rmdir_err, FuseError::Conflict));
        let rmtree_err = daemon
            .rmtree("/docs")
            .expect_err("rmtree on file path should fail");
        assert!(matches!(rmtree_err, FuseError::Conflict));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_health_probe_before_and_after_recovery() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        assert!(daemon.is_worker_alive(), "daemon worker should be alive");
        daemon
            .health()
            .expect("health probe should pass before recovery");
        daemon
            .startup_recover()
            .expect("startup recovery should pass");
        daemon
            .health()
            .expect("health probe should pass after recovery");
        daemon
            .health_with_delay(Duration::from_millis(0))
            .expect("delayed health probe should pass");
        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_preserves_error_mapping() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon();

        let err = daemon
            .read("/daemon.txt", 0, 1)
            .expect_err("read should fail before recovery");
        assert!(matches!(err, FuseError::Unavailable));

        daemon.startup_recover().expect("recovery should pass");
        let missing = daemon
            .read("/missing.txt", 0, 1)
            .expect_err("missing read should fail");
        assert!(matches!(missing, FuseError::NotFound));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_request_timeout_returns_timeout() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let mut daemon = FuseApi::new(core).spawn_daemon();
        daemon.startup_recover().expect("recovery should pass");
        daemon.set_request_timeout(Some(Duration::from_millis(10)));
        let err = daemon
            .health_with_delay(Duration::from_millis(80))
            .expect_err("delayed health should time out and map to timeout");
        assert!(matches!(err, FuseError::Timeout));

        daemon.shutdown().expect("shutdown should pass");
    }

    #[test]
    fn fuse_daemon_rejects_new_requests_when_pending_limit_is_saturated() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let wal = WalLog::open(temp.path().join("wal.log")).expect("wal should open");
        let chunks = Arc::new(FsChunkStore::new(temp.path().join("chunks")).expect("chunks init"));
        let metadata = Arc::new(InMemoryMetadataHook::new());
        let cache = TwoTierChunkCache::new(8, 32);
        let pipeline = WritePipeline::new(wal, chunks, metadata, 4).expect("pipeline init");
        let core = FileSystemCore::new(pipeline, cache);
        let daemon = FuseApi::new(core).spawn_daemon_with_limits(1);
        daemon.startup_recover().expect("recovery should pass");

        thread::scope(|scope| {
            let daemon_ref = &daemon;
            let handle = scope.spawn(move || daemon_ref.health_with_delay(Duration::from_millis(80)));

            for _ in 0..20 {
                if daemon.pending_requests() >= 1 {
                    break;
                }
                thread::sleep(Duration::from_millis(2));
            }

            let saturated = daemon
                .health()
                .expect_err("health should fail while pending cap is reached");
            assert!(matches!(saturated, FuseError::Unavailable));

            let held_result = handle.join().expect("held request thread should not panic");
            held_result.expect("held request should finish successfully");
        });

        assert_eq!(daemon.pending_requests(), 0);
        daemon.health().expect("health should pass after pending load drains");
        daemon.shutdown().expect("shutdown should pass");
    }
}
