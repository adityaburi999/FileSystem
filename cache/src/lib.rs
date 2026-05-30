use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache internal lock poisoned")]
    Poisoned,

    #[error("cache io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("cache serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    pub file_path: String,
    pub version: u64,
    pub chunk_id: String,
}

pub trait ChunkCache: Send + Sync {
    fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>, CacheError>;
    fn put(&self, key: CacheKey, value: Vec<u8>) -> Result<(), CacheError>;
    fn invalidate_path(&self, file_path: &str) -> Result<(), CacheError>;
    fn invalidate_all(&self) -> Result<(), CacheError>;
}

pub struct TwoTierChunkCache {
    l1: Mutex<LruTier>,
    l2: Mutex<LruTier>,
}

impl TwoTierChunkCache {
    pub fn new(l1_capacity: usize, l2_capacity: usize) -> Self {
        Self {
            l1: Mutex::new(LruTier::new(l1_capacity.max(1))),
            l2: Mutex::new(LruTier::new(l2_capacity.max(1))),
        }
    }
}

impl ChunkCache for TwoTierChunkCache {
    fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>, CacheError> {
        if let Some(bytes) = self
            .l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .get(key)
            .cloned()
        {
            return Ok(Some(bytes));
        }

        let l2_bytes = self
            .l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .get(key)
            .cloned();

        if let Some(ref bytes) = l2_bytes {
            // Promote into L1 after L2 hit.
            self.l1
                .lock()
                .map_err(|_| CacheError::Poisoned)?
                .put(key.clone(), bytes.clone());
        }

        Ok(l2_bytes)
    }

    fn put(&self, key: CacheKey, value: Vec<u8>) -> Result<(), CacheError> {
        self.l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .put(key.clone(), value.clone());
        self.l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .put(key, value);
        Ok(())
    }

    fn invalidate_path(&self, file_path: &str) -> Result<(), CacheError> {
        self.l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_path(file_path);
        self.l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_path(file_path);
        Ok(())
    }

    fn invalidate_all(&self) -> Result<(), CacheError> {
        self.l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_all();
        self.l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_all();
        Ok(())
    }
}

pub struct PersistentTwoTierChunkCache {
    l1: Mutex<LruTier>,
    l2: Mutex<DiskLruTier>,
}

impl PersistentTwoTierChunkCache {
    pub fn open<P: AsRef<Path>>(
        root: P,
        l1_capacity: usize,
        l2_capacity: usize,
    ) -> Result<Self, CacheError> {
        let l2 = DiskLruTier::open(root.as_ref().to_path_buf(), l2_capacity.max(1))?;
        Ok(Self {
            l1: Mutex::new(LruTier::new(l1_capacity.max(1))),
            l2: Mutex::new(l2),
        })
    }
}

impl ChunkCache for PersistentTwoTierChunkCache {
    fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>, CacheError> {
        if let Some(bytes) = self
            .l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .get(key)
            .cloned()
        {
            return Ok(Some(bytes));
        }

        let l2_bytes = self
            .l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .get(key)?
            .cloned();

        if let Some(ref bytes) = l2_bytes {
            self.l1
                .lock()
                .map_err(|_| CacheError::Poisoned)?
                .put(key.clone(), bytes.clone());
        }

        Ok(l2_bytes)
    }

    fn put(&self, key: CacheKey, value: Vec<u8>) -> Result<(), CacheError> {
        self.l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .put(key.clone(), value.clone())?;
        self.l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .put(key, value);
        Ok(())
    }

    fn invalidate_path(&self, file_path: &str) -> Result<(), CacheError> {
        self.l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_path(file_path);
        self.l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_path(file_path)?;
        Ok(())
    }

    fn invalidate_all(&self) -> Result<(), CacheError> {
        self.l1
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_all();
        self.l2
            .lock()
            .map_err(|_| CacheError::Poisoned)?
            .invalidate_all()?;
        Ok(())
    }
}

struct LruTier {
    capacity: usize,
    data: HashMap<CacheKey, Vec<u8>>,
    order: VecDeque<CacheKey>,
}

impl LruTier {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            data: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<&Vec<u8>> {
        if !self.data.contains_key(key) {
            return None;
        }
        self.touch(key.clone());
        self.data.get(key)
    }

    fn put(&mut self, key: CacheKey, value: Vec<u8>) {
        if self.data.contains_key(&key) {
            self.data.insert(key.clone(), value);
            self.touch(key);
            return;
        }

        self.data.insert(key.clone(), value);
        self.order.push_back(key);
        self.evict_if_needed();
    }

    fn invalidate_path(&mut self, file_path: &str) {
        self.data.retain(|k, _| k.file_path != file_path);
        self.order.retain(|k| k.file_path != file_path);
    }

    fn invalidate_all(&mut self) {
        self.data.clear();
        self.order.clear();
    }

    fn touch(&mut self, key: CacheKey) {
        self.order.retain(|k| k != &key);
        self.order.push_back(key);
    }

    fn evict_if_needed(&mut self) {
        while self.data.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.data.remove(&oldest);
            } else {
                break;
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DiskEntry {
    key: CacheKey,
    value: Vec<u8>,
}

struct DiskLruTier {
    capacity: usize,
    root: PathBuf,
    data: HashMap<CacheKey, Vec<u8>>,
    order: VecDeque<CacheKey>,
}

impl DiskLruTier {
    fn open(root: PathBuf, capacity: usize) -> Result<Self, CacheError> {
        fs::create_dir_all(&root)?;
        let mut tier = Self {
            capacity,
            root,
            data: HashMap::new(),
            order: VecDeque::new(),
        };
        tier.load_existing()?;
        tier.evict_if_needed()?;
        Ok(tier)
    }

    fn get(&mut self, key: &CacheKey) -> Result<Option<&Vec<u8>>, CacheError> {
        if !self.data.contains_key(key) {
            return Ok(None);
        }
        self.touch(key.clone());
        Ok(self.data.get(key))
    }

    fn put(&mut self, key: CacheKey, value: Vec<u8>) -> Result<(), CacheError> {
        self.persist_entry(&key, &value)?;
        if self.data.contains_key(&key) {
            self.data.insert(key.clone(), value);
            self.touch(key);
            return Ok(());
        }

        self.data.insert(key.clone(), value);
        self.order.push_back(key);
        self.evict_if_needed()?;
        Ok(())
    }

    fn invalidate_path(&mut self, file_path: &str) -> Result<(), CacheError> {
        let keys: Vec<CacheKey> = self
            .data
            .keys()
            .filter(|k| k.file_path == file_path)
            .cloned()
            .collect();
        for key in keys {
            self.data.remove(&key);
            self.order.retain(|k| k != &key);
            self.delete_entry_file(&key)?;
        }
        Ok(())
    }

    fn invalidate_all(&mut self) -> Result<(), CacheError> {
        self.data.clear();
        self.order.clear();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn load_existing(&mut self) -> Result<(), CacheError> {
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(_) => {
                    let _ = fs::remove_file(&path);
                    continue;
                }
            };
            let parsed: DiskEntry = match serde_json::from_slice(&bytes) {
                Ok(parsed) => parsed,
                Err(_) => {
                    let _ = fs::remove_file(&path);
                    continue;
                }
            };
            if self.data.contains_key(&parsed.key) {
                self.touch(parsed.key.clone());
                self.data.insert(parsed.key, parsed.value);
            } else {
                self.order.push_back(parsed.key.clone());
                self.data.insert(parsed.key, parsed.value);
            }
        }
        Ok(())
    }

    fn touch(&mut self, key: CacheKey) {
        self.order.retain(|k| k != &key);
        self.order.push_back(key);
    }

    fn evict_if_needed(&mut self) -> Result<(), CacheError> {
        while self.data.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.data.remove(&oldest);
                self.delete_entry_file(&oldest)?;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn persist_entry(&self, key: &CacheKey, value: &[u8]) -> Result<(), CacheError> {
        let entry = DiskEntry {
            key: key.clone(),
            value: value.to_vec(),
        };
        let payload = serde_json::to_vec(&entry)?;
        fs::write(self.entry_path(key), payload)?;
        Ok(())
    }

    fn delete_entry_file(&self, key: &CacheKey) -> Result<(), CacheError> {
        match fs::remove_file(self.entry_path(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CacheError::Io(e)),
        }
    }

    fn entry_path(&self, key: &CacheKey) -> PathBuf {
        self.root.join(format!("{}.json", key_fingerprint(key)))
    }
}

fn key_fingerprint(key: &CacheKey) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(key.file_path.as_bytes());
    hasher.update(&[0]);
    hasher.update(key.version.to_string().as_bytes());
    hasher.update(&[0]);
    hasher.update(key.chunk_id.as_bytes());
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn two_tier_put_get_invalidate() {
        let cache = TwoTierChunkCache::new(1, 2);
        let key = CacheKey {
            file_path: "/x".to_string(),
            version: 1,
            chunk_id: "abc".to_string(),
        };

        cache
            .put(key.clone(), b"data".to_vec())
            .expect("put should work");
        let hit = cache.get(&key).expect("get should work");
        assert_eq!(hit.as_deref(), Some(&b"data"[..]));

        cache
            .invalidate_path("/x")
            .expect("invalidate should work");
        assert!(cache.get(&key).expect("get should work").is_none());

        cache
            .put(key.clone(), b"data".to_vec())
            .expect("put should work");
        cache
            .invalidate_all()
            .expect("invalidate all should work");
        assert!(cache.get(&key).expect("get should work").is_none());
    }

    #[test]
    fn persistent_cache_survives_reopen() {
        let temp = tempdir().expect("temp dir should exist");
        let root = temp.path().join("warm-cache");
        let key = CacheKey {
            file_path: "/x".to_string(),
            version: 1,
            chunk_id: "abc".to_string(),
        };

        {
            let cache = PersistentTwoTierChunkCache::open(&root, 2, 8).expect("cache should open");
            cache
                .put(key.clone(), b"persisted".to_vec())
                .expect("put should work");
        }

        {
            let cache = PersistentTwoTierChunkCache::open(&root, 2, 8).expect("cache should reopen");
            let hit = cache.get(&key).expect("get should work");
            assert_eq!(hit.as_deref(), Some(&b"persisted"[..]));
        }
    }

    #[test]
    fn persistent_cache_invalidate_path_removes_disk_entries() {
        let temp = tempdir().expect("temp dir should exist");
        let root = temp.path().join("warm-cache");
        let key_a = CacheKey {
            file_path: "/a".to_string(),
            version: 1,
            chunk_id: "a".to_string(),
        };
        let key_b = CacheKey {
            file_path: "/b".to_string(),
            version: 1,
            chunk_id: "b".to_string(),
        };

        {
            let cache = PersistentTwoTierChunkCache::open(&root, 2, 8).expect("cache should open");
            cache
                .put(key_a.clone(), b"aaa".to_vec())
                .expect("put should work");
            cache
                .put(key_b.clone(), b"bbb".to_vec())
                .expect("put should work");
            cache
                .invalidate_path("/a")
                .expect("invalidate path should work");
        }

        {
            let cache = PersistentTwoTierChunkCache::open(&root, 2, 8).expect("cache should reopen");
            assert!(cache.get(&key_a).expect("get should work").is_none());
            let hit = cache.get(&key_b).expect("get should work");
            assert_eq!(hit.as_deref(), Some(&b"bbb"[..]));
        }
    }
}
