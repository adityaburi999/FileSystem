use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache internal lock poisoned")]
    Poisoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
