//! Moka-based cache implementation for improved performance and memory management

use super::{CacheStats, StorageLevel};
use crate::traits::Data;
use anyhow::Result;
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// Configuration for Moka cache
#[derive(Debug, Clone)]
pub struct MokaCacheConfig {
    /// Maximum number of entries
    pub max_capacity: u64,
    /// Time to live for entries
    pub time_to_live: Option<Duration>,
    /// Time to idle for entries
    pub time_to_idle: Option<Duration>,
    /// Initial capacity
    pub initial_capacity: Option<usize>,
}

impl Default for MokaCacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: 10_000,
            time_to_live: Some(Duration::from_secs(3600)), // 1 hour
            time_to_idle: Some(Duration::from_secs(1800)), // 30 minutes
            initial_capacity: Some(1000),
        }
    }
}

/// Cached block data with metadata
#[derive(Debug, Clone)]
pub struct MokaCachedBlock<T> {
    /// The cached data
    pub data: Vec<T>,
    /// Storage level used for this block
    pub storage_level: StorageLevel,
    /// Size in bytes (estimated)
    pub size_bytes: usize,
    /// Timestamp when cached
    pub cached_at: SystemTime,
    /// Number of times this block has been accessed
    pub access_count: Arc<RwLock<u64>>,
}

impl<T> MokaCachedBlock<T> {
    pub fn new(data: Vec<T>, storage_level: StorageLevel) -> Self {
        let size_bytes = data.len() * std::mem::size_of::<T>();
        Self {
            data,
            storage_level,
            size_bytes,
            cached_at: SystemTime::now(),
            access_count: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn access(&self) -> &Vec<T> {
        let mut count = self.access_count.write().await;
        *count += 1;
        &self.data
    }

    pub async fn get_access_count(&self) -> u64 {
        *self.access_count.read().await
    }
}

/// Unique identifier for a cached block (Moka-specific)
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheBlockId {
    /// RDD ID
    pub rdd_id: usize,
    /// Partition index
    pub partition_index: usize,
}

impl CacheBlockId {
    pub fn new(rdd_id: usize, partition_index: usize) -> Self {
        Self {
            rdd_id,
            partition_index,
        }
    }
}

/// Moka-based cache wrapper that provides the same interface as HashMap-based cache
pub struct MokaCache<K, V> {
    /// The underlying Moka cache
    cache: Cache<K, V>,
    /// Cache statistics
    stats: Arc<RwLock<CacheStats>>,
    /// Configuration
    config: MokaCacheConfig,
}

impl<K, V> MokaCache<K, V>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    /// Create a new Moka cache with default configuration
    pub fn new() -> Self {
        Self::with_config(MokaCacheConfig::default())
    }

    /// Create a new Moka cache with custom configuration
    pub fn with_config(config: MokaCacheConfig) -> Self {
        let mut builder = Cache::builder().max_capacity(config.max_capacity);

        if let Some(ttl) = config.time_to_live {
            builder = builder.time_to_live(ttl);
        }

        if let Some(tti) = config.time_to_idle {
            builder = builder.time_to_idle(tti);
        }

        if let Some(initial_capacity) = config.initial_capacity {
            builder = builder.initial_capacity(initial_capacity);
        }

        let cache = builder.build();

        Self {
            cache,
            stats: Arc::new(RwLock::new(CacheStats::default())),
            config,
        }
    }

    /// Insert a value into the cache
    pub async fn insert(&self, key: K, value: V) {
        self.cache.insert(key, value).await;
        self.cache.run_pending_tasks().await;
        let mut stats = self.stats.write().await;
        stats.record_cached_partition();
    }

    /// Get a value from the cache
    pub async fn get(&self, key: &K) -> Option<V> {
        let result = self.cache.get(key).await;
        let mut stats = self.stats.write().await;

        if result.is_some() {
            stats.record_hit();
        } else {
            stats.record_miss();
        }

        result
    }

    /// Remove a value from the cache
    pub async fn remove(&self, key: &K) -> Option<V> {
        let result = self.cache.remove(key).await;
        if result.is_some() {
            let mut stats = self.stats.write().await;
            stats.record_evicted_partition();
        }
        result
    }

    /// Check if the cache contains a key
    pub async fn contains_key(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }

    /// Clear all entries from the cache
    pub async fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        let mut stats = self.stats.write().await;
        *stats = CacheStats::default();
    }

    /// Get the number of entries in the cache
    pub async fn len(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }

    /// Check if the cache is empty
    pub async fn is_empty(&self) -> bool {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count() == 0
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    /// Get all keys in the cache
    pub async fn keys(&self) -> Vec<K> {
        // Note: Moka doesn't provide a direct way to get all keys
        // This is a limitation we'll need to work around
        // For now, we'll return an empty vector and track keys separately if needed
        Vec::new()
    }

    /// Run pending maintenance tasks
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }

    /// Get cache configuration
    pub fn config(&self) -> &MokaCacheConfig {
        &self.config
    }
}

impl<K, V> Default for MokaCache<K, V>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> std::fmt::Debug for MokaCache<K, V>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaCache")
            .field("max_capacity", &self.config.max_capacity)
            .field("time_to_live", &self.config.time_to_live)
            .field("time_to_idle", &self.config.time_to_idle)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_moka_cache_basic_operations() {
        let cache: MokaCache<String, i32> = MokaCache::new();

        // Test insert and get
        cache.insert("key1".to_string(), 42).await;
        assert_eq!(cache.get(&"key1".to_string()).await, Some(42));
        assert_eq!(cache.get(&"nonexistent".to_string()).await, None);

        // Test contains_key
        assert!(cache.contains_key(&"key1".to_string()).await);
        assert!(!cache.contains_key(&"nonexistent".to_string()).await);

        // Test len
        assert_eq!(cache.len().await, 1);
        assert!(!cache.is_empty().await);

        // Test remove
        assert_eq!(cache.remove(&"key1".to_string()).await, Some(42));
        assert_eq!(cache.remove(&"key1".to_string()).await, None);
        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_moka_cache_stats() {
        let cache: MokaCache<String, i32> = MokaCache::new();

        // Insert some values
        cache.insert("key1".to_string(), 1).await;
        cache.insert("key2".to_string(), 2).await;

        // Test hits and misses
        let _val1 = cache.get(&"key1".to_string()).await; // hit
        let _val2 = cache.get(&"key1".to_string()).await; // hit
        let _val3 = cache.get(&"nonexistent".to_string()).await; // miss

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!(stats.hit_ratio() > 0.0);
    }

    #[tokio::test]
    async fn test_moka_cache_clear() {
        let cache: MokaCache<String, i32> = MokaCache::new();

        // Insert some values
        cache.insert("key1".to_string(), 1).await;
        cache.insert("key2".to_string(), 2).await;
        assert_eq!(cache.len().await, 2);

        // Clear cache
        cache.clear().await;
        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);

        // Stats should be reset
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    }
}
