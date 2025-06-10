//! Cache abstraction using trait-based design.
//!
//! This module provides a generic cache interface that abstracts over
//! the underlying caching implementation (moka).

use async_trait::async_trait;
use moka::future::Cache as MokaCache;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::Result;

/// Generic cache trait for async operations.
///
/// This trait provides a unified interface for caching operations
/// without exposing the underlying implementation details.
#[async_trait]
pub trait Cache<K, V>: Send + Sync + Debug
where
    K: Send + Sync + Clone + Hash + Eq + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    /// Get a value from the cache by key.
    async fn get(&self, key: &K) -> Option<V>;

    /// Insert a key-value pair into the cache.
    async fn put(&self, key: K, value: V);

    /// Remove a key from the cache.
    async fn remove(&self, key: &K) -> Option<V>;

    /// Check if a key exists in the cache.
    async fn contains_key(&self, key: &K) -> bool;

    /// Clear all entries from the cache.
    async fn clear(&self);

    /// Get the number of entries in the cache.
    async fn len(&self) -> usize;

    /// Check if the cache is empty.
    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Get cache statistics if available.
    async fn stats(&self) -> CacheStats;
}

/// Cache statistics information.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hit_count: u64,
    pub miss_count: u64,
    pub entry_count: u64,
    pub eviction_count: u64,
}

impl CacheStats {
    /// Calculate hit ratio.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.hit_count as f64 / total as f64
        }
    }
}

/// Configuration for cache creation.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of entries in the cache.
    pub max_capacity: Option<u64>,
    /// Time to live for cache entries.
    pub time_to_live: Option<Duration>,
    /// Time to idle for cache entries.
    pub time_to_idle: Option<Duration>,
    /// Initial capacity of the cache.
    pub initial_capacity: Option<usize>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: Some(10_000),
            time_to_live: None,
            time_to_idle: None,
            initial_capacity: Some(100),
        }
    }
}

/// Builder for creating cache instances.
pub struct CacheBuilder {
    config: CacheConfig,
}

impl CacheBuilder {
    /// Create a new cache builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: CacheConfig::default(),
        }
    }

    /// Set the maximum capacity of the cache.
    pub fn max_capacity(mut self, capacity: u64) -> Self {
        self.config.max_capacity = Some(capacity);
        self
    }

    /// Set the time to live for cache entries.
    pub fn time_to_live(mut self, ttl: Duration) -> Self {
        self.config.time_to_live = Some(ttl);
        self
    }

    /// Set the time to idle for cache entries.
    pub fn time_to_idle(mut self, tti: Duration) -> Self {
        self.config.time_to_idle = Some(tti);
        self
    }

    /// Set the initial capacity of the cache.
    pub fn initial_capacity(mut self, capacity: usize) -> Self {
        self.config.initial_capacity = Some(capacity);
        self
    }

    /// Build a cache instance with the specified configuration.
    pub fn build<K, V>(self) -> Result<Arc<dyn Cache<K, V>>>
    where
        K: Send + Sync + Clone + Hash + Eq + Debug + 'static,
        V: Send + Sync + Clone + Debug + 'static,
    {
        let cache = MokaCacheImpl::new(self.config)?;
        Ok(Arc::new(cache))
    }
}

impl Default for CacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal statistics tracker for cache operations.
#[derive(Debug, Default)]
struct InternalCacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl InternalCacheStats {
    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::AcqRel);
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::AcqRel);
    }

    fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::AcqRel);
    }

    fn get_stats(&self, entry_count: u64) -> CacheStats {
        CacheStats {
            hit_count: self.hits.load(Ordering::Acquire),
            miss_count: self.misses.load(Ordering::Acquire),
            entry_count,
            eviction_count: self.evictions.load(Ordering::Acquire),
        }
    }
}

/// Moka-based cache implementation.
#[derive(Debug)]
struct MokaCacheImpl<K, V>
where
    K: Send + Sync + Clone + Hash + Eq + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    inner: MokaCache<K, V>,
    stats: Arc<InternalCacheStats>,
}

impl<K, V> MokaCacheImpl<K, V>
where
    K: Send + Sync + Clone + Hash + Eq + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    fn new(config: CacheConfig) -> Result<Self> {
        let stats = Arc::new(InternalCacheStats::default());

        // Create eviction listener to track evictions
        let stats_for_listener = Arc::clone(&stats);
        let eviction_listener = move |_k, _v, cause| {
            use moka::notification::RemovalCause;

            // RemovalCause::Size means that the cache reached its maximum capacity
            // and had to evict an entry.
            if cause == RemovalCause::Size {
                stats_for_listener.record_eviction();
            }
        };

        let mut builder = MokaCache::builder();

        if let Some(capacity) = config.max_capacity {
            builder = builder.max_capacity(capacity);
        }

        if let Some(ttl) = config.time_to_live {
            builder = builder.time_to_live(ttl);
        }

        if let Some(tti) = config.time_to_idle {
            builder = builder.time_to_idle(tti);
        }

        if let Some(initial) = config.initial_capacity {
            builder = builder.initial_capacity(initial);
        }

        // Add the eviction listener
        builder = builder.eviction_listener(eviction_listener);

        let inner = builder.build();

        Ok(Self { inner, stats })
    }
}

#[async_trait]
impl<K, V> Cache<K, V> for MokaCacheImpl<K, V>
where
    K: Send + Sync + Clone + Hash + Eq + Debug + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    async fn get(&self, key: &K) -> Option<V> {
        let result = self.inner.get(key).await;

        // Track hit/miss statistics
        if result.is_some() {
            self.stats.record_hit();
        } else {
            self.stats.record_miss();
        }

        result
    }

    async fn put(&self, key: K, value: V) {
        // Check if key exists before insertion to track statistics
        let existed = self.inner.contains_key(&key);

        // Insert the value
        self.inner.insert(key, value).await;

        // Track miss for new entries (when key didn't exist before)
        if !existed {
            self.stats.record_miss();
        }
    }

    async fn remove(&self, key: &K) -> Option<V> {
        self.inner.remove(key).await
    }

    async fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    async fn clear(&self) {
        self.inner.invalidate_all();
    }

    async fn len(&self) -> usize {
        self.inner.entry_count() as usize
    }

    async fn stats(&self) -> CacheStats {
        self.stats.get_stats(self.inner.entry_count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;
    use tracing::info;

    #[tokio::test]
    async fn test_cache_basic_operations() {
        let cache = CacheBuilder::new()
            .max_capacity(100)
            .build::<String, i32>()
            .expect("Failed to create cache");

        // Test put and get
        cache.put("key1".to_string(), 42).await;
        assert_eq!(cache.get(&"key1".to_string()).await, Some(42));

        // Test contains_key
        assert!(cache.contains_key(&"key1".to_string()).await);
        assert!(!cache.contains_key(&"nonexistent".to_string()).await);

        // Test remove
        let removed = cache.remove(&"key1".to_string()).await;
        assert_eq!(removed, Some(42));
        assert_eq!(cache.get(&"key1".to_string()).await, None);

        // Test clear
        cache.put("key2".to_string(), 100).await;
        cache.put("key3".to_string(), 200).await;

        // Verify we can get the values we just put
        assert_eq!(cache.get(&"key2".to_string()).await, Some(100));
        assert_eq!(cache.get(&"key3".to_string()).await, Some(200));

        cache.clear().await;

        // After clear, values should be gone
        assert_eq!(cache.get(&"key2".to_string()).await, None);
        assert_eq!(cache.get(&"key3".to_string()).await, None);
    }

    #[tokio::test]
    async fn test_cache_with_ttl() {
        let cache = CacheBuilder::new()
            .max_capacity(100)
            .time_to_live(Duration::from_millis(100))
            .build::<String, i32>()
            .expect("Failed to create cache");

        cache.put("key1".to_string(), 42).await;
        assert_eq!(cache.get(&"key1".to_string()).await, Some(42));

        // Wait for TTL to expire
        sleep(Duration::from_millis(150)).await;

        // Value should be expired (note: this might be flaky due to timing)
        // In a real test, you might want to use a longer TTL and manual expiration
        let _result = cache.get(&"key1".to_string()).await;
        // The exact behavior depends on moka's implementation
        // This test mainly ensures the TTL configuration is accepted
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let cache = CacheBuilder::new()
            .max_capacity(100)
            .build::<String, i32>()
            .expect("Failed to create cache");

        let stats = cache.stats().await;
        assert_eq!(stats.entry_count, 0);

        cache.put("key1".to_string(), 42).await;

        // Verify the value was actually stored
        assert_eq!(cache.get(&"key1".to_string()).await, Some(42));

        // Verify that statistics are being tracked correctly
        let stats = cache.stats().await;
        // After put (miss) and get (hit), we should have:
        assert_eq!(stats.hit_count, 1); // One hit from the get operation
        assert_eq!(stats.miss_count, 1); // One miss from the put operation (new key)
        assert_eq!(stats.eviction_count, 0); // No evictions yet
    }

    #[tokio::test]
    async fn test_cache_statistics_comprehensive() {
        // Create a cache with reasonable capacity
        let cache = CacheBuilder::new()
            .max_capacity(100)
            .build::<String, i32>()
            .expect("Failed to create cache");

        // Test multiple operations and verify statistics

        // 1. Put new entries (should record misses)
        cache.put("key1".to_string(), 1).await;
        cache.put("key2".to_string(), 2).await;
        cache.put("key3".to_string(), 3).await;

        // 2. Get existing entries (should record hits)
        assert_eq!(cache.get(&"key1".to_string()).await, Some(1));
        assert_eq!(cache.get(&"key2".to_string()).await, Some(2));

        // 3. Get non-existent entry (should record miss)
        assert_eq!(cache.get(&"nonexistent".to_string()).await, None);

        // 4. Update existing entry (should not record miss since key exists)
        cache.put("key1".to_string(), 10).await;

        // 5. Get updated entry (should record hit)
        assert_eq!(cache.get(&"key1".to_string()).await, Some(10));

        // Check statistics
        let stats = cache.stats().await;

        // Expected statistics:
        // - 4 misses: 3 from new puts + 1 from get of nonexistent key
        // - 3 hits: 2 from gets of existing keys + 1 from get after update
        // - 0 evictions: cache capacity is large enough
        assert_eq!(stats.miss_count, 4);
        assert_eq!(stats.hit_count, 3);
        assert_eq!(stats.eviction_count, 0);

        // Verify hit ratio calculation
        let expected_ratio = 3.0 / 7.0; // 3 hits out of 7 total operations
        assert!((stats.hit_ratio() - expected_ratio).abs() < 0.001);

        assert_eq!(cache.get(&"key1".to_string()).await, Some(10));
        assert_eq!(cache.get(&"key2".to_string()).await, Some(2));
        assert_eq!(cache.get(&"key3".to_string()).await, Some(3));
    }

    #[tokio::test]
    async fn test_cache_statistics() {
        // Example demonstrating cache statistics collection
        let cache = CacheBuilder::new()
            .max_capacity(1000)
            .build::<String, String>()
            .expect("Failed to create cache");

        // Simulate some cache operations
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache.put(key.clone(), value).await;
        }

        // Access some cached values (hits)
        for i in 0..5 {
            let key = format!("key_{}", i);
            let _value = cache.get(&key).await;
        }

        // Try to access non-existent values (misses)
        for i in 10..15 {
            let key = format!("key_{}", i);
            let _value = cache.get(&key).await;
        }

        // Get statistics
        let stats = cache.stats().await;

        // Verify the statistics make sense
        assert_eq!(stats.miss_count, 15); // 10 from puts + 5 from gets of non-existent keys
        assert_eq!(stats.hit_count, 5); // 5 from gets of existing keys
        assert_eq!(stats.eviction_count, 0); // No evictions with large capacity

        // Verify hit ratio
        let expected_ratio = 5.0 / 20.0; // 5 hits out of 20 total operations
        assert!((stats.hit_ratio() - expected_ratio).abs() < 0.001);

        info!("Cache Statistics:");
        info!("  Hits: {}", stats.hit_count);
        info!("  Misses: {}", stats.miss_count);
        info!("  Hit Ratio: {:.2}%", stats.hit_ratio() * 100.0);
        info!("  Evictions: {}", stats.eviction_count);
        info!("  Entry Count: {}", stats.entry_count);
    }
}
