//! Moka-based broadcast cache implementation for improved performance

use super::{BroadcastCacheStats, BroadcastId, SerializableBroadcast};
use crate::cache::{MokaCache, MokaCacheConfig};
use std::time::Duration;

/// Moka-based cache for broadcast variables on executors
#[derive(Debug)]
pub struct MokaBroadcastCache {
    /// Moka cache for broadcast variables
    cache: MokaCache<BroadcastId, SerializableBroadcast>,
}

impl Default for MokaBroadcastCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MokaBroadcastCache {
    /// Create a new Moka-based broadcast cache
    pub fn new() -> Self {
        Self::with_config(Self::default_config())
    }

    /// Create a new Moka-based broadcast cache with custom configuration
    pub fn with_config(config: MokaCacheConfig) -> Self {
        let cache = MokaCache::with_config(config);
        Self { cache }
    }

    /// Get default configuration for broadcast cache
    fn default_config() -> MokaCacheConfig {
        MokaCacheConfig {
            max_capacity: 1000, // Reasonable limit for broadcast variables
            time_to_live: Some(Duration::from_secs(3600 * 24)), // 24 hours
            time_to_idle: Some(Duration::from_secs(3600 * 2)), // 2 hours
            initial_capacity: Some(100),
        }
    }

    /// Cache a broadcast variable
    pub async fn cache_broadcast(&self, broadcast: SerializableBroadcast) {
        self.cache.insert(broadcast.id.clone(), broadcast).await;
        self.cache.run_pending_tasks().await;
    }

    /// Get a cached broadcast variable
    pub async fn get_broadcast(&self, id: &BroadcastId) -> Option<SerializableBroadcast> {
        self.cache.get(id).await
    }

    /// Check if a broadcast variable is cached
    pub async fn contains(&self, id: &BroadcastId) -> bool {
        self.cache.contains_key(id).await
    }

    /// Remove a broadcast variable from cache
    pub async fn remove_broadcast(&self, id: &BroadcastId) -> bool {
        self.cache.remove(id).await.is_some()
    }

    /// Clear all cached broadcast variables
    pub async fn clear(&self) {
        self.cache.clear().await;
    }

    /// Get cache statistics
    pub async fn stats(&self) -> BroadcastCacheStats {
        let _cache_stats = self.cache.stats().await;
        let cached_broadcasts = self.cache.len().await as usize;

        // Estimate total size - this is approximate since we can't easily get all values
        // In a real implementation, we might track this separately
        let total_size_bytes = cached_broadcasts * 1024; // Rough estimate

        BroadcastCacheStats {
            cached_broadcasts,
            total_size_bytes,
        }
    }

    /// Get the number of cached broadcast variables
    pub async fn len(&self) -> usize {
        self.cache.run_pending_tasks().await;
        self.cache.len().await as usize
    }

    /// Check if the cache is empty
    pub async fn is_empty(&self) -> bool {
        self.cache.is_empty().await
    }

    /// Run pending maintenance tasks
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}

/// Moka-based manager for broadcast variables on the driver
#[derive(Debug)]
pub struct MokaBroadcastManager {
    /// Moka cache for broadcast variables
    broadcasts: MokaCache<BroadcastId, SerializableBroadcast>,
}

impl Default for MokaBroadcastManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MokaBroadcastManager {
    /// Create a new Moka-based broadcast manager
    pub fn new() -> Self {
        Self::with_config(Self::default_config())
    }

    /// Create a new Moka-based broadcast manager with custom configuration
    pub fn with_config(config: MokaCacheConfig) -> Self {
        let broadcasts = MokaCache::with_config(config);
        Self { broadcasts }
    }

    /// Get default configuration for broadcast manager
    fn default_config() -> MokaCacheConfig {
        MokaCacheConfig {
            max_capacity: 10000,                                // Higher limit for driver
            time_to_live: Some(Duration::from_secs(3600 * 48)), // 48 hours
            time_to_idle: Some(Duration::from_secs(3600 * 4)),  // 4 hours
            initial_capacity: Some(1000),
        }
    }

    /// Register a broadcast variable
    pub async fn register_broadcast(&self, broadcast: SerializableBroadcast) {
        self.broadcasts
            .insert(broadcast.id.clone(), broadcast)
            .await;
        self.broadcasts.run_pending_tasks().await;
    }

    /// Get a broadcast variable
    pub async fn get_broadcast(&self, id: &BroadcastId) -> Option<SerializableBroadcast> {
        self.broadcasts.get(id).await
    }

    /// Check if a broadcast variable exists
    pub async fn contains_broadcast(&self, id: &BroadcastId) -> bool {
        self.broadcasts.contains_key(id).await
    }

    /// Remove a broadcast variable
    pub async fn remove_broadcast(&self, id: &BroadcastId) -> bool {
        self.broadcasts.remove(id).await.is_some()
    }

    /// Get all broadcast IDs
    pub async fn list_broadcasts(&self) -> Vec<BroadcastId> {
        // Note: Moka doesn't provide direct access to all keys
        // This is a limitation we need to work around
        // For now, we'll return an empty vector
        // In a production system, we might maintain a separate index
        Vec::new()
    }

    /// Clear all broadcast variables
    pub async fn clear(&self) {
        self.broadcasts.clear().await;
    }

    /// Get the number of broadcast variables
    pub async fn len(&self) -> usize {
        self.broadcasts.run_pending_tasks().await;
        self.broadcasts.len().await as usize
    }

    /// Check if the manager is empty
    pub async fn is_empty(&self) -> bool {
        self.broadcasts.is_empty().await
    }

    /// Run pending maintenance tasks
    pub async fn run_pending_tasks(&self) {
        self.broadcasts.run_pending_tasks().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::{BroadcastVariable, SerializableBroadcast};

    #[tokio::test]
    async fn test_moka_broadcast_cache() {
        let cache = MokaBroadcastCache::new();
        let value = vec![1, 2, 3, 4, 5];
        let broadcast = BroadcastVariable::new(value.clone());
        let serializable = SerializableBroadcast::from_broadcast(&broadcast).unwrap();

        // Cache broadcast
        cache.cache_broadcast(serializable.clone()).await;

        // Check if cached
        assert!(cache.contains(broadcast.id()).await);

        // Get cached broadcast
        let retrieved = cache.get_broadcast(broadcast.id()).await.unwrap();
        let deserialized: BroadcastVariable<Vec<i32>> = retrieved.to_broadcast().unwrap();
        assert_eq!(deserialized.value(), &value);

        // Get stats
        let stats = cache.stats().await;
        assert_eq!(stats.cached_broadcasts, 1);
        assert!(stats.total_size_bytes > 0);

        // Remove broadcast
        assert!(cache.remove_broadcast(broadcast.id()).await);
        assert!(!cache.contains(broadcast.id()).await);
    }

    #[tokio::test]
    async fn test_moka_broadcast_manager() {
        let manager = MokaBroadcastManager::new();
        let value = vec![1, 2, 3, 4, 5];
        let broadcast = BroadcastVariable::new(value.clone());
        let serializable = SerializableBroadcast::from_broadcast(&broadcast).unwrap();

        // Register broadcast
        manager.register_broadcast(serializable.clone()).await;

        // Check if exists
        assert!(manager.contains_broadcast(broadcast.id()).await);

        // Get broadcast
        let retrieved = manager.get_broadcast(broadcast.id()).await.unwrap();
        let deserialized: BroadcastVariable<Vec<i32>> = retrieved.to_broadcast().unwrap();
        assert_eq!(deserialized.value(), &value);

        // Check count
        assert_eq!(manager.len().await, 1);
        assert!(!manager.is_empty().await);

        // Remove broadcast
        assert!(manager.remove_broadcast(broadcast.id()).await);
        assert!(!manager.contains_broadcast(broadcast.id()).await);
        assert_eq!(manager.len().await, 0);
        assert!(manager.is_empty().await);
    }

    #[tokio::test]
    async fn test_moka_broadcast_cache_clear() {
        let cache = MokaBroadcastCache::new();

        // Add multiple broadcasts
        for i in 0..5 {
            let value = vec![i; 5];
            let broadcast = BroadcastVariable::new(value);
            let serializable = SerializableBroadcast::from_broadcast(&broadcast).unwrap();
            cache.cache_broadcast(serializable).await;
        }

        assert_eq!(cache.len().await, 5);
        assert!(!cache.is_empty().await);

        // Clear cache
        cache.clear().await;
        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_moka_broadcast_manager_clear() {
        let manager = MokaBroadcastManager::new();

        // Add multiple broadcasts
        for i in 0..5 {
            let value = vec![i; 5];
            let broadcast = BroadcastVariable::new(value);
            let serializable = SerializableBroadcast::from_broadcast(&broadcast).unwrap();
            manager.register_broadcast(serializable).await;
        }

        assert_eq!(manager.len().await, 5);
        assert!(!manager.is_empty().await);

        // Clear manager
        manager.clear().await;
        assert_eq!(manager.len().await, 0);
        assert!(manager.is_empty().await);
    }
}
