//! Moka-based block manager for RDD caching with improved performance

use super::block_manager::BlockId;
use super::{CacheStats, MokaCache, MokaCacheConfig, MokaCachedBlock, StorageLevel};
use crate::traits::Data;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Moka-based cache manager for RDD blocks
pub struct MokaBlockManager<T: Data> {
    /// In-memory cache using Moka
    memory_cache: MokaCache<BlockId, MokaCachedBlock<T>>,
    /// Disk cache using KVStore
    disk_cache: Arc<dyn barks_kvstore::KVStore<Key = BlockId, Value = Vec<T>>>,
    /// Cache statistics
    stats: Arc<RwLock<CacheStats>>,
    /// Maximum memory usage (in bytes)
    max_memory_bytes: usize,
    /// Current memory usage (in bytes)
    current_memory_bytes: Arc<RwLock<usize>>,
}

impl<T: Data> MokaBlockManager<T> {
    /// Create a new Moka-based block manager
    pub fn new(
        disk_cache: Arc<dyn barks_kvstore::KVStore<Key = BlockId, Value = Vec<T>>>,
        max_memory_bytes: usize,
    ) -> Self {
        Self::with_config(disk_cache, max_memory_bytes, MokaCacheConfig::default())
    }

    /// Create a new Moka-based block manager with custom cache configuration
    pub fn with_config(
        disk_cache: Arc<dyn barks_kvstore::KVStore<Key = BlockId, Value = Vec<T>>>,
        max_memory_bytes: usize,
        cache_config: MokaCacheConfig,
    ) -> Self {
        // Configure Moka cache for block management
        let mut config = cache_config;

        // Set reasonable defaults for block caching
        if config.time_to_live.is_none() {
            config.time_to_live = Some(Duration::from_secs(7200)); // 2 hours
        }
        if config.time_to_idle.is_none() {
            config.time_to_idle = Some(Duration::from_secs(3600)); // 1 hour
        }

        // Calculate max capacity based on memory limit and estimated block size
        let estimated_block_size = 1024 * 1024; // 1MB per block estimate
        let max_capacity = (max_memory_bytes / estimated_block_size).max(100) as u64;
        config.max_capacity = max_capacity;

        let memory_cache = MokaCache::with_config(config);

        Self {
            memory_cache,
            disk_cache,
            stats: Arc::new(RwLock::new(CacheStats::default())),
            max_memory_bytes,
            current_memory_bytes: Arc::new(RwLock::new(0)),
        }
    }

    /// Cache a block with the specified storage level
    pub async fn cache_block(
        &self,
        block_id: BlockId,
        data: Vec<T>,
        storage_level: StorageLevel,
    ) -> Result<()> {
        if !storage_level.is_cached() {
            return Ok(());
        }

        let block = MokaCachedBlock::new(data, storage_level);

        // Cache in memory if required
        if storage_level.use_memory() {
            // Check if we need to evict blocks to make space
            if self.needs_eviction(block.size_bytes).await {
                self.evict_blocks(block.size_bytes).await?;
            }

            // Insert into Moka cache
            self.memory_cache
                .insert(block_id.clone(), block.clone())
                .await;

            // Update memory usage tracking
            let mut current_memory = self.current_memory_bytes.write().await;
            *current_memory += block.size_bytes;

            // Update statistics
            let mut stats = self.stats.write().await;
            stats.record_cached_partition();
            stats.update_memory_usage(*current_memory as u64);
        }

        // Cache on disk if required
        if storage_level.use_disk() {
            self.disk_cache
                .put(&block_id, block.data)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to cache block to disk: {}", e))?;

            let mut stats = self.stats.write().await;
            let current_disk_usage = stats.disk_used;
            stats.update_disk_usage(current_disk_usage + block.size_bytes as u64);
        }

        Ok(())
    }

    /// Get a cached block
    pub async fn get_block(&self, block_id: &BlockId) -> Result<Option<Vec<T>>> {
        let mut stats = self.stats.write().await;

        // Try memory cache first
        if let Some(block) = self.memory_cache.get(block_id).await {
            stats.record_hit();
            return Ok(Some(block.access().await.clone()));
        }

        // Try disk cache
        if let Some(data) = self
            .disk_cache
            .get(block_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get block from disk cache: {}", e))?
        {
            stats.record_hit();
            return Ok(Some(data));
        }

        stats.record_miss();
        Ok(None)
    }

    /// Remove a block from cache
    pub async fn remove_block(&self, block_id: &BlockId) -> Result<()> {
        // Remove from memory cache
        if let Some(block) = self.memory_cache.remove(block_id).await {
            let mut current_memory = self.current_memory_bytes.write().await;
            *current_memory = current_memory.saturating_sub(block.size_bytes);

            let mut stats = self.stats.write().await;
            stats.record_evicted_partition();
            stats.update_memory_usage(*current_memory as u64);
        }

        // Remove from disk cache
        self.disk_cache
            .remove(block_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to remove block from disk cache: {}", e))?;

        Ok(())
    }

    /// Get cache statistics
    pub async fn get_stats(&self) -> CacheStats {
        // Combine stats from Moka cache and our own tracking
        let moka_stats = self.memory_cache.stats().await;
        let mut stats = self.stats.read().await.clone();

        // Update with Moka cache stats
        stats.hits += moka_stats.hits;
        stats.misses += moka_stats.misses;

        stats
    }

    /// Clear all cached blocks
    pub async fn clear(&self) -> Result<()> {
        // Clear memory cache
        self.memory_cache.clear().await;

        // Reset memory usage tracking
        let mut current_memory = self.current_memory_bytes.write().await;
        *current_memory = 0;

        // Clear disk cache
        self.disk_cache
            .clear()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clear disk cache: {}", e))?;

        // Reset statistics
        let mut stats = self.stats.write().await;
        *stats = CacheStats::default();

        Ok(())
    }

    /// Check if eviction is needed
    async fn needs_eviction(&self, new_block_size: usize) -> bool {
        let current_memory = *self.current_memory_bytes.read().await;
        current_memory + new_block_size > self.max_memory_bytes
    }

    /// Evict blocks to make space
    /// Note: Moka handles eviction automatically based on its policies,
    /// but we still need to track memory usage manually
    async fn evict_blocks(&self, space_needed: usize) -> Result<()> {
        // Run pending maintenance tasks to trigger eviction
        self.memory_cache.run_pending_tasks().await;

        // Check if we have enough space now
        let current_memory = *self.current_memory_bytes.read().await;
        if current_memory + space_needed <= self.max_memory_bytes {
            return Ok(());
        }

        // If we still don't have enough space, we may need to force eviction
        // For now, we'll rely on Moka's automatic eviction policies
        // In a production system, we might implement additional logic here

        Ok(())
    }

    /// Get the current memory usage
    pub async fn memory_usage(&self) -> usize {
        *self.current_memory_bytes.read().await
    }

    /// Get the maximum memory limit
    pub fn max_memory(&self) -> usize {
        self.max_memory_bytes
    }

    /// Get the number of cached blocks in memory
    pub async fn cached_blocks_count(&self) -> u64 {
        self.memory_cache.len().await
    }

    /// Check if the cache is empty
    pub async fn is_empty(&self) -> bool {
        self.memory_cache.is_empty().await
    }
}

impl<T: Data> std::fmt::Debug for MokaBlockManager<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaBlockManager")
            .field("max_memory_bytes", &self.max_memory_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barks_kvstore::MemoryKVStore;

    #[tokio::test]
    async fn test_moka_block_manager_basic_operations() {
        let disk_cache = Arc::new(MemoryKVStore::new());
        let manager = MokaBlockManager::new(disk_cache, 64 * 1024 * 1024); // 64MB

        let block_id = BlockId::new(1, 0);
        let data = vec![1, 2, 3, 4, 5];

        // Test caching
        manager
            .cache_block(block_id.clone(), data.clone(), StorageLevel::MemoryOnly)
            .await
            .unwrap();

        // Test retrieval
        let retrieved = manager.get_block(&block_id).await.unwrap();
        assert_eq!(retrieved, Some(data));

        // Test removal
        manager.remove_block(&block_id).await.unwrap();
        let retrieved_after_removal = manager.get_block(&block_id).await.unwrap();
        assert_eq!(retrieved_after_removal, None);
    }

    #[tokio::test]
    async fn test_moka_block_manager_storage_levels() {
        let disk_cache = Arc::new(MemoryKVStore::new());
        let manager = MokaBlockManager::new(disk_cache, 64 * 1024 * 1024);

        let block_id = BlockId::new(1, 0);
        let data = vec![1, 2, 3, 4, 5];

        // Test memory and disk storage
        manager
            .cache_block(block_id.clone(), data.clone(), StorageLevel::MemoryAndDisk)
            .await
            .unwrap();

        let retrieved = manager.get_block(&block_id).await.unwrap();
        assert_eq!(retrieved, Some(data));

        // Test disk only storage
        let block_id2 = BlockId::new(2, 0);
        let data2 = vec![6, 7, 8, 9, 10];

        manager
            .cache_block(block_id2.clone(), data2.clone(), StorageLevel::DiskOnly)
            .await
            .unwrap();

        let retrieved2 = manager.get_block(&block_id2).await.unwrap();
        assert_eq!(retrieved2, Some(data2));
    }

    #[tokio::test]
    async fn test_moka_block_manager_stats() {
        let disk_cache = Arc::new(MemoryKVStore::new());
        let manager = MokaBlockManager::new(disk_cache, 64 * 1024 * 1024);

        let block_id = BlockId::new(1, 0);
        let data = vec![1, 2, 3, 4, 5];

        // Cache a block
        manager
            .cache_block(block_id.clone(), data, StorageLevel::MemoryOnly)
            .await
            .unwrap();

        // Access it multiple times
        let _result1 = manager.get_block(&block_id).await.unwrap();
        let _result2 = manager.get_block(&block_id).await.unwrap();
        let _result3 = manager.get_block(&BlockId::new(999, 0)).await.unwrap(); // miss

        let stats = manager.get_stats().await;
        assert!(stats.hits >= 2);
        assert!(stats.misses >= 1);
    }
}
