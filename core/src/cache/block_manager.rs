//! Block manager for RDD caching

use super::{CacheStats, StorageLevel};
use crate::traits::Data;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Unique identifier for a cached block
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockId {
    /// RDD ID
    pub rdd_id: usize,
    /// Partition index
    pub partition_index: usize,
}

impl BlockId {
    pub fn new(rdd_id: usize, partition_index: usize) -> Self {
        Self {
            rdd_id,
            partition_index,
        }
    }
}

/// Cached block data
#[derive(Debug, Clone)]
pub struct CachedBlock<T> {
    /// The cached data
    pub data: Vec<T>,
    /// Storage level used for this block
    pub storage_level: StorageLevel,
    /// Size in bytes (estimated)
    pub size_bytes: usize,
    /// Timestamp when cached
    pub cached_at: std::time::SystemTime,
    /// Number of times this block has been accessed
    pub access_count: u64,
}

impl<T> CachedBlock<T> {
    pub fn new(data: Vec<T>, storage_level: StorageLevel) -> Self {
        let size_bytes = data.len() * std::mem::size_of::<T>();
        Self {
            data,
            storage_level,
            size_bytes,
            cached_at: std::time::SystemTime::now(),
            access_count: 0,
        }
    }

    pub fn access(&mut self) -> &Vec<T> {
        self.access_count += 1;
        &self.data
    }
}

/// Cache manager for RDD blocks
pub struct BlockManager<T: Data> {
    /// In-memory cache
    memory_cache: Arc<RwLock<HashMap<BlockId, CachedBlock<T>>>>,
    /// Disk cache using KVStore
    disk_cache: Arc<dyn barks_kvstore::KVStore<Key = BlockId, Value = Vec<T>>>,
    /// Cache statistics
    stats: Arc<RwLock<CacheStats>>,
    /// Maximum memory usage (in bytes)
    max_memory_bytes: usize,
    /// Current memory usage (in bytes)
    current_memory_bytes: Arc<RwLock<usize>>,
}

impl<T: Data> BlockManager<T> {
    /// Create a new block manager
    pub fn new(
        disk_cache: Arc<dyn barks_kvstore::KVStore<Key = BlockId, Value = Vec<T>>>,
        max_memory_bytes: usize,
    ) -> Self {
        Self {
            memory_cache: Arc::new(RwLock::new(HashMap::new())),
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

        let block = CachedBlock::new(data, storage_level);

        // Cache in memory if required
        if storage_level.use_memory() {
            // Check if we need to evict blocks to make space
            if self.needs_eviction(block.size_bytes).await {
                self.evict_blocks(block.size_bytes).await?;
            }

            let mut memory_cache = self.memory_cache.write().await;
            let mut current_memory = self.current_memory_bytes.write().await;

            memory_cache.insert(block_id.clone(), block.clone());
            *current_memory += block.size_bytes;

            let mut stats = self.stats.write().await;
            stats.record_cached_partition();
            stats.update_memory_usage(*current_memory as u64);
        }

        // Cache on disk if required
        if storage_level.use_disk() {
            self.disk_cache.put(&block_id, block.data).await?;

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
        {
            let mut memory_cache = self.memory_cache.write().await;
            if let Some(block) = memory_cache.get_mut(block_id) {
                stats.record_hit();
                return Ok(Some(block.access().clone()));
            }
        }

        // Try disk cache
        if let Some(data) = self.disk_cache.get(block_id).await? {
            stats.record_hit();
            return Ok(Some(data));
        }

        stats.record_miss();
        Ok(None)
    }

    /// Remove a block from cache
    pub async fn remove_block(&self, block_id: &BlockId) -> Result<()> {
        // Remove from memory cache
        {
            let mut memory_cache = self.memory_cache.write().await;
            if let Some(block) = memory_cache.remove(block_id) {
                let mut current_memory = self.current_memory_bytes.write().await;
                *current_memory = current_memory.saturating_sub(block.size_bytes);

                let mut stats = self.stats.write().await;
                stats.record_evicted_partition();
                stats.update_memory_usage(*current_memory as u64);
            }
        }

        // Remove from disk cache
        self.disk_cache.remove(block_id).await?;

        Ok(())
    }

    /// Get cache statistics
    pub async fn get_stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    /// Clear all cached blocks
    pub async fn clear(&self) -> Result<()> {
        {
            let mut memory_cache = self.memory_cache.write().await;
            memory_cache.clear();

            let mut current_memory = self.current_memory_bytes.write().await;
            *current_memory = 0;
        }

        self.disk_cache.clear().await?;

        let mut stats = self.stats.write().await;
        *stats = CacheStats::default();

        Ok(())
    }

    /// Check if eviction is needed
    async fn needs_eviction(&self, new_block_size: usize) -> bool {
        let current_memory = *self.current_memory_bytes.read().await;
        current_memory + new_block_size > self.max_memory_bytes
    }

    /// Evict blocks to make space (LRU policy)
    async fn evict_blocks(&self, space_needed: usize) -> Result<()> {
        let mut memory_cache = self.memory_cache.write().await;
        let mut current_memory = self.current_memory_bytes.write().await;
        let mut stats = self.stats.write().await;

        // Sort blocks by access count and cached time (LRU)
        let mut blocks: Vec<_> = memory_cache.iter().collect();
        blocks.sort_by(|a, b| {
            a.1.access_count
                .cmp(&b.1.access_count)
                .then(a.1.cached_at.cmp(&b.1.cached_at))
        });

        let mut freed_space = 0;
        let mut blocks_to_remove = Vec::new();

        for (block_id, block) in blocks {
            if freed_space >= space_needed {
                break;
            }

            blocks_to_remove.push(block_id.clone());
            freed_space += block.size_bytes;
        }

        for block_id in blocks_to_remove {
            if let Some(block) = memory_cache.remove(&block_id) {
                *current_memory = current_memory.saturating_sub(block.size_bytes);
                stats.record_evicted_partition();
            }
        }

        stats.update_memory_usage(*current_memory as u64);
        Ok(())
    }
}

impl<T: Data> std::fmt::Debug for BlockManager<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockManager")
            .field("max_memory_bytes", &self.max_memory_bytes)
            .finish()
    }
}
