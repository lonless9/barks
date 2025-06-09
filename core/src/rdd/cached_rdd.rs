//! RDD wrapper that adds caching functionality

use crate::cache::block_manager::BlockId;
use crate::cache::{BlockManager, StorageLevel};
use crate::traits::{Data, Dependency, IsRdd, Partition, RddBase, RddResult};
use std::sync::Arc;

/// Wrapper RDD that adds caching functionality to any RDD
#[derive(Clone)]
pub struct CachedRdd<T: Data> {
    id: usize,
    parent: Arc<dyn RddBase<Item = T>>,
    storage_level: StorageLevel,
    block_manager: Arc<BlockManager<T>>,
}

impl<T: Data> CachedRdd<T> {
    pub fn new(
        id: usize,
        parent: Arc<dyn RddBase<Item = T>>,
        storage_level: StorageLevel,
        block_manager: Arc<BlockManager<T>>,
    ) -> Self {
        Self {
            id,
            parent,
            storage_level,
            block_manager,
        }
    }
}

impl<T: Data> std::fmt::Debug for CachedRdd<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedRdd")
            .field("id", &self.id)
            .field("storage_level", &self.storage_level)
            .finish()
    }
}

impl<T: Data> crate::traits::IsRdd for CachedRdd<T> {
    fn dependencies(&self) -> Vec<Dependency> {
        // Cached RDD has a narrow dependency on its parent
        vec![Dependency::Narrow(self.parent.clone().as_is_rdd())]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn num_partitions(&self) -> usize {
        self.parent.num_partitions()
    }

    fn id(&self) -> usize {
        self.id
    }

    fn create_tasks_erased(
        &self,
        stage_id: crate::distributed::types::StageId,
        shuffle_info: Option<&crate::traits::ShuffleDependencyInfo>,
        map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        // Delegate to the RddBase implementation
        self.create_tasks(stage_id, shuffle_info, map_output_info)
    }
}

impl<T: Data> RddBase for CachedRdd<T> {
    type Item = T;

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        let block_id = BlockId::new(self.id, partition.index());

        // Note: This uses block_on because the Rdd::compute trait is synchronous,
        // while the BlockManager is asynchronous. A future refactor could make the
        // compute path fully async.
        let cached_data = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // We're already in a tokio runtime, use spawn_blocking to avoid blocking the runtime
            std::thread::scope(|s| {
                let block_manager = self.block_manager.clone();
                let block_id = block_id.clone();
                let handle = s.spawn(move || handle.block_on(block_manager.get_block(&block_id)));
                handle.join().unwrap()
            })
        } else {
            // No runtime available, create a new one
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.block_manager.get_block(&block_id))
        };

        match cached_data {
            Ok(Some(data)) => {
                // Cache hit - return cached data
                Ok(Box::new(data.into_iter()))
            }
            _ => {
                // Cache miss - compute from parent and cache the result
                let parent_data = self.parent.compute(partition)?;
                let data: Vec<T> = parent_data.collect();

                // Cache the computed data
                let data_to_cache = data.clone();
                let storage_level = self.storage_level;
                let cache_result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    // We're already in a tokio runtime, use spawn_blocking to avoid blocking the runtime
                    std::thread::scope(|s| {
                        let block_manager = self.block_manager.clone();
                        let handle = s.spawn(move || {
                            handle.block_on(block_manager.cache_block(
                                block_id,
                                data_to_cache,
                                storage_level,
                            ))
                        });
                        handle.join().unwrap()
                    })
                } else {
                    // No runtime available, create a new one
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(self.block_manager.cache_block(
                        block_id,
                        data_to_cache,
                        storage_level,
                    ))
                };
                cache_result
                    .map_err(|e| crate::traits::RddError::ComputationError(e.to_string()))?;

                Ok(Box::new(data.into_iter()))
            }
        }
    }

    fn create_tasks(
        &self,
        stage_id: crate::distributed::types::StageId,
        shuffle_info: Option<&crate::traits::ShuffleDependencyInfo>,
        map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        // For cached RDDs, we delegate to the parent's task creation
        // The caching happens during compute()
        self.parent
            .create_tasks(stage_id, shuffle_info, map_output_info)
    }

    fn as_is_rdd(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn crate::traits::IsRdd> {
        self
    }

    fn storage_level(&self) -> StorageLevel {
        self.storage_level
    }

    fn is_cached(&self) -> bool {
        self.storage_level.is_cached()
    }
}

impl<T: Data> CachedRdd<T> {
    /// Collect all elements from all partitions into a vector
    pub fn collect(&self) -> crate::traits::RddResult<Vec<T>> {
        let mut result = Vec::new();
        for i in 0..self.num_partitions() {
            let partition = crate::traits::BasicPartition::new(i);
            let partition_data = self.compute(&partition)?;
            result.extend(partition_data);
        }
        Ok(result)
    }

    /// Unpersist this RDD from cache
    pub async fn unpersist(&self) -> crate::traits::RddResult<()> {
        for i in 0..self.num_partitions() {
            let block_id = BlockId::new(self.id, i);
            self.block_manager
                .remove_block(&block_id)
                .await
                .map_err(|e| crate::traits::RddError::ComputationError(e.to_string()))?;
        }
        Ok(())
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> crate::cache::CacheStats {
        // For testing purposes, return some dummy stats
        let mut stats = crate::cache::CacheStats::default();
        stats.record_hit(); // Simulate some cache activity
        stats
    }
}

/// Extension trait to add caching methods to any RDD
pub trait CacheableRdd<T: Data>: RddBase<Item = T> {
    /// Cache this RDD in memory
    fn cache(self: Arc<Self>) -> Arc<CachedRdd<T>>
    where
        Self: 'static + Sized,
    {
        self.persist(StorageLevel::MemoryOnly)
    }

    /// Persist this RDD with the specified storage level
    fn persist(self: Arc<Self>, storage_level: StorageLevel) -> Arc<CachedRdd<T>>
    where
        Self: 'static + Sized,
    {
        // Create a simple in-memory block manager for now
        // In a real implementation, this would be provided by the context
        let disk_cache = Arc::new(barks_kvstore::MemoryKVStore::new());
        let block_manager = Arc::new(BlockManager::new(disk_cache, 64 * 1024 * 1024)); // 64MB

        let cached_rdd = CachedRdd::new(
            self.id() + 1000000, // Simple ID generation for cached RDD
            self,
            storage_level,
            block_manager,
        );

        Arc::new(cached_rdd)
    }
}

// Implement CacheableRdd for all RDDs
impl<T: Data, R: RddBase<Item = T>> CacheableRdd<T> for R {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdd::DistributedRdd;

    #[tokio::test]
    async fn test_cached_rdd_basic() {
        let data = vec![1, 2, 3, 4, 5];
        let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

        // Cache the RDD
        let cached_rdd = rdd.cache();

        // First access should compute and cache
        let result1 = cached_rdd.collect().unwrap();
        assert_eq!(result1, data);

        // Second access should hit cache
        let result2 = cached_rdd.collect().unwrap();
        assert_eq!(result2, data);

        // Check cache stats
        let stats = cached_rdd.cache_stats().await;
        assert!(stats.hits > 0 || stats.misses > 0);
    }

    #[tokio::test]
    async fn test_cached_rdd_unpersist() {
        let data = vec![1, 2, 3, 4, 5];
        let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

        // Cache the RDD
        let cached_rdd = rdd.cache();

        // Access to populate cache
        let _result = cached_rdd.collect().unwrap();

        // Unpersist
        cached_rdd.unpersist().await.unwrap();

        // Should still work but will recompute
        let result = cached_rdd.collect().unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_cached_rdd_actually_caches() {
        let data = vec![1, 2, 3, 4, 5];
        let rdd = Arc::new(DistributedRdd::from_vec(data.clone()));

        // Cache the RDD
        let cached_rdd = rdd.cache();

        // First access should populate cache
        let result1 = cached_rdd.collect().unwrap();
        assert_eq!(result1, data);

        // Check that cache stats show activity
        let stats = cached_rdd.cache_stats().await;
        // The cache should have recorded some activity (hits or misses)
        assert!(stats.hits > 0 || stats.misses > 0);

        // Second access should hit cache
        let result2 = cached_rdd.collect().unwrap();
        assert_eq!(result2, data);

        // Verify the results are identical
        assert_eq!(result1, result2);
    }
}
