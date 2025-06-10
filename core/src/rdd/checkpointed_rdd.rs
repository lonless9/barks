//! RDD implementation that provides checkpointing functionality
//!
//! Checkpointing allows RDDs to save their computed partitions to persistent storage,
//! truncating the lineage and improving fault tolerance for long-running jobs.

use crate::traits::{Data, Dependency, IsRdd, Partition, RddBase, RddError, RddResult};
use barks_kvstore::{KVStore, LevelDBKVStore};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

/// A partition identifier for checkpointed data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointPartition {
    index: usize,
    checkpoint_id: String,
}

impl CheckpointPartition {
    pub fn new(index: usize, checkpoint_id: String) -> Self {
        Self {
            index,
            checkpoint_id,
        }
    }
}

impl Partition for CheckpointPartition {
    fn index(&self) -> usize {
        self.index
    }

    fn id(&self) -> String {
        format!("checkpoint_{}_{}", self.checkpoint_id, self.index)
    }
}

/// RDD that represents checkpointed data
///
/// This RDD type stores computed partition data persistently and acts as a new source
/// in the RDD lineage, effectively truncating the dependency graph.
pub struct CheckpointedRdd<T: Data> {
    id: usize,
    checkpoint_id: String,
    num_partitions: usize,
    /// Persistent storage for checkpointed data
    storage: LevelDBKVStore<usize, Vec<T>>,
    /// Optional reference to the original RDD for lazy checkpointing
    original_rdd: Option<Arc<dyn RddBase<Item = T>>>,
    /// Whether the checkpoint has been materialized
    is_materialized: Arc<std::sync::Mutex<bool>>,
}

impl<T: Data> CheckpointedRdd<T> {
    /// Create a new checkpointed RDD
    pub fn new(
        id: usize,
        checkpoint_id: String,
        num_partitions: usize,
        storage_path: PathBuf,
        original_rdd: Option<Arc<dyn RddBase<Item = T>>>,
    ) -> RddResult<Self> {
        let storage = LevelDBKVStore::new(storage_path)
            .map_err(|e| RddError::CheckpointError(e.to_string()))?;

        Ok(Self {
            id,
            checkpoint_id,
            num_partitions,
            storage,
            original_rdd,
            is_materialized: Arc::new(std::sync::Mutex::new(false)),
        })
    }

    /// Check if the checkpoint has been materialized
    pub fn is_materialized(&self) -> bool {
        *self.is_materialized.lock().unwrap()
    }

    /// Materialize the checkpoint by computing and storing all partitions
    pub async fn materialize(&self) -> RddResult<()> {
        if self.is_materialized() {
            return Ok(());
        }

        let original_rdd = self.original_rdd.as_ref().ok_or_else(|| {
            RddError::CheckpointError("No original RDD to checkpoint".to_string())
        })?;

        info!(
            "Materializing checkpoint {} with {} partitions",
            self.checkpoint_id, self.num_partitions
        );

        // Compute and store each partition
        for partition_idx in 0..self.num_partitions {
            let partition = crate::traits::BasicPartition::new(partition_idx);
            let partition_data = original_rdd.compute(&partition)?;

            // Collect the iterator into a vector for storage
            let data: Vec<T> = partition_data.collect();
            // Store in LevelDB
            self.storage
                .put(&partition_idx, data)
                .await
                .map_err(|e| RddError::CheckpointError(e.to_string()))?;
        }

        // Mark as materialized
        *self.is_materialized.lock().unwrap() = true;
        info!(
            "Successfully materialized checkpoint {}",
            self.checkpoint_id
        );

        Ok(())
    }

    /// Load partition data from checkpoint storage
    async fn load_partition(&self, partition_idx: usize) -> RddResult<Vec<T>> {
        self.storage
            .get(&partition_idx)
            .await
            .map_err(|e| RddError::CheckpointError(e.to_string()))?
            .ok_or_else(|| {
                RddError::CheckpointError(format!(
                    "Partition {} not found in checkpoint",
                    partition_idx
                ))
            })
    }
}

impl<T: Data> IsRdd for CheckpointedRdd<T> {
    fn id(&self) -> usize {
        self.id
    }

    fn dependencies(&self) -> Vec<Dependency> {
        // Checkpointed RDDs have no dependencies - they are new sources
        vec![]
    }

    fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    fn as_any(&self) -> &dyn Any {
        self
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
    ) -> RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        self.create_tasks(stage_id, shuffle_info, map_output_info)
    }
}

impl<T: Data> RddBase for CheckpointedRdd<T> {
    type Item = T;

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        if !self.is_materialized() {
            return Err(RddError::CheckpointError(
                "Checkpoint not materialized. Call materialize() first.".to_string(),
            ));
        }
        let partition_idx = partition.index();

        // The compute function is synchronous, but storage is async.
        // We must block on the future to get the data.
        // This pattern is used elsewhere in the codebase (e.g., CachedRdd).
        let data = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // We're already in a tokio runtime.
            std::thread::scope(|s| {
                let handle = s.spawn(move || handle.block_on(self.load_partition(partition_idx)));
                handle.join().unwrap()
            })
        } else {
            // No runtime available, create a new one for this operation.
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.load_partition(partition_idx))
        }?;

        Ok(Box::new(data.into_iter()))
    }

    fn partitions(&self) -> Vec<Arc<dyn Partition>> {
        (0..self.num_partitions)
            .map(|i| {
                Arc::new(CheckpointPartition::new(i, self.checkpoint_id.clone()))
                    as Arc<dyn Partition>
            })
            .collect()
    }

    fn create_tasks(
        &self,
        _stage_id: crate::distributed::types::StageId,
        _shuffle_info: Option<&crate::traits::ShuffleDependencyInfo>,
        _map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        // For now, return an error as checkpointed RDDs should be handled specially
        // In a full implementation, this would create tasks that read from checkpoint storage
        Err(RddError::NotImplemented(
            "CheckpointedRdd task creation not yet implemented".to_string(),
        ))
    }

    fn as_is_rdd(self: Arc<Self>) -> Arc<dyn IsRdd> {
        self
    }
}

impl<T: Data> std::fmt::Debug for CheckpointedRdd<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointedRdd")
            .field("id", &self.id)
            .field("checkpoint_id", &self.checkpoint_id)
            .field("num_partitions", &self.num_partitions)
            .field("is_materialized", &self.is_materialized())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdd::DistributedRdd;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_checkpointed_rdd_creation() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_path_buf();

        let original_rdd: Arc<dyn RddBase<Item = i32>> =
            Arc::new(DistributedRdd::from_vec(vec![1, 2, 3, 4, 5]));

        let checkpointed = CheckpointedRdd::new(
            1,
            "test_checkpoint".to_string(),
            2,
            storage_path,
            Some(original_rdd),
        )
        .unwrap();

        assert_eq!(checkpointed.id(), 1);
        assert_eq!(checkpointed.num_partitions(), 2);
        assert!(!checkpointed.is_materialized());
        assert_eq!(checkpointed.dependencies().len(), 0);
    }

    #[tokio::test]
    async fn test_checkpoint_materialization() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_path_buf();

        let original_rdd: Arc<dyn RddBase<Item = i32>> = Arc::new(
            DistributedRdd::from_vec_with_partitions(vec![1, 2, 3, 4], 2),
        );

        let checkpointed = CheckpointedRdd::new(
            1,
            "test_checkpoint".to_string(),
            2,
            storage_path,
            Some(original_rdd),
        )
        .unwrap();

        // Materialize the checkpoint
        checkpointed.materialize().await.unwrap();
        assert!(checkpointed.is_materialized());

        // Test that we can compute from the checkpoint
        let partition = crate::traits::BasicPartition::new(0);
        let result = checkpointed.compute(&partition).unwrap();
        let data: Vec<i32> = result.collect();
        assert!(!data.is_empty());
    }
}
