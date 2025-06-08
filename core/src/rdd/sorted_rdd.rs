//! RDD that represents a sort operation by key.

use crate::shuffle::{Partitioner, RangePartitioner};
use crate::traits::{Data, Dependency, Partition, RddBase, RddResult};
use std::sync::Arc;

/// SortedRdd represents an RDD that has been sorted by key.
/// It uses a RangePartitioner to ensure global ordering across partitions.
#[derive(Clone, Debug)]
pub struct SortedRdd<K: Data, V: Data> {
    id: usize,
    parent: Arc<dyn RddBase<Item = (K, V)>>,
    partitioner: Arc<RangePartitioner<K>>,
    ascending: bool,
}

impl<K: Data, V: Data> SortedRdd<K, V>
where
    K: Ord + std::fmt::Debug,
{
    pub fn new(
        id: usize,
        parent: Arc<dyn RddBase<Item = (K, V)>>,
        partitioner: Arc<RangePartitioner<K>>,
        ascending: bool,
    ) -> Self {
        Self {
            id,
            parent,
            partitioner,
            ascending,
        }
    }

    /// Create a SortedRdd by sampling the parent RDD to determine range bounds.
    /// This is the typical way to create a sorted RDD in practice.
    pub fn from_sample(
        id: usize,
        parent: Arc<dyn RddBase<Item = (K, V)>>,
        num_partitions: u32,
        sample_keys: Vec<K>,
        ascending: bool,
    ) -> Self {
        let partitioner = Arc::new(RangePartitioner::from_sample(num_partitions, sample_keys));
        Self::new(id, parent, partitioner, ascending)
    }
}

impl<K: Data, V: Data> RddBase for SortedRdd<K, V>
where
    K: Ord + std::fmt::Debug + std::hash::Hash,
{
    type Item = (K, V);

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        // TODO For local execution, we simulate the sort by collecting all data
        // and sorting it. In a distributed environment, this would use
        // range partitioning and fetch shuffle blocks.

        let partition_index = partition.index();

        // Get all data from parent partitions (simulating shuffle read)
        let mut all_data = Vec::new();
        for i in 0..self.parent.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.parent.compute(&parent_partition)?;
            all_data.extend(parent_data);
        }

        // Filter data that belongs to this partition based on range partitioning
        let mut partition_data: Vec<(K, V)> = Vec::new();

        for (key, value) in all_data {
            // TODO Use hash partitioning here instead of range partitioning
            // This would use the RangePartitioner
            let key_partition = {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                (hasher.finish() % self.partitioner.num_partitions() as u64) as usize
            };

            if key_partition == partition_index {
                partition_data.push((key, value));
            }
        }

        // Sort the data within this partition
        if self.ascending {
            partition_data.sort_by(|a, b| a.0.cmp(&b.0));
        } else {
            partition_data.sort_by(|a, b| b.0.cmp(&a.0));
        }

        Ok(Box::new(partition_data.into_iter()))
    }

    fn num_partitions(&self) -> usize {
        self.partitioner.num_partitions() as usize
    }

    fn dependencies(&self) -> Vec<Dependency> {
        // Sort creates a shuffle dependency on the parent RDD
        vec![Dependency::Shuffle(Arc::new(()))]
    }

    fn id(&self) -> usize {
        self.id
    }
}

impl<K: Data, V: Data> SortedRdd<K, V>
where
    K: Ord + std::fmt::Debug + std::hash::Hash,
{
    /// Collect all elements from all partitions into a vector
    pub fn collect(&self) -> crate::traits::RddResult<Vec<(K, V)>> {
        let mut result = Vec::new();
        for i in 0..self.num_partitions() {
            let partition = crate::traits::BasicPartition::new(i);
            let partition_data = self.compute(&partition)?;
            result.extend(partition_data);
        }
        Ok(result)
    }
}

/// Helper function to sample keys from an RDD for range partitioning.
/// This would typically be called before creating a SortedRdd.
pub fn sample_keys_for_sorting<K, V>(
    rdd: &Arc<dyn RddBase<Item = (K, V)>>,
    sample_size: usize,
) -> Vec<K>
where
    K: Data + Ord + Clone,
    V: Data,
{
    // In a real implementation, this would:
    // 1. Sample a fraction of the RDD data
    // 2. Extract keys from the sampled data
    // 3. Return a representative sample for range partitioning

    // For now, return an empty vector as a placeholder
    // The actual implementation would involve running a sampling job
    Vec::new()
}

/// Utility to create a sorted RDD with automatic sampling
pub fn create_sorted_rdd<K, V>(
    id: usize,
    parent: Arc<dyn RddBase<Item = (K, V)>>,
    num_partitions: u32,
    ascending: bool,
    sample_size: usize,
) -> SortedRdd<K, V>
where
    K: Data + Ord + Clone + std::fmt::Debug,
    V: Data,
{
    // Sample keys from the parent RDD
    let sample_keys = sample_keys_for_sorting(&parent, sample_size);

    // Create the sorted RDD with the sampled keys
    SortedRdd::from_sample(id, parent, num_partitions, sample_keys, ascending)
}
