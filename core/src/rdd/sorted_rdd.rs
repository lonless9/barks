//! RDD that represents a sort operation by key.

use crate::shuffle::{Partitioner, RangePartitioner};
use crate::traits::{
    Data, Dependency, Partition, PartitionerType, RddBase, RddResult, ShuffleDependencyInfo,
};
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
        // For local execution, we simulate the sort by collecting all data
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
            // Use the RangePartitioner to determine the correct partition
            let key_partition = self.partitioner.get_partition_for(&key) as usize;

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
        vec![Dependency::Shuffle(ShuffleDependencyInfo {
            shuffle_id: self.id,
            parent_rdd_id: self.parent.id(),
            num_partitions: self.partitioner.num_partitions(),
            partitioner_type: PartitionerType::Range {
                num_partitions: self.partitioner.num_partitions(),
            },
        })]
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

        // For sorted RDDs, we need to collect partitions in the right order
        // For ascending sort: collect partitions 0, 1, 2, ...
        // For descending sort: collect partitions ..., 2, 1, 0
        let partition_indices: Vec<usize> = if self.ascending {
            (0..self.num_partitions()).collect()
        } else {
            (0..self.num_partitions()).rev().collect()
        };

        for i in partition_indices {
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
    // Sample data from all partitions to get a representative set of keys
    let mut sampled_keys = Vec::new();
    let num_partitions = rdd.num_partitions();

    if num_partitions == 0 || sample_size == 0 {
        return Vec::new();
    }

    // Calculate how many samples to take from each partition
    let samples_per_partition = (sample_size / num_partitions).max(1);

    for i in 0..num_partitions {
        let partition = crate::traits::BasicPartition::new(i);
        if let Ok(partition_data) = rdd.compute(&partition) {
            for (count, (key, _)) in partition_data.enumerate() {
                if count >= samples_per_partition {
                    break;
                }
                sampled_keys.push(key);
            }
        }
    }

    // Sort and deduplicate the sampled keys
    sampled_keys.sort();
    sampled_keys.dedup();

    // If we have too many samples, take a subset
    if sampled_keys.len() > sample_size {
        let step = sampled_keys.len() / sample_size;
        sampled_keys = sampled_keys.into_iter().step_by(step.max(1)).collect();
    }

    sampled_keys
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
    // Sample keys from the parent RDD for range partitioning
    // Use a reasonable default sample size if none provided
    let effective_sample_size = if sample_size == 0 {
        (num_partitions as usize * 20).max(100) // 20 samples per partition, minimum 100
    } else {
        sample_size
    };

    let sample_keys = sample_keys_for_sorting(&parent, effective_sample_size);

    // Create the sorted RDD with the sampled keys
    SortedRdd::from_sample(id, parent, num_partitions, sample_keys, ascending)
}
