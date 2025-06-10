//! RDD that represents a sort operation by key.

use crate::shuffle::{Partitioner, RangePartitioner};
use crate::traits::{
    Data, Dependency, IsRdd, Partition, PartitionerType, RddBase, RddResult, ShuffleDependencyInfo,
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

impl<K: Data, V: Data> crate::traits::IsRdd for SortedRdd<K, V>
where
    K: Ord + std::fmt::Debug + std::hash::Hash,
{
    fn dependencies(&self) -> Vec<Dependency> {
        // Sort creates a shuffle dependency on the parent RDD
        vec![Dependency::Shuffle(
            self.parent.clone().as_is_rdd(),
            ShuffleDependencyInfo {
                shuffle_id: self.id,
                num_partitions: self.partitioner.num_partitions(),
                partitioner_type: PartitionerType::Range {
                    num_partitions: self.partitioner.num_partitions(),
                },
            },
        )]
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn num_partitions(&self) -> usize {
        self.partitioner.num_partitions() as usize
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
        // from the parent RDD, partitioning it, and then sorting the
        // requested partition. This is a local shuffle simulation.

        let partition_index = partition.index();

        // 1. Materialize and partition the parent RDD's data locally.
        let mut shuffled_data: Vec<Vec<(K, V)>> = vec![Vec::new(); self.num_partitions()];

        for i in 0..self.parent.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.parent.compute(&parent_partition)?;

            for (key, value) in parent_data {
                let p_idx = self.partitioner.get_partition(&key) as usize;
                shuffled_data[p_idx].push((key, value));
            }
        }

        // 2. Get the data for the requested partition.
        // We need to handle the case where the index might be out of bounds, though
        // the scheduler should prevent this.
        let mut partition_data = shuffled_data
            .into_iter()
            .nth(partition_index)
            .unwrap_or_default();

        // 3. Sort the data within this partition.
        if self.ascending {
            partition_data.sort_by(|a, b| a.0.cmp(&b.0));
        } else {
            partition_data.sort_by(|a, b| b.0.cmp(&a.0));
        }

        Ok(Box::new(partition_data.into_iter()))
    }

    fn create_tasks(
        &self,
        _stage_id: crate::distributed::types::StageId,
        _shuffle_info: Option<&crate::traits::ShuffleDependencyInfo>, // This can be ignored here
        map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        let map_output_info = map_output_info.ok_or_else(|| {
            crate::traits::RddError::TaskCreationError(
                "SortedRdd requires map output info for shuffle dependency".to_string(),
            )
        })?;

        // We expect one shuffle dependency (the parent RDD)
        if map_output_info.len() != 1 {
            return Err(crate::traits::RddError::TaskCreationError(format!(
                "SortedRdd expects exactly 1 shuffle dependency, got {}",
                map_output_info.len()
            )));
        }

        // Extract map locations from the first (and only) shuffle dependency
        let parent_map_info = &map_output_info[0];
        let map_locations: Vec<(String, u32)> = parent_map_info
            .iter()
            .enumerate()
            .map(|(map_id, (_map_status, exec_info))| {
                let shuffle_addr = format!("{}:{}", exec_info.host, exec_info.shuffle_port);
                (shuffle_addr, map_id as u32)
            })
            .collect();

        // Use the RDD ID as shuffle ID for sorting
        let shuffle_id = self.id as u32;

        // Try to create tasks for supported type combinations
        self.create_typed_sort_tasks(shuffle_id, &map_locations)
    }

    fn as_is_rdd(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn crate::traits::IsRdd> {
        self
    }
}

impl<K: Data, V: Data> SortedRdd<K, V>
where
    K: Ord + std::fmt::Debug + std::hash::Hash,
{
    /// Helper method to create tasks for supported type combinations
    fn create_typed_sort_tasks(
        &self,
        shuffle_id: u32,
        map_locations: &[(String, u32)],
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        use crate::distributed::task::SortTask;

        let mut tasks: Vec<Box<dyn crate::distributed::task::Task>> = Vec::new();

        // Use TypeId to determine the concrete types at runtime
        use std::any::TypeId;
        let k_type = TypeId::of::<K>();
        let v_type = TypeId::of::<V>();

        // Handle String -> i32 (most common case)
        if k_type == TypeId::of::<String>() && v_type == TypeId::of::<i32>() {
            for i in 0..self.num_partitions() {
                let task = SortTask::<String, i32>::new(
                    shuffle_id,
                    i as u32,
                    map_locations.to_vec(),
                    self.ascending,
                );
                tasks.push(Box::new(task));
            }
            return Ok(tasks);
        }

        // Handle i32 -> String
        if k_type == TypeId::of::<i32>() && v_type == TypeId::of::<String>() {
            for i in 0..self.num_partitions() {
                let task = SortTask::<i32, String>::new(
                    shuffle_id,
                    i as u32,
                    map_locations.to_vec(),
                    self.ascending,
                );
                tasks.push(Box::new(task));
            }
            return Ok(tasks);
        }

        // If no supported type combination is found, return an error
        Err(crate::traits::RddError::ContextError(format!(
            "Task creation for SortedRdd<{}, {}> is not supported yet. \
            Supported combinations: (String, i32), (i32, String)",
            std::any::type_name::<K>(),
            std::any::type_name::<V>()
        )))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdd::DistributedRdd;
    use crate::shuffle::RangePartitioner;
    use crate::traits::IsRdd;

    #[test]
    fn test_sample_keys_for_sorting() {
        let data: Vec<(i32, String)> = (0..100).map(|i| (i, "a".to_string())).collect();
        let rdd = Arc::new(DistributedRdd::from_vec_with_partitions(data, 10));
        let rdd_trait: Arc<dyn RddBase<Item = (i32, String)>> = rdd;
        let samples = sample_keys_for_sorting(&rdd_trait, 10);
        assert!(samples.len() <= 10);
        // With this data, samples should be roughly evenly spaced
        println!("Samples: {:?}", samples);
        assert!(samples[0] >= 0);
        assert!(samples.last().unwrap() < &100);
    }

    #[test]
    fn test_sorted_rdd_local_compute() {
        let data: Vec<(i32, &str)> = vec![
            (5, "e"),
            (2, "b"),
            (8, "h"),
            (1, "a"),
            (9, "i"),
            (4, "d"),
            (7, "g"),
            (3, "c"),
            (6, "f"),
        ];
        let owned_data: Vec<(i32, String)> =
            data.into_iter().map(|(k, v)| (k, v.to_string())).collect();

        let parent_rdd = Arc::new(DistributedRdd::from_vec_with_partitions(owned_data, 2));

        // Manually create partitioner for predictable testing
        let range_bounds = vec![5]; // Partition 0: keys <= 5, Partition 1: keys > 5
        let partitioner = Arc::new(RangePartitioner::new(2, range_bounds));

        // Test ascending sort
        let sorted_rdd_asc = SortedRdd::new(1, parent_rdd.clone(), partitioner.clone(), true);
        let result_asc = sorted_rdd_asc.collect().unwrap();
        let expected_asc: Vec<(i32, String)> = vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (4, "d".into()),
            (5, "e".into()),
            (6, "f".into()),
            (7, "g".into()),
            (8, "h".into()),
            (9, "i".into()),
        ];
        assert_eq!(result_asc, expected_asc);

        // Test descending sort
        let sorted_rdd_desc = SortedRdd::new(2, parent_rdd, partitioner, false);
        let result_desc = sorted_rdd_desc.collect().unwrap();
        let mut expected_desc = expected_asc;
        expected_desc.reverse();
        assert_eq!(result_desc, expected_desc);
    }

    #[test]
    fn test_sorted_rdd_dependencies() {
        let parent_rdd: Arc<DistributedRdd<(String, i32)>> =
            Arc::new(DistributedRdd::from_vec(vec![]));
        let partitioner = Arc::new(RangePartitioner::from_sample(3, vec!["m".into()]));
        let sorted_rdd = SortedRdd::new(123, parent_rdd.clone(), partitioner, true);

        let deps = sorted_rdd.dependencies();
        assert_eq!(deps.len(), 1);

        match &deps[0] {
            Dependency::Shuffle(rdd, info) => {
                assert_eq!(rdd.id(), parent_rdd.id());
                assert_eq!(info.shuffle_id, 123);
                assert_eq!(info.num_partitions, 3);
                assert!(matches!(
                    info.partitioner_type,
                    PartitionerType::Range { .. }
                ));
            }
            _ => panic!("Expected a ShuffleDependency"),
        }
    }

    #[test]
    fn test_create_sorted_rdd_helper() {
        let parent_rdd: Arc<DistributedRdd<(i32, String)>> =
            Arc::new(DistributedRdd::from_vec(vec![(1, "test".to_string())]));
        let parent_trait: Arc<dyn RddBase<Item = (i32, String)>> = parent_rdd;
        let sorted_rdd = create_sorted_rdd(1, parent_trait, 4, true, 100);

        assert_eq!(sorted_rdd.num_partitions(), 4);
        assert!(sorted_rdd.ascending);
    }
}
