//! RDD that represents a global distinct operation using shuffle.

use crate::shuffle::{Aggregator, Partitioner};
use crate::traits::{
    Data, Dependency, IsRdd, Partition, PartitionerType, RddBase, RddError, RddResult,
    ShuffleDependencyInfo,
};
use std::collections::HashSet;
use std::sync::Arc;

/// DistinctRdd performs global deduplication using shuffle operations.
/// It ensures that duplicate elements are removed across all partitions.
#[derive(Clone, Debug)]
pub struct DistinctRdd<T: Data> {
    id: usize,
    parent: Arc<dyn RddBase<Item = T>>,
    partitioner: Arc<dyn Partitioner<T>>,
}

impl<T: Data> DistinctRdd<T>
where
    T: std::hash::Hash + Eq,
{
    pub fn new(
        id: usize,
        parent: Arc<dyn RddBase<Item = T>>,
        partitioner: Arc<dyn Partitioner<T>>,
    ) -> Self {
        Self {
            id,
            parent,
            partitioner,
        }
    }
}

impl<T: Data> crate::traits::IsRdd for DistinctRdd<T>
where
    T: std::hash::Hash + Eq,
{
    fn dependencies(&self) -> Vec<Dependency> {
        // Distinct creates a shuffle dependency on the parent RDD
        let shuffle_info = ShuffleDependencyInfo {
            shuffle_id: self.id,
            num_partitions: self.partitioner.num_partitions(),
            partitioner_type: PartitionerType::Hash {
                num_partitions: self.partitioner.num_partitions(),
                seed: 42,
            },
        };
        vec![Dependency::Shuffle(
            self.parent.clone().as_is_rdd(),
            shuffle_info,
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

impl<T: Data> RddBase for DistinctRdd<T>
where
    T: std::hash::Hash + Eq,
{
    type Item = T;

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        // For local execution, we simulate the shuffle by computing the parent RDD
        // partitions, re-partitioning the data, and then applying deduplication
        // to the data for the requested partition.

        let partition_index = partition.index();
        let num_partitions = self.num_partitions();

        // 1. Create one set for each output partition to collect unique elements.
        let mut partition_sets: Vec<HashSet<T>> =
            (0..num_partitions).map(|_| HashSet::new()).collect();

        // 2. Compute all parent partitions and distribute elements to output partitions.
        for parent_partition_index in 0..self.parent.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(parent_partition_index);
            let parent_data = self.parent.compute(&parent_partition)?;

            for element in parent_data {
                let target_partition = self.partitioner.get_partition(&element) as usize;
                if target_partition < num_partitions {
                    partition_sets[target_partition].insert(element);
                }
            }
        }

        // 3. Get the unique elements for the requested partition.
        let unique_elements: Vec<T> = partition_sets
            .into_iter()
            .nth(partition_index)
            .unwrap_or_default()
            .into_iter()
            .collect();

        Ok(Box::new(unique_elements.into_iter()))
    }

    fn create_tasks(
        &self,
        _stage_id: crate::distributed::types::StageId,
        _shuffle_info: Option<&crate::traits::ShuffleDependencyInfo>,
        map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        let parent_map_info = map_output_info
            .and_then(|infos| infos.first())
            .ok_or_else(|| {
                RddError::ShuffleError(
                    "Could not find map outputs for distinct shuffle".to_string(),
                )
            })?;

        // Convert map output info to the format expected by tasks
        let map_output_locations: Vec<(String, u32)> = parent_map_info
            .iter()
            .enumerate()
            .map(|(map_id, (_, executor_info))| {
                (
                    format!("{}:{}", executor_info.host, executor_info.shuffle_port),
                    map_id as u32,
                )
            })
            .collect();

        let mut tasks = Vec::new();
        for partition_index in 0..self.num_partitions() {
            // Create the appropriate task type based on T
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<String>() {
                let task = crate::distributed::task::DistinctTask::<String>::new(
                    self.id as u32,
                    partition_index as u32,
                    map_output_locations.clone(),
                );
                tasks.push(Box::new(task) as Box<dyn crate::distributed::task::Task>);
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                let task = crate::distributed::task::DistinctTask::<i32>::new(
                    self.id as u32,
                    partition_index as u32,
                    map_output_locations.clone(),
                );
                tasks.push(Box::new(task) as Box<dyn crate::distributed::task::Task>);
            } else {
                return Err(RddError::ShuffleError(format!(
                    "Unsupported type for distinct operation: {:?}",
                    std::any::type_name::<T>()
                )));
            }
        }

        Ok(tasks)
    }

    fn as_is_rdd(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn crate::traits::IsRdd> {
        self
    }
}

impl<T: Data> DistinctRdd<T>
where
    T: std::hash::Hash + Eq,
{
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
}

/// Aggregator for distinct operations that simply collects unique elements
#[derive(Debug)]
pub struct DistinctAggregator<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> DistinctAggregator<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Default for DistinctAggregator<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Data> Aggregator<T, (), HashSet<T>> for DistinctAggregator<T>
where
    T: std::hash::Hash + Eq,
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn create_combiner(&self, _value: ()) -> HashSet<T> {
        HashSet::new()
    }

    fn merge_value(&self, combiner: HashSet<T>, _value: ()) -> HashSet<T> {
        // For distinct, we don't use the value, we just track the key
        combiner
    }

    fn merge_combiners(&self, mut combiner1: HashSet<T>, combiner2: HashSet<T>) -> HashSet<T> {
        combiner1.extend(combiner2);
        combiner1
    }

    fn to_serializable(&self) -> Result<crate::shuffle::SerializableAggregator, String> {
        Err("DistinctAggregator cannot be serialized yet".to_string())
    }
}
