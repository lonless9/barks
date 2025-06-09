//! RDD that represents a repartition operation using shuffle.

use crate::shuffle::Partitioner;
use crate::traits::{
    Data, Dependency, IsRdd, Partition, PartitionerType, RddBase, RddError, RddResult,
    ShuffleDependencyInfo,
};
use std::sync::Arc;

/// RepartitionRdd redistributes data across a different number of partitions using shuffle.
/// This is used when increasing the number of partitions or when a custom partitioner is needed.
#[derive(Clone, Debug)]
pub struct RepartitionRdd<T: Data> {
    id: usize,
    parent: Arc<dyn RddBase<Item = T>>,
    partitioner: Arc<dyn Partitioner<T>>,
}

impl<T: Data> RepartitionRdd<T> {
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

impl<T: Data> crate::traits::IsRdd for RepartitionRdd<T>
where
    T: std::hash::Hash + Eq,
{
    fn dependencies(&self) -> Vec<Dependency> {
        // Repartition creates a shuffle dependency on the parent RDD
        let shuffle_info = ShuffleDependencyInfo {
            shuffle_id: self.id,
            num_partitions: self.partitioner.num_partitions(),
            partitioner_type: PartitionerType::Hash {
                num_partitions: self.partitioner.num_partitions(),
                seed: 0,
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

impl<T: Data> RddBase for RepartitionRdd<T>
where
    T: std::hash::Hash + Eq,
{
    type Item = T;

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        // For local execution, we simulate the shuffle by computing the parent RDD
        // partitions, re-partitioning the data, and then returning the data
        // for the requested partition.

        let partition_index = partition.index();
        let num_partitions = self.num_partitions();

        // 1. Create one buffer for each output partition.
        let mut partition_buffers: Vec<Vec<T>> = (0..num_partitions).map(|_| Vec::new()).collect();

        // 2. Compute all parent partitions and distribute elements to output partitions.
        for parent_partition_index in 0..self.parent.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(parent_partition_index);
            let parent_data = self.parent.compute(&parent_partition)?;

            for element in parent_data {
                let target_partition = self.partitioner.get_partition(&element) as usize;
                if target_partition < num_partitions {
                    partition_buffers[target_partition].push(element);
                }
            }
        }

        // 3. Get the elements for the requested partition.
        let partition_data: Vec<T> = partition_buffers
            .into_iter()
            .nth(partition_index)
            .unwrap_or_default();

        Ok(Box::new(partition_data.into_iter()))
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
                    "Could not find map outputs for repartition shuffle".to_string(),
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
                let task = crate::distributed::task::RepartitionTask::<String>::new(
                    self.id as u32,
                    partition_index as u32,
                    map_output_locations.clone(),
                );
                tasks.push(Box::new(task) as Box<dyn crate::distributed::task::Task>);
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                let task = crate::distributed::task::RepartitionTask::<i32>::new(
                    self.id as u32,
                    partition_index as u32,
                    map_output_locations.clone(),
                );
                tasks.push(Box::new(task) as Box<dyn crate::distributed::task::Task>);
            } else {
                return Err(RddError::ShuffleError(format!(
                    "Unsupported type for repartition operation: {:?}",
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

impl<T: Data> RepartitionRdd<T>
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

/// Simple round-robin partitioner for repartitioning
#[derive(Debug, Clone)]
pub struct RoundRobinPartitioner {
    num_partitions: u32,
}

impl RoundRobinPartitioner {
    pub fn new(num_partitions: u32) -> Self {
        Self { num_partitions }
    }
}

impl<T> Partitioner<T> for RoundRobinPartitioner
where
    T: std::hash::Hash,
{
    fn get_partition(&self, key: &T) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.num_partitions as u64) as u32
    }

    fn num_partitions(&self) -> u32 {
        self.num_partitions
    }
}

/// Random partitioner for better data distribution
#[derive(Debug, Clone)]
pub struct RandomPartitioner {
    num_partitions: u32,
    seed: u64,
}

impl RandomPartitioner {
    pub fn new(num_partitions: u32, seed: u64) -> Self {
        Self {
            num_partitions,
            seed,
        }
    }
}

impl<T> Partitioner<T> for RandomPartitioner
where
    T: std::hash::Hash,
{
    fn get_partition(&self, key: &T) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.seed.hash(&mut hasher);
        key.hash(&mut hasher);
        (hasher.finish() % self.num_partitions as u64) as u32
    }

    fn num_partitions(&self) -> u32 {
        self.num_partitions
    }
}
