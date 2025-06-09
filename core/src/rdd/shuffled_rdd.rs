//! RDD that represents a shuffle dependency.

use crate::shuffle::{Aggregator, Partitioner};
use crate::traits::{
    Data, Dependency, Partition, PartitionerType, RddBase, RddError, RddResult,
    ShuffleDependencyInfo,
};
use std::any::Any;
use std::sync::Arc;

/// ShuffledRdd is an RDD that has a shuffle dependency on its parent.
/// It is the result of operations like `reduceByKey` and `groupByKey`.
///
/// K: Key type
/// V: Value type of the parent RDD
/// C: Combiner type (output value type)
#[derive(Clone, Debug)]
pub struct ShuffledRdd<K: Data, V: Data, C: Data> {
    id: usize,
    #[allow(dead_code)]
    pub parent: Arc<dyn RddBase<Item = (K, V)>>,
    #[allow(dead_code)]
    pub aggregator: Arc<dyn Aggregator<K, V, C>>,
    pub partitioner: Arc<dyn Partitioner<K>>,
}

impl<K: Data, V: Data, C: Data> ShuffledRdd<K, V, C> {
    pub fn new(
        id: usize,
        parent: Arc<dyn RddBase<Item = (K, V)>>,
        aggregator: Arc<dyn Aggregator<K, V, C>>,
        partitioner: Arc<dyn Partitioner<K>>,
    ) -> Self {
        Self {
            id,
            parent,
            aggregator,
            partitioner,
        }
    }
}

impl<K: Data, V: Data, C: Data> RddBase for ShuffledRdd<K, V, C>
where
    K: std::hash::Hash + Eq,
{
    type Item = (K, C);

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        // For local execution, we simulate the shuffle by computing the parent RDD
        // partitions, re-partitioning the data, and then applying the aggregator
        // to the data for the requested partition. This simulates a local shuffle
        // with map-side combine.

        let partition_index = partition.index();
        let num_partitions = self.num_partitions();

        // 1. Create one combiner map for each output partition.
        let mut combiners: Vec<std::collections::HashMap<K, C>> = (0..num_partitions)
            .map(|_| std::collections::HashMap::new())
            .collect();

        // 2. Iterate through parent partitions and apply map-side combine logic.
        for i in 0..self.parent.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.parent.compute(&parent_partition)?;

            for (key, value) in parent_data {
                let p_idx = self.partitioner.get_partition(&key) as usize;
                let combiner_map = &mut combiners[p_idx];

                match combiner_map.entry(key.clone()) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        let new_combiner = self.aggregator.merge_value(e.get().clone(), value);
                        e.insert(new_combiner);
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let new_combiner = self.aggregator.create_combiner(value);
                        e.insert(new_combiner);
                    }
                }
            }
        }

        // 3. Get the aggregated data for the requested partition.
        let aggregated_data: Vec<(K, C)> = combiners
            .into_iter()
            .nth(partition_index)
            .unwrap_or_default()
            .into_iter()
            .collect();

        Ok(Box::new(aggregated_data.into_iter()))
    }

    fn num_partitions(&self) -> usize {
        self.partitioner.num_partitions() as usize
    }

    fn dependencies(&self) -> Vec<Dependency> {
        // Create a proper shuffle dependency
        let shuffle_info = ShuffleDependencyInfo {
            shuffle_id: self.id,
            num_partitions: self.partitioner.num_partitions(),
            partitioner_type: PartitionerType::Hash {
                num_partitions: self.partitioner.num_partitions(),
                seed: 0,
            },
        };
        vec![Dependency::Shuffle(
            unsafe {
                std::mem::transmute::<Arc<dyn RddBase<Item = (K, V)>>, Arc<dyn Any + Send + Sync>>(
                    self.parent.clone(),
                )
            },
            shuffle_info,
        )]
    }

    fn id(&self) -> usize {
        self.id
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
        let parent_map_info = map_output_info
            .and_then(|infos| infos.first())
            .ok_or_else(|| {
                RddError::ShuffleError("Could not find map outputs for shuffle".to_string())
            })?;

        let map_locations: Vec<(String, u32)> = parent_map_info
            .iter()
            .enumerate() // gives map_id (partition index of map task)
            .map(|(map_id, (_map_status, exec_info))| {
                let shuffle_addr = format!("{}:{}", exec_info.host, exec_info.shuffle_port);
                (shuffle_addr, map_id as u32)
            })
            .collect();

        // Use the aggregator's to_serializable method instead of downcasting
        let aggregator_data = self
            .aggregator
            .to_serializable()
            .and_then(|sa| sa.serialize())
            .unwrap_or_default();

        // Try to create tasks for supported type combinations
        self.create_typed_tasks(self.id as u32, &map_locations, &aggregator_data)
    }
}

impl<K: Data, V: Data, C: Data> ShuffledRdd<K, V, C>
where
    K: std::hash::Hash + Eq,
{
    /// Helper method to create tasks for supported type combinations
    /// This reduces downcasting to a single location and makes it easier to add new types
    fn create_typed_tasks(
        &self,
        shuffle_id: u32,
        map_locations: &[(String, u32)],
        aggregator_data: &[u8],
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        use crate::distributed::task::ShuffleReduceTask;
        use crate::shuffle::ReduceAggregator;

        let mut tasks: Vec<Box<dyn crate::distributed::task::Task>> = Vec::new();

        // Check for String -> i32 -> i32 (most common case)
        if self
            .as_any()
            .downcast_ref::<ShuffledRdd<String, i32, i32>>()
            .is_some()
        {
            for i in 0..self.num_partitions() {
                let task = ShuffleReduceTask::<String, i32, i32, ReduceAggregator<i32>>::new(
                    shuffle_id,
                    i as u32,
                    map_locations.to_vec(),
                    aggregator_data.to_vec(),
                );
                tasks.push(Box::new(task));
            }
            return Ok(tasks);
        }

        // Check for i32 -> String -> String
        if self
            .as_any()
            .downcast_ref::<ShuffledRdd<i32, String, String>>()
            .is_some()
        {
            for i in 0..self.num_partitions() {
                let task = ShuffleReduceTask::<i32, String, String, ReduceAggregator<String>>::new(
                    shuffle_id,
                    i as u32,
                    map_locations.to_vec(),
                    aggregator_data.to_vec(),
                );
                tasks.push(Box::new(task));
            }
            return Ok(tasks);
        }

        // If no supported type combination is found, return an error
        Err(RddError::ContextError(format!(
            "Task creation for ShuffledRdd with item type {:?} is not supported yet. \
            Supported combinations: (String, i32, i32), (i32, String, String)",
            std::any::type_name::<<Self as crate::traits::RddBase>::Item>()
        )))
    }
}

impl<K: Data, V: Data, C: Data> ShuffledRdd<K, V, C>
where
    K: std::hash::Hash + Eq,
{
    /// Collect all elements from all partitions into a vector
    pub fn collect(&self) -> crate::traits::RddResult<Vec<(K, C)>> {
        let mut result = Vec::new();
        for i in 0..self.num_partitions() {
            let partition = crate::traits::BasicPartition::new(i);
            let partition_data = self.compute(&partition)?;
            result.extend(partition_data);
        }
        Ok(result)
    }
}
