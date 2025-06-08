//! RDD that represents a shuffle dependency.

use crate::distributed::task::ShuffleReduceTask;
use crate::shuffle::{
    Aggregator, GroupByKeyAggregator, Partitioner, ReduceAggregator, SerializableAggregator,
};
use crate::traits::{
    Data, Dependency, Partition, PartitionerType, RddBase, RddError, RddResult,
    ShuffleDependencyInfo,
};
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
        // and applying the aggregator locally. In a distributed environment,
        // this would fetch shuffle blocks from remote executors.

        let partition_index = partition.index();

        // Get all data from parent partitions (simulating shuffle read)
        let mut all_data = Vec::new();
        for i in 0..self.parent.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.parent.compute(&parent_partition)?;
            all_data.extend(parent_data);
        }

        // Group data by key and partition
        let mut partitioned_data: std::collections::HashMap<K, Vec<V>> =
            std::collections::HashMap::new();

        for (key, value) in all_data {
            let key_partition = self.partitioner.get_partition(&key) as usize;
            // Only include data that belongs to the current partition
            if key_partition == partition_index {
                partitioned_data.entry(key).or_default().push(value);
            }
        }

        // Apply aggregator to combine values for each key
        let aggregated_data: Vec<(K, C)> = partitioned_data
            .into_iter()
            .map(|(key, values)| {
                let mut combined = None;
                for value in values {
                    combined = Some(match combined {
                        None => self.aggregator.create_combiner(value),
                        Some(c) => self.aggregator.merge_value(c, value),
                    });
                }
                (key, combined.unwrap())
            })
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
            unsafe { std::mem::transmute(self.parent.clone()) },
            shuffle_info,
        )]
    }

    fn id(&self) -> usize {
        self.id
    }

    fn create_tasks(
        &self,
        _stage_id: crate::distributed::types::StageId,
        map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        let parent_map_info = map_output_info
            .and_then(|infos| infos.get(0))
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

        // This is a workaround due to the aggregator being a non-serializable trait object.
        // A better design would be to store a serializable representation of the aggregator.
        let aggregator_data = if let Some(_) = self
            .aggregator
            .as_any()
            .downcast_ref::<ReduceAggregator<i32>>()
        {
            SerializableAggregator::AddI32
                .serialize()
                .map_err(|e| RddError::SerializationError(e))?
        } else if let Some(_) = self
            .aggregator
            .as_any()
            .downcast_ref::<ReduceAggregator<String>>()
        {
            SerializableAggregator::ConcatString
                .serialize()
                .map_err(|e| RddError::SerializationError(e))?
        } else if let Some(_) = self
            .aggregator
            .as_any()
            .downcast_ref::<GroupByKeyAggregator<i32>>()
        {
            SerializableAggregator::GroupI32
                .serialize()
                .map_err(|e| RddError::SerializationError(e))?
        } else {
            return Err(RddError::SerializationError(
                "Unsupported aggregator type for task creation".to_string(),
            ));
        };

        let mut tasks: Vec<Box<dyn crate::distributed::task::Task>> = Vec::new();

        // This part uses downcasting to create the correctly typed task, which is consistent
        // with other parts of the codebase but indicates a lack of true generics.
        if let Some(_) = self
            .as_any()
            .downcast_ref::<ShuffledRdd<String, i32, i32>>()
        {
            for i in 0..self.num_partitions() {
                let task = ShuffleReduceTask::<String, i32, i32, ReduceAggregator<i32>>::new(
                    self.id as u32,
                    i as u32,
                    map_locations.clone(),
                    aggregator_data.clone(),
                );
                tasks.push(Box::new(task));
            }
        } else {
            return Err(RddError::ContextError(format!(
                "Task creation for ShuffledRdd with item type {:?} is not supported yet.",
                std::any::type_name::<Self::Item>()
            )));
        }

        Ok(tasks)
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
