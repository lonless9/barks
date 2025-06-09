//! RDD that represents a join operation between two RDDs.

use crate::shuffle::Partitioner;
use crate::traits::{
    Data, Dependency, Partition, PartitionerType, RddBase, RddResult, ShuffleDependencyInfo,
};
use std::any::Any;
use std::sync::Arc;

/// Type alias for cogrouped data to reduce type complexity
type CogroupedData<K, V, W> = Vec<(K, (Vec<V>, Vec<W>))>;

/// JoinedRdd represents the result of joining two RDDs by key.
/// It performs a hash join using shuffle operations.
#[derive(Clone, Debug)]
pub struct JoinedRdd<K: Data, V: Data, W: Data> {
    id: usize,
    left_rdd: Arc<dyn RddBase<Item = (K, V)>>,
    right_rdd: Arc<dyn RddBase<Item = (K, W)>>,
    partitioner: Arc<dyn Partitioner<K>>,
}

impl<K: Data, V: Data, W: Data> JoinedRdd<K, V, W> {
    pub fn new(
        id: usize,
        left_rdd: Arc<dyn RddBase<Item = (K, V)>>,
        right_rdd: Arc<dyn RddBase<Item = (K, W)>>,
        partitioner: Arc<dyn Partitioner<K>>,
    ) -> Self {
        Self {
            id,
            left_rdd,
            right_rdd,
            partitioner,
        }
    }
}

impl<K: Data, V: Data, W: Data> RddBase for JoinedRdd<K, V, W>
where
    K: std::hash::Hash + Eq,
{
    type Item = (K, (V, W));

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        // For local execution, we simulate the join by computing both parent RDDs
        // and performing a hash join. In a distributed environment,
        // this would fetch shuffle blocks from remote executors.

        let partition_index = partition.index();

        // Get all data from left RDD partitions
        let mut left_data = Vec::new();
        for i in 0..self.left_rdd.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.left_rdd.compute(&parent_partition)?;
            left_data.extend(parent_data);
        }

        // Get all data from right RDD partitions
        let mut right_data = Vec::new();
        for i in 0..self.right_rdd.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.right_rdd.compute(&parent_partition)?;
            right_data.extend(parent_data);
        }

        // Group left data by key and partition
        let mut left_partitioned: std::collections::HashMap<K, Vec<V>> =
            std::collections::HashMap::new();
        for (key, value) in left_data {
            let key_partition = self.partitioner.get_partition(&key) as usize;
            if key_partition == partition_index {
                left_partitioned.entry(key).or_default().push(value);
            }
        }

        let mut right_partitioned: std::collections::HashMap<K, Vec<W>> =
            std::collections::HashMap::new();
        for (key, value) in right_data {
            let key_partition = self.partitioner.get_partition(&key) as usize;
            if key_partition == partition_index {
                right_partitioned.entry(key).or_default().push(value);
            }
        }

        // Perform inner join
        let mut joined_data = Vec::new();
        for (key, left_values) in left_partitioned {
            if let Some(right_values) = right_partitioned.get(&key) {
                for left_value in left_values {
                    for right_value in right_values {
                        joined_data.push((key.clone(), (left_value.clone(), right_value.clone())));
                    }
                }
            }
        }

        Ok(Box::new(joined_data.into_iter()))
    }

    fn num_partitions(&self) -> usize {
        self.partitioner.num_partitions() as usize
    }

    fn dependencies(&self) -> Vec<Dependency> {
        // Join creates shuffle dependencies on both parent RDDs
        vec![
            Dependency::Shuffle(
                unsafe {
                    std::mem::transmute::<Arc<dyn RddBase<Item = (K, V)>>, Arc<dyn Any + Send + Sync>>(
                        self.left_rdd.clone(),
                    )
                },
                ShuffleDependencyInfo {
                    shuffle_id: self.id,
                    num_partitions: self.partitioner.num_partitions(),
                    partitioner_type: PartitionerType::Hash {
                        num_partitions: self.partitioner.num_partitions(),
                        seed: 0,
                    },
                },
            ),
            Dependency::Shuffle(
                unsafe {
                    std::mem::transmute::<Arc<dyn RddBase<Item = (K, W)>>, Arc<dyn Any + Send + Sync>>(
                        self.right_rdd.clone(),
                    )
                },
                ShuffleDependencyInfo {
                    shuffle_id: self.id + 1, // Different shuffle ID for right RDD
                    num_partitions: self.partitioner.num_partitions(),
                    partitioner_type: PartitionerType::Hash {
                        num_partitions: self.partitioner.num_partitions(),
                        seed: 0,
                    },
                },
            ),
        ]
    }

    fn id(&self) -> usize {
        self.id
    }

    fn create_tasks(
        &self,
        _stage_id: crate::distributed::types::StageId,
        _map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        // JoinedRdd is built on top of CogroupedRdd, so it has the same implementation challenges.
        // The join operation would:
        // 1. Use CoGroupTask to cogroup the two parent RDDs
        // 2. Apply join logic to filter out keys that don't exist in both sides
        // 3. Flatten the result to produce (K, (V, W)) tuples
        //
        // For now, we'll return the same error as CogroupedRdd since they share the same
        // underlying implementation requirements.
        Err(crate::traits::RddError::TaskCreationError(
            "JoinedRdd task creation requires type-specific implementation. \
            JoinedRdd is built on CoGroupTask which needs to be instantiated with the \
            correct generic types that match the RDD's K, V, W parameters. This is a \
            known limitation that requires architectural improvements to the task system."
                .to_string(),
        ))
    }
}

impl<K: Data, V: Data, W: Data> JoinedRdd<K, V, W>
where
    K: std::hash::Hash + Eq,
{
    /// Collect all elements from all partitions into a vector
    pub fn collect(&self) -> crate::traits::RddResult<Vec<(K, (V, W))>> {
        let mut result = Vec::new();
        for i in 0..self.num_partitions() {
            let partition = crate::traits::BasicPartition::new(i);
            let partition_data = self.compute(&partition)?;
            result.extend(partition_data);
        }
        Ok(result)
    }
}

/// CogroupedRdd represents the result of cogrouping two RDDs by key.
/// This is the foundation for join operations and groups values from both RDDs.
#[derive(Clone, Debug)]
pub struct CogroupedRdd<K: Data, V: Data, W: Data> {
    id: usize,
    left_rdd: Arc<dyn RddBase<Item = (K, V)>>,
    right_rdd: Arc<dyn RddBase<Item = (K, W)>>,
    partitioner: Arc<dyn Partitioner<K>>,
}

impl<K: Data, V: Data, W: Data> CogroupedRdd<K, V, W> {
    pub fn new(
        id: usize,
        left_rdd: Arc<dyn RddBase<Item = (K, V)>>,
        right_rdd: Arc<dyn RddBase<Item = (K, W)>>,
        partitioner: Arc<dyn Partitioner<K>>,
    ) -> Self {
        Self {
            id,
            left_rdd,
            right_rdd,
            partitioner,
        }
    }
}

impl<K: Data, V: Data, W: Data> RddBase for CogroupedRdd<K, V, W>
where
    K: std::hash::Hash + Eq,
{
    type Item = (K, (Vec<V>, Vec<W>));

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compute(
        &self,
        partition: &dyn Partition,
    ) -> RddResult<Box<dyn Iterator<Item = Self::Item>>> {
        // For local execution, we simulate the cogroup by computing both parent RDDs
        // and grouping values by key. In a distributed environment,
        // this would fetch shuffle blocks from remote executors.

        let partition_index = partition.index();

        // Get all data from left RDD partitions
        let mut left_data = Vec::new();
        for i in 0..self.left_rdd.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.left_rdd.compute(&parent_partition)?;
            left_data.extend(parent_data);
        }

        // Get all data from right RDD partitions
        let mut right_data = Vec::new();
        for i in 0..self.right_rdd.num_partitions() {
            let parent_partition = crate::traits::BasicPartition::new(i);
            let parent_data = self.right_rdd.compute(&parent_partition)?;
            right_data.extend(parent_data);
        }

        // Group left data by key and partition
        let mut left_partitioned: std::collections::HashMap<K, Vec<V>> =
            std::collections::HashMap::new();
        for (key, value) in left_data {
            let key_partition = self.partitioner.get_partition(&key) as usize;
            if key_partition == partition_index {
                left_partitioned.entry(key).or_default().push(value);
            }
        }

        let mut right_partitioned: std::collections::HashMap<K, Vec<W>> =
            std::collections::HashMap::new();
        for (key, value) in right_data {
            let key_partition = self.partitioner.get_partition(&key) as usize;
            if key_partition == partition_index {
                right_partitioned.entry(key).or_default().push(value);
            }
        }

        // Combine all keys from both sides
        let mut all_keys: std::collections::HashSet<K> = std::collections::HashSet::new();
        all_keys.extend(left_partitioned.keys().cloned());
        all_keys.extend(right_partitioned.keys().cloned());

        // Create cogrouped results
        let cogrouped_data: CogroupedData<K, V, W> = all_keys
            .into_iter()
            .map(|key| {
                let left_values = left_partitioned.get(&key).cloned().unwrap_or_default();
                let right_values = right_partitioned.get(&key).cloned().unwrap_or_default();
                (key, (left_values, right_values))
            })
            .collect();

        Ok(Box::new(cogrouped_data.into_iter()))
    }

    fn num_partitions(&self) -> usize {
        self.partitioner.num_partitions() as usize
    }

    fn dependencies(&self) -> Vec<Dependency> {
        // Cogroup creates shuffle dependencies on both parent RDDs
        vec![
            Dependency::Shuffle(
                unsafe {
                    std::mem::transmute::<Arc<dyn RddBase<Item = (K, V)>>, Arc<dyn Any + Send + Sync>>(
                        self.left_rdd.clone(),
                    )
                },
                ShuffleDependencyInfo {
                    shuffle_id: self.id,
                    num_partitions: self.partitioner.num_partitions(),
                    partitioner_type: PartitionerType::Hash {
                        num_partitions: self.partitioner.num_partitions(),
                        seed: 0,
                    },
                },
            ),
            Dependency::Shuffle(
                unsafe {
                    std::mem::transmute::<Arc<dyn RddBase<Item = (K, W)>>, Arc<dyn Any + Send + Sync>>(
                        self.right_rdd.clone(),
                    )
                },
                ShuffleDependencyInfo {
                    shuffle_id: self.id + 1, // Different shuffle ID for right RDD
                    num_partitions: self.partitioner.num_partitions(),
                    partitioner_type: PartitionerType::Hash {
                        num_partitions: self.partitioner.num_partitions(),
                        seed: 0,
                    },
                },
            ),
        ]
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
        let map_output_info = map_output_info.ok_or_else(|| {
            crate::traits::RddError::TaskCreationError(
                "CogroupedRdd requires map output info for shuffle dependencies".to_string(),
            )
        })?;

        // We expect two shuffle dependencies (left and right RDD)
        if map_output_info.len() != 2 {
            return Err(crate::traits::RddError::TaskCreationError(format!(
                "CogroupedRdd expects exactly 2 shuffle dependencies, got {}",
                map_output_info.len()
            )));
        }

        // For now, we'll return an error indicating this needs to be implemented
        // with proper type handling. The CoGroupTask needs to be created with the
        // correct generic types that match K, V, W.
        //
        // The challenge is that we need to create CoGroupTask<K, V, C, A> where:
        // - K, V come from the generic parameters of CogroupedRdd
        // - C, A need to be determined based on the aggregation operation
        //
        // This requires either:
        // 1. Making CoGroupTask work with trait objects instead of generics
        // 2. Using macros to generate type-specific implementations
        // 3. Implementing a type registry system
        //
        // For the TODO0 implementation, we'll mark this as requiring further work.
        Err(crate::traits::RddError::TaskCreationError(
            "CogroupedRdd task creation requires type-specific implementation. \
            The CoGroupTask needs to be instantiated with the correct generic types \
            that match the RDD's K, V, W parameters. This is a known limitation \
            that requires architectural improvements to the task system."
                .to_string(),
        ))
    }
}

impl<K: Data, V: Data, W: Data> CogroupedRdd<K, V, W>
where
    K: std::hash::Hash + Eq,
{
    /// Collect all elements from all partitions into a vector
    pub fn collect(&self) -> crate::traits::RddResult<CogroupedData<K, V, W>> {
        let mut result = Vec::new();
        for i in 0..self.num_partitions() {
            let partition = crate::traits::BasicPartition::new(i);
            let partition_data = self.compute(&partition)?;
            result.extend(partition_data);
        }
        Ok(result)
    }
}
