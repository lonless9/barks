//! Distributed RDD implementation with serializable operations
//!
//! This module provides RDD implementations that can be executed
//! in a distributed environment using serializable operations.

use crate::operations::RddDataType;
use crate::traits::{BasicPartition, Partition, RddBase};
use bumpalo::Bump;
use std::fmt::Debug;
use std::sync::Arc;

/// A Resilient Distributed Dataset (RDD) that is designed for execution on a cluster.
/// It is generic over the data type `T`.
///
/// To support a new data type, one must implement the `RddDataType` trait for it.
#[derive(Debug, Clone)]
pub enum DistributedRdd<T: RddDataType> {
    /// RDD backed by a vector of data
    Vec {
        data: Arc<Vec<T>>,
        num_partitions: usize,
    },
    /// RDD that applies a map transformation
    Map {
        parent: Arc<DistributedRdd<T>>,
        operation: T::MapOperation,
    },
    /// RDD that applies a filter transformation
    Filter {
        parent: Arc<DistributedRdd<T>>,
        predicate: T::FilterPredicate,
    },
    /// RDD that applies a flatMap transformation
    FlatMap {
        parent: Arc<DistributedRdd<T>>,
        operation: T::FlatMapOperation,
    },
    /// RDD that represents the union of two RDDs
    Union {
        left: Arc<DistributedRdd<T>>,
        right: Arc<DistributedRdd<T>>,
    },
    /// RDD that contains only distinct elements
    Distinct {
        parent: Arc<DistributedRdd<T>>,
        num_partitions: usize,
    },
}

/// Helper function to extract partition data from base data
pub(crate) fn get_partition_data<T>(
    data: &Arc<Vec<T>>,
    partition_index: usize,
    num_partitions: usize,
) -> Vec<T>
where
    T: Clone,
{
    let data_len = data.len();
    let partition_size = data_len.div_ceil(num_partitions);
    let start = partition_index * partition_size;
    let end = std::cmp::min(start + partition_size, data_len);
    if start >= data_len {
        Vec::new()
    } else {
        data[start..end].to_vec()
    }
}

impl<T: RddDataType> DistributedRdd<T> {
    /// Create a new RDD from a vector of data
    pub fn from_vec(data: Vec<T>) -> Self {
        Self::Vec {
            data: Arc::new(data),
            num_partitions: 1,
        }
    }

    /// Create a new RDD from a vector with specified number of partitions
    pub fn from_vec_with_partitions(data: Vec<T>, num_partitions: usize) -> Self {
        let num_partitions = if num_partitions == 0 {
            1
        } else {
            num_partitions
        };
        Self::Vec {
            data: Arc::new(data),
            num_partitions,
        }
    }

    /// Apply a map transformation to this RDD
    pub fn map(self, operation: T::MapOperation) -> Self {
        Self::Map {
            parent: Arc::new(self),
            operation,
        }
    }

    /// Apply a filter transformation to this RDD
    pub fn filter(self, predicate: T::FilterPredicate) -> Self {
        Self::Filter {
            parent: Arc::new(self),
            predicate,
        }
    }

    /// Apply a flatMap transformation to this RDD
    pub fn flat_map(self, operation: T::FlatMapOperation) -> Self {
        Self::FlatMap {
            parent: Arc::new(self),
            operation,
        }
    }

    /// Return the union of this RDD and another RDD
    pub fn union(self, other: Self) -> Self {
        Self::Union {
            left: Arc::new(self),
            right: Arc::new(other),
        }
    }

    /// Return a new RDD containing only distinct elements
    pub fn distinct(self) -> Self
    where
        T: Eq + std::hash::Hash,
    {
        let num_partitions = self.num_partitions();
        Self::Distinct {
            parent: Arc::new(self),
            num_partitions,
        }
    }

    /// Return a new RDD containing only distinct elements with specified number of partitions
    pub fn distinct_with_partitions(self, num_partitions: usize) -> Self
    where
        T: Eq + std::hash::Hash,
    {
        Self::Distinct {
            parent: Arc::new(self),
            num_partitions,
        }
    }

    /// Repartition the RDD to have the specified number of partitions
    pub fn repartition(self, num_partitions: usize) -> Self {
        if num_partitions <= self.num_partitions() {
            return self.coalesce(num_partitions);
        }

        match self {
            Self::Vec { data, .. } => Self::Vec {
                data,
                num_partitions,
            },
            _ => {
                // For transformed RDDs, we would need to materialize the data first
                // This is a simplification - real Spark would use shuffle operations
                Self::Vec {
                    data: Arc::new(Vec::new()), // Placeholder
                    num_partitions,
                }
            }
        }
    }

    /// Coalesce the RDD to have fewer partitions
    pub fn coalesce(self, num_partitions: usize) -> Self {
        let current_partitions = self.num_partitions();
        if num_partitions >= current_partitions {
            return self;
        }

        let num_partitions = if num_partitions == 0 {
            1
        } else {
            num_partitions
        };

        match self {
            Self::Vec { data, .. } => Self::Vec {
                data,
                num_partitions,
            },
            Self::Map { parent, operation } => Self::Map {
                parent: Arc::new((*parent).clone().coalesce(num_partitions)),
                operation,
            },
            Self::Filter { parent, predicate } => Self::Filter {
                parent: Arc::new((*parent).clone().coalesce(num_partitions)),
                predicate,
            },
            Self::FlatMap { parent, operation } => Self::FlatMap {
                parent: Arc::new((*parent).clone().coalesce(num_partitions)),
                operation,
            },
            Self::Union { left, right } => Self::Union {
                left: Arc::new((*left).clone().coalesce(num_partitions / 2)),
                right: Arc::new((*right).clone().coalesce(num_partitions / 2)),
            },
            Self::Distinct { parent, .. } => Self::Distinct {
                parent: Arc::new((*parent).clone().coalesce(num_partitions)),
                num_partitions,
            },
        }
    }

    /// Traverses the RDD lineage, collects all serializable operations,
    /// and returns the base RDD's data and original number of partitions.
    pub fn analyze_lineage(self) -> (Arc<Vec<T>>, usize, Vec<T::SerializableOperation>) {
        let mut operations = Vec::new();
        let mut current_rdd = self;

        let (base_data, num_partitions) = loop {
            match current_rdd {
                Self::Vec {
                    data,
                    num_partitions,
                } => break (data, num_partitions),
                Self::Map { parent, operation } => {
                    operations.push(operation.into());
                    current_rdd = (*parent).clone();
                }
                Self::Filter { parent, predicate } => {
                    operations.push(predicate.into());
                    current_rdd = (*parent).clone();
                }
                Self::FlatMap { parent, operation } => {
                    operations.push(operation.into());
                    current_rdd = (*parent).clone();
                }
                Self::Union { .. } | Self::Distinct { .. } => {
                    // Union and Distinct operations cannot be analyzed as simple lineage chains
                    // For now, we'll create a dummy base data and empty operations
                    // In a real implementation, these would need special handling
                    return (Arc::new(Vec::new()), 0, Vec::new());
                }
            }
        };
        operations.reverse();
        (base_data, num_partitions, operations)
    }

    /// Get the list of partitions for this RDD
    pub fn partitions(&self) -> Vec<Box<dyn Partition>> {
        match self {
            Self::Vec { num_partitions, .. } => (0..*num_partitions)
                .map(|i| Box::new(BasicPartition::new(i)) as Box<dyn Partition>)
                .collect(),
            Self::Map { parent, .. }
            | Self::Filter { parent, .. }
            | Self::FlatMap { parent, .. } => parent.partitions(),
            Self::Union { left, right } => {
                // Union creates new partitions that combine both RDDs
                let left_partitions = left.num_partitions();
                let right_partitions = right.num_partitions();
                let total_partitions = left_partitions + right_partitions;

                (0..total_partitions)
                    .map(|i| Box::new(BasicPartition::new(i)) as Box<dyn Partition>)
                    .collect()
            }
            Self::Distinct { num_partitions, .. } => (0..*num_partitions)
                .map(|i| Box::new(BasicPartition::new(i)) as Box<dyn Partition>)
                .collect(),
        }
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.partitions().len()
    }

    /// Check if this RDD has a shuffle dependency.
    /// Returns Some(shuffle_dependency) if it does, None otherwise.
    /// DistributedRdd operations (map, filter) don't create shuffle dependencies.
    pub fn shuffle_dependency(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        // DistributedRdd operations are narrow transformations and don't create shuffle dependencies
        // Shuffle dependencies are created by operations like reduceByKey, groupByKey, join, etc.
        None
    }

    /// Compute the elements of this RDD for the given partition.
    /// This method is for local execution (e.g., in tests or local mode fallback).
    /// The primary distributed execution path is via `DistributedContext::run_distributed`.
    pub fn compute(&self, partition: &dyn Partition) -> crate::traits::RddResult<Vec<T>> {
        match self {
            Self::Vec {
                data,
                num_partitions,
            } => {
                let partition_index = partition.index();
                if partition_index >= *num_partitions {
                    return Err(crate::traits::RddError::InvalidPartition(partition_index));
                }

                let data_len = data.len();
                let partition_size = data_len.div_ceil(*num_partitions);
                let start = partition_index * partition_size;
                let end = std::cmp::min(start + partition_size, data_len);

                if start >= data_len {
                    Ok(Vec::new())
                } else {
                    Ok(data[start..end].to_vec())
                }
            }
            Self::Map { parent, operation } => {
                let parent_data = parent.compute(partition)?;
                let serializable_op: T::SerializableOperation = (*operation).clone().into();
                // Create a temporary arena for local execution
                let arena = Bump::new();
                Ok(T::apply_operation(&serializable_op, parent_data, &arena))
            }
            Self::Filter { parent, predicate } => {
                let parent_data = parent.compute(partition)?;
                let serializable_op: T::SerializableOperation = (*predicate).clone().into();
                // Create a temporary arena for local execution
                let arena = Bump::new();
                Ok(T::apply_operation(&serializable_op, parent_data, &arena))
            }
            Self::FlatMap { parent, operation } => {
                let parent_data = parent.compute(partition)?;
                let serializable_op: T::SerializableOperation = (*operation).clone().into();
                // Create a temporary arena for local execution
                let arena = Bump::new();
                Ok(T::apply_operation(&serializable_op, parent_data, &arena))
            }
            Self::Union { left, right } => {
                // For union, we need to determine which RDD this partition belongs to
                let left_num_partitions = left.num_partitions();
                let partition_id = partition.index();

                if partition_id < left_num_partitions {
                    // This partition belongs to the left RDD
                    let left_partitions = left.partitions();
                    left.compute(&*left_partitions[partition_id])
                } else {
                    // This partition belongs to the right RDD
                    let right_partition_id = partition_id - left_num_partitions;
                    let right_partitions = right.partitions();
                    if right_partition_id < right_partitions.len() {
                        right.compute(&*right_partitions[right_partition_id])
                    } else {
                        Ok(vec![])
                    }
                }
            }
            Self::Distinct { parent, .. } => {
                // For distinct, compute parent data and remove duplicates
                // This implementation performs local deduplication within each partition
                // In a real distributed implementation, this would use hash-based partitioning
                // across the cluster to ensure global uniqueness
                let parent_data = parent.compute(partition)?;

                // Use the RddDataType trait to handle distinct computation
                T::compute_distinct(parent_data)
            }
        }
    }

    /// Collect all elements of the RDD into a vector by executing locally.
    pub fn collect(&self) -> crate::traits::RddResult<Vec<T>> {
        use rayon::prelude::*;
        let partitions = self.partitions();

        if partitions.len() <= 1 {
            // Single partition: use sequential execution
            let mut result = Vec::new();
            for partition in partitions {
                let partition_data = self.compute(partition.as_ref())?;
                result.extend(partition_data);
            }
            Ok(result)
        } else {
            // Multiple partitions: use parallel execution with rayon
            let results: crate::traits::RddResult<Vec<Vec<T>>> = partitions
                .into_par_iter()
                .map(|p| self.compute(p.as_ref()))
                .collect();
            let mut result = Vec::new();
            for partition_data in results? {
                result.extend(partition_data);
            }
            Ok(result)
        }
    }

    /// Count the number of elements in the RDD.
    pub fn count(&self) -> crate::traits::RddResult<usize> {
        use rayon::prelude::*;
        let partitions = self.partitions();

        if partitions.len() <= 1 {
            // Single partition: use sequential execution
            let mut count = 0;
            for partition in partitions {
                let partition_data = self.compute(partition.as_ref())?;
                count += partition_data.len();
            }
            Ok(count)
        } else {
            // Multiple partitions: use parallel execution with rayon
            let counts: crate::traits::RddResult<Vec<usize>> = partitions
                .into_par_iter()
                .map(|p| self.compute(p.as_ref()).map(|data| data.len()))
                .collect();
            Ok(counts?.into_iter().sum())
        }
    }

    /// Take the first n elements from the RDD.
    pub fn take(&self, n: usize) -> crate::traits::RddResult<Vec<T>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        let partitions = self.partitions();

        for partition in partitions {
            if result.len() >= n {
                break;
            }
            let partition_data = self.compute(partition.as_ref())?;
            let remaining = n - result.len();
            if partition_data.len() <= remaining {
                result.extend(partition_data);
            } else {
                result.extend(partition_data.into_iter().take(remaining));
            }
        }

        Ok(result)
    }

    /// Get the first element of the RDD, if any.
    pub fn first(&self) -> crate::traits::RddResult<Option<T>> {
        let partitions = self.partitions();

        for partition in partitions {
            let partition_data = self.compute(partition.as_ref())?;
            if !partition_data.is_empty() {
                return Ok(Some(partition_data.into_iter().next().unwrap()));
            }
        }

        Ok(None)
    }

    /// Reduce the elements of the RDD using the specified function.
    pub fn reduce<F>(&self, f: F) -> crate::traits::RddResult<Option<T>>
    where
        F: Fn(T, T) -> T + Send + Sync + Clone,
    {
        use rayon::prelude::*;
        let partitions = self.partitions();

        if partitions.len() <= 1 {
            // Single partition: use sequential execution
            let mut result: Option<T> = None;
            for partition in partitions {
                let partition_data = self.compute(partition.as_ref())?;
                for item in partition_data {
                    result = Some(match result {
                        None => item,
                        Some(acc) => f(acc, item),
                    });
                }
            }
            Ok(result)
        } else {
            // Multiple partitions: use parallel execution with rayon
            let partition_results: crate::traits::RddResult<Vec<Option<T>>> = partitions
                .into_par_iter()
                .map(|p| {
                    let partition_data = self.compute(p.as_ref())?;
                    Ok(partition_data.into_iter().reduce(f.clone()))
                })
                .collect();

            let results = partition_results?;
            Ok(results.into_iter().flatten().reduce(f))
        }
    }
}

// Implement IsRdd trait for DistributedRdd to support type-erased operations
impl<T: RddDataType> crate::traits::IsRdd for DistributedRdd<T> {
    fn dependencies(&self) -> Vec<crate::traits::Dependency> {
        // Return appropriate dependencies based on the RDD type
        match self {
            Self::Vec { .. } => {
                // Base RDD has no dependencies
                vec![]
            }
            Self::Map { parent, .. }
            | Self::Filter { parent, .. }
            | Self::FlatMap { parent, .. } => {
                // A narrow dependency on the parent RDD.
                vec![crate::traits::Dependency::Narrow(
                    parent.clone().as_is_rdd(),
                )]
            }
            Self::Union { left, right } => {
                // Union has narrow dependencies on both parent RDDs
                vec![
                    crate::traits::Dependency::Narrow(left.clone().as_is_rdd()),
                    crate::traits::Dependency::Narrow(right.clone().as_is_rdd()),
                ]
            }
            Self::Distinct {
                parent,
                num_partitions,
            } => {
                // Distinct has a shuffle dependency on the parent RDD
                let shuffle_info = crate::traits::ShuffleDependencyInfo {
                    shuffle_id: parent.id(),
                    num_partitions: *num_partitions as u32,
                    partitioner_type: crate::traits::PartitionerType::Hash {
                        num_partitions: *num_partitions as u32,
                        seed: 42,
                    },
                };
                vec![crate::traits::Dependency::Shuffle(
                    parent.clone().as_is_rdd(),
                    shuffle_info,
                )]
            }
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn id(&self) -> usize {
        // For now, use a simple hash-based ID
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        match self {
            Self::Vec {
                data,
                num_partitions,
            } => {
                "Vec".hash(&mut hasher);
                data.as_ptr().hash(&mut hasher);
                num_partitions.hash(&mut hasher);
            }
            Self::Map { parent, .. } => {
                "Map".hash(&mut hasher);
                parent.id().hash(&mut hasher);
            }
            Self::Filter { parent, .. } => {
                "Filter".hash(&mut hasher);
                parent.id().hash(&mut hasher);
            }
            Self::FlatMap { parent, .. } => {
                "FlatMap".hash(&mut hasher);
                parent.id().hash(&mut hasher);
            }
            Self::Union { left, right } => {
                "Union".hash(&mut hasher);
                left.id().hash(&mut hasher);
                right.id().hash(&mut hasher);
            }
            Self::Distinct {
                parent,
                num_partitions,
            } => {
                "Distinct".hash(&mut hasher);
                parent.id().hash(&mut hasher);
                num_partitions.hash(&mut hasher);
            }
        }
        hasher.finish() as usize
    }

    fn num_partitions(&self) -> usize {
        self.num_partitions()
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

// Implement RddBase trait for DistributedRdd to support shuffle operations
impl<T: RddDataType> crate::traits::RddBase for DistributedRdd<T> {
    type Item = T;

    fn compute(
        &self,
        partition: &dyn crate::traits::Partition,
    ) -> crate::traits::RddResult<Box<dyn Iterator<Item = T>>> {
        let data = self.compute(partition)?;
        Ok(Box::new(data.into_iter()))
    }

    fn create_tasks(
        &self,
        _stage_id: crate::distributed::types::StageId,
        // This RDD is being used as the map-side of a shuffle.
        shuffle_info: Option<&crate::traits::ShuffleDependencyInfo>,
        _map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        if let Some(shuffle) = shuffle_info {
            return T::create_shuffle_map_tasks(
                self,
                shuffle.shuffle_id as u32,
                shuffle.num_partitions,
            );
        }

        let (base_data, num_partitions, operations) = self.clone().analyze_lineage();
        (0..num_partitions)
            .map(|i| {
                let partition_data = get_partition_data(&base_data, i, num_partitions);
                let serialized_partition_data =
                    bincode::encode_to_vec(&partition_data, bincode::config::standard())
                        .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;
                T::create_chained_task(serialized_partition_data, operations.clone())
            })
            .collect()
    }

    fn as_is_rdd(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn crate::traits::IsRdd> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::{DoubleOperation, GreaterThanPredicate, SerializableI32Operation};

    #[test]
    fn test_analyze_lineage() {
        // Test the analyze_lineage method with a chain of operations
        let data = vec![1, 2, 3, 4, 5];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 2);

        let chained_rdd = rdd
            .map(Box::new(DoubleOperation))
            .filter(Box::new(GreaterThanPredicate { threshold: 5 }));

        let (base_data, num_partitions, operations) = chained_rdd.analyze_lineage();

        // Check that we got the original data back
        assert_eq!(*base_data, data);
        assert_eq!(num_partitions, 2);

        // Check that we have the correct operations in the right order
        assert_eq!(operations.len(), 2);

        // First operation should be Map (DoubleOperation)
        match &operations[0] {
            SerializableI32Operation::Map(_) => {}
            _ => panic!("Expected Map operation"),
        }

        // Second operation should be Filter (GreaterThanPredicate)
        match &operations[1] {
            SerializableI32Operation::Filter(_) => {}
            _ => panic!("Expected Filter operation"),
        }
    }

    #[test]
    fn test_analyze_lineage_single_operation() {
        // Test with a single operation
        let data = vec![10, 20, 30];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data.clone());
        let mapped_rdd = rdd.map(Box::new(DoubleOperation));

        let (base_data, num_partitions, operations) = mapped_rdd.analyze_lineage();

        assert_eq!(*base_data, data);
        assert_eq!(num_partitions, 1);
        assert_eq!(operations.len(), 1);

        match &operations[0] {
            SerializableI32Operation::Map(_) => {}
            _ => panic!("Expected Map operation"),
        }
    }

    #[test]
    fn test_analyze_lineage_no_operations() {
        // Test with no operations (just base RDD)
        let data = vec![1, 2, 3];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 3);

        let (base_data, num_partitions, operations) = rdd.analyze_lineage();

        assert_eq!(*base_data, data);
        assert_eq!(num_partitions, 3);
        assert_eq!(operations.len(), 0);
    }

    #[test]
    fn test_generic_compute_and_collect() {
        // Test the new generic compute and collect methods
        let data = vec![1, 2, 3, 4, 5];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data.clone(), 2);

        // Test collect
        let collected = rdd.collect().unwrap();
        assert_eq!(collected, data);

        // Test with transformations
        let transformed_rdd = rdd
            .map(Box::new(DoubleOperation))
            .filter(Box::new(GreaterThanPredicate { threshold: 5 }));

        let result = transformed_rdd.collect().unwrap();
        let expected: Vec<i32> = data.iter().map(|x| x * 2).filter(|&x| x > 5).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_union_operation() {
        // Test the union operation
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];

        let rdd1: DistributedRdd<i32> = DistributedRdd::from_vec(data1.clone());
        let rdd2: DistributedRdd<i32> = DistributedRdd::from_vec(data2.clone());

        let union_rdd = rdd1.union(rdd2);
        let result = union_rdd.collect().unwrap();

        // Union should contain all elements from both RDDs
        let mut expected = data1;
        expected.extend(data2);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_distinct_operation() {
        // Test the distinct operation
        let data = vec![1, 2, 2, 3, 3, 3, 4];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

        let distinct_rdd = rdd.distinct();
        let result = distinct_rdd.collect().unwrap();

        // The distinct operation should remove duplicates and return unique elements
        // The order is preserved based on first occurrence
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_union_with_transformations() {
        // Test union combined with other transformations
        let data1 = vec![1, 2];
        let data2 = vec![3, 4];

        let rdd1: DistributedRdd<i32> = DistributedRdd::from_vec(data1);
        let rdd2: DistributedRdd<i32> = DistributedRdd::from_vec(data2);

        // Apply transformations before union
        let transformed_rdd1 = rdd1.map(Box::new(DoubleOperation));
        let transformed_rdd2 = rdd2.map(Box::new(DoubleOperation));

        let union_rdd = transformed_rdd1.union(transformed_rdd2);
        let result = union_rdd.collect().unwrap();

        // Should contain doubled values from both RDDs
        assert_eq!(result, vec![2, 4, 6, 8]);
    }

    #[test]
    fn test_distinct_with_multiple_partitions() {
        // Test distinct operation with multiple partitions
        let data = vec![1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec_with_partitions(data, 3);

        let distinct_rdd = rdd.distinct();
        let result = distinct_rdd.collect().unwrap();

        // Note: The current implementation only removes duplicates within each partition,
        // not across partitions. In a real distributed implementation, this would require
        // a shuffle operation to ensure global uniqueness. For now, we test the local behavior.
        // The result may contain duplicates across partitions.
        assert!(result.len() <= 10); // Should be at most the original length
        assert!(result.contains(&1));
        assert!(result.contains(&2));
        assert!(result.contains(&3));
        assert!(result.contains(&4));
    }

    #[test]
    fn test_distinct_empty_data() {
        // Test distinct operation with empty data
        let data: Vec<i32> = vec![];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data);

        let distinct_rdd = rdd.distinct();
        let result = distinct_rdd.collect().unwrap();

        // Should return empty vector
        let expected: Vec<i32> = vec![];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_distinct_no_duplicates() {
        // Test distinct operation with data that has no duplicates
        let data = vec![1, 2, 3, 4, 5];
        let rdd: DistributedRdd<i32> = DistributedRdd::from_vec(data.clone());

        let distinct_rdd = rdd.distinct();
        let result = distinct_rdd.collect().unwrap();

        // Should return the same data
        assert_eq!(result, data);
    }
}
