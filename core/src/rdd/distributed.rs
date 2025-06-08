//! Distributed RDD implementation with serializable operations
//!
//! This module provides RDD implementations that can be executed
//! in a distributed environment using serializable operations.

use crate::operations::RddDataType;
use crate::traits::{BasicPartition, Partition};
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
        parent: Box<DistributedRdd<T>>,
        operation: T::MapOperation,
    },
    /// RDD that applies a filter transformation
    Filter {
        parent: Box<DistributedRdd<T>>,
        predicate: T::FilterPredicate,
    },
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
            parent: Box::new(self),
            operation,
        }
    }

    /// Apply a filter transformation to this RDD
    pub fn filter(self, predicate: T::FilterPredicate) -> Self {
        Self::Filter {
            parent: Box::new(self),
            predicate,
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
                parent: Box::new(parent.coalesce(num_partitions)),
                operation,
            },
            Self::Filter { parent, predicate } => Self::Filter {
                parent: Box::new(parent.coalesce(num_partitions)),
                predicate,
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
                    current_rdd = *parent;
                }
                Self::Filter { parent, predicate } => {
                    operations.push(predicate.into());
                    current_rdd = *parent;
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
            Self::Map { parent, .. } | Self::Filter { parent, .. } => parent.partitions(),
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

// Implement RddBase trait for DistributedRdd to support shuffle operations
impl<T: RddDataType> crate::traits::RddBase for DistributedRdd<T> {
    type Item = T;

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compute(
        &self,
        partition: &dyn crate::traits::Partition,
    ) -> crate::traits::RddResult<Box<dyn Iterator<Item = T>>> {
        let data = self.compute(partition)?;
        Ok(Box::new(data.into_iter()))
    }

    fn num_partitions(&self) -> usize {
        self.num_partitions()
    }

    fn dependencies(&self) -> Vec<crate::traits::Dependency> {
        // Return appropriate dependencies based on the RDD type
        match self {
            Self::Vec { .. } => {
                // Base RDD has no dependencies
                vec![]
            }
            Self::Map { parent, .. } | Self::Filter { parent, .. } => {
                // Narrow dependency on parent RDD
                vec![crate::traits::Dependency::Narrow(
                    crate::traits::NarrowDependency {
                        parent_rdd_id: parent.id(),
                        partition_mapping: crate::traits::NarrowDependencyType::OneToOne,
                    },
                )]
            }
        }
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
        }
        hasher.finish() as usize
    }

    fn create_tasks(
        &self,
        _stage_id: crate::distributed::types::StageId,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        // This is a type-specific implementation that demonstrates the concept
        // In a real system, we would need implementations for each supported type

        use std::any::TypeId;

        // Check if this is an i32 RDD and handle it specifically
        if TypeId::of::<T>() == TypeId::of::<i32>() {
            return self.create_tasks_for_i32(_stage_id);
        }

        // Check if this is a (String, i32) RDD and handle it specifically
        if TypeId::of::<T>() == TypeId::of::<(String, i32)>() {
            return self.create_tasks_for_string_i32_tuple(_stage_id);
        }

        // For other types, we still use unimplemented for now
        // In a complete system, we would add more type-specific implementations
        unimplemented!(
            "DistributedRdd::create_tasks needs implementation for type {:?}",
            std::any::type_name::<T>()
        )
    }
}

impl<T> DistributedRdd<T>
where
    T: crate::operations::RddDataType,
{
    /// Create tasks specifically for i32 RDDs
    fn create_tasks_for_i32(
        &self,
        _stage_id: crate::distributed::types::StageId,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        use crate::distributed::task::ChainedTask;

        // This is safe because we've already checked the type
        let self_as_i32 =
            unsafe { std::mem::transmute::<&DistributedRdd<T>, &DistributedRdd<i32>>(self) };

        // Analyze the lineage to get base data and operations - clone to avoid move
        let (base_data, num_partitions, operations) = self_as_i32.clone().analyze_lineage();

        let mut tasks: Vec<Box<dyn crate::distributed::task::Task>> = Vec::new();

        // Create a task for each partition
        for partition_index in 0..num_partitions {
            // Calculate partition data for this specific partition
            let partition_size = base_data.len() / num_partitions;
            let start = partition_index * partition_size;
            let end = if partition_index == num_partitions - 1 {
                base_data.len()
            } else {
                start + partition_size
            };

            let partition_data = base_data[start..end].to_vec();

            // Serialize the partition data
            let serialized_partition_data =
                bincode::encode_to_vec(&partition_data, bincode::config::standard())
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

            // Create the chained task for i32
            let task = ChainedTask::<i32>::new(serialized_partition_data, operations.clone());

            tasks.push(Box::new(task));
        }

        Ok(tasks)
    }

    /// Create tasks specifically for (String, i32) RDDs
    fn create_tasks_for_string_i32_tuple(
        &self,
        _stage_id: crate::distributed::types::StageId,
    ) -> crate::traits::RddResult<Vec<Box<dyn crate::distributed::task::Task>>> {
        use crate::distributed::task::ChainedTask;

        // This is safe because we've already checked the type
        let self_as_tuple = unsafe {
            std::mem::transmute::<&DistributedRdd<T>, &DistributedRdd<(String, i32)>>(self)
        };

        // Analyze the lineage to get base data and operations - clone to avoid move
        let (base_data, num_partitions, operations) = self_as_tuple.clone().analyze_lineage();

        let mut tasks: Vec<Box<dyn crate::distributed::task::Task>> = Vec::new();

        // Create a task for each partition
        for partition_index in 0..num_partitions {
            // Calculate partition data for this specific partition
            let partition_size = base_data.len() / num_partitions;
            let start = partition_index * partition_size;
            let end = if partition_index == num_partitions - 1 {
                base_data.len()
            } else {
                start + partition_size
            };

            let partition_data = base_data[start..end].to_vec();

            // Serialize the partition data
            let serialized_partition_data =
                bincode::encode_to_vec(&partition_data, bincode::config::standard())
                    .map_err(|e| crate::traits::RddError::SerializationError(e.to_string()))?;

            // Create the chained task for (String, i32)
            let task =
                ChainedTask::<(String, i32)>::new(serialized_partition_data, operations.clone());

            tasks.push(Box::new(task));
        }

        Ok(tasks)
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
}
