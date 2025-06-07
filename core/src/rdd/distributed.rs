//! Distributed RDD implementation with serializable operations
//!
//! This module provides RDD implementations that can be executed
//! in a distributed environment using serializable operations.

use crate::operations::RddDataType;
use crate::traits::{BasicPartition, Partition};
use std::fmt::Debug;
use std::sync::Arc;

/// A Resilient Distributed Dataset (RDD) that is designed for execution on a cluster.
/// It is generic over the data type `T`.
///
/// To support a new data type, one must implement the `RddDataType` trait for it.
#[derive(Debug)]
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
                    // This part is tricky. We need to convert from T::MapOperation to T::SerializableOperation
                    // We do this by downcasting the data type T.
                    let op_any = &operation as &dyn std::any::Any;
                    if let Some(i32_op) =
                        op_any.downcast_ref::<Box<dyn crate::operations::I32Operation>>()
                    {
                        let serializable_op =
                            crate::operations::SerializableI32Operation::Map(i32_op.clone());
                        // We need to cast this to T::SerializableOperation
                        let op_any = &serializable_op as &dyn std::any::Any;
                        if let Some(typed_op) = op_any.downcast_ref::<T::SerializableOperation>() {
                            operations.push(typed_op.clone());
                        }
                    } else if let Some(str_op) =
                        op_any.downcast_ref::<Box<dyn crate::operations::StringOperation>>()
                    {
                        let serializable_op =
                            crate::operations::SerializableStringOperation::Map(str_op.clone());
                        let op_any = &serializable_op as &dyn std::any::Any;
                        if let Some(typed_op) = op_any.downcast_ref::<T::SerializableOperation>() {
                            operations.push(typed_op.clone());
                        }
                    }
                    current_rdd = *parent;
                }
                Self::Filter { parent, predicate } => {
                    let pred_any = &predicate as &dyn std::any::Any;
                    if let Some(i32_pred) =
                        pred_any.downcast_ref::<Box<dyn crate::operations::I32Predicate>>()
                    {
                        let serializable_op =
                            crate::operations::SerializableI32Operation::Filter(i32_pred.clone());
                        let op_any = &serializable_op as &dyn std::any::Any;
                        if let Some(typed_op) = op_any.downcast_ref::<T::SerializableOperation>() {
                            operations.push(typed_op.clone());
                        }
                    } else if let Some(str_pred) =
                        pred_any.downcast_ref::<Box<dyn crate::operations::StringPredicate>>()
                    {
                        let serializable_op =
                            crate::operations::SerializableStringOperation::Filter(
                                str_pred.clone(),
                            );
                        let op_any = &serializable_op as &dyn std::any::Any;
                        if let Some(typed_op) = op_any.downcast_ref::<T::SerializableOperation>() {
                            operations.push(typed_op.clone());
                        }
                    }
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
}

// Specific implementations for i32
impl DistributedRdd<i32> {
    /// Compute the elements of this RDD for the given partition (for i32)
    pub fn compute(&self, partition: &dyn Partition) -> crate::traits::RddResult<Vec<i32>> {
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
                let partition_size = (data_len + num_partitions - 1) / num_partitions;
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
                Ok(parent_data
                    .into_iter()
                    .map(|item| operation.execute(item))
                    .collect())
            }
            Self::Filter { parent, predicate } => {
                let parent_data = parent.compute(partition)?;
                Ok(parent_data
                    .into_iter()
                    .filter(|item| predicate.test(item))
                    .collect())
            }
        }
    }

    /// Collect all elements from all partitions (for i32)
    pub fn collect(&self) -> crate::traits::RddResult<Vec<i32>> {
        let partitions = self.partitions();
        let mut result = Vec::new();

        for partition in partitions {
            let partition_data = self.compute(partition.as_ref())?;
            result.extend(partition_data);
        }

        Ok(result)
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
}
