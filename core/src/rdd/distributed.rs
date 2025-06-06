//! Distributed RDD implementation with serializable operations
//!
//! This module provides RDD implementations that can be executed
//! in a distributed environment using serializable operations.

use crate::operations::{
    I32Operation, I32Predicate, SerializableI32Operation, StringOperation, StringPredicate,
};
use crate::traits::{BasicPartition, Partition, RddError, RddResult};
use std::fmt::Debug;
use std::sync::Arc;

/// Distributed RDD for i32 values with serializable operations
#[derive(Debug)]
pub enum DistributedI32Rdd {
    /// RDD backed by a vector of data
    Vec {
        data: Arc<Vec<i32>>,
        num_partitions: usize,
    },
    /// RDD that applies a map transformation
    Map {
        parent: Box<DistributedI32Rdd>,
        operation: Box<dyn I32Operation>,
    },
    /// RDD that applies a filter transformation
    Filter {
        parent: Box<DistributedI32Rdd>,
        predicate: Box<dyn I32Predicate>,
    },
}

/// Distributed RDD for String values with serializable operations
#[derive(Debug)]
pub enum DistributedStringRdd {
    /// RDD backed by a vector of data
    Vec {
        data: Arc<Vec<String>>,
        num_partitions: usize,
    },
    /// RDD that applies a map transformation
    Map {
        parent: Box<DistributedStringRdd>,
        operation: Box<dyn StringOperation>,
    },
    /// RDD that applies a filter transformation
    Filter {
        parent: Box<DistributedStringRdd>,
        predicate: Box<dyn StringPredicate>,
    },
}

impl DistributedI32Rdd {
    /// Create a new RDD from a vector of data
    pub fn from_vec(data: Vec<i32>) -> Self {
        Self::Vec {
            data: Arc::new(data),
            num_partitions: 1,
        }
    }

    /// Create a new RDD from a vector with specified number of partitions
    pub fn from_vec_with_partitions(data: Vec<i32>, num_partitions: usize) -> Self {
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
    pub fn map(self, operation: Box<dyn I32Operation>) -> DistributedI32Rdd {
        DistributedI32Rdd::Map {
            parent: Box::new(self),
            operation,
        }
    }

    /// Apply a filter transformation to this RDD
    pub fn filter(self, predicate: Box<dyn I32Predicate>) -> DistributedI32Rdd {
        DistributedI32Rdd::Filter {
            parent: Box::new(self),
            predicate,
        }
    }

    /// Repartition the RDD to have the specified number of partitions
    pub fn repartition(self, num_partitions: usize) -> DistributedI32Rdd {
        if num_partitions <= self.num_partitions() {
            return self.coalesce(num_partitions);
        }

        match self {
            DistributedI32Rdd::Vec { data, .. } => DistributedI32Rdd::Vec {
                data,
                num_partitions,
            },
            _ => {
                // For transformed RDDs, we would need to materialize the data first
                // This is a simplification - real Spark would use shuffle operations
                DistributedI32Rdd::Vec {
                    data: Arc::new(Vec::new()), // Placeholder
                    num_partitions,
                }
            }
        }
    }

    /// Coalesce the RDD to have fewer partitions
    pub fn coalesce(self, num_partitions: usize) -> DistributedI32Rdd {
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
            DistributedI32Rdd::Vec { data, .. } => DistributedI32Rdd::Vec {
                data,
                num_partitions,
            },
            DistributedI32Rdd::Map { parent, operation } => DistributedI32Rdd::Map {
                parent: Box::new(parent.coalesce(num_partitions)),
                operation,
            },
            DistributedI32Rdd::Filter { parent, predicate } => DistributedI32Rdd::Filter {
                parent: Box::new(parent.coalesce(num_partitions)),
                predicate,
            },
        }
    }

    /// Traverses the RDD lineage, collects all serializable operations in reverse order,
    /// and returns the base RDD's data and partitions.
    pub fn analyze_lineage(mut self) -> (Arc<Vec<i32>>, usize, Vec<SerializableI32Operation>) {
        let mut operations = Vec::new();

        loop {
            match self {
                DistributedI32Rdd::Vec {
                    data,
                    num_partitions,
                } => {
                    // Reached the base RDD. Reverse the collected operations
                    // to get the correct execution order.
                    operations.reverse();
                    return (data, num_partitions, operations);
                }
                DistributedI32Rdd::Map { parent, operation } => {
                    operations.push(SerializableI32Operation::Map(dyn_clone::clone_box(
                        &*operation,
                    )));
                    self = *parent;
                }
                DistributedI32Rdd::Filter { parent, predicate } => {
                    operations.push(SerializableI32Operation::Filter(dyn_clone::clone_box(
                        &*predicate,
                    )));
                    self = *parent;
                }
            }
        }
    }

    /// Compute the elements of this RDD for the given partition
    pub fn compute(&self, partition: &dyn Partition) -> RddResult<Vec<i32>> {
        match self {
            DistributedI32Rdd::Vec {
                data,
                num_partitions,
            } => {
                let partition_index = partition.index();
                if partition_index >= *num_partitions {
                    return Err(RddError::InvalidPartition(partition_index));
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
            DistributedI32Rdd::Map { parent, operation } => {
                let parent_data = parent.compute(partition)?;
                Ok(parent_data
                    .into_iter()
                    .map(|item| operation.execute(item))
                    .collect())
            }
            DistributedI32Rdd::Filter { parent, predicate } => {
                let parent_data = parent.compute(partition)?;
                Ok(parent_data
                    .into_iter()
                    .filter(|item| predicate.test(item))
                    .collect())
            }
        }
    }

    /// Get the list of partitions for this RDD
    pub fn partitions(&self) -> Vec<Box<dyn Partition>> {
        match self {
            DistributedI32Rdd::Vec { num_partitions, .. } => (0..*num_partitions)
                .map(|i| Box::new(BasicPartition::new(i)) as Box<dyn Partition>)
                .collect(),
            DistributedI32Rdd::Map { parent, .. } | DistributedI32Rdd::Filter { parent, .. } => {
                parent.partitions()
            }
        }
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.partitions().len()
    }

    /// Collect all elements from all partitions
    pub fn collect(&self) -> RddResult<Vec<i32>> {
        let partitions = self.partitions();
        let mut result = Vec::new();

        for partition in partitions {
            let partition_data = self.compute(partition.as_ref())?;
            result.extend(partition_data);
        }

        Ok(result)
    }
}

impl DistributedStringRdd {
    /// Create a new RDD from a vector of data
    pub fn from_vec(data: Vec<String>) -> Self {
        Self::Vec {
            data: Arc::new(data),
            num_partitions: 1,
        }
    }

    /// Create a new RDD from a vector with specified number of partitions
    pub fn from_vec_with_partitions(data: Vec<String>, num_partitions: usize) -> Self {
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
    pub fn map(self, operation: Box<dyn StringOperation>) -> DistributedStringRdd {
        DistributedStringRdd::Map {
            parent: Box::new(self),
            operation,
        }
    }

    /// Apply a filter transformation to this RDD
    pub fn filter(self, predicate: Box<dyn StringPredicate>) -> DistributedStringRdd {
        DistributedStringRdd::Filter {
            parent: Box::new(self),
            predicate,
        }
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        match self {
            DistributedStringRdd::Vec { num_partitions, .. } => *num_partitions,
            DistributedStringRdd::Map { parent, .. }
            | DistributedStringRdd::Filter { parent, .. } => parent.num_partitions(),
        }
    }

    /// Collect all elements from all partitions
    pub fn collect(&self) -> RddResult<Vec<String>> {
        let partitions = self.partitions();
        let mut result = Vec::new();

        for partition in partitions {
            let partition_data = self.compute(partition.as_ref())?;
            result.extend(partition_data);
        }

        Ok(result)
    }

    /// Get the list of partitions for this RDD
    pub fn partitions(&self) -> Vec<Box<dyn Partition>> {
        match self {
            DistributedStringRdd::Vec { num_partitions, .. } => (0..*num_partitions)
                .map(|i| Box::new(BasicPartition::new(i)) as Box<dyn Partition>)
                .collect(),
            DistributedStringRdd::Map { parent, .. }
            | DistributedStringRdd::Filter { parent, .. } => parent.partitions(),
        }
    }

    /// Compute the elements of this RDD for the given partition
    pub fn compute(&self, partition: &dyn Partition) -> RddResult<Vec<String>> {
        match self {
            DistributedStringRdd::Vec {
                data,
                num_partitions,
            } => {
                let partition_index = partition.index();
                if partition_index >= *num_partitions {
                    return Err(RddError::InvalidPartition(partition_index));
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
            DistributedStringRdd::Map { parent, operation } => {
                let parent_data = parent.compute(partition)?;
                Ok(parent_data
                    .into_iter()
                    .map(|item| operation.execute(item))
                    .collect())
            }
            DistributedStringRdd::Filter { parent, predicate } => {
                let parent_data = parent.compute(partition)?;
                Ok(parent_data
                    .into_iter()
                    .filter(|item| predicate.test(item))
                    .collect())
            }
        }
    }
}
