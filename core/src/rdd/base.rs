//! Base RDD implementations
//!
//! This module contains the fundamental RDD types for Phase 0 implementation.
//! Uses a simplified concrete approach for single-threaded execution.

use crate::traits::{BasicPartition, Partition, RddError, RddResult};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

/// SimpleRdd is a concrete RDD implementation for Phase 0
/// It represents different types of RDD operations as an enum
pub enum SimpleRdd<T>
where
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    /// RDD backed by a vector of data
    Vec {
        data: Arc<Vec<T>>,
        num_partitions: usize,
    },
    /// RDD that applies a map transformation
    Map {
        parent: Box<SimpleRdd<T>>,
        func: Arc<dyn Fn(T) -> T + Send + Sync>,
    },
    /// RDD that applies a filter transformation
    Filter {
        parent: Box<SimpleRdd<T>>,
        predicate: Arc<dyn Fn(&T) -> bool + Send + Sync>,
    },
}

impl<T> Debug for SimpleRdd<T>
where
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimpleRdd::Vec {
                data,
                num_partitions,
            } => f
                .debug_struct("SimpleRdd::Vec")
                .field("data_len", &data.len())
                .field("num_partitions", num_partitions)
                .finish(),
            SimpleRdd::Map { parent, .. } => f
                .debug_struct("SimpleRdd::Map")
                .field("parent", parent)
                .field("func", &"<map_function>")
                .finish(),
            SimpleRdd::Filter { parent, .. } => f
                .debug_struct("SimpleRdd::Filter")
                .field("parent", parent)
                .field("predicate", &"<filter_predicate>")
                .finish(),
        }
    }
}

impl<T> Clone for SimpleRdd<T>
where
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    fn clone(&self) -> Self {
        match self {
            SimpleRdd::Vec {
                data,
                num_partitions,
            } => SimpleRdd::Vec {
                data: Arc::clone(data),
                num_partitions: *num_partitions,
            },
            SimpleRdd::Map { parent, func } => SimpleRdd::Map {
                parent: parent.clone(),
                func: Arc::clone(func),
            },
            SimpleRdd::Filter { parent, predicate } => SimpleRdd::Filter {
                parent: parent.clone(),
                predicate: Arc::clone(predicate),
            },
        }
    }
}

impl<T> SimpleRdd<T>
where
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
{
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
    pub fn map<F>(self, f: F) -> SimpleRdd<T>
    where
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        SimpleRdd::Map {
            parent: Box::new(self),
            func: Arc::new(f),
        }
    }

    /// Apply a filter transformation to this RDD
    pub fn filter<F>(self, predicate: F) -> SimpleRdd<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        SimpleRdd::Filter {
            parent: Box::new(self),
            predicate: Arc::new(predicate),
        }
    }

    /// Repartition the RDD to have the specified number of partitions
    /// This increases parallelism by redistributing data across more partitions
    /// Similar to Spark's repartition() method
    pub fn repartition(self, num_partitions: usize) -> SimpleRdd<T> {
        if num_partitions <= self.num_partitions() {
            // If not increasing partitions, use coalesce instead
            return self.coalesce(num_partitions);
        }

        // For increasing partitions, we need to collect and redistribute
        // This is a simplified implementation - in a real distributed system,
        // this would involve shuffling data across the network
        SimpleRdd::Vec {
            data: match self {
                SimpleRdd::Vec { data, .. } => data,
                _ => {
                    // For transformed RDDs, we need to materialize the data first
                    // This is a simplification - real Spark would use shuffle operations
                    Arc::new(Vec::new()) // Placeholder - would need full computation
                }
            },
            num_partitions,
        }
    }

    /// Coalesce the RDD to have fewer partitions
    /// This reduces parallelism to avoid excessive small tasks
    /// Similar to Spark's coalesce() method
    pub fn coalesce(self, num_partitions: usize) -> SimpleRdd<T> {
        let current_partitions = self.num_partitions();
        if num_partitions >= current_partitions {
            return self; // No need to coalesce
        }

        let num_partitions = if num_partitions == 0 {
            1
        } else {
            num_partitions
        };

        match self {
            SimpleRdd::Vec { data, .. } => SimpleRdd::Vec {
                data,
                num_partitions,
            },
            SimpleRdd::Map { parent, func } => SimpleRdd::Map {
                parent: Box::new(parent.coalesce(num_partitions)),
                func,
            },
            SimpleRdd::Filter { parent, predicate } => SimpleRdd::Filter {
                parent: Box::new(parent.coalesce(num_partitions)),
                predicate,
            },
        }
    }

    /// Compute the elements of this RDD for the given partition
    pub fn compute(&self, partition: &dyn Partition) -> RddResult<Vec<T>> {
        match self {
            SimpleRdd::Vec {
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
            SimpleRdd::Map { parent, func } => {
                let parent_data = parent.compute(partition)?;
                Ok(parent_data.into_iter().map(|item| func(item)).collect())
            }
            SimpleRdd::Filter { parent, predicate } => {
                let parent_data = parent.compute(partition)?;
                Ok(parent_data
                    .into_iter()
                    .filter(|item| predicate(item))
                    .collect())
            }
        }
    }

    /// Get the list of partitions for this RDD
    pub fn partitions(&self) -> Vec<Box<dyn Partition>> {
        match self {
            SimpleRdd::Vec { num_partitions, .. } => (0..*num_partitions)
                .map(|i| Box::new(BasicPartition::new(i)) as Box<dyn Partition>)
                .collect(),
            SimpleRdd::Map { parent, .. } | SimpleRdd::Filter { parent, .. } => parent.partitions(),
        }
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.partitions().len()
    }
}
