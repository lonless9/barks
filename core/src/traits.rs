//! Core traits for Barks distributed computing framework
//!
//! This module defines the fundamental abstractions for RDDs (Resilient Distributed Datasets)
//! and related operations in the Barks framework.

use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

/// Error types for RDD operations
#[derive(Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RddError {
    #[error("Computation failed: {0}")]
    ComputationError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Invalid partition: {0}")]
    InvalidPartition(usize),

    #[error("Context error: {0}")]
    ContextError(String),

    #[error("Shuffle error: {0}")]
    ShuffleError(String),

    #[error("Task creation error: {0}")]
    TaskCreationError(String),
}

/// Result type for RDD operations
pub type RddResult<T> = Result<T, RddError>;

/// Partition represents a logical partition of data in an RDD
pub trait Partition: Send + Sync + Debug {
    /// Get the partition index
    fn index(&self) -> usize;

    /// Get a unique identifier for this partition
    fn id(&self) -> String {
        format!("partition_{}", self.index())
    }
}

/// Basic partition implementation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicPartition {
    index: usize,
}

impl BasicPartition {
    pub fn new(index: usize) -> Self {
        Self { index }
    }
}

impl Partition for BasicPartition {
    fn index(&self) -> usize {
        self.index
    }
}

/// Base trait for type-erased RDDs. This allows storing different RDDs in a dependency graph.
pub trait IsRdd: Send + Sync + Debug + Any {
    fn id(&self) -> usize;
    fn dependencies(&self) -> Vec<Dependency>;
    fn num_partitions(&self) -> usize;
    fn as_any(&self) -> &dyn Any;

    /// Create tasks for a given stage. This method allows type-erased task creation.
    fn create_tasks_erased(
        &self,
        stage_id: crate::distributed::types::StageId,
        shuffle_info: Option<&ShuffleDependencyInfo>,
        map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> RddResult<Vec<Box<dyn crate::distributed::task::Task>>>;
}

/// Base trait for all RDDs, containing generic methods.
pub trait RddBase: IsRdd {
    type Item: Data;

    /// Compute the elements of this RDD for the given partition
    fn compute(&self, partition: &dyn Partition)
    -> RddResult<Box<dyn Iterator<Item = Self::Item>>>;

    /// Get the list of partitions for this RDD
    fn partitions(&self) -> Vec<Arc<dyn Partition>> {
        (0..self.num_partitions())
            .map(|i| Arc::new(BasicPartition::new(i)) as Arc<dyn Partition>)
            .collect()
    }

    /// Get the storage level for this RDD
    fn storage_level(&self) -> crate::cache::StorageLevel {
        crate::cache::StorageLevel::None
    }

    /// Check if this RDD is cached
    fn is_cached(&self) -> bool {
        self.storage_level().is_cached()
    }

    /// Create tasks for a given stage. This method eliminates the need for downcasting
    /// in the distributed scheduler by allowing each RDD type to define its own task creation logic.
    fn create_tasks(
        &self,
        stage_id: crate::distributed::types::StageId,
        // Info about the shuffle this stage is a map stage for, if any.
        shuffle_info: Option<&ShuffleDependencyInfo>,
        // Add this parameter to pass information from the driver.
        // It contains, for each parent dependency, a Vec of outputs from each map task.
        map_output_info: Option<
            &[Vec<(
                barks_network_shuffle::traits::MapStatus,
                crate::distributed::types::ExecutorInfo,
            )>],
        >,
    ) -> RddResult<Vec<Box<dyn crate::distributed::task::Task>>>;

    /// Helper method to get an Arc<dyn IsRdd> from an Arc<dyn RddBase>
    /// This works around the lack of trait upcasting coercion in Rust
    fn as_is_rdd(self: Arc<Self>) -> Arc<dyn IsRdd>;
}

/// A data type that can be used in an RDD.
pub trait Data:
    Send
    + Sync
    + Clone
    + Debug
    + Serialize
    + for<'de> Deserialize<'de>
    + bincode::Encode
    + bincode::Decode<()>
    + 'static
{
}
impl<T> Data for T where
    T: Send
        + Sync
        + Clone
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + bincode::Encode
        + bincode::Decode<()>
        + 'static
{
}

/// A trait for types that can be treated as a key-value pair.
pub trait Pair {
    type Key: Data;
    type Value: Data;

    fn key(&self) -> &Self::Key;
    fn value(&self) -> &Self::Value;
    fn into_pair(self) -> (Self::Key, Self::Value);
}

impl<K, V> Pair for (K, V)
where
    K: Data,
    V: Data,
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        &self.0
    }

    fn value(&self) -> &Self::Value {
        &self.1
    }

    fn into_pair(self) -> (Self::Key, Self::Value) {
        self
    }
}

/// Represents a dependency of an RDD on its parent(s).
#[derive(Clone)]
pub enum Dependency {
    /// A narrow dependency on a parent RDD.
    Narrow(Arc<dyn IsRdd>),
    /// A shuffle dependency, which marks a stage boundary. Holds the parent RDD and shuffle metadata.
    Shuffle(Arc<dyn IsRdd>, ShuffleDependencyInfo),
}

/// Represents a shuffle dependency between RDDs
#[derive(Clone, Debug)]
pub struct ShuffleDependencyInfo {
    pub shuffle_id: usize,
    pub num_partitions: u32,
    pub partitioner_type: PartitionerType,
}

/// Types of partitioners for shuffle dependencies
#[derive(Clone, Debug)]
pub enum PartitionerType {
    Hash { num_partitions: u32, seed: u64 },
    Range { num_partitions: u32 },
    Custom { num_partitions: u32 },
}

impl std::fmt::Debug for Dependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dependency::Narrow(_) => write!(f, "NarrowDependency"),
            Dependency::Shuffle(_, info) => write!(f, "ShuffleDependency({:?})", info),
        }
    }
}
