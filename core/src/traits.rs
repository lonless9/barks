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

/// Base trait for all RDDs, containing non-generic methods.
pub trait RddBase: Send + Sync + Debug + Any {
    type Item: Data;

    /// Downcast self to a `&dyn Any`.
    fn as_any(&self) -> &dyn Any;

    /// Compute the elements of this RDD for the given partition
    fn compute(&self, partition: &dyn Partition)
    -> RddResult<Box<dyn Iterator<Item = Self::Item>>>;

    /// Get the number of partitions
    fn num_partitions(&self) -> usize;

    /// Get dependencies of this RDD (for lineage tracking)
    fn dependencies(&self) -> Vec<Dependency>;

    /// Get a unique ID for this RDD.
    fn id(&self) -> usize;

    /// Get the list of partitions for this RDD
    fn partitions(&self) -> Vec<Arc<dyn Partition>> {
        (0..self.num_partitions())
            .map(|i| Arc::new(BasicPartition::new(i)) as Arc<dyn Partition>)
            .collect()
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
}

/// A data type that can be used in an RDD.
pub trait Data:
    Send + Sync + Clone + Debug + Serialize + for<'de> Deserialize<'de> + 'static
{
}
impl<T> Data for T where
    T: Send + Sync + Clone + Debug + Serialize + for<'de> Deserialize<'de> + 'static
{
}

/// Represents a dependency of an RDD on its parent(s).
#[derive(Clone)]
pub enum Dependency {
    /// A narrow dependency on a parent RDD. The parent is stored as `Any` to handle different RDD types.
    Narrow(Arc<dyn Any + Send + Sync>),
    /// A shuffle dependency, which marks a stage boundary. Holds the parent RDD and shuffle metadata.
    Shuffle(Arc<dyn Any + Send + Sync>, ShuffleDependencyInfo),
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
