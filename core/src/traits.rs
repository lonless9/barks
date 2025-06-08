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
    /// A one-to-one dependency where each partition of the child RDD depends on a single partition of the parent.
    Narrow(NarrowDependency),
    /// A shuffle dependency where partitions of the child RDD depend on multiple partitions of the parent.
    Shuffle(ShuffleDependencyInfo),
}

/// Represents a narrow dependency between RDDs
#[derive(Clone, Debug)]
pub struct NarrowDependency {
    pub parent_rdd_id: usize,
    pub partition_mapping: NarrowDependencyType,
}

/// Types of narrow dependencies
#[derive(Clone, Debug)]
pub enum NarrowDependencyType {
    /// One-to-one mapping: child partition i depends on parent partition i
    OneToOne,
    /// Range dependency: child partition depends on a range of parent partitions
    Range { start: usize, end: usize },
}

/// Represents a shuffle dependency between RDDs
#[derive(Clone, Debug)]
pub struct ShuffleDependencyInfo {
    pub shuffle_id: usize,
    pub parent_rdd_id: usize,
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
            Dependency::Narrow(_) => write!(f, "Narrow(...)"),
            Dependency::Shuffle(_) => write!(f, "Shuffle(...)"),
        }
    }
}
