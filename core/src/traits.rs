//! Core traits for Barks distributed computing framework
//!
//! This module defines the fundamental abstractions for RDDs (Resilient Distributed Datasets)
//! and related operations in the Barks framework.

use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;
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

/// Core RDD trait that defines the fundamental operations for Resilient Distributed Datasets
/// This trait is object-safe and contains only the essential methods for computation
pub trait Rdd<T>: Send + Sync + Debug
where
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    /// Compute the elements of this RDD for the given partition
    fn compute(&self, partition: &dyn Partition) -> RddResult<Vec<T>>;

    /// Get the list of partitions for this RDD
    fn partitions(&self) -> Vec<Box<dyn Partition>>;

    /// Get the number of partitions
    fn num_partitions(&self) -> usize {
        self.partitions().len()
    }

    /// Get dependencies of this RDD (for lineage tracking)
    fn dependencies(&self) -> Vec<Box<dyn Any + Send + Sync>> {
        Vec::new()
    }
}
