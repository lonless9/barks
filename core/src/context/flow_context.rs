//! FlowContext - Local execution context for RDD operations
//!
//! This module provides a simple, non-distributed context for managing
//! RDD operations in a single-threaded environment.

use crate::rdd::SimpleRdd;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// FlowContext manages RDD operations in a local, single-threaded environment
#[derive(Debug)]
pub struct FlowContext {
    app_name: String,
}

impl FlowContext {
    /// Create a new FlowContext with the given application name
    pub fn new(app_name: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
        }
    }

    /// Get the application name
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// Create an RDD from a vector of data
    pub fn parallelize<T>(&self, data: Vec<T>) -> SimpleRdd<T>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        SimpleRdd::from_vec(data)
    }

    /// Create an RDD from a vector with specified number of partitions
    pub fn parallelize_with_partitions<T>(&self, data: Vec<T>, num_partitions: usize) -> SimpleRdd<T>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        SimpleRdd::from_vec_with_partitions(data, num_partitions)
    }
}

impl Default for FlowContext {
    fn default() -> Self {
        Self::new("barks-app")
    }
}
