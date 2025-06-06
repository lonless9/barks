//! FlowContext - Local execution context for RDD operations
//!
//! This module provides a simple, non-distributed context for managing
//! RDD operations in a single-threaded environment.

use crate::rdd::SimpleRdd;
use crate::scheduler::{LocalScheduler, Task};
use crate::traits::{Partition, RddResult};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

/// FlowContext manages RDD operations in a local environment with unified parallel/sequential execution
#[derive(Debug)]
pub struct FlowContext {
    app_name: String,
    scheduler: LocalScheduler,
    default_parallelism: usize,
}

impl FlowContext {
    /// Create a new FlowContext with the given application name
    pub fn new(app_name: impl Into<String>) -> Self {
        let scheduler = LocalScheduler::default();
        let default_parallelism = scheduler.num_threads();
        Self {
            app_name: app_name.into(),
            scheduler,
            default_parallelism,
        }
    }

    /// Create a new FlowContext with the given application name and number of threads
    pub fn new_with_threads(app_name: impl Into<String>, num_threads: usize) -> Self {
        Self {
            app_name: app_name.into(),
            scheduler: LocalScheduler::new(num_threads),
            default_parallelism: num_threads,
        }
    }

    /// Get the application name
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// Get the scheduler
    pub fn scheduler(&self) -> &LocalScheduler {
        &self.scheduler
    }

    /// Get the number of threads in the scheduler
    pub fn num_threads(&self) -> usize {
        self.scheduler.num_threads()
    }

    /// Get the default parallelism (number of partitions to use when not specified)
    pub fn default_parallelism(&self) -> usize {
        self.default_parallelism
    }

    /// Create an RDD from a vector of data using default parallelism
    /// Similar to Spark's parallelize() without numSlices parameter
    pub fn parallelize<T>(&self, data: Vec<T>) -> SimpleRdd<T>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        SimpleRdd::from_vec_with_partitions(data, self.default_parallelism)
    }

    /// Create an RDD from a vector with specified number of partitions
    /// Similar to Spark's parallelize(data, numSlices)
    /// When num_slices is None, uses defaultParallelism
    /// When num_slices is Some(1), creates single partition (no parallel advantages)
    /// When num_slices is Some(n > 1), enables parallel processing
    pub fn parallelize_with_slices<T>(
        &self,
        data: Vec<T>,
        num_slices: Option<usize>,
    ) -> SimpleRdd<T>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        let num_partitions = num_slices.unwrap_or(self.default_parallelism);
        // Ensure minimum of 2 partitions for parallel processing unless explicitly set to 1
        let num_partitions = if num_slices.is_none() && num_partitions < 2 {
            2
        } else {
            num_partitions
        };
        SimpleRdd::from_vec_with_partitions(data, num_partitions)
    }

    /// Create an RDD from a vector with specified number of partitions
    /// Kept for backward compatibility
    pub fn parallelize_with_partitions<T>(
        &self,
        data: Vec<T>,
        num_partitions: usize,
    ) -> SimpleRdd<T>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        SimpleRdd::from_vec_with_partitions(data, num_partitions)
    }

    /// Run an RDD computation in parallel and collect the results
    pub fn run<T>(&self, rdd: SimpleRdd<T>) -> RddResult<Vec<T>>
    where
        T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + Debug + 'static,
    {
        // Create tasks for each partition
        let partitions = rdd.partitions();
        let rdd_arc = Arc::new(rdd);
        let tasks: Vec<Task<T>> = partitions
            .into_iter()
            .map(|partition| {
                let rdd_clone = Arc::clone(&rdd_arc);
                let compute_fn = Arc::new(move |p: &dyn Partition| rdd_clone.compute(p));
                Task::new(partition, compute_fn)
            })
            .collect();

        // Execute tasks in parallel using the scheduler
        self.scheduler.execute_and_collect(tasks)
    }
}

impl Default for FlowContext {
    fn default() -> Self {
        Self::new("barks-app")
    }
}
